from datetime import datetime, timezone
from types import SimpleNamespace as NS
import threading
import time

import sys
from types import ModuleType

import pytest

from conftest import native_nodes, native_domain
from dataclasses import dataclass
from fastsearch.subscription import (SUBSCRIPTION_SCOPE, make_advance_sources,
                                     make_full_pack_first, make_owned_task)


def scan_fixture(full=True, page_limit=None):
    domain = native_domain()
    scope = native_nodes("app/chain/search/scan.py", {"SearchScan"}, {
        "datetime": datetime, "timezone": timezone, "time": time, "SEARCH_SLICE_SECONDS": 60,
        "UNMATCHED_PAGE_LIMIT": 3,
        "logger": NS(warning=lambda *_: None),
        "torrent_snapshot": lambda torrent: {"id": torrent.id},
        "SearchResultOwner": NS(_identify_page=lambda **kwargs: (
            [domain.SearchResourceEvidence(t.id, frozenset({"1:season"})) for t in kwargs["torrents"]],
            {t.id: NS(torrent_info=t) for t in kwargs["torrents"]})),
    })
    Scan = scope["SearchScan"]
    scan = object.__new__(Scan)
    scan.owner = object()
    scan.execution = NS(task_id="task", task_lease="task-lease",
                        lease=NS(subscription_id=3, owner_token="owner-lease"))
    scan.sources = NS(queries={"1:show": {"site": "1"}, "2:show": {"site": "2"}})
    scan.collection = domain.SearchCollection({"1:season"}, {
        key: domain.SearchSourceCursor(page_limit=page_limit) for key in scan.queries})
    scan.full, scan.retry, scan.retry_reasons = full, {}, {}
    scan.live, scan.candidates = {}, {}
    scan.media, scan.identity, scan.disambiguation = NS(), {}, {}
    scan.plan = NS(custom_words=[], area="title")
    scan._stopping = lambda: False
    scan._budget = lambda: budget
    scan.checkpoint = lambda: checkpoints.append(threading.get_ident())
    scan._stop_unmatched_query = lambda key: False
    scan._defer = lambda: (_ for _ in ()).throw(RuntimeError("deferred"))
    checkpoints, requests, budget = [], [], object()
    request_lock = threading.Lock()
    def fetch(key, page, *, budget):
        with request_lock:
            requests.append((key, page, threading.get_ident(), budget))
        time.sleep(.005)
        torrent = NS(id=f"{key}:{page}", enclosure="ticket", title="pack" if page == 1 else "episode")
        return NS(torrents=[torrent], retry_at=None, wait_reason=None,
                  observation=NS(attempted=True, outcome="success", raw_count=1, has_more=page < 1, error=None))
    scan.sources.fetch = fetch
    scan.sources.pageable = lambda key: True
    submitted = []
    def choose(ready, submit):
        submitted.append([context.torrent_info.title for context in scan.live.values()])
        scan.collection.settle(submit(list(scan.live.values()), ready))
    scan.choose = choose
    return scan, Scan._advance_sources, requests, checkpoints, submitted


def activate(scan):
    ex = scan.execution
    return SUBSCRIPTION_SCOPE.set((scan.owner, ex.task_id, ex.task_lease, ex.lease.subscription_id, ex.lease.owner_token))


@pytest.mark.parametrize("full", [True, False])
def test_native_step_ready_retains_deep_pack_and_single_state_writer(full):
    scan, native, requests, checkpoints, submitted = scan_fixture(full=full)
    call = make_advance_sources(native, lambda: True)
    token = activate(scan)
    try:
        assert call(scan, list(scan.queries), lambda items, ready: ready, time.monotonic())
        assert not submitted
        assert all(cursor.next_page == 1 for cursor in scan.collection.sources.values())
        assert call(scan, list(scan.queries), lambda items, ready: ready, time.monotonic())
    finally:
        SUBSCRIPTION_SCOPE.reset(token)
    assert len(submitted) == 1 and submitted[0].count("pack") == 2
    assert sorted((key, page) for key, page, *_ in requests) == [
        ("1:show", 0), ("1:show", 1), ("2:show", 0), ("2:show", 1)]
    assert set(checkpoints) == {threading.get_ident()}
    assert all(thread_id != threading.get_ident() for _, _, thread_id, _ in requests)
    assert "step" not in vars(scan)
    assert not scan.collection.remaining


def test_native_single_page_policy_not_expanded():
    scan, native, requests, _, submitted = scan_fixture(page_limit=1)
    token = activate(scan)
    try:
        make_advance_sources(native, lambda: True)(scan, list(scan.queries), lambda items, ready: ready, time.monotonic())
    finally:
        SUBSCRIPTION_SCOPE.reset(token)
    assert len(submitted) == 1
    assert all(page == 0 for _, page, *_ in requests)
    assert scan.collection.active_sources(full=True) == []


@pytest.mark.parametrize("mismatch", ["owner", "task_id", "task_lease", "subscription_id", "owner_token"])
def test_scope_mismatch_uses_original_only(mismatch):
    scan, native, requests, _, _ = scan_fixture()
    token = activate(scan)
    if mismatch == "owner":
        scan.owner = object()
    elif mismatch in {"task_id", "task_lease"}:
        setattr(scan.execution, mismatch, "wrong")
    else:
        setattr(scan.execution.lease, mismatch, "wrong")
    try:
        make_advance_sources(native, lambda: True)(scan, list(scan.queries), lambda items, ready: ready, time.monotonic())
    finally:
        SUBSCRIPTION_SCOPE.reset(token)
    assert all(thread_id == threading.get_ident() for _, _, thread_id, _ in requests)


def test_recovery_site_is_not_prefetched_and_instance_overrides_restored():
    scan, _, requests, _, _ = scan_fixture()
    scan.candidates["old"] = {"source": "2:show", "page": 8}
    scan.queries["3:show"] = {"site": "3"}
    scan.collection.sources["3:show"] = native_domain().SearchSourceCursor()
    original_fetch = scan.sources.fetch
    original_step = scan.step
    def advance(scan, keys, submit, started):
        # Recovery outside step must never consume the prefetched current page.
        scan.sources.fetch("2:show", 8, budget=scan._budget())
        scan.step("1:show")
        raise RuntimeError("deferred")
    token = activate(scan)
    try:
        with pytest.raises(RuntimeError, match="deferred"):
            make_advance_sources(advance, lambda: True)(scan, list(scan.queries), None, time.monotonic())
    finally:
        SUBSCRIPTION_SCOPE.reset(token)
    assert [(key, page) for key, page, *_ in requests if key == "2:show"] == [("2:show", 8)]
    assert scan.sources.fetch is original_fetch
    assert scan.step == original_step
    assert "step" not in vars(scan)
    assert not any(t.name.startswith("fastsearch-page") for t in threading.enumerate())


@pytest.mark.parametrize("source", ["new", "manual", "fallback", "targeted"])
def test_runner_source_scope_not_subscription_state(source):
    runner = NS(task=NS(source=source), searchchain=object())
    lease = NS(subscription_id=9, owner_token="lease")
    def native(runner, **kwargs):
        assert (SUBSCRIPTION_SCOPE.get() is not None) == (source == "new")
        assert kwargs["subscribe"].state == "R"
        return 42
    assert make_owned_task(native, lambda: True)(runner, subscribe=NS(state="R"), task_id="t",
                                               lease_token="l", execution_lease=lease, cancelled=lambda: False) == 42
    assert SUBSCRIPTION_SCOPE.get() is None


@dataclass(frozen=True, slots=True)
class FakeSubscribe:
    best_version: int = 0
    best_version_full: int = 0
    downloaded: tuple = ()


def priority_module(monkeypatch):
    package = ModuleType("app.application.subscription")
    package.__path__ = []
    priority = ModuleType("app.application.subscription.priority")
    priority.is_full_best_version_enabled = lambda subscribe: bool(
        subscribe.best_version and subscribe.best_version_full)
    priority.get_downloaded_best_version_episodes = lambda subscribe: list(subscribe.downloaded)
    package.priority = priority
    monkeypatch.setitem(sys.modules, "app.application.subscription", package)
    monkeypatch.setitem(sys.modules, "app.application.subscription.priority", priority)
    return priority


def test_full_pack_first_asks_once_more_on_a_first_search(monkeypatch):
    priority_module(monkeypatch)
    seen = []

    def original(subscribe, mediakey):
        seen.append((subscribe.best_version, mediakey))
        return {"season": 1} if subscribe.best_version else None

    build = make_full_pack_first(original, lambda: True)
    assert build(FakeSubscribe(), "media-key") == {"season": 1}
    assert seen == [(0, "media-key"), (1, "media-key")]


def test_full_pack_first_never_questions_a_native_answer(monkeypatch):
    priority_module(monkeypatch)
    seen = []

    def original(subscribe, mediakey):
        seen.append(subscribe.best_version)
        return {"native": True}

    build = make_full_pack_first(original, lambda: True)
    assert build(FakeSubscribe(), "k") == {"native": True}
    assert seen == [0]


def test_full_pack_first_leaves_downloaded_and_full_best_version_alone(monkeypatch):
    priority_module(monkeypatch)
    seen = []

    def original(subscribe, mediakey):
        seen.append(subscribe.best_version)
        return None

    build = make_full_pack_first(original, lambda: True)
    assert build(FakeSubscribe(downloaded=(1, 2)), "k") is None
    assert build(FakeSubscribe(best_version=1, best_version_full=1), "k") is None
    assert seen == [0, 1]


def test_full_pack_first_stays_off_when_the_plugin_is_disabled(monkeypatch):
    priority_module(monkeypatch)
    seen = []

    def original(subscribe, mediakey):
        seen.append(subscribe.best_version)
        return None

    build = make_full_pack_first(original, lambda: False)
    assert build(FakeSubscribe(), "k") is None
    assert seen == [0]
