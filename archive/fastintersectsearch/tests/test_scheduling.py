import asyncio
from contextlib import aclosing
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS
import threading
import time

import pytest

from conftest import native_nodes
from fastsearch.hooks import Hooks
from fastsearch.scheduling import (SiteRequestGate, make_site_sync, make_site_async,
    make_read_pages, make_execution_run, make_execution_events, make_keyword_delay,
    make_resource_pages, make_title_scope, SEARCH_SCOPE)


def test_binding_inheritance_and_third_party_ownership():
    class Base:
        def run(self, submit):
            return self, submit
    class Child(Base):
        pass
    original = Child.run
    hooks = Hooks()
    hooks.install(Child, "run", lambda self, submit: original(self, submit))
    child = Child()
    assert child.run(3) == (child, 3)
    hooks.close()
    assert "run" not in vars(Child)
    hooks.install(Child, "run", lambda self, submit: 1)
    third = lambda self, submit: 2
    Child.run = third
    hooks.close()
    assert Child.run is third


def test_static_descriptor_restored():
    class Owner:
        @staticmethod
        def run(owner, plan):
            return owner, plan
    original = vars(Owner)["run"]
    hooks = Hooks()
    hooks.install(Owner, "run", staticmethod(lambda owner, plan: (owner, plan)))
    assert Owner().run(1, 2) == (1, 2)
    hooks.close()
    assert vars(Owner)["run"] is original


def test_same_site_sync_async_spacing_and_other_site_independence():
    async def scenario():
        gate = SiteRequestGate(interval=.04, poll_interval=.001)
        starts, active, lock = [], set(), threading.Lock()
        def transport(owner, site, keyword, mtype=None, page=0):
            with lock:
                assert site["id"] not in active
                active.add(site["id"])
                starts.append((site["id"], time.monotonic(), keyword, page))
            time.sleep(.012)
            with lock:
                active.remove(site["id"])
            return [keyword, page]
        async def async_transport(*args, **kwargs):
            return await asyncio.to_thread(transport, *args, **kwargs)
        sync = make_site_sync(transport, gate, lambda: True)
        asynchronous = make_site_async(async_transport, gate, lambda: True)
        results = await asyncio.gather(
            asyncio.to_thread(sync, None, {"id": 1}, "a", page=2),
            asynchronous(None, {"id": 1}, "b", page=3),
            asynchronous(None, {"id": 2}, "c", page=4))
        assert results == [["a", 2], ["b", 3], ["c", 4]]
        first_site = sorted(t for site, t, _, _ in starts if site == 1)
        other = next(t for site, t, _, _ in starts if site == 2)
        assert first_site[1] - first_site[0] >= .039
        assert other < first_site[1]
        assert not active
    asyncio.run(scenario())


def test_cancelled_transport_keeps_slot_until_worker_finishes():
    async def scenario():
        gate = SiteRequestGate(interval=0, poll_interval=.001)
        entered, release, calls = asyncio.Event(), asyncio.Event(), []
        async def transport(owner, site, keyword, mtype=None, page=0):
            calls.append(keyword)
            if keyword == "first":
                entered.set()
                await release.wait()
            return keyword
        call = make_site_async(transport, gate, lambda: True)
        first = asyncio.create_task(call(None, {"id": 1}, "first"))
        await entered.wait()
        first.cancel()
        second = asyncio.create_task(call(None, {"id": 1}, "second"))
        await asyncio.sleep(.01)
        assert calls == ["first"]
        assert not first.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert await second == "second"
        assert calls == ["first", "second"]
    asyncio.run(scenario())


def test_waiter_cancellation_does_not_block_fifo():
    async def scenario():
        gate = SiteRequestGate(interval=0, poll_interval=.001)
        state, ticket = gate.enqueue({"id": 1})
        assert gate.claim(state, ticket, 0)
        async def transport(*args, **kwargs):
            return 7
        call = make_site_async(transport, gate, lambda: True)
        waiting = asyncio.create_task(call(None, {"id": 1}, "a"))
        await asyncio.sleep(.005)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        gate.release(state, ticket)
        assert await call(None, {"id": 1}, "b") == 7
    asyncio.run(scenario())


def test_manual_native_scheduler_fast_site_advances_without_round_barrier():
    async def scenario():
        registry = NS(create=lambda coroutine, **kwargs: asyncio.create_task(coroutine))
        scope = native_nodes("app/chain/search/pagination.py", {
            "_run_site_page", "_submit_site_page", "_cancel_pending_pages", "SearchPaginationOwner"},
            {"asyncio": asyncio, "time": time, "_SearchOwnerBase": object,
             "get_task_registry": lambda: registry, "runtime_stop_state": NS(is_system_stopped=False)})
        owner = scope["SearchPaginationOwner"]()
        owner.runtime_config = NS(search_threadpool_size=2)
        reads, slow_release = [], asyncio.Event()
        async def offload(function, key, page):
            if key == "slow1":
                await slow_release.wait()
            reads.append((key, page))
            return {"page": page, "can_continue": True, "error": None}, [key]
        async def original(*args):
            raise AssertionError("enabled movie must use adapted scheduler")
            yield
        manual = NS(plan=NS(mediainfo=NS(type="电影")), owner=owner, read=None,
                    sources=NS(queries={key: {"site": site} for key, site in
                                       [("fast1", "1"), ("slow1", "2"), ("fast2", "1")]}))
        call = make_read_pages(original, offload, lambda: True)
        events = call(manual, list(manual.sources.queries), 3)
        async with aclosing(events):
            assert (await anext(events))[0] == "fast1"
            assert (await asyncio.wait_for(anext(events), .5))[0] == "fast2"
            slow_release.set()
            result = await anext(events)
            assert result[0] == "slow1" and result[2] == ["slow1"]
            assert result[1]["pages"] == [{"page": 3, "can_continue": True, "error": None}]
        assert reads == [("fast1", 3), ("fast2", 3), ("slow1", 3)]
    asyncio.run(scenario())


@pytest.mark.parametrize("music,plugins,expected", [(False, False, 0), (False, True, 9), (True, False, 9)])
def test_execution_plan_identity_rules_and_wait_scope(music, plugins, expected):
    owner = object()
    plan = NS(mediainfo=NS(type="音乐" if music else "电影"), rule_groups=[],
              no_exists=object(), filter_params=object(), candidate_filter=object())
    delay = make_keyword_delay(lambda *_: 9, lambda: True)
    def original(actual_owner, actual_plan, media_chain):
        assert actual_owner is owner and actual_plan is plan
        assert delay(owner, 2) == expected
        assert delay(object(), 2) == 9
        return actual_plan
    assert make_execution_run(original, lambda: True, lambda: not plugins)(owner, plan, None) is plan
    assert SEARCH_SCOPE.get() is None


def test_event_scope_reset_before_yield_across_tasks():
    async def scenario():
        owner = object()
        async def native(owner, plan, media_chain, streaming=True):
            for i in range(2):
                assert SEARCH_SCOPE.get() is owner
                yield i
        generator = make_execution_events(native, lambda: True, lambda: True)(owner, NS(mediainfo=NS(type="电影")), None)
        assert await asyncio.create_task(anext(generator)) == 0
        assert SEARCH_SCOPE.get() is None
        assert await asyncio.create_task(anext(generator)) == 1
        await asyncio.create_task(generator.aclose())
        assert SEARCH_SCOPE.get() is None
    asyncio.run(scenario())


def test_zero_interval_keeps_one_request_in_flight_without_waiting():
    """默认 0 秒：只保证同站不并发，不在两次请求之间加任何时间。"""
    now = [0.0]
    gate = SiteRequestGate(interval=0.0, clock=lambda: now[0], poll_interval=0)
    state, first = gate.enqueue({"id": 1})
    assert gate.claim(state, first, 0.0)
    now[0] = 10.0                       # 上一个请求跑了 10 秒
    gate.release(state, first)
    _, second = gate.enqueue({"id": 1})
    assert gate.claim(state, second, 0.0)   # 结束即可发，不等
    gate.release(state, second)


def test_interval_counts_from_previous_request_end():
    now = [0.0]
    gate = SiteRequestGate(interval=3.0, clock=lambda: now[0], poll_interval=0)
    state, first = gate.enqueue({"id": 1})
    assert gate.claim(state, first, 3.0)      # starts at t=0
    now[0] = 10.0                              # the request itself took 10 seconds
    gate.release(state, first)
    _, second = gate.enqueue({"id": 1})
    now[0] = 12.9
    assert not gate.claim(state, second, 3.0)  # only 2.9s since it *finished*
    now[0] = 13.0
    assert gate.claim(state, second, 3.0)      # 3s after it finished
    gate.release(state, second)


def test_sync_wrapper_waits_after_a_slow_request_finishes():
    gate = SiteRequestGate(interval=.05, poll_interval=.001)
    stamps = []
    def transport(owner, site, keyword, mtype=None, page=0):
        stamps.append((keyword, time.monotonic()))
        if keyword == "slow":
            time.sleep(.1)
        return keyword
    call = make_site_sync(transport, gate, lambda: True)
    call(None, {"id": 1}, "slow")
    call(None, {"id": 1}, "fast")
    assert stamps[1][1] - stamps[0][1] >= .15  # 0.1s request + 0.05s interval


def test_native_minimum_survives_budget_reservation_delay_and_next_manual_call():
    now = [0.0]
    gate = SiteRequestGate(clock=lambda: now[0])
    state, first = gate.enqueue({"id": 1})
    now[0] = 8  # The native subscription budget was reserved earlier at t=0.
    assert gate.claim(state, first, 10)
    gate.release(state, first)
    _, second = gate.enqueue({"id": 1})
    now[0] = 13
    assert not gate.claim(state, second, 5)
    now[0] = 18
    assert gate.claim(state, second, 5)
    gate.release(state, second)


def test_music_bypasses_gate_and_errors_release_slot():
    gate = SiteRequestGate(interval=0)
    def transport(owner, site, keyword, mtype=None, page=0):
        if keyword == "broken":
            raise ValueError("network failure")
        return keyword
    call = make_site_sync(transport, gate, lambda: True)
    assert call(None, {"id": 1}, "song", mtype="音乐") == "song"
    assert not gate.states
    with pytest.raises(ValueError):
        call(None, {"id": 1}, "broken")
    assert call(None, {"id": 1}, "concert", mtype="电影") == "concert"
    stats = gate.snapshot()
    assert stats[0]["requests"] == 2 and stats[0]["errors"] == 1


class FakeTorrent:
    def __init__(self, name):
        self.site_name, self.title, self.description = "site", name, name


class PageOwner:
    """Minimal stand-in for the native per-page scheduler used by _read_pages."""
    async def _iter_site_page_results(self, *, indexer_sites, search_pages, search_page,
                                      should_continue, task_owner):
        for site in indexer_sites:
            index = 0
            while True:
                results = await search_page(site, search_pages[index])
                yield site, search_pages[index], results, False, 0.0
                if not should_continue(site, results) or index + 1 >= len(search_pages):
                    break
                index += 1


def paged_manual(pages, pageable=True):
    manual = NS(plan=NS(mediainfo=NS(type="电影")), owner=PageOwner(), torrents=[])
    manual.queries = {"1:alpha": {"site": "1", "keyword": "alpha"}}
    manual.sources = NS(queries=manual.queries, pageable=lambda key: pageable)
    manual.calls = []
    def read(key, page):
        manual.calls.append(page)
        torrents = [FakeTorrent(name) for name in pages.get(page, [])]
        return ({"source": "1:alpha:sid", "page": page, "error": None,
                 "can_continue": bool(torrents)}, torrents)
    manual.read = read
    return manual


async def collect(wrapper, manual, keys, page=0):
    out = []
    async with aclosing(wrapper(manual, keys, page)) as stream:
        async for key, facts, torrents in stream:
            out.append((key, facts, torrents))
    return out


def test_site_pages_are_walked_until_the_site_runs_out():
    async def offload(operation, *args):
        return operation(*args)
    manual = paged_manual({0: ["a", "b"], 1: ["c"], 2: []})
    wrapper = make_read_pages(lambda *_: None, offload, lambda: True, lambda: 10)
    result = asyncio.run(collect(wrapper, manual, ["1:alpha"]))
    key, facts, _ = result[-1]
    assert manual.calls == [0, 1, 2]
    assert [[item.title for item in torrents] for _, _, torrents in result] == [["a", "b"], ["c"], []]
    assert facts["page"] == 2  # preserve the terminal empty page
    assert [entry["page"] for entry in facts["pages"]] == [0, 1, 2]


def test_page_cap_and_duplicates_are_respected():
    async def offload(operation, *args):
        return operation(*args)
    manual = paged_manual({0: ["a"], 1: ["a", "b"], 2: ["c"]})
    wrapper = make_read_pages(lambda *_: None, offload, lambda: True, lambda: 2)
    result = asyncio.run(collect(wrapper, manual, ["1:alpha"]))
    assert manual.calls == [0, 1]
    # 插件不再自己按站点+标题+描述去重，重复项交给 MP 的规则链与 _remove_duplicate。
    assert [item.title for _, _, torrents in result for item in torrents] == ["a", "a", "b"]


def test_non_pageable_site_and_errors_stop_the_walk():
    async def offload(operation, *args):
        return operation(*args)
    manual = paged_manual({0: ["a"], 1: ["b"]}, pageable=False)
    wrapper = make_read_pages(lambda *_: None, offload, lambda: True, lambda: 10)
    asyncio.run(collect(wrapper, manual, ["1:alpha"]))
    assert manual.calls == [0]

    manual = paged_manual({0: ["a"]})
    def failing_read(key, page):
        manual.calls.append(page)
        if page == 0:
            return {"source": "s", "page": 0, "error": None, "can_continue": True}, [FakeTorrent("a")]
        return {"source": "s", "page": 1, "error": "site error", "can_continue": True}, []
    manual.read = failing_read
    wrapper = make_read_pages(lambda *_: None, offload, lambda: True, lambda: 10)
    asyncio.run(collect(wrapper, manual, ["1:alpha"]))
    assert manual.calls == [0, 1]


def test_failed_first_page_does_not_advance_or_hide_failure():
    async def offload(operation, *args):
        return operation(*args)
    manual = paged_manual({})
    def failing_read(key, page):
        manual.calls.append(page)
        return {"page": page, "error": "timeout", "can_continue": True}, []
    manual.read = failing_read
    wrapper = make_read_pages(lambda *_: None, offload, lambda: True, lambda: 10)
    result = asyncio.run(collect(wrapper, manual, ["1:alpha"]))
    assert manual.calls == [0]
    assert result[0][1]["error"] == "timeout" and result[0][1]["page"] == 0


def test_resource_pages_only_override_inside_a_media_search():
    wrapper = make_resource_pages(lambda: 3, lambda: True, lambda: 10)
    assert wrapper() == 3                      # outside a媒体搜索: MP's own value
    token = SEARCH_SCOPE.set(object())
    try:
        assert wrapper() == 10                 # inside影视搜索: our page cap
    finally:
        SEARCH_SCOPE.reset(token)
    assert wrapper() == 3
    assert make_resource_pages(lambda: 3, lambda: False, lambda: 10)() == 3


def test_disabled_streaming_aggregates_without_losing_pages():
    async def offload(operation, *args):
        return operation(*args)
    manual = paged_manual({0: ["a"], 1: ["b"], 2: []})
    wrapper = make_read_pages(lambda *_: None, offload, lambda: True, lambda: 10, lambda: False)
    result = asyncio.run(collect(wrapper, manual, ["1:alpha"]))
    assert len(result) == 1
    assert [t.title for t in result[0][2]] == ["a", "b"]
    assert result[0][1]["page"] == 2


def title_probe(seen, asynchronous=False):
    if asynchronous:
        async def original(self, keyword, mediainfo=None, sites=None, page=0, area="title", mtype=None):
            seen.append(SEARCH_SCOPE.get())
            return ["ok"]
    else:
        def original(self, keyword, mediainfo=None, sites=None, page=0, area="title", mtype=None):
            seen.append(SEARCH_SCOPE.get())
            return ["ok"]
    return original


@pytest.mark.parametrize("asynchronous", [False, True])
def test_title_search_publishes_and_clears_the_page_budget_scope(asynchronous):
    seen = []
    scoped = make_title_scope(title_probe(seen, asynchronous), lambda: True, asynchronous)
    owner, media = object(), NS(type=NS(value="电视剧"))
    result = asyncio.run(scoped(owner, "kw", mediainfo=media)) if asynchronous else scoped(owner, "kw", mediainfo=media)
    assert result == ["ok"]
    assert seen == [owner]
    assert SEARCH_SCOPE.get() is None


@pytest.mark.parametrize("asynchronous", [False, True])
def test_title_scope_stays_off_for_music_and_disabled(asynchronous):
    seen = []
    scoped = make_title_scope(title_probe(seen, asynchronous), lambda: True, asynchronous)
    music, disabled = NS(type=NS(value="音乐")), make_title_scope(
        title_probe(seen, asynchronous), lambda: False, asynchronous)
    calls = [
        (scoped, {"mtype": music.type}),
        (disabled, {"mediainfo": NS(type=NS(value="电视剧"))}),
    ]
    for function, kwargs in calls:
        if asynchronous:
            asyncio.run(function(object(), "kw", **kwargs))
        else:
            function(object(), "kw", **kwargs)
    assert seen == [None, None]


def test_title_scope_prefers_explicit_mtype_and_keeps_arguments():
    seen, passed = [], []
    def original(self, keyword, mediainfo=None, sites=None, page=0, area="title", mtype=None):
        seen.append(SEARCH_SCOPE.get())
        passed.append((keyword, mediainfo, sites, page, area, mtype))
        return "ok"
    scoped = make_title_scope(original, lambda: True)
    owner = object()
    assert scoped(owner, "kw", mediainfo=NS(type=NS(value="电视剧")), sites=[7], page=2,
                  area="imdbid", mtype=NS(value="电影")) == "ok"
    assert seen == [owner]
    assert passed == [("kw", NS(type=NS(value="电视剧")), [7], 2, "imdbid", NS(value="电影"))]


def test_title_scope_is_held_across_every_stream_pull():
    seen = []
    async def original(self, keyword, mediainfo=None, sites=None, page=0, area="title", mtype=None):
        seen.append(SEARCH_SCOPE.get())
        yield {"page": 1}
        seen.append(SEARCH_SCOPE.get())
        yield {"page": 2}
    scoped = make_title_scope(original, lambda: True, generator=True)
    owner = object()
    async def collect():
        return [item async for item in scoped(owner, "kw")]
    assert asyncio.run(collect()) == [{"page": 1}, {"page": 2}]
    assert seen == [owner, owner]
    assert SEARCH_SCOPE.get() is None
