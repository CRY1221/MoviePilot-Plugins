import asyncio
from concurrent.futures import ThreadPoolExecutor
import sys
import threading
import time
from types import ModuleType, SimpleNamespace as NS

import pytest

from fastsearch.recognition import ACTIVE, SearchCache, SearchReuse, make_meta, make_same_work, make_scope
from conftest import native_nodes


def test_parallel_single_flight_and_mutation_isolation():
    cache, calls = SearchCache(), []
    ready = threading.Barrier(8)
    def operation():
        calls.append(1)
        time.sleep(.03)
        return NS(episodes=[1, 2])
    def work():
        ready.wait()
        return cache.reuse("a", operation)
    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(lambda _: work(), range(8)))
    assert len(calls) == 1
    values[0].episodes.append(9)
    assert values[1].episodes == [1, 2]
    assert cache.reuse("a", operation).episodes == [1, 2]


def test_different_keys_are_not_serialized():
    cache = SearchCache()
    barrier = threading.Barrier(2)
    def compute():
        barrier.wait(timeout=2)
        return 1
    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(cache.reuse, "a", compute)
        b = pool.submit(cache.reuse, "b", compute)
        assert a.result() == b.result() == 1


def test_failure_retry_and_bounded_cache():
    cache = SearchCache(limit=1)
    def failed():
        raise ValueError("failed")
    with pytest.raises(ValueError):
        cache.reuse("a", failed)
    assert cache.reuse("a", lambda: 1) == 1
    assert cache.reuse("b", lambda: 2) == 2
    assert cache.reuse("b", lambda: 3) == 3
    assert len(cache.entries) == 1


def test_meta_inputs_config_and_music_fallback():
    calls, configuration = [], ["v1"]
    def native(torrent, custom_words, mediainfo):
        calls.append(torrent)
        return NS(title=torrent.title, words=list(custom_words or []))
    wrapped = make_meta(native, lambda: True, lambda: configuration[0])
    token = ACTIVE.set(SearchReuse())
    try:
        movie = NS(type="movie")
        a = NS(title="Alpha", description="S01")
        b = NS(title="Alpha", description="S01")
        wrapped(a, [], movie).words.append("mutated")
        assert wrapped(b, [], movie).words == []
        assert len(calls) == 1  # Both candidates survive; only calculation is reused.
        wrapped(NS(title="Alpha", description="S02"), [], movie)
        wrapped(a, ["rewrite"], movie)
        wrapped(a, [], NS(type="tv"))
        configuration[0] = "v2"
        wrapped(a, [], movie)
        assert len(calls) == 5
        wrapped(a, [], NS(type="music"))
        wrapped(a, [], NS(type="music"))
        assert len(calls) == 7
    finally:
        ACTIVE.reset(token)
    wrapped(a, [], movie)
    assert len(calls) == 8


def test_same_work_reuses_identity_not_decision(monkeypatch):
    module, calls = ModuleType("app.chain.media"), []
    class MediaChain:
        def recognize_by_meta(self, meta, obtain_images):
            calls.append(meta)
            return NS(identity="123")
    module.MediaChain = MediaChain
    monkeypatch.setitem(sys.modules, "app.chain.media", module)
    def native(torrent, torrent_meta, mediainfo, cache):
        key = torrent_meta.title
        if key not in cache:
            cache[key] = MediaChain().recognize_by_meta(torrent_meta, False)
        return cache[key].identity == mediainfo.identity
    wrapped = make_same_work(native, lambda: True, lambda meta: meta.title)
    token = ACTIVE.set(SearchReuse())
    try:
        assert wrapped(None, NS(title="Alpha"), NS(identity="123"), {})
        assert not wrapped(None, NS(title="Alpha"), NS(identity="456"), {})
        assert len(calls) == 1
    finally:
        ACTIVE.reset(token)


def test_scope_isolated_and_generator_does_not_leak(monkeypatch):
    reports = []
    monkeypatch.setattr(SearchReuse, "report", lambda scope: reports.append(scope))
    def native(owner, plan, media_chain):
        return ACTIVE.get()
    wrapped = make_scope(native, lambda: True, lambda kind: kind == "music")
    args = dict(owner=NS(), plan=NS(mediainfo=NS(type="movie")), media_chain=None)
    assert wrapped(**args) is not wrapped(**args)
    assert ACTIVE.get() is None
    assert wrapped(NS(), NS(mediainfo=NS(type="music")), None) is None
    async def native_events(manual, params):
        yield ACTIVE.get()
        yield ACTIVE.get()
    events = make_scope(native_events, lambda: True, lambda _: False, generator=True)
    async def run():
        iterator = events(NS(media=NS(type="movie")), {})
        scope = await anext(iterator)
        assert ACTIVE.get() is None
        assert await anext(iterator) is scope
        await iterator.aclose()
        assert ACTIVE.get() is None
    asyncio.run(run())
    assert len(reports) == 3


def test_unchanged_native_operations_produce_equal_results(monkeypatch):
    """Exercise actual pinned MP functions, including rejection/None paths."""
    calls = []
    def parser(title, subtitle, custom_words, mtype):
        calls.append(title)
        return NS(org_string=title, cn_name="", en_name=title, year="2025",
                  type=mtype, subtitle=subtitle, apply_words=list(custom_words))
    class MediaChain:
        def recognize_by_meta(self, meta, obtain_images):
            return NS(identity=meta.en_name) if meta.en_name != "Missing" else None
    helper = NS(match_same_work_evidence=lambda target_mediainfo, candidate_mediainfo, torrent_meta:
                (target_mediainfo.identity == candidate_mediainfo.identity, "identity"))
    scope = native_nodes("app/chain/search/result.py",
                         {"_torrent_meta", "_disambiguation_key", "_same_work_matched"},
                         {"MetaInfo": parser, "MediaChain": MediaChain,
                          "TorrentHelper": helper, "logger": NS(info=lambda _: None)})
    module = ModuleType("app.chain.media")
    module.MediaChain = MediaChain
    monkeypatch.setitem(sys.modules, "app.chain.media", module)
    meta = make_meta(scope["_torrent_meta"], lambda: True, lambda: "config")
    same = make_same_work(scope["_same_work_matched"], lambda: True, scope["_disambiguation_key"])
    token = ACTIVE.set(SearchReuse())
    try:
        native_outputs, cached_outputs = [], []
        target = NS(type="movie", identity="Alpha")
        for title in ("Alpha", "Other", "Missing", "Alpha"):
            torrent = NS(title=title, description="1080p", site_name="Site")
            baseline = scope["_torrent_meta"](torrent, [], target)
            reused = meta(torrent, [], target)
            assert vars(reused) == vars(baseline)
            native_outputs.append(scope["_same_work_matched"](torrent, baseline, target, {}))
            cached_outputs.append(same(torrent, reused, target, {}))
        assert native_outputs == cached_outputs == [True, False, False, True]
        assert ACTIVE.get().parse.hits == 1
        assert ACTIVE.get().identity.hits == 1
    finally:
        ACTIVE.reset(token)


def test_generator_exception_and_cancel_release_scope(monkeypatch):
    monkeypatch.setattr(SearchReuse, "report", lambda scope: None)
    async def native(manual, params):
        yield {"type": "progress"}
        if params.get("fail"):
            raise ValueError("failed")
        await asyncio.sleep(10)
    wrapped = make_scope(native, lambda: True, lambda _: False, generator=True)
    async def run():
        iterator = wrapped(NS(media=NS(type="movie")), {"fail": True})
        await anext(iterator)
        with pytest.raises(ValueError):
            await anext(iterator)
        assert ACTIVE.get() is None
        iterator = wrapped(NS(media=NS(type="movie")), {})
        await anext(iterator)
        pending = asyncio.create_task(anext(iterator))
        await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert ACTIVE.get() is None
    asyncio.run(run())
