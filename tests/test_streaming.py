"""Streaming parse must overlap fetching and still match native result ordering."""
import asyncio
import sys
import threading
import time
from contextlib import aclosing
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace as NS

import pytest

from fastsearch.streaming import ParsePool, make_page_events, make_dedup, parse_page, DEFER_DEDUP
from conftest import native_nodes


@dataclass
class Context:
    site: str
    title: str
    media_info_is_target: bool = True

    def to_dict(self):
        return {"site": self.site, "title": self.title}


def build(monkeypatch, delays, keywords=("alpha", "beta")):
    """Fake host: per-site page chains with controlled durations."""
    clock, parse_calls, fetch_done = [], [], []
    lock = threading.Lock()
    module = ModuleType("app.chain.search.manual")

    async def offload(operation, *args):
        return operation(*args)

    async def read_pages(manual, keys, page):
        for key in keys:
            await asyncio.sleep(delays.get(key, .01))
            with lock:
                fetch_done.append((key, time.monotonic()))
            torrents = [Context(key, f"{key}-{page}")]
            if "empty" in key:
                torrents = []
            yield key, {"source": key, "page": page, "error": None}, torrents

    module._offload, module._read_pages = offload, read_pages
    monkeypatch.setitem(sys.modules, "app.chain.search.manual", module)

    batches, saved = [], []
    def results(torrents):
        with lock:
            parse_calls.append((tuple(item.title for item in torrents), time.monotonic()))
        batches.append(tuple(item.title for item in torrents))
        return [item for item in torrents]

    class Manual:
        raw_title = False
        media = NS(type="电影", media_source="themoviedb", media_id="758891")
        plan = NS(area="title")
        torrents = []
        def __init__(self):
            self.queries = {f"1:{keywords[0]}": {"site": "1", "keyword": keywords[0]},
                            f"2:{keywords[0]}": {"site": "2", "keyword": keywords[0]},
                            f"3:{keywords[0]}": {"site": "3", "keyword": keywords[0]}}
            self.sources = NS(queries=self.queries, add_keywords=self.add_keywords)
            self.round = 0
        def add_keywords(self, all_names=False):
            self.round += 1
            if self.round > 1:
                return []
            self.queries = {f"{site}:{keyword}": {"site": site, "keyword": keyword}
                            for site in ("1", "2", "3") for keyword in keywords}
            self.sources.queries = self.queries
            return list(self.queries)
        def results(self, torrents):
            return results(torrents)
        def preview(self, torrents):
            return list(torrents)
        async def _async_save_results(self, contexts):
            saved.append(tuple(item.title for item in contexts))
        def save_last_search_params(self, **kwargs):
            pass
        owner = None

    manual = Manual()
    manual.owner = NS(runtime_config=NS(search_multiple_name=True),
                      _async_save_results=manual._async_save_results,
                      save_last_search_params=manual.save_last_search_params)

    async def original(manual, params):
        yield {"type": "native"}

    pool = ParsePool(workers=2, sorter=lambda items: sorted(items, key=lambda item: item.title),
                     dedup=lambda items: list({item.title: item for item in items}.values()))
    stream = make_page_events(original, lambda: True, pool)
    return NS(manual=manual, stream=stream, pool=pool, batches=batches, parse_calls=parse_calls,
              fetch_done=fetch_done, saved=saved, clock=clock)


def run(harness, params=None):
    async def scenario():
        events = []
        async with aclosing(harness.stream(harness.manual, params or {"page": 0})) as stream:
            async for event in stream:
                events.append(event)
        return events
    return asyncio.run(scenario())


def test_first_parse_runs_while_other_sites_still_fetch(monkeypatch):
    delays = {"1:alpha": .05, "2:alpha": .35, "3:alpha": .05,
              "1:beta": .05, "2:beta": .35, "3:beta": .05}
    harness = build(monkeypatch, delays)
    events = run(harness)
    try:
        last_fetch = max(moment for _, moment in harness.fetch_done)
        first_parse = min(moment for _, moment in harness.parse_calls)
        assert first_parse < last_fetch - .1, "解析必须与抓取重叠，而不是等到全部抓完"
        kinds = [event["type"] for event in events]
        assert kinds.count("append") == 6 and "replace" in kinds and kinds[-1] == "done"
        assert len([event for event in events if event["stage"] == "searching"]) == 6
        assert all(event["stage"] == "matched" for event in events if event["type"] == "append")
        ordered = sorted((f"{site}:{keyword}-0" for site in "123" for keyword in ("alpha", "beta")))
        final = sorted(item["title"] for item in events[-2]["items"])
        assert final == ordered
        assert events[-2]["sources"] and events[-1]["total_items"] == len(ordered)
    finally:
        harness.pool.close()


def test_streaming_matches_native_single_pass_set_and_order(monkeypatch):
    delays = {"1:alpha": .05, "2:alpha": .02, "3:alpha": .01,
              "1:beta": .01, "2:beta": .03, "3:beta": .01}
    harness = build(monkeypatch, delays)
    events = run(harness)
    try:
        parsed = [title for batch in harness.batches for title in batch]
        contexts = sorted((Context(title.split(":")[0], title) for title in parsed),
                          key=lambda item: item.title)
        expected = harness.pool.merge([[item] for item in contexts])
        assert [item["title"] for item in events[-2]["items"]] == [item.title for item in expected]
        # Every source page is parsed exactly once.
        assert sorted(parsed) == sorted(set(parsed))
    finally:
        harness.pool.close()


@pytest.mark.parametrize("params", [{"page": 0, "source": "1:alpha:abc"}, {"page": 0}])
def test_music_and_continuation_pages_stay_native(monkeypatch, params):
    harness = build(monkeypatch, {"1:alpha": .01})
    harness.manual.media = NS(type="音乐")
    assert [event["type"] for event in run(harness, params)] == ["native"]
    harness.pool.close()


def test_empty_source_pages_are_not_parsed(monkeypatch):
    delays = {"1:alpha": .01, "2:empty": .01, "3:alpha": .01,
              "1:beta": .01, "2:beta": .01, "3:beta": .01}
    harness = build(monkeypatch, delays, keywords=("alpha", "empty"))
    run(harness)
    try:
        assert all(batch for batch in harness.batches)
        assert harness.saved and len(harness.saved[0]) == len(harness.batches)
    finally:
        harness.pool.close()


def test_filtered_result_is_delivered_before_last_fetch(monkeypatch):
    harness = build(monkeypatch, {"1:alpha": .01, "2:alpha": .25}, keywords=("alpha",))
    harness.manual.preview = lambda *_: (_ for _ in ()).throw(AssertionError("raw preview used"))
    observed = []
    async def scenario():
        async with aclosing(harness.stream(harness.manual, {})) as events:
            async for event in events:
                if event["type"] == "append" and event["items"]:
                    observed.append((time.monotonic(), len(harness.fetch_done)))
    try:
        asyncio.run(scenario())
        assert observed[0][1] < 3
        assert observed[0][0] < max(moment for _, moment in harness.fetch_done)
    finally:
        harness.pool.close()


def test_parse_failure_does_not_publish_partial_success(monkeypatch):
    harness = build(monkeypatch, {}, keywords=("alpha",))
    def fail(torrents):
        raise RuntimeError("parser unavailable")
    harness.manual.results = fail
    try:
        with pytest.raises(RuntimeError, match="parser unavailable"):
            run(harness)
        assert harness.saved == []
    finally:
        harness.pool.close()


def test_same_source_pages_are_processed_early_and_merged_in_page_order(monkeypatch):
    harness = build(monkeypatch, {}, keywords=("alpha",))
    second_fetched = threading.Event()
    first_processed = threading.Event()
    async def pages(manual, keys, page):
        yield "1:alpha", {"page": 0, "pages": [{"page": 0}]}, [Context("1", "first"), Context("1", "dup")]
        await asyncio.sleep(.08)
        assert first_processed.is_set(), "第一页必须在第二页请求结束前开始处理"
        second_fetched.set()
        yield "1:alpha", {"page": 1, "pages": [{"page": 0}, {"page": 1}]}, [Context("1", "second"), Context("1", "dup")]
        yield "1:alpha", {"page": 2, "pages": [{"page": 0}, {"page": 1}, {"page": 2}]}, []
    monkeypatch.setattr(sys.modules["app.chain.search.manual"], "_read_pages", pages)
    def results(torrents):
        if torrents[0].title == "first":
            first_processed.set()
            assert second_fetched.wait(1)
            time.sleep(.03)  # The second page finishes parsing first.
        return torrents
    harness.manual.results = results
    harness.pool.sorter = lambda contexts: contexts
    harness.pool.dedup = lambda contexts: list({c.title: c for c in contexts}.values())
    try:
        events = run(harness)
        assert [item["title"] for item in events[-2]["items"]] == ["first", "dup", "second"]
        assert events[-2]["candidate_items"] == 4
        assert events[-2]["sources"][0]["page"] == 2
        assert len(events[-2]["sources"][0]["pages"]) == 3
        assert len([e for e in events if e["type"] == "append"]) == 2
        assert [t.title for t in harness.manual.torrents] == ["first", "dup", "second", "dup"]
    finally:
        harness.pool.close()


def test_defer_page_dedup_preserves_native_global_order_and_last_representative():
    host = native_nodes("app/chain/search/result.py", {"SearchResultOwner"}, {"_SearchOwnerBase": object})["SearchResultOwner"]
    native_dedup = host._remove_duplicate
    host._remove_duplicate = staticmethod(make_dedup(native_dedup))
    def context(title, seeders):
        return NS(torrent_info=NS(site_name="site", title=title, description="", seeders=seeders),
                  media_info_is_target=True)
    first, other, last = context("duplicate", 100), context("other", 50), context("duplicate", 1)
    sorter = lambda contexts: sorted(contexts, key=lambda c: c.torrent_info.seeders, reverse=True)
    manual = NS(results=lambda items: host._remove_duplicate(sorter(items)))
    native = manual.results([first, last, other])
    assert native == [last, other]
    # Dedup on each page would incorrectly put the unique resource first.
    assert native_dedup(sorter(manual.results([first, last]) + manual.results([other]))) == [other, last]
    pool = ParsePool(sorter=sorter, dedup=native_dedup)
    try:
        incremental = pool.merge([parse_page(manual, [first, last]), parse_page(manual, [other])])
        assert incremental == native
        assert not DEFER_DEDUP.get()
        assert host._remove_duplicate([first, last]) == [last]
    finally:
        pool.close()
