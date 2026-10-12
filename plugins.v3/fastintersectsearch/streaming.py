"""Overlap MoviePilot's own result parsing with site fetching.

The native manual search collects every source page first and only then runs the
filter/match/sort pipeline, so the user waits for fetching plus parsing. This
adapter keeps the native business pipeline untouched (same ``manual.results``,
same rules, same parameters) and simply runs it for each finished source while
the remaining sites are still being fetched. Only the final merge order is
reproduced locally with MoviePilot's own sorter/dedup functions.
"""
import asyncio
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from contextlib import aclosing
from contextvars import copy_context
from contextvars import ContextVar
from functools import wraps

from .scheduling import is_music, AGGREGATE_PAGES
from .rulecompat import VALIDATION

DEFER_DEDUP = ContextVar("fastsearch_defer_page_dedup", default=False)
EXEC_PLAN = ContextVar("fastsearch_execution_plan", default=None)
LAST_COMPARISON = None


def make_dedup(original):
    @wraps(original)
    def dedup(_torrents):
        # Keep every successfully matched Context until the native final sort.
        # Dedup before that sort loses an earlier duplicate's sort position.
        return _torrents if DEFER_DEDUP.get() else original(_torrents)
    return dedup


def parse_page(manual, torrents):
    token = DEFER_DEDUP.set(True)
    try:
        return manual.results(torrents)
    finally:
        DEFER_DEDUP.reset(token)


def compare_contexts(incremental, native):
    """Return field paths only; never return credentials or download URLs."""
    left, right = [c.to_dict() for c in incremental], [c.to_dict() for c in native]
    def identity(item):
        torrent = item.get("torrent_info", {})
        return tuple(torrent.get(k) for k in ("site_name", "title", "description"))
    differences = []
    def walk(a, b, path):
        if a == b or len(differences) >= 30:
            return
        if isinstance(a, dict) and isinstance(b, dict):
            for key in sorted(a.keys() | b.keys()):
                walk(a.get(key), b.get(key), path + "." + key)
        elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
            for index, (x, y) in enumerate(zip(a, b)):
                walk(x, y, f"{path}[{index}]")
        else:
            differences.append(path)
    walk(left, right, "contexts")
    return {"equivalent": left == right, "same_order": list(map(identity, left)) == list(map(identity, right)),
            "same_resources": set(map(identity, left)) == set(map(identity, right)),
            "different_fields": differences}


class ParsePool:
    """Dedicated executor so parsing never competes with site fetching."""

    def __init__(self, workers=4, sorter=None, dedup=None):
        self.executor = ThreadPoolExecutor(max_workers=max(1, workers),
                                           thread_name_prefix="fastsearch-parse")
        self.sorter = sorter
        self.dedup = dedup

    async def run(self, operation, *args):
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self.executor, copy_context().run, operation, *args)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # A cancelled await cannot stop the Python worker. Drain it before
            # the search closes or a reload restores the native hooks.
            while not future.done():
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not future.cancelled():
                future.exception()
            raise

    def merge(self, batches):
        """Reproduce the tail of SearchResultOwner._parse_result for pre-parsed batches."""
        merged = [context for batch in batches for context in batch]
        merged = self.sorter(merged)
        merged.sort(key=lambda context: not context.media_info_is_target)
        return self.dedup(merged)

    def close(self):
        self.executor.shutdown(wait=False, cancel_futures=True)


def warm_meta(torrents, custom_words, mediainfo):
    """Parse one finished site batch so the native parse finds it cached.

    The custom words must be the very ones the native call will use; a key that
    differs by even one field makes this pure duplicated work, so the caller
    passes the plan's own value rather than a guess.
    """
    from app.chain.search import result
    for torrent in torrents:
        result._torrent_meta(torrent, custom_words, mediainfo)


def make_plan_scope(original, enabled):
    """Publish the running plan so per-batch parsing can reuse its own settings."""
    @wraps(original)
    async def events(owner, plan, media_chain, streaming=True):
        token = EXEC_PLAN.set(plan if enabled() else None)
        try:
            async for event in original(owner, plan, media_chain, streaming=streaming):
                yield event
        finally:
            EXEC_PLAN.reset(token)
    return events


def make_provider_warm(original, enabled, pool):
    """Parse each site batch while the remaining sites are still being fetched.

    The native resolution loop hands one batch to ``_parse_result`` only after
    every site has answered, so parsing otherwise sits on the critical path. The
    batches themselves arrive incrementally, so each one can be parsed during the
    waits of the ones after it. Only the existing parse cache is populated here;
    the search result still comes from the untouched native call.
    """
    @wraps(original)
    async def provider_events(owner, step, streaming):
        params = getattr(step, "params", None)
        plan = EXEC_PLAN.get()
        mediainfo = params.get("mediainfo") if isinstance(params, dict) else None
        if (not enabled() or plan is None or mediainfo is None
                or is_music(getattr(mediainfo, "type", None))):
            async for event in original(owner, step, streaming):
                yield event
            return
        custom_words = plan.custom_words
        pending = []
        try:
            async for event in original(owner, step, streaming):
                items = event.get("items") if isinstance(event, dict) else None
                if items:
                    pending.append(asyncio.create_task(
                        pool.run(warm_meta, list(items), custom_words, mediainfo)))
                yield event
        finally:
            # Only the final batch cannot overlap anything; earlier ones already
            # ran while the sites after them were still answering.
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
    return provider_events


def make_page_events(original, enabled, pool):
    """Stream filtered results: parse every source page while the rest still loads."""

    @wraps(original)
    async def page_events(manual, params):
        if (not enabled() or is_music(getattr(manual.media, "type", None))
                or params.get("source")):
            # Continuation pages and music keep the untouched native generator.
            async with aclosing(original(manual, params)) as events:
                while True:
                    token = AGGREGATE_PAGES.set(True)
                    try:
                        event = await anext(events)
                    except StopAsyncIteration:
                        break
                    finally:
                        AGGREGATE_PAGES.reset(token)
                    yield event
            return

        from app.chain.search.manual import _read_pages

        page = max(0, int(params.get("page") or 0))
        keys = manual.sources.add_keywords(
            all_names=bool(manual.owner.runtime_config.search_multiple_name))
        facts_by_key, torrents_by_key = {}, {}
        pending, preview_count, contexts = {}, 0, []
        batches_by_key = {}
        try:
            while keys:
                pages = _read_pages(manual, keys, page)
                async with aclosing(pages):
                    next_page = asyncio.create_task(anext(pages))
                    try:
                        while next_page is not None or pending:
                            active = list(pending.values())
                            if next_page is not None:
                                active.append(next_page)
                            finished, _ = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
                            if next_page in finished:
                                try:
                                    key, facts, torrents = next_page.result()
                                except StopAsyncIteration:
                                    next_page = None
                                else:
                                    facts_by_key[key] = facts
                                    torrents_by_key.setdefault(key, []).extend(torrents)
                                    manual.torrents.extend(torrents)
                                    if torrents:
                                        batch_id = (key, facts.get("page", page))
                                        pending[batch_id] = asyncio.create_task(pool.run(parse_page, manual, torrents))
                                    next_page = asyncio.create_task(anext(pages))
                                    yield {"type": "progress", "stage": "searching",
                                           "value": len(facts_by_key) / len(manual.sources.queries) * 90,
                                           "text": f"已抓取 {len(facts_by_key)} / {len(manual.sources.queries)} 个搜索来源",
                                           "candidate_items": len(manual.torrents),
                                           "sources": list(facts_by_key.values())}
                            for batch_id, task in list(pending.items()):
                                if task not in finished:
                                    continue
                                # Propagate parsing failures instead of silently losing resources.
                                batch = task.result()
                                if not isinstance(batch, list):
                                    raise TypeError("MP 搜索结果解析未返回列表")
                                del pending[batch_id]
                                key, batch_page = batch_id
                                batches_by_key.setdefault(key, {})[batch_page] = batch
                                preview_count += len(batch)
                                yield {"type": "append", "stage": "matched",
                                       "items": [context.to_dict() for context in batch],
                                       "total_items": preview_count,
                                       "candidate_items": len(manual.torrents)}
                    finally:
                        if next_page is not None:
                            next_page.cancel()
                            await asyncio.gather(next_page, return_exceptions=True)
                # Parsing already ran while the pages above were arriving; only the
                # remaining batches and the merge are left on the critical path.
                manual.torrents = [torrent for key in manual.sources.queries
                                   for torrent in torrents_by_key.get(key, [])]
                yield {"type": "progress", "stage": "filtering", "value": 98,
                       "text": f"正在过滤匹配 {len(manual.torrents)} 个候选资源 ..."}
                contexts = await pool.run(
                    pool.merge, [batch for key in manual.sources.queries
                                 for _, batch in sorted(batches_by_key.get(key, {}).items())])
                if VALIDATION.get() is not None:
                    global LAST_COMPARISON
                    checks = VALIDATION.get()
                    token = VALIDATION.set(None)
                    try:
                        # Each candidate was already compared against native
                        # Python above. Compare the native whole-result pass
                        # without doing that expensive comparison twice.
                        baseline = await pool.run(manual.results, deepcopy(manual.torrents))
                    finally:
                        VALIDATION.reset(token)
                    LAST_COMPARISON = (contexts, baseline)
                    checks.append({"kind": "final_merge", "candidates": len(manual.torrents),
                                             **compare_contexts(contexts, baseline),
                                             "incremental_results": len(contexts), "native_results": len(baseline)})
                if contexts:
                    break
                keys = manual.sources.add_keywords()
        finally:
            if pending:
                await asyncio.gather(*pending.values(), return_exceptions=True)
                pending.clear()

        if params.get("source") is None:
            # 首页沿用原搜索的结果缓存与上次搜索参数；续页不覆盖它们。
            await manual.owner._async_save_results(contexts)
            manual.owner.save_last_search_params(
                keyword=params.get("keyword") if manual.raw_title else None,
                media_source=None if manual.raw_title else manual.media.media_source,
                media_id=None if manual.raw_title else manual.media.media_id,
                mtype=manual.media.type, area=manual.plan.area,
                season=None if manual.raw_title else params.get("season"),
                sites=params.get("sites"), result_type="torrent")
        items = [context.to_dict() for context in contexts]
        source_facts = [facts_by_key[key] for key in manual.sources.queries if key in facts_by_key]
        yield {"type": "replace", "stage": "filtered", "value": 100, "items": items,
               "total_items": len(items), "candidate_items": len(manual.torrents),
               "sources": source_facts}
        yield {"type": "done", "stage": "done", "total_items": len(items),
               "sources": source_facts}

    return page_events
