"""Overlap MoviePilot's own result parsing with site fetching.

The native manual search collects every source page first and only then runs the
filter/match/sort pipeline, so the user waits for fetching plus parsing. This
adapter keeps the native business pipeline untouched (same ``manual.results``,
same rules, same parameters) and simply runs it for each finished source while
the remaining sites are still being fetched. Only the final merge order is
reproduced locally with MoviePilot's own sorter/dedup functions.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import aclosing
from contextvars import copy_context
from functools import wraps

from .scheduling import is_music


class ParsePool:
    """Dedicated executor so parsing never competes with site fetching."""

    def __init__(self, workers=4, sorter=None, dedup=None):
        self.executor = ThreadPoolExecutor(max_workers=max(1, workers),
                                           thread_name_prefix="fastsearch-parse")
        self.sorter = sorter
        self.dedup = dedup

    async def run(self, operation, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, copy_context().run, operation, *args)

    def merge(self, batches):
        """Reproduce the tail of SearchResultOwner._parse_result for pre-parsed batches."""
        merged = [context for batch in batches for context in batch]
        merged = self.sorter(merged)
        merged.sort(key=lambda context: not context.media_info_is_target)
        return self.dedup(merged)

    def close(self):
        self.executor.shutdown(wait=False, cancel_futures=True)


def make_page_events(original, enabled, pool):
    """Stream filtered results: parse every source page while the rest still loads."""

    @wraps(original)
    async def page_events(manual, params):
        if (not enabled() or is_music(getattr(manual.media, "type", None))
                or params.get("source")):
            # Continuation pages and music keep the untouched native generator.
            async with aclosing(original(manual, params)) as events:
                async for event in events:
                    yield event
            return

        from app.chain.search.manual import _offload, _read_pages

        page = max(0, int(params.get("page") or 0))
        keys = manual.sources.add_keywords(
            all_names=bool(manual.owner.runtime_config.search_multiple_name))
        facts_by_key, torrents_by_key = {}, {}
        pending, preview_count, contexts = {}, 0, []
        try:
            while keys:
                pages = _read_pages(manual, keys, page)
                async with aclosing(pages):
                    async for key, facts, torrents in pages:
                        facts_by_key[key], torrents_by_key[key] = facts, torrents
                        manual.torrents.extend(torrents)
                        if torrents:
                            pending[key] = asyncio.ensure_future(pool.run(manual.results, torrents))
                        previews = await _offload(manual.preview, torrents)
                        preview_count += len(previews)
                        yield {"type": "append", "stage": "searching",
                               "value": len(facts_by_key) / len(manual.sources.queries) * 90,
                               "text": f"已完成 {len(facts_by_key)} / {len(manual.sources.queries)} 个搜索来源",
                               "items": [context.to_dict() for context in previews],
                               "total_items": preview_count, "candidate_items": len(manual.torrents)}
                # Parsing already ran while the pages above were arriving; only the
                # remaining batches and the merge are left on the critical path.
                manual.torrents = [torrent for key in manual.sources.queries
                                   for torrent in torrents_by_key.get(key, [])]
                yield {"type": "progress", "stage": "filtering", "value": 98,
                       "text": f"正在过滤匹配 {len(manual.torrents)} 个候选资源 ..."}
                batches = await asyncio.gather(*pending.values(), return_exceptions=True)
                pending.clear()
                contexts = await pool.run(
                    pool.merge, [batch for batch in batches if isinstance(batch, list)])
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
