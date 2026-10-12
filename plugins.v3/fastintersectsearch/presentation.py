"""Legacy title SSE adapter, retaining native Context objects until final sort."""
from contextlib import aclosing
from functools import wraps

from .scheduling import is_music


def make_title_stream(original, enabled, build_params, build_contexts, offload, sorter, default_rules=lambda: None):
    @wraps(original)
    async def stream(owner, title, page=0, sites=None, cache_local=False, mtype=None, rule_groups=None):
        if not enabled() or is_music(mtype):
            async with aclosing(original(owner, title, page=page, sites=sites, cache_local=cache_local,
                                         mtype=mtype, rule_groups=rule_groups)) as events:
                async for event in events:
                    yield event
            return
        search_params, cache_params = build_params(title=title, page=page, sites=sites,
                                                   mtype=mtype, include_empty_type=True)
        if cache_local:
            owner.cancel_ai_recommend()
            await owner.async_save_last_search_params(**cache_params)
        if rule_groups is None:
            rule_groups = default_rules()
        contexts, count = [], 0
        events = owner._SearchChain__async_search_all_sites_stream(**search_params)
        async with aclosing(events):
            async for event in events:
                event = dict(event)
                torrents = event.pop("items", []) or []
                count += len(torrents)
                filtered = await offload(owner._filter_title_search_torrents,
                                         torrents=torrents, rule_groups=rule_groups)
                batch = build_contexts(filtered, mtype, owner._build_title_search_meta)
                contexts.extend(batch)
                yield {**event, "type": "append", "items": [context.to_dict() for context in batch],
                       "total_items": len(contexts)}
        contexts = await offload(sorter, contexts)
        if cache_local:
            await owner._async_save_results(contexts)
        yield {"type": "done", "text": f"搜索完成，共 {len(contexts)} 个资源",
               "items": [context.to_dict() for context in contexts],
               "total_items": len(contexts), "candidate_items": count}
    return stream
