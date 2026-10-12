"""In-process measurement so a comparison run needs nothing but the API token.

The MoviePilot chain cannot be driven from a bare script (its runtime context is
configured by the startup composition root), so the measurement lives inside the
plugin: it runs the Web search path in the running app and reports the numbers we
compare across versions.
"""
import time
from contextlib import aclosing


def resolve_mtype(value):
    """Accept Chinese labels, enum names or None for the media type."""
    from app.schemas.types import MediaType
    if value is None:
        return None
    if isinstance(value, MediaType):
        return value
    text = str(getattr(value, "value", value)).strip().lower()
    if text in {"movie", "电影"} or text.endswith(".movie"):
        return MediaType.MOVIE
    if text in {"tv", "电视剧", "剧集"} or text.endswith(".tv"):
        return MediaType.TV
    return None


async def run_search(media_id, mtype=None, media_source="themoviedb"):
    """Drive one Web page search and collect timing, counts and stage marks."""
    from app.chain.search.facade import SearchChain

    chain = SearchChain()
    params = {"media_source": media_source, "media_id": str(media_id),
              "mtype": resolve_mtype(mtype), "page": 0}
    started = time.monotonic()
    marks, state = [], {"appends": 0, "candidates": 0, "results": 0, "sources": 0}
    complete, failed, source_facts = False, False, []
    first_matched = None
    async with aclosing(chain.search_page_events(params=params)) as events:
        async for event in events:
            kind = event.get("type")
            if "candidate_items" in event:
                state["candidates"] = event["candidate_items"]
            if "sources" in event:
                source_facts = event["sources"]
                state["sources"] = len(source_facts)
            if kind == "error":
                failed = True
                break
            if kind == "append":
                state["appends"] += 1
                if event.get("stage") == "matched" and event.get("items") and first_matched is None:
                    first_matched = round(time.monotonic() - started, 2)
            elif kind == "progress":
                marks.append({"stage": event.get("stage"), "at": round(time.monotonic() - started, 2)})
            elif kind in {"replace", "done"}:
                state["results"] = event.get("total_items", state["results"])
                if kind == "replace" and event.get("items") and first_matched is None:
                    first_matched = round(time.monotonic() - started, 2)
                complete = complete or kind == "done"
    return {"media_id": str(media_id), "elapsed": round(time.monotonic() - started, 2),
            "status": "failed" if failed else "complete" if complete else "incomplete",
            "first_matched": first_matched, "source_facts": source_facts, "marks": marks, **state}
