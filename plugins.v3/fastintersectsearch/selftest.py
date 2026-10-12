"""In-process measurement so a comparison run needs nothing but the API token.

The MoviePilot chain cannot be driven from a bare script (its runtime context is
configured by the startup composition root), so the measurement lives inside the
plugin: it runs the Web search path in the running app and reports the numbers we
compare across versions.
"""
import time
from copy import deepcopy
from contextlib import aclosing


def check_rules():
    """No site requests: isolate each rule and compare Rust with native Python."""
    from app.adapters.system import rust
    from app.application.rules import BUILTIN_RULE_SET, RuleHelper
    from app.domain.context import TorrentInfo
    from app.domain.metainfo import _rust_parse_options
    from app.modules.filter import FilterModule
    from .rulecompat import compatible_rules

    rules = deepcopy(BUILTIN_RULE_SET)
    rules.update({rule.id: rule.model_dump() for rule in RuleHelper().get_custom_rules()})
    normalized = compatible_rules(rules)
    module = FilterModule()
    module.rule_set = rules
    rows = []
    for key in rules:
        groups = [{"name": "compatibility-check", "rule_string": key}]
        torrents = [TorrentInfo(title="Diagnostic.2026.1080p.WEB-DL-GTR", description="", size=1024**3)]
        row = {"rule": key, "rewritten": rules[key] != normalized[key]}
        for name, definitions in (("original", rules), ("compatible", normalized)):
            try:
                result = rust._moviepilot_rust.filter_torrents_fast(
                    groups, torrents, {key: definitions[key]}, None, _rust_parse_options())
                row[name] = "ok"
            except Exception as error:
                row[name] = f"{type(error).__name__}: {str(error)[:300]}"
        if row["rewritten"] and row["compatible"] == "ok":
            # Fixed corpus includes all ASCII separators, known release groups,
            # nonmatches, descriptions and labels; no credentials or URLs.
            titles = [f"Diagnostic.2026.1080p.WEB-DL{chr(c)}{team}{tail}"
                      for c in range(32, 127) for team in ("GTR", "PbK", "CHD", "ADE", "NTb", "NGB", "RO", "WiKi", "unknown")
                      for tail in ("", "A", ".")]
            corpus = [TorrentInfo(title=t, description="", size=1024**3) for t in titles]
            native = module._FilterModule__filter_torrents_with_python(groups, deepcopy(corpus), None)
            expected = [(titles.index(t.title), t.pri_order) for t in native]
            actual = rust._moviepilot_rust.filter_torrents_fast(
                groups, corpus, {key: normalized[key]}, None, _rust_parse_options())
            row["corpus"] = len(corpus)
            row["equivalent"] = list(map(tuple, actual)) == expected
        rows.append(row)
    return {"success": all(row["compatible"] == "ok" and row.get("equivalent", True) for row in rows), "data": rows}


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


async def run_search(media_id, mtype=None, media_source="themoviedb", site_id=None, observe=None):
    """Drive one Web page search and collect timing, counts and stage marks."""
    from app.chain.search.facade import SearchChain
    from app.adapters.system import rust

    chain = SearchChain()
    params = {"media_source": media_source, "media_id": str(media_id),
              "mtype": resolve_mtype(mtype), "page": 0}
    if site_id is not None:
        params["sites"] = [site_id]
    started = time.monotonic()
    marks, state = [], {"appends": 0, "candidates": 0, "results": 0, "sources": 0}
    complete, failed, source_facts = False, False, []
    first_matched = None
    recognition_cache = None
    async with aclosing(chain.search_page_events(params=params)) as events:
        async for event in events:
            kind = event.get("type")
            if observe is not None:
                observe({"type": kind, "stage": event.get("stage"),
                         "at": round(time.monotonic() - started, 2),
                         "candidates": event.get("candidate_items")})
            if "recognition_cache" in event:
                recognition_cache = event["recognition_cache"]
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
            "first_matched": first_matched, "source_facts": source_facts, "marks": marks,
            "recognition_cache": recognition_cache, "rust_runtime": rust.status(), **state}
