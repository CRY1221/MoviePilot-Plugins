"""Self-test helper: media type parsing and the shape of a measurement run."""
import enum
import sys
import types as pytypes
import asyncio

from fastsearch import selftest


class _MediaType(str, enum.Enum):
    MOVIE = "电影"
    TV = "电视剧"


def patch_types(monkeypatch):
    module = pytypes.ModuleType("app.schemas.types")
    module.MediaType = _MediaType
    monkeypatch.setitem(sys.modules, "app.schemas.types", module)


def test_resolve_mtype_accepts_labels_enum_and_none(monkeypatch):
    patch_types(monkeypatch)
    assert selftest.resolve_mtype("电影") == _MediaType.MOVIE
    assert selftest.resolve_mtype("movie") == _MediaType.MOVIE
    assert selftest.resolve_mtype("TV") == _MediaType.TV
    assert selftest.resolve_mtype(_MediaType.MOVIE) == _MediaType.MOVIE
    assert selftest.resolve_mtype(None) is None
    assert selftest.resolve_mtype("音乐") is None


def test_run_records_final_sources_and_first_filtered_time(monkeypatch):
    patch_types(monkeypatch)
    class Chain:
        async def search_page_events(self, **kwargs):
            yield {"type": "append", "stage": "searching", "items": [1], "candidate_items": 8}
            yield {"type": "append", "stage": "matched", "items": [1]}
            yield {"type": "replace", "items": [1], "total_items": 1,
                   "sources": [{"page": 2, "pages": [{"page": 0}, {"page": 1}, {"page": 2}]}]}
            yield {"type": "done", "total_items": 1,
                   "sources": [{"page": 2, "pages": [{"page": 0}, {"page": 1}, {"page": 2}]}]}
    module = pytypes.ModuleType("app.chain.search.facade")
    module.SearchChain = Chain
    monkeypatch.setitem(sys.modules, module.__name__, module)
    result = asyncio.run(selftest.run_search("27205"))
    assert result["status"] == "complete" and result["sources"] == 1
    assert result["first_matched"] is not None and result["candidates"] == 8
    assert len(result["source_facts"][0]["pages"]) == 3


def test_run_error_is_failure_and_closes_stream(monkeypatch):
    patch_types(monkeypatch)
    closed = []
    class Chain:
        async def search_page_events(self, **kwargs):
            try:
                yield {"type": "error", "message": "private failure"}
            finally:
                closed.append(True)
    module = pytypes.ModuleType("app.chain.search.facade")
    module.SearchChain = Chain
    monkeypatch.setitem(sys.modules, module.__name__, module)
    result = asyncio.run(selftest.run_search("27205"))
    assert result["status"] == "failed" and closed == [True]
    assert "private failure" not in str(result)
