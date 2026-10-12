import ast
import asyncio
from dataclasses import dataclass
from enum import Enum
import importlib
from types import ModuleType, SimpleNamespace as NS
import sys

from pydantic import BaseModel, ValidationError
import pytest

from conftest import ROOT, KERNEL, native_nodes
from fastsearch.compat import require
from fastsearch.presentation import make_title_stream


def test_all_installed_hook_signatures_match_pinned_native_source():
    definitions = {}
    for relative in ["app/chain/base.py", "app/chain/search/manual.py", "app/chain/search/execution.py",
                     "app/chain/search/scan.py", "app/chain/subscribe/searchtask.py", "app/chain/search/title.py",
                         "app/chain/search/facade.py", "app/chain/search/pagination.py",
                         "app/chain/search/plan.py", "app/chain/search/result.py",
                         "app/adapters/system/rust.py", "app/domain/metainfo.py",
                         "app/application/rules.py", "app/runtime/progress.py"]:
        tree = ast.parse((KERNEL / relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions.setdefault(node.name, []).append(node)
    tree = ast.parse((ROOT / "plugins.v3/fastintersectsearch/compat.py").read_text(encoding="utf-8"))
    checked = []
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id != "require":
            continue
        name, arguments = call.args[1].value, call.args[2].value.split()
        matching = [node for node in definitions[name] if [a.arg for a in
                    [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]] == arguments]
        assert matching, name
        expected = call.args[3].value if len(call.args) > 3 else "sync"
        node = matching[0]
        actual = "sync"
        if isinstance(node, ast.AsyncFunctionDef):
            actual = "generator" if any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(node)) else "async"
        assert actual == expected, name
        checked.append(name)
    assert len(checked) == 31


def test_compatibility_rejects_unknown_or_wrapped_hooks():
    def run(self, submit):
        return None
    run.__module__ = "app.chain.search.scan"
    target = NS(run=run)
    assert require(target, "run", "self submit") is run
    with pytest.raises(RuntimeError):
        require(target, "run", "self extra submit")
    run.__wrapped__ = lambda: None
    with pytest.raises(RuntimeError):
        require(target, "run", "self submit")


def test_plugin_implements_all_native_abstract_methods():
    tree = ast.parse((KERNEL / "app/sdk/plugin/base.py").read_text(encoding="utf-8"))
    needed = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
              and any(isinstance(deco, ast.Name) and deco.id == "abstractmethod" for deco in node.decorator_list)}
    plugin = ast.parse((ROOT / "plugins.v3/fastintersectsearch/__init__.py").read_text(encoding="utf-8"))
    actual = {node.name for node in ast.walk(plugin) if isinstance(node, ast.FunctionDef)}
    assert needed <= actual


def tools_module(monkeypatch):
    class MediaType(str, Enum):
        MOVIE = "电影"
        TV = "电视剧"
        MUSIC = "音乐"
    class MediaSource(str, Enum):
        TMDB = "themoviedb"
    class ToolTag(str, Enum):
        Read = "read"
        Resource = "resource"
    calls = []
    @dataclass
    class Context:
        value: int
        def to_dict(self):
            return {"value": self.value}
    class Chain:
        async def async_search_by_title_stream(self, **kwargs):
            calls.append(("title", kwargs))
            yield {"type": "done", "items": [{"value": 2}, {"value": 1}]}
        async def async_search_by_id_stream(self, **kwargs):
            calls.append(("id", kwargs))
            yield {"type": "done", "items": [{"value": 2}, {"value": 1}]}
    for name, attrs in {
        "app.agent.tools.base": {"MoviePilotTool": BaseModel, "ToolTag": ToolTag},
        "app.chain.search.facade": {"SearchChain": Chain},
        "app.schemas.types": {"MediaSource": MediaSource, "MediaType": MediaType},
    }.items():
        module = ModuleType(name)
        module.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(sys.modules["fastsearch"], "FastIntersectSearch",
                        NS(get_instance=lambda: NS(get_state=lambda: True)), raising=False)
    sys.modules.pop("fastsearch.tools", None)
    return importlib.import_module("fastsearch.tools"), calls


def test_tool_schema_passthrough_and_display_limit(monkeypatch):
    module, calls = tools_module(monkeypatch)
    schema = module.FastIntersectSearchTool().args_schema.model_json_schema()
    assert {"title", "media_source", "media_id", "rule_groups", "page"} <= schema["properties"].keys()
    tool = module.FastIntersectSearchTool()
    result = asyncio.run(tool.run(title="concert", page=3, rule_groups=[], site_ids=[7], top_k=1))
    assert result["success"] and result["total"] == 2 and result["shown"] == 1
    assert result["items"] == [{"value": 2}] and result["site_completeness"] == "unknown"
    assert calls == [("title", {"title": "concert", "page": 3, "rule_groups": [],
                                "mtype": None, "sites": [7], "cache_local": False})]
    asyncio.run(tool.run(media_source="themoviedb", media_id="1", season=2, include_candidates=True))
    assert calls[-1][0] == "id"
    assert calls[-1][1]["season"] == 2 and calls[-1][1]["include_candidates"] is True


@pytest.mark.parametrize("params", [
    {"title": "x", "require_complete": True}, {"title": "x", "en_title": "y"},
    {"media_source": "themoviedb", "media_id": "1", "page": 1},
    {"media_source": "themoviedb", "media_id": "1", "rule_groups": []},
    {"title": "x", "season": 2}, {"title": "x", "unsupported": True},
])
def test_unsupported_tool_inputs_fail_before_io(monkeypatch, params):
    module, calls = tools_module(monkeypatch)
    with pytest.raises(ValidationError):
        asyncio.run(module.FastIntersectSearchTool().run(**params))
    assert calls == []


def test_title_final_sort_keeps_context_and_cache_order_and_rejects_filtered_items():
    async def scenario():
        parameters, filtered_rules, saved, retained = [], [], [], []
        params = native_nodes("app/chain/search/title.py", {"_build_title_search_params"})["_build_title_search_params"]
        class Context:
            def __init__(self, value):
                self.value, self.allowed_episodes = value, {1, 2}
            def to_dict(self):
                return {"value": self.value}
        async def provider(**kwargs):
            parameters.append(kwargs)
            yield {"items": [1, -1], "site": "first"}
            yield {"items": [3], "site": "second"}
        def filtering(*, torrents, rule_groups):
            filtered_rules.append(rule_groups)
            return [value for value in torrents if value != -1]
        def contexts(items, mtype, build_meta):
            result = [Context(value) for value in items]
            retained.extend(result)
            return result
        def sorter(items):
            assert all(item in retained for item in items)
            assert all(item.allowed_episodes == {1, 2} for item in items)
            return sorted(items, key=lambda item: -item.value)
        async def save(items):
            saved.extend(items)
        async def save_params(**kwargs):
            pass
        async def offload(fn, *args, **kwargs):
            return fn(*args, **kwargs)
        owner = NS(_SearchChain__async_search_all_sites_stream=provider,
                   _filter_title_search_torrents=filtering, _build_title_search_meta=None,
                   _async_save_results=save, cancel_ai_recommend=lambda: None, async_save_last_search_params=save_params)
        async def original(*args, **kwargs):
            yield {"type": "music"}
        stream = make_title_stream(original, lambda: True, params, contexts, offload, sorter)
        events = [event async for event in stream(owner, "concert", page=2, cache_local=True, rule_groups=[])]
        assert filtered_rules == [[], []]
        assert events[-1]["items"] == [{"value": 3}, {"value": 1}]
        assert [item.to_dict() for item in saved] == events[-1]["items"]
        assert events[-1]["candidate_items"] == 3
        assert parameters[0]["page"] == 2
        assert [event async for event in stream(owner, "song", mtype="音乐")] == [{"type": "music"}]
    asyncio.run(scenario())


@pytest.mark.parametrize("events,status,success", [
    ([{"type": "error", "message": "private remote error"}], "failed", False),
    ([{"type": "append", "items": [{"value": 1}]}], "incomplete", False),
    ([{"type": "done", "items": []}], "empty", True),
])
def test_tool_errors_and_missing_final_event_are_not_empty_success(monkeypatch, events, status, success):
    module, _ = tools_module(monkeypatch)
    closed = []
    class Chain:
        async def async_search_by_title_stream(self, **kwargs):
            try:
                for event in events:
                    yield event
            finally:
                closed.append(True)
    monkeypatch.setattr(module, "SearchChain", Chain)
    result = asyncio.run(module.FastIntersectSearchTool().run(title="x"))
    assert result["status"] == status and result["success"] is success
    assert "private remote error" not in str(result)
    assert closed == [True]


def test_mcp_results_do_not_include_download_credentials(monkeypatch):
    module, _ = tools_module(monkeypatch)
    original = {"torrent_info": {"title": "film", "site_name": "site", "site_cookie": "secret",
                                "enclosure": "encoded-secret", "page_url": "signed-link"}}
    class Chain:
        async def async_search_by_title_stream(self, **kwargs):
            yield {"type": "done", "items": [original]}
    monkeypatch.setattr(module, "SearchChain", Chain)
    result = asyncio.run(module.FastIntersectSearchTool().run(title="film"))
    assert result["items"] == [{"torrent_info": {"title": "film", "site_name": "site"}}]
    assert original["torrent_info"]["site_cookie"] == "secret"
