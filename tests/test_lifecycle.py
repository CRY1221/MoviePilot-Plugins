"""Exercise plugin lifecycle against isolated host boundaries, without MP services."""
import ast
import importlib
import math
import sys
from types import ModuleType, SimpleNamespace as NS

import pytest

from conftest import ROOT
from fastsearch import compat
from fastsearch.hooks import Hooks


def host(monkeypatch):
    def module(name):
        result = ModuleType(name)
        result.__path__ = []
        monkeypatch.setitem(sys.modules, name, result)
        if "." in name:
            parent, _, child = name.rpartition(".")
            if parent not in sys.modules:
                module(parent)
            monkeypatch.setattr(sys.modules[parent], child, result, raising=False)
        return result
    modules = {name: module("app." + name) for name in [
        "chain.search.facade", "chain.search.manual", "chain.search.execution", "chain.search.scan",
        "chain.search.title", "chain.search.result", "chain.subscribe.searchtask", "application.torrent.download",
        "application.subscription.sitebudget", "sdk.plugin"]}
    targets = {
        "SearchChain": type("SearchChain", (), {}), "manual": modules["chain.search.manual"],
        "execution": modules["chain.search.execution"], "SearchScan": type("SearchScan", (), {}),
        "SearchSources": type("SearchSources", (), {}), "SearchTitleOwner": type("SearchTitleOwner", (), {}),
        "SubscriptionSearchTaskRunner": type("SubscriptionSearchTaskRunner", (), {}),
    }
    targets["execution"].SearchExecutionOwner = type("SearchExecutionOwner", (), {})
    targets["execution.SearchExecutionOwner"] = targets["execution"].SearchExecutionOwner
    tree = ast.parse((ROOT / "plugins.v3/fastintersectsearch/compat.py").read_text(encoding="utf-8"))
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name) or call.func.id != "require":
            continue
        target = targets[ast.unparse(call.args[0])]
        name, args = call.args[1].value, call.args[2].value.split()
        kind = call.args[3].value if len(call.args) > 3 else "sync"
        code = ("async " if kind != "sync" else "") + f"def {name}({', '.join(args)}):\n    "
        code += "yield None" if kind == "generator" else "return None"
        scope = {"__name__": "app.isolated_host"}
        exec(code, scope)
        function = scope[name]
        if target is targets["execution.SearchExecutionOwner"]:
            function = staticmethod(function)
        setattr(target, name, function)
    modules["chain.search.facade"].SearchChain = targets["SearchChain"]
    modules["chain.search.scan"].SearchScan = targets["SearchScan"]
    modules["chain.search.scan"].SearchSources = targets["SearchSources"]
    modules["chain.search.scan"].AUTOMATIC_REQUEST_INTERVAL = 10
    modules["chain.search.title"].SearchTitleOwner = targets["SearchTitleOwner"]
    modules["chain.subscribe.searchtask"].SubscriptionSearchTaskRunner = targets["SubscriptionSearchTaskRunner"]
    modules["application.subscription.sitebudget"].SubscriptionSiteBudget = type("SubscriptionSiteBudget", (), {})
    modules["sdk.plugin"].PluginManager = lambda: NS(get_plugin_modules=lambda: {})
    modules["application.torrent.download"].TorrentHelper = NS(sort_torrents=lambda items: items)
    modules["chain.search.result"].SearchResultOwner = type(
        "SearchResultOwner", (), {"_remove_duplicate": staticmethod(lambda items: items)})
    modules["chain.search.manual"]._offload = None
    modules["chain.search.title"]._build_title_search_params = None
    modules["chain.search.title"]._build_title_contexts = None
    return targets


def test_install_and_restore_all_descriptors(monkeypatch):
    targets = host(monkeypatch)
    before = {key: dict(vars(target)) for key, target in targets.items()}
    hooks = compat.install(lambda: True, lambda: True, 5)
    assert len(hooks.patches) == 11
    hooks.close()
    for key, target in targets.items():
        assert vars(target) == before[key]


def test_install_rolls_back_owned_changes_on_mid_transaction_error(monkeypatch):
    targets = host(monkeypatch)
    before = {key: dict(vars(target)) for key, target in targets.items()}
    install = Hooks.install
    count = [0]
    def fail(self, *args):
        count[0] += 1
        if count[0] == 4:
            raise RuntimeError("injected install failure")
        install(self, *args)
    monkeypatch.setattr(Hooks, "install", fail)
    with pytest.raises(RuntimeError, match="injected"):
        compat.install(lambda: True, lambda: True, 5, lambda: True)
    for key, target in targets.items():
        assert vars(target) == before[key]


def plugin_class():
    tree = ast.parse((ROOT / "plugins.v3/fastintersectsearch/__init__.py").read_text(encoding="utf-8"))
    tree.body = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    scope = {"__name__": "fastsearch", "__package__": "fastsearch", "math": math,
             "_PluginBase": object, "logger": NS(error=lambda *_: None)}
    exec(compile(tree, "plugin_entry", "exec"), scope)
    return scope["FastIntersectSearch"]


def test_reload_then_old_stop_does_not_unhook_new_instance(monkeypatch):
    host(monkeypatch)
    Plugin = plugin_class()
    first, second = Plugin(), Plugin()
    first.init_plugin({"enabled": True, "site_sleep_seconds": 3})
    assert first.get_state()
    second.init_plugin({"enabled": True})
    assert not first.get_state() and second.get_state()
    first.stop_service()
    assert Plugin.get_instance() is second
    assert len(second._hooks.patches) == 11
    second.stop_service()
    assert Plugin.get_instance() is None


def test_incompatible_host_leaves_plugin_disabled(monkeypatch):
    targets = host(monkeypatch)
    targets["SearchScan"].run = lambda self, unknown, extra: None
    plugin = plugin_class()()
    plugin.init_plugin({"enabled": True})
    assert not plugin.get_state() and plugin._hooks is None
