"""Exercise plugin lifecycle against isolated host boundaries, without MP services."""
import ast
import importlib
import math
import sys
from types import ModuleType, SimpleNamespace as NS

import pytest

from conftest import ROOT, KERNEL, native_nodes
from fastsearch import compat
from fastsearch.hooks import Hooks
from fastsearch.streaming import ParsePool, parse_page


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
        "chain.search.title", "chain.search.result", "chain.search.pagination",
        "chain.subscribe.searchtask", "chain.subscribe.facade", "application.torrent.download",
        "application.subscription.sitebudget", "sdk.plugin", "domain.metainfo",
        "adapters.system.rust", "application.rules", "runtime.progress", "schemas.types"]}
    targets = {
        "SearchChain": type("SearchChain", (), {}), "manual": modules["chain.search.manual"],
        "execution": modules["chain.search.execution"], "SearchScan": type("SearchScan", (), {}),
        "SearchSources": type("SearchSources", (), {}), "SearchTitleOwner": type("SearchTitleOwner", (), {}),
        "SubscriptionSearchTaskRunner": type("SubscriptionSearchTaskRunner", (), {}),
        "SearchPaginationOwner": type("SearchPaginationOwner", (), {}),
        "SubscribeChain": type("SubscribeChain", (), {
            "parse_subscribe_source_keyword": staticmethod(lambda origin: None)}),
        "result": modules["chain.search.result"],
        "metainfo": modules["domain.metainfo"], "rust": modules["adapters.system.rust"],
        "RuleHelper": type("RuleHelper", (), {}), "ProgressHelper": type("ProgressHelper", (), {}),
        "SearchResultOwner": type("SearchResultOwner", (), {}),
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
        if isinstance(target, type) and (not args or args[0] != "self"):
            function = staticmethod(function)
        setattr(target, name, function)
    modules["chain.search.facade"].SearchChain = targets["SearchChain"]
    modules["chain.subscribe.facade"].SubscribeChain = targets["SubscribeChain"]
    modules["chain.search.scan"].SearchScan = targets["SearchScan"]
    modules["chain.search.scan"].SearchSources = targets["SearchSources"]
    modules["chain.search.scan"].AUTOMATIC_REQUEST_INTERVAL = 10
    modules["chain.search.title"].SearchTitleOwner = targets["SearchTitleOwner"]
    modules["chain.search.pagination"].SearchPaginationOwner = targets["SearchPaginationOwner"]
    modules["chain.subscribe.searchtask"].SubscriptionSearchTaskRunner = targets["SubscriptionSearchTaskRunner"]
    modules["application.subscription.sitebudget"].SubscriptionSiteBudget = type("SubscriptionSiteBudget", (), {})
    modules["sdk.plugin"].PluginManager = lambda: NS(get_plugin_modules=lambda: {})
    modules["application.rules"].RuleHelper = targets["RuleHelper"]
    modules["runtime.progress"].ProgressHelper = targets["ProgressHelper"]
    modules["application.torrent.download"].TorrentHelper = NS(sort_torrents=lambda items: items)
    modules["schemas.types"].MediaType = NS(TV=NS(value="电视剧"))
    modules["chain.search.result"].SearchResultOwner = targets["SearchResultOwner"]
    modules["chain.search.result"]._disambiguation_key = lambda meta: meta.title
    modules["domain.metainfo"]._rust_parse_options = lambda: {"_cache_key": "test"}
    modules["domain.metainfo"].get_metainfo_accelerator = lambda: None
    modules["chain.search.manual"]._offload = None
    modules["chain.search.title"]._build_title_search_params = None
    modules["chain.search.title"]._build_title_contexts = None
    return targets


def test_install_and_restore_all_descriptors(monkeypatch):
    targets = host(monkeypatch)
    before = {key: dict(vars(target)) for key, target in targets.items()}
    hooks = compat.install(lambda: True, lambda: True, 5)
    assert len(hooks.patches) == 33
    hooks.close()
    for key, target in targets.items():
        assert vars(target) == before[key]


def test_real_facade_dedup_alias_is_deferred_and_restored(monkeypatch):
    targets = host(monkeypatch)
    owner = native_nodes("app/chain/search/result.py", {"SearchResultOwner"},
                         {"_SearchOwnerBase": object})["SearchResultOwner"]
    native_dedup = owner._remove_duplicate
    targets["SearchResultOwner"]._remove_duplicate = staticmethod(native_dedup)
    # Execute the exact native facade alias, which is a separate descriptor.
    tree = ast.parse((KERNEL / "app/chain/search/facade.py").read_text(encoding="utf-8"))
    facade = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "SearchChain")
    alias = next(n for n in facade.body if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == "_remove_duplicate" for t in n.targets))
    scope = {"SearchResultOwner": owner}
    exec(compile(ast.Module(body=[alias], type_ignores=[]), "native_facade_alias", "exec"), scope)
    chain = targets["SearchChain"]
    chain._remove_duplicate = scope["_remove_duplicate"]
    descriptor = vars(chain)["_remove_duplicate"]
    def context(title, seeders):
        return NS(torrent_info=NS(site_name="site", title=title, description="", seeders=seeders),
                  media_info_is_target=True)
    first, other, last = context("duplicate", 100), context("other", 50), context("duplicate", 1)
    sorter = lambda items: sorted(items, key=lambda c: c.torrent_info.seeders, reverse=True)
    manual = NS(results=lambda items: chain._remove_duplicate(sorter(items)))
    expected = manual.results([first, last, other])
    hooks = compat.install(lambda: True, lambda: True, 3)
    pool = ParsePool(sorter=sorter, dedup=native_dedup)
    try:
        assert pool.merge([parse_page(manual, [first, last]), parse_page(manual, [other])]) == expected
        assert manual.results([first, last]) == [last]
    finally:
        pool.close()
        hooks.close()
    assert vars(chain)["_remove_duplicate"] is descriptor


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
             "_PluginBase": object, "logger": NS(error=lambda *_: None),
             "eventmanager": NS(register=lambda *args, **kwargs: (lambda handler: handler)),
             "Event": object, "ChainEventType": NS(ResourceSelection="resource.selection")}
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
    assert len(second._hooks.patches) == 33
    second.stop_service()
    assert Plugin.get_instance() is None


def form_models(node):
    if isinstance(node, dict):
        found = []
        props = node.get("props")
        if isinstance(props, dict) and "model" in props:
            found.append(props["model"])
        for value in node.values():
            found += form_models(value)
        return found
    if isinstance(node, list):
        return [model for item in node for model in form_models(item)]
    return []


def test_form_shows_every_model_it_defaults_and_hides_the_rest():
    form, default = plugin_class()().get_form()
    models = form_models(form)
    assert sorted(models) == sorted(default)
    # 边抓边洗、中英搜索与一季同组是默认行为，不占界面；代码仍从配置读，缺省为开。
    assert not {"streaming_pipeline", "bilingual_names", "single_group_download"} & set(models)


def test_incompatible_host_leaves_plugin_disabled(monkeypatch):
    targets = host(monkeypatch)
    targets["SearchScan"].run = lambda self, unknown, extra: None
    plugin = plugin_class()()
    plugin.init_plugin({"enabled": True})
    assert not plugin.get_state() and plugin._hooks is None
