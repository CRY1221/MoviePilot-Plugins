"""All private MP 3.1.4 dependencies and transactional installation live here."""
import inspect
from dataclasses import replace

from .hooks import Hooks
from .scheduling import (
    SHARED_GATE, is_music, make_site_sync, make_site_async, make_read_pages,
    make_execution_run, make_execution_events, make_keyword_delay,
    make_resource_pages,
)
from .subscription import make_owned_task, make_advance_sources
from .presentation import make_title_stream
from .streaming import ParsePool, make_page_events, make_dedup
from .names import make_prepare_params
from .recognition import make_meta, make_same_work, make_scope, make_stage, make_probe, make_native_error
from .rulecompat import make_filter


def require(target, name, parameters, kind="sync"):
    function = getattr(target, name)
    if tuple(inspect.signature(function).parameters) != tuple(parameters.split()):
        raise RuntimeError(f"MP 接口签名不兼容：{name}")
    actual = "generator" if inspect.isasyncgenfunction(function) else "async" if inspect.iscoroutinefunction(function) else "sync"
    if actual != kind or not function.__module__.startswith("app.") or hasattr(function, "__wrapped__"):
        raise RuntimeError(f"MP 接口已被替换或类型不兼容：{name}，请检查其他插件并正常重启")
    return function


def install(enabled, subscription_enabled, interval, streaming_enabled=None, max_pages=None,
            names_enabled=None):
    streaming_enabled = streaming_enabled or enabled
    max_pages = max_pages or (lambda: 1)
    names_enabled = names_enabled or enabled
    from app.chain.search.facade import SearchChain
    from app.chain.search import manual, execution
    from app.chain.search.scan import SearchScan, SearchSources, AUTOMATIC_REQUEST_INTERVAL
    from app.application.subscription.sitebudget import SubscriptionSiteBudget
    from app.chain.search import title
    from app.chain.search.title import SearchTitleOwner
    from app.chain.search.result import SearchResultOwner
    from app.chain.search import result
    from app.domain import metainfo
    from app.adapters.system import rust
    from app.application.rules import RuleHelper
    from app.runtime.progress import ProgressHelper
    from app.chain.subscribe.searchtask import SubscriptionSearchTaskRunner
    from app.application.torrent.download import TorrentHelper
    from app.sdk.plugin import PluginManager

    def no_plugin_sources():
        try:
            modules = PluginManager().get_plugin_modules()
            return isinstance(modules, dict) and all(
                isinstance(methods, dict) and not ({"search_torrents", "async_search_torrents"} & methods.keys())
                for methods in modules.values())
        except Exception:
            return False

    def minimum_interval(owner):
        # Native budget claims precede the gate wait. Also enforce the native
        # minimum at the real request start, not just at budget reservation time.
        return AUTOMATIC_REQUEST_INTERVAL if isinstance(
            getattr(owner, "_subscription_site_budget", None), SubscriptionSiteBudget) else 0

    sync = require(SearchChain, "search_site_torrents", "self site keyword mtype page")
    asynchronous = require(SearchChain, "async_search_site_torrents", "self site keyword mtype page", "async")
    read = require(manual, "_read_pages", "manual keys page", "generator")
    page_events = require(manual, "_page_events", "manual params", "generator")
    torrent_meta = require(result, "_torrent_meta", "torrent custom_words mediainfo")
    same_work = require(result, "_same_work_matched", "torrent torrent_meta mediainfo cache")
    filter_results = require(result, "_filter_torrents", "owner torrents mediainfo rule_groups filter_params progress diagnostics")
    match_results = require(result, "_match_torrents", "torrents mediainfo season_episodes custom_words progress include_candidates diagnostics")
    rust_filter = require(rust, "filter_torrents", "groups torrent_list rule_set mediainfo metainfo_options")
    rust_parse = require(rust, "parse_metainfo", "title subtitle options")
    python_parse = require(metainfo, "_build_python_meta_info", "title subtitle custom_words")
    project_meta = require(metainfo, "_meta_from_rust", "parsed")
    rule_groups = require(RuleHelper, "get_rule_group_by_media", "self media group_names")
    progress_update = require(ProgressHelper, "update", "self value text data")
    native_error = require(rust, "_raise_non_rust_panic", "err")
    dedup = require(SearchResultOwner, "_remove_duplicate", "_torrents")

    def parse_configuration():
        # MP clears/rebuilds these options when parsing settings change. Include
        # the current accelerator identity for fallback/reload transitions.
        return (metainfo._rust_parse_options()["_cache_key"], id(metainfo.get_metainfo_accelerator()))
    run = require(execution.SearchExecutionOwner, "run", "owner plan media_chain")
    events = require(execution.SearchExecutionOwner, "events", "owner plan media_chain streaming", "generator")
    delay = require(execution, "_keyword_delay", "owner search_count")
    from app.chain.search.pagination import SearchPaginationOwner
    resource_pages = require(SearchPaginationOwner, "_get_search_resource_pages", "")
    facade_resource_pages = require(SearchChain, "_get_search_resource_pages", "")
    prepare_params = require(SearchChain, "_prepare_params",
                             "mediainfo keyword no_exists include_candidates")
    advance = require(SearchScan, "_advance_sources", "self keys submit started")
    require(SearchScan, "run", "self submit")
    require(SearchScan, "step", "self key")
    require(SearchSources, "fetch", "self key page budget")
    owned = require(SubscriptionSearchTaskRunner, "_execute_owned_task",
                    "self subscribe task_id lease_token execution_lease cancelled")
    resolve = require(SearchTitleOwner, "_resolve_title_request", "self request")
    title_stream = require(SearchTitleOwner, "async_search_by_title_stream",
                           "self title page sites cache_local mtype rule_groups", "generator")
    require(SearchChain, "async_search_by_id_stream",
            "self media_source media_id mtype area season sites cache_local music_type include_candidates", "generator")
    require(SearchChain, "async_search_by_title_stream",
            "self title page sites cache_local mtype rule_groups", "generator")

    def sorted_title(owner, request):
        result = resolve(owner, request)
        if enabled() and not is_music(request.mtype):
            return replace(result, contexts=TorrentHelper.sort_torrents(result.contexts))
        return result

    # Parsing runs in its own pool so it overlaps site fetching instead of
    # competing with it or waiting until the very end.
    parse_pool = ParsePool(workers=4, sorter=TorrentHelper.sort_torrents,
                           dedup=dedup)
    patches = [
        (SearchChain, "search_site_torrents", make_site_sync(sync, SHARED_GATE, enabled, minimum_interval)),
        (SearchChain, "async_search_site_torrents", make_site_async(asynchronous, SHARED_GATE, enabled, minimum_interval)),
        (manual, "_read_pages", make_read_pages(read, manual._offload, enabled, max_pages, streaming_enabled)),
        (result, "_torrent_meta", make_meta(torrent_meta, enabled, parse_configuration)),
        (result, "_same_work_matched", make_same_work(same_work, enabled, result._disambiguation_key, parse_configuration)),
        (result, "_filter_torrents", make_stage(filter_results, enabled, "filter")),
        (result, "_match_torrents", make_stage(match_results, enabled, "match")),
        (rust, "filter_torrents", make_probe(make_filter(rust_filter, enabled), enabled, "rust_filter")),
        (rust, "parse_metainfo", make_probe(rust_parse, enabled, "rust_parse")),
        (metainfo, "_build_python_meta_info", make_probe(python_parse, enabled, "python_parse")),
        (metainfo, "_meta_from_rust", make_probe(project_meta, enabled, "rust_projection")),
        (RuleHelper, "get_rule_group_by_media", make_probe(rule_groups, enabled, "rule_group_lookup")),
        (ProgressHelper, "update", make_probe(progress_update, enabled, "progress_update")),
        (rust, "_raise_non_rust_panic", make_native_error(native_error, enabled)),
        (SearchResultOwner, "_remove_duplicate", staticmethod(make_dedup(dedup))),
        (manual, "_page_events", make_scope(make_page_events(page_events, streaming_enabled, parse_pool), enabled, is_music, generator=True)),
        (execution.SearchExecutionOwner, "run", staticmethod(make_scope(make_execution_run(run, enabled, no_plugin_sources), enabled, is_music))),
        (execution.SearchExecutionOwner, "events", staticmethod(make_scope(make_execution_events(events, enabled, no_plugin_sources), enabled, is_music, generator=True))),
        (execution, "_keyword_delay", make_keyword_delay(delay, enabled)),
        (SearchPaginationOwner, "_get_search_resource_pages",
         staticmethod(make_resource_pages(resource_pages, enabled, max_pages))),
        (SearchChain, "_get_search_resource_pages",
         staticmethod(make_resource_pages(facade_resource_pages, enabled, max_pages))),
        (SearchChain, "_prepare_params",
         staticmethod(make_prepare_params(prepare_params, names_enabled))),
        (SearchScan, "_advance_sources", make_advance_sources(advance, subscription_enabled)),
        (SubscriptionSearchTaskRunner, "_execute_owned_task", make_owned_task(owned, subscription_enabled)),
        (SearchTitleOwner, "_resolve_title_request", make_scope(sorted_title, enabled, is_music)),
        (SearchTitleOwner, "async_search_by_title_stream", make_scope(make_title_stream(
            title_stream, enabled, title._build_title_search_params, title._build_title_contexts,
            manual._offload, TorrentHelper.sort_torrents,
            lambda: title.get_configured_system_config().get(title.SystemConfigKey.SearchFilterRuleGroups) or []),
            enabled, is_music, generator=True)),
    ]
    hooks = Hooks()
    hooks.add_closer(parse_pool.close)
    try:
        SHARED_GATE.interval = interval
        for target, name, wrapper in patches:
            hooks.install(target, name, wrapper)
    except Exception:
        hooks.close()
        raise
    return hooks
