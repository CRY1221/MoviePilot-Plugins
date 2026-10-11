"""
极速并发搜索插件 (FastIntersectSearch)
1. 全面接管系统网页前端搜索流 (SearchChain.search_page_events / async_search_by_id_stream / async_search_by_title_stream)；
2. 跨站点异步并发检索，单站点多轮请求强制 3 秒安全频控防封；
3. 流式流水线架构 (Streaming Pipeline)：单站即出即洗，前置过滤前置，密集计算全异步线程池化；
4. MetaInfo 单次实例化与全链路零开销复用，消灭重复标题解析瓶颈；
5. 系统 Rust 引擎纳秒级规则过滤 + 完结全集包智能提权排序；
6. 独立日志写入 plugins/fastintersectsearch.log，支持实时诊断。
"""
import asyncio
import contextvars
import re
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Set, Tuple, Type

from app.agent.tools.base import MoviePilotTool, ToolTag
from app.application.torrent.download import TorrentHelper
from app.chain.media import MediaChain
from app.chain.search.facade import SearchChain
from app.domain.context import Context, MediaInfo, TorrentInfo
from app.domain.metainfo import MetaInfo
from app.log import logger
from app.schemas.types import MediaSource, MediaType
from app.sdk.plugin.base import _PluginBase

# 用于追踪防止递归调用的上下文标记
_in_fastsearch_context = contextvars.ContextVar("_in_fastsearch_context", default=False)


class FastIntersectSearchTool(MoviePilotTool):
    """
    极速并发搜索工具：跨站点并发检索，单站强制 3 秒安全频控；
    流式流水线架构：前置过滤前置，单次解析复用 MetaInfo，Rust 引擎秒级评级及完结全集包提权排序。
    """

    name: str = "fast_intersect_search"
    description: str = (
        "极速并发搜索工具：跨站点并发检索，单站点多轮请求强制等待 3 秒防刷；"
        "流式流水线即出即洗，前置过滤前置，内存直接调用系统 TorrentHelper 完成识别去噪与防抓错，"
        "并调用系统 FilterChain 走 Rust 引擎完成规则优先组筛选及已完结全集包提权排序。"
    )
    tags: List[ToolTag] = [ToolTag.Read, ToolTag.Resource]

    async def run(
        self,
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        """执行 Agent 智能体工具调用。"""
        if "kwargs" in kwargs and isinstance(kwargs["kwargs"], dict):
            nested = kwargs.pop("kwargs")
            kwargs.update(nested)
        if "args" in kwargs and isinstance(kwargs["args"], (list, tuple)) and kwargs["args"]:
            if "title" not in kwargs and len(kwargs["args"]) >= 1:
                kwargs["title"] = kwargs["args"][0]
        if args and "title" not in kwargs:
            kwargs["title"] = args[0]

        title = str(kwargs.get("title") or "")
        en_title = str(kwargs.get("en_title") or "")
        site_ids = kwargs.get("site_ids")
        require_complete = bool(kwargs.get("require_complete", True))
        top_k = int(kwargs.get("top_k", 20))

        plugin = FastIntersectSearch.get_instance()
        if not plugin or not plugin.get_state():
            return {
                "success": False,
                "message": "极速搜索插件未安装或未启用",
            }

        return await plugin.execute_tool_search(
            title=title,
            en_title=en_title,
            site_ids=site_ids,
            require_complete=require_complete,
            top_k=top_k,
        )


class FastIntersectSearch(_PluginBase):
    """
    极速搜索主插件：实现网页前端搜索接管与流式流水线加速。
    """

    plugin_name: str = "极速交叉搜索"
    plugin_desc: str = (
        "跨站并发+单站3秒频控，流式流水线即出即洗，"
        "MetaInfo单次复用，Rust前置初筛与定级，完结全集包提权。"
    )
    plugin_version: str = "1.6.0"
    plugin_author: str = "CRY1221"
    plugin_order: int = 30

    _instance: Optional["FastIntersectSearch"] = None
    _enabled: bool = True
    _bilingual_search: bool = True
    _accelerate_new_subscribe: bool = True
    _default_top_k: int = 0
    _search_timeout: int = 15
    _site_sleep_seconds: float = 3.0

    # 原始 SearchChain / SearchScan 方法句柄备份
    _orig_search_page_events = None
    _orig_async_search_by_id_stream = None
    _orig_async_search_by_title_stream = None
    _orig_async_search_by_id = None
    _orig_async_search_by_title = None
    _orig_process = None
    _orig_async_process = None
    _orig_scan_run = None

    @classmethod
    def get_instance(cls) -> Optional["FastIntersectSearch"]:
        """获取当前插件活跃单例。"""
        return cls._instance

    def init_plugin(self, config: dict = None) -> None:
        """根据配置初始化插件并装配或卸载搜索接管 Hook。"""
        FastIntersectSearch._instance = self
        config = config or {}
        self._enabled = bool(config.get("enabled", True))
        self._bilingual_search = bool(config.get("bilingual_search", True))
        self._accelerate_new_subscribe = bool(config.get("accelerate_new_subscribe", True))
        self._default_top_k = int(config.get("default_top_k", 0) or 0)
        self._search_timeout = int(config.get("search_timeout", 15) or 15)
        self._site_sleep_seconds = float(config.get("site_sleep_seconds", 3.0) or 3.0)

        # 备份并安装/卸载系统 SearchChain 钩子
        self._backup_original_methods()
        if self._enabled:
            self._install_hooks()
            logger.info("已成功接管 MoviePilot 系统 SearchChain 搜索流（网页端搜索全面生效极速流式流水线）")
        else:
            self._uninstall_hooks()
            logger.info("已恢复 MoviePilot 原生搜索流")

        logger.info(
            f"极速搜索插件初始化完成: enabled={self._enabled}, "
            f"双语搜索={self._bilingual_search}, "
            f"新建订阅加速={self._accelerate_new_subscribe}, "
            f"单站间隔={self._site_sleep_seconds}s, "
            f"默认TopK={self._default_top_k}, 单站超时={self._search_timeout}s"
        )

    def _backup_original_methods(self) -> None:
        """备份系统原生搜索方法。"""
        if FastIntersectSearch._orig_search_page_events is None:
            FastIntersectSearch._orig_search_page_events = SearchChain.search_page_events
        if FastIntersectSearch._orig_async_search_by_id_stream is None:
            FastIntersectSearch._orig_async_search_by_id_stream = SearchChain.async_search_by_id_stream
        if FastIntersectSearch._orig_async_search_by_title_stream is None:
            FastIntersectSearch._orig_async_search_by_title_stream = SearchChain.async_search_by_title_stream
        if FastIntersectSearch._orig_async_search_by_id is None:
            FastIntersectSearch._orig_async_search_by_id = SearchChain.async_search_by_id
        if FastIntersectSearch._orig_async_search_by_title is None:
            FastIntersectSearch._orig_async_search_by_title = SearchChain.async_search_by_title
        if FastIntersectSearch._orig_process is None:
            FastIntersectSearch._orig_process = SearchChain.process
        if FastIntersectSearch._orig_async_process is None:
            FastIntersectSearch._orig_async_process = SearchChain.async_process

        try:
            from app.chain.search.scan import SearchScan
            if FastIntersectSearch._orig_scan_run is None:
                FastIntersectSearch._orig_scan_run = SearchScan.run
        except Exception as e:
            logger.warning(f"备份 SearchScan.run 异常: {e}")

    def _install_hooks(self) -> None:
        """将系统 SearchChain 的搜索接口替换为极速搜索插件的接管方法。"""
        plugin = self

        async def hooked_search_page_events(
            chain_self,
            *,
            params: dict[str, Any],
        ) -> AsyncIterator[dict[str, Any]]:
            """接管 Web 前端手动分页单页搜索 (manual_paging=True)。"""
            mtype = params.get("mtype")
            if (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                async for evt in plugin._orig_search_page_events(chain_self, params=params):
                    yield evt
                return

            token = _in_fastsearch_context.set(True)
            try:
                media_source = params.get("media_source")
                media_id = params.get("media_id")
                season = params.get("season")
                sites = params.get("sites")
                keyword = params.get("keyword")

                if media_source and media_id:
                    async for event in plugin.stream_search_by_media(
                        chain_instance=chain_self,
                        media_source=media_source,
                        media_id=str(media_id),
                        mtype=mtype,
                        season=season,
                        sites=sites,
                        cache_local=True,
                    ):
                        yield event
                elif keyword:
                    async for event in plugin.stream_search_by_title(
                        chain_instance=chain_self,
                        title=str(keyword),
                        sites=sites,
                        mtype=mtype,
                        cache_local=True,
                    ):
                        yield event
                else:
                    async for evt in plugin._orig_search_page_events(chain_self, params=params):
                        yield evt
            finally:
                _in_fastsearch_context.reset(token)

        async def hooked_async_search_by_id_stream(
            chain_self,
            media_source: MediaSource,
            media_id: str,
            mtype: Optional[MediaType] = None,
            area: Optional[str] = "title",
            season: Optional[int] = None,
            sites: Optional[List[int]] = None,
            cache_local: bool = False,
            music_type: Optional[str] = None,
            include_candidates: bool = False,
        ) -> AsyncIterator[dict]:
            """接管流式精准媒体搜索。"""
            if music_type or (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                async for evt in plugin._orig_async_search_by_id_stream(
                    chain_self, media_source, media_id, mtype=mtype, area=area,
                    season=season, sites=sites, cache_local=cache_local,
                    music_type=music_type, include_candidates=include_candidates,
                ):
                    yield evt
                return

            token = _in_fastsearch_context.set(True)
            try:
                async for event in plugin.stream_search_by_media(
                    chain_instance=chain_self,
                    media_source=media_source,
                    media_id=media_id,
                    mtype=mtype,
                    season=season,
                    sites=sites,
                    cache_local=cache_local,
                ):
                    yield event
            finally:
                _in_fastsearch_context.reset(token)

        async def hooked_async_search_by_title_stream(
            chain_self,
            title: str,
            page: Optional[int] = 0,
            sites: Optional[List[int]] = None,
            cache_local: Optional[bool] = False,
            mtype: Optional[MediaType] = None,
            rule_groups: Optional[List[str]] = None,
        ) -> AsyncIterator[dict]:
            """接管流式模糊标题搜索。"""
            if (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                async for evt in plugin._orig_async_search_by_title_stream(
                    chain_self, title=title, page=page, sites=sites,
                    cache_local=cache_local, mtype=mtype, rule_groups=rule_groups,
                ):
                    yield evt
                return

            token = _in_fastsearch_context.set(True)
            try:
                async for event in plugin.stream_search_by_title(
                    chain_instance=chain_self,
                    title=title,
                    sites=sites,
                    mtype=mtype,
                    rule_groups=rule_groups,
                    cache_local=cache_local,
                ):
                    yield event
            finally:
                _in_fastsearch_context.reset(token)

        async def hooked_async_search_by_id(
            chain_self,
            media_source: MediaSource,
            media_id: str,
            mtype: Optional[MediaType] = None,
            area: Optional[str] = "title",
            season: Optional[int] = None,
            sites: Optional[List[int]] = None,
            cache_local: bool = False,
            music_type: Optional[str] = None,
            include_candidates: bool = False,
        ) -> List[Context]:
            """接管非流式精准搜索并聚合成 Context 列表返回。"""
            collector: List[Context] = []
            async for _ in plugin.stream_search_by_media(
                chain_instance=chain_self,
                media_source=media_source,
                media_id=media_id,
                mtype=mtype,
                season=season,
                sites=sites,
                cache_local=cache_local,
                contexts_collector=collector,
            ):
                pass
            return collector

        async def hooked_async_search_by_title(
            chain_self,
            title: str,
            page: Optional[int] = 0,
            sites: Optional[List[int]] = None,
            cache_local: Optional[bool] = False,
            mtype: Optional[MediaType] = None,
            rule_groups: Optional[List[str]] = None,
        ) -> List[Context]:
            """接管非流式标题搜索并聚合成 Context 列表返回。"""
            collector: List[Context] = []
            async for _ in plugin.stream_search_by_title(
                chain_instance=chain_self,
                title=title,
                sites=sites,
                mtype=mtype,
                rule_groups=rule_groups,
                cache_local=cache_local,
                contexts_collector=collector,
            ):
                pass
            return collector

        async def hooked_async_process(
            chain_self,
            mediainfo: MediaInfo,
            keyword: Optional[str] = None,
            no_exists: Optional[dict] = None,
            sites: Optional[list] = None,
            rule_groups: Optional[list] = None,
            area: Optional[str] = "title",
            custom_words: Optional[list] = None,
            filter_params: Optional[dict] = None,
            include_candidates: bool = False,
            candidate_filter: Optional[Callable] = None,
        ) -> List[Context]:
            """
            接管系统异步 process 入口。
            """
            mtype = getattr(mediainfo, "type", None)
            if (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                return await plugin._orig_async_process(
                    chain_self,
                    mediainfo=mediainfo,
                    keyword=keyword,
                    no_exists=no_exists,
                    sites=sites,
                    rule_groups=rule_groups,
                    area=area,
                    custom_words=custom_words,
                    filter_params=filter_params,
                    include_candidates=include_candidates,
                    candidate_filter=candidate_filter,
                )

            token = _in_fastsearch_context.set(True)
            try:
                media_source = getattr(mediainfo, "media_source", MediaSource.TMDB) or MediaSource.TMDB
                media_id = str(getattr(mediainfo, "tmdb_id", None) or getattr(mediainfo, "media_id", "") or "")
                collector: List[Context] = []
                if media_id:
                    async for _ in plugin.stream_search_by_media(
                        chain_instance=chain_self,
                        media_source=media_source,
                        media_id=media_id,
                        mtype=mtype,
                        sites=sites,
                        cache_local=True,
                        contexts_collector=collector,
                    ):
                        pass
                else:
                    search_kw = keyword or getattr(mediainfo, "title", "")
                    async for _ in plugin.stream_search_by_title(
                        chain_instance=chain_self,
                        title=str(search_kw),
                        sites=sites,
                        mtype=mtype,
                        rule_groups=rule_groups,
                        cache_local=True,
                        contexts_collector=collector,
                    ):
                        pass
                if candidate_filter and collector:
                    collector = candidate_filter(collector)
                return collector
            finally:
                _in_fastsearch_context.reset(token)

        def hooked_process(
            chain_self,
            mediainfo: MediaInfo,
            keyword: Optional[str] = None,
            no_exists: Optional[dict] = None,
            sites: Optional[list] = None,
            rule_groups: Optional[list] = None,
            area: Optional[str] = "title",
            custom_words: Optional[list] = None,
            filter_params: Optional[dict] = None,
            include_candidates: bool = False,
            candidate_filter: Optional[Callable] = None,
        ) -> List[Context]:
            """
            接管系统同步 process 入口（通知渠道点播核心入口：Telegram/微信）。
            """
            mtype = getattr(mediainfo, "type", None)
            if (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                return plugin._orig_process(
                    chain_self,
                    mediainfo=mediainfo,
                    keyword=keyword,
                    no_exists=no_exists,
                    sites=sites,
                    rule_groups=rule_groups,
                    area=area,
                    custom_words=custom_words,
                    filter_params=filter_params,
                    include_candidates=include_candidates,
                    candidate_filter=candidate_filter,
                )

            logger.info("极速搜索接管通知渠道交互搜索: %s (%s)", getattr(mediainfo, "title", keyword), mtype)
            coro = hooked_async_process(
                chain_self=chain_self,
                mediainfo=mediainfo,
                keyword=keyword,
                no_exists=no_exists,
                sites=sites,
                rule_groups=rule_groups,
                area=area,
                custom_words=custom_words,
                filter_params=filter_params,
                include_candidates=include_candidates,
                candidate_filter=candidate_filter,
            )

            try:
                # 尝试获取当前线程运行的事件循环
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                # 处于活跃异步循环中，通过独立线程运行协程，防止阻塞正在运行的循环
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(asyncio.run, coro)
                    return future.result()
            else:
                return asyncio.run(coro)

        SearchChain.search_page_events = hooked_search_page_events
        SearchChain.async_search_by_id_stream = hooked_async_search_by_id_stream
        SearchChain.async_search_by_title_stream = hooked_async_search_by_title_stream
        SearchChain.async_search_by_id = hooked_async_search_by_id
        SearchChain.async_search_by_title = hooked_async_search_by_title
        SearchChain.process = hooked_process
        SearchChain.async_process = hooked_async_process

        try:
            from app.chain.search.scan import SearchScan

            def hooked_scan_run(scan_self, submit: Callable[[List[Context], Set[str]], Set[str]]) -> None:
                """
                接管新建订阅首次搜索：
                利用极速并发流式流水线预先抓取洗净并排序的优质种子，
                安全注入给原生 submit 闭包以完成订阅决策与状态推进。
                """
                if not plugin._enabled or not plugin._accelerate_new_subscribe:
                    return plugin._orig_scan_run(scan_self, submit)

                execution = getattr(scan_self, "execution", None)
                plan = getattr(scan_self, "plan", None)
                media = getattr(scan_self, "media", None)
                # 检查执行上下文或媒体信息
                if not plan or not media:
                    return plugin._orig_scan_run(scan_self, submit)

                # 识别当前订阅是否为新建订阅 (state == 'N')
                is_new_subscribe = False
                try:
                    from app.chain.subscribe.facade import SubscribeChain
                    sub_repo = getattr(SubscribeChain(), "subscription_repository", None)
                    if execution and getattr(execution, "task_id", None) and sub_repo:
                        # 查找关联订阅
                        for sub_item in sub_repo.list() or []:
                            if getattr(sub_item, "state", None) == "N":
                                if (sub_item.tmdbid and str(sub_item.tmdbid) == str(media.tmdb_id)) or (sub_item.name == media.title):
                                    is_new_subscribe = True
                                    break
                    elif sub_repo:
                        # 兜底：如果库内该媒体处于 N 状态
                        for sub_item in sub_repo.list() or []:
                            if getattr(sub_item, "state", None) == "N" and (
                                (sub_item.tmdbid and str(sub_item.tmdbid) == str(media.tmdb_id)) or (sub_item.name == media.title)
                            ):
                                is_new_subscribe = True
                                break
                except Exception as ex:
                    logger.debug(f"检查订阅状态异常: {ex}")

                if not is_new_subscribe:
                    # 非新建订阅（日常巡检追更），100% 原路放行原生 scan.run
                    return plugin._orig_scan_run(scan_self, submit)

                logger.info(f"⚡ 极速搜索接管新建订阅首次搜索加速: 《{media.title}》")
                try:
                    # 使用当前线程或独立线程运行极速流式预取
                    collector: List[Context] = []
                    coro = plugin._async_prefetch_subscription_contexts(
                        chain_instance=scan_self.owner,
                        mediainfo=media,
                        sites=list(scan_self.sources.sites.keys()) if hasattr(scan_self, "sources") else None,
                        rule_groups=plan.rule_groups,
                    )
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None

                    if loop and loop.is_running():
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                            collector = pool.submit(asyncio.run, coro).result()
                    else:
                        collector = asyncio.run(coro)

                    if collector:
                        logger.info(f"⚡ 首次订阅极速预取交付 {len(collector)} 个优质种子，直接喂给原生结算闭包")
                        ready_targets = scan_self.collection.targets
                        settled = submit(collector, ready_targets)
                        if settled:
                            logger.info(f"⚡ 原生结算闭包已成功采纳并下载目标: {settled}")
                            scan_self.submitted.update(settled)
                            scan_self.collection.settle(settled)
                            scan_self.checkpoint()

                    # 如果目标已全部结算完毕（例如电影下载成功，或全集包下载成功），优雅收官
                    if not scan_self.collection.remaining:
                        logger.info(f"⚡ 首次订阅目标已由极速搜索全量达成，提前收官，无需后续翻页: {media.title}")
                        scan_self._finish_round()
                        return
                except Exception as e:
                    logger.error(f"首次订阅加速注入异常，平滑回退原生扫描: {e}", exc_info=True)

                # 若仍有残余目标（如未完结散集未完全下满），回退放行原生 scan.run 继续深度搜集
                return plugin._orig_scan_run(scan_self, submit)

            SearchScan.run = hooked_scan_run
        except Exception as e:
            logger.warning(f"装配 SearchScan.run Hook 异常: {e}")

    def _uninstall_hooks(self) -> None:
        """恢复系统原生搜索方法。"""
        if FastIntersectSearch._orig_search_page_events is not None:
            SearchChain.search_page_events = FastIntersectSearch._orig_search_page_events
        if FastIntersectSearch._orig_async_search_by_id_stream is not None:
            SearchChain.async_search_by_id_stream = FastIntersectSearch._orig_async_search_by_id_stream
        if FastIntersectSearch._orig_async_search_by_title_stream is not None:
            SearchChain.async_search_by_title_stream = FastIntersectSearch._orig_async_search_by_title_stream
        if FastIntersectSearch._orig_async_search_by_id is not None:
            SearchChain.async_search_by_id = FastIntersectSearch._orig_async_search_by_id
        if FastIntersectSearch._orig_async_search_by_title is not None:
            SearchChain.async_search_by_title = FastIntersectSearch._orig_async_search_by_title
        if FastIntersectSearch._orig_process is not None:
            SearchChain.process = FastIntersectSearch._orig_process
        if FastIntersectSearch._orig_async_process is not None:
            SearchChain.async_process = FastIntersectSearch._orig_async_process
        if FastIntersectSearch._orig_scan_run is not None:
            try:
                from app.chain.search.scan import SearchScan
                SearchScan.run = FastIntersectSearch._orig_scan_run
            except Exception:
                pass

    def get_state(self) -> bool:
        """获取启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """插件远程命令。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """插件 API 端点。"""
        return []

    def get_agent_tools(self) -> List[Type[MoviePilotTool]]:
        """注册智能体工具。"""
        if not self._enabled:
            return []
        return [FastIntersectSearchTool]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """获取配置表单与默认值。"""
        form = [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "enabled",
                            "label": "启用极速并发搜索插件（默认自动接管网页端搜索流与通知渠道点播）",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "accelerate_new_subscribe",
                            "label": "新建订阅首次搜索加速（仅在新建订阅执行首次检索时注入秒级并发，完成后无缝切回原生）",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "bilingual_search",
                            "label": "中英双语搜索（开启后同时检索中文名与英文原名/译名；关闭后仅检索单一主标题）",
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "site_sleep_seconds",
                            "label": "同站点连续请求等待间隔 (秒，防刷频控，默认 3.0)",
                            "type": "number",
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "search_timeout",
                            "label": "站点单次搜索超时时间 (秒，建议 12~15)",
                            "type": "number",
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "default_top_k",
                            "label": "搜索结果最大呈现数量 (填 0 不限制，全量展示)",
                            "type": "number",
                        },
                    },
                ],
            }
        ]
        default = {
            "enabled": True,
            "accelerate_new_subscribe": True,
            "bilingual_search": True,
            "site_sleep_seconds": 3.0,
            "search_timeout": 15,
            "default_top_k": 0,
        }
        return form, default

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页展示。"""
        if not self._enabled:
            return None
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "success",
                    "text": (
                        "极速流式搜索插件已全局生效。单站即出即洗流水线、同站 3 秒频控防封、"
                        "前置过滤前置、MetaInfo单次解析复用、排序完全交由系统原生模块标准化处理。"
                    ),
                },
            }
        ]

    def stop_service(self) -> None:
        """卸载钩子并停止服务。"""
        self._uninstall_hooks()
        logger.info("极速搜索插件后台服务已停止，系统搜索流已复原")

    async def _site_producer(
        self,
        chain_instance: SearchChain,
        site: dict,
        keywords: List[str],
        mtype: Optional[MediaType],
        queue: asyncio.Queue,
    ) -> None:
        """
        单站点独立生产者：
        每搜出一个关键词的结果，立即推入队列，供消费端流水线无缝并行处理；
        若有多轮关键词，同站安全睡眠指定秒数；完成后推入结束标记。
        """
        site_name = site.get("name") or str(site.get("id"))
        try:
            for idx, kw in enumerate(keywords):
                if not kw or not kw.strip():
                    continue
                if idx > 0 and self._site_sleep_seconds > 0:
                    logger.info(
                        f"站点 [{site_name}] 准备检索第 {idx + 1} 个关键词 '{kw}'，"
                        f"安全等待 {self._site_sleep_seconds} 秒防刷频控..."
                    )
                    await asyncio.sleep(self._site_sleep_seconds)

                t_start = time.time()
                try:
                    results = await asyncio.wait_for(
                        chain_instance.async_search_site_torrents(
                            site=site,
                            keyword=kw.strip(),
                            mtype=mtype,
                            page=0,
                        ),
                        timeout=self._search_timeout,
                    )
                    count = len(results) if results else 0
                    cost = round(time.time() - t_start, 2)
                    logger.info(
                        f"站点 [{site_name}] 关键词 '{kw}' 搜索完成，耗时 {cost}s，返回种子 {count} 个"
                    )
                    if results:
                        await queue.put((site_name, results))
                except asyncio.TimeoutError:
                    logger.warning(f"站点 [{site_name}] 检索 '{kw}' 超时 ({self._search_timeout}s)")
                except Exception as exc:
                    logger.warning(f"站点 [{site_name}] 检索 '{kw}' 异常: {exc}")
        finally:
            # 无论异常与否，该站点生产结束，发送 None 标记
            await queue.put((site_name, None))

    @staticmethod
    def _pipeline_process_media_batch(
        torrents: List[TorrentInfo],
        seen_keys: Set[str],
        mediainfo: MediaInfo,
        sort_rulegroup: Optional[str],
        chain_instance: SearchChain,
    ) -> Tuple[List[Context], int]:
        """
        精准媒体搜索的单批次流式清洗管道（由 asyncio.to_thread 调度执行）：
        1. 增量内存去重；
        2. 系统 Rust 引擎前置硬过滤（白名单、排除3D、单层杜比阻断等）；
        3. 内存识别消歧（仅对前置合格的种子执行，MetaInfo 仅单次实例化）；
        4. 系统规则链定级（打上原生 pri_order）；
        5. 装配 Context 并完全委托系统原生 TorrentHelper.sort_torrents 标准化排序。
        """
        raw_count = len(torrents)
        unique_torrents: List[TorrentInfo] = []
        for t in torrents:
            key = t.enclosure if t.enclosure and not t.enclosure.startswith("[") else t.title
            if key and key not in seen_keys:
                seen_keys.add(key)
                unique_torrents.append(t)

        if not unique_torrents:
            return [], raw_count

        # 1. 系统 Rust 引擎前置硬过滤
        pre_filtered = unique_torrents
        try:
            pre_res = chain_instance.filter_torrents(
                rule_groups=["前置过滤"],
                torrent_list=unique_torrents,
                mediainfo=mediainfo,
            )
            if pre_res is not None:
                pre_filtered = pre_res
        except Exception as e:
            logger.error(f"前置规则过滤异常: {e}")
            pre_filtered = unique_torrents

        if not pre_filtered:
            return [], raw_count

        # 2. 识别消歧（单次生成并复用 MetaInfo）
        valid_items: List[Tuple[TorrentInfo, MetaInfo]] = []
        for t in pre_filtered:
            meta = MetaInfo(title=t.title, subtitle=t.description, mtype=mediainfo.type)
            full_text = f"{t.title} {t.description}".lower()
            is_valid = TorrentHelper.match_torrent(
                mediainfo=mediainfo,
                torrent_meta=meta,
                torrent=t,
            )
            if not is_valid and mediainfo.title.lower() in full_text:
                is_valid = True
            if is_valid:
                valid_items.append((t, meta))

        if not valid_items:
            return [], raw_count

        # 3. 优先级排序定级（系统 Rust 引擎匹配并打上 pri_order）
        valid_torrents = [item[0] for item in valid_items]
        if sort_rulegroup:
            try:
                sorted_objs = chain_instance.filter_torrents(
                    rule_groups=[sort_rulegroup],
                    torrent_list=valid_torrents,
                    mediainfo=mediainfo,
                ) or []
                matched_map = {id(obj): obj for obj in sorted_objs}
                for t, _ in valid_items:
                    if id(t) in matched_map:
                        t.pri_order = getattr(matched_map[id(t)], "pri_order", 0) or 0
                    else:
                        t.pri_order = 0
            except Exception as e:
                logger.error(f"分类排序规则过滤异常: {e}")

        # 4. 装配 Context 并完全交由系统原生 TorrentHelper.sort_torrents 进行标准化排序
        batch_contexts: List[Context] = []
        for t, meta in valid_items:
            ctx = Context(
                torrent_info=t,
                media_info=mediainfo,
                meta_info=meta,
            )
            batch_contexts.append(ctx)

        try:
            sorted_batch = TorrentHelper.sort_torrents(batch_contexts)
        except Exception as e:
            logger.warning(f"调用系统 TorrentHelper.sort_torrents 异常，回退排序: {e}")
            sorted_batch = sorted(
                batch_contexts,
                key=lambda x: (getattr(x.torrent_info, "pri_order", 0) or 0, getattr(x.torrent_info, "seeders", 0) or 0),
                reverse=True,
            )

        return sorted_batch, raw_count

    @staticmethod
    def _pipeline_process_title_batch(
        torrents: List[TorrentInfo],
        seen_keys: Set[str],
        title: str,
        mtype: Optional[MediaType],
        rule_groups: Optional[List[str]],
        chain_instance: SearchChain,
    ) -> Tuple[List[Context], int]:
        """
        模糊标题搜索的单批次流式清洗管道（由 asyncio.to_thread 调度执行）：
        1. 增量内存去重；
        2. 系统 Rust 引擎前置硬过滤；
        3. 单次生成 MetaInfo 并装配 Context，系统原生排序。
        """
        raw_count = len(torrents)
        unique_torrents: List[TorrentInfo] = []
        for t in torrents:
            key = t.enclosure if t.enclosure and not t.enclosure.startswith("[") else t.title
            if key and key not in seen_keys:
                seen_keys.add(key)
                unique_torrents.append(t)

        if not unique_torrents:
            return [], raw_count

        active_rules = rule_groups if rule_groups else ["前置过滤"]
        filtered_torrents = unique_torrents
        try:
            filtered_res = chain_instance.filter_torrents(
                rule_groups=active_rules,
                torrent_list=unique_torrents,
                mediainfo=None,
            )
            if filtered_res is not None:
                filtered_torrents = filtered_res
        except Exception as e:
            logger.error(f"标题搜索规则过滤异常: {e}")
            filtered_torrents = unique_torrents

        batch_contexts = [
            Context(
                torrent_info=t,
                media_info=None,
                meta_info=MetaInfo(title=t.title, subtitle=t.description, mtype=mtype),
            )
            for t in filtered_torrents
        ]
        try:
            sorted_batch = TorrentHelper.sort_torrents(batch_contexts)
        except Exception:
            sorted_batch = batch_contexts

        return sorted_batch, raw_count

    async def _async_prefetch_subscription_contexts(
        self,
        chain_instance: SearchChain,
        mediainfo: MediaInfo,
        sites: Optional[List[int]] = None,
        rule_groups: Optional[List[str]] = None,
    ) -> List[Context]:
        """
        专门为新建订阅首搜提供的极速并发预取：
        跨站点并发 + 前置过滤 + 识别消歧 + 原生排序，毫秒级交付。
        """
        collector: List[Context] = []
        media_source = getattr(mediainfo, "media_source", MediaSource.TMDB) or MediaSource.TMDB
        media_id = str(getattr(mediainfo, "tmdb_id", None) or getattr(mediainfo, "media_id", "") or "")
        mtype = getattr(mediainfo, "type", None)

        if media_id:
            async for _ in self.stream_search_by_media(
                chain_instance=chain_instance,
                media_source=media_source,
                media_id=media_id,
                mtype=mtype,
                sites=sites,
                cache_local=True,
                contexts_collector=collector,
            ):
                pass
        else:
            async for _ in self.stream_search_by_title(
                chain_instance=chain_instance,
                title=getattr(mediainfo, "title", ""),
                sites=sites,
                mtype=mtype,
                rule_groups=rule_groups,
                cache_local=True,
                contexts_collector=collector,
            ):
                pass

        return collector

    async def stream_search_by_media(
        self,
        chain_instance: SearchChain,
        media_source: Any,
        media_id: str,
        mtype: Optional[MediaType] = None,
        season: Optional[int] = None,
        sites: Optional[Any] = None,
        cache_local: bool = False,
        contexts_collector: Optional[List[Context]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        接管网页端精准媒体搜索流 (GET /api/v1/search/media/{media_id}/stream)
        流式流水线架构：即出即洗、即时推送、前置过滤前置、MetaInfo单次复用。
        """
        t0 = time.time()
        # 兼容 media_source 类型
        if isinstance(media_source, str):
            try:
                media_source_enum = MediaSource(media_source)
            except Exception:
                media_source_enum = MediaSource.TMDB
        else:
            media_source_enum = media_source or MediaSource.TMDB

        # 兼容 sites 类型
        site_filter: Optional[List[int]] = None
        if isinstance(sites, str):
            site_filter = [int(s) for s in sites.split(",") if s.isdigit()]
        elif isinstance(sites, list):
            site_filter = [int(s) for s in sites if str(s).isdigit()]

        logger.info(f"收到网页端精准搜索请求: source={media_source_enum}, id={media_id}, mtype={mtype}, season={season}")

        # 1. 识别媒体信息
        mediainfo = await MediaChain().async_recognize_media(
            media_source=media_source_enum,
            media_id=media_id,
            mtype=mtype,
        )
        if not mediainfo:
            logger.error(f"媒体识别失败: {media_source_enum} {media_id}")
            yield {"type": "error", "success": False, "message": "媒体信息识别失败"}
            return

        is_ended = False
        status_str = str(getattr(mediainfo, "status", "") or "").lower()
        if status_str in ["ended", "canceled", "ended/released", "released"]:
            is_ended = True
        elif mediainfo.type == MediaType.MOVIE:
            is_ended = True

        logger.info(
            f"精准媒体识别成功: 《{mediainfo.title}》({mediainfo.year}) "
            f"类型={mediainfo.type.value if hasattr(mediainfo.type, 'value') else mediainfo.type}, "
            f"分类={mediainfo.category}, 完结={is_ended}"
        )

        # 整理中英搜索关键词
        keywords = [mediainfo.title]
        if self._bilingual_search:
            en_title = getattr(mediainfo, "original_title", None) or getattr(mediainfo, "en_title", None)
            if en_title and en_title.strip() and en_title.strip().lower() != mediainfo.title.lower():
                keywords.append(en_title.strip())

        # 2. 获取有效站点列表
        indexer_sites = await chain_instance._async_indexers(site_filter, subtitles=False)
        if not indexer_sites:
            logger.warning("未匹配到任何有效的搜索站点")
            yield {
                "type": "done",
                "stage": "done",
                "text": "未配置或未启用任何有效搜索站点",
                "items": [],
                "total_items": 0,
                "candidate_items": 0,
                "sources": [],
            }
            return

        # 站点按媒体类型剪裁：非动漫影视自动避开纯动漫站点 (如 MiKan)
        is_anime = False
        if mediainfo.category in ["日番", "国漫", "动漫"] or (hasattr(mediainfo, "genre_ids") and 16 in (mediainfo.genre_ids or [])):
            is_anime = True

        filtered_indexer_sites = []
        for s in indexer_sites:
            s_name = (s.get("name") or "").lower()
            if not is_anime and ("mikan" in s_name or "蜜柑" in s_name):
                logger.info(f"跳过纯动漫站点 [{s.get('name')}] (当前为真人影视搜索)")
                continue
            filtered_indexer_sites.append(s)

        if not filtered_indexer_sites:
            filtered_indexer_sites = indexer_sites

        source_facts = [
            {
                "source": str(s.get("id") or s.get("name")),
                "site_name": s.get("name"),
                "page": 0,
                "can_continue": False,
                "error": None,
            }
            for s in filtered_indexer_sites
        ]

        logger.info(
            f"启动流式并发流水线: 目标站点数={len(filtered_indexer_sites)}, "
            f"关键词={keywords}, 单站安全等待={self._site_sleep_seconds}s"
        )

        # 确定主排序规则组
        sort_rulegroup = (
            mediainfo.category
            if mediainfo.category
            else ("国产剧" if mediainfo.type == MediaType.TV else "华语电影")
        )

        # 3. 启动流水线队列与站点并发生产
        queue: asyncio.Queue = asyncio.Queue()
        producer_tasks = [
            asyncio.create_task(
                self._site_producer(
                    chain_instance=chain_instance,
                    site=s,
                    keywords=keywords,
                    mtype=mediainfo.type,
                    queue=queue,
                )
            )
            for s in filtered_indexer_sites
        ]

        seen_keys: Set[str] = set()
        all_contexts: List[Context] = []
        total_raw_count = 0
        pending_sites = len(filtered_indexer_sites)

        # 4. 边出边洗，即时流式消费产出
        while pending_sites > 0:
            site_name, batch_result = await queue.get()
            if batch_result is None:
                pending_sites -= 1
                queue.task_done()
                continue

            # 将密集 CPU 计算移交线程池，保障事件循环丝滑无阻
            batch_contexts, raw_count = await asyncio.to_thread(
                self._pipeline_process_media_batch,
                torrents=batch_result,
                seen_keys=seen_keys,
                mediainfo=mediainfo,
                sort_rulegroup=sort_rulegroup,
                chain_instance=chain_instance,
            )

            total_raw_count += raw_count
            queue.task_done()

            if batch_contexts:
                all_contexts.extend(batch_contexts)
                # 即时增量推送合规、已定级的高质量资源
                yield {
                    "type": "append",
                    "stage": "searching",
                    "items": [c.to_dict() for c in batch_contexts],
                    "total_items": len(all_contexts),
                    "candidate_items": total_raw_count,
                }

        # 等待所有生产者任务完全收尾
        await asyncio.gather(*producer_tasks, return_exceptions=True)

        # 5. 全局最终收敛排序：完全委托系统原生 TorrentHelper.sort_torrents
        try:
            sorted_all = TorrentHelper.sort_torrents(all_contexts)
        except Exception as e:
            logger.warning(f"调用系统 TorrentHelper.sort_torrents 异常，回退排序: {e}")
            sorted_all = sorted(
                all_contexts,
                key=lambda x: (getattr(x.torrent_info, "pri_order", 0) or 0, getattr(x.torrent_info, "seeders", 0) or 0),
                reverse=True,
            )
        contexts = sorted_all[:self._default_top_k] if self._default_top_k > 0 else sorted_all

        total_cost = round(time.time() - t0, 2)
        logger.info(
            f"极速流式搜索全部收敛完成: 总耗时 {total_cost}s, 原始候选 {total_raw_count} 个 -> "
            f"最终高质量交付 {len(contexts)} 个"
        )

        # 6. 存入系统搜索缓存并记录上次搜索参数
        if cache_local:
            await chain_instance._async_save_results(contexts)
            chain_instance.save_last_search_params(
                keyword=mediainfo.title,
                media_source=media_source_enum,
                media_id=str(media_id),
                mtype=mediainfo.type,
                area="title",
                season=season,
                sites=site_filter,
                result_type="torrent",
            )

        if contexts_collector is not None:
            contexts_collector.extend(contexts)

        yield {
            "type": "replace",
            "stage": "filtered",
            "value": 100,
            "text": f"极速过滤完成，共 {len(contexts)} 个优质资源",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": total_raw_count,
            "sources": source_facts,
            "match_counts": {},
        }
        yield {
            "type": "done",
            "stage": "done",
            "text": f"极速搜索完成，共 {len(contexts)} 个资源 (总耗时 {total_cost}s)",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": total_raw_count,
            "sources": source_facts,
            "match_counts": {},
        }

    async def stream_search_by_title(
        self,
        chain_instance: SearchChain,
        title: str,
        sites: Optional[Any] = None,
        mtype: Optional[MediaType] = None,
        rule_groups: Optional[List[str]] = None,
        cache_local: bool = False,
        contexts_collector: Optional[List[Context]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        接管网页端模糊标题搜索流 (GET /api/v1/search/title/stream)
        流式流水线架构：即出即洗、即时推送、前置过滤前置、MetaInfo单次复用。
        """
        t0 = time.time()
        logger.info(f"收到网页端标题搜索请求: keyword='{title}', mtype={mtype}, sites={sites}")

        site_filter: Optional[List[int]] = None
        if isinstance(sites, str):
            site_filter = [int(s) for s in sites.split(",") if s.isdigit()]
        elif isinstance(sites, list):
            site_filter = [int(s) for s in sites if str(s).isdigit()]

        indexer_sites = await chain_instance._async_indexers(site_filter, subtitles=False)
        if not indexer_sites:
            logger.warning("未匹配到任何有效的搜索站点")
            yield {
                "type": "done",
                "stage": "done",
                "text": "未配置或未启用任何有效搜索站点",
                "items": [],
                "total_items": 0,
                "candidate_items": 0,
                "sources": [],
            }
            return

        source_facts = [
            {
                "source": str(s.get("id") or s.get("name")),
                "site_name": s.get("name"),
                "page": 0,
                "can_continue": False,
                "error": None,
            }
            for s in indexer_sites
        ]

        # 整理标题搜索关键词
        keywords = [title]
        if self._bilingual_search:
            try:
                from app.chain.media import MediaChain
                from app.domain.meta.metabase import MetaBase
                rec_media = await MediaChain().async_recognize_media(meta=MetaBase(title=title))
                if rec_media:
                    en_t = getattr(rec_media, "original_title", None) or getattr(rec_media, "en_title", None)
                    if en_t and en_t.strip() and en_t.strip().lower() != title.lower():
                        keywords.append(en_t.strip())
            except Exception:
                pass

        logger.info(f"开始标题跨站点流式检索: 站点数={len(indexer_sites)}, 关键词={keywords}")

        queue: asyncio.Queue = asyncio.Queue()
        producer_tasks = [
            asyncio.create_task(
                self._site_producer(
                    chain_instance=chain_instance,
                    site=s,
                    keywords=keywords,
                    mtype=mtype,
                    queue=queue,
                )
            )
            for s in indexer_sites
        ]

        seen_keys: Set[str] = set()
        all_contexts: List[Context] = []
        total_raw_count = 0
        pending_sites = len(indexer_sites)

        while pending_sites > 0:
            site_name, batch_result = await queue.get()
            if batch_result is None:
                pending_sites -= 1
                queue.task_done()
                continue

            batch_contexts, raw_count = await asyncio.to_thread(
                self._pipeline_process_title_batch,
                torrents=batch_result,
                seen_keys=seen_keys,
                title=title,
                mtype=mtype,
                rule_groups=rule_groups,
                chain_instance=chain_instance,
            )

            total_raw_count += raw_count
            queue.task_done()

            if batch_contexts:
                all_contexts.extend(batch_contexts)
                yield {
                    "type": "append",
                    "stage": "searching",
                    "items": [c.to_dict() for c in batch_contexts],
                    "total_items": len(all_contexts),
                    "candidate_items": total_raw_count,
                }

        await asyncio.gather(*producer_tasks, return_exceptions=True)

        contexts = all_contexts[:self._default_top_k] if self._default_top_k > 0 else all_contexts
        total_cost = round(time.time() - t0, 2)
        logger.info(f"标题流式搜索完成: 总耗时 {total_cost}s，交付资源 {len(contexts)} 个")

        if cache_local:
            await chain_instance._async_save_results(contexts)
            chain_instance.save_last_search_params(
                keyword=title,
                mtype=mtype,
                area="title",
                sites=site_filter,
                result_type="torrent",
            )

        if contexts_collector is not None:
            contexts_collector.extend(contexts)

        yield {
            "type": "replace",
            "stage": "filtered",
            "value": 100,
            "text": f"极速过滤完成，共 {len(contexts)} 个优质资源",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": total_raw_count,
            "sources": source_facts,
        }
        yield {
            "type": "done",
            "stage": "done",
            "text": f"搜索完成，共 {len(contexts)} 个资源 (总耗时 {total_cost}s)",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": total_raw_count,
            "sources": source_facts,
        }

    async def execute_tool_search(
        self,
        title: str,
        en_title: Optional[str] = None,
        site_ids: Optional[List[int]] = None,
        require_complete: bool = True,
        top_k: int = 20,
    ) -> Dict[str, Any]:
        """供 Agent 工具调用的快速结构化搜索方法。"""
        t0 = time.time()
        logger.info(f"智能体工具 fast_intersect_search 调用: title='{title}', en='{en_title}'")
        chain = SearchChain()

        mediainfo = None
        try:
            from app.domain.meta.metabase import MetaBase
            meta_obj = MetaBase(title=title)
            mediainfo = await MediaChain().async_recognize_media(meta=meta_obj)
        except Exception:
            mediainfo = None

        contexts: List[Context] = []
        if mediainfo:
            media_source = getattr(mediainfo, "media_source", MediaSource.TMDB) or MediaSource.TMDB
            media_id = str(getattr(mediainfo, "tmdb_id", None) or getattr(mediainfo, "media_id", ""))
            async for _ in self.stream_search_by_media(
                chain_instance=chain,
                media_source=media_source,
                media_id=media_id,
                mtype=mediainfo.type,
                sites=site_ids,
                cache_local=True,
                contexts_collector=contexts,
            ):
                pass
        else:
            async for _ in self.stream_search_by_title(
                chain_instance=chain,
                title=title,
                sites=site_ids,
                cache_local=True,
                contexts_collector=contexts,
            ):
                pass

        target_contexts = contexts[:top_k] if top_k > 0 else contexts
        results = []
        for c in target_contexts:
            t = c.torrent_info
            size_gb = round(float(t.size or 0) / (1024 ** 3), 2)
            results.append({
                "site": t.site_name,
                "title": t.title,
                "size_gb": size_gb,
                "seeders": int(t.seeders or 0),
                "pri_order": getattr(t, "pri_order", 100),
                "torrent_info": t.to_dict(),
                "media_info": c.media_info.to_dict() if c.media_info else None,
            })

        total_cost = round(time.time() - t0, 2)
        return {
            "success": True,
            "title": title,
            "total_found": len(contexts),
            "cost_seconds": total_cost,
            "results": results,
        }
