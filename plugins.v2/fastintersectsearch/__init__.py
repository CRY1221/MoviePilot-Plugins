"""
极速并发搜索插件 (FastIntersectSearch)
1. 无缝接管系统网页前端搜索流 (SearchChain.search_page_events / async_search_by_id_stream / async_search_by_title_stream)；
2. 跨站点并发检索，单站点内部多轮请求强制 3 秒安全频控防封；
3. 内存直接调用系统 TorrentHelper.match_torrent 防抓错消歧；
4. 内存直接调用系统 FilterChain 走底层 Rust 引擎完成规则优先组打分与体积/码率红线过滤；
5. 自动识别 TMDB 完结状态，对整季全集包强力提权并排序；
6. 独立日志系统，写入 plugins/fastintersectsearch.log，前端实时日志可直接查看。
"""
import asyncio
import contextvars
import re
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type
from urllib.parse import quote

import httpx
from app.agent.tools.base import MoviePilotTool, ToolTag
from app.application.torrent.download import TorrentHelper
from app.chain.media import MediaChain
from app.chain.search.facade import SearchChain
from app.domain.context import Context, MediaInfo, TorrentInfo
from app.domain.metainfo import MetaInfo
from app.log import logger
from app.runtime.config import settings
from app.schemas.types import MediaSource, MediaType
from app.sdk.plugin.base import _PluginBase

# 用于追踪防止递归调用的上下文标记
_in_fastsearch_context = contextvars.ContextVar("_in_fastsearch_context", default=False)


class FastIntersectSearchTool(MoviePilotTool):
    """
    极速并发搜索工具：跨站点并发检索，单站强制 3 秒安全频控；
    内存直接调用系统模块完成识别防抓错与规则优先组过滤，自动对已完结影视全集包提权排序。
    """

    name: str = "fast_intersect_search"
    description: str = (
        "极速并发搜索工具：跨站点并发检索，单站点多轮请求强制等待 3 秒防刷；"
        "内存直接调用系统 TorrentHelper 完成识别去噪与防抓错，"
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
    极速搜索主插件：实现网页前端搜索接管与独立日志记录。
    """

    plugin_name: str = "极速交叉搜索"
    plugin_desc: str = (
        "跨站并发+单站3秒频控防刷，全面接管网页前端搜索流，"
        "内存识别去噪，Rust规则链秒级评级，完结全集包优先提权。"
    )
    plugin_version: str = "1.1.0"
    plugin_author: str = "CRY1221"
    plugin_order: int = 30

    _instance: Optional["FastIntersectSearch"] = None
    _enabled: bool = True
    _takeover_web_search: bool = True
    _default_top_k: int = 30
    _auto_prioritize_complete: bool = True
    _search_timeout: int = 20
    _use_system_rules: bool = True
    _site_sleep_seconds: float = 3.0

    # 原始 SearchChain 方法句柄备份
    _orig_search_page_events = None
    _orig_async_search_by_id_stream = None
    _orig_async_search_by_title_stream = None
    _orig_async_search_by_id = None
    _orig_async_search_by_title = None

    @classmethod
    def get_instance(cls) -> Optional["FastIntersectSearch"]:
        """获取当前插件活跃单例。"""
        return cls._instance

    def init_plugin(self, config: dict = None) -> None:
        """根据配置初始化插件并装配或卸载搜索接管 Hook。"""
        FastIntersectSearch._instance = self
        config = config or {}
        self._enabled = bool(config.get("enabled", True))
        self._takeover_web_search = bool(config.get("takeover_web_search", True))
        self._default_top_k = int(config.get("default_top_k", 30))
        self._auto_prioritize_complete = bool(config.get("auto_prioritize_complete", True))
        self._search_timeout = int(config.get("search_timeout", 20))
        self._use_system_rules = bool(config.get("use_system_rules", True))
        self._site_sleep_seconds = float(config.get("site_sleep_seconds", 3.0))

        # 备份并安装/卸载系统 SearchChain 钩子
        self._backup_original_methods()
        if self._enabled and self._takeover_web_search:
            self._install_hooks()
            logger.info("已成功接管 MoviePilot 系统 SearchChain 搜索流（网页端搜索全面生效极速并发）")
        else:
            self._uninstall_hooks()
            logger.info("已恢复 MoviePilot 原生搜索流")

        logger.info(
            f"极速搜索插件初始化完成: enabled={self._enabled}, "
            f"接管网页端={self._takeover_web_search}, 单站间隔={self._site_sleep_seconds}s, "
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
                    cache_local=bool(cache_local),
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
            """接管非流式精准媒体搜索。"""
            if music_type or (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                return await plugin._orig_async_search_by_id(
                    chain_self, media_source, media_id, mtype=mtype, area=area,
                    season=season, sites=sites, cache_local=cache_local,
                    music_type=music_type, include_candidates=include_candidates,
                )
            results = []
            async for event in hooked_async_search_by_id_stream(
                chain_self, media_source, media_id, mtype=mtype, area=area,
                season=season, sites=sites, cache_local=cache_local,
                music_type=music_type, include_candidates=include_candidates,
            ):
                if event.get("type") == "done":
                    results = event.get("contexts") or []
            return results

        async def hooked_async_search_by_title(
            chain_self,
            title: str,
            page: Optional[int] = 0,
            sites: Optional[List[int]] = None,
            cache_local: Optional[bool] = False,
            mtype: Optional[MediaType] = None,
            rule_groups: Optional[List[str]] = None,
        ) -> List[Context]:
            """接管非流式模糊标题搜索。"""
            if (mtype and mtype == MediaType.MUSIC) or _in_fastsearch_context.get():
                return await plugin._orig_async_search_by_title(
                    chain_self, title=title, page=page, sites=sites,
                    cache_local=cache_local, mtype=mtype, rule_groups=rule_groups,
                )
            results = []
            async for event in hooked_async_search_by_title_stream(
                chain_self, title=title, page=page, sites=sites,
                cache_local=cache_local, mtype=mtype, rule_groups=rule_groups,
            ):
                if event.get("type") == "done":
                    results = event.get("contexts") or []
            return results

        SearchChain.search_page_events = hooked_search_page_events
        SearchChain.async_search_by_id_stream = hooked_async_search_by_id_stream
        SearchChain.async_search_by_title_stream = hooked_async_search_by_title_stream
        SearchChain.async_search_by_id = hooked_async_search_by_id
        SearchChain.async_search_by_title = hooked_async_search_by_title

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
                            "label": "启用极速并发搜索插件",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "takeover_web_search",
                            "label": "全局接管网页端搜索（使网页前端搜索秒级响应）",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "use_system_rules",
                            "label": "应用 MoviePilot 系统过滤与优先级规则链 (Rust引擎)",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "auto_prioritize_complete",
                            "label": "影视完结时自动提权整季全集包 (Complete)",
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
                            "model": "default_top_k",
                            "label": "搜索结果最大呈现数量 (默认 30)",
                            "type": "number",
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "search_timeout",
                            "label": "站点单次搜索超时时间 (秒，默认 20)",
                            "type": "number",
                        },
                    },
                ],
            }
        ]
        default = {
            "enabled": True,
            "takeover_web_search": True,
            "use_system_rules": True,
            "auto_prioritize_complete": True,
            "site_sleep_seconds": 3.0,
            "default_top_k": 30,
            "search_timeout": 20,
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
                        "极速搜索插件已全局生效。跨站点异步并发、同站 3 秒频控防封、"
                        "已全面接管网页前端搜索流，支持系统 Rust 规则链秒级评级及完结全集包提权。"
                    ),
                },
            }
        ]

    def stop_service(self) -> None:
        """卸载钩子并停止服务。"""
        self._uninstall_hooks()
        logger.info("极速搜索插件后台服务已停止，系统搜索流已复原")

    async def _search_site_worker(
        self,
        chain_instance: SearchChain,
        site: dict,
        keywords: List[str],
        mtype: Optional[MediaType] = None,
    ) -> List[TorrentInfo]:
        """
        单站点内部工作协程：
        按关键词顺序检索，若触发多轮检索，强制等待 self._site_sleep_seconds 秒。
        """
        site_name = site.get("name") or str(site.get("id"))
        site_torrents: List[TorrentInfo] = []

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
                # 设置超时，防止个别 PT 站卡死拖慢整个并发流水线
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
                    site_torrents.extend(results)
            except asyncio.TimeoutError:
                logger.warning(f"站点 [{site_name}] 检索 '{kw}' 超时 ({self._search_timeout}s)")
            except Exception as exc:
                logger.warning(f"站点 [{site_name}] 检索 '{kw}' 异常: {exc}")

        return site_torrents

    async def stream_search_by_media(
        self,
        chain_instance: SearchChain,
        media_source: Any,
        media_id: str,
        mtype: Optional[MediaType] = None,
        season: Optional[int] = None,
        sites: Optional[Any] = None,
        cache_local: bool = False,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        接管网页端精准媒体搜索流 (GET /api/v1/search/media/{media_id}/stream)
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

        logger.info(
            f"开始跨站点并发检索: 目标站点数={len(indexer_sites)}, "
            f"关键词={keywords}, 单站安全等待={self._site_sleep_seconds}s"
        )

        # 3. 跨站点并发检索，实时流式推送初始 append 事件
        raw_torrents: List[TorrentInfo] = []
        contexts: List[Context] = []

        async def worker_wrapper(site_item):
            return await self._search_site_worker(
                chain_instance=chain_instance,
                site=site_item,
                keywords=keywords,
                mtype=mediainfo.type,
            )

        tasks = [asyncio.create_task(worker_wrapper(s)) for s in indexer_sites]

        # 边完成边发布中间状态
        for completed_task in asyncio.as_completed(tasks):
            site_res = await completed_task
            if site_res:
                raw_torrents.extend(site_res)
                # 构造临时 Context 供网页端展示即时进度
                temp_contexts = [
                    Context(
                        torrent_info=t,
                        media_info=mediainfo,
                        meta_info=MetaInfo(title=t.title, subtitle=t.description),
                    )
                    for t in site_res
                ]
                yield {
                    "type": "append",
                    "stage": "searching",
                    "items": [c.to_dict() for c in temp_contexts],
                    "total_items": len(raw_torrents),
                    "candidate_items": len(raw_torrents),
                }

        fetch_cost = round(time.time() - t0, 2)
        logger.info(f"所有站点检索完毕，耗时 {fetch_cost}s，累计获取原始种子 {len(raw_torrents)} 个")

        # 4. 内存去重
        unique_map: Dict[str, TorrentInfo] = {}
        for t in raw_torrents:
            key = t.enclosure if t.enclosure and not t.enclosure.startswith("[") else t.title
            if key and key not in unique_map:
                unique_map[key] = t
        unique_torrents = list(unique_map.values())

        # 5. 内存调用系统 TorrentHelper.match_torrent 进行识别去噪比对
        identified_torrents: List[Tuple[TorrentInfo, bool]] = []
        for t in unique_torrents:
            meta = MetaInfo(title=t.title, subtitle=t.description)
            full_text = f"{t.title} {t.description}".lower()
            is_complete = bool(
                re.search(r"\b(complete|全集|全\d+集|全\d+期|s01\s*complete)\b", full_text, re.IGNORECASE)
                or ("全" in full_text and "集" in full_text)
                or ("完结" in (t.labels or []))
            )
            is_valid_work = TorrentHelper.match_torrent(
                mediainfo=mediainfo,
                torrent_meta=meta,
                torrent=t,
            )
            if not is_valid_work and mediainfo.title.lower() in full_text:
                is_valid_work = True
            if is_valid_work:
                identified_torrents.append((t, is_complete))

        logger.info(
            f"内存识别消歧完成: 唯一候选 {len(unique_torrents)} 个 -> "
            f"识别吻合 {len(identified_torrents)} 个，剔除噪音 {len(unique_torrents) - len(identified_torrents)} 个"
        )

        # 6. 内存直接调用系统规则过滤链 (走系统 Rust 加速引擎)
        rulegroup_to_apply = (
            mediainfo.category
            if mediainfo.category
            else ("国产剧" if mediainfo.type == MediaType.TV else "华语电影")
        )
        valid_torrent_objs = [item[0] for item in identified_torrents]

        t_filter = time.time()
        filtered_objs: List[TorrentInfo] = []
        if self._use_system_rules and valid_torrent_objs:
            try:
                filtered_objs = chain_instance.filter_torrents(
                    rule_groups=[rulegroup_to_apply],
                    torrent_list=valid_torrent_objs,
                    mediainfo=mediainfo,
                ) or []
            except Exception as e:
                logger.error(f"调用系统 Rust 规则过滤异常: {e}")
                filtered_objs = valid_torrent_objs
        else:
            filtered_objs = valid_torrent_objs

        filter_cost = round((time.time() - t_filter) * 1000, 2)
        logger.info(
            f"系统规则链 (Rust引擎) 过滤完成: 规则组='{rulegroup_to_apply}', "
            f"入选 {len(filtered_objs)}/{len(valid_torrent_objs)}, 耗时 {filter_cost}ms"
        )

        filtered_ids = {id(obj): obj for obj in filtered_objs}

        # 7. 打分排序与完结提权
        scored_contexts: List[Tuple[float, Context]] = []
        for t_obj, is_complete in identified_torrents:
            if id(t_obj) not in filtered_ids:
                continue

            matched_obj = filtered_ids[id(t_obj)]
            pri_order = getattr(matched_obj, "pri_order", 100) or 100
            t_obj.pri_order = pri_order

            score = max(0, 100 - pri_order + 1) * 10
            if is_complete:
                if is_ended or self._auto_prioritize_complete:
                    score += 300
                else:
                    score += 50
            seeders = int(t_obj.seeders or 0)
            score += min(seeders, 30)

            ctx = Context(
                torrent_info=t_obj,
                media_info=mediainfo,
                meta_info=MetaInfo(title=t_obj.title, subtitle=t_obj.description),
            )
            scored_contexts.append((score, ctx))

        scored_contexts.sort(key=lambda x: x[0], reverse=True)
        contexts = [item[1] for item in scored_contexts[:self._default_top_k]]

        total_cost = round(time.time() - t0, 2)
        logger.info(f"极速搜索全部收敛完成，总耗时 {total_cost}s，最终向前端交付高质量种子 {len(contexts)} 个")

        # 8. 存入系统搜索缓存并记录上次搜索参数
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

        yield {
            "type": "replace",
            "stage": "filtered",
            "value": 100,
            "text": f"极速过滤完成，共 {len(contexts)} 个优质资源",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": len(raw_torrents),
            "sources": source_facts,
            "match_counts": {},
        }
        yield {
            "type": "done",
            "stage": "done",
            "contexts": contexts,
            "text": f"极速搜索完成，共 {len(contexts)} 个资源 (总耗时 {total_cost}s)",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": len(raw_torrents),
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
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        接管网页端模糊标题搜索流 (GET /api/v1/search/title/stream)
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

        logger.info(f"开始标题跨站点并发检索: 站点数={len(indexer_sites)}, 关键词='{title}'")

        raw_torrents: List[TorrentInfo] = []

        async def worker_wrapper(site_item):
            return await self._search_site_worker(
                chain_instance=chain_instance,
                site=site_item,
                keywords=[title],
                mtype=mtype,
            )

        tasks = [asyncio.create_task(worker_wrapper(s)) for s in indexer_sites]

        for completed_task in asyncio.as_completed(tasks):
            site_res = await completed_task
            if site_res:
                raw_torrents.extend(site_res)
                temp_contexts = [
                    Context(
                        torrent_info=t,
                        media_info=None,
                        meta_info=MetaInfo(title=t.title, subtitle=t.description, mtype=mtype),
                    )
                    for t in site_res
                ]
                yield {
                    "type": "append",
                    "stage": "searching",
                    "items": [c.to_dict() for c in temp_contexts],
                    "total_items": len(raw_torrents),
                    "candidate_items": len(raw_torrents),
                }

        # 内存去重
        unique_map: Dict[str, TorrentInfo] = {}
        for t in raw_torrents:
            key = t.enclosure if t.enclosure and not t.enclosure.startswith("[") else t.title
            if key and key not in unique_map:
                unique_map[key] = t
        unique_torrents = list(unique_map.values())

        # 规则链过滤
        if self._use_system_rules and unique_torrents:
            try:
                filtered_torrents = chain_instance.filter_torrents(
                    rule_groups=rule_groups or [],
                    torrent_list=unique_torrents,
                    mediainfo=None,
                ) or []
            except Exception as e:
                logger.error(f"标题搜索规则过滤异常: {e}")
                filtered_torrents = unique_torrents
        else:
            filtered_torrents = unique_torrents

        contexts = [
            Context(
                torrent_info=t,
                media_info=None,
                meta_info=MetaInfo(title=t.title, subtitle=t.description, mtype=mtype),
            )
            for t in filtered_torrents[:self._default_top_k]
        ]

        total_cost = round(time.time() - t0, 2)
        logger.info(f"标题搜索完成，总耗时 {total_cost}s，交付资源 {len(contexts)} 个")

        if cache_local:
            await chain_instance._async_save_results(contexts)
            chain_instance.save_last_search_params(
                keyword=title,
                mtype=mtype,
                area="title",
                sites=site_filter,
                result_type="torrent",
            )

        yield {
            "type": "replace",
            "stage": "filtered",
            "value": 100,
            "text": f"极速过滤完成，共 {len(contexts)} 个优质资源",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": len(raw_torrents),
            "sources": source_facts,
        }
        yield {
            "type": "done",
            "stage": "done",
            "contexts": contexts,
            "text": f"搜索完成，共 {len(contexts)} 个资源 (总耗时 {total_cost}s)",
            "items": [c.to_dict() for c in contexts],
            "total_items": len(contexts),
            "candidate_items": len(raw_torrents),
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

        # 尝试查询 TMDB 媒体信息
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
            async for evt in self.stream_search_by_media(
                chain_instance=chain,
                media_source=media_source,
                media_id=media_id,
                mtype=mediainfo.type,
                sites=site_ids,
                cache_local=True,
            ):
                if evt.get("type") == "done":
                    contexts = evt.get("contexts") or []
        else:
            async for evt in self.stream_search_by_title(
                chain_instance=chain,
                title=title,
                sites=site_ids,
                cache_local=True,
            ):
                if evt.get("type") == "done":
                    contexts = evt.get("contexts") or []

        results = []
        for c in contexts[:top_k]:
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
