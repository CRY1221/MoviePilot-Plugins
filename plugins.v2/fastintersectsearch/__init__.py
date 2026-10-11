"""
极速并发搜索插件 (FastIntersectSearch)
1. 跨站点并发检索，单站点内部多轮/翻页请求强制 3 秒安全频控；
2. 资源获取后移除简单字符串交集，直接在内存中调用系统 TorrentHelper.match_torrent 防抓错去噪；
3. 内存直接调用系统 chain.filter_torrents 走底层 Rust 引擎完成规则优先组评级筛选；
4. 对已完结影视整季全集包强力提权并排序。
"""
import asyncio
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Type
from urllib.parse import quote

import httpx
from app.agent.tools.base import MoviePilotTool, ToolTag
from app.application.torrent.download import TorrentHelper
from app.domain.context import MediaInfo, TorrentInfo
from app.domain.metainfo import MetaInfo
from app.log import logger
from app.runtime.config import settings
from app.schemas.types import MediaType
from app.sdk.plugin.base import _PluginBase


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
        # 兼容外层嵌套 kwargs/args 的 Agent 调用格式
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
        top_k = int(kwargs.get("top_k", 10))

        plugin = FastIntersectSearch.get_instance()
        if not plugin or not plugin.get_state():
            return {
                "success": False,
                "message": "极速搜索插件未安装或未启用",
            }

        return await plugin.async_search(
            title=title,
            en_title=en_title,
            site_ids=site_ids,
            require_complete=require_complete,
            top_k=top_k,
        )


class FastIntersectSearch(_PluginBase):
    """
    极速搜索主插件
    """

    plugin_name: str = "极速交叉搜索"
    plugin_desc: str = (
        "跨站并发+单站3秒频控防刷，内存直接调用系统模块识别去噪，"
        "原生规则链秒级评级，并对已完结影视全集包优先提权。"
    )
    plugin_version: str = "1.0.0"
    plugin_author: str = "CRY1221"
    plugin_order: int = 30

    _instance: Optional["FastIntersectSearch"] = None
    _enabled: bool = True
    _default_top_k: int = 10
    _auto_prioritize_complete: bool = True
    _search_timeout: int = 25
    _use_system_rules: bool = True
    _site_sleep_seconds: float = 3.0

    @classmethod
    def get_instance(cls) -> Optional["FastIntersectSearch"]:
        return cls._instance

    def init_plugin(self, config: dict = None) -> None:
        """根据配置初始化插件。"""
        FastIntersectSearch._instance = self
        config = config or {}
        self._enabled = bool(config.get("enabled", True))
        self._default_top_k = int(config.get("default_top_k", 10))
        self._auto_prioritize_complete = bool(config.get("auto_prioritize_complete", True))
        self._search_timeout = int(config.get("search_timeout", 25))
        self._use_system_rules = bool(config.get("use_system_rules", True))
        self._site_sleep_seconds = float(config.get("site_sleep_seconds", 3.0))

    def get_state(self) -> bool:
        """获取启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_agent_tools(self) -> List[Type[MoviePilotTool]]:
        if not self._enabled:
            return []
        return [FastIntersectSearchTool]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        form = [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "enabled",
                            "label": "启用极速并发搜索",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "use_system_rules",
                            "label": "应用 MoviePilot 系统过滤与优先级规则链",
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
                            "label": "同站点连续请求等待间隔 (秒，防刷频控)",
                            "type": "number",
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "default_top_k",
                            "label": "默认返回候选结果数量",
                            "type": "number",
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "search_timeout",
                            "label": "站点单次搜索超时时间 (秒)",
                            "type": "number",
                        },
                    },
                ],
            }
        ]
        default = {
            "enabled": True,
            "use_system_rules": True,
            "auto_prioritize_complete": True,
            "site_sleep_seconds": 3.0,
            "default_top_k": 10,
            "search_timeout": 25,
        }
        return form, default

    def get_page(self) -> Optional[List[dict]]:
        if not self._enabled:
            return None
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "success",
                    "text": (
                        "极速搜索插件已生效。支持跨站并发、单站3秒频控防封、"
                        "内存直接调用系统模块防抓错与Rust规则链秒级评级。"
                    ),
                },
            }
        ]

    def stop_service(self) -> None:
        pass

    async def _fetch_single_query(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        site_id: int,
        keyword: str,
    ) -> List[dict]:
        """异步请求单站点单次搜索。"""
        url = f"{base_url}/api/v1/search/title?keyword={quote(keyword)}&sites={site_id}"
        try:
            resp = await client.get(url, timeout=self._search_timeout)
            if resp.status_code == 200:
                data = resp.json().get("data")
                return data if isinstance(data, list) else []
        except Exception as exc:
            logger.debug(f"[FastIntersectSearch] 站点 {site_id} 搜索 {keyword} 异常: {exc}")
        return []

    async def _fetch_site_worker(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        site_id: int,
        keywords: List[str],
    ) -> List[dict]:
        """
        单个站点的工作协程：不同站点之间并发，但在该站点内依次执行关键词；
        若有多轮请求，强制间隔 self._site_sleep_seconds 秒。
        """
        site_items: List[dict] = []
        for idx, kw in enumerate(keywords):
            if idx > 0 and self._site_sleep_seconds > 0:
                logger.info(
                    f"[FastIntersectSearch] 站点 {site_id} 准备搜索第 {idx + 1} 个词 '{kw}'，"
                    f"安全等待 {self._site_sleep_seconds} 秒防刷..."
                )
                await asyncio.sleep(self._site_sleep_seconds)

            res = await self._fetch_single_query(client, base_url, site_id, kw)
            if res:
                site_items.extend(res)
        return site_items

    async def async_search(
        self,
        title: str,
        en_title: Optional[str] = None,
        site_ids: Optional[List[int]] = None,
        require_complete: bool = True,
        top_k: int = 10,
    ) -> Dict[str, Any]:
        """
        执行极速搜索：跨站并发、同站 3 秒频控、内存识别去噪、系统 Rust 规则过滤与完结提权。
        """
        t0 = time.time()
        base_url = f"http://127.0.0.1:{settings.PORT}"
        headers = {"X-API-KEY": settings.API_TOKEN}

        # 1. 识别媒体信息，确定类型、年份、别名与完结状态
        media_item: Optional[dict] = None
        is_ended = False
        media_category = ""
        media_type = ""
        media_year = ""
        media_names: List[str] = []

        try:
            async with httpx.AsyncClient(headers=headers, timeout=8) as meta_client:
                r = await meta_client.get(f"{base_url}/api/v1/media/search?title={quote(title)}&count=5")
                if r.status_code == 200:
                    candidates = r.json().get("data") or []
                    for c in candidates:
                        c_title = c.get("title") or ""
                        if c_title.lower() == title.lower() or title.lower() in c_title.lower():
                            media_item = c
                            break
                    if not media_item and candidates:
                        media_item = candidates[0]

                if media_item:
                    mid = media_item.get("tmdb_id") or media_item.get("media_id")
                    mtype = media_item.get("type") or "电影"
                    media_type = mtype
                    media_year = str(media_item.get("year") or "")
                    if not en_title:
                        en_title = media_item.get("original_title") or media_item.get("en_title")

                    r_detail = await meta_client.get(
                        f"{base_url}/api/v1/media/{mid}?media_source=themoviedb&type_name={quote(mtype)}"
                    )
                    if r_detail.status_code == 200:
                        detail_data = r_detail.json().get("data") or {}
                        media_category = detail_data.get("category") or ""
                        media_names = detail_data.get("names") or []
                        status_str = str(detail_data.get("status") or "").lower()
                        if status_str in ["ended", "canceled", "ended/released", "released"]:
                            is_ended = True
                        elif mtype == "电影":
                            is_ended = True
                        if not en_title:
                            en_title = detail_data.get("en_title") or detail_data.get("original_title")
        except Exception as e:
            logger.debug(f"[FastIntersectSearch] 查询媒体详情失败: {e}")

        # 整理要搜索的关键词列表（同站去重）
        search_keywords = [title.strip()]
        if en_title and en_title.strip() and en_title.strip().lower() != title.strip().lower():
            search_keywords.append(en_title.strip())

        # 2. 获取有效站点列表
        if not site_ids:
            try:
                async with httpx.AsyncClient(headers=headers, timeout=5) as site_client:
                    r = await site_client.get(f"{base_url}/api/v1/site/")
                    if r.status_code == 200:
                        all_sites = r.json().get("data") or []
                        site_ids = [s.get("id") for s in all_sites if s.get("is_active") and s.get("id")]
            except Exception:
                site_ids = []

        if not site_ids:
            site_ids = [1, 9, 8, 4, 2, 5, 3]

        # 3. 跨站点并发发起请求，单站点内部多轮串行（强制 3 秒频控）
        logger.info(
            f"[FastIntersectSearch] 开始跨站点并发搜索: 站点数={len(site_ids)}, "
            f"关键词={search_keywords}, 单站间隔={self._site_sleep_seconds}s"
        )
        fetch_start_time = time.time()
        async with httpx.AsyncClient(headers=headers) as search_client:
            tasks = [
                self._fetch_site_worker(search_client, base_url, sid, search_keywords)
                for sid in site_ids
            ]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

        fetch_cost = round(time.time() - fetch_start_time, 2)

        all_raw_items: List[dict] = []
        for res in fetch_results:
            if isinstance(res, list):
                all_raw_items.extend(res)

        # 4. 内存去重（按 enclosure 或 title）
        unique_map: Dict[str, dict] = {}
        for item in all_raw_items:
            t_info = item.get("torrent_info") or {}
            raw_title = t_info.get("title") or ""
            enclosure = t_info.get("enclosure") or ""
            key = enclosure if enclosure and not enclosure.startswith("[") else raw_title
            if not key:
                continue
            if key not in unique_map:
                unique_map[key] = item

        candidates_raw = list(unique_map.values())

        # 构造用于系统模块调用的 MediaInfo 对象
        m_type_enum = MediaType.TV if media_type == "电视剧" else MediaType.MOVIE
        target_mediainfo = MediaInfo(
            title=title,
            year=media_year,
            type=m_type_enum,
        )
        all_match_names = set(media_names)
        all_match_names.add(title)
        if en_title:
            all_match_names.add(en_title)
        target_mediainfo.names = list(all_match_names)

        # 5. 内存调用系统模块识别去噪（防抓错）
        # 将原始数据转化为系统 TorrentInfo 对象
        identified_torrents: List[Tuple[TorrentInfo, dict, bool]] = []
        for item in candidates_raw:
            raw_t = item.get("torrent_info") or {}
            torrent_obj = TorrentInfo(
                site=raw_t.get("site"),
                site_name=raw_t.get("site_name"),
                site_cookie=raw_t.get("site_cookie"),
                site_ua=raw_t.get("site_ua"),
                site_proxy=raw_t.get("site_proxy", False),
                site_order=raw_t.get("site_order", 0),
                title=raw_t.get("title") or "",
                description=raw_t.get("description") or "",
                enclosure=raw_t.get("enclosure"),
                page_url=raw_t.get("page_url"),
                size=raw_t.get("size") or 0,
                seeders=raw_t.get("seeders") or 0,
                peers=raw_t.get("peers") or 0,
                grabs=raw_t.get("grabs") or 0,
                pubdate=raw_t.get("pubdate"),
                category=raw_t.get("category"),
                labels=raw_t.get("labels") or [],
            )

            # 解析种子 meta
            meta = MetaInfo(title=torrent_obj.title, subtitle=torrent_obj.description)

            # 判断整季全集包特征
            full_text = f"{torrent_obj.title} {torrent_obj.description}".lower()
            is_complete = bool(
                re.search(r"\b(complete|全集|全\d+集|全\d+期|s01\s*complete)\b", full_text, re.IGNORECASE)
                or ("全" in full_text and "集" in full_text)
                or ("完结" in (torrent_obj.labels or []))
            )

            # 内存直接调用系统 TorrentHelper.match_torrent 进行识别去噪比对
            is_valid_work = TorrentHelper.match_torrent(
                mediainfo=target_mediainfo,
                torrent_meta=meta,
                torrent=torrent_obj,
            )

            # 如果 match_torrent 返回 False，但标题直接包含主标题，也允许保留防误杀
            if not is_valid_work:
                if title.lower() in full_text:
                    is_valid_work = True

            if is_valid_work:
                identified_torrents.append((torrent_obj, item, is_complete))

        # 6. 内存直接调用系统规则过滤链 (走系统 Rust 加速引擎)
        rulegroup_to_apply = media_category if media_category else ("国产剧" if media_type == "电视剧" else "华语电影")
        valid_torrent_objs = [t[0] for t in identified_torrents]

        filter_start_time = time.time()
        filtered_objs: List[TorrentInfo] = []
        if self._use_system_rules and valid_torrent_objs:
            try:
                # 内存直接调用 self.chain.filter_torrents
                filtered_objs = self.chain.filter_torrents(
                    rule_groups=[rulegroup_to_apply],
                    torrent_list=valid_torrent_objs,
                    mediainfo=target_mediainfo,
                ) or []
            except Exception as e:
                logger.error(f"[FastIntersectSearch] 内存调用系统规则过滤异常: {e}")
                filtered_objs = valid_torrent_objs
        else:
            filtered_objs = valid_torrent_objs

        filter_cost = round((time.time() - filter_start_time) * 1000, 2)
        logger.info(
            f"[FastIntersectSearch] 系统规则链内存过滤完成: 候选={len(valid_torrent_objs)}, "
            f"剩余={len(filtered_objs)}, 耗时={filter_cost}ms"
        )

        # 建立过滤后的映射集合
        filtered_ids = {id(obj): obj for obj in filtered_objs}

        # 7. 综合评级打分与完结提权
        scored_results = []
        for torrent_obj, raw_item, is_complete in identified_torrents:
            # 未通过系统规则则剔除
            if id(torrent_obj) not in filtered_ids:
                continue

            matched_obj = filtered_ids[id(torrent_obj)]
            # 系统优先级 pri_order 越小代表越优先 (例如 1 代表最顶层)
            pri_order = getattr(matched_obj, "pri_order", 100) or 100
            sys_score = max(0, 100 - pri_order + 1) * 10

            score = sys_score

            # 完结状态下的全集包强力提权
            if is_complete:
                if is_ended or require_complete:
                    score += 300  # 完结全集强力提权
                else:
                    score += 50

            # 做种数加分
            seeders = int(torrent_obj.seeders or 0)
            score += min(seeders, 30)

            # 体积计算
            size_gb = round(float(torrent_obj.size or 0) / (1024 ** 3), 2)

            scored_results.append({
                "score": score,
                "pri_order": pri_order,
                "is_complete": is_complete,
                "is_ended": is_ended,
                "site": torrent_obj.site_name,
                "title": torrent_obj.title,
                "size_gb": size_gb,
                "seeders": seeders,
                "torrent_info": raw_item.get("torrent_info"),
                "media_info": raw_item.get("media_info"),
            })

        # 按打分降序排序
        scored_results.sort(key=lambda x: x["score"], reverse=True)
        top_results = scored_results[: (top_k or self._default_top_k)]

        total_cost = round(time.time() - t0, 2)

        return {
            "success": True,
            "title": title,
            "en_title": en_title,
            "media_type": media_type,
            "category": media_category,
            "is_ended": is_ended,
            "sites_searched": len(site_ids),
            "total_raw_found": len(all_raw_items),
            "unique_found": len(candidates_raw),
            "identified_count": len(identified_torrents),
            "rule_passed_count": len(scored_results),
            "fetch_cost_seconds": fetch_cost,
            "filter_cost_ms": filter_cost,
            "total_cost_seconds": total_cost,
            "results": top_results,
        }
