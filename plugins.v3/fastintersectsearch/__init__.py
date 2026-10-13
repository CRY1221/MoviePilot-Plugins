"""极速搜索 V3: schedule network work, retain MoviePilot business decisions."""
import math

from app.sdk.plugin import _PluginBase
from app.sdk.logging import logger
from app.sdk.events import Event, eventmanager
from app.schemas.types import ChainEventType


class FastIntersectSearch(_PluginBase):
    plugin_name = "极速搜索"
    plugin_desc = ("跨站并发+同站默认3秒频控，站内结果边到边解析；"
                   "接管Web搜索、通知渠道点播与订阅搜索，"
                   "规则与排序沿用 MP，订阅按集下载可收敛到同一制作组。")
    plugin_version = "3.6.20"
    plugin_author = "CRY1221"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/search.png"
    plugin_order = 30
    author_url = "https://github.com/CRY1221"
    _instance = None

    def __init__(self):
        super().__init__()
        self._hooks = None
        self._enabled = False
        self._subscription = True
        self._streaming = True
        self._max_pages = 1
        self._names = True
        self._group_lock = True
        self._group_lock_apply = None
        self._interval = 3.0
        self._status = "未启用"
        self._test_progress = None

    @classmethod
    def get_instance(cls):
        return cls._instance

    def init_plugin(self, config=None):
        previous = type(self)._instance
        if previous is not None:
            previous.stop_service()
        self.stop_service()
        config = config or {}
        self._subscription = bool(config.get("accelerate_new_subscribe", True))
        self._streaming = bool(config.get("streaming_pipeline", True))
        try:
            self._max_pages = min(20, max(1, int(config.get("max_pages_per_site", 1) or 1)))
        except (TypeError, ValueError):
            self._max_pages = 1
        self._names = bool(config.get("bilingual_names", True))
        self._group_lock = bool(config.get("single_group_download", True))
        self._group_lock_apply = None
        if not config.get("enabled", False):
            return
        try:
            # Old 3-second configs are deliberately migrated to the agreed minimum.
            interval = float(config.get("site_sleep_seconds", 3))
            if not math.isfinite(interval):
                raise ValueError("站点间隔必须是有限数字")
            self._interval = max(1.0, interval)
            from app.application.torrent.download import TorrentHelper
            from app.chain.subscribe.facade import SubscribeChain
            from app.schemas.types import MediaType
            from .compat import install
            from .grouplock import make_group_lock
            self._group_lock_apply = make_group_lock(
                self._group_lock_enabled,
                TorrentHelper.sort_torrents, SubscribeChain.parse_subscribe_source_keyword,
                MediaType.TV.value, log=logger)
            self._hooks = install(self.get_state, lambda: self.get_state() and self._subscription,
                                  max(1.0, interval),
                                  lambda: self.get_state() and self._streaming,
                                  lambda: self._max_pages,
                                  lambda: self.get_state() and self._names)
            self._enabled = True
            type(self)._instance = self
            self._status = "已启用（本地调度；原生规则与下载决策）"
        except Exception as error:
            self.stop_service()
            self._status = f"未启用：{type(error).__name__}，请查看兼容性日志"
            logger.error(f"极速搜索适配安装失败：{error}")

    @eventmanager.register(ChainEventType.ResourceSelection)
    def on_resource_selection(self, event: Event = None):
        """把订阅下载的候选收敛到这一季该用的那一个制作组。

        走 MP 自己的扩展点：`app/chain/download/selection.py` 会回读 `updated_contexts`。
        不改规则、不改优先级、不改排序；认不出制作组的候选一律放行。
        """
        apply = self._group_lock_apply
        data = getattr(event, "event_data", None)
        if apply is None or data is None:
            return
        try:
            narrowed = apply(getattr(data, "origin", None),
                             list(getattr(data, "contexts", None) or []))
        except Exception as error:
            logger.warning(f"极速搜索制作组收敛失败，本轮不干预：{error}")
            return
        if narrowed is None:
            return
        data.updated = True
        data.updated_contexts = narrowed

    def get_state(self):
        return self._enabled

    def _group_lock_enabled(self):
        """一季同组跟着「接管订阅搜索」走。

        关掉订阅接管的人就是不想插件碰订阅，那就别去改他这一季下哪个组。
        """
        return self._enabled and self._subscription and self._group_lock

    def stop_service(self):
        self._enabled = False
        if self._hooks is not None:
            self._hooks.close()
            self._hooks = None
        if type(self)._instance is self:
            type(self)._instance = None
        self._status = "未启用"

    def get_form(self):
        """配置表单：只留需要用户决策的开关，其余按默认值运行。

        边抓边洗（`streaming_pipeline`）与一季同组（`single_group_download`）是插件的
        默认行为，不占用界面；一季同组没有自己的开关，它跟着「接管订阅搜索」走——
        不希望插件碰订阅的人，不会希望插件去改他这一季下哪个组。
        """
        def line(text, css="text-body-2"):
            return {"component": "div", "props": {"class": css}, "text": text}

        def column(width, *content):
            return {"component": "VCol", "props": {"cols": 12, "md": width, "class": "py-3"},
                    "content": list(content)}

        def switch(model, label, hint):
            return {"component": "VSwitch", "props": {
                "model": model, "label": label, "color": "primary",
                "hint": hint, "persistent-hint": True, "class": "mb-1"}}

        def number(model, label, hint, minimum=1, maximum=None):
            props = {"model": model, "label": label, "type": "number", "min": minimum,
                     "hint": hint, "persistent-hint": True, "class": "mb-1"}
            if maximum is not None:
                props["max"] = maximum
            return {"component": "VTextField", "props": props}

        intro = {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                                                  "class": "mt-1 mb-3"}, "content": [
            line("⚡ 插件介绍", "text-subtitle-1 font-weight-bold mb-2"),
            line("启用后接管 MoviePilot 的搜索链路：Web 网页端搜索、通知渠道点播"
                 "（Telegram / 微信机器人），以及可选的订阅搜索与订阅资源检查。"
                 "所有搜索都按「跨站并发抓取 + 同站固定间隔 + 每站结果边到边解析」执行，"
                 "再交给 MP 原生的过滤与排序。音乐搜索完全放行，不做任何干预。",
                 "text-body-2 mb-4"),
            line("⚖️ 规则与排序", "text-subtitle-1 font-weight-bold mb-2"),
            line("过滤、匹配、排序严格沿用【设定 → 规则 → 优先级规则组】：本插件不改规则、"
                 "不改优先级、不复活被拒资源。建议先校准好优先级规则组，排序结果才合心意。",
                 "text-body-2 mb-4"),
            line("🛡️ 兼容性与失败表现", "text-subtitle-1 font-weight-bold mb-2"),
            line("插件逐个校验所依赖的 MP 内部接口签名。任一处对不上，插件会自动停用并在日志里"
                 "写明原因，不会带着错误的假设继续跑；总开关关闭时不安装任何补丁。"),
        ]}

        form = [{"component": "VForm", "content": [
            {"component": "VRow", "content": [
                column(6, switch("enabled", "启用极速搜索",
                                 "总开关。关闭后不安装任何补丁，搜索行为与 MP 原生完全一致。")),
            ]},
            {"component": "VRow", "content": [column(12, intro)]},
            {"component": "VDivider", "props": {"class": "my-6"}},
            {"component": "VRow", "content": [
                column(6, number("site_sleep_seconds", "同站请求间隔（秒）",
                                 "默认 3，从上次请求结束算起。调小更快，但更容易被站点限流或临时封禁。")),
                column(6, number("max_pages_per_site", "每站最多抓取页数（1-20）",
                                 "默认 1，即只搜首页。调大能翻到更多资源，代价是更慢、对站点请求更多；"
                                 "站点没有更多结果时会自动停，不会空抓。", maximum=20)),
            ]},
            {"component": "VDivider", "props": {"class": "my-6"}},
            {"component": "VRow", "content": [
                column(12, line("可选功能", "text-subtitle-2 font-weight-bold")),
            ]},
            {"component": "VRow", "content": [
                column(6, switch("bilingual_names", "中英搜索",
                                 "开启时只用原名、中文名、英文名搜索（最多 3 个）；"
                                 "关闭时按 MP 系统设置展开全部别名。"
                                 "显式关键词与音乐搜索不受影响。")),
                column(6, switch("accelerate_new_subscribe", "接管订阅搜索",
                                 "接管订阅的搜索与下载：并发预取各站待请求页，首次搜索更快；"
                                 "订阅按集下载时把候选收敛到同一个制作组，不混组。"
                                 "不希望插件碰订阅的话可以关掉，订阅这一块完全交回 MP。")),
            ]},
        ]}]
        return form, {"enabled": False, "site_sleep_seconds": 3, "max_pages_per_site": 1,
                      "bilingual_names": True, "accelerate_new_subscribe": True}

    def get_page(self):
        """插件详情页：沿用旧版黑曜石玻璃拟态耗时大盘，数据取自本进程实时统计。"""
        from .page import render
        from .scheduling import SHARED_GATE
        summary = (f"同站间隔 {self._interval:.0f} 秒（从上次请求结束起算）｜每站最多 {self._max_pages} 页｜"
                   f"接管订阅搜索 {'开' if self._subscription else '关'}")
        return render(self._status, SHARED_GATE.snapshot(), self._site_icon_map(), summary)

    def _site_icon_map(self):
        """站点图标（Base64/URL），与旧版一致，取自站点图标表。"""
        icons = {}
        try:
            from app.db.models.siteicon import SiteIcon
            from app.db import get_db
            for item in next(get_db()).query(SiteIcon).all():
                if item.name and item.base64:
                    icons[item.name] = item.base64
                elif item.name and item.url:
                    icons[item.name] = item.url
        except Exception:
            pass
        return icons

    def get_agent_tools(self):
        from .tools import FastIntersectSearchTool
        return [FastIntersectSearchTool]

    def get_api(self):
        """对外暴露自检接口，用于在应用进程内跑一次搜索并取回统计。"""
        return [{
            "path": "/selftest",
            "endpoint": self.api_selftest,
            "methods": ["GET"],
            "auth": "apikey",
            "summary": "极速搜索自检",
            "description": "在容器进程内执行一次网页搜索，返回耗时、候选/结果数量与各站点请求次数。",
        }, {"path": "/rulecheck", "endpoint": self.api_rulecheck,
            "methods": ["GET"], "auth": "apikey", "summary": "规则兼容性只读自检"},
            {"path": "/diagnostics", "endpoint": self.api_diagnostics,
             "methods": ["GET"], "auth": "apikey", "summary": "自检进度与解析线程状态"}]

    def api_rulecheck(self):
        from .selftest import check_rules
        return check_rules()

    def api_diagnostics(self):
        import sys
        import threading
        from pathlib import Path
        from .streaming import LAST_COMPARISON, compare_contexts
        frames = sys._current_frames()
        workers = []
        for thread in threading.enumerate():
            if not thread.name.startswith("fastsearch-"):
                continue
            frame, stack = frames.get(thread.ident), []
            while frame is not None and len(stack) < 12:
                stack.append({"file": Path(frame.f_code.co_filename).name,
                              "function": frame.f_code.co_name, "line": frame.f_lineno})
                frame = frame.f_back
            workers.append({"worker": thread.name, "stack": stack})
        return {"success": True, "data": {"progress": self._test_progress,
                                           "last_result": self.get_data("last_selftest"), "workers": workers,
                                           "last_comparison": compare_contexts(*LAST_COMPARISON) if LAST_COMPARISON else None}}

    async def api_selftest(self, media_id: str = "758891", mtype: str = "电影",
                           media_source: str = "themoviedb", site_id: int | None = None, verify: bool = False,
                           verify_filter: bool = True):
        """自检：跑一次搜索，并给出这次搜索新增的站点请求统计。"""
        from . import selftest
        from .scheduling import SHARED_GATE
        before = {item["site"]: item["requests"] for item in SHARED_GATE.snapshot()}
        from .rulecompat import VALIDATION, VERIFY_FILTER
        checks = [] if verify else None
        token = VALIDATION.set(checks)
        filter_token = VERIFY_FILTER.set(verify_filter)
        self._test_progress = {"status": "running"}
        try:
            result = await selftest.run_search(media_id=media_id, mtype=mtype, media_source=media_source, site_id=site_id,
                                              observe=lambda event: setattr(self, "_test_progress", {"status": "running", **event}))
        except BaseException as error:
            self._test_progress = {"status": "failed", "error": type(error).__name__}
            raise
        finally:
            VALIDATION.reset(token)
            VERIFY_FILTER.reset(filter_token)
        if checks is not None:
            result["validation"] = checks
            result["equivalent"] = bool(checks) and all(check["equivalent"] for check in checks)
        result["sites"] = [
            {**item, "requests": item["requests"] - before.get(item["site"], 0)}
            for item in SHARED_GATE.snapshot()
            if item["requests"] - before.get(item["site"], 0) > 0
        ]
        response = {"success": result["status"] == "complete" and result.get("equivalent", True), "data": result}
        self.save_data("last_selftest", response)
        self._test_progress = {"status": result["status"]}
        return response
