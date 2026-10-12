"""极速搜索 V3: schedule network work, retain MoviePilot business decisions."""
import math

from app.sdk.plugin import _PluginBase
from app.log import logger


class FastIntersectSearch(_PluginBase):
    plugin_name = "极速搜索"
    plugin_desc = "跨站并发、同站默认 5 秒；过滤、排序和订阅下载沿用 MP。"
    plugin_version = "3.3.0"
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
        self._max_pages = 10
        self._names = True
        self._status = "未启用"

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
            self._max_pages = min(20, max(1, int(config.get("max_pages_per_site", 10) or 10)))
        except (TypeError, ValueError):
            self._max_pages = 10
        self._names = bool(config.get("bilingual_names", True))
        if not config.get("enabled", False):
            return
        try:
            # Old 3-second configs are deliberately migrated to the agreed minimum.
            interval = float(config.get("site_sleep_seconds", 3))
            if not math.isfinite(interval):
                raise ValueError("站点间隔必须是有限数字")
            from .compat import install
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

    def get_state(self):
        return self._enabled

    def stop_service(self):
        self._enabled = False
        if self._hooks is not None:
            self._hooks.close()
            self._hooks = None
        if type(self)._instance is self:
            type(self)._instance = None
        self._status = "未启用"

    def get_form(self):
        return [{"component": "VForm", "content": [
            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用极速搜索"}},
            {"component": "VSwitch", "props": {"model": "accelerate_new_subscribe", "label": "加速新订阅首次搜索"}},
            {"component": "VTextField", "props": {"model": "site_sleep_seconds", "label": "同站请求间隔（秒，从上次请求结束算起）", "type": "number", "min": 1}},
            {"component": "VSwitch", "props": {"model": "streaming_pipeline", "label": "边抓边洗（结果解析与抓取重叠）"}},
            {"component": "VSwitch", "props": {"model": "bilingual_names", "label": "中英搜索（只用原名/中文名/英文名，替代 MP 多名称展开）"}},
            {"component": "VTextField", "props": {"model": "max_pages_per_site", "label": "每站最多抓取页数（1-20）", "type": "number", "min": 1, "max": 20}},
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal"}, "text":
             "各站独立并发；解析与抓取重叠以缩短收尾等待。过滤、匹配、排序与下载决策仍由 MP 执行；保留 MP 更严格的站点与订阅限制。旧版双语、搜索超时、全集参数不再单独生效。"},
        ]}], {"enabled": False, "accelerate_new_subscribe": True, "site_sleep_seconds": 3,
              "streaming_pipeline": True, "max_pages_per_site": 10, "bilingual_names": True}

    def get_page(self):
        from .scheduling import SHARED_GATE
        return [
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal"}, "text": self._status},
            {"component": "VCardTitle", "text": "搜索耗时总览"},
            {"component": "VCardText", "text": "当前进程最近 72 小时、最多 5000 次请求样本。排队与实际调用耗时分开统计；异常数包含取消，不代表站点返回的业务错误总数。"},
            {"component": "VDataTable", "props": {"headers": [
                {"title": title, "key": key} for title, key in [
                    ("站点", "site"), ("请求数", "requests"), ("上次调用（秒）", "last"),
                    ("平均调用（秒）", "mean"), ("平均排队（秒）", "wait"), ("异常/取消", "errors")]],
                "items": SHARED_GATE.snapshot(), "items-per-page": 20}},
        ]

    def get_agent_tools(self):
        from .tools import FastIntersectSearchTool
        return [FastIntersectSearchTool]

    def get_api(self):
        return []
