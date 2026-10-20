"""
番号云下载插件。

按番号从 JavBus 取种，经自建 CloudDrive2 gRPC 接入提交 115 云下载，
落盘后清理广告文件、重命名并按配置移动到库目录。
"""

from __future__ import annotations

import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from app.plugins import _PluginBase
from app.sdk.logging import logger
from app.schemas.types import MessageType

from .cd2 import Cd2Client, Cd2Error, status_name
from .javdb import JavDBError, JavDBSource
from .javranking import JavRankingError, JavRankingSource
from .library import (
    EmbyLibrary,
    actual_is_better,
    candidate_may_be_better,
    estimate_candidate_quality,
    probe_media,
    quality_from_item,
)
from .pipeline import (
    ACTIVE_STATES,
    STATE_DOWNLOADING,
    STATE_FAILED,
    STATE_FINISHED,
    STATE_MOVED,
    STATE_ORGANIZED,
    STATE_SKIPPED,
    STATE_SUBMITTED,
    TERMINAL_STATES,
    dir_has_video,
    locate_download_dir,
    move_dir,
    remove_empty_dirs,
    replace_existing_files,
    safe_name,
    tidy_dir,
)
from .selector import DEFAULT_MIN_VIDEO_SIZE, has_chinese_sub, select
from .source import JavBusSource, JavBusError, info_hash_of, normalize_code

MAX_RECORDS = 200

# 待提交队列：只存番号与批次，任务记录在真正提交时创建
MAX_QUEUE = 1000
QUEUE_BATCH = 5
BATCH_FAILURE_DISPLAY = 20

# 整理阶段连续失败多少次后停止重试
MAX_ORGANIZE_ATTEMPTS = 5

# 删除旧离线任务连续失败多少次后停止换种
MAX_REMOVE_ATTEMPTS = 3

DEFAULT_POLL_SECONDS = 10
MIN_POLL_SECONDS = 5
DEFAULT_CANDIDATE_TIMEOUT_MINUTES = 5
DEFAULT_QUEUE_TIMEOUT_MINUTES = 30
PROACTIVE_BATCH = 5

# 插件重载时可能短暂出现两个服务实例，用模块级锁和通知去重兜底
_ADVANCE_LOCK = threading.Lock()
_NOTIFY_LOCK = threading.Lock()
_PROACTIVE_LOCK = threading.Lock()
_NOTIFIED: set = set()


def _already_notified(kind: str, record: Dict[str, Any]) -> bool:
    """同一任务同一候选只发一次同类通知。"""
    key = (kind, record.get("code"), record.get("info_hash") or "")
    with _NOTIFY_LOCK:
        if key in _NOTIFIED:
            return True
        _NOTIFIED.add(key)
        if len(_NOTIFIED) > 2000:
            _NOTIFIED.clear()
            _NOTIFIED.add(key)
        return False
DEFAULT_MAX_CANDIDATES = 3


class JavCloudDownload(_PluginBase):
    """番号云下载插件主类。"""

    plugin_name = "番号云下载"
    plugin_desc = "按番号从 JavBus 取种，经 CloudDrive2 云下载到 115，完成后清理、改名并可移动到库目录。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    plugin_version = "0.9.1"
    plugin_label = "工具"
    plugin_author = "CRY1221"
    plugin_config_prefix = "javclouddownload_"
    plugin_order = 60
    auth_level = 1

    _enabled = False
    _source_base = "https://www.javbus.com"
    _cd2_host = ""
    _cd2_port = 19798
    _cd2_token = ""
    _cd2_username = ""
    _cd2_password = ""
    _download_dir = ""
    _library_dir = ""
    _move_enabled = False
    _allow_uncensored = False
    _allow_junk = False
    _delete_junk_only = True
    _poll_seconds = DEFAULT_POLL_SECONDS
    _candidate_timeout_minutes = DEFAULT_CANDIDATE_TIMEOUT_MINUTES
    _candidate_queue_timeout_minutes = DEFAULT_QUEUE_TIMEOUT_MINUTES
    _max_candidates = DEFAULT_MAX_CANDIDATES
    _max_wait_minutes = 240
    _source_mode = "javbus"
    _use_javbus = True
    _use_javranking = False
    _use_javdb = False
    _javdb_cookie = ""
    _javranking_dir = ""
    _emby_library = ""
    _library_map = ""
    _emby_options_at = 0.0
    _emby_options: List[dict] = []
    _enable_upgrade = False
    _replace_existing = True
    _proactive_enabled = False
    _proactive: Dict[str, Any] = {}
    _enable_filter_rules = True
    _min_video_size_mb = 2048
    _min_video_size_bytes = 2048 * 1024 ** 2
    _status = "未启用"

    def __init__(self) -> None:
        """初始化实例状态。"""
        super().__init__()
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._queue: List[Dict[str, Any]] = []
        self._batches: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._poll_lock = threading.Lock()

    def init_plugin(self, config: dict = None) -> None:
        """
        读取配置并恢复任务记录。

        :param config: 插件配置
        """
        # 生命周期会在替换旧实例时调用 stop_service；这里不能先落盘，
        # 否则新实例的空 _tasks 会覆盖已保存的任务记录。
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._source_base = (config.get("source_base") or "https://www.javbus.com").strip()
        if (
            "use_javbus" in config
            or "use_javranking" in config
            or "use_javdb" in config
        ):
            self._use_javbus = bool(config.get("use_javbus", True))
            self._use_javranking = bool(config.get("use_javranking", False))
            self._use_javdb = bool(config.get("use_javdb", False))
        else:
            mode = (config.get("source_mode") or "javbus").strip().lower()
            self._use_javbus = mode in ("javbus", "auto")
            self._use_javranking = mode == "javranking"
            self._use_javdb = mode == "javdb"
        self._javdb_cookie = (config.get("javdb_cookie") or "").strip()
        if self._use_javdb and not self._javdb_cookie:
            logger.warning("已勾选 JavDB 但未填写 Cookie，该来源会取种失败")
        selected = sum(
            [self._use_javbus, self._use_javranking, self._use_javdb]
        )
        if selected > 1:
            self._source_mode = "auto"
        elif self._use_javdb:
            self._source_mode = "javdb"
        elif self._use_javranking:
            self._source_mode = "javranking"
        else:
            self._source_mode = "javbus"
        default_cache = str(Path(self.get_data_path()) / "javranking")
        self._javranking_dir = (config.get("javranking_dir") or default_cache).strip()
        self._emby_library = (config.get("emby_library") or "").strip()
        self._library_map = (config.get("library_map") or "").strip()
        self._enable_upgrade = bool(config.get("enable_upgrade", False))
        self._replace_existing = bool(config.get("replace_existing", True))
        self._proactive_enabled = bool(config.get("proactive_upgrade", False))
        self._enable_filter_rules = bool(config.get("enable_filter_rules", True))
        raw_min_size = config.get("min_video_size_mb")
        self._min_video_size_mb = 2048 if raw_min_size is None else max(0, int(raw_min_size))
        self._min_video_size_bytes = self._min_video_size_mb * 1024 ** 2
        self._cd2_host = (config.get("cd2_host") or "").strip()
        self._cd2_port = int(config.get("cd2_port") or 19798)
        self._cd2_token = (config.get("cd2_token") or "").strip()
        self._cd2_username = (config.get("cd2_username") or "").strip()
        self._cd2_password = (config.get("cd2_password") or "").strip()
        self._download_dir = (config.get("download_dir") or "").strip().rstrip("/")
        self._library_dir = (config.get("library_dir") or "").strip().rstrip("/")
        self._move_enabled = bool(config.get("move_to_library"))
        self._allow_uncensored = bool(config.get("allow_uncensored"))
        self._allow_junk = bool(config.get("allow_junk"))
        self._delete_junk_only = bool(config.get("delete_junk_only", True))
        self._poll_seconds = max(
            MIN_POLL_SECONDS,
            int(config.get("poll_seconds") or DEFAULT_POLL_SECONDS),
        )
        self._candidate_timeout_minutes = max(
            1,
            int(config.get("candidate_timeout_minutes") or DEFAULT_CANDIDATE_TIMEOUT_MINUTES),
        )
        self._candidate_queue_timeout_minutes = max(
            1,
            int(config.get("candidate_queue_timeout_minutes") or DEFAULT_QUEUE_TIMEOUT_MINUTES),
        )
        self._max_candidates = max(
            1,
            int(config.get("max_candidates") or DEFAULT_MAX_CANDIDATES),
        )
        # 旧版本配置，仅用于兼容升级前已经在运行的任务
        self._max_wait_minutes = max(5, int(config.get("max_wait_minutes") or 240))

        stored = self.get_data("tasks") or {}
        self._tasks = stored if isinstance(stored, dict) else {}
        queued = self.get_data("queue") or []
        self._queue = [
            item for item in queued
            if isinstance(item, dict) and item.get("code")
        ] if isinstance(queued, list) else []
        batches = self.get_data("batches") or {}
        self._batches = batches if isinstance(batches, dict) else {}
        proactive = self.get_data("proactive") or {}
        self._proactive = proactive if isinstance(proactive, dict) else {}
        self._register_proactive_listener()

        if not self._enabled:
            self._status = "未启用"
            return
        if not self._cd2_host:
            self._status = "配置不完整：缺少 CloudDrive2 地址，请在设置中填写"
            return
        if not self._cd2_token and not (self._cd2_username and self._cd2_password):
            self._status = "配置不完整：需要 API 令牌，或同时填写 CD2 账号和密码"
            return
        if not self._download_dir:
            self._status = "配置不完整：缺少云下载目录，请在设置中填写"
            return
        self._status = (
            f"运行中，数据源 {self._source_label()}，云下载目录 {self._download_dir}"
        )
        logger.info(
            f"番号云下载初始化：{self._status}，"
            f"待提交 {len(self._queue)} 条，历史任务 {len(self._tasks)} 条"
        )
        state = self.get_data("proactive_state") or {}
        if not isinstance(state, dict):
            state = {}
        if (
            self._proactive_enabled
            and self._enable_upgrade
            and self._emby_library
            and not state.get("enabled")
        ):
            state["enabled"] = True
            state["last_scan_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_data("proactive_state", state)
            threading.Thread(target=self.scan_proactive, daemon=True).start()
        elif not self._proactive_enabled and state.get("enabled"):
            state["enabled"] = False
            self.save_data("proactive_state", state)

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    def _register_proactive_listener(self) -> None:
        """注册主动洗版按钮回调；宿主不支持时只记录日志。"""
        try:
            from app.runtime.events import eventmanager
            from app.schemas.types import EventType

            eventmanager.add_event_listener(
                EventType.MessageAction, self._on_message_action
            )
        except Exception as error:
            logger.warning(f"注册主动洗版按钮回调失败：{type(error).__name__}: {error}")

    def get_service(self) -> List[Dict[str, Any]]:
        """注册离线任务推进服务。"""
        if not self._enabled:
            return []
        return [
            {
                "id": f"{self.__class__.__name__}_poll",
                "name": "番号云下载任务推进",
                "trigger": "interval",
                "func": self.advance_tasks,
                "kwargs": {"seconds": self._poll_seconds},
            }
        ]

    def get_agent_tools(self) -> List[Any]:
        """注册智能助手工具：提交番号、查询进度。"""
        if not self._enabled:
            return []
        # 延迟导入：离线测试没有 app.agent 模块，顶层导入会直接失败。
        from .agent_tools import JavSubmitTool, JavTasksTool

        return [JavSubmitTool, JavTasksTool]

    # ---------- 内部工具 ----------

    def _client(self) -> Cd2Client:
        """按当前配置构造 CD2 客户端。"""
        if not self._cd2_host:
            raise Cd2Error("未配置 CloudDrive2 地址")
        return Cd2Client(
            host=self._cd2_host,
            port=self._cd2_port,
            token=self._cd2_token,
            username=self._cd2_username,
            password=self._cd2_password,
        )

    def _persist(self) -> None:
        """把任务、队列与批次状态写回插件数据。"""
        with self._lock:
            if len(self._tasks) > MAX_RECORDS:
                # 未结束的任务和尚未计入批次的终态记录不能被裁掉，
                # 否则队列提交或批量汇总会永久丢失。
                keep: List[Tuple[str, Dict[str, Any]]] = []
                history: List[Tuple[str, Dict[str, Any]]] = []
                for item in self._tasks.items():
                    record = item[1]
                    settled = (
                        record.get("state") in TERMINAL_STATES
                        and not (
                            record.get("batch_id")
                            and not record.get("batch_counted")
                        )
                    )
                    (history if settled else keep).append(item)
                if len(keep) < MAX_RECORDS:
                    history.sort(
                        key=lambda kv: kv[1].get("updated_at", ""), reverse=True
                    )
                    self._tasks = dict(keep + history[:MAX_RECORDS - len(keep)])
            notified = [
                item for item in self._batches.items()
                if item[1].get("notified")
            ]
            if len(notified) > 20:
                notified.sort(
                    key=lambda kv: kv[1].get("created_at", ""), reverse=True
                )
                for key, _ in notified[20:]:
                    self._batches.pop(key, None)
            self.save_data("tasks", self._tasks)
            self.save_data("queue", self._queue)
            self.save_data("batches", self._batches)

    def _stamp(self, record: Dict[str, Any]) -> None:
        """更新记录时间戳。"""
        record["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _new_record(self, code: str, name: str) -> Dict[str, Any]:
        """创建新的任务记录。"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "code": code,
            "name": name,
            "state": STATE_SUBMITTED,
            "folder": self._download_dir,
            "library_dir": self._library_dir,
            "created_at": now,
            "updated_at": now,
            "attempts": 0,
            "organize_attempts": 0,
            "remove_attempts": 0,
            "candidates": [],
            "candidate_index": -1,
            "candidate_started_at": "",
            "candidate_attempts": [],
            "existing": None,
            "actual_quality": None,
            "notified_finish": False,
            "notified_failure": False,
            "message": "",
        }

    def _find_library_item(self, code: str, probe_missing: bool = True):
        """
        查询 Emby 成人库；必要时用 CD2 ffprobe 补真实质量。

        :param code: 归一化番号
        :param probe_missing: Emby 缺高度/体积时是否用 ffprobe 补；普通下载传 False
        """
        try:
            hit = EmbyLibrary(self._emby_library, self._library_map).find_item(code)
        except Exception as error:
            logger.warning(f"Emby 库检查失败：{type(error).__name__}: {error}")
            return None
        if hit is None:
            return None
        quality = dict(hit.quality or {})
        if (not quality.get("height") or not quality.get("size")) and hit.item_id:
            detail = EmbyLibrary(self._emby_library, self._library_map).get_item_detail(
                hit.server, hit.item_id
            )
            if detail:
                detailed = quality_from_item(detail)
                quality.update({k: v for k, v in detailed.items() if v})
        if (probe_missing and (not quality.get("height") or not quality.get("size"))
                and hit.path):
            client = None
            try:
                client = self._client()
                probed = probe_media(client, hit.path)
                if probed:
                    quality.update({k: v for k, v in probed.items() if v})
            except Exception as error:
                logger.warning(f"库内文件 ffprobe 失败：{type(error).__name__}: {error}")
            finally:
                if client is not None:
                    client.close()
        hit.quality = quality
        if not hit.relative_dir and not hit.cd2_dir:
            hit.relative_dir = EmbyLibrary(
                self._emby_library, self._library_map
            ).relative_dir_for(hit.server, hit.item_id)
        if not hit.cd2_dir:
            hit.cd2_dir = EmbyLibrary(
                self._emby_library, self._library_map
            ).cd2_dir_for(hit.server, hit.item_id)
        return hit

    def _scan_low_res(self) -> List[Dict[str, Any]]:
        """扫描 Emby 成人库中低于 1080P 的条目。"""
        if not self._emby_library:
            return []
        return EmbyLibrary(
            self._emby_library, self._library_map
        ).list_low_res(1080)

    def _save_proactive(self) -> None:
        """保存主动洗版记录。"""
        self.save_data("proactive", self._proactive)

    def scan_proactive(self) -> Dict[str, Any]:
        """
        扫描低清条目并发起洗版询问。

        每次最多提醒 ``PROACTIVE_BATCH`` 条；用户忽略后记录为 ``declined``，
        后续扫描不再提醒。
        """
        if not self._proactive_enabled:
            return {"ok": False, "reason": "未启用主动洗版"}
        if not self._enabled:
            return {"ok": False, "reason": "插件未启用"}
        if not self._enable_upgrade:
            return {"ok": False, "reason": "未启用洗版"}
        if not self._emby_library:
            return {"ok": False, "reason": "未配置 Emby 成人库"}
        if not _PROACTIVE_LOCK.acquire(blocking=False):
            return {"ok": False, "reason": "主动洗版扫描正在进行"}
        try:
            rows = self._scan_low_res()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_codes = {str(row.get("code") or "") for row in rows}
            pending: List[Dict[str, Any]] = []
            with self._lock:
                if rows:
                    for code, old in list(self._proactive.items()):
                        if (
                            old.get("status") in ("pending", "notified", "upgrading")
                            and code not in current_codes
                        ):
                            # 库内已经不再低清（例如已洗版或 Emby 更新），不再追问
                            old["status"] = "resolved"
                            old["updated_at"] = now
                            self._proactive[code] = old
                for row in rows:
                    code = str(row.get("code") or "")
                    if not code:
                        continue
                    old = self._proactive.get(code) or {}
                    if old.get("status") in (
                        "declined", "notified", "upgrading", "upgraded",
                        "skipped", "failed",
                    ):
                        continue
                    task = self._tasks.get(code) or {}
                    if task.get("state") in ACTIVE_STATES:
                        continue
                    record = {
                        "code": code,
                        "title": row.get("title") or "",
                        "path": row.get("path") or "",
                        "item_id": row.get("item_id") or "",
                        "height": int(row.get("height") or 0),
                        "width": int(row.get("width") or 0),
                        "bitrate": int(row.get("bitrate") or 0),
                        "size": int(row.get("size") or 0),
                        "status": "pending",
                        "notified_at": old.get("notified_at") or "",
                        "created_at": old.get("created_at") or now,
                        "updated_at": now,
                    }
                    self._proactive[code] = record
                    pending.append(record)
                self._save_proactive()
            listed = pending[:PROACTIVE_BATCH]
            notified = 0
            if listed and self._notify_proactive_summary(rows, pending, listed):
                with self._lock:
                    for record in listed:
                        record["status"] = "notified"
                        record["notified_at"] = now
                        record["updated_at"] = now
                        self._proactive[record["code"]] = record
                    self._save_proactive()
                notified = len(listed)
            return {
                "ok": True,
                "low_res": len(rows),
                "new": len(pending),
                "notified": notified,
                "items": pending,
            }
        except Exception as error:
            logger.warning(f"主动洗版扫描失败：{type(error).__name__}: {error}")
            return {"ok": False, "reason": str(error)}
        finally:
            _PROACTIVE_LOCK.release()

    def _notify_proactive_summary(self, rows: List[Dict[str, Any]],
                                  pending: List[Dict[str, Any]],
                                  listed: List[Dict[str, Any]]) -> bool:
        """发送一条汇总通知：总数 + 前 5 条列表 + 对应操作按钮。"""
        plugin_id = self.__class__.__name__
        lines = [
            f"本次扫描发现 {len(rows)} 条低于 1080P，待处理 {len(pending)} 条。"
        ]
        for index, record in enumerate(listed, 1):
            code = str(record.get("code") or "")
            height = int(record.get("height") or 0)
            width = int(record.get("width") or 0)
            size = int(record.get("size") or 0)
            size_text = f"，{size / 1024 ** 3:.2f}GB" if size else ""
            dimension = f"{width}×{height}" if width else f"{height}P"
            lines.append(f"{index}. {code} {dimension}{size_text}")
        if len(pending) > len(listed):
            lines.append(
                f"仅显示前 {len(listed)} 条，其余 {len(pending) - len(listed)} 条"
                "可在插件记录或 API 查看。"
            )
        buttons = []
        for record in listed:
            code = str(record.get("code") or "")
            buttons.append([
                {
                    "text": f"洗版 {code}",
                    "callback_data": f"[PLUGIN]{plugin_id}|proactive:upgrade:{code}",
                },
                {
                    "text": f"忽略 {code}",
                    "callback_data": f"[PLUGIN]{plugin_id}|proactive:dismiss:{code}",
                },
            ])
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载低清扫描：共 {len(rows)} 条",
                text="\n".join(lines),
                buttons=buttons,
            )
            return True
        except Exception as error:
            logger.warning(f"主动洗版提醒发送失败：{type(error).__name__}: {error}")
            return False

    def _on_message_action(self, event: Any) -> None:
        """处理通知按钮回调。"""
        data = getattr(event, "event_data", None) or {}
        if hasattr(data, "model_dump"):
            data = data.model_dump()
        if not isinstance(data, dict):
            return
        plugin_id = str(data.get("plugin_id") or "")
        if plugin_id != self.__class__.__name__:
            return
        text = str(data.get("text") or "")
        parts = text.split(":", 2)
        if len(parts) != 3 or parts[0] != "proactive":
            return
        self.handle_proactive_action(parts[1], parts[2])

    def handle_proactive_action(self, action: str, code: str) -> Dict[str, Any]:
        """
        处理主动洗版的用户选择。

        :param action: ``upgrade`` 或 ``dismiss``
        :param code: 番号
        :return: 处理结果
        """
        normalized = normalize_code(code)
        if not self._enabled:
            return {"ok": False, "reason": "插件未启用"}
        with self._lock:
            record = self._proactive.get(normalized)
        if not record:
            return {"ok": False, "reason": f"没有 {normalized} 的主动洗版记录"}
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if action == "dismiss":
            with self._lock:
                record["status"] = "declined"
                record["updated_at"] = now
                self._proactive[normalized] = record
                self._save_proactive()
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载已忽略洗版：{normalized}",
                text=f"{normalized} 已记录，不再重复提醒。",
            )
            return {"ok": True, "status": "declined"}
        if action != "upgrade":
            return {"ok": False, "reason": f"未知操作：{action}"}
        if not self._enable_upgrade:
            return {"ok": False, "reason": "未启用洗版"}
        with self._lock:
            record["status"] = "upgrading"
            record["updated_at"] = now
            self._proactive[normalized] = record
            self._save_proactive()
        try:
            result = self.submit(
                [normalized], wait_seconds=0, upgrade=True, force=True
            )
        except Exception as error:
            with self._lock:
                record["status"] = "failed"
                record["updated_at"] = now
                record["message"] = str(error)
                self._proactive[normalized] = record
                self._save_proactive()
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载洗版失败：{normalized}",
                text=str(error),
            )
            return {"ok": False, "status": "failed", "reason": str(error)}
        item = (result.get("items") or [{}])[0]
        if item.get("skipped"):
            status = "skipped"
        elif item.get("ok"):
            status = "upgrading"
        else:
            status = "failed"
        with self._lock:
            record["status"] = status
            record["updated_at"] = now
            record["message"] = item.get("reason") or item.get("state") or ""
            self._proactive[normalized] = record
            self._save_proactive()
        self.post_message(
            mtype=MessageType.Plugin,
            title=f"番号云下载洗版：{normalized}",
            text=(
                f"{normalized} 洗版已提交。"
                if status == "upgrading"
                else f"{normalized} 未提交洗版：{item.get('reason') or status}"
            ),
        )
        return {
            "ok": bool(item.get("ok")),
            "status": status,
            "result": result,
        }

    def _skip_record(self, record: Dict[str, Any], message: str) -> Dict[str, Any]:
        """把任务标记为跳过，不提交下载。"""
        record.update({"state": STATE_SKIPPED, "message": message})
        self._stamp(record)
        with self._lock:
            self._tasks[record["code"]] = record
        logger.info(f"番号云下载跳过 {record.get('code')}：{message}")
        return {
            "code": record["code"],
            "ok": True,
            "skipped": True,
            "name": record.get("name"),
            "reason": message,
        }


    @staticmethod
    def _elapsed_minutes(value: str) -> float:
        """
        计算某个时间点到现在的分钟数。

        :param value: ``YYYY-MM-DD HH:MM:SS`` 格式时间
        :return: 已过去的分钟数；无法解析时返回 0
        """
        try:
            started = datetime.strptime(value or "", "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return 0.0
        return (datetime.now() - started).total_seconds() / 60

    def _prepare_candidates(self, detail: Dict[str, Any],
                            limit: Optional[int] = None
                            ) -> List[Dict[str, Any]]:
        """
        把选种结果整理成可重试的候选列表。

        按 infoHash 去重，最多保留 ``max_candidates`` 条。

        :param detail: ``preview`` 返回的详情
        :param limit: 最多保留条数；为空时用配置的 max_candidates
        :return: 候选列表
        """
        raw = list(detail.get("candidates") or [])
        chosen = detail.get("chosen")
        if chosen and chosen not in raw:
            raw.insert(0, chosen)

        max_count = max(1, int(limit if limit is not None else self._max_candidates))
        result: List[Dict[str, Any]] = []
        seen = set()
        for item in raw:
            magnet = (item.get("magnet") or "").strip()
            if not magnet:
                continue
            key = (item.get("info_hash") or info_hash_of(magnet) or magnet).lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(
                {
                    "name": item.get("name") or "",
                    "magnet": magnet,
                    "info_hash": item.get("info_hash") or info_hash_of(magnet),
                    "size_bytes": int(item.get("size_bytes") or 0),
                    "size_text": item.get("size_text") or "",
                }
            )
            if len(result) >= max_count:
                break
        return result

    def _record_attempt(self, record: Dict[str, Any], result: str,
                        detail: str = "") -> None:
        """
        记录当前候选的结束原因，便于排查换种过程。

        :param record: 任务记录
        :param result: ``finished`` / ``timeout`` / ``error`` / ``submit_failed``
        :param detail: 补充说明
        """
        index = int(record.get("candidate_index") or 0)
        candidates = record.get("candidates") or []
        candidate = candidates[index] if 0 <= index < len(candidates) else {}
        attempts = record.setdefault("candidate_attempts", [])
        attempts.append(
            {
                "index": index,
                "name": candidate.get("name") or "",
                "info_hash": candidate.get("info_hash") or "",
                "started_at": record.get("submitted_at") or "",
                "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "result": result,
                "detail": detail,
            }
        )

    def _begin_candidate(self, client: Cd2Client, record: Dict[str, Any],
                         index: int) -> bool:
        """
        提交指定候选并重置该候选的计时状态。

        :param client: CD2 客户端
        :param record: 任务记录
        :param index: 候选下标
        :return: 提交成功返回 True
        """
        candidates = record.get("candidates") or []
        if index < 0 or index >= len(candidates):
            return False
        candidate = candidates[index]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record.update(
            {
                "candidate_index": index,
                "magnet": candidate.get("magnet") or "",
                "info_hash": candidate.get("info_hash") or "",
                "magnet_name": candidate.get("name") or "",
                "size_bytes": int(candidate.get("size_bytes") or 0),
                "size_text": candidate.get("size_text") or "",
                "state": STATE_SUBMITTED,
                "submitted_at": now,
                "candidate_started_at": "",
                "offline_state": "",
                "percent": 0,
                "task_name": "",
                "remove_attempts": 0,
                "organize_attempts": 0,
                "attempts": 0,
                "message": f"提交候选 {index + 1}/{len(candidates)}",
            }
        )
        self._stamp(record)
        try:
            submission = client.add_offline(
                candidate["magnet"], record.get("folder") or self._download_dir
            )
        except Cd2Error as error:
            self._record_attempt(record, "submit_failed", str(error))
            record["message"] = f"候选 {index + 1} 提交失败：{error}"
            self._stamp(record)
            return False
        record["message"] = (
            f"已提交候选 {index + 1}/{len(candidates)}（{submission}）"
        )
        self._stamp(record)
        return True

    def _notify_finish(self, record: Dict[str, Any]) -> None:
        """发送一次下载完成通知。批量提交只发最终汇总。"""
        if record.get("notified_finish"):
            return
        if record.get("batch_id"):
            record["notified_finish"] = True
            return
        if _already_notified("finish", record):
            record["notified_finish"] = True
            return
        record["notified_finish"] = True
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载完成：{record.get('code')}",
                text=(
                    f"{record.get('name') or record.get('code')} "
                    f"{record.get('size_text') or ''} 已下载完成，开始整理（尚未入库）"
                ).strip(),
            )
        except Exception as error:  # 通知失败不影响下载与整理
            logger.warning(f"番号云下载完成通知发送失败：{error}")

    def _notify_failure(self, record: Dict[str, Any], message: str) -> None:
        """发送一次最终失败通知。批量提交只发最终汇总。"""
        if record.get("notified_failure"):
            return
        if record.get("batch_id"):
            record["notified_failure"] = True
            return
        if _already_notified("failure", record):
            record["notified_failure"] = True
            return
        record["notified_failure"] = True
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载失败：{record.get('code')}",
                text=(
                    f"{record.get('name') or record.get('code')} "
                    f"{record.get('size_text') or ''}：{message}"
                ).strip(),
            )
        except Exception as error:
            logger.warning(f"番号云下载失败通知发送失败：{error}")

    def _notify_organize_failure(self, record: Dict[str, Any],
                                  message: str) -> None:
        """整理/落盘失败时单独通知，避免被理解成下载失败。"""
        if record.get("notified_failure"):
            return
        if record.get("batch_id"):
            record["notified_failure"] = True
            return
        if _already_notified("organize_failure", record):
            record["notified_failure"] = True
            return
        record["notified_failure"] = True
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载整理失败：{record.get('code')}",
                text=(
                    f"{record.get('name') or record.get('code')} "
                    f"{record.get('size_text') or ''} 已下载完成，"
                    f"但整理/落盘失败：{message}"
                ).strip(),
            )
        except Exception as error:
            logger.warning(f"番号云下载整理失败通知发送失败：{error}")

    def _fail(self, record: Dict[str, Any], message: str) -> None:
        """把任务标记为最终失败并发通知。"""
        record.update({"state": STATE_FAILED, "message": message})
        self._stamp(record)
        logger.error(f"番号云下载失败 {record.get('code')}：{message}")
        self._notify_failure(record, message)

    # ---------- 核心动作 ----------

    def _source_label(self) -> str:
        """返回当前勾选的来源名称。"""
        names = []
        if self._use_javbus:
            names.append("JavBus")
        if self._use_javranking:
            names.append("JavRanking")
        if self._use_javdb:
            names.append("JavDB")
        return "+".join(names) or "未选择"

    def _fetch_detail(self, code: str) -> Dict[str, Any]:
        """
        按勾选的来源读取番号详情，合并磁力并按 infoHash 去重。

        JavRanking 勾选后，本地缓存没有磁力时会按需抓取详情页；
        JavDB 需要配置 Cookie，磁力名称只有番号、没有分辨率；
        多个来源勾选时，任一失败仍可使用其他来源。

        :param code: 番号
        :return: 详情字典
        """
        normalized = normalize_code(code)
        if not self._use_javbus and not self._use_javranking and not self._use_javdb:
            raise JavBusError("未勾选任何种子来源")
        def load_ranking() -> Dict[str, Any]:
            return JavRankingSource(self._javranking_dir).fetch_remote_detail(
                normalized
            )

        def load_javbus() -> Dict[str, Any]:
            source = JavBusSource(base_url=self._source_base)
            try:
                detail = source.fetch_detail(normalized)
                detail["source"] = "javbus"
                return detail
            finally:
                source.close()

        def load_javdb() -> Dict[str, Any]:
            source = JavDBSource(cookie=self._javdb_cookie)
            try:
                return source.fetch_detail(normalized)
            finally:
                source.close()

        loaded: List[tuple] = []
        errors: List[str] = []
        futures = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            if self._use_javranking:
                futures[pool.submit(load_ranking)] = "JavRanking"
            if self._use_javbus:
                futures[pool.submit(load_javbus)] = "JavBus"
            if self._use_javdb:
                futures[pool.submit(load_javdb)] = "JavDB"
            for future in as_completed(futures):
                name = futures[future]
                try:
                    loaded.append((name, future.result()))
                except Exception as error:
                    errors.append(f"{name}：{error}")
                    if "冷却期" in str(error):
                        logger.debug(f"{name} 获取失败（冷却期）：{error}")
                    else:
                        logger.info(f"{name} 获取失败：{error}")
        if (
            (self._use_javranking or self._use_javdb)
            and not self._use_javbus
            and not any(detail.get("magnets") for _, detail in loaded)
        ):
            # JavRanking / JavDB 都被 Cloudflare 拦下时，用 JavBus 兜底，
            # 避免只勾这两个来源的配置完全下不了种。
            logger.warning("JavRanking/JavDB 不可用，自动回退 JavBus 取种")
            try:
                loaded.append(("JavBus", load_javbus()))
            except Exception as error:
                errors.append(f"JavBus：{error}")
                logger.warning(f"JavBus 回退失败：{error}")
        loaded.sort(key=lambda item: {"JavRanking": 0, "JavBus": 1}.get(item[0], 2))
        details = [detail for _, detail in loaded]
        if not details:
            raise JavBusError("；".join(errors) or f"{normalized} 没有可用来源")
        base = dict(details[0])
        magnets: List[Dict[str, Any]] = []
        seen = set()
        for detail in details:
            for magnet in detail.get("magnets") or []:
                info_hash = (
                    magnet.get("info_hash")
                    or info_hash_of(magnet.get("magnet") or "")
                ).lower()
                if info_hash and info_hash in seen:
                    continue
                if info_hash:
                    seen.add(info_hash)
                magnets.append(magnet)
        base["magnets"] = magnets
        base["has_magnets"] = bool(magnets)
        base["source"] = "+".join(
            str(detail.get("source") or "") for detail in details
            if detail.get("source") and detail.get("magnets")
        )
        for key in ("title", "release_date", "duration", "detail_url"):
            if not base.get(key):
                base[key] = next(
                    (detail.get(key) for detail in details if detail.get(key)), ""
                )
        if not magnets:
            raise JavBusError("；".join(errors) or f"{normalized} 没有可用磁力")
        return base

    def preview(self, code: str) -> Dict[str, Any]:
        """
        只抓取并展示候选，不提交任何任务。

        :param code: 番号
        :return: 候选与选种结果
        """
        detail = self._fetch_detail(code)
        picked = select(
            detail["magnets"],
            allow_uncensored=self._allow_uncensored,
            allow_junk=self._allow_junk,
            enable_rules=self._enable_filter_rules,
            min_video_size=self._min_video_size_bytes,
        )
        return {
            "code": detail.get("code") or normalize_code(code),
            "source": detail.get("source"),
            "has_magnets": detail.get("has_magnets", bool(detail.get("magnets"))),
            "title": detail.get("title"),
            "release_date": detail.get("release_date"),
            "duration": detail.get("duration"),
            "detail_url": detail.get("detail_url"),
            "total": picked.get("total"),
            "tier": picked.get("tier"),
            "chosen": picked.get("chosen"),
            "excluded": picked.get("excluded", []),
            "candidates": (picked.get("candidates") or [])[:20],
        }

    def enqueue(self, codes: List[str]) -> Dict[str, Any]:
        """
        把番号加入待提交队列并立即返回。

        实际的取种与 115 提交由定时服务在后台逐条完成；
        同一个批次的全部任务结束后发一条汇总通知。

        :param codes: 番号列表
        :return: 入队结果
        """
        if not self._enabled:
            return {"ok": False, "reason": "插件未启用"}
        if not self._download_dir:
            return {"ok": False, "reason": "未配置云下载目录"}
        normalized: List[str] = []
        invalid: List[str] = []
        for raw in codes or []:
            text = str(raw or "").strip()
            if not text:
                continue
            code = normalize_code(text)
            if code:
                normalized.append(code)
            else:
                invalid.append(text)
        with self._lock:
            queued = {str(item.get("code") or "") for item in self._queue}
            busy = {
                code for code, record in self._tasks.items()
                if record.get("state") in ACTIVE_STATES
            }
            accepted: List[str] = []
            skipped: List[Dict[str, str]] = []
            for code in dict.fromkeys(normalized):
                if code in queued or code in busy:
                    skipped.append({"code": code, "reason": "已在队列或处理中"})
                else:
                    accepted.append(code)
            if len(self._queue) + len(accepted) > MAX_QUEUE:
                return {
                    "ok": False,
                    "reason": (
                        f"队列已满：当前 {len(self._queue)} 条，上限 {MAX_QUEUE} 条，"
                        f"本次最多还能加入 {max(0, MAX_QUEUE - len(self._queue))} 条"
                    ),
                }
            if not accepted:
                return {
                    "ok": True,
                    "queued": 0,
                    "skipped": skipped,
                    "invalid": invalid,
                    "message": "这些番号已在队列或处理中，没有新增",
                }
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:6]}"
            ahead = len(self._queue)
            for code in accepted:
                self._queue.append({
                    "code": code,
                    "batch_id": batch_id,
                    "added_at": now,
                })
            self._batches[batch_id] = {
                "id": batch_id,
                "created_at": now,
                "total": len(accepted),
                "done": 0,
                "failed": 0,
                "skipped": 0,
                "failures": [],
                "notified": False,
                "finished_at": "",
            }
            self._persist()
        logger.info(
            f"番号云下载入队 {len(accepted)} 条，前面 {ahead} 条，"
            f"批次 {batch_id}"
        )
        return {
            "ok": True,
            "batch_id": batch_id,
            "queued": len(accepted),
            "queue_size": ahead + len(accepted),
            "skipped": skipped,
            "invalid": invalid,
            "message": f"已添加至队列：{len(accepted)} 条（前面还有 {ahead} 条）",
        }

    def submit(self, codes: List[str], wait_seconds: int = 0,
               upgrade: bool = False, force: bool = False) -> Dict[str, Any]:
        """
        提交番号云下载任务。

        取种与提交离线在本次调用内完成；若给出 ``wait_seconds`` 会同步推进一段时间，
        否则交由定时服务继续推进。

        :param codes: 番号列表
        :param wait_seconds: 同步等待秒数，0 表示不等待
        :param upgrade: 是否请求洗版；仅在插件启用洗版时生效
        :param force: 强制洗版，跳过下载前粗筛；下载后仍会 ffprobe 复核
        :return: 每个番号的处理结果
        """
        results: List[Dict[str, Any]] = []
        for raw in codes:
            normalized = normalize_code(raw)
            if not normalized:
                continue
            try:
                results.append(
                    self._submit_one(normalized, wait_seconds, upgrade, force)
                )
            except Exception as error:
                logger.error(
                    f"番号云下载提交失败 {normalized}：{type(error).__name__}: {error}"
                )
                record = self._tasks.get(normalized) or self._new_record(normalized, normalized)
                record.update({"state": STATE_FAILED, "message": str(error)})
                self._stamp(record)
                self._notify_failure(record, str(error))
                with self._lock:
                    self._tasks[normalized] = record
                results.append({"code": normalized, "ok": False, "reason": str(error)})
        self._persist()
        return {"ok": all(item.get("ok", True) for item in results), "items": results}

    def _submit_one(self, code: str, wait_seconds: int,
                    upgrade: bool = False, force: bool = False,
                    batch_id: str = "") -> Dict[str, Any]:
        """
        处理单个番号的提交。

        :param code: 归一化番号
        :param wait_seconds: 同步等待秒数
        :param upgrade: 是否请求洗版
        :param force: 强制洗版
        :param batch_id: 所属批量任务 ID；空表示不是队列提交
        :return: 处理结果
        """
        name = safe_name(code)
        record = self._tasks.get(code) or self._new_record(code, name)
        record["name"] = name
        if batch_id:
            record["batch_id"] = batch_id
            record["batch_counted"] = False
            record["notified_finish"] = False
            record["notified_failure"] = False
        upgrade_requested = bool(self._enable_upgrade and (upgrade or force))
        existing = self._find_library_item(code, probe_missing=upgrade_requested)
        if existing is not None and not upgrade_requested:
            return self._skip_record(record, "Emby 库已有该番号，未启用洗版，跳过下载")

        detail = self.preview(code)
        if not detail.get("chosen") and not detail.get("candidates"):
            raise JavBusError(f"{code} 没有可用磁力")
        if not self._download_dir:
            raise Cd2Error("未配置云下载目录，请先在插件设置中填写")
        # 洗版要先在全部过滤结果里挑最好的，再截断到重试上限
        candidates = self._prepare_candidates(detail, limit=20)
        if not candidates:
            raise JavBusError(f"{code} 没有可提交的候选磁力")
        name = safe_name(detail.get("code") or code)
        record["name"] = name
        if existing is not None:
            qualities = [
                estimate_candidate_quality(
                    candidate.get("name") or "",
                    int(candidate.get("size_bytes") or 0),
                )
                for candidate in candidates
            ]
            best_index = max(
                range(len(candidates)),
                key=lambda index: (
                    int(qualities[index].get("height") or 0),
                    int(qualities[index].get("size") or 0),
                    bool(qualities[index].get("has_chinese_sub")),
                ),
            )
            if not force and not candidate_may_be_better(
                existing.quality, qualities[best_index]
            ):
                record.update({
                    "name": name,
                    "existing": {
                        "server": existing.server,
                        "item_id": existing.item_id,
                        "path": existing.path,
                        "quality": existing.quality,
                        "relative_dir": existing.relative_dir,
                        "cd2_dir": existing.cd2_dir,
                    },
                })
                return self._skip_record(
                    record,
                    "库内已有版本质量不低于当前候选，跳过洗版",
                )
            # 洗版优先尝试质量最高的候选，再按原顺序重试其它候选
            if best_index:
                candidates = [candidates[best_index]] + [
                    candidate for index, candidate in enumerate(candidates)
                    if index != best_index
                ]
        candidates = candidates[:self._max_candidates]
        record.update(
            {
                "name": name,
                "tier": detail.get("tier"),
                "title": detail.get("title"),
                "source": detail.get("source"),
                "candidates": candidates,
                "candidate_index": -1,
                "candidate_started_at": "",
                "candidate_attempts": [],
                "remove_attempts": 0,
                "notified_finish": False,
                "notified_failure": False,
                "existing": (
                    {
                        "server": existing.server,
                        "item_id": existing.item_id,
                        "path": existing.path,
                        "quality": existing.quality,
                        "relative_dir": existing.relative_dir,
                        "cd2_dir": existing.cd2_dir,
                    }
                    if existing is not None
                    else None
                ),
            }
        )
        self._stamp(record)
        with self._lock:
            self._tasks[code] = record

        client = self._client()
        try:
            existing_dir = f"{self._download_dir.rstrip('/')}/{name}"
            if client.find(existing_dir) is not None and dir_has_video(client, existing_dir):
                # 已落盘的成片仍要走整理/移动，重复提交才能把文件送进库目录
                record.update(
                    {
                        "state": STATE_FINISHED,
                        "final_dir": existing_dir,
                        "message": "云下载目录已存在同名成片，开始整理",
                    }
                )
                self._stamp(record)
                self._organize(client, record, 0)
                return {
                    "code": code,
                    "ok": True,
                    "skipped": record.get("state") == STATE_ORGANIZED,
                    "moved": record.get("state") == STATE_MOVED,
                    "name": name,
                    "state": record.get("state"),
                    "final_dir": record.get("final_dir"),
                    "reason": record.get("message"),
                }
            quota = client.quota(self._download_dir)
            record["quota"] = quota
            if quota.get("left", 0) <= 0:
                record.update({"state": STATE_FAILED, "message": "115 离线配额已用完"})
                self._stamp(record)
                return {"code": code, "ok": False, "reason": record["message"]}

            submitted = False
            for index in range(len(candidates)):
                if self._begin_candidate(client, record, index):
                    submitted = True
                    break
            if not submitted:
                self._fail(record, "所有候选磁力都提交失败")
                return {"code": code, "ok": False, "reason": record["message"]}
            if wait_seconds > 0:
                self._advance_record(client, record, wait_seconds)
        finally:
            client.close()
        return {
            "code": code,
            "ok": True,
            "name": name,
            "tier": record.get("tier"),
            "size": record.get("size_text"),
            "state": record.get("state"),
        }

    def _wait_exceeded(self, record: Dict[str, Any]) -> bool:
        """
        判断下载阶段是否已超过最长等待时间。

        :param record: 任务记录
        :return: 超时返回 True
        """
        if record.get("state") not in (STATE_SUBMITTED, STATE_DOWNLOADING):
            return False
        if self._max_wait_minutes <= 0:
            return False
        try:
            submitted_at = datetime.strptime(
                record["submitted_at"], "%Y-%m-%d %H:%M:%S"
            )
        except (KeyError, TypeError, ValueError):
            return False
        elapsed_minutes = (datetime.now() - submitted_at).total_seconds() / 60
        return elapsed_minutes > self._max_wait_minutes

    @staticmethod
    def _offline_index(tasks: List[Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        把离线任务列表整理成 infoHash / 名称索引。

        :param tasks: ``client.list_offline`` 返回的任务列表
        :return: ``(按 infoHash, 按名称)`` 两个字典
        """
        by_hash: Dict[str, Any] = {}
        by_name: Dict[str, Any] = {}
        for task in tasks:
            info_hash = (getattr(task, "infoHash", "") or "").lower()
            if info_hash:
                by_hash[info_hash] = task
            name = getattr(task, "name", "") or ""
            if name and name not in by_name:
                by_name[name] = task
        return by_hash, by_name

    def _lookup_offline(self, client: Cd2Client, record: Dict[str, Any],
                        offline_index: Optional[Tuple[Dict[str, Any], Dict[str, Any]]]
                        ) -> Optional[Any]:
        """
        在当前候选的离线任务列表中定位任务。

        :param client: CD2 客户端
        :param record: 任务记录
        :param offline_index: 批量索引；None 时回退到单次查询
        :return: 离线任务；未找到返回 None
        """
        folder = record.get("folder") or self._download_dir
        info_hash = record.get("info_hash") or ""
        if offline_index is not None:
            by_hash, by_name = offline_index
            target = info_hash.lower()
            if target and target in by_hash:
                return by_hash[target]
            for candidate_name in (
                record.get("task_name"),
                record.get("magnet_name"),
                record.get("name"),
            ):
                if candidate_name and candidate_name in by_name:
                    return by_name[candidate_name]
            return None
        return client.find_offline(
            folder,
            info_hash=info_hash,
            name=record.get("task_name") or record.get("name") or "",
        )

    def _advance_legacy(self, client: Cd2Client, record: Dict[str, Any],
                        wait_seconds: int = 0) -> None:
        """
        推进升级前创建的旧任务，保持原有 240 分钟等待行为。

        :param client: CD2 客户端
        :param record: 旧任务记录
        :param wait_seconds: 未使用，保留签名一致性
        """
        code = record.get("code") or ""
        name = record.get("name") or code
        info_hash = record.get("info_hash") or ""
        folder = record.get("folder") or self._download_dir
        record["attempts"] = int(record.get("attempts") or 0) + 1
        if not record.get("submitted_at"):
            record["submitted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            task = client.find_offline(folder, info_hash=info_hash, name=name)
        except Cd2Error as error:
            record.update({"message": f"查询离线任务失败，稍后重试：{error}"})
            self._stamp(record)
            return

        if task is None:
            if self._wait_exceeded(record):
                self._fail(
                    record,
                    f"下载等待超过 {self._max_wait_minutes} 分钟，未查询到离线任务",
                )
                return
            record["message"] = "尚未查询到离线任务"
            self._stamp(record)
            return

        record["task_name"] = task.name
        state = status_name(task.status)
        record["offline_state"] = state
        record["percent"] = round(float(task.percendDone or 0), 1)
        if state == "OFFLINE_ERROR":
            self._fail(record, "云下载任务失败")
            return
        if state != "OFFLINE_FINISHED":
            if self._wait_exceeded(record):
                self._fail(
                    record,
                    f"下载等待超过 {self._max_wait_minutes} 分钟"
                    f"（最后状态 {state} {record['percent']}%）",
                )
                return
            record.update({"state": STATE_DOWNLOADING,
                           "message": f"下载中 {record['percent']}%"})
            self._stamp(record)
            return

        record["state"] = STATE_FINISHED
        record["message"] = "下载完成，开始整理"
        self._stamp(record)
        self._notify_finish(record)
        self._organize(client, record, wait_seconds)

    def _switch_candidate(self, client: Cd2Client, record: Dict[str, Any],
                          reason: str, result: str = "timeout",
                          exhausted_message: str = "") -> None:
        """
        删除当前候选已下载的文件和离线任务记录，然后提交下一条候选。

        :param client: CD2 客户端
        :param record: 任务记录
        :param reason: 换种原因
        :param result: 写进 candidate_attempts 的结果
        :param exhausted_message: 所有候选耗尽时的失败说明
        :param task: 当前离线任务；None 表示列表中还没出现
        """
        folder = record.get("folder") or self._download_dir
        target = locate_download_dir(
            client,
            folder,
            record.get("name") or record.get("code") or "",
            record.get("task_name") or "",
        )
        if target is not None:
            try:
                removed = client.delete(target)
                error_text = "" if removed else "删除文件接口返回失败"
            except Exception as error:
                removed = False
                error_text = f"{type(error).__name__}: {error}"
            if removed:
                # 删除后回查，确认目录确实消失
                try:
                    remaining = client.find(target)
                except Exception as error:
                    removed = False
                    error_text = f"删除后无法确认：{type(error).__name__}: {error}"
                else:
                    if remaining is not None:
                        removed = False
                        error_text = "删除接口返回成功，但目录仍存在"
            if not removed:
                attempts = int(record.get("remove_attempts") or 0) + 1
                record["remove_attempts"] = attempts
                record["message"] = (
                    f"删除已下载文件失败，重试 {attempts}/{MAX_REMOVE_ATTEMPTS}"
                )
                if attempts >= MAX_REMOVE_ATTEMPTS:
                    self._fail(
                        record,
                        f"删除已下载文件失败：{error_text or '未知错误'}",
                    )
                else:
                    self._stamp(record)
                return

        old_hash = (record.get("info_hash") or "").lower()
        if old_hash:
            try:
                client.remove_offline(folder, [old_hash], delete_files=False)
                logger.info(f"番号云下载已清理离线任务记录：{old_hash}")
            except Exception as error:
                logger.warning(
                    f"番号云下载清理离线任务记录失败：{type(error).__name__}: {error}"
                )
        record["remove_attempts"] = 0
        self._record_attempt(record, result, reason)
        candidates = record.get("candidates") or []
        next_index = int(record.get("candidate_index") or 0) + 1
        if next_index >= len(candidates):
            self._fail(
                record,
                exhausted_message or f"{len(candidates)} 条候选均未在时限内完成",
            )
            return
        for index in range(next_index, len(candidates)):
            if self._begin_candidate(client, record, index):
                return
        self._fail(record, "所有候选磁力都提交失败")

    def _advance_record(self, client: Cd2Client, record: Dict[str, Any],
                        wait_seconds: int = 0,
                        offline_index: Optional[Tuple[Dict[str, Any], Dict[str, Any]]] = None
                        ) -> None:
        """
        推进单条任务：查状态 → 整理 / 超时换种。

        :param client: CD2 客户端
        :param record: 任务记录
        :param wait_seconds: 单次等待秒数；0 表示只检查一次
        :param offline_index: 本轮批量拉取的离线任务索引
        """
        if record.get("state") == STATE_FINISHED:
            self._organize(client, record, wait_seconds)
            return
        if not record.get("candidates"):
            self._advance_legacy(client, record, wait_seconds)
            return

        record["attempts"] = int(record.get("attempts") or 0) + 1
        task = self._lookup_offline(client, record, offline_index)

        if task is None:
            if self._elapsed_minutes(record.get("submitted_at")) >= self._candidate_queue_timeout_minutes:
                self._switch_candidate(
                    client, record,
                    f"115 排队超过 {self._candidate_queue_timeout_minutes} 分钟",
                )
            else:
                record.update({"state": STATE_SUBMITTED,
                               "message": "等待 115 接收任务"})
                self._stamp(record)
            return

        record["task_name"] = task.name
        state = status_name(task.status)
        record["offline_state"] = state
        previous_percent = float(record.get("percent") or 0)
        record["percent"] = round(float(task.percendDone or 0), 1)

        if state == "OFFLINE_FINISHED":
            record["state"] = STATE_FINISHED
            record["message"] = "下载完成，开始整理"
            logger.info(f"番号云下载 {record.get('code')} 下载完成，开始整理")
            self._record_attempt(record, "finished", f"{record['percent']}%")
            self._stamp(record)
            self._notify_finish(record)
            self._organize(client, record, wait_seconds)
            return

        if state == "OFFLINE_ERROR":
            logger.warning(f"番号云下载 {record.get('code')} 离线任务失败，换种")
            self._switch_candidate(client, record, "离线任务失败", result="error")
            return

        if state == "OFFLINE_DOWNLOADING":
            if record["percent"] - previous_percent >= 10:
                logger.info(
                    f"番号云下载 {record.get('code')} 下载中 "
                    f"{record['percent']}%"
                )
            if not record.get("candidate_started_at"):
                record["candidate_started_at"] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            elapsed = self._elapsed_minutes(record["candidate_started_at"])
            if elapsed >= self._candidate_timeout_minutes:
                self._switch_candidate(
                    client, record,
                    f"下载超过 {self._candidate_timeout_minutes} 分钟"
                    f"（{record['percent']}%）",
                )
                return
            record.update({
                "state": STATE_DOWNLOADING,
                "message": (
                    f"下载中 {record['percent']}%"
                    f"（候选 {int(record.get('candidate_index') or 0) + 1}"
                    f"/{len(record.get('candidates') or [])}）"
                ),
            })
            self._stamp(record)
            return

        # OFFLINE_INIT / OFFLINE_UNKNOWN：仍在排队，按 115 排队超时处理
        if self._elapsed_minutes(record.get("submitted_at")) >= self._candidate_queue_timeout_minutes:
            self._switch_candidate(
                client, record,
                f"115 排队超过 {self._candidate_queue_timeout_minutes} 分钟（{state}）",
            )
        else:
            record.update({"state": STATE_SUBMITTED,
                           "message": f"排队中（{state}）"})
            self._stamp(record)

    def _organize_give_up(self, record: Dict[str, Any], message: str) -> bool:
        """
        整理连续失败超过上限时终止重试。

        :param record: 任务记录
        :param message: 本次失败原因
        :return: True 表示已终止，调用方应立即返回
        """
        attempts = int(record.get("organize_attempts") or 0)
        if attempts <= MAX_ORGANIZE_ATTEMPTS:
            return False
        record.update({
            "state": STATE_FAILED,
            "message": f"{message}（已重试 {attempts} 次）",
        })
        self._stamp(record)
        self._notify_organize_failure(record, record["message"])
        return True

    def _organize(self, client: Cd2Client, record: Dict[str, Any],
                  wait_seconds: int = 0) -> None:
        """
        整理已下载目录并可选移动。

        :param client: CD2 客户端
        :param record: 任务记录
        :param wait_seconds: 未使用，保留签名一致性
        """
        name = record.get("name") or record.get("code")
        folder = record.get("folder") or self._download_dir
        record["organize_attempts"] = int(record.get("organize_attempts") or 0) + 1

        target = locate_download_dir(
            client, folder, name, record.get("task_name", "")
        )
        if target is None:
            # 115 刚完成时 CD2 目录缓存可能还没刷新，强制重查一次
            target = locate_download_dir(
                client, folder, name, record.get("task_name", ""),
                force_refresh=True,
            )
        if target is None:
            message = "未找到落盘目录"
            if self._organize_give_up(record, message):
                return
            record.update({"state": STATE_FINISHED, "message": f"{message}，稍后重试"})
            self._stamp(record)
            return

        candidates = record.get("candidates") or []
        index = int(record.get("candidate_index") or 0)
        candidate_name = ""
        if 0 <= index < len(candidates):
            candidate_name = candidates[index].get("name") or ""
        if not candidate_name:
            candidate_name = record.get("task_name") or ""
        keep_c = has_chinese_sub(candidate_name)
        file_base = f"{name}-C" if keep_c else name

        tidied = tidy_dir(
            client, target, name,
            delete_junk_only=self._delete_junk_only,
            enable_filter_rules=self._enable_filter_rules,
            # 整理阶段只清小视频垃圾，和候选最小体积（默认 2GB）是两回事
            min_video_size=DEFAULT_MIN_VIDEO_SIZE,
            file_name=file_base,
        )
        if tidied.get("ok"):
            try:
                removed_dirs = remove_empty_dirs(
                    client, tidied.get("final_dir") or target
                )
                if removed_dirs:
                    tidied["deleted_dirs"] = removed_dirs
            except Exception as error:
                logger.warning(f"清理空目录失败：{error}")
        record["tidy"] = tidied
        if not tidied.get("ok"):
            reason = str(tidied.get("reason") or "")
            if reason.startswith("所有视频都小于") or reason == "未找到视频文件":
                # 盘里没有可用的视频文件（例如只有 ISO 镜像和广告），
                # 重试同一个种子没有意义，直接删掉换下一条候选。
                logger.warning(
                    f"番号云下载 {record.get('code')} 没有可用视频，"
                    f"换下一条候选：{reason}"
                )
                self._switch_candidate(
                    client, record, f"整理失败：{reason}",
                    result="organize_failed",
                    exhausted_message=f"所有候选都没有可用视频（{reason}）",
                )
                return
            message = f"整理失败：{tidied.get('reason')}"
            if self._organize_give_up(record, message):
                return
            record.update({"state": STATE_FINISHED, "message": message})
            self._stamp(record)
            return

        final_dir = tidied["final_dir"]
        existing = record.get("existing") or None
        actual = None
        if existing:
            actual = probe_media(client, tidied.get("final_file") or "")
            if actual:
                height = int(actual.get("height") or 0)
                if height and height < 720:
                    reason = f"下载后真实分辨率 {height}P 低于 720P"
                    if record.get("candidates"):
                        self._switch_candidate(
                            client, record, reason, result="low_quality",
                            exhausted_message="所有候选下载后都低于 720P",
                        )
                    else:
                        self._fail(record, reason)
                    return
                record["actual_quality"] = actual
        if existing:
            if not actual:
                record.update({
                    "state": STATE_ORGANIZED,
                    "final_dir": final_dir,
                    "final_files": tidied.get("final_files") or [],
                    "final_file": tidied.get("final_file"),
                    "message": "库内质量未知，无法确认洗版效果，保留在云下载目录",
                })
                self._stamp(record)
                return
            if not actual_is_better(existing.get("quality") or {}, actual):
                try:
                    deleted = client.delete(final_dir)
                except Exception as error:
                    deleted = False
                    logger.warning(f"洗版复核失败后删除下载文件出错：{error}")
                if not deleted or client.find(final_dir) is not None:
                    if self._organize_give_up(record, "洗版复核未通过，但删除下载文件失败"):
                        return
                    record.update({
                        "state": STATE_FINISHED,
                        "message": "洗版复核未通过，但删除下载文件失败，稍后重试",
                    })
                    self._stamp(record)
                    return
                record.update({
                    "state": STATE_SKIPPED,
                    "final_dir": "",
                    "final_files": [],
                    "final_file": "",
                    "message": "下载后真实分辨率不高于库内版本，已删除下载文件",
                })
                self._stamp(record)
                return
        record.update(
            {
                "state": STATE_ORGANIZED,
                "final_dir": final_dir,
                "final_files": tidied.get("final_files") or [tidied.get("final_file")],
                "final_file": tidied.get("final_file"),
                "message": f"已整理，删除 {len(tidied.get('deleted') or [])} 个文件",
            }
        )
        self._stamp(record)

        library = self._library_dir
        if not self._move_enabled:
            record["message"] = "已整理；整理后移动未开启，保留在云下载目录"
            self._stamp(record)
            return
        if not library:
            record["message"] = "已整理；未配置库目录，无法移动"
            self._stamp(record)
            return
        if existing and not self._replace_existing:
            record["message"] = "库内已有版本，替换开关关闭，保留在云下载目录"
            self._stamp(record)
            return
        dest_dir = ""
        if existing and self._replace_existing:
            # 优先用用户配置的目录映射定位旧目录，避免库结构不统一
            dest_dir = existing.get("cd2_dir") or ""
            if not dest_dir:
                relative = existing.get("relative_dir") or ""
                if relative:
                    dest_dir = f"{library.rstrip('/')}/{relative}"
        if existing and dest_dir:
            old_name = str(
                (existing.get("quality") or {}).get("path") or ""
            ).rstrip("/").rsplit("/", 1)[-1]
            moved = replace_existing_files(
                client, final_dir, dest_dir,
                tidied.get("final_files") or [],
                old_name=old_name, code=name,
            )
        else:
            moved = move_dir(
                client, final_dir, library,
                replace=bool(existing) and self._replace_existing,
                dest_dir=dest_dir,
            )
        record["move"] = moved
        if moved.get("ok"):
            moved_dir = moved.get("final_dir") or ""
            moved_files = moved.get("final_files") or [
                f"{moved_dir}/{PurePosixPath(item).name}"
                for item in (record.get("final_files") or [])
            ]
            record.update({"state": STATE_MOVED,
                           "final_dir": moved_dir,
                           "final_files": moved_files,
                           "final_file": moved_files[0] if moved_files else "",
                           "message": "已移动到库目录"})
            if existing and existing.get("item_id"):
                refreshed = EmbyLibrary(self._emby_library).refresh_item(
                    existing.get("server") or "", existing.get("item_id") or ""
                )
                record["emby_refreshed"] = refreshed
        else:
            message = f"整理完成但移动失败：{moved.get('reason')}"
            if self._organize_give_up(record, message):
                return
            # 保持在可推进状态，下一次轮询重试整理与移动。
            record.update({"state": STATE_FINISHED, "message": message})
        self._stamp(record)

    def _drain_queue(self, limit: int) -> Tuple[int, int]:
        """
        从队首取若干番号并执行提交。

        :param limit: 本轮最多处理的条数
        :return: ``(成功条数, 失败条数)``
        """
        with self._lock:
            items = list(self._queue[:max(0, limit)])
        if not items:
            return 0, 0
        ok = failed = 0
        for item in items:
            code = str(item.get("code") or "")
            batch_id = str(item.get("batch_id") or "")
            if not code:
                self._remove_queue_item(item)
                continue
            record = self._tasks.get(code)
            if record is not None and record.get("state") in ACTIVE_STATES:
                # 上次进程在提交途中中断：任务已经在推进，直接出队
                logger.info(f"番号云下载队列跳过 {code}：已有进行中的任务")
                self._remove_queue_item(item)
                continue
            try:
                logger.info(f"番号云下载队列提交 {code}（批次 {batch_id}）")
                result = self._submit_one(code, 0, batch_id=batch_id)
                if result.get("ok"):
                    ok += 1
                else:
                    failed += 1
            except Exception as error:
                failed += 1
                record = self._tasks.get(code) or self._new_record(
                    code, safe_name(code)
                )
                if batch_id:
                    record["batch_id"] = batch_id
                    record["batch_counted"] = False
                self._fail(record, f"{type(error).__name__}: {error}")
                with self._lock:
                    self._tasks[code] = record
            self._remove_queue_item(item)
        return ok, failed

    def _remove_queue_item(self, item: Dict[str, Any]) -> None:
        """按对象身份移除一个队列条目。"""
        with self._lock:
            self._queue = [queued for queued in self._queue if queued is not item]

    def _settle_batches(self) -> None:
        """统计批次内的终态任务，全部结束时发一条汇总通知。"""
        completed: List[Dict[str, Any]] = []
        seen: set = set()
        with self._lock:
            for record in self._tasks.values():
                batch_id = str(record.get("batch_id") or "")
                if not batch_id or record.get("batch_counted"):
                    continue
                state = record.get("state")
                if state not in TERMINAL_STATES:
                    continue
                record["batch_counted"] = True
                batch = self._batches.get(batch_id)
                if batch is None or batch.get("notified"):
                    continue
                if state in (STATE_ORGANIZED, STATE_MOVED):
                    batch["done"] = int(batch.get("done") or 0) + 1
                elif state == STATE_SKIPPED:
                    batch["skipped"] = int(batch.get("skipped") or 0) + 1
                else:
                    batch["failed"] = int(batch.get("failed") or 0) + 1
                    failures = batch.setdefault("failures", [])
                    if len(failures) < 200:
                        failures.append({
                            "code": record.get("code") or "",
                            "reason": record.get("message") or "",
                        })
                counted = (
                    int(batch.get("done") or 0)
                    + int(batch.get("failed") or 0)
                    + int(batch.get("skipped") or 0)
                )
                if counted >= int(batch.get("total") or 0) and batch_id not in seen:
                    seen.add(batch_id)
                    completed.append(batch)
        for batch in completed:
            if self._notify_batch_summary(batch):
                with self._lock:
                    batch["notified"] = True
                    batch["finished_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

    @staticmethod
    def _short_reason(reason: Any) -> str:
        """把多来源错误链压成一句可读的失败原因。"""
        text = str(reason or "").strip()
        text = re.sub(r"^[A-Za-z]+Error:\s*", "", text)
        if "；" in text:
            text = text.rsplit("；", 1)[-1]
        text = re.sub(r"^JavBus：", "", text)
        if len(text) > 60:
            text = text[:60] + "…"
        return text or "未知原因"

    def _notify_batch_summary(self, batch: Dict[str, Any]) -> bool:
        """发送一条批量任务汇总通知。"""
        total = int(batch.get("total") or 0)
        done = int(batch.get("done") or 0)
        failed = int(batch.get("failed") or 0)
        skipped = int(batch.get("skipped") or 0)
        lines = [f"本次提交 {total} 条：完成 {done} 条，失败 {failed} 条"]
        if skipped:
            lines[0] += f"，跳过 {skipped} 条"
        failures = [
            item for item in (batch.get("failures") or [])
            if isinstance(item, dict)
        ]
        if failures:
            shown = failures[:BATCH_FAILURE_DISPLAY]
            detail = "；".join(
                f"{item.get('code') or '未知'}（{self._short_reason(item.get('reason'))}）"
                for item in shown
            )
            if len(failures) > len(shown):
                detail += f"；等 {len(failures)} 条"
            lines.append("失败：" + detail)
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载批量完成：{total} 条",
                text="\n".join(lines),
            )
            logger.info(
                f"番号云下载批次完成 {batch.get('id')}："
                f"提交 {total}，完成 {done}，失败 {failed}，跳过 {skipped}"
            )
            return True
        except Exception as error:
            logger.warning(f"番号云下载批量汇总发送失败：{error}")
            return False

    def advance_tasks(self, minutes: int = 0) -> str:
        """
        定时推进所有进行中的任务。

        :param minutes: 兼容旧调度参数，此处未使用
        :return: 执行摘要
        """
        if not self._enabled:
            return "插件未启用"
        if not _ADVANCE_LOCK.acquire(blocking=False):
            logger.debug("番号云下载上一轮任务推进尚未结束，跳过本轮")
            return "上一轮任务推进尚未结束"
        try:
            queued_ok, queued_failed = self._drain_queue(QUEUE_BATCH)
            with self._lock:
                pending = [r for r in self._tasks.values() if r.get("state") in ACTIVE_STATES]
            if not pending:
                self._settle_batches()
                self._persist()
                if queued_ok or queued_failed:
                    summary = (
                        f"队列处理 {queued_ok + queued_failed} 条："
                        f"成功 {queued_ok}，失败 {queued_failed}"
                    )
                    logger.info(f"番号云下载 {summary}")
                    return summary
                logger.debug("番号云下载：队列为空，没有待推进的任务")
                return "没有待推进的任务"
            client = None
            done = failed = 0
            try:
                client = self._client()
                folders = {
                    record.get("folder") or self._download_dir
                    for record in pending
                    if record.get("state") != STATE_FINISHED
                }
                offline_indexes: Dict[str, Optional[Tuple[Dict[str, Any], Dict[str, Any]]]] = {}
                for folder in folders:
                    try:
                        offline_indexes[folder] = self._offline_index(
                            client.list_offline(folder)
                        )
                    except Cd2Error as error:
                        offline_indexes[folder] = None
                        logger.warning(f"番号云下载读取离线列表失败：{error}")

                for record in pending:
                    folder = record.get("folder") or self._download_dir
                    try:
                        if record.get("state") == STATE_FINISHED:
                            self._organize(client, record, 0)
                        elif offline_indexes.get(folder) is None:
                            record["message"] = "读取 CD2 离线列表失败，稍后重试"
                            self._stamp(record)
                        else:
                            self._advance_record(
                                client, record, 0,
                                offline_index=offline_indexes.get(folder),
                            )
                        if record.get("state") in (STATE_ORGANIZED, STATE_MOVED):
                            done += 1
                        elif record.get("state") == STATE_FAILED:
                            failed += 1
                    except Exception as error:
                        record["message"] = f"{type(error).__name__}: {error}"
                        self._stamp(record)
                        failed += 1
            finally:
                if client is not None:
                    client.close()
                self._settle_batches()
                self._persist()
            summary = f"推进 {len(pending)} 条：完成 {done}，失败 {failed}"
            if queued_ok or queued_failed:
                summary = (
                    f"队列提交 {queued_ok + queued_failed} 条"
                    f"（成功 {queued_ok}，失败 {queued_failed}）；{summary}"
                )
            logger.info(f"番号云下载 {summary}")
            return summary
        finally:
            _ADVANCE_LOCK.release()

    # ---------- 扩展点 ----------

    def get_api(self) -> List[Dict[str, Any]]:
        """注册插件 API。"""
        return [
            {
                "path": "/preview",
                "endpoint": self.api_preview,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "预览候选磁力",
                "description": "按当前数据源读取番号候选磁力并给出选种结果，不提交任务",
            },
            {
                "path": "/submit",
                "endpoint": self.api_submit,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "提交云下载",
                "description": "按番号提交云下载任务，支持逗号或换行分隔的多个番号",
            },
            {
                "path": "/tasks",
                "endpoint": self.api_tasks,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "查询任务列表",
                "description": "返回插件记录的任务状态",
            },
            {
                "path": "/proactive",
                "endpoint": self.api_proactive,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "主动洗版记录",
                "description": "返回低于 1080P 的库内条目与用户选择状态",
            },
            {
                "path": "/proactive/scan",
                "endpoint": self.api_proactive_scan,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "立即扫描主动洗版",
                "description": "扫描 Emby 成人库并发送低清提醒",
            },
            {
                "path": "/proactive/action",
                "endpoint": self.api_proactive_action,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "处理主动洗版",
                "description": "payload: {code, action}，action 为 upgrade 或 dismiss",
            },
            {
                "path": "/probe",
                "endpoint": self.api_probe,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "CD2 连通性探测",
                "description": "检查 CloudDrive2 连接、离线配额与任务数量",
            },
            {
                "path": "/advance",
                "endpoint": self.api_advance,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "立即推进任务",
                "description": "立即执行一次任务推进，不必等待定时服务",
            },
            {
                "path": "/sources",
                "endpoint": self.api_sources,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "数据源状态",
                "description": "返回当前数据源、JavRanking 缓存统计与可用能力",
            },
            {
                "path": "/libraries",
                "endpoint": self.api_libraries,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "Emby 媒体库列表",
                "description": "列出已启用的 Emby 媒体库，用于选择成人影片库",
            },
            {
                "path": "/library_lookup",
                "endpoint": self.api_library_lookup,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "Emby 番号查询",
                "description": "按番号查询 Emby 成人库条目与真实质量",
            },
            {
                "path": "/library_items",
                "endpoint": self.api_library_items,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "Emby 库条目样本",
                "description": "列出配置的 Emby 成人库前若干条，用于确认库名和路径",
            },
            {
                "path": "/ranking",
                "endpoint": self.api_ranking,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "JavRanking 榜单",
                "description": "读取本地缓存的榜单，如 main / javlibrary_top250 / javdb_top250",
            },
            {
                "path": "/lookup",
                "endpoint": self.api_lookup,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "番号缓存查询",
                "description": "查询 JavRanking 缓存中的番号、标题、详情页与磁力缓存状态",
            },
        ]

    def api_preview(self, code: str = "") -> Dict[str, Any]:
        """
        预览候选磁力。

        :param code: 番号
        :return: 预览结果
        """
        if not code:
            return {"ok": False, "reason": "缺少 code 参数"}
        try:
            return {"ok": True, "data": self.preview(code)}
        except (JavBusError, Cd2Error, JavRankingError) as error:
            return {"ok": False, "reason": str(error)}

    def api_submit(self, payload: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        提交云下载任务。

        :param payload: 含 codes（字符串或列表）与可选 wait_seconds
        :return: 提交结果
        """
        payload = payload or {}
        raw = payload.get("codes") or payload.get("code") or ""
        codes = raw if isinstance(raw, list) else [c for c in str(raw).replace(",", "\n").splitlines() if c.strip()]
        codes = [str(c).strip() for c in codes if str(c).strip()]
        if not codes:
            return {"ok": False, "reason": "缺少 codes 参数"}
        wait_seconds = int(payload.get("wait_seconds") or 0)
        upgrade = bool(payload.get("upgrade"))
        force = bool(payload.get("force"))
        return self.submit(
            codes, wait_seconds=wait_seconds, upgrade=upgrade, force=force
        )

    def api_tasks(self) -> Dict[str, Any]:
        """返回任务记录与待提交队列。"""
        with self._lock:
            items = sorted(
                self._tasks.values(), key=lambda r: r.get("updated_at", ""), reverse=True
            )
            queued = [
                {
                    "code": item.get("code"),
                    "batch_id": item.get("batch_id"),
                    "added_at": item.get("added_at"),
                    "position": index + 1,
                }
                for index, item in enumerate(self._queue)
            ]
        return {
            "ok": True,
            "total": len(items),
            "items": items,
            "queued_count": len(queued),
            "queued": queued,
        }

    def api_proactive(self) -> Dict[str, Any]:
        """返回主动洗版记录。"""
        with self._lock:
            items = sorted(
                self._proactive.values(),
                key=lambda r: r.get("updated_at", ""),
                reverse=True,
            )
        return {"ok": True, "total": len(items), "items": items}

    def api_proactive_scan(self) -> Dict[str, Any]:
        """立即执行一次主动洗版扫描。"""
        return self.scan_proactive()

    def api_proactive_action(self, payload: Dict[str, Any] = None) -> Dict[str, Any]:
        """处理主动洗版选择。"""
        payload = payload or {}
        code = str(payload.get("code") or "").strip()
        action = str(payload.get("action") or "").strip().lower()
        if not code or action not in ("upgrade", "dismiss"):
            return {"ok": False, "reason": "需要 code 和 action（upgrade/dismiss）"}
        return self.handle_proactive_action(action, code)

    def api_probe(self) -> Dict[str, Any]:
        """探测 CD2 连接与权限。"""
        try:
            client = self._client()
        except Cd2Error as error:
            return {"ok": False, "reason": str(error)}
        try:
            info = client.system_info()
            result: Dict[str, Any] = {
                "address": client.address,
                "user": getattr(info, "userName", ""),
                "version": getattr(info, "serverVersion", ""),
            }
            try:
                result["quota"] = client.quota(self._download_dir)
            except Cd2Error as error:
                result["quota_error"] = str(error)
            try:
                result["offline_tasks"] = len(client.list_offline(self._download_dir))
            except Cd2Error as error:
                result["offline_error"] = str(error)
            return {"ok": True, "data": result}
        finally:
            client.close()

    def api_sources(self) -> Dict[str, Any]:
        """返回数据源与 JavRanking 缓存状态。"""
        try:
            stats = JavRankingSource(self._javranking_dir).stats()
        except JavRankingError as error:
            stats = {"available": False, "error": str(error)}
        return {
            "ok": True,
            "data": {
                "mode": self._source_mode,
                "sites": {
                    "javbus": self._use_javbus,
                    "javranking": self._use_javranking,
                    "javdb": self._use_javdb,
                },
                "sources": [
                    {
                        "name": "javbus",
                        "available": True,
                        "capabilities": ["metadata", "magnets"],
                    },
                    {
                        "name": "javranking",
                        "available": bool(stats.get("available")),
                        "capabilities": ["metadata", "rankings", "magnets"],
                        "stats": stats,
                    },
                    {
                        "name": "javdb",
                        "available": bool(self._javdb_cookie),
                        "capabilities": ["metadata", "magnets"],
                    },
                ],
            },
        }

    def api_libraries(self) -> Dict[str, Any]:
        """返回 Emby 媒体库列表。"""
        try:
            items = EmbyLibrary().list_libraries()
            return {"ok": True, "total": len(items), "items": items}
        except Exception as error:
            return {"ok": False, "reason": f"{type(error).__name__}: {error}"}

    def api_library_lookup(self, code: str = "") -> Dict[str, Any]:
        """查询 Emby 成人库内的番号与质量。"""
        if not code:
            return {"ok": False, "reason": "缺少 code 参数"}
        hit = self._find_library_item(normalize_code(code))
        if hit is None:
            return {"ok": False, "reason": "Emby 成人库未找到该番号"}
        return {
            "ok": True,
            "data": {
                "server": hit.server,
                "item_id": hit.item_id,
                "path": hit.path,
                "quality": hit.quality,
                "relative_dir": hit.relative_dir,
                "cd2_dir": hit.cd2_dir,
            },
        }

    def api_library_items(self, limit: int = 20) -> Dict[str, Any]:
        """列出 Emby 成人库条目样本。"""
        try:
            items = EmbyLibrary(self._emby_library).list_items(max(1, min(int(limit), 200)))
            return {"ok": True, "total": len(items), "items": items}
        except Exception as error:
            return {"ok": False, "reason": f"{type(error).__name__}: {error}"}

    def api_ranking(self, list_name: str = "main", limit: int = 50) -> Dict[str, Any]:
        """读取 JavRanking 本地榜单。"""
        try:
            rows = JavRankingSource(self._javranking_dir).rankings(list_name, limit)
            return {"ok": True, "list": list_name, "total": len(rows), "items": rows}
        except JavRankingError as error:
            return {"ok": False, "reason": str(error)}

    def api_lookup(self, code: str = "") -> Dict[str, Any]:
        """查询番号在 JavRanking 缓存中的状态。"""
        if not code:
            return {"ok": False, "reason": "缺少 code 参数"}
        try:
            item = JavRankingSource(self._javranking_dir).lookup(code)
            if item is None:
                return {"ok": False, "reason": f"JavRanking 缓存中没有番号 {code}"}
            return {"ok": True, "data": item}
        except JavRankingError as error:
            return {"ok": False, "reason": str(error)}

    def api_advance(self) -> Dict[str, Any]:
        """立即推进一次任务。"""
        return {"ok": True, "summary": self.advance_tasks(0)}

    def _emby_library_options(self) -> List[dict]:
        """读取 Emby 媒体库列表，供设置页下拉选择。"""
        if self._emby_options and time.time() - self._emby_options_at < 300:
            return self._emby_options
        options: List[dict] = []
        try:
            for item in EmbyLibrary().list_libraries():
                name = str(item.get("name") or "").strip()
                if not name or any(option["value"] == name for option in options):
                    continue
                server = str(item.get("server") or "").strip()
                options.append({
                    "title": f"{name}（{server}）" if server else name,
                    "value": name,
                })
        except Exception as error:
            logger.warning(f"读取 Emby 媒体库列表失败：{type(error).__name__}: {error}")
        if self._emby_library and not any(
            option["value"] == self._emby_library for option in options
        ):
            options.append({
                "title": self._emby_library,
                "value": self._emby_library,
            })
        self._emby_options = options
        self._emby_options_at = time.time()
        return options

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单与默认值。"""
        library_options = self._emby_library_options()
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "allow_uncensored",
                                            "label": "允许无码破解资源",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "delete_junk_only",
                                            "label": "只保留主视频",
                                            "hint": "开启时只保留体积最大的视频和字幕，删除推广小视频；关闭时保留全部视频，只删非视频文件",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VCheckbox",
                                        "props": {
                                            "model": "use_javbus",
                                            "label": "JavBus",
                                            "hide-details": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VCheckbox",
                                        "props": {
                                            "model": "use_javranking",
                                            "label": "JavRanking（按需抓取磁力）",
                                            "hide-details": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "javranking_dir",
                                            "label": "JavRanking 缓存目录",
                                            "placeholder": "留空使用插件数据目录/javranking",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VCheckbox",
                                        "props": {
                                            "model": "use_javdb",
                                            "label": "JavDB（需 Cookie）",
                                            "hide-details": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 9},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "javdb_cookie",
                                            "label": "JavDB Cookie",
                                            "placeholder": "从浏览器复制整段 Cookie，必须包含 cf_clearance",
                                            "rows": 2,
                                            "hint": "只在取种请求里发送，不写日志；Cookie 失效会触发 Cloudflare 挑战，更新这里即可",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_filter_rules",
                                            "label": "启用识别筛选规则",
                                            "hint": "关闭后不排除无码、假4K、720P和小视频，直接按体积选最大",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_video_size_mb",
                                            "label": "候选最小体积（MB）",
                                            "type": "number",
                                            "hint": "小于该体积的磁力候选不下载，默认 2048MB（2GB）",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_host",
                                            "label": "CloudDrive2 地址",
                                            "placeholder": "填写 CloudDrive2 的 IP 地址或域名",
                                            "hint": "必填，填写运行 CloudDrive2 的设备地址",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_port",
                                            "label": "gRPC 端口",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_token",
                                            "label": "API 令牌（填账号密码时可不填）",
                                            "type": "password",
                                            "hint": "账号和密码都填写时，优先用账号登录",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_username",
                                            "label": "账号（与密码一起填写时优先）",
                                            "hint": "用于获取完整权限 JWT；不填则使用 API 令牌",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_password",
                                            "label": "密码",
                                            "type": "password",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "move_to_library",
                                            "label": "整理完成后移动到库目录",
                                            "hint": "默认关闭：先确认清理与改名结果无误，再开启",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "download_dir",
                                            "label": "云下载目录",
                                            "placeholder": "填写 CD2 中的下载目录",
                                            "hint": "必填，例如 /CloudDrive/下载；请按你的 CD2 挂载路径填写",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "emby_library",
                                            "label": "Emby 成人影片库",
                                            "items": library_options,
                                            "clearable": True,
                                            "hint": "从已配置的 Emby 媒体服务器读取；留空搜索全部电影/视频库",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "proactive_upgrade",
                                            "label": "主动洗版提醒",
                                            "hint": "开启后执行一次扫库，列出低于 1080P 的条目并询问；需同时开启洗版",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_upgrade",
                                            "label": "启用洗版",
                                            "hint": "开启后，请求洗版时才比较画质并替换",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3, "v-show": "enable_upgrade"},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "replace_existing",
                                            "label": "洗版通过后替换旧目录",
                                            "hint": "默认开启；替换前会删除库内同名旧目录",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "v-show": "enable_upgrade"},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "library_map",
                                            "label": "Emby 目录映射（洗版替换用）",
                                            "placeholder": "/mnt/strm/adult:/cd2/adult",
                                            "hint": "每行一条，格式：Emby 路径:CD2 路径；用于定位旧片所在目录并删除",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "library_dir",
                                            "label": "库目录（开启移动后生效）",
                                            "placeholder": "填写 CD2 中的库目录",
                                            "hint": "开启移动后必填，例如 /CloudDrive/媒体库",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "poll_seconds",
                                            "label": "轮询间隔（秒）",
                                            "type": "number",
                                            "hint": "默认 10，最小 5",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "candidate_timeout_minutes",
                                            "label": "下载超时（分钟）",
                                            "hint": "进入下载后超过该时间仍未完成，换下一个候选",
                                            "persistent-hint": True,
                                            "type": "number",
                                            "hint": "进入下载后计时，默认 5",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "candidate_queue_timeout_minutes",
                                            "label": "115 排队超时（分钟）",
                                            "hint": "提交后超过该时间仍未开始下载，换下一个候选",
                                            "persistent-hint": True,
                                            "type": "number",
                                            "hint": "从提交算起，默认 10",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_candidates",
                                            "label": "最多尝试候选数",
                                            "type": "number",
                                            "hint": "默认 3",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "source_base",
                                            "label": "种子源地址",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {"type": "info", "variant": "tonal"},
                        "text": "数据源：JavBus 直连可用；JavRanking 读取本地缓存，"
                                "缺少磁力时按需抓取详情页，失败回退旧缓存；"
                                "JavDB 需要填写含 cf_clearance 的 Cookie，"
                                "它的磁力名称只有番号、没有分辨率，按体积参与选种。"
                                "选种顺序：4K > 2K > 1080P > 720P，同档中字优先、再按体积；"
                                "无码破解、AI拉伸/修复、假4K、480P 与小于 2GB 的候选默认排除；"
                                "洗版时低于 720P 自动换种。"
                                "任务由定时服务异步推进。",
                    },
                ],
            }
        ], {
            "enabled": False,
            "use_javbus": True,
            "use_javranking": False,
            "use_javdb": False,
            "javdb_cookie": "",
            "javranking_dir": "",
            "emby_library": "",
            "library_map": "",
            "enable_upgrade": False,
            "replace_existing": True,
            "proactive_upgrade": False,
            "enable_filter_rules": True,
            "min_video_size_mb": 2048,
            "source_base": "https://www.javbus.com",
            "cd2_host": "",
            "cd2_port": 19798,
            "cd2_token": "",
            "cd2_username": "",
            "cd2_password": "",
            "download_dir": "",
            "library_dir": "",
            "move_to_library": False,
            "allow_uncensored": False,
            "allow_junk": False,
            "delete_junk_only": True,
            "poll_seconds": DEFAULT_POLL_SECONDS,
            "candidate_timeout_minutes": DEFAULT_CANDIDATE_TIMEOUT_MINUTES,
            "candidate_queue_timeout_minutes": DEFAULT_QUEUE_TIMEOUT_MINUTES,
            "max_candidates": DEFAULT_MAX_CANDIDATES,
            "max_wait_minutes": 240,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页。"""
        with self._lock:
            items = sorted(
                self._tasks.values(), key=lambda r: r.get("updated_at", ""), reverse=True
            )[:10]
            queued_count = len(self._queue)
        lines = [
            f"{r.get('code')}｜{r.get('state')}｜{r.get('percent', 0)}%｜{r.get('message') or ''}"
            for r in items
        ]
        try:
            stats = JavRankingSource(self._javranking_dir).stats()
            if stats.get("available"):
                cache_text = (
                    f"数据源：{self._source_mode}｜JavRanking 缓存："
                    f"{stats.get('videos', 0)} 部，详情 {stats.get('details', 0)} 条"
                )
            else:
                cache_text = f"数据源：{self._source_mode}｜JavRanking 缓存未就绪"
        except JavRankingError as error:
            cache_text = f"数据源：{self._source_mode}｜JavRanking 缓存读取失败：{error}"
        proactive_text = ""
        if self._proactive_enabled:
            with self._lock:
                records = list(self._proactive.values())
            pending = [
                r for r in records
                if r.get("status") in ("pending", "notified")
            ]
            upgrading = [r for r in records if r.get("status") == "upgrading"]
            declined = [r for r in records if r.get("status") == "declined"]
            proactive_text = (
                f"主动洗版：待处理 {len(pending)}，进行中 {len(upgrading)}，"
                f"已忽略 {len(declined)}"
            )
            if pending:
                proactive_text += "\n" + "、".join(
                    f"{r.get('code')} {r.get('height')}P" for r in pending[:10]
                )
        pages: List[dict] = [
            {
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal"},
                "text": self._status,
            },
            {
                "component": "VAlert",
                "props": {"type": "secondary", "variant": "tonal"},
                "text": cache_text,
            },
        ]
        if proactive_text:
            pages.append(
                {
                    "component": "VAlert",
                    "props": {"type": "warning", "variant": "tonal"},
                    "text": proactive_text,
                }
            )
        if queued_count:
            pages.append(
                {
                    "component": "VAlert",
                    "props": {"type": "info", "variant": "tonal"},
                    "text": f"待提交队列：{queued_count} 条",
                }
            )
        if lines:
            pages.append(
                {
                    "component": "VAlert",
                    "props": {"type": "secondary", "variant": "tonal"},
                    "text": "最近任务：\n" + "\n".join(lines),
                }
            )
        return pages

    def stop_service(self) -> None:
        """停止服务并落盘任务记录。"""
        try:
            from app.runtime.events import eventmanager
            from app.schemas.types import EventType

            eventmanager.remove_event_listener(
                EventType.MessageAction, self._on_message_action
            )
        except Exception:
            pass
        try:
            self._persist()
            self._save_proactive()
        except Exception as error:  # pragma: no cover - 落盘失败不应阻断卸载
            logger.warning(f"番号云下载保存任务记录失败：{error}")
