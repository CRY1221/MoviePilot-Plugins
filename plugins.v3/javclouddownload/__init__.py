"""
番号云下载插件。

按番号从 JavBus 取种，经自建 CloudDrive2 gRPC 接入提交 115 云下载，
落盘后清理广告文件、重命名并按配置移动到库目录。
"""

from __future__ import annotations

import threading
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from app.plugins import _PluginBase
from app.sdk.logging import logger
from app.schemas.types import MessageType

from .cd2 import Cd2Client, Cd2Error, status_name
from .javranking import JavRankingError, JavRankingSource
from .pipeline import (
    ACTIVE_STATES,
    STATE_DOWNLOADING,
    STATE_FAILED,
    STATE_FINISHED,
    STATE_MOVED,
    STATE_ORGANIZED,
    STATE_SUBMITTED,
    dir_has_video,
    locate_download_dir,
    move_dir,
    safe_name,
    tidy_dir,
)
from .selector import select
from .source import JavBusSource, JavBusError, info_hash_of, normalize_code

MAX_RECORDS = 200

# 整理阶段连续失败多少次后停止重试
MAX_ORGANIZE_ATTEMPTS = 5

# 删除旧离线任务连续失败多少次后停止换种
MAX_REMOVE_ATTEMPTS = 3

DEFAULT_POLL_SECONDS = 10
MIN_POLL_SECONDS = 5
DEFAULT_CANDIDATE_TIMEOUT_MINUTES = 5
DEFAULT_QUEUE_TIMEOUT_MINUTES = 10
DEFAULT_MAX_CANDIDATES = 3


class JavCloudDownload(_PluginBase):
    """番号云下载插件主类。"""

    plugin_name = "番号云下载"
    plugin_desc = "按番号从 JavBus 取种，经 CloudDrive2 云下载到 115，完成后清理、改名并可移动到库目录。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    plugin_version = "0.2.0"
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
    _javranking_dir = ""
    _enable_filter_rules = True
    _min_video_size_mb = 300
    _min_video_size_bytes = 300 * 1024 ** 2
    _status = "未启用"

    def __init__(self) -> None:
        """初始化实例状态。"""
        super().__init__()
        self._tasks: Dict[str, Dict[str, Any]] = {}
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
        self._source_mode = (config.get("source_mode") or "javbus").strip().lower()
        if self._source_mode not in ("javbus", "javranking", "auto"):
            self._source_mode = "javbus"
        default_cache = str(Path(self.get_data_path()) / "javranking")
        self._javranking_dir = (config.get("javranking_dir") or default_cache).strip()
        self._enable_filter_rules = bool(config.get("enable_filter_rules", True))
        raw_min_size = config.get("min_video_size_mb")
        self._min_video_size_mb = 300 if raw_min_size is None else max(0, int(raw_min_size))
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

        if not self._enabled:
            self._status = "未启用"
            return
        if not self._cd2_host:
            self._status = "配置不完整：缺少 CloudDrive2 地址，请在设置中填写"
            return
        if not self._cd2_token and not self._cd2_username:
            self._status = "配置不完整：需要 API 令牌或账号密码"
            return
        if not self._download_dir:
            self._status = "配置不完整：缺少云下载目录，请在设置中填写"
            return
        self._status = (
            f"运行中，数据源 {self._source_mode}，云下载目录 {self._download_dir}"
        )

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

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
        """把任务记录写回插件数据。"""
        with self._lock:
            if len(self._tasks) > MAX_RECORDS:
                ordered = sorted(
                    self._tasks.items(), key=lambda kv: kv[1].get("updated_at", ""), reverse=True
                )[:MAX_RECORDS]
                self._tasks = dict(ordered)
            self.save_data("tasks", self._tasks)

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
            "notified_finish": False,
            "notified_failure": False,
            "message": "",
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

    def _prepare_candidates(self, detail: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        把选种结果整理成可重试的候选列表。

        按 infoHash 去重，最多保留 ``max_candidates`` 条。

        :param detail: ``preview`` 返回的详情
        :return: 候选列表
        """
        raw = list(detail.get("candidates") or [])
        chosen = detail.get("chosen")
        if chosen and chosen not in raw:
            raw.insert(0, chosen)

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
            if len(result) >= self._max_candidates:
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
        """发送一次下载完成通知。"""
        if record.get("notified_finish"):
            return
        record["notified_finish"] = True
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载完成：{record.get('code')}",
                text=(
                    f"{record.get('name') or record.get('code')} "
                    f"{record.get('size_text') or ''} 已下载完成，开始整理"
                ).strip(),
            )
        except Exception as error:  # 通知失败不影响下载与整理
            logger.warning(f"番号云下载完成通知发送失败：{error}")

    def _notify_failure(self, record: Dict[str, Any], message: str) -> None:
        """发送一次最终失败通知。"""
        if record.get("notified_failure"):
            return
        record["notified_failure"] = True
        try:
            self.post_message(
                mtype=MessageType.Plugin,
                title=f"番号云下载失败：{record.get('code')}",
                text=f"{record.get('name') or record.get('code')}：{message}",
            )
        except Exception as error:
            logger.warning(f"番号云下载失败通知发送失败：{error}")

    def _fail(self, record: Dict[str, Any], message: str) -> None:
        """把任务标记为最终失败并发通知。"""
        record.update({"state": STATE_FAILED, "message": message})
        self._stamp(record)
        self._notify_failure(record, message)

    # ---------- 核心动作 ----------

    def _fetch_detail(self, code: str) -> Dict[str, Any]:
        """
        按配置的数据源读取番号详情。

        ``auto`` 优先用 JavRanking 缓存；缓存没有磁力或不可用时回退 JavBus。

        :param code: 番号
        :return: 详情字典
        """
        normalized = normalize_code(code)
        if self._source_mode in ("javranking", "auto"):
            try:
                detail = JavRankingSource(self._javranking_dir).fetch_detail(normalized)
                if self._source_mode == "javranking":
                    if not detail.get("magnets"):
                        raise JavRankingError(
                            f"JavRanking 缓存没有 {normalized} 的磁力；"
                            "请先刷新详情页，或把数据源改为 auto/javbus"
                        )
                    return detail
                if detail.get("magnets"):
                    return detail
                logger.info(f"JavRanking 缓存无磁力，回退 JavBus：{normalized}")
            except JavRankingError as error:
                if self._source_mode == "javranking":
                    raise
                logger.info(f"JavRanking 缓存不可用，回退 JavBus：{error}")

        source = JavBusSource(base_url=self._source_base)
        try:
            detail = source.fetch_detail(normalized)
            detail["source"] = "javbus"
            return detail
        finally:
            source.close()

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

    def submit(self, codes: List[str], wait_seconds: int = 0) -> Dict[str, Any]:
        """
        提交番号云下载任务。

        取种与提交离线在本次调用内完成；若给出 ``wait_seconds`` 会同步推进一段时间，
        否则交由定时服务继续推进。

        :param codes: 番号列表
        :param wait_seconds: 同步等待秒数，0 表示不等待
        :return: 每个番号的处理结果
        """
        results: List[Dict[str, Any]] = []
        for raw in codes:
            normalized = normalize_code(raw)
            if not normalized:
                continue
            try:
                results.append(self._submit_one(normalized, wait_seconds))
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

    def _submit_one(self, code: str, wait_seconds: int) -> Dict[str, Any]:
        """
        处理单个番号的提交。

        :param code: 归一化番号
        :param wait_seconds: 同步等待秒数
        :return: 处理结果
        """
        detail = self.preview(code)
        if not detail.get("chosen") and not detail.get("candidates"):
            raise JavBusError(f"{code} 没有可用磁力")
        if not self._download_dir:
            raise Cd2Error("未配置云下载目录，请先在插件设置中填写")
        candidates = self._prepare_candidates(detail)
        if not candidates:
            raise JavBusError(f"{code} 没有可提交的候选磁力")
        name = safe_name(detail.get("code") or code)
        record = self._tasks.get(code) or self._new_record(code, name)
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
            }
        )
        self._stamp(record)
        with self._lock:
            self._tasks[code] = record

        client = self._client()
        try:
            existing_dir = f"{self._download_dir.rstrip('/')}/{name}"
            if client.find(existing_dir) is not None and dir_has_video(client, existing_dir):
                record.update(
                    {
                        "state": STATE_ORGANIZED,
                        "final_dir": existing_dir,
                        "message": "云下载目录已存在同名成片，跳过提交",
                    }
                )
                self._stamp(record)
                return {
                    "code": code,
                    "ok": True,
                    "skipped": True,
                    "name": name,
                    "reason": record["message"],
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
                          task: Optional[Any] = None) -> None:
        """
        删除当前候选的离线任务，然后提交下一条候选。

        删除失败时不提交下一条，避免两个任务同时下载。

        :param client: CD2 客户端
        :param record: 任务记录
        :param reason: 换种原因
        :param result: 写进 candidate_attempts 的结果
        :param task: 当前离线任务；None 表示列表中还没出现
        """
        folder = record.get("folder") or self._download_dir
        info_hash = record.get("info_hash") or ""
        if task is not None and info_hash:
            try:
                removed = client.remove_offline(
                    folder, [info_hash], delete_files=True
                )
                error_text = "" if removed else "删除接口返回失败"
            except Exception as error:
                removed = False
                error_text = f"{type(error).__name__}: {error}"
            if not removed:
                attempts = int(record.get("remove_attempts") or 0) + 1
                record["remove_attempts"] = attempts
                record["message"] = (
                    f"删除旧离线任务失败，重试 {attempts}/{MAX_REMOVE_ATTEMPTS}"
                )
                if attempts >= MAX_REMOVE_ATTEMPTS:
                    self._fail(
                        record,
                        f"删除旧离线任务失败：{error_text or '未知错误'}",
                    )
                else:
                    self._stamp(record)
                return

        record["remove_attempts"] = 0
        self._record_attempt(record, result, reason)
        candidates = record.get("candidates") or []
        next_index = int(record.get("candidate_index") or 0) + 1
        if next_index >= len(candidates):
            self._fail(
                record,
                f"{len(candidates)} 条候选均未在时限内完成",
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
                    f"任务启动超过 {self._candidate_queue_timeout_minutes} 分钟",
                    task=None,
                )
            else:
                record.update({"state": STATE_SUBMITTED,
                               "message": "等待 115 接收任务"})
                self._stamp(record)
            return

        record["task_name"] = task.name
        state = status_name(task.status)
        record["offline_state"] = state
        record["percent"] = round(float(task.percendDone or 0), 1)

        if state == "OFFLINE_FINISHED":
            record["state"] = STATE_FINISHED
            record["message"] = "下载完成，开始整理"
            self._record_attempt(record, "finished", f"{record['percent']}%")
            self._stamp(record)
            self._notify_finish(record)
            self._organize(client, record, wait_seconds)
            return

        if state == "OFFLINE_ERROR":
            self._switch_candidate(client, record, "离线任务失败", result="error", task=task)
            return

        if state == "OFFLINE_DOWNLOADING":
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
                    task=task,
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

        # OFFLINE_INIT / OFFLINE_UNKNOWN：仍在排队，按启动超时处理
        if self._elapsed_minutes(record.get("submitted_at")) >= self._candidate_queue_timeout_minutes:
            self._switch_candidate(
                client, record,
                f"任务启动超过 {self._candidate_queue_timeout_minutes} 分钟（{state}）",
                task=task,
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
        self._notify_failure(record, record["message"])
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
            message = "未找到落盘目录"
            if self._organize_give_up(record, message):
                return
            record.update({"state": STATE_FINISHED, "message": f"{message}，稍后重试"})
            self._stamp(record)
            return

        tidied = tidy_dir(
            client, target, name,
            delete_junk_only=self._delete_junk_only,
            enable_filter_rules=self._enable_filter_rules,
            min_video_size=self._min_video_size_bytes,
        )
        record["tidy"] = tidied
        if not tidied.get("ok"):
            message = f"整理失败：{tidied.get('reason')}"
            if self._organize_give_up(record, message):
                return
            record.update({"state": STATE_FINISHED, "message": message})
            self._stamp(record)
            return

        final_dir = tidied["final_dir"]
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
        moved = move_dir(client, final_dir, library)
        record["move"] = moved
        if moved.get("ok"):
            moved_dir = moved.get("final_dir") or ""
            moved_files = [
                f"{moved_dir}/{PurePosixPath(item).name}"
                for item in (record.get("final_files") or [])
            ]
            record.update({"state": STATE_MOVED,
                           "final_dir": moved_dir,
                           "final_files": moved_files,
                           "final_file": moved_files[0] if moved_files else "",
                           "message": "已移动到库目录"})
        else:
            message = f"整理完成但移动失败：{moved.get('reason')}"
            if self._organize_give_up(record, message):
                return
            # 保持在可推进状态，下一次轮询重试整理与移动。
            record.update({"state": STATE_FINISHED, "message": message})
        self._stamp(record)

    def advance_tasks(self, minutes: int = 0) -> str:
        """
        定时推进所有进行中的任务。

        :param minutes: 兼容旧调度参数，此处未使用
        :return: 执行摘要
        """
        if not self._enabled:
            return "插件未启用"
        if not self._poll_lock.acquire(blocking=False):
            return "上一轮任务推进尚未结束"
        try:
            with self._lock:
                pending = [r for r in self._tasks.values() if r.get("state") in ACTIVE_STATES]
            if not pending:
                return "没有待推进的任务"
            client = self._client()
            done = failed = 0
            try:
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
                client.close()
                self._persist()
            summary = f"推进 {len(pending)} 条：完成 {done}，失败 {failed}"
            logger.info(f"番号云下载 {summary}")
            return summary
        finally:
            self._poll_lock.release()

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
        return self.submit(codes, wait_seconds=wait_seconds)

    def api_tasks(self) -> Dict[str, Any]:
        """返回任务记录。"""
        with self._lock:
            items = sorted(
                self._tasks.values(), key=lambda r: r.get("updated_at", ""), reverse=True
            )
        return {"ok": True, "total": len(items), "items": items}

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
                "sources": [
                    {
                        "name": "javbus",
                        "available": True,
                        "capabilities": ["metadata", "magnets"],
                    },
                    {
                        "name": "javranking",
                        "available": bool(stats.get("available")),
                        "capabilities": ["metadata", "rankings", "magnets_if_cached"],
                        "stats": stats,
                    },
                ],
            },
        }

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

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单与默认值。"""
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
                                "props": {"cols": 12, "md": 5},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "source_mode",
                                            "label": "数据源",
                                            "items": [
                                                {"title": "JavBus（直连）", "value": "javbus"},
                                                {"title": "JavRanking（本地缓存）", "value": "javranking"},
                                                {"title": "自动（JavRanking 优先，缺失回退 JavBus）", "value": "auto"},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 7},
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
                                            "label": "小视频阈值（MB）",
                                            "type": "number",
                                            "hint": "小于该体积的视频视为垃圾，默认 300MB",
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
                                            "label": "API 令牌",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_username",
                                            "label": "账号（无令牌时使用）",
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
                                            "label": "单条候选超时（分钟）",
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
                                            "label": "任务启动超时（分钟）",
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
                                "没有详情页缓存的番号无法取磁力，可改用「自动」回退 JavBus。"
                                "选种顺序：第一会所新片 > 中文字幕 > 4K（≥5GB）> 体积最大；"
                                "无码破解、假4K、720P 与小于阈值的小视频默认排除。"
                                "任务由定时服务异步推进。",
                    },
                ],
            }
        ], {
            "enabled": False,
            "source_mode": "javbus",
            "javranking_dir": "",
            "enable_filter_rules": True,
            "min_video_size_mb": 300,
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
            self._persist()
        except Exception as error:  # pragma: no cover - 落盘失败不应阻断卸载
            logger.warning(f"番号云下载保存任务记录失败：{error}")
