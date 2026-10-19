"""
番号云下载插件。

按番号从 JavBus 取种，经自建 CloudDrive2 gRPC 接入提交 115 云下载，
落盘后清理广告文件、重命名并按配置移动到库目录。
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.plugins import _PluginBase
from app.sdk.logging import logger

from .cd2 import Cd2Client, Cd2Error, status_name
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


class JavCloudDownload(_PluginBase):
    """番号云下载插件主类。"""

    plugin_name = "番号云下载"
    plugin_desc = "按番号从 JavBus 取种，经 CloudDrive2 云下载到 115，完成后清理、改名并可移动到库目录。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    plugin_version = "0.1.3"
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
    _download_dir = "/115open/云下载"
    _library_dir = ""
    _move_enabled = False
    _allow_uncensored = False
    _allow_junk = False
    _delete_junk_only = True
    _poll_minutes = 5
    _max_wait_minutes = 240
    _status = "未启用"

    def __init__(self) -> None:
        """初始化实例状态。"""
        super().__init__()
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

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
        self._cd2_host = (config.get("cd2_host") or "").strip()
        self._cd2_port = int(config.get("cd2_port") or 19798)
        self._cd2_token = (config.get("cd2_token") or "").strip()
        self._cd2_username = (config.get("cd2_username") or "").strip()
        self._cd2_password = (config.get("cd2_password") or "").strip()
        self._download_dir = (config.get("download_dir") or "/115open/云下载").rstrip("/")
        self._library_dir = (config.get("library_dir") or "").strip().rstrip("/")
        self._move_enabled = bool(config.get("move_to_library"))
        self._allow_uncensored = bool(config.get("allow_uncensored"))
        self._allow_junk = bool(config.get("allow_junk"))
        self._delete_junk_only = bool(config.get("delete_junk_only", True))
        self._poll_minutes = max(1, int(config.get("poll_minutes") or 5))
        self._max_wait_minutes = max(5, int(config.get("max_wait_minutes") or 240))

        stored = self.get_data("tasks") or {}
        self._tasks = stored if isinstance(stored, dict) else {}

        if not self._enabled:
            self._status = "未启用"
            return
        if not self._cd2_host:
            self._status = "配置不完整：缺少 CloudDrive2 地址"
            return
        if not self._cd2_token and not self._cd2_username:
            self._status = "配置不完整：需要 API 令牌或账号密码"
            return
        self._status = f"运行中，云下载目录 {self._download_dir}"

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
                "kwargs": {"minutes": self._poll_minutes},
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
            "message": "",
        }

    # ---------- 核心动作 ----------

    def preview(self, code: str) -> Dict[str, Any]:
        """
        只抓取并展示候选，不提交任何任务。

        :param code: 番号
        :return: 候选与选种结果
        """
        normalized = normalize_code(code)
        source = JavBusSource(base_url=self._source_base)
        try:
            detail = source.fetch_detail(normalized)
        finally:
            source.close()
        picked = select(
            detail["magnets"],
            allow_uncensored=self._allow_uncensored,
            allow_junk=self._allow_junk,
        )
        return {
            "code": detail.get("code") or normalized,
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
        chosen = detail.get("chosen")
        if not chosen:
            raise JavBusError(f"{code} 没有可用磁力")
        name = safe_name(detail.get("code") or code)
        record = self._tasks.get(code) or self._new_record(code, name)
        record.update(
            {
                "name": name,
                "tier": detail.get("tier"),
                "magnet_name": chosen.get("name"),
                "size_text": chosen.get("size_text"),
                "info_hash": chosen.get("info_hash") or info_hash_of(chosen.get("magnet", "")),
                "title": detail.get("title"),
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
            submission = client.add_offline(chosen["magnet"], self._download_dir)
            record.update({
                "state": STATE_SUBMITTED,
                "message": f"已提交（{submission}）",
                "submitted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "attempts": 0,
                "organize_attempts": 0,
            })
            self._stamp(record)
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

    def _advance_record(self, client: Cd2Client, record: Dict[str, Any],
                        wait_seconds: int = 0) -> None:
        """
        推进单条任务：查状态 → 整理 → 移动。

        :param client: CD2 客户端
        :param record: 任务记录
        :param wait_seconds: 单次等待秒数；0 表示只检查一次
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
            record.update({"state": STATE_FAILED, "message": str(error)})
            self._stamp(record)
            return

        if task is None:
            if self._wait_exceeded(record):
                record.update({
                    "state": STATE_FAILED,
                    "message": f"下载等待超过 {self._max_wait_minutes} 分钟，未查询到离线任务",
                })
                self._stamp(record)
                return
            record["message"] = "尚未查询到离线任务"
            self._stamp(record)
            return

        record["task_name"] = task.name
        state = status_name(task.status)
        record["offline_state"] = state
        record["percent"] = round(float(task.percendDone or 0), 1)
        if state == "OFFLINE_ERROR":
            record.update({"state": STATE_FAILED, "message": "云下载任务失败"})
            self._stamp(record)
            return
        if state != "OFFLINE_FINISHED":
            if self._wait_exceeded(record):
                record.update({
                    "state": STATE_FAILED,
                    "message": f"下载等待超过 {self._max_wait_minutes} 分钟"
                               f"（最后状态 {state} {record['percent']}%）",
                })
                self._stamp(record)
                return
            record.update({"state": STATE_DOWNLOADING,
                           "message": f"下载中 {record['percent']}%"})
            self._stamp(record)
            return

        record["state"] = STATE_FINISHED
        record["message"] = "下载完成，开始整理"
        self._stamp(record)
        self._organize(client, record, wait_seconds)

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

        tidied = tidy_dir(client, target, name, delete_junk_only=self._delete_junk_only)
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
            record.update({"state": STATE_MOVED,
                           "final_dir": moved.get("final_dir"),
                           "final_file": f"{moved.get('final_dir')}/{name}{tidied.get('ext', '')}",
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

        :param minutes: 服务注入的间隔参数，此处未使用
        :return: 执行摘要
        """
        if not self._enabled:
            return "插件未启用"
        with self._lock:
            pending = [r for r in self._tasks.values() if r.get("state") in ACTIVE_STATES]
        if not pending:
            return "没有待推进的任务"
        client = self._client()
        done = failed = 0
        try:
            for record in pending:
                try:
                    self._advance_record(client, record, 0)
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
                "description": "按番号抓取 JavBus 候选磁力并给出选种结果，不提交任务",
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
        except (JavBusError, Cd2Error) as error:
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cd2_host",
                                            "label": "CloudDrive2 地址",
                                            "placeholder": "192.168.5.103",
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
                                            "placeholder": "/115open/云下载",
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
                                            "placeholder": "/115open/adult",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "poll_minutes",
                                            "label": "任务推进间隔（分钟）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_wait_minutes",
                                            "label": "下载最长等待（分钟）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
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
                        "text": "选种顺序：第一会所新片 > 4K > 中文字幕 > 体积最大；"
                                "无码破解与广告资源默认排除。任务由定时服务异步推进。",
                    },
                ],
            }
        ], {
            "enabled": False,
            "source_base": "https://www.javbus.com",
            "cd2_host": "192.168.5.103",
            "cd2_port": 19798,
            "cd2_token": "",
            "cd2_username": "",
            "cd2_password": "",
            "download_dir": "/115open/云下载",
            "library_dir": "",
            "move_to_library": False,
            "allow_uncensored": False,
            "allow_junk": False,
            "delete_junk_only": True,
            "poll_minutes": 5,
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
        pages: List[dict] = [
            {
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal"},
                "text": self._status,
            }
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
