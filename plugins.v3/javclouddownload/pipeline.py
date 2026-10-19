"""
云下载流水线：取种 → 选种 → 提交离线 → 轮询 → 清理 → 改名 → 移动。

模块与插件运行时分离开，便于独立测试与复用。
"""

from __future__ import annotations

import re
import time
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional

from app.sdk.logging import logger

from .cd2 import Cd2Client, Cd2Error, status_name
from .selector import pick_video, select
from .source import JavBusSource, info_hash_of, normalize_code

# 任务状态
STATE_PENDING = "pending"
STATE_SUBMITTED = "submitted"
STATE_DOWNLOADING = "downloading"
STATE_FINISHED = "finished"
STATE_ORGANIZED = "organized"
STATE_MOVED = "moved"
STATE_FAILED = "failed"

ACTIVE_STATES = (STATE_PENDING, STATE_SUBMITTED, STATE_DOWNLOADING, STATE_FINISHED)

# 整理目录名时的非法字符
INVALID_NAME_CHARS = re.compile(r'[\\/:*?"<>|]')

# 视频扩展名集合，与选种模块保持一致
VIDEO_EXT_SET = {
    ".mp4", ".mkv", ".avi", ".wmv", ".ts", ".m2ts", ".mov",
    ".flv", ".rmvb", ".rm", ".mpg", ".mpeg", ".webm", ".iso",
}

# 字幕扩展名集合：整理时随视频保留
SUBTITLE_EXT_SET = {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt"}


def safe_name(value: str) -> str:
    """
    清理名称中的路径非法字符。

    :param value: 原始名称
    :return: 可安全用作文件名/目录名的字符串
    """
    cleaned = INVALID_NAME_CHARS.sub("_", (value or "").strip())
    return cleaned.strip(" .") or "unnamed"


def collect_files(client: Cd2Client, path: str, depth: int = 2) -> List[tuple]:
    """
    递归收集目录下的文件三元组。

    :param client: CD2 客户端
    :param path: 起始目录
    :param depth: 剩余递归层数
    :return: ``(path, size, name)`` 列表
    """
    files: List[tuple] = []
    if depth < 0:
        return files
    try:
        items = client.list_dir(path)
    except Cd2Error as error:
        logger.warning(f"列目录失败 {path}：{error}")
        return files
    for item in items:
        child = f"{path.rstrip('/')}/{item.name}"
        if getattr(item, "isDirectory", False):
            files.extend(collect_files(client, child, depth - 1))
        else:
            files.append((child, int(getattr(item, "size", 0) or 0), item.name))
    return files


def locate_download_dir(client: Cd2Client, folder: str, name: str,
                        task_name: str = "") -> Optional[str]:
    """
    在云下载目录下定位任务落盘目录。

    优先使用离线任务名对应目录：同一番号重复下载时，旧目录可能仍存在，
    若先匹配番号会整理到上一次的目录，遗漏本次下载内容。

    :param client: CD2 客户端
    :param folder: 云下载根目录
    :param name: 目标番号名
    :param task_name: 离线任务名（目录名可能与番号不同）
    :return: 实际目录路径；未找到返回 None
    """
    for candidate in filter(None, (task_name, name)):
        path = f"{folder.rstrip('/')}/{candidate}"
        if client.find(path) is not None:
            return path
    return None


def dir_has_video(client: Cd2Client, path: str) -> bool:
    """
    判断目录内是否已有视频文件。

    :param client: CD2 客户端
    :param path: 目录路径
    :return: 至少存在一个视频文件时返回 True
    """
    try:
        files = collect_files(client, path, depth=1)
    except Cd2Error:
        return False
    return pick_video(files) is not None


def tidy_dir(client: Cd2Client, target: str, name: str,
             delete_junk_only: bool = True) -> Dict[str, Any]:
    """
    清理目录内的广告文件并重命名视频、字幕与目录。

    字幕文件默认随主视频保留，避免误删中文字幕。

    :param client: CD2 客户端
    :param target: 目标目录
    :param name: 目标番号名
    :param delete_junk_only: 为 True 时保留全部视频与字幕，只删其余文件
    :return: 整理结果
    """
    files = collect_files(client, target)
    if not files:
        return {"ok": False, "reason": "目录为空或未落盘"}

    video = pick_video(files)
    if video is None:
        return {"ok": False, "reason": "未找到视频文件", "files": [f[2] for f in files]}

    keep_path, keep_size, keep_name = video
    subtitles = [
        item for item in files
        if PurePosixPath(item[2]).suffix.lower() in SUBTITLE_EXT_SET
    ]
    if delete_junk_only:
        # 保留全部视频与字幕，删除广告、链接等其余文件
        keep_paths = {keep_path} | {item[0] for item in subtitles}
        to_delete = [
            (path, fname, size)
            for path, size, fname in files
            if path not in keep_paths
        ]
    else:
        # 只保留体积最大的视频及其字幕
        keep_paths = {keep_path} | {item[0] for item in subtitles}
        to_delete = [item for item in files if item[0] not in keep_paths]

    deleted: List[str] = []
    for path, fname, size in to_delete:
        try:
            if client.delete(path):
                deleted.append(f"{fname} ({size})")
        except Cd2Error as error:
            logger.warning(f"删除失败 {path}：{error}")

    ext = PurePosixPath(keep_name).suffix
    target_file_name = f"{name}{ext}"
    renamed_file = None
    if keep_name != target_file_name:
        if client.rename(keep_path, target_file_name):
            renamed_file = target_file_name

    # 字幕跟随主视频改名，保留语言后缀（如 .zh.srt）
    old_stem = PurePosixPath(keep_name).stem
    renamed_subtitles: List[str] = []
    for path, _size, fname in subtitles:
        stem = PurePosixPath(fname).stem
        suffix = PurePosixPath(fname).suffix
        if not stem.lower().startswith(old_stem.lower()):
            continue
        new_name = f"{name}{stem[len(old_stem):]}{suffix}"
        if new_name == fname:
            continue
        try:
            if client.rename(path, new_name):
                renamed_subtitles.append(new_name)
        except Cd2Error as error:
            logger.warning(f"字幕改名失败 {path}：{error}")

    folder_name = target.rstrip("/").split("/")[-1]
    final_dir = target
    renamed_dir = None
    skipped_dir_rename = None
    if folder_name != name:
        parent = str(PurePosixPath(target).parent)
        sibling = f"{parent}/{name}"
        if client.find(sibling) is not None:
            # 目标同名目录已存在，保留当前目录名，避免覆盖已有内容
            skipped_dir_rename = f"{sibling} 已存在"
        elif client.rename(target, name):
            final_dir = sibling
            renamed_dir = final_dir

    return {
        "ok": True,
        "kept": f"{keep_name} ({keep_size})",
        "deleted": deleted,
        "renamed_file": renamed_file,
        "renamed_subtitles": renamed_subtitles,
        "renamed_dir": renamed_dir,
        "skipped_dir_rename": skipped_dir_rename,
        "final_dir": final_dir,
        "final_file": f"{final_dir}/{target_file_name}",
        "ext": ext,
    }


def move_dir(client: Cd2Client, source_dir: str, dest_parent: str) -> Dict[str, Any]:
    """
    把整理完成的目录移动到目标父目录。

    :param client: CD2 客户端
    :param source_dir: 源目录
    :param dest_parent: 目标父目录
    :return: 移动结果
    """
    name = source_dir.rstrip("/").split("/")[-1]
    dest = f"{dest_parent.rstrip('/')}/{name}"
    if source_dir.rstrip("/") == dest:
        return {"ok": True, "skipped": True, "final_dir": dest}
    try:
        existing = client.find(dest)
        if existing is not None:
            logger.warning(f"目标已存在，跳过移动：{dest}")
            return {"ok": False, "reason": "目标已存在", "final_dir": dest}
        if client.move([source_dir], dest_parent, overwrite=False):
            return {"ok": True, "final_dir": dest}
    except Cd2Error as error:
        return {"ok": False, "reason": str(error)}
    return {"ok": False, "reason": "移动未返回成功"}


def wait_offline(client: Cd2Client, folder: str, info_hash: str, name: str,
                 timeout_seconds: int, interval_seconds: int = 10,
                 should_stop=None) -> Dict[str, Any]:
    """
    轮询离线任务状态直到完成或超时。

    只调用离线任务列表接口，不刷新目录，避免触发网盘侧限流。

    :param client: CD2 客户端
    :param folder: 云下载目录
    :param info_hash: 任务 infoHash
    :param name: 任务名称
    :param timeout_seconds: 最长等待秒数
    :param interval_seconds: 轮询间隔秒数
    :param should_stop: 可选回调，返回 True 时提前中止
    :return: 含 state 的结果字典
    """
    deadline = time.time() + max(0, timeout_seconds)
    last_state = ""
    while True:
        if should_stop is not None and should_stop():
            return {"ok": False, "state": last_state or "unknown", "reason": "任务已取消"}
        try:
            task = client.find_offline(folder, info_hash=info_hash, name=name)
        except Cd2Error as error:
            return {"ok": False, "state": "error", "reason": str(error)}
        if task is not None:
            last_state = status_name(task.status)
            if last_state == "OFFLINE_FINISHED":
                return {
                    "ok": True,
                    "state": last_state,
                    "task_name": task.name,
                    "size": int(task.size or 0),
                }
            if last_state == "OFFLINE_ERROR":
                return {"ok": False, "state": last_state, "reason": "云下载任务失败"}
        if time.time() >= deadline:
            return {"ok": False, "state": last_state or "unknown", "reason": "等待超时"}
        time.sleep(max(1, interval_seconds))


def run_once(
    code: str,
    cd2: Cd2Client,
    folder: str,
    name: Optional[str] = None,
    source_base: str = "https://www.javbus.com",
    allow_uncensored: bool = False,
    allow_junk: bool = False,
    wait_seconds: int = 600,
    poll_interval: int = 10,
    move_target: str = "",
    delete_junk_only: bool = True,
    dry_run: bool = False,
    should_stop=None,
) -> Dict[str, Any]:
    """
    执行单个番号的完整云下载流程。

    :param code: 番号
    :param cd2: CD2 客户端
    :param folder: 云下载目录
    :param name: 目标名称；默认使用归一化番号
    :param source_base: 种子源根地址
    :param allow_uncensored: 是否允许无码破解资源
    :param allow_junk: 是否允许广告类资源
    :param wait_seconds: 云下载最长等待秒数
    :param poll_interval: 轮询间隔
    :param move_target: 整理完成后移动到的父目录；为空则不移动
    :param delete_junk_only: 只删非视频文件
    :param dry_run: 只做挑选与规划，不提交
    :param should_stop: 可选中止回调
    :return: 流程结果
    """
    normalized = normalize_code(code)
    target_name = safe_name(name or normalized)
    result: Dict[str, Any] = {"code": normalized, "name": target_name}

    source = JavBusSource(base_url=source_base)
    try:
        detail = source.fetch_detail(normalized)
    finally:
        source.close()

    picked = select(detail["magnets"], allow_uncensored=allow_uncensored,
                    allow_junk=allow_junk)
    result["title"] = detail.get("title")
    result["release_date"] = detail.get("release_date")
    result["magnet_total"] = picked.get("total")
    result["excluded"] = picked.get("excluded", [])
    if not picked.get("ok"):
        result.update({"ok": False, "state": STATE_FAILED,
                       "reason": picked.get("reason")})
        return result

    chosen = picked["chosen"]
    result.update(
        {
            "tier": picked.get("tier"),
            "magnet_name": chosen.get("name"),
            "size_text": chosen.get("size_text"),
            "info_hash": chosen.get("info_hash") or info_hash_of(chosen.get("magnet", "")),
        }
    )

    if dry_run:
        result.update({"ok": True, "state": "planned", "magnet": chosen.get("magnet")})
        return result

    quota = cd2.quota(folder)
    result["quota"] = quota
    if quota.get("left", 0) <= 0:
        result.update({"ok": False, "state": STATE_FAILED, "reason": "115 离线配额已用完"})
        return result

    submission = cd2.add_offline(chosen["magnet"], folder)
    result["submission"] = submission

    waited = wait_offline(
        cd2, folder, result["info_hash"] or "", target_name,
        timeout_seconds=wait_seconds, interval_seconds=poll_interval,
        should_stop=should_stop,
    )
    result["wait"] = waited
    if not waited.get("ok"):
        result.update({"ok": False, "state": STATE_DOWNLOADING,
                       "reason": waited.get("reason")})
        return result

    target_dir = locate_download_dir(cd2, folder, target_name, waited.get("task_name", ""))
    if target_dir is None:
        result.update({"ok": False, "state": STATE_FINISHED,
                       "reason": "任务已完成但未找到落盘目录"})
        return result

    tidied = tidy_dir(cd2, target_dir, target_name, delete_junk_only=delete_junk_only)
    result["tidy"] = tidied
    if not tidied.get("ok"):
        result.update({"ok": False, "state": STATE_FINISHED,
                       "reason": tidied.get("reason")})
        return result

    final_dir = tidied["final_dir"]
    if move_target:
        moved = move_dir(cd2, final_dir, move_target)
        result["move"] = moved
        if not moved.get("ok"):
            result.update({"ok": False, "state": STATE_ORGANIZED,
                           "reason": moved.get("reason"), "final_dir": final_dir})
            return result
        final_dir = moved["final_dir"]

    result.update({"ok": True, "state": STATE_MOVED if move_target else STATE_ORGANIZED,
                   "final_dir": final_dir,
                   "final_file": f"{final_dir}/{target_name}{tidied.get('ext', '')}"})
    return result
