"""
Emby 库基线与真实分辨率探测。

下载前用 Emby 条目做质量基线；下载后用 CD2 直链 + ffprobe 读候选真实分辨率。
"""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional
from urllib.parse import quote

from app.sdk.logging import logger

from .cd2 import Cd2Client
from .selector import has_chinese_sub, is_720p, is_uhd

# 只要明确的 1440P；发布名里的 2K 通常是 2000kbps 码率
RES_2K_PATTERN = re.compile(r"(?:^|[\s._\-])(?:1440P|1440I)(?:$|[\s._\-])", re.I)
RES_1080_PATTERN = re.compile(r"(?:^|[\s._\-])(?:1080P|1080I|FHD)(?:$|[\s._\-])", re.I)
# JAV 发布名里的 4K/6K/8K 多数表示码率（4000/6000/8000 kbps）
BITRATE_TAG_PATTERN = re.compile(r"(?:^|[\s._\-])([468])K(?:$|[\s._\-])", re.I)

# Emby 条目里常见的中文字幕语言标记
CHINESE_SUB_LANGS = {"chi", "zho", "zh", "chs", "cht", "cn"}

# Emby 条目扫描较慢，短时间缓存，避免每次提交都扫一遍
_ITEM_CACHE: Dict[tuple, tuple[float, list]] = {}
_ITEM_CACHE_TTL = 300.0


@dataclass
class LibraryHit:
    """Emby 库内命中条目。"""

    server: str
    item_id: str
    path: str
    quality: Dict[str, Any]
    relative_dir: str = ""
    cd2_dir: str = ""


def _has_chinese_stream(streams: Any) -> bool:
    """判断 MediaStreams 里是否有中文字幕。"""
    for stream in streams or []:
        if str(stream.get("Type") or "").lower() != "subtitle":
            continue
        language = str(stream.get("Language") or "").lower()
        title = str(stream.get("DisplayTitle") or stream.get("Title") or "")
        if language in CHINESE_SUB_LANGS or "中" in title or "chinese" in title.lower():
            return True
    return False


def quality_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    从 Emby BaseItemDto 提取真实质量。

    :param item: Emby 搜索返回的条目
    :return: 含 height/width/bitrate/size/has_chinese_sub/path 的字典
    """
    sources = item.get("MediaSources") or []
    source = max(
        sources,
        key=lambda value: (
            int(value.get("Height") or 0),
            int(value.get("Bitrate") or 0),
            int(value.get("Size") or 0),
        ),
        default={},
    )
    streams = source.get("MediaStreams") or item.get("MediaStreams") or []
    video_streams = [
        stream for stream in streams
        if str(stream.get("Type") or "").lower() == "video"
    ]
    video = max(video_streams, key=lambda value: int(value.get("Height") or 0), default={})
    height = int(video.get("Height") or source.get("Height") or item.get("Height") or 0)
    width = int(video.get("Width") or source.get("Width") or item.get("Width") or 0)
    bitrate = int(
        video.get("BitRate")
        or source.get("Bitrate")
        or item.get("Bitrate")
        or 0
    )
    size = int(source.get("Size") or item.get("Size") or 0)
    path = str(source.get("Path") or item.get("Path") or "")
    return {
        "height": height,
        "width": width,
        "bitrate": bitrate,
        "size": size,
        "has_chinese_sub": _has_chinese_stream(streams),
        "path": path,
    }


def estimate_candidate_quality(name: str, size_bytes: int) -> Dict[str, Any]:
    """
    下载前根据磁力名称和体积估算候选质量。

    :param name: 磁力名称
    :param size_bytes: 体积字节数
    :return: 含 height/size/has_chinese_sub 的字典
    """
    height = 0
    if is_uhd(name, size_bytes):
        height = 2160
    elif RES_2K_PATTERN.search(name or ""):
        height = 1440
    elif is_720p(name):
        height = 720
    elif RES_1080_PATTERN.search(name or ""):
        height = 1080
    bitrate_tag = BITRATE_TAG_PATTERN.search(name or "")
    return {
        "height": height,
        "bitrate": int(bitrate_tag.group(1)) * 1_000_000 if bitrate_tag else 0,
        "size": int(size_bytes or 0),
        "has_chinese_sub": has_chinese_sub(name),
    }


def candidate_may_be_better(existing: Dict[str, Any],
                            candidate: Dict[str, Any]) -> bool:
    """
    下载前粗判候选是否值得洗版。

    :param existing: Emby 库内真实质量
    :param candidate: 候选估算质量
    :return: 值得下载返回 True
    """
    existing_height = int(existing.get("height") or 0)
    candidate_height = int(candidate.get("height") or 0)
    existing_size = int(existing.get("size") or 0)
    candidate_size = int(candidate.get("size") or 0)

    if candidate_height and existing_height and candidate_height > existing_height:
        return True
    if candidate_height and not existing_height and candidate_height >= 1440:
        return True
    if candidate_height and existing_height and candidate_height < existing_height:
        return False
    if (
        candidate.get("has_chinese_sub")
        and not existing.get("has_chinese_sub")
        and candidate_height >= existing_height
    ):
        return True
    existing_bitrate = int(existing.get("bitrate") or 0)
    candidate_bitrate = int(candidate.get("bitrate") or 0)
    if existing_bitrate > 0 and candidate_bitrate >= int(existing_bitrate * 1.15):
        return True
    if existing_size > 0 and candidate_size >= int(existing_size * 1.5):
        return True
    return False


def actual_is_better(existing: Dict[str, Any],
                     actual: Dict[str, Any]) -> bool:
    """
    下载后按真实质量判断是否值得替换。

    :param existing: Emby 库内真实质量
    :param actual: ffprobe 读到的候选真实质量
    :return: 值得替换返回 True
    """
    existing_height = int(existing.get("height") or 0)
    actual_height = int(actual.get("height") or 0)
    if actual_height and existing_height:
        if actual_height > existing_height:
            return True
        if actual_height < existing_height:
            return False
    elif actual_height >= 2160:
        return True

    if (
        actual.get("has_chinese_sub")
        and not existing.get("has_chinese_sub")
        and actual_height >= existing_height
    ):
        return True

    existing_size = int(existing.get("size") or 0)
    actual_size = int(actual.get("size") or 0)
    if existing_size > 0 and actual_size >= int(existing_size * 1.1):
        return True
    existing_bitrate = int(existing.get("bitrate") or 0)
    actual_bitrate = int(actual.get("bitrate") or 0)
    if existing_bitrate > 0 and actual_bitrate >= int(existing_bitrate * 1.1):
        return True
    return False


def resolution_label(height: int) -> str:
    """
    把视频高度转成文件名用的分辨率标签。

    :param height: 视频高度
    :return: 2160P/1440P/1080P/720P；无法判断返回空字符串
    """
    value = int(height or 0)
    if value >= 2160:
        return "2160P"
    if value >= 1440:
        return "1440P"
    if value >= 1080:
        return "1080P"
    if value >= 720:
        return "720P"
    return ""


def probe_media(client: Cd2Client, path: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """
    通过 CD2 直链 + ffprobe 读取真实媒体信息。

    :param client: CD2 客户端
    :param path: CD2 中的文件路径
    :param timeout: ffprobe 超时秒数
    :return: 质量字典；失败返回 None
    """
    if not path or shutil.which("ffprobe") is None:
        return None
    try:
        url = client.download_url(path)
    except Exception as error:
        logger.warning(f"读取 CD2 直链失败：{type(error).__name__}: {error}")
        return None
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-print_format", "json",
                "-show_streams", "-show_format",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=max(5, timeout),
        )
    except Exception as error:
        logger.warning(f"ffprobe 读取失败：{type(error).__name__}: {error}")
        return None
    if result.returncode != 0:
        logger.warning(f"ffprobe 返回失败：{result.stderr[:200]}")
        return None
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return None

    streams = payload.get("streams") or []
    video = next(
        (stream for stream in streams if stream.get("codec_type") == "video"),
        {},
    )
    subtitles = [stream for stream in streams if stream.get("codec_type") == "subtitle"]
    has_chinese = any(
        str(stream.get("tags", {}).get("language") or "").lower()
        in CHINESE_SUB_LANGS
        for stream in subtitles
    )
    return {
        "height": int(video.get("height") or 0),
        "width": int(video.get("width") or 0),
        "bitrate": int(video.get("bit_rate") or 0),
        "size": int((payload.get("format") or {}).get("size") or 0),
        "has_chinese_sub": has_chinese,
    }


def _parse_path_map(value: str) -> list:
    """解析 ``Emby路径:CD2路径`` 映射，支持换行或分号分隔。"""
    pairs = []
    for line in (value or "").replace("；", ";").splitlines():
        for part in line.split(";"):
            part = part.strip()
            if not part or ":" not in part:
                continue
            left, right = part.split(":", 1)
            left = left.strip().rstrip("/")
            right = right.strip().rstrip("/")
            if left and right:
                pairs.append((left, right))
    return pairs


class EmbyLibrary:
    """查询 Emby 成人影片库。"""

    def __init__(self, library_filter: str = "", path_map: str = "") -> None:
        self._filter = (library_filter or "").strip()
        self._path_map = _parse_path_map(path_map)

    def _mapped_dir(self, item_path: str) -> str:
        """把 Emby 侧文件路径按配置映射成 CD2 目录。"""
        for emby_root, cd2_root in self._path_map:
            prefix = f"{emby_root}/"
            if not item_path.startswith(prefix):
                continue
            relative = item_path[len(prefix):]
            parent = str(PurePosixPath(relative).parent)
            return cd2_root if parent == "." else f"{cd2_root}/{parent}"
        return ""

    @staticmethod
    def _services() -> Dict[str, Any]:
        """获取启用的 Emby 服务。"""
        try:
            from app.sdk.services import MediaServerHelper

            return MediaServerHelper().get_services(type_filter="emby")
        except Exception as error:
            logger.warning(f"读取 Emby 服务失败：{type(error).__name__}: {error}")
            return {}

    def _allowed_library_ids(self, instance: Any) -> Optional[set]:
        """按配置解析允许的 Emby 库 ID；空配置返回 None。"""
        if not self._filter:
            return None
        try:
            libraries = instance.get_librarys() or []
        except Exception:
            return None
        allowed = set()
        for library in libraries:
            name = str(getattr(library, "name", "") or "")
            library_id = str(getattr(library, "id", "") or "")
            if self._filter in (name, library_id):
                allowed.add(library_id)
        return allowed

    @staticmethod
    def _library_roots(instance: Any) -> Dict[str, str]:
        """读取媒体库 ID 到根路径的映射；get_librarys 没有路径时查 VirtualFolders。"""
        roots: Dict[str, str] = {}
        try:
            libraries = instance.get_librarys() or []
        except Exception:
            libraries = []
        for library in libraries:
            library_id = str(getattr(library, "id", "") or "")
            root = str(getattr(library, "path", "") or "")
            if library_id and root:
                roots[library_id] = root
        if roots:
            return roots
        try:
            response = instance.get_data(
                "[HOST]emby/Library/VirtualFolders?api_key=[APIKEY]"
            )
            rows = (response.json() if response is not None else None) or []
        except Exception as error:
            logger.warning(f"读取 Emby 媒体库路径失败：{type(error).__name__}: {error}")
            return roots
        for row in rows:
            library_id = str(row.get("ItemId") or "")
            locations = row.get("Locations") or []
            if library_id and locations:
                roots[library_id] = str(locations[0])
        return roots

    @staticmethod
    def _relative_dir(item_path: str, root: str) -> str:
        """把 Emby 条目路径换算成媒体库内的相对目录。"""
        if not item_path or not root:
            return ""
        try:
            relative = PurePosixPath(item_path).relative_to(root)
        except ValueError:
            return ""
        parent = relative.parent
        return "" if str(parent) == "." else str(parent)

    def _search(self, instance: Any, code: str) -> list:
        """在单个 Emby 实例中搜索番号。"""
        fields = "Path,MediaSources,MediaStreams,ParentId,OriginalTitle,ProductionYear"
        url = (
            "[HOST]emby/Items?Recursive=true"
            "&IncludeItemTypes=Movie,Video"
            f"&SearchTerm={quote(code)}"
            f"&Fields={fields}"
            "&Limit=20&IncludeSearchTypes=false&api_key=[APIKEY]"
        )
        try:
            response = instance.get_data(url)
            if response is None:
                return []
            return list((response.json() or {}).get("Items") or [])
        except Exception as error:
            logger.warning(f"Emby 搜索失败：{type(error).__name__}: {error}")
            return []

    def _item_detail(self, instance: Any, item_id: str) -> Optional[Dict[str, Any]]:
        """按 item id 读取 Emby 条目详情。"""
        if not item_id:
            return None
        url = (
            f"[HOST]emby/Items?Ids={item_id}"
            "&Fields=Path,MediaSources,MediaStreams,ParentId,OriginalTitle,ProductionYear"
            "&api_key=[APIKEY]"
        )
        try:
            response = instance.get_data(url)
            if response is None:
                return None
            items = (response.json() or {}).get("Items") or []
            return items[0] if items else None
        except Exception:
            return None

    def _fallback_items(self, instance: Any, code: str,
                        allowed: Optional[set]) -> list:
        """SearchTerm 不命中时，用 get_items 扫描配置的库。"""
        result = []
        try:
            libraries = instance.get_librarys() or []
        except Exception:
            return result
        normalized = code.lower()
        for library in libraries:
            library_id = str(getattr(library, "id", "") or "")
            if allowed is not None and library_id not in allowed:
                continue
            try:
                items = instance.get_items(library_id, 0, 5000) or []
            except Exception:
                continue
            for item in items:
                title = str(getattr(item, "title", "") or "")
                path = str(getattr(item, "path", "") or "")
                if normalized not in f"{title} {path}".lower():
                    continue
                item_id = str(getattr(item, "item_id", "") or "")
                detail = self._item_detail(instance, item_id)
                if detail:
                    result.append(detail)
                if len(result) >= 5:
                    return result
        return result

    def _match_items(self, server: str, items: list, code: str,
                     allowed: Optional[set],
                     roots: Optional[Dict[str, str]] = None) -> Optional[LibraryHit]:
        """从 Emby 条目列表中挑出番号匹配项。"""
        normalized = code.lower()
        for item in items:
            parent = str(item.get("ParentId") or "")
            if allowed is not None and parent not in allowed:
                continue
            haystack = " ".join(
                str(item.get(key) or "")
                for key in ("Name", "OriginalTitle", "Path")
            ).lower()
            if normalized not in haystack:
                continue
            quality = quality_from_item(item)
            return LibraryHit(
                server=server,
                item_id=str(item.get("Id") or ""),
                path=str(quality.get("path") or item.get("Path") or ""),
                quality=quality,
                relative_dir=self._relative_dir(
                    str(item.get("Path") or ""),
                    (roots or {}).get(parent, ""),
                ),
                cd2_dir=self._mapped_dir(str(item.get("Path") or "")),
            )
        return None

    @staticmethod
    def _mp_db_path() -> Optional[Path]:
        """MP 的 user.db 路径；读不到时返回 None。"""
        try:
            from app.sdk.config import settings

            return Path(settings.CONFIG_PATH) / "user.db"
        except Exception:
            fallback = Path("/config/user.db")
            return fallback if fallback.is_file() else None

    def _mp_rows(self, code: str, db_path: Any = None,
                 limit: int = 20) -> list:
        """
        从 MP 已同步的 Emby 条目索引里快速找番号。

        :param code: 归一化番号
        :param db_path: 测试用数据库路径；为空时用 MP 的 user.db
        :param limit: 最多返回条数
        :return: [{server, library, item_id, title, path}]
        """
        db = Path(db_path) if db_path else self._mp_db_path()
        if db is None or not db.is_file():
            return []
        needle = f"%{code.lower()}%"
        try:
            conn = sqlite3.connect(f"file:{quote(str(db))}?mode=ro", uri=True)
            try:
                rows = conn.execute(
                    "select server, library, item_id, title, original_title, path "
                    "from mediaserveritem "
                    "where server = 'emby' and ("
                    "lower(title) like ? or lower(original_title) like ? "
                    "or lower(path) like ?) limit ?",
                    (needle, needle, needle, max(1, int(limit))),
                ).fetchall()
            finally:
                conn.close()
        except sqlite3.Error as error:
            logger.warning(f"读取 MP 媒体库索引失败：{error}")
            return []
        pattern = re.compile(
            rf"(?<![A-Za-z0-9]){re.escape(code)}(?![0-9])", re.I
        )
        result = []
        for row in rows:
            haystack = f"{row[3] or ''} {row[4] or ''} {row[5] or ''}"
            if not pattern.search(haystack):
                continue
            result.append({
                "server": row[0] or "emby",
                "library": str(row[1] or ""),
                "item_id": str(row[2] or ""),
                "title": row[3] or "",
                "path": row[5] or "",
            })
        return result

    def find_item(self, code: str) -> Optional[LibraryHit]:
        """
        查找 Emby 库内已有番号。

        :param code: 归一化番号
        :return: 命中条目；未命中返回 None
        """
        normalized = (code or "").strip().lower()
        if not normalized:
            return None
        for server, service in self._services().items():
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            # 先用 MP 同步的本地索引定位条目，再向 Emby 查一次详情
            for row in self._mp_rows(code):
                detail = self._item_detail(instance, row["item_id"])
                hit = self._match_items(server, [detail] if detail else [], code, None)
                if hit is not None:
                    return hit
            allowed = self._allowed_library_ids(instance)
            roots = self._library_roots(instance)
            hit = self._match_items(
                server, self._search(instance, code), code, allowed, roots
            )
            if hit is not None:
                return hit
            hit = self._match_items(
                server, self._fallback_items(instance, code, allowed),
                code, allowed, roots,
            )
            if hit is not None:
                return hit
        # 最后用 get_items 直接扫配置的库；STRM 条目在 Emby 搜索里经常不命中
        for item in self.list_items(5000):
            haystack = f"{item.get('title') or ''} {item.get('path') or ''}".lower()
            if normalized in haystack:
                return LibraryHit(
                    server=str(item.get("server") or ""),
                    item_id=str(item.get("item_id") or ""),
                    path=str(item.get("path") or ""),
                    quality={"path": str(item.get("path") or "")},
                )
        return None

    def refresh_item(self, server: str, item_id: str) -> bool:
        """
        刷新 Emby 单个条目。

        :param server: Emby 服务名
        :param item_id: Emby item id
        :return: 是否成功
        """
        if not item_id:
            return False
        for name, service in self._services().items():
            if server and name != server:
                continue
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            try:
                response = instance.post_data(
                    f"[HOST]emby/Items/{item_id}/Refresh"
                    "?Recursive=true&api_key=[APIKEY]"
                )
                return response is not None
            except Exception as error:
                logger.warning(f"Emby 刷新失败：{type(error).__name__}: {error}")
        return False

    def cd2_dir_for(self, server: str, item_id: str) -> str:
        """
        按条目详情和目录映射换算出 CD2 侧旧目录。

        :param server: Emby 服务名
        :param item_id: Emby item id
        :return: CD2 目录；无法映射返回空字符串
        """
        if not item_id or not self._path_map:
            return ""
        for name, service in self._services().items():
            if server and name != server:
                continue
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            detail = self._item_detail(instance, item_id)
            if detail:
                return self._mapped_dir(str(detail.get("Path") or ""))
        return ""

    def relative_dir_for(self, server: str, item_id: str) -> str:
        """
        按条目详情和媒体库根路径换算相对目录。

        :param server: Emby 服务名
        :param item_id: Emby item id
        :return: 相对目录；无法换算返回空字符串
        """
        if not item_id:
            return ""
        for name, service in self._services().items():
            if server and name != server:
                continue
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            detail = self._item_detail(instance, item_id)
            if not detail:
                continue
            path = str(detail.get("Path") or "")
            for root in self._library_roots(instance).values():
                relative = self._relative_dir(path, root)
                if relative:
                    return relative
        return ""

    def get_item_detail(self, server: str, item_id: str) -> Optional[Dict[str, Any]]:
        """按服务名和 item id 读取 Emby 详情。"""
        if not item_id:
            return None
        for name, service in self._services().items():
            if server and name != server:
                continue
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            detail = self._item_detail(instance, item_id)
            if detail:
                return detail
        return None

    def list_libraries(self) -> list:
        """
        列出 Emby 媒体库，供设置页选择成人影片库。

        :return: [{server, id, name, type, path}]
        """
        result = []
        for server, service in self._services().items():
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            try:
                libraries = instance.get_librarys() or []
            except Exception as error:
                logger.warning(f"读取 Emby 媒体库失败：{type(error).__name__}: {error}")
                continue
            for library in libraries:
                result.append({
                    "server": server,
                    "id": str(getattr(library, "id", "") or ""),
                    "name": str(getattr(library, "name", "") or ""),
                    "type": str(getattr(library, "type", "") or ""),
                    "path": str(getattr(library, "path", "") or ""),
                })
        return result

    def list_items(self, limit: int = 50) -> list:
        """
        列出目标库内的条目，供调试库配置。

        :param limit: 最多返回条数
        :return: [{server, library, item_id, title, path}]
        """
        cache_key = (self._filter, int(limit))
        cached = _ITEM_CACHE.get(cache_key)
        if cached and cached[0] > time.time():
            return cached[1]
        result = []
        for server, service in self._services().items():
            instance = getattr(service, "instance", None)
            if instance is None:
                continue
            allowed = self._allowed_library_ids(instance)
            try:
                libraries = instance.get_librarys() or []
            except Exception:
                continue
            for library in libraries:
                library_id = str(getattr(library, "id", "") or "")
                if allowed is not None and library_id not in allowed:
                    continue
                try:
                    items = instance.get_items(library_id, 0, limit) or []
                except Exception as error:
                    logger.warning(f"读取 Emby 库条目失败：{type(error).__name__}: {error}")
                    continue
                for item in items:
                    result.append({
                        "server": server,
                        "library": library_id,
                        "item_id": str(getattr(item, "item_id", "") or ""),
                        "title": str(getattr(item, "title", "") or ""),
                        "path": str(getattr(item, "path", "") or ""),
                    })
                    if len(result) >= limit:
                        _ITEM_CACHE[cache_key] = (time.time() + _ITEM_CACHE_TTL, result)
                        return result
        _ITEM_CACHE[cache_key] = (time.time() + _ITEM_CACHE_TTL, result)
        return result
