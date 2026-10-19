"""
磁力候选的筛选与排序规则。

排序优先级（从高到低）：
1. 带「第一会所新片」标志（``第一會所新片`` / ``SIS001``）；
2. 4K / UHD；
3. 中文字幕（``-C`` / ``_CH`` / ``中字`` / ``字幕``）；
4. 其余。

硬性排除（列出即出局）：
- 无码破解 ``-U`` / ``-UC`` / ``uncensored``；
- 广告或非视频资源（命名含推广域名、纯压缩包等）。

同一档内按体积从大到小取。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# 第一会所标志
SIS001_PATTERNS = ("第一會所新片", "第一会所新片", "SIS001", "sis001")

# 4K 标志
UHD_PATTERN = re.compile(r"(?:^|[\s._\-\[@])(?:4K|UHD|2160P|2160I)(?:$|[\s._\-\]@])", re.I)

# 中文字幕标志
CHS_PATTERN = re.compile(r"(?:^|[\s._\-])(?:C|CH|CHS|CHT)(?:$|[\s._\-])|中字|字幕|中文字幕", re.I)

# 无码破解（需要排除）
UNCENSORED_PATTERN = re.compile(
    r"(?:^|[\s._\-])(?:UC|U|uncensored|unc)(?:$|[\s._\-])|無碼|无码|破解", re.I
)

# 广告或非视频资源
JUNK_PATTERN = re.compile(
    r"\.(?:zip|rar|7z|iso)$|(?:\.cc|\.com|\.net|\.xyz|\.top|\.vip)[\s@_-]|"
    r"游戏大全|推广|广告",
    re.I,
)

VIDEO_EXT = {
    ".mp4", ".mkv", ".avi", ".wmv", ".ts", ".m2ts", ".mov",
    ".flv", ".rmvb", ".rm", ".mpg", ".mpeg", ".webm", ".iso",
}


def has_sis001(name: str) -> bool:
    """
    判断磁力名称是否带「第一会所新片」标志。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    lowered = (name or "").lower()
    return any(pattern.lower() in lowered for pattern in SIS001_PATTERNS)


def is_uhd(name: str) -> bool:
    """
    判断是否为 4K/UHD 资源。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(UHD_PATTERN.search(name or ""))


def has_chinese_sub(name: str) -> bool:
    """
    判断是否标注中文字幕。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(CHS_PATTERN.search(name or ""))


def is_uncensored(name: str) -> bool:
    """
    判断是否为无码破解资源。

    注意：``-UC`` 与 ``-U`` 形态需先于中文字幕判定，避免 ``UC`` 被误判。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(UNCENSORED_PATTERN.search(name or ""))


def is_junk(name: str) -> bool:
    """
    判断是否为广告或非视频资源。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(JUNK_PATTERN.search(name or ""))


def rank(magnet: Dict[str, Any]) -> tuple:
    """
    计算候选排序键（数值越小越优先）。

    :param magnet: 磁力条目
    :return: 可直接用于 sorted 的元组
    """
    name = magnet.get("name") or ""
    if has_sis001(name):
        tier = 0
    elif is_uhd(name):
        tier = 1
    elif has_chinese_sub(name):
        tier = 2
    else:
        tier = 3
    # 同档内体积大的优先，因此体积取负值
    return (tier, -int(magnet.get("size_bytes") or 0))


def select(
    magnets: List[Dict[str, Any]],
    allow_uncensored: bool = False,
    allow_junk: bool = False,
) -> Dict[str, Any]:
    """
    从候选列表中选出最佳磁力。

    :param magnets: ``source.fetch_detail`` 返回的 magnets 列表
    :param allow_uncensored: 是否允许无码破解资源
    :param allow_junk: 是否允许广告/压缩包类资源
    :return: 含 chosen 与 rejected/excluded 说明的字典
    """
    candidates: List[Dict[str, Any]] = []
    excluded: List[Dict[str, Any]] = []

    for magnet in magnets:
        name = magnet.get("name") or ""
        reason = None
        if not allow_uncensored and is_uncensored(name):
            reason = "无码破解"
        elif not allow_junk and is_junk(name):
            reason = "广告或非视频"
        if reason:
            excluded.append({"name": name, "reason": reason})
        else:
            candidates.append(magnet)

    if not candidates:
        return {
            "ok": False,
            "reason": "所有候选均被排除规则过滤",
            "chosen": None,
            "excluded": excluded,
            "total": len(magnets),
        }

    ordered = sorted(candidates, key=rank)
    chosen = ordered[0]
    name = chosen.get("name") or ""
    if has_sis001(name):
        tier = "第一会所新片"
    elif is_uhd(name):
        tier = "4K"
    elif has_chinese_sub(name):
        tier = "中文字幕"
    else:
        tier = "体积最大"
    return {
        "ok": True,
        "chosen": chosen,
        "tier": tier,
        "excluded": excluded,
        "total": len(magnets),
        "candidates": ordered,
    }


def pick_video(files: List[tuple]) -> Optional[tuple]:
    """
    从 ``(path, size, name)`` 列表中选出体积最大的视频文件。

    :param files: 文件三元组列表
    :return: 选中的三元组；无视频时返回 None
    """
    videos = [
        item for item in files
        if (item[2].rpartition(".")[2] and f".{item[2].rpartition('.')[2].lower()}" in VIDEO_EXT)
    ]
    if not videos:
        return None
    return max(videos, key=lambda item: item[1])
