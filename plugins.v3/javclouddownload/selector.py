"""
磁力候选的筛选与排序规则。

排序优先级（从高到低）：
1. 中文字幕（``-C`` / ``_CH`` / ``中字`` / ``字幕``），且体积不低于最大候选的一半；
2. 明确的 UHD / 2160P / 2160I，且体积不小于 5GB；裸 4K 视为码率，不算分辨率；
3. 其余。

硬性排除（列出即出局）：
- 无码破解 ``-U`` / ``-UC`` / ``uncensored``；
- 假 4K：标了 UHD/2160P/2160I，但体积小于 5GB；
- 720P / 720I；
- 480P / 360P 及以下；
- 广告或非视频资源（命名含推广域名、纯压缩包等）。

同一档内按体积从大到小取；不把分辨率标签当作主要质量依据。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# 真 4K 标志；JAV 发布名里的 4K/6K/8K 多为码率标签，不能当分辨率
UHD_PATTERN = re.compile(r"(?:^|[\s._\-\[@])(?:UHD|2160P|2160I)(?:$|[\s._\-\]@])", re.I)

# 低于该体积的 4K 视为假 4K
UHD_MIN_SIZE = 5 * 1024 ** 3

# 明确标注 720P 的资源直接排除
RES_720_PATTERN = re.compile(r"(?:^|[\s._\-])(?:720P|720I)(?:$|[\s._\-])", re.I)

# 明确标注 480P/360P 及以下的资源直接排除
RES_LOW_PATTERN = re.compile(
    r"(?:^|[\s._\-])(?:480P|480I|360P|360I)(?:$|[\s._\-])", re.I
)

# 小于该体积的视频视为垃圾；可在插件设置里覆盖
DEFAULT_MIN_VIDEO_SIZE = 300 * 1024 ** 2

# 中文字幕标志
CHS_PATTERN = re.compile(r"(?:^|[\s._\-])(?:C|CH|CHS|CHT)(?:$|[\s._\-])|中字|字幕|中文字幕", re.I)

# 无码破解（需要排除）
UNCENSORED_PATTERN = re.compile(
    r"(?:^|[\s._\-])(?:UC|U|uncensored|unc)(?:$|[\s._\-])|無碼|无码|破解", re.I
)

# 广告或非视频资源
JUNK_PATTERN = re.compile(
    r"\.(?:zip|rar|7z)$|(?:\.cc|\.com|\.net|\.xyz|\.top|\.vip)[\s@_-]|"
    r"游戏大全|推广|广告",
    re.I,
)

VIDEO_EXT = {
    ".mp4", ".mkv", ".avi", ".wmv", ".ts", ".m2ts", ".mov",
    ".flv", ".rmvb", ".rm", ".mpg", ".mpeg", ".webm",
}


def is_uhd(name: str, size_bytes: int = 0) -> bool:
    """
    判断是否为可信的 4K/UHD 资源。

    标了 4K 但体积小于 5GB 的视为假 4K，不进入 4K 档；体积未知时也不按 4K 排。

    :param name: 磁力名称
    :param size_bytes: 体积字节数
    :return: 可信 4K 返回 True
    """
    if not UHD_PATTERN.search(name or ""):
        return False
    return int(size_bytes or 0) >= UHD_MIN_SIZE


def is_fake_uhd(name: str, size_bytes: int = 0) -> bool:
    """
    判断是否为标了 4K 但体积不足的假 4K。

    :param name: 磁力名称
    :param size_bytes: 体积字节数
    :return: 假 4K 返回 True
    """
    if not UHD_PATTERN.search(name or ""):
        return False
    size = int(size_bytes or 0)
    return 0 < size < UHD_MIN_SIZE


def is_720p(name: str) -> bool:
    """
    判断是否明确标注 720P/720I。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(RES_720_PATTERN.search(name or ""))


def is_low_res(name: str) -> bool:
    """
    判断是否明确标注 480P/360P 及以下。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(RES_LOW_PATTERN.search(name or ""))


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


def rank(magnet: Dict[str, Any], max_size: int = 0) -> tuple:
    """
    计算候选排序键（数值越小越优先）。

    中文字幕只在体积达到最大候选一半时进入第一档；否则按体积与其它候选竞争。

    :param magnet: 磁力条目
    :param max_size: 过滤后候选中的最大体积
    :return: 可直接用于 sorted 的元组
    """
    name = magnet.get("name") or ""
    size_bytes = int(magnet.get("size_bytes") or 0)
    if has_chinese_sub(name) and (
        not max_size or size_bytes >= int(max_size * 0.5)
    ):
        tier = 0
    elif is_uhd(name, size_bytes):
        tier = 1
    else:
        tier = 2
    # 同档内体积大的优先，因此体积取负值
    return (tier, -size_bytes)


def select(
    magnets: List[Dict[str, Any]],
    allow_uncensored: bool = False,
    allow_junk: bool = False,
    enable_rules: bool = True,
    min_video_size: int = DEFAULT_MIN_VIDEO_SIZE,
) -> Dict[str, Any]:
    """
    从候选列表中选出最佳磁力。

    :param magnets: ``source.fetch_detail`` 返回的 magnets 列表
    :param allow_uncensored: 是否允许无码破解资源
    :param allow_junk: 是否允许广告/压缩包类资源
    :param enable_rules: 是否启用识别筛选规则；关闭时直接按体积最大
    :param min_video_size: 小于该体积的候选视为垃圾
    :return: 含 chosen 与 rejected/excluded 说明的字典
    """
    if not enable_rules:
        ordered = sorted(
            magnets, key=lambda item: -int(item.get("size_bytes") or 0)
        )
        if not ordered:
            return {
                "ok": False,
                "reason": "没有候选",
                "chosen": None,
                "excluded": [],
                "total": 0,
            }
        return {
            "ok": True,
            "chosen": ordered[0],
            "tier": "体积最大（规则关闭）",
            "excluded": [],
            "total": len(magnets),
            "candidates": ordered,
        }

    candidates: List[Dict[str, Any]] = []
    excluded: List[Dict[str, Any]] = []
    min_size = max(0, int(min_video_size or 0))
    min_mb = round(min_size / 1024 ** 2)

    for magnet in magnets:
        name = magnet.get("name") or ""
        size_bytes = int(magnet.get("size_bytes") or 0)
        reason = None
        if not allow_uncensored and is_uncensored(name):
            reason = "无码破解"
        elif is_fake_uhd(name, size_bytes):
            reason = "假4K（小于5GB）"
        elif is_720p(name):
            reason = "720P"
        elif is_low_res(name):
            reason = "480P/360P"
        elif 0 < size_bytes < min_size:
            reason = f"小于{min_mb}MB"
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

    max_size = max(int(item.get("size_bytes") or 0) for item in candidates)
    ordered = sorted(candidates, key=lambda item: rank(item, max_size))
    chosen = ordered[0]
    name = chosen.get("name") or ""
    size_bytes = int(chosen.get("size_bytes") or 0)
    if has_chinese_sub(name) and size_bytes >= int(max_size * 0.5):
        tier = "中文字幕"
    elif is_uhd(name, size_bytes):
        tier = "4K"
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
