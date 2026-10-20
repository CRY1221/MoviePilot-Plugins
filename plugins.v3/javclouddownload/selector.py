"""
磁力候选的筛选与排序规则。

排序优先级（从高到低）：
1. 分辨率：4K（4K/UHD/2160P）> 2K（2K/QHD/1440P）> 1080P > 720P；
   名称里看不出分辨率的候选与 1080P 同档；
2. 同一档内优先中文字幕（``-C`` / ``_CH`` / ``中字`` / ``字幕``）；
3. 同一档、同一字幕条件内按体积从大到小。

硬性排除（列出即出局）：
- 无码破解：``-U`` / ``-UC`` / ``uncensored`` / ``leaked`` / ``無修正`` / ``无码`` / ``破解``；
- AI 拉伸或修复：独立 ``AI`` 标记 / ``AI高画質リマスター`` / ``4Kリマスター版`` /
  ``Upscale`` / ``修复版`` / ``超分`` / ``补帧`` 等；
- 假 4K：标了 4K/UHD/2160P，但体积小于 5GB；
- 480P / 360P 及以下；
- 体积小于 2GB 的候选（可在插件设置里覆盖）；
- 广告或非视频资源（命名含推广域名、纯压缩包等）。

720P 不再直接排除，只在分辨率排序里排最后。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# 真 4K 标志：4K / UHD / 2160P / 2160I 都表示 4K 分辨率
UHD_PATTERN = re.compile(
    r"(?:^|[\s._\-\[@])(?:4K|UHD|2160P|2160I)(?:$|[\s._\-\]@])", re.I
)

# 2K 标志
K2_PATTERN = re.compile(
    r"(?:^|[\s._\-\[@])(?:2K|QHD|1440P|1440I)(?:$|[\s._\-\]@])", re.I
)

# 1080P 标志
FHD_PATTERN = re.compile(
    r"(?:^|[\s._\-\[@])(?:1080P|1080I|FHD)(?:$|[\s._\-\]@])", re.I
)

# 低于该体积的 4K 视为假 4K
UHD_MIN_SIZE = 5 * 1024 ** 3

# 明确标注 720P 的资源排在最后一档
RES_720_PATTERN = re.compile(r"(?:^|[\s._\-])(?:720P|720I)(?:$|[\s._\-])", re.I)

# 明确标注 480P/360P 及以下的资源直接排除
RES_LOW_PATTERN = re.compile(
    r"(?:^|[\s._\-])(?:480P|480I|360P|360I|240P|240I)(?:$|[\s._\-])", re.I
)

# AI 拉伸 / 修复 / 放大：标了这些的视为假高清
# 实测名称：AI高画質リマスター、4Kリマスター版、.AI.、4K60FPS
AI_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])AI(?![A-Za-z0-9])"          # 独立 AI 标记：.AI. / [AI] / -AI
    r"|リマスター|Remaster(?:ed)?"
    r"|修复版|高清修复|4K修复|超分|补帧|插帧"
    r"|Upscal(?:e|ed|ing)?|アップスケール",
    re.I,
)

# 小于该体积的视频文件视为垃圾，用于整理阶段；可在插件设置里单独调整
DEFAULT_MIN_VIDEO_SIZE = 300 * 1024 ** 2

# 小于该体积的磁力候选直接不下载（默认 2GB）
DEFAULT_MIN_MAGNET_SIZE = 2 * 1024 ** 3

# 中文字幕标志
CHS_PATTERN = re.compile(r"(?:^|[\s._\-])(?:C|CH|CHS|CHT)(?:$|[\s._\-])|中字|字幕|中文字幕", re.I)

# 无码破解（需要排除）
# 实测名称：-UC / -U.torrent / 无码破解 / (破解) / [Uncensored] /
#          (無修正-流出) / (無修正-漏れ) / Uncensored Leaked / 無碼流出
UNCENSORED_PATTERN = re.compile(
    r"(?:^|[\s._\-\[(])(?:UC|U|uncensored|uncen|unc|leaked|leak)(?:$|[\s._\-\])])"
    r"|無碼|无码|無修正|无修正|破解|無碼流出|无码流出|モザイク破壊|リーク",
    re.I,
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

# 分辨率档位，数值越小越优先；未标分辨率按 1080P 档处理
TIER_4K, TIER_2K, TIER_1080, TIER_720 = 0, 1, 2, 3


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


def is_2k(name: str) -> bool:
    """
    判断是否明确标注 2K/1440P。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(K2_PATTERN.search(name or ""))


def is_1080p(name: str) -> bool:
    """
    判断是否明确标注 1080P/FHD。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(FHD_PATTERN.search(name or ""))


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


def is_ai_upscaled(name: str) -> bool:
    """
    判断是否为 AI 拉伸 / 修复 / 放大的资源。

    :param name: 磁力名称
    :return: 命中返回 True
    """
    return bool(AI_PATTERN.search(name or ""))


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


def resolution_tier(name: str) -> int:
    """
    返回候选的分辨率档位，数值越小越优先。

    名称里看不出分辨率的按 1080P 档处理。

    :param name: 磁力名称
    :return: 档位编号
    """
    text = name or ""
    if UHD_PATTERN.search(text):
        return TIER_4K
    if K2_PATTERN.search(text):
        return TIER_2K
    if FHD_PATTERN.search(text):
        return TIER_1080
    if RES_720_PATTERN.search(text):
        return TIER_720
    return TIER_1080


def resolution_label(name: str) -> str:
    """
    返回用于展示的分辨率标签。

    :param name: 磁力名称
    :return: 4K/2K/1080P/720P/未标分辨率
    """
    text = name or ""
    if UHD_PATTERN.search(text):
        return "4K"
    if K2_PATTERN.search(text):
        return "2K"
    if FHD_PATTERN.search(text):
        return "1080P"
    if RES_720_PATTERN.search(text):
        return "720P"
    return "未标分辨率"


def rank(magnet: Dict[str, Any], max_size: int = 0) -> tuple:
    """
    计算候选排序键（数值越小越优先）。

    分辨率优先，同档内中文字幕优先，再按体积从大到小。

    :param magnet: 磁力条目
    :param max_size: 兼容旧调用，已不参与排序
    :return: 可直接用于 sorted 的元组
    """
    name = magnet.get("name") or ""
    size_bytes = int(magnet.get("size_bytes") or 0)
    sub_rank = 0 if has_chinese_sub(name) else 1
    # 同档内体积大的优先，因此体积取负值
    return (resolution_tier(name), sub_rank, -size_bytes)


def select(
    magnets: List[Dict[str, Any]],
    allow_uncensored: bool = False,
    allow_junk: bool = False,
    enable_rules: bool = True,
    min_video_size: int = DEFAULT_MIN_MAGNET_SIZE,
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
        elif is_ai_upscaled(name):
            reason = "AI拉伸/修复"
        elif is_fake_uhd(name, size_bytes):
            reason = "假4K（小于5GB）"
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

    ordered = sorted(candidates, key=rank)
    chosen = ordered[0]
    name = chosen.get("name") or ""
    tier = resolution_label(name)
    if has_chinese_sub(name):
        tier = f"{tier}（中字）"
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
