"""
JavBus 种子源。

访问约束（实测）：
1. 站点对华语用户豁免年龄验证，请求需带中文 ``Accept-Language``；
2. 详情页磁力列表由 ajax 接口返回，必须带 ``Referer``，否则 Cloudflare 返回 403；
3. gid 为每次请求轮换的动态 token，必须从详情页现取；
4. 瞬时并发 >= 3 或速率 > 约 6/s 会返回 429，冷却约 15 秒。

因此本模块使用单线程限速抓取，并在 429 时退避重试。
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urljoin

from app.sdk.logging import logger
from app.sdk.network import RequestUtils

DEFAULT_BASE = "https://www.javbus.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
ACCEPT_LANGUAGE = "zh-TW,zh;q=0.9"

# 番号形态：ABC-123 / ABC123 / ABC-123_2026-10-18 / 092226_001
CODE_PATTERN = re.compile(
    r"^[A-Za-z]{2,10}[-_]?\d{2,6}(?:[-_]\d{2,4})?(?:[_-]\d{4}-\d{2}-\d{2})?$"
)
SIZE_PATTERN = re.compile(r"^[\d.]+\s*(GB|GiB|MB|MiB|TB)$", re.IGNORECASE)
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# 限速：单线程最小间隔，保守低于实测阈值
MIN_INTERVAL = 0.6
RETRY_ON_429 = 3
COOLDOWN_429 = 15

_lock = threading.Lock()
_last_request_at = 0.0


class JavBusError(RuntimeError):
    """JavBus 抓取失败。"""


def normalize_code(value: str) -> str:
    """
    归一化番号：去空格、统一大写前缀。

    :param value: 用户输入或文件名片段
    :return: 归一化后的番号
    """
    text = (value or "").strip()
    text = re.sub(r"\s+", "", text)
    match = re.match(r"^([A-Za-z]+)[-_]?(\d.*)$", text)
    if match:
        return f"{match.group(1).upper()}-{match.group(2)}"
    return text.upper()


def parse_size(text: str) -> int:
    """
    把体积文本换算为字节数。

    :param text: 例如 ``6.05GB``、``999.65MB``
    :return: 字节数；无法解析时返回 0
    """
    match = re.match(r"^([\d.]+)\s*(TB|GB|MB|KiB|MiB|GiB|KB)$", (text or "").strip(), re.I)
    if not match:
        return 0
    value = float(match.group(1))
    unit = match.group(2).upper()
    factor = {
        "TB": 1024 ** 4,
        "GB": 1024 ** 3,
        "GIB": 1024 ** 3,
        "MB": 1024 ** 2,
        "MIB": 1024 ** 2,
        "KB": 1024,
        "KIB": 1024,
    }.get(unit, 0)
    return int(value * factor)


def info_hash_of(magnet: str) -> Optional[str]:
    """
    从磁力链接提取 infoHash。

    :param magnet: 磁力链接
    :return: 40 位 infoHash；无匹配返回 None
    """
    match = re.search(r"btih:([0-9a-zA-Z]{32,40})", magnet or "")
    return match.group(1) if match else None


class JavBusSource:
    """JavBus 详情页与磁力列表抓取器。"""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE,
        timeout: int = 25,
        min_interval: float = MIN_INTERVAL,
    ) -> None:
        """
        初始化抓取器。

        :param base_url: 站点根地址
        :param timeout: 单次请求超时秒数
        :param min_interval: 两次请求之间的最小间隔秒数
        """
        self._base = (base_url or DEFAULT_BASE).rstrip("/")
        self._timeout = timeout or 25
        self._min_interval = max(0.0, float(min_interval or 0))
        self._http = RequestUtils(
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": ACCEPT_LANGUAGE,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=self._timeout,
            use_session=True,
        )

    def close(self) -> None:
        """释放底层 HTTP 会话。"""
        try:
            self._http.close()
        except Exception:
            pass

    def _throttle(self) -> None:
        """执行全局最小间隔限速。"""
        global _last_request_at
        with _lock:
            wait = self._min_interval - (time.time() - _last_request_at)
            if wait > 0:
                time.sleep(wait)
            _last_request_at = time.time()

    def _get(self, path: str, referer: Optional[str] = None) -> str:
        """
        发起一次受控 GET 请求，429 时退避重试。

        :param path: 相对路径或绝对地址
        :param referer: 可选 Referer；ajax 接口必需
        :return: 响应文本
        """
        url = path if path.startswith("http") else urljoin(self._base + "/", path.lstrip("/"))
        # 注意：RequestUtils.request 只在 headers 键缺失时回落到实例默认头，
        # 显式传 None 或空字典都会丢掉 Accept-Language，从而被重定向到年龄验证页。
        headers: Dict[str, str] = {
            "User-Agent": USER_AGENT,
            "Accept-Language": ACCEPT_LANGUAGE,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if referer:
            headers["Referer"] = referer
        for attempt in range(RETRY_ON_429):
            self._throttle()
            response = self._http.request("get", url, headers=dict(headers))
            if response is None:
                raise JavBusError(f"请求失败：{url}")
            if response.status_code == 429:
                logger.warning(f"JavBus 触发限流，{COOLDOWN_429}s 后重试（{attempt + 1}/{RETRY_ON_429}）")
                time.sleep(COOLDOWN_429)
                continue
            if response.status_code == 404:
                raise JavBusError("番号不存在或页面已下架")
            if response.status_code != 200:
                raise JavBusError(f"请求异常：HTTP {response.status_code}")
            text = response.text or ""
            if "driver-verify" in text:
                raise JavBusError("被重定向到年龄验证页，请检查网络出口语言环境")
            if "Attention Required" in text and "Cloudflare" in text:
                raise JavBusError("被 Cloudflare 拦截，缺少有效 Referer")
            return text
        raise JavBusError("连续触发限流，已放弃本次请求")

    def _resolve(self, code: str) -> tuple[str, str]:
        """
        确认番号对应的详情页，并返回已抓取的 HTML。

        多数番号可直接按番号访问；详情页可能带日期后缀（如
        ``SUN-074_2026-10-18``），或直连落在 200 的空页上，因此用页面里
        是否存在 ``gid`` 判断是否真的是详情页，不是再回退站内搜索。

        :param code: 归一化番号
        :return: ``(详情页相对路径, 页面 HTML)``
        """
        direct = f"/{quote(code)}"
        try:
            html = self._get(direct)
        except JavBusError:
            html = ""
        if html and re.search(r"var gid\s*=\s*\d+", html):
            return direct, html

        try:
            html = self._get(f"/search/{quote(code)}")
        except JavBusError as error:
            raise JavBusError(f"站内未搜到番号 {code}（{error}）") from error
        links = re.findall(r'<a class="movie-box" href="([^"]+)"', html)
        if not links:
            raise JavBusError(f"站内未搜到番号 {code}")
        normalized = normalize_code(code)
        target = links[0]
        # 优先精确匹配：路径最后一段归一化后等于番号
        for link in links:
            slug = link.rstrip("/").split("/")[-1]
            if normalize_code(slug) == normalized:
                target = link
                break
        return target, self._get(target)

    def resolve_path(self, code: str) -> str:
        """
        兼容旧调用：只返回详情页相对路径。

        :param code: 归一化番号
        :return: 详情页相对路径
        """
        return self._resolve(code)[0]

    def fetch_detail(self, code: str) -> Dict[str, Any]:
        """
        抓取详情页并解析磁力列表。

        :param code: 番号
        :return: 含 title/genres/magnets 等字段的字典
        """
        path, html = self._resolve(code)
        detail_url = urljoin(self._base + "/", path.lstrip("/"))

        gid = (re.search(r"var gid\s*=\s*(\d+)", html) or [None, None])[1]
        if not gid:
            raise JavBusError("详情页缺少 gid，可能页面结构变更")
        img = (re.search(r"var img\s*=\s*'([^']+)'", html) or [None, ""])[1]
        uc = (re.search(r"var uc\s*=\s*(\d+)", html) or [None, "0"])[1]

        title = (re.search(r"<title>([^<]*)</title>", html) or [None, ""])[1]
        title = re.sub(r"\s*-\s*JavBus\s*$", "", title).strip()
        # 元数据字段被 span/p 标签分隔，先剥离标签再匹配
        plain = re.sub(r"<[^>]+>", " ", html)
        code_text = (re.search(r"識別碼:\s*([A-Za-z0-9_-]+)", plain) or [None, code])[1]
        release = (re.search(r"發行日期:\s*(\d{4}-\d{2}-\d{2})", plain) or [None, ""])[1]
        duration = (re.search(r"長度:\s*(\d+)分鐘", plain) or [None, ""])[1]

        ajax = (
            f"/ajax/uncledatoolsbyajax.php?gid={gid}&lang=zh"
            f"&img={img}&uc={uc or 0}&floor=500"
        )
        table = self._get(ajax, referer=detail_url)

        magnets: List[Dict[str, Any]] = []
        for row in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", table):
            magnet = (re.search(r"(magnet:\?xt=urn:btih:[a-zA-Z0-9]+)", row) or [None, None])[1]
            if not magnet:
                continue
            cells = [
                re.sub(r"<[^>]+>", "", cell).strip()
                for cell in re.findall(r"<a[^>]*>([\s\S]*?)</a>", row)
            ]
            cells = [cell for cell in cells if cell]
            if not cells:
                continue
            name = cells[0]
            size_text = next((c for c in cells if SIZE_PATTERN.match(c)), "")
            date = next((c for c in cells if DATE_PATTERN.match(c)), "")
            magnets.append(
                {
                    "name": name,
                    "size_text": size_text,
                    "size_bytes": parse_size(size_text),
                    "date": date,
                    "magnet": magnet,
                    "info_hash": info_hash_of(magnet),
                }
            )

        if not magnets:
            raise JavBusError(f"{code} 详情页没有可用磁力")

        return {
            "code": code_text,
            "query": code,
            "title": title,
            "detail_url": detail_url,
            "release_date": release,
            "duration": duration,
            "magnets": magnets,
        }


def fetch(code: str, base_url: str = DEFAULT_BASE) -> Dict[str, Any]:
    """
    抓取指定番号的种子信息（便捷封装）。

    :param code: 番号
    :param base_url: 站点根地址
    :return: 详情字典
    """
    source = JavBusSource(base_url=base_url)
    try:
        return source.fetch_detail(normalize_code(code))
    finally:
        source.close()
