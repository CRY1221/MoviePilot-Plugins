"""JavDB 种子源。

JavDB 受 Cloudflare 保护，必须带登录 Cookie（含 ``cf_clearance``）才能访问；
Cookie 失效时站点返回挑战页，本模块直接抛错，由上层回退其他来源。
Cookie 只在请求时使用，不写日志、不落盘。

页面结构（2026-10 实测）：

- 搜索：``/search?q=<番号>&f=all``，结果是 ``<a class="box" href="/v/xxx">``；
- 详情：标题在 ``<h2 class="title is-4">``，封面在 ``column-video-cover`` 内的 ``img``；
- 磁力：``<a href="magnet:...">`` 内的 ``span.name``，体积在 ``span.meta``（如 ``14.16GB, 6個文件``）。

JavDB 的磁力名称通常只有番号，没有分辨率和中字标记，因此按体积参与选种。
"""

from __future__ import annotations

import html as html_lib
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urljoin

from app.sdk.logging import logger
from app.sdk.network import RequestUtils

from .source import info_hash_of, normalize_code, parse_size
from .throttle import MIN_INTERVAL, wait as throttle_wait

DEFAULT_BASE = "https://javdb.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9"

SEARCH_ITEM = re.compile(
    r'<a href="(/v/[^"]+)"[^>]*class="box"[^>]*>(.*?)</a>', re.S
)
SEARCH_TITLE = re.compile(
    r'<div class="video-title">\s*<strong>([^<]*)</strong>(.*?)</div>', re.S
)
SEARCH_COVER = re.compile(
    r'<div class="cover[^"]*">.*?<img[^>]+src="([^"]+)"', re.S
)
DETAIL_TITLE = re.compile(r'<h2 class="title is-4">(.*?)</h2>', re.S)
DETAIL_COVER = re.compile(
    r'<div class="column column-video-cover">.*?<img[^>]+src="([^"]+)"', re.S
)
MAGNET_ANCHOR = re.compile(
    r'<a href="(magnet:\?xt=urn:btih:[^"]+)"[^>]*>(.*?)</a>', re.S
)
MAGNET_NAME = re.compile(r'<span class="name">([^<]*)</span>', re.I)
MAGNET_META = re.compile(r'<span class="meta">\s*([^<]*)</span>', re.I)

# Cloudflare 挑战页的标志。
# 注意：正常的页面也会被注入 challenge-platform 脚本，不能用它判断被拦，
# 只在非 200 响应里看标题和 cf-mitigated。
CHALLENGE_MARKERS = (
    "Just a moment",
    "Attention Required",
    "cf-mitigated",
)


class JavDBError(RuntimeError):
    """JavDB 抓取失败。"""


def parse_cookie(text: str) -> Dict[str, str]:
    """
    把浏览器里复制的 Cookie 字符串解析成字典。

    :param text: ``a=b; c=d`` 形式的 Cookie
    :return: Cookie 字典
    """
    cookies: Dict[str, str] = {}
    for part in (text or "").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        if name:
            cookies[name] = value.strip()
    return cookies


def _plain(text: str) -> str:
    """去掉 HTML 标签并还原实体。"""
    return html_lib.unescape(re.sub(r"<[^>]+>", " ", text or ""))


def _same_code(left: str, right: str) -> bool:
    """比较番号，忽略分隔符与 FC2 的 ``PPV`` 段。"""
    def core(value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", (value or "").upper()).replace("PPV", "")

    return bool(core(left)) and core(left) == core(right)


def parse_search(text: str) -> List[Dict[str, Any]]:
    """
    解析搜索结果列表。

    :param text: 搜索页 HTML
    :return: ``[{href, code, title, cover}]``
    """
    results: List[Dict[str, Any]] = []
    for href, body in SEARCH_ITEM.findall(text or ""):
        title_match = SEARCH_TITLE.search(body)
        if not title_match:
            continue
        code = _plain(title_match.group(1)).strip()
        title = re.sub(r"\s+", " ", _plain(title_match.group(2))).strip()
        cover = SEARCH_COVER.search(body)
        results.append(
            {
                "href": href,
                "code": code,
                "title": title,
                "cover": cover.group(1) if cover else "",
            }
        )
    return results


def parse_detail(text: str) -> Dict[str, Any]:
    """
    解析详情页标题与封面。

    :param text: 详情页 HTML
    :return: ``{code, title, cover}``
    """
    title_match = DETAIL_TITLE.search(text or "")
    raw = re.sub(r"\s+", " ", _plain(title_match.group(1))).strip() if title_match else ""
    # 中文界面会先给译名，再带「顯示原標題」和原标题；保留原标题
    if "顯示原標題" in raw:
        raw = raw.split("顯示原標題", 1)[1].strip()
    parts = raw.split(" ", 1)
    cover = DETAIL_COVER.search(text or "")
    return {
        "code": parts[0] if parts else "",
        "title": parts[1].strip() if len(parts) > 1 else "",
        "cover": cover.group(1) if cover else "",
    }


def parse_magnets(text: str) -> List[Dict[str, Any]]:
    """
    从 JavDB 详情页解析磁力列表。

    :param text: 详情页 HTML
    :return: 与 ``JavBusSource.fetch_detail`` 相同结构的磁力列表
    """
    magnets: List[Dict[str, Any]] = []
    seen = set()
    for href, inner in MAGNET_ANCHOR.findall(text or ""):
        magnet = html_lib.unescape(href).strip()
        info_hash = info_hash_of(magnet)
        if not info_hash or info_hash.lower() in seen:
            continue
        seen.add(info_hash.lower())
        name_match = MAGNET_NAME.search(inner)
        name = (
            html_lib.unescape(name_match.group(1)).strip() if name_match else ""
        )
        if not name:
            dn = re.search(r"[?&]dn=([^&]+)", magnet)
            name = unquote(dn.group(1)) if dn else info_hash
        meta_match = MAGNET_META.search(inner)
        meta = _plain(meta_match.group(1)).strip() if meta_match else ""
        size_text = meta.split(",")[0].strip() if meta else ""
        magnets.append(
            {
                "name": name,
                "size_text": size_text,
                "size_bytes": parse_size(size_text),
                "date": "",
                "magnet": magnet,
                "info_hash": info_hash,
            }
        )
    return magnets


class JavDBSource:
    """JavDB 搜索、详情与磁力抓取器。"""

    name = "javdb"

    def __init__(
        self,
        cookie: str = "",
        base_url: str = DEFAULT_BASE,
        timeout: int = 30,
        min_interval: float = MIN_INTERVAL,
    ) -> None:
        """
        初始化抓取器。

        :param cookie: 浏览器复制的整段 Cookie
        :param base_url: 站点根地址
        :param timeout: 单次请求超时秒数
        :param min_interval: 两次请求之间的最小间隔秒数
        """
        self._cookie = (cookie or "").strip()
        self._base = (base_url or DEFAULT_BASE).rstrip("/")
        self._timeout = timeout or 30
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

    def _get(self, path: str, referer: Optional[str] = None) -> str:
        """
        发起一次带 Cookie 的 GET 请求。

        :param path: 相对路径或绝对地址
        :param referer: 可选 Referer
        :return: 响应文本
        """
        if not self._cookie:
            raise JavDBError("未配置 JavDB Cookie")
        url = path if path.startswith("http") else urljoin(self._base + "/", path.lstrip("/"))
        headers: Dict[str, str] = {
            "User-Agent": USER_AGENT,
            "Accept-Language": ACCEPT_LANGUAGE,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            # 直接带原始 Cookie 字符串：会话值里有 %2B 这类转义，
            # 走 cookies= 字典会被二次转义，cf_clearance 也就失效了。
            "Cookie": self._cookie,
        }
        if referer:
            headers["Referer"] = referer
        throttle_wait("javdb", self._min_interval)
        response = self._http.request("get", url, headers=dict(headers))
        if response is None:
            raise JavDBError(f"请求失败：{url}")
        status = getattr(response, "status_code", 0)
        text = response.text or ""
        headers = getattr(response, "headers", {}) or {}
        mitigated = str(headers.get("cf-mitigated", "")).lower()
        if status in (403, 503) and (
            mitigated == "challenge"
            or any(marker in text for marker in CHALLENGE_MARKERS)
            or "challenge-platform" in text
        ):
            raise JavDBError(
                "JavDB 触发 Cloudflare 挑战，请在设置里更新 Cookie（需含 cf_clearance）"
            )
        if status == 404:
            raise JavDBError("番号不存在或页面已下架")
        if status != 200:
            raise JavDBError(f"请求异常：HTTP {status}")
        return text

    def search(self, code: str) -> List[Dict[str, Any]]:
        """
        按番号搜索。

        :param code: 番号
        :return: 搜索结果列表
        """
        return parse_search(self._get(f"/search?q={quote(code)}&f=all"))

    def fetch_detail(self, code: str) -> Dict[str, Any]:
        """
        抓取详情页并解析磁力。

        :param code: 番号
        :return: 含 title/cover/magnets 的字典
        """
        normalized = normalize_code(code)
        results = self.search(normalized)
        target = next(
            (item for item in results if _same_code(item["code"], normalized)),
            None,
        )
        if target is None and len(results) == 1:
            # 只有一个结果时才接受非精确匹配，避免抓错番号
            target = results[0]
        if target is None:
            raise JavDBError(f"JavDB 未搜到番号 {normalized}")
        search_url = urljoin(self._base + "/", f"search?q={quote(normalized)}&f=all")
        text = self._get(target["href"], referer=search_url)
        info = parse_detail(text)
        magnets = parse_magnets(text)
        if not magnets:
            raise JavDBError(f"JavDB 详情页没有 {normalized} 的磁力")
        return {
            "code": info.get("code") or target["code"],
            "query": normalized,
            "title": info.get("title") or target["title"],
            "detail_url": urljoin(self._base + "/", target["href"].lstrip("/")),
            "release_date": "",
            "duration": "",
            "cover": info.get("cover") or target.get("cover", ""),
            "magnets": magnets,
            "source": self.name,
        }
