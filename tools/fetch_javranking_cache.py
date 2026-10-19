"""在 MoviePilot 容器内用浏览器抓取 JavRanking 缓存。

JavRanking 有 Cloudflare 挑战，直接 HTTP 是 403。本脚本使用 MP 自带的
``cloakbrowser`` 打开页面，等挑战通过后，在页面内用 ``fetch`` 取数据。

运行方式（在 MP 宿主机执行）：

    docker exec -i moviepilot-v3 python3 - < tools/fetch_javranking_cache.py

常用参数：

    --out /config/plugins/JavCloudDownload/javranking/raw
    --codes START-257,ABF-394         只抓这些番号的详情页
    --codes-file /config/temp/codes.txt
    --skip-index                      只抓详情页
    --sleep 3                         详情页之间的间隔秒数

输出：

    raw/llms-full.txt                 完整目录（番号 → 详情页）
    raw/llms.txt                      站点 Agent 指南
    raw/pages/*.md                    榜单 Markdown
    raw/main_rank_3380.json           全站神作总榜
    raw/details/<番号>.json           详情页（含磁力）
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

BASE = "https://javranking.cc"
DEFAULT_OUT = "/config/plugins/JavCloudDownload/javranking/raw"

VIDEO_LINK = re.compile(
    r"^- \[(?P<label>.+?)\]\((?P<url>https://javranking\.cc/"
    r"(?P<lang>zh-hans|en)/videos/(?P<id>\d+)/)\)\s*$"
)
CODE_HEAD = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,24}$")
MAGNET_ITEM = re.compile(r'<li[^>]*class="[^"]*magnet-item[^"]*"[^>]*>([\s\S]*?)</li>', re.I)
MAGNET_HREF = re.compile(r'href="(magnet:\?xt=urn:btih:[^"]+)"', re.I)
MAGNET_NAME = re.compile(r'class="[^"]*magnet-item__name[^"]*"[^>]*title="([^"]*)"', re.I)
MAGNET_SIZE = re.compile(r'class="[^"]*magnet-tag--size[^"]*"[^>]*>([^<]+)<', re.I)
DATE = re.compile(r"(\d{4}-\d{2}-\d{2})")
SIZE = re.compile(r"^([\d.]+)\s*(TB|GB|MB|KB|GIB|MIB|KIB)$", re.I)


def normalize_code(value: str) -> str:
    """把番号统一成大写、用连字符连接的形式。"""
    text = re.sub(r"\s+", "", value or "").upper()
    match = re.match(r"^([A-Z]+)[-_]?(\d.*)$", text)
    return f"{match.group(1)}-{match.group(2)}" if match else text


def split_code_title(label: str) -> tuple[str, str]:
    """从 ``CODE: 标题`` 或 ``CODE 标题`` 中拆出番号与标题。"""
    head, sep, tail = (label or "").partition(":")
    if sep and CODE_HEAD.fullmatch(head.strip()):
        return normalize_code(head.strip()), tail.strip()
    match = re.match(r"^([A-Za-z0-9][A-Za-z0-9_-]{1,24})\s+(.+)$", (label or "").strip())
    if match:
        return normalize_code(match.group(1)), match.group(2).strip()
    return "", (label or "").strip()


def parse_size(text: str) -> int:
    """把体积文本换算成字节数。"""
    match = SIZE.match((text or "").strip())
    if not match:
        return 0
    value = float(match.group(1))
    unit = match.group(2).upper()
    factor = {
        "TB": 1024 ** 4, "GB": 1024 ** 3, "GIB": 1024 ** 3,
        "MB": 1024 ** 2, "MIB": 1024 ** 2,
        "KB": 1024, "KIB": 1024,
    }.get(unit, 0)
    return int(value * factor)


def info_hash_of(magnet: str) -> str:
    """从磁力链接提取 infoHash。"""
    match = re.search(r"btih:([0-9a-zA-Z]{32,40})", magnet or "")
    return match.group(1) if match else ""


def parse_llms_index(text: str) -> dict[str, dict[str, Any]]:
    """解析 llms-full.txt，返回 番号 → {id, url, title}。"""
    index: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        match = VIDEO_LINK.match(line.strip())
        if not match:
            continue
        label = match.group("label").strip()
        code, title = split_code_title(label)
        if not code:
            continue
        item = index.setdefault(code, {
            "id": int(match.group("id")),
            "url": match.group("url"),
            "title": title,
        })
        if match.group("lang") == "zh-hans":
            item["id"] = int(match.group("id"))
            item["url"] = match.group("url")
            item["title"] = title
    return index


def parse_detail(html: str, code: str, detail_url: str) -> dict[str, Any]:
    """从详情页 HTML 解析标题、日期与磁力。"""
    title, release, duration = "", "", ""
    for block in re.findall(
        r'<script type="application/ld\+json">([\s\S]*?)</script>', html, re.I
    ):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "Movie":
            title = str(data.get("name") or "")
            release = str(data.get("datePublished") or "")
            duration = str(data.get("duration") or "")
            break

    magnets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for block in MAGNET_ITEM.findall(html):
        href_match = MAGNET_HREF.search(block)
        if not href_match:
            continue
        magnet = html_lib.unescape(href_match.group(1)).strip()
        info_hash = info_hash_of(magnet)
        if info_hash and info_hash in seen:
            continue
        seen.add(info_hash)
        name_match = MAGNET_NAME.search(block)
        size_match = MAGNET_SIZE.search(block)
        name = html_lib.unescape(name_match.group(1)).strip() if name_match else ""
        size_text = size_match.group(1).strip() if size_match else ""
        magnets.append({
            "name": name,
            "size_text": size_text,
            "size_bytes": parse_size(size_text),
            "date": "",
            "magnet": magnet,
            "info_hash": info_hash,
        })

    return {
        "code": normalize_code(code),
        "title": title,
        "release_date": release,
        "duration": duration,
        "detail_url": detail_url,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "magnets": magnets,
    }


def parse_cards(cards: list[dict[str, Any]]) -> list[list[Any]]:
    """把页面卡片整理成 ``[排名, 番号, 标题, 日期]``。"""
    rows: list[list[Any]] = []
    for fallback_rank, card in enumerate(cards, start=1):
        label = (card.get("label") or "").strip()
        code, title = split_code_title(label.split(". 当前榜单")[0])
        if not code:
            continue
        rank_match = re.search(r"当前榜单\s*#(\d+)", label)
        rank = int(rank_match.group(1)) if rank_match else fallback_rank
        date_match = DATE.search(card.get("text") or "")
        if code:
            rows.append([rank, code, title, date_match.group(1) if date_match else ""])
    rows.sort(key=lambda row: row[0])
    return rows


class Browser:
    """轻量封装，负责启动浏览器、通过 Cloudflare、页面内 fetch。"""

    def __init__(self, headless: bool = True) -> None:
        from cloakbrowser import launch_context

        self._context = launch_context(headless=headless)
        self.page = self._context.new_page()

    def close(self) -> None:
        try:
            self.page.close()
        finally:
            self._context.close()

    def open(self, url: str) -> str:
        """打开页面并等待 Cloudflare 挑战结束。"""
        self.page.goto(url, timeout=60000, wait_until="domcontentloaded")
        deadline = time.time() + 60
        last = ""
        while time.time() < deadline:
            try:
                last = self.page.evaluate("() => document.documentElement.outerHTML")
                if "Just a moment" not in last and "JavRanking" in self.page.title():
                    return last
            except Exception:
                pass
            self.page.wait_for_timeout(1000)
        return last

    def fetch_text(self, url: str) -> str:
        """在页面内 fetch 文本；请求会带上 Cloudflare 会话。"""
        result = self.page.evaluate(
            """async (u) => {
                const r = await fetch(u, {credentials: 'include'});
                return {status: r.status, text: await r.text()};
            }""",
            url,
        )
        if result.get("status") != 200:
            raise RuntimeError(f"fetch {url} -> HTTP {result.get('status')}")
        return result.get("text") or ""

    def cards(self) -> list[dict[str, Any]]:
        """读取榜单页上的全部影片卡片。"""
        return self.page.evaluate(
            """() => Array.from(document.querySelectorAll('[data-video-card-cover]')).map(a => ({
                label: a.getAttribute('aria-label') || '',
                href: a.getAttribute('href') || '',
                text: (a.closest('article') || a.parentElement || a).innerText || ''
            }))"""
        )


def write_text(path: Path, text: str) -> None:
    """写文本文件并创建父目录。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--codes", default="")
    parser.add_argument("--codes-file", default="")
    parser.add_argument("--skip-index", action="store_true")
    parser.add_argument("--sleep", type=float, default=3.0)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    codes: list[str] = []
    if args.codes:
        codes.extend(part.strip() for part in re.split(r"[,，\n]", args.codes) if part.strip())
    if args.codes_file:
        codes.extend(
            line.strip() for line in Path(args.codes_file).read_text(
                encoding="utf-8", errors="replace"
            ).splitlines() if line.strip()
        )
    codes = [normalize_code(code) for code in codes]
    if args.limit > 0:
        codes = codes[: args.limit]

    browser = Browser()
    try:
        browser.open(BASE + "/zh-hans/")
        index: dict[str, dict[str, Any]] = {}

        if not args.skip_index:
            for name, url in (
                ("llms.txt", BASE + "/llms.txt"),
                ("llms-full.txt", BASE + "/llms-full.txt"),
                ("pages/jr_most_awarded.md", BASE + "/zh-hans/most-awarded-videos.md"),
                ("pages/jr_javlibrary_top250.md", BASE + "/zh-hans/rankings/javlibrary-top250.md"),
                ("pages/jr_javdb_top250.md", BASE + "/zh-hans/rankings/javdb-top250.md"),
                ("pages/jr_index.md", BASE + "/zh-hans/index.md"),
            ):
                text = browser.fetch_text(url)
                write_text(out / name, text)
                print(f"saved {name} ({len(text)} chars)")

            browser.open(BASE + "/zh-hans/most-awarded-videos/")
            cards = browser.cards()
            rows = parse_cards(cards)
            main_path = out / "main_rank_3380.json"
            # 卡片文案偶尔缺少番号；用上一版同排名条目补齐，避免一次刷新丢行。
            by_rank: dict[int, list[Any]] = {}
            if main_path.is_file():
                try:
                    for row in json.loads(main_path.read_text(encoding="utf-8")):
                        by_rank[int(row[0])] = row
                except (OSError, ValueError, TypeError):
                    by_rank = {}
            for row in rows:
                by_rank[int(row[0])] = row
            merged = [by_rank[rank] for rank in sorted(by_rank)]
            write_text(main_path, json.dumps(merged, ensure_ascii=False))
            print(f"saved main_rank_3380.json ({len(rows)} parsed, {len(merged)} merged)")
            index = parse_llms_index((out / "llms-full.txt").read_text(encoding="utf-8"))
        else:
            llms_path = out / "llms-full.txt"
            if llms_path.is_file():
                index = parse_llms_index(llms_path.read_text(encoding="utf-8"))

        for code in codes:
            item = index.get(code)
            if not item:
                print(f"skip {code}: llms-full 中没有该番号")
                continue
            url = item["url"]
            html = browser.fetch_text(url)
            detail = parse_detail(html, code, url)
            detail["video_id"] = item.get("id")
            write_text(
                out / "details" / f"{code}.json",
                json.dumps(detail, ensure_ascii=False, indent=2),
            )
            print(f"saved details/{code}.json ({len(detail['magnets'])} magnets)")
            time.sleep(max(0.0, args.sleep))
    finally:
        browser.close()


if __name__ == "__main__":
    main()
