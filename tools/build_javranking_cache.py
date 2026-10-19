"""把 MP 助手导出的 JavRanking 原始文件整理成本地缓存。

输入（``data/javranking/raw/``）：

- ``llms-full.txt``：JavRanking 的完整目录，含番号、标题与详情页链接
- ``main_rank_3380.json``：全站神作总榜 3380 条（排名、番号、标题、日期）
- ``three_lists.json``：JavLibrary TOP250 / JAVDB TOP250 等榜单

输出（``data/javranking/``）：

- ``videos.json``：按详情页 ID 去重的影片
- ``code_index.json``：番号 → 详情页
- ``rankings.json``：按榜单分组的条目
- ``javranking.db``：SQLite，表 ``videos`` / ``rankings``

只做本地解析，不访问网络。
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "javranking" / "raw"
OUT = ROOT / "data" / "javranking"

VIDEO_LINK = re.compile(
    r"^- \[(?P<label>.+?)\]\((?P<url>https://javranking\.cc/"
    r"(?P<lang>zh-hans|en)/videos/(?P<id>\d+)/)\)\s*$"
)
CODE_HEAD = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{1,24}$")


def normalize_code(value: str) -> str:
    """把番号统一成大写、用连字符连接的形式。"""
    text = re.sub(r"\s+", "", value or "").upper()
    match = re.match(r"^([A-Z]+)[-_]?(\d.*)$", text)
    return f"{match.group(1)}-{match.group(2)}" if match else text


def split_label(label: str) -> tuple[str, str]:
    """从 ``CODE: 标题`` 或 ``CODE 标题`` 中拆出番号与标题。"""
    head, sep, tail = (label or "").partition(":")
    if sep and CODE_HEAD.fullmatch(head.strip()):
        return normalize_code(head.strip()), tail.strip()
    match = re.match(r"^([A-Za-z0-9][A-Za-z0-9_-]{1,24})\s+(.+)$", (label or "").strip())
    if match:
        return normalize_code(match.group(1)), match.group(2).strip()
    return "", (label or "").strip()


def load_videos() -> dict[int, dict[str, Any]]:
    """解析 llms-full.txt，按详情页 ID 合并中英文条目。"""
    path = RAW / "llms-full.txt"
    videos: dict[int, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = VIDEO_LINK.match(line.strip())
        if not match:
            continue
        video_id = int(match.group("id"))
        lang = match.group("lang")
        code, title = split_label(match.group("label"))
        item = videos.setdefault(video_id, {"id": video_id, "code": "", "title": "", "url": ""})
        if code and not item["code"]:
            item["code"] = code
        if title and not item["title"]:
            item["title"] = title
        if lang == "zh-hans" or not item["url"]:
            item["url"] = match.group("url")
        if lang == "zh-hans" and title:
            item["title"] = title
            if code:
                item["code"] = code
    return videos


def load_rankings(videos: dict[int, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """读取榜单文件，并把番号映射到详情页。"""
    by_code = {item["code"]: item for item in videos.values() if item["code"]}
    rankings: dict[str, list[dict[str, Any]]] = {}

    main_rows = json.loads((RAW / "main_rank_3380.json").read_text(encoding="utf-8"))
    rankings["main"] = [
        {
            "rank": int(row[0]),
            "code": normalize_code(str(row[1])),
            "title": str(row[2]),
            "date": str(row[3]) if len(row) > 3 else "",
            "video_id": by_code.get(normalize_code(str(row[1])), {}).get("id"),
            "url": by_code.get(normalize_code(str(row[1])), {}).get("url", ""),
        }
        for row in main_rows
    ]

    three = json.loads((RAW / "three_lists.json").read_text(encoding="utf-8"))
    for key, rows in three.items():
        # ``three_lists.json`` 里的 main 只是前 250，完整 3380 条已在上面写入。
        name = {
            "main": "main_top250",
            "javlib": "javlibrary_top250",
            "javdb": "javdb_top250",
        }.get(key, key)
        rankings[name] = [
            {
                "rank": int(row[0]),
                "code": normalize_code(str(row[1])),
                "title": str(row[2]),
                "date": str(row[3]) if len(row) > 3 else "",
                "video_id": by_code.get(normalize_code(str(row[1])), {}).get("id"),
                "url": by_code.get(normalize_code(str(row[1])), {}).get("url", ""),
            }
            for row in rows
        ]
    return rankings


def load_details() -> list[dict[str, Any]]:
    """读取浏览器抓下来的详情页缓存（``raw/details/*.json``）。"""
    details_dir = RAW / "details"
    if not details_dir.is_dir():
        return []
    details: list[dict[str, Any]] = []
    for path in sorted(details_dir.glob("*.json")):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(item, dict) or not item.get("code"):
            continue
        details.append(item)
    return details


def write_outputs(videos: dict[int, dict[str, Any]],
                  rankings: dict[str, list[dict[str, Any]]]) -> None:
    """写出 JSON 与 SQLite。"""
    OUT.mkdir(parents=True, exist_ok=True)
    ordered = sorted(videos.values(), key=lambda item: item["id"])
    (OUT / "videos.json").write_text(
        json.dumps(ordered, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    code_index: dict[str, dict[str, Any]] = {}
    for item in ordered:
        if item["code"]:
            code_index.setdefault(item["code"], item)
    (OUT / "code_index.json").write_text(
        json.dumps(code_index, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (OUT / "rankings.json").write_text(
        json.dumps(rankings, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    db_path = OUT / "javranking.db"
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    try:
        con.executescript(
            """
            CREATE TABLE videos (
                id INTEGER PRIMARY KEY,
                code TEXT,
                title TEXT,
                url TEXT
            );
            CREATE INDEX idx_videos_code ON videos(code);
            CREATE TABLE rankings (
                list TEXT NOT NULL,
                rank INTEGER NOT NULL,
                code TEXT,
                title TEXT,
                date TEXT,
                video_id INTEGER,
                url TEXT,
                PRIMARY KEY (list, rank)
            );
            CREATE INDEX idx_rankings_code ON rankings(code);
            CREATE TABLE details (
                code TEXT PRIMARY KEY,
                video_id INTEGER,
                title TEXT,
                release_date TEXT,
                duration TEXT,
                url TEXT,
                magnets_json TEXT,
                fetched_at TEXT
            );
            """
        )
        con.executemany(
            "INSERT INTO videos(id, code, title, url) VALUES (?, ?, ?, ?)",
            [(item["id"], item["code"], item["title"], item["url"]) for item in ordered],
        )
        rows = [
            (name, item["rank"], item["code"], item["title"], item["date"],
             item["video_id"], item["url"])
            for name, items in rankings.items()
            for item in items
        ]
        con.executemany(
            "INSERT INTO rankings(list, rank, code, title, date, video_id, url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        details = load_details()
        con.executemany(
            "INSERT OR REPLACE INTO details"
            "(code, video_id, title, release_date, duration, url, magnets_json, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    normalize_code(str(item.get("code") or "")),
                    item.get("video_id"),
                    item.get("title") or "",
                    item.get("release_date") or "",
                    item.get("duration") or "",
                    item.get("detail_url") or "",
                    json.dumps(item.get("magnets") or [], ensure_ascii=False),
                    item.get("fetched_at") or "",
                )
                for item in details
            ],
        )
        con.commit()
    finally:
        con.close()


def main() -> None:
    videos = load_videos()
    rankings = load_rankings(videos)
    write_outputs(videos, rankings)
    with_code = sum(1 for item in videos.values() if item["code"])
    print(f"videos: {len(videos)}（含番号 {with_code}）")
    for name, rows in rankings.items():
        print(f"{name}: {len(rows)}")
    print(f"details: {len(load_details())}")
    print(f"output: {OUT}")


if __name__ == "__main__":
    main()
