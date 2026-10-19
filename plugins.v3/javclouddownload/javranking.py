"""JavRanking 本地缓存数据源。

插件本身不访问 JavRanking（Cloudflare 会拦截直接 HTTP），只读取由
``tools/fetch_javranking_cache.py`` 在 MP 浏览器会话里抓下来的缓存：

- ``videos``：番号、标题、详情页
- ``details``：详情页解析出的磁力（可选）
- ``rankings``：榜单条目

缓存默认放在插件数据目录的 ``javranking/javranking.db``。
"""

from __future__ import annotations

import html as html_lib
import json
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from app.sdk.logging import logger
from app.sdk.network import RequestUtils

from .source import info_hash_of, normalize_code, parse_size
from .throttle import wait as throttle_wait


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
MAGNET_ITEM = re.compile(
    r'<li[^>]*class="[^"]*magnet-item[^"]*"[^>]*>([\s\S]*?)</li>', re.I
)
MAGNET_HREF = re.compile(
    r'href="(magnet:\?xt=urn:btih:[^"]+)"', re.I
)
MAGNET_NAME = re.compile(
    r'class="[^"]*magnet-item__name[^"]*"[^>]*title="([^"]*)"', re.I
)
MAGNET_SIZE = re.compile(
    r'class="[^"]*magnet-tag--size[^"]*"[^>]*>([^<]+)<', re.I
)
DETAIL_TTL_HOURS = 168


def parse_magnet_html(text: str) -> List[Dict[str, Any]]:
    """
    从 JavRanking 详情页 HTML 解析磁力列表。

    :param text: 详情页 HTML
    :return: 与 JavBusSource.fetch_detail 相同结构的磁力列表
    """
    magnets: List[Dict[str, Any]] = []
    seen = set()
    for block in MAGNET_ITEM.findall(text or ""):
        href = MAGNET_HREF.search(block)
        if not href:
            continue
        magnet = html_lib.unescape(href.group(1)).strip()
        info_hash = info_hash_of(magnet)
        if not info_hash or info_hash.lower() in seen:
            continue
        seen.add(info_hash.lower())
        name = MAGNET_NAME.search(block)
        name_text = html_lib.unescape(name.group(1)).strip() if name else ""
        if not name_text:
            name_text = re.sub(r"<[^>]+>", "", block).strip()[:120]
        size_text = MAGNET_SIZE.search(block)
        size_value = size_text.group(1).strip() if size_text else ""
        magnets.append({
            "name": name_text,
            "size_text": size_value,
            "size_bytes": parse_size(size_value),
            "date": "",
            "magnet": magnet,
            "info_hash": info_hash,
        })
    return magnets


class JavRankingError(RuntimeError):
    """JavRanking 缓存不可用或缺少数据。"""


class JavRankingSource:
    """读取 JavRanking 本地缓存的只读数据源。"""

    name = "javranking"

    def __init__(self, cache_dir: str, db_name: str = "javranking.db") -> None:
        """
        初始化缓存数据源。

        :param cache_dir: 缓存目录
        :param db_name: SQLite 文件名
        """
        self._dir = Path(cache_dir)
        self._db = self._dir / db_name

    @property
    def db_path(self) -> Path:
        """缓存数据库路径。"""
        return self._db

    def available(self) -> bool:
        """缓存数据库是否存在。"""
        return self._db.is_file()

    def _connect(self) -> sqlite3.Connection:
        """以只读方式打开缓存数据库。"""
        if not self.available():
            raise JavRankingError(f"JavRanking 缓存不存在：{self._db}")
        return sqlite3.connect(f"file:{quote(str(self._db))}?mode=ro", uri=True)

    @staticmethod
    def _has_details(conn: sqlite3.Connection) -> bool:
        """判断缓存是否包含详情页表。"""
        row = conn.execute(
            "select 1 from sqlite_master where type='table' and name='details'"
        ).fetchone()
        return row is not None

    def stats(self) -> Dict[str, Any]:
        """返回缓存统计，供插件设置页与 /sources 使用。"""
        if not self.available():
            return {"available": False, "path": str(self._db)}
        conn = self._connect()
        try:
            videos = conn.execute("select count(*) from videos").fetchone()[0]
            rankings = conn.execute("select count(*) from rankings").fetchone()[0]
            details = 0
            if self._has_details(conn):
                details = conn.execute("select count(*) from details").fetchone()[0]
            return {
                "available": True,
                "path": str(self._db),
                "videos": videos,
                "rankings": rankings,
                "details": details,
                "updated_at": self._db.stat().st_mtime,
            }
        except sqlite3.Error as error:
            raise JavRankingError(f"读取 JavRanking 缓存失败：{error}") from error
        finally:
            conn.close()

    def lookup(self, code: str) -> Optional[Dict[str, Any]]:
        """按番号查询缓存中的影片与详情页状态。"""
        normalized = normalize_code(code)
        conn = self._connect()
        try:
            row = conn.execute(
                "select id, code, title, url from videos where code = ?",
                (normalized,),
            ).fetchone()
            if not row:
                return None
            result: Dict[str, Any] = {
                "video_id": row[0],
                "code": row[1],
                "title": row[2],
                "detail_url": row[3],
                "has_magnets": False,
            }
            if self._has_details(conn):
                detail = conn.execute(
                    "select release_date, duration, magnets_json, fetched_at "
                    "from details where code = ?",
                    (normalized,),
                ).fetchone()
                if detail:
                    result.update({
                        "release_date": detail[0] or "",
                        "duration": detail[1] or "",
                        "has_magnets": bool(json.loads(detail[2] or "[]")),
                        "fetched_at": detail[3] or "",
                    })
            return result
        except sqlite3.Error as error:
            raise JavRankingError(f"查询 JavRanking 缓存失败：{error}") from error
        finally:
            conn.close()

    @staticmethod
    def label_for(list_name: str) -> str:
        """把榜单键转成页面显示名称。"""
        fixed = {
            "main": "综合榜",
            "main_top250": "综合 Top250",
            "javlibrary_top250": "JavLibrary Top250",
            "javdb_top250": "JavDB Top250",
            "javdb_uncensored_top250": "无码 Top250",
            "javdb_fc2_top250": "FC2 Top250",
            "dmm_weekly": "DMM 周榜",
        }
        if list_name in fixed:
            return fixed[list_name]
        match = re.match(r"javdb_top250_(\d{4})$", list_name)
        if match:
            return f"JAVDB {match.group(1)} Top250"
        match = re.match(r"dmm_monthly_(\d{4})_(\d{2})$", list_name)
        if match:
            return f"DMM 月榜 {match.group(1)}-{match.group(2)}"
        return list_name.replace("_", " ")

    @staticmethod
    def cover_url(video_id: Any) -> str:
        """按 JavRanking 详情页 ID 拼封面地址。"""
        try:
            value = int(video_id or 0)
        except (TypeError, ValueError):
            return ""
        return f"https://static.javranking.cc/video/cover/{value}.webp" if value else ""

    def lists(self) -> List[Dict[str, Any]]:
        """列出缓存中的全部榜单及条数。"""
        order = {
            "main": 0, "main_top250": 1, "javlibrary_top250": 2,
            "javdb_top250": 3, "javdb_uncensored_top250": 4,
            "javdb_fc2_top250": 5,
        }
        conn = self._connect()
        try:
            rows = conn.execute(
                "select list, count(*) from rankings group by list"
            ).fetchall()
        except sqlite3.Error as error:
            raise JavRankingError(f"读取 JavRanking 榜单失败：{error}") from error
        finally:
            conn.close()
        lists = [
            {"list": row[0], "label": self.label_for(row[0]), "count": row[1]}
            for row in rows
        ]
        lists.sort(key=lambda item: (
            order.get(item["list"], 10),
            item["list"],
        ))
        return lists

    def rankings(self, list_name: str = "main", limit: int = 50,
                 offset: int = 0) -> List[Dict[str, Any]]:
        """读取指定榜单的条目。"""
        conn = self._connect()
        try:
            rows = conn.execute(
                "select rank, code, title, date, video_id, url from rankings "
                "where list = ? order by rank limit ? offset ?",
                (
                    list_name,
                    max(1, min(int(limit or 50), 5000)),
                    max(0, int(offset or 0)),
                ),
            ).fetchall()
            return [
                {
                    "rank": row[0],
                    "code": row[1],
                    "title": row[2],
                    "date": row[3],
                    "video_id": row[4],
                    "detail_url": row[5],
                    "cover": self.cover_url(row[4]),
                }
                for row in rows
            ]
        except sqlite3.Error as error:
            raise JavRankingError(f"读取 JavRanking 榜单失败：{error}") from error
        finally:
            conn.close()

    def _save_detail(self, code: str, magnets: List[Dict[str, Any]]) -> None:
        """把抓到的磁力写入 details 表。"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(str(self._db), timeout=10)
        try:
            row = conn.execute(
                "select id, title, url from videos where code = ?", (code,)
            ).fetchone()
            if not row:
                raise JavRankingError(f"JavRanking 缓存中没有番号 {code}")
            old = conn.execute(
                "select release_date, duration from details where code = ?", (code,)
            ).fetchone()
            conn.execute(
                "insert or replace into details("
                "code, video_id, title, release_date, duration, url, "
                "magnets_json, fetched_at) values(?,?,?,?,?,?,?,?)",
                (
                    code, row[0], row[1], (old[0] if old else "") or "",
                    (old[1] if old else "") or "", row[2],
                    json.dumps(magnets, ensure_ascii=False), now,
                ),
            )
            conn.commit()
        except sqlite3.Error as error:
            raise JavRankingError(f"写入 JavRanking 详情失败：{error}") from error
        finally:
            conn.close()

    def _fetch_page(self, url: str) -> str:
        """带 3 秒全局限速抓取 JavRanking 详情页。"""
        headers = {
            "User-Agent": USER_AGENT,
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        http = RequestUtils(headers=headers, timeout=30, use_session=True)
        try:
            throttle_wait("javranking")
            response = http.request("get", url, headers=dict(headers))
            if response is None or response.status_code != 200:
                status = getattr(response, "status_code", "无响应")
                raise JavRankingError(f"请求 JavRanking 失败：{status}")
            content = getattr(response, "content", None)
            if isinstance(content, (bytes, bytearray)):
                return content.decode("utf-8", "replace")
            return response.text or ""
        finally:
            try:
                http.close()
            except Exception:
                pass

    def fetch_remote_detail(self, code: str) -> Dict[str, Any]:
        """
        读取详情；缓存没有磁力或已过期时按需抓取 JavRanking 详情页。

        :param code: 番号
        :return: 与 JavBusSource.fetch_detail 相同形状的字典
        """
        normalized = normalize_code(code)
        cached = self.fetch_detail(normalized)
        fetched_at = str(cached.get("details_fetched_at") or "")
        fresh = False
        if cached.get("magnets") and fetched_at:
            try:
                fresh = datetime.strptime(fetched_at, "%Y-%m-%d %H:%M:%S") > (
                    datetime.now() - timedelta(hours=DETAIL_TTL_HOURS)
                )
            except ValueError:
                fresh = False
        if fresh:
            return cached
        url = str(cached.get("detail_url") or "")
        if not url:
            raise JavRankingError(f"JavRanking 缓存中没有 {normalized} 的详情页地址")
        try:
            text = self._fetch_page(url)
            magnets = parse_magnet_html(text)
        except Exception as error:
            if cached.get("magnets"):
                logger.warning(f"JavRanking 抓取失败，使用旧缓存：{error}")
                return cached
            raise JavRankingError(f"JavRanking 抓取失败：{error}") from error
        if not magnets:
            if cached.get("magnets"):
                return cached
            raise JavRankingError(f"JavRanking 详情页没有 {normalized} 的磁力")
        self._save_detail(normalized, magnets)
        return self.fetch_detail(normalized)

    def fetch_detail(self, code: str) -> Dict[str, Any]:
        """
        读取番号详情；没有详情缓存时返回元数据与空磁力列表。

        :param code: 番号
        :return: 与 JavBusSource.fetch_detail 相同形状的字典
        """
        normalized = normalize_code(code)
        conn = self._connect()
        try:
            row = conn.execute(
                "select v.id, v.code, v.title, v.url, "
                "d.release_date, d.duration, d.magnets_json, d.fetched_at "
                "from videos v left join details d on d.code = v.code "
                "where v.code = ?",
                (normalized,),
            ).fetchone()
            if not row:
                raise JavRankingError(f"JavRanking 缓存中没有番号 {normalized}")
            magnets: List[Dict[str, Any]] = []
            if row[6]:
                try:
                    parsed = json.loads(row[6])
                    if isinstance(parsed, list):
                        magnets = parsed
                except json.JSONDecodeError:
                    magnets = []
            return {
                "code": row[1],
                "title": row[2],
                "detail_url": row[3],
                "release_date": row[4] or "",
                "duration": row[5] or "",
                "magnets": magnets,
                "source": self.name,
                "has_magnets": bool(magnets),
                "details_fetched_at": row[7] or "",
            }
        except sqlite3.Error as error:
            raise JavRankingError(f"读取 JavRanking 详情失败：{error}") from error
        finally:
            conn.close()
