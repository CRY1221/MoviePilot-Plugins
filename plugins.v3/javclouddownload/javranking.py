"""JavRanking 本地缓存数据源。

插件本身不访问 JavRanking（Cloudflare 会拦截直接 HTTP），只读取由
``tools/fetch_javranking_cache.py`` 在 MP 浏览器会话里抓下来的缓存：

- ``videos``：番号、标题、详情页
- ``details``：详情页解析出的磁力（可选）
- ``rankings``：榜单条目

缓存默认放在插件数据目录的 ``javranking/javranking.db``。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from .source import normalize_code


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

    def rankings(self, list_name: str = "main", limit: int = 50) -> List[Dict[str, Any]]:
        """读取指定榜单的前 N 条。"""
        allowed = {"main", "main_top250", "javlibrary_top250", "javdb_top250"}
        if list_name not in allowed:
            raise JavRankingError(f"未知榜单：{list_name}")
        conn = self._connect()
        try:
            rows = conn.execute(
                "select rank, code, title, date, video_id, url from rankings "
                "where list = ? order by rank limit ?",
                (list_name, max(1, min(int(limit or 50), 500))),
            ).fetchall()
            return [
                {
                    "rank": row[0],
                    "code": row[1],
                    "title": row[2],
                    "date": row[3],
                    "video_id": row[4],
                    "detail_url": row[5],
                }
                for row in rows
            ]
        except sqlite3.Error as error:
            raise JavRankingError(f"读取 JavRanking 榜单失败：{error}") from error
        finally:
            conn.close()

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
