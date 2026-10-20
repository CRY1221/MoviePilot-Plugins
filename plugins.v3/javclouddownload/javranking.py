"""JavRanking 数据源。

读取本地缓存获取番号与详情页地址；磁力缓存缺失或超过 7 天时，
按需直接抓取详情页并写回 ``details``：

- ``videos``：番号、标题、详情页
- ``details``：按需抓取并缓存的磁力
- ``rankings``：榜单条目

缓存默认放在插件数据目录的 ``javranking/javranking.db``。
"""

from __future__ import annotations

import html as html_lib
import json
import re
import sqlite3
import threading
import time
from datetime import datetime
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

# Cloudflare：浏览器过一次盾取得 cf_clearance，之后普通 HTTP 复用它。
# 票失效或再次被拦时才重新开浏览器；取票失败再按 1/5 分钟冷却并回退 JavBus。
CHALLENGE_RETRY_SECONDS = 5.0
COOLDOWN_FIRST_SECONDS = 60
COOLDOWN_REPEAT_SECONDS = 300
TICKET_FILENAME = "cf_ticket.json"
TICKET_TTL_SECONDS = 1800
TICKET_REUSE_SECONDS = 10
_block_until = 0.0
_challenge_streak = 0
_block_lock = threading.Lock()
_ticket_lock = threading.Lock()


def _blocked() -> bool:
    """返回当前是否处于 Cloudflare 冷却期。"""
    with _block_lock:
        return time.time() < _block_until


def _block_for(seconds: int = COOLDOWN_FIRST_SECONDS) -> None:
    """进入冷却期，期间不再发起直连请求。"""
    global _block_until
    with _block_lock:
        _block_until = max(_block_until, time.time() + seconds)


def _note_challenge() -> int:
    """记录一次挑战失败并按连续次数设置冷却时长。"""
    global _challenge_streak
    with _block_lock:
        _challenge_streak += 1
        seconds = (
            COOLDOWN_FIRST_SECONDS
            if _challenge_streak == 1
            else COOLDOWN_REPEAT_SECONDS
        )
    _block_for(seconds)
    return seconds


def _reset_challenge() -> None:
    """请求成功后清除连续挑战计数。"""
    global _challenge_streak, _block_until
    with _block_lock:
        _challenge_streak = 0
        _block_until = 0.0


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

    @property
    def _ticket_path(self) -> Path:
        """Cloudflare 通行票缓存文件。"""
        return self._dir / TICKET_FILENAME

    def _load_ticket(self) -> Optional[Dict[str, Any]]:
        """读取本地保存的通行票。"""
        try:
            data = json.loads(self._ticket_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(data, dict):
            return None
        cookies = data.get("cookies")
        ua = str(data.get("ua") or "").strip()
        if not isinstance(cookies, dict) or not cookies or not ua:
            return None
        return data

    def _save_ticket(self, cookies: Dict[str, str], ua: str) -> None:
        """原子写入通行票。"""
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "cookies": cookies,
            "ua": ua,
            "created_at": time.time(),
        }
        temp = self._ticket_path.with_suffix(".tmp")
        temp.write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
        temp.replace(self._ticket_path)

    def _clear_ticket(self) -> None:
        """删除失效的通行票。"""
        try:
            self._ticket_path.unlink()
        except OSError:
            pass

    def _fresh_ticket(self) -> Optional[Dict[str, Any]]:
        """返回仍在有效期内的通行票。"""
        ticket = self._load_ticket()
        if not ticket:
            return None
        created_at = float(ticket.get("created_at") or 0)
        if time.time() - created_at > TICKET_TTL_SECONDS:
            return None
        return ticket

    def _mint_ticket(self) -> Dict[str, Any]:
        """用 cloakbrowser 过一次盾并保存 cf_clearance。"""
        with _ticket_lock:
            existing = self._fresh_ticket()
            if existing:
                created_at = float(existing.get("created_at") or 0)
                if time.time() - created_at < TICKET_REUSE_SECONDS:
                    return existing
            try:
                from cloakbrowser import launch_context
            except Exception as error:  # pragma: no cover - 依赖容器环境
                raise JavRankingError(
                    f"未安装 cloakbrowser，无法过 Cloudflare：{error}"
                ) from error
            context = None
            try:
                context = launch_context(headless=True)
                page = context.new_page()
                page.goto(
                    "https://javranking.cc/zh-hans/",
                    timeout=60000,
                    wait_until="domcontentloaded",
                )
                deadline = time.time() + 60
                while time.time() < deadline:
                    html = page.evaluate(
                        "() => document.documentElement.outerHTML"
                    )
                    if (
                        "Just a moment" not in html
                        and "JavRanking" in page.title()
                    ):
                        break
                    page.wait_for_timeout(1000)
                cookies = {
                    str(item.get("name")): str(item.get("value"))
                    for item in context.cookies()
                    if item.get("name") and item.get("value")
                }
                ua = str(page.evaluate("() => navigator.userAgent") or "")
                if not cookies.get("cf_clearance") or not ua:
                    raise JavRankingError("浏览器未能取得 cf_clearance")
                self._save_ticket(cookies, ua)
                logger.info(
                    f"JavRanking 已取得 Cloudflare 通行票"
                    f"（{len(cookies)} 个 cookie），有效期按 {TICKET_TTL_SECONDS // 60} 分钟处理"
                )
                return {
                    "cookies": cookies,
                    "ua": ua,
                    "created_at": time.time(),
                }
            except JavRankingError:
                raise
            except Exception as error:  # pragma: no cover - 浏览器环境相关
                raise JavRankingError(f"浏览器过盾失败：{error}") from error
            finally:
                if context is not None:
                    try:
                        context.close()
                    except Exception:
                        pass

    def _http_get(self, url: str,
                  ticket: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        用普通 HTTP 请求详情页。

        :param url: 详情页地址
        :param ticket: 通行票；为空表示裸请求
        :return: 200 时返回文本；命中 Cloudflare 挑战时返回 None
        """
        ua = str((ticket or {}).get("ua") or USER_AGENT)
        headers = {
            "User-Agent": ua,
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://javranking.cc/zh-hans/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
        }
        cookies = (ticket or {}).get("cookies") or None
        http = RequestUtils(headers=headers, timeout=30, use_session=True)
        try:
            throttle_wait("javranking")
            response = http.request(
                "get", url, headers=dict(headers), cookies=cookies
            )
            if response is None:
                raise JavRankingError("请求 JavRanking 失败：无响应")
            status = getattr(response, "status_code", 0)
            if status == 200:
                _reset_challenge()
                content = getattr(response, "content", None)
                if isinstance(content, (bytes, bytearray)):
                    return content.decode("utf-8", "replace")
                return response.text or ""
            challenge = (
                status in (403, 503)
                and str(
                    (getattr(response, "headers", {}) or {}).get(
                        "cf-mitigated", ""
                    )
                ).lower() == "challenge"
            )
            if challenge:
                return None
            raise JavRankingError(f"请求 JavRanking 失败：HTTP {status}")
        finally:
            try:
                http.close()
            except Exception:
                pass

    def _fetch_page(self, url: str) -> str:
        """抓取 JavRanking 详情页；优先复用通行票，必要时开浏览器取票。"""
        if _blocked():
            raise JavRankingError(
                "JavRanking 处于 Cloudflare 冷却期，暂时跳过直连抓取"
            )
        ticket = self._fresh_ticket()
        if ticket:
            text = self._http_get(url, ticket)
            if text is not None:
                return text
            # 票已失效：删掉后重新取票
            self._clear_ticket()
        else:
            text = self._http_get(url)
            if text is not None:
                return text
            logger.warning(
                "JavRanking 触发 Cloudflare 挑战，"
                f"{CHALLENGE_RETRY_SECONDS:g} 秒后重试"
            )
            time.sleep(CHALLENGE_RETRY_SECONDS)
            text = self._http_get(url)
            if text is not None:
                return text
        try:
            ticket = self._mint_ticket()
            text = self._http_get(url, ticket)
            if text is not None:
                return text
        except JavRankingError as error:
            logger.warning(f"JavRanking 过盾失败：{error}")
        seconds = _note_challenge()
        raise JavRankingError(
            "JavRanking 触发 Cloudflare 挑战（403），"
            f"冷却 {seconds // 60} 分钟后重试"
        )

    def fetch_remote_detail(self, code: str) -> Dict[str, Any]:
        """
        抓取 JavRanking 详情页并返回磁力。

        详情缓存只提供番号索引和详情页地址，不再用旧磁力兜底；
        抓取失败时由上层直接回退 JavBus，避免使用过期种子。

        :param code: 番号
        :return: 与 JavBusSource.fetch_detail 相同形状的字典
        """
        normalized = normalize_code(code)
        cached = self.fetch_detail(normalized)
        url = str(cached.get("detail_url") or "")
        if not url:
            raise JavRankingError(f"JavRanking 缓存中没有 {normalized} 的详情页地址")
        try:
            text = self._fetch_page(url)
            magnets = parse_magnet_html(text)
        except Exception as error:
            raise JavRankingError(f"JavRanking 抓取失败：{error}") from error
        if not magnets:
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
