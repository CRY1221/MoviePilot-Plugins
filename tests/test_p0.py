"""番号云下载 P0 回归测试。

不连接 MoviePilot、JavBus 或 CloudDrive2：

- 用空壳替换 `app.*` 模块，把插件源码复制成临时包后导入
- 用假 CD2 客户端验证整理逻辑
- 用假 `_get` 验证 JavBus 详情页只请求一次

运行：``python3 tests/test_p0.py``
"""

from __future__ import annotations

import importlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
for candidate in (HERE / "番号云下载", HERE / "plugins.v3" / "javclouddownload"):
    if candidate.is_dir():
        SRC = candidate
        break
else:  # pragma: no cover - 仓库布局变化时给出明确提示
    raise SystemExit("找不到插件源码目录")

TMP = tempfile.mkdtemp(prefix="jcd-pkg-")
PKG = Path(TMP) / "jcd"
PKG.mkdir()
for name in ("__init__.py", "cd2.py", "javranking.py", "pipeline.py", "selector.py", "source.py", "pyproject.toml"):
    shutil.copy(SRC / name, PKG / name)


# ---------- 替换 MoviePilot 运行时 ----------

app = types.ModuleType("app")
app.__path__ = []
plugins = types.ModuleType("app.plugins")


class _PluginBase:
    """只实现本测试用到的最小插件数据接口。"""

    def __init__(self) -> None:
        self._store = {}
        self._data_path = Path(tempfile.mkdtemp(prefix="mp-data-"))

    def get_data(self, key=None, plugin_id=None):
        return self._store.get(key)

    def save_data(self, key, value, plugin_id=None):
        self._store[key] = value

    def get_data_path(self, plugin_id=None):
        return self._data_path


plugins._PluginBase = _PluginBase
sdk = types.ModuleType("app.sdk")
sdk.__path__ = []
logging_mod = types.ModuleType("app.sdk.logging")


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


logging_mod.logger = _Logger()
network_mod = types.ModuleType("app.sdk.network")


class RequestUtils:
    def __init__(self, **kwargs):
        pass

    def request(self, *args, **kwargs):
        return None

    def close(self):
        pass


network_mod.RequestUtils = RequestUtils

grpc = types.ModuleType("grpc")


class _RpcError(Exception):
    pass


def _insecure_channel(*args, **kwargs):
    return None


grpc.RpcError = _RpcError
grpc.insecure_channel = _insecure_channel

sys.modules.update({
    "app": app,
    "app.plugins": plugins,
    "app.sdk": sdk,
    "app.sdk.logging": logging_mod,
    "app.sdk.network": network_mod,
    "grpc": grpc,
})
sys.path.insert(0, TMP)
jcd = importlib.import_module("jcd")
from jcd import pipeline, source  # noqa: E402

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    print(("PASS " if condition else "FAIL ") + name + (f" | {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


# ---------- 假 CD2 客户端 ----------

class _File:
    def __init__(self, name: str, size: int = 0, is_dir: bool = False) -> None:
        self.name = name
        self.size = size
        self.isDirectory = is_dir


class _FakeClient:
    def __init__(self, tree, dirs) -> None:
        self.tree = tree
        self.dirs = set(dirs)
        self.deleted: list[str] = []
        self.renamed: list[tuple[str, str]] = []

    def list_dir(self, path):
        return self.tree.get(path, [])

    def find(self, path):
        return object() if path in self.dirs else None

    def delete(self, path):
        self.deleted.append(path)
        return True

    def rename(self, path, new_name):
        self.renamed.append((path, new_name))
        return True

    def find_offline(self, folder, info_hash="", name=""):
        return None


GB = 1024 ** 3


def _tidy_case(delete_junk_only: bool):
    tree = {
        "/dl/old": [
            _File("main.mp4", 10 * GB),
            _File("sample.mp4", 100 * 1024 ** 2),
            _File("main.chs.srt", 100 * 1024),
            _File("ad.txt", 1024),
        ]
    }
    client = _FakeClient(tree, {"/dl/old"})
    result = pipeline.tidy_dir(client, "/dl/old", "ABC-123", delete_junk_only=delete_junk_only)
    return client, result


client, result = _tidy_case(True)
check("主视频模式：主视频被保留并改名", ("/dl/old/main.mp4", "ABC-123.mp4") in client.renamed, str(client.renamed))
check("主视频模式：删除推广视频", "/dl/old/sample.mp4" in client.deleted, str(client.deleted))
check("主视频模式：删除广告文件", "/dl/old/ad.txt" in client.deleted, str(client.deleted))
check("主视频模式：保留字幕", all("srt" not in path for path in client.deleted), str(client.deleted))
check("主视频模式：目录改名", result.get("final_dir") == "/dl/ABC-123", str(result))

client, result = _tidy_case(False)
check("保留全部视频模式：不删 sample", "/dl/old/sample.mp4" not in client.deleted, str(client.deleted))
check("保留全部视频模式：仍删广告", "/dl/old/ad.txt" in client.deleted, str(client.deleted))
check("保留全部视频模式：保留字幕", all("srt" not in path for path in client.deleted), str(client.deleted))


# ---------- JavBus：直连详情页只请求一次 ----------

DETAIL = """<html><title>ABC-123 - JavBus</title>
<script>var gid = 123; var img = 'x'; var uc = 0;</script>
<span>識別碼: ABC-123</span><span>發行日期: 2026-10-18</span><span>長度: 120分鐘</span>
</html>"""
AJAX = """<table><tr><td><a href="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567">ABC-123 4K</a><a>6.05GB</a><a>2026-10-18</a></td></tr></table>"""
AJAX_PATH = "/ajax/uncledatoolsbyajax.php?gid=123&lang=zh&img=x&uc=0&floor=500"


class _FakeSource(source.JavBusSource):
    def __init__(self, pages):
        super().__init__()
        self.pages = pages
        self.calls: list[str] = []

    def _get(self, path, referer=None):
        self.calls.append(path)
        if path not in self.pages:
            raise source.JavBusError("404")
        return self.pages[path]


direct = _FakeSource({"/ABC-123": DETAIL, AJAX_PATH: AJAX})
detail = direct.fetch_detail("ABC-123")
check("直连：详情页只请求一次", direct.calls.count("/ABC-123") == 1, str(direct.calls))
check("直连：解析到磁力", len(detail["magnets"]) == 1, str(detail))

fallback = _FakeSource({
    "/ABC-123": "<html>not detail</html>",
    "/search/ABC-123": '<a class="movie-box" href="/ABC-123_2026-10-18">x</a>',
    "/ABC-123_2026-10-18": DETAIL,
    AJAX_PATH: AJAX,
})
fallback_detail = fallback.fetch_detail("ABC-123")
check("回退：命中搜索链接", "/search/ABC-123" in fallback.calls and "/ABC-123_2026-10-18" in fallback.calls, str(fallback.calls))
check("回退：解析到磁力", len(fallback_detail["magnets"]) == 1, str(fallback_detail))


# ---------- 插件生命周期与任务推进 ----------

inst = jcd.JavCloudDownload()
inst._store["tasks"] = {"ABC-123": {"code": "ABC-123", "state": "failed"}}
inst.init_plugin({"enabled": False})
check("init_plugin 保留已存任务", inst._store["tasks"].get("ABC-123") is not None, repr(inst._store))

inst2 = jcd.JavCloudDownload()
inst2._max_wait_minutes = 1
record = {
    "code": "ABC-123",
    "name": "ABC-123",
    "state": "submitted",
    "submitted_at": "2020-01-01 00:00:00",
    "folder": "/dl",
}
inst2._advance_record(_FakeClient({}, set()), record)
check("超过 max_wait 标记失败", record.get("state") == "failed", str(record))

finished = {"state": "finished", "submitted_at": "2020-01-01 00:00:00"}
check("已完成任务不受 max_wait 影响", inst2._wait_exceeded(finished) is False, str(finished))

limited = {"code": "ABC-123", "organize_attempts": jcd.MAX_ORGANIZE_ATTEMPTS + 1}
gave_up = inst2._organize_give_up(limited, "未找到落盘目录")
check("整理超过上限停止重试", gave_up and limited.get("state") == "failed", str(limited))


# ---------- JavRanking 缓存数据源 ----------

CACHE = Path(tempfile.mkdtemp(prefix="jr-cache-"))
DB = CACHE / "javranking.db"
MAGNETS = [
    {
        "name": "START-257-UC.torrent.无码破解",
        "size_text": "7.38GB", "size_bytes": 7924214661, "date": "",
        "magnet": "magnet:?xt=urn:btih:" + "a" * 40,
        "info_hash": "a" * 40,
    },
    {
        "name": "START-257-C.torrent",
        "size_text": "5.82GB", "size_bytes": 6249177415, "date": "",
        "magnet": "magnet:?xt=urn:btih:" + "b" * 40,
        "info_hash": "b" * 40,
    },
]
con = sqlite3.connect(DB)
con.executescript(
    """
    CREATE TABLE videos (id INTEGER PRIMARY KEY, code TEXT, title TEXT, url TEXT);
    CREATE TABLE rankings (list TEXT, rank INTEGER, code TEXT, title TEXT,
                           date TEXT, video_id INTEGER, url TEXT);
    CREATE TABLE details (code TEXT PRIMARY KEY, video_id INTEGER, title TEXT,
                          release_date TEXT, duration TEXT, url TEXT,
                          magnets_json TEXT, fetched_at TEXT);
    """
)
con.execute("insert into videos values (78, 'START-257', '测试标题', 'https://javranking.cc/zh-hans/videos/78/')")
con.execute("insert into videos values (3127, 'ABF-394', '无详情标题', 'https://javranking.cc/zh-hans/videos/3127/')")
con.execute("insert into rankings values ('main', 11, 'START-257', '测试标题', '2025-03-06', 78, 'https://javranking.cc/zh-hans/videos/78/')")
con.execute(
    "insert into details values (?, ?, ?, ?, ?, ?, ?, ?)",
    ("START-257", 78, "测试标题", "2025-03-06", "PT144M",
     "https://javranking.cc/zh-hans/videos/78/", json.dumps(MAGNETS, ensure_ascii=False), "2026-10-20 01:42:04"),
)
con.commit(); con.close()

jr = jcd.JavRankingSource(str(CACHE))
stats = jr.stats()
check("缓存：videos 统计", stats.get("videos") == 2, str(stats))
check("缓存：details 统计", stats.get("details") == 1, str(stats))
detail = jr.fetch_detail("START-257")
check("缓存：读取详情磁力", len(detail.get("magnets") or []) == 2 and detail.get("source") == "javranking", str(detail))
check("缓存：番号查询", (jr.lookup("START-257") or {}).get("has_magnets") is True)
check("缓存：榜单读取", len(jr.rankings("main", 10)) == 1)
try:
    jr.fetch_detail("NOPE-000")
    check("缓存：未知番号报错", False)
except jcd.JavRankingError:
    check("缓存：未知番号报错", True)

inst3 = jcd.JavCloudDownload()
inst3._source_mode = "javranking"
inst3._javranking_dir = str(CACHE)
inst3._allow_uncensored = False
inst3._allow_junk = False
preview = inst3.preview("START-257")
check("数据源：javranking 选种", preview.get("source") == "javranking" and preview.get("tier") == "中文字幕", str(preview))
check("数据源：排除 UC 后选中 -C", (preview.get("chosen") or {}).get("name") == "START-257-C.torrent", str(preview.get("chosen")))

try:
    inst3.preview("ABF-394")
    check("数据源：javranking 无磁力报错", False)
except jcd.JavRankingError:
    check("数据源：javranking 无磁力报错", True)

inst4 = jcd.JavCloudDownload()
inst4._source_mode = "auto"
inst4._javranking_dir = str(CACHE)
inst4._allow_uncensored = False
inst4._allow_junk = False
check("数据源：auto 命中缓存", inst4._fetch_detail("START-257").get("source") == "javranking")

original = jcd.source.JavBusSource.fetch_detail
def fake_fetch(self, code):
    return {
        "code": code, "title": "JavBus 回退", "release_date": "", "duration": "",
        "detail_url": "", "magnets": [{"name": "X-C", "size_text": "1GB", "size_bytes": 1,
                                       "date": "", "magnet": "magnet:?xt=urn:btih:" + "c" * 40,
                                       "info_hash": "c" * 40}],
    }
jcd.source.JavBusSource.fetch_detail = fake_fetch
try:
    fallback = inst4._fetch_detail("ABF-394")
    check("数据源：auto 无磁力回退 JavBus", fallback.get("source") == "javbus", str(fallback))
finally:
    jcd.source.JavBusSource.fetch_detail = original

print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
