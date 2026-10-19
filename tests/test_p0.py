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
import time
import types
from datetime import datetime, timedelta
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
for name in ("__init__.py", "cd2.py", "javranking.py", "library.py", "pipeline.py", "selector.py", "source.py", "throttle.py", "pyproject.toml"):
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
        self.messages = []

    def get_data(self, key=None, plugin_id=None):
        return self._store.get(key)

    def save_data(self, key, value, plugin_id=None):
        self._store[key] = value

    def get_data_path(self, plugin_id=None):
        return self._data_path

    def post_message(self, **kwargs):
        self.messages.append(kwargs)


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

schemas_mod = types.ModuleType("app.schemas")
schemas_mod.__path__ = []
types_mod = types.ModuleType("app.schemas.types")


class MessageType:
    Plugin = "插件"


types_mod.MessageType = MessageType

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
    "app.schemas": schemas_mod,
    "app.schemas.types": types_mod,
    "grpc": grpc,
})
sys.path.insert(0, TMP)
jcd = importlib.import_module("jcd")
from jcd import cd2, pipeline, selector, source  # noqa: E402
from jcd import library as media_library  # noqa: E402


class _OfflineStatus:
    NAMES = {
        0: "OFFLINE_INIT",
        1: "OFFLINE_DOWNLOADING",
        2: "OFFLINE_FINISHED",
        3: "OFFLINE_ERROR",
    }

    @classmethod
    def Name(cls, value):
        return cls.NAMES.get(int(value), "OFFLINE_UNKNOWN")


class _FakePB:
    OfflineFileStatus = _OfflineStatus


cd2.pb = _FakePB
cd2.pb_grpc = object()

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


class _FakeOfflineTask:
    def __init__(self, name: str, info_hash: str, status: int = 0,
                 percent: float = 0) -> None:
        self.name = name
        self.infoHash = info_hash
        self.status = status
        self.percendDone = percent


class _FakeClient:
    def __init__(self, tree, dirs, offline=None) -> None:
        self.tree = tree
        self.dirs = set(dirs)
        self.deleted: list[str] = []
        self.renamed: list[tuple[str, str]] = []
        self.offline = list(offline or [])
        self.added: list[tuple[str, str]] = []
        self.removed: list[tuple[str, list[str], bool]] = []
        self.remove_result = True
        self.delete_result = True
        self.moved: list[tuple[list[str], str, bool]] = []

    def list_dir(self, path, force_refresh=False):
        return self.tree.get(path, [])

    def find(self, path):
        return object() if path in self.dirs else None

    def delete(self, path):
        self.deleted.append(path)
        if not self.delete_result:
            return False
        self.dirs.discard(path)
        self.tree.pop(path, None)
        return True

    def rename(self, path, new_name):
        self.renamed.append((path, new_name))
        return True

    def find_offline(self, folder, info_hash="", name="", force_refresh=False):
        target = (info_hash or "").lower()
        for task in self.offline:
            if target and (task.infoHash or "").lower() == target:
                return task
            if name and task.name == name:
                return task
        return None

    def list_offline(self, folder, force_refresh=False):
        return list(self.offline)

    def add_offline(self, url, folder):
        self.added.append((url, folder))
        return "submitted"

    def remove_offline(self, folder, info_hashes, delete_files=False):
        hashes = {(item or "").lower() for item in info_hashes}
        self.removed.append((folder, list(info_hashes), delete_files))
        if not self.remove_result:
            return False
        self.offline = [
            task for task in self.offline
            if (task.infoHash or "").lower() not in hashes
        ]
        return True

    def quota(self, folder):
        return {"total": 100, "used": 0, "left": 100}

    def move(self, paths, dest_parent, overwrite=True):
        self.moved.append((list(paths), dest_parent, overwrite))
        for path in paths:
            name = path.rstrip("/").split("/")[-1]
            self.dirs.discard(path)
            self.dirs.add(f"{dest_parent.rstrip('/')}/{name}")
        return True

    def close(self):
        pass


GB = 1024 ** 3


def _tidy_case(delete_junk_only: bool):
    tree = {
        "/dl/old": [
            _File("main.mp4", 10 * GB),
            _File("sample.mp4", 400 * 1024 ** 2),
            _File("main.chs.srt", 100 * 1024),
            _File("ad.txt", 1024),
            _File("poster.jpg", 2048),
            _File("movie.nfo", 4096),
        ]
    }
    client = _FakeClient(tree, {"/dl/old"})
    result = pipeline.tidy_dir(client, "/dl/old", "ABC-123", delete_junk_only=delete_junk_only)
    return client, result


client, result = _tidy_case(True)
check("主视频模式：主视频被保留并改名", ("/dl/old/main.mp4", "ABC-123.mp4") in client.renamed, str(client.renamed))
check("主视频模式：删除推广视频", "/dl/old/sample.mp4" in client.deleted, str(client.deleted))
check("主视频模式：删除广告文件", "/dl/old/ad.txt" in client.deleted, str(client.deleted))
check("主视频模式：删除图片和 nfo", "/dl/old/poster.jpg" in client.deleted and "/dl/old/movie.nfo" in client.deleted, str(client.deleted))
check("主视频模式：保留字幕", all("srt" not in path for path in client.deleted), str(client.deleted))
check("主视频模式：目录改名", result.get("final_dir") == "/dl/ABC-123", str(result))

client, result = _tidy_case(False)
check("保留全部视频模式：不删 sample", "/dl/old/sample.mp4" not in client.deleted, str(client.deleted))
check("保留全部视频模式：仍删广告", "/dl/old/ad.txt" in client.deleted, str(client.deleted))
check("保留全部视频模式：保留字幕", all("srt" not in path for path in client.deleted), str(client.deleted))

# ---------- 多段资源（CD1/CD2、part1/part2）----------

def _tidy_multipart_case(prefix: str):
    tree = {
        "/dl/multi": [
            _File(f"{prefix}-CD1.mp4", 6 * GB),
            _File(f"{prefix}-CD2.mp4", 6 * GB),
            _File("sample.mp4", 400 * 1024 ** 2),
            _File(f"{prefix}-CD1.chs.srt", 100 * 1024),
        ]
    }
    client = _FakeClient(tree, {"/dl/multi"})
    result = pipeline.tidy_dir(client, "/dl/multi", "ABC-123", delete_junk_only=True)
    return client, result

client, result = _tidy_multipart_case("OLD")
check("多段：两个分段都保留", "/dl/multi/OLD-CD1.mp4" not in client.deleted and "/dl/multi/OLD-CD2.mp4" not in client.deleted, str(client.deleted))
check("多段：推广视频仍删除", "/dl/multi/sample.mp4" in client.deleted, str(client.deleted))
check("多段：两个分段都改名", ("/dl/multi/OLD-CD1.mp4", "ABC-123 - part1.mp4") in client.renamed and ("/dl/multi/OLD-CD2.mp4", "ABC-123 - part2.mp4") in client.renamed, str(client.renamed))
check("多段：final_files 两条", len(result.get("final_files") or []) == 2, str(result))
check("多段：目录改名", result.get("final_dir") == "/dl/ABC-123", str(result))

tree = {
    "/dl/part": [
        _File("OLD-part1.mkv", 5 * GB),
        _File("OLD-part2.mkv", 5 * GB),
    ]
}
client = _FakeClient(tree, {"/dl/part"})
result = pipeline.tidy_dir(client, "/dl/part", "XYZ-999", delete_junk_only=True)
check("多段：part1/part2 识别", ("/dl/part/OLD-part1.mkv", "XYZ-999 - part1.mkv") in client.renamed and ("/dl/part/OLD-part2.mkv", "XYZ-999 - part2.mkv") in client.renamed, str(client.renamed))

tree = {
    "/dl/cn": [
        _File("main.mp4", 10 * GB),
        _File("main.chs.srt", 100 * 1024),
    ]
}
client = _FakeClient(tree, {"/dl/cn"})
result = pipeline.tidy_dir(
    client, "/dl/cn", "ABC-123", delete_junk_only=True, file_name="ABC-123-C"
)
check("中文单文件：番号带 -C",
      ("/dl/cn/main.mp4", "ABC-123-C.mp4") in client.renamed,
      str(client.renamed))
check("中文单文件：字幕跟随改名",
      ("/dl/cn/main.chs.srt", "ABC-123-C.chs.srt") in client.renamed,
      str(client.renamed))

tree = {
    "/dl/cnpart": [
        _File("OLD-CD0.mp4", 6 * GB),
        _File("OLD-CD1.mp4", 6 * GB),
    ]
}
client = _FakeClient(tree, {"/dl/cnpart"})
result = pipeline.tidy_dir(
    client, "/dl/cnpart", "ABC-123", delete_junk_only=True,
    file_name="ABC-123-C",
)
check("中文多段：part 从 1 开始",
      ("/dl/cnpart/OLD-CD0.mp4", "ABC-123-C - part1.mp4") in client.renamed
      and ("/dl/cnpart/OLD-CD1.mp4", "ABC-123-C - part2.mp4") in client.renamed,
      str(client.renamed))

tree = {
    "/dl/full": [
        _File("ABC-123.mp4", 10 * GB),
        _File("promo-CD1.mp4", 5 * 1024 ** 2),
        _File("promo-CD2.mp4", 5 * 1024 ** 2),
    ]
}
client = _FakeClient(tree, {"/dl/full"})
result = pipeline.tidy_dir(client, "/dl/full", "ABC-123", delete_junk_only=True)
check("多段：小体积伪分段不参与", "/dl/full/ABC-123.mp4" not in client.deleted, str(client.deleted))
check("多段：伪分段被删除", "/dl/full/promo-CD1.mp4" in client.deleted and "/dl/full/promo-CD2.mp4" in client.deleted, str(client.deleted))

tree = {
    "/dl/full2": [
        _File("ABC-123.mp4", 10 * GB),
        _File("ABC-123-CD1.mp4", 6 * GB),
        _File("ABC-123-CD2.mp4", 6 * GB),
    ]
}
client = _FakeClient(tree, {"/dl/full2"})
result = pipeline.tidy_dir(client, "/dl/full2", "ABC-123", delete_junk_only=True)
check("多段：已有完整成片时不被分段覆盖", "/dl/full2/ABC-123.mp4" not in client.deleted, str(client.deleted))
check("多段：完整成片场景删除分段", "/dl/full2/ABC-123-CD1.mp4" in client.deleted, str(client.deleted))


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

fresh = jcd.JavCloudDownload()
check("默认配置不含个人 CD2 地址", fresh._cd2_host == "", fresh._cd2_host)
check("默认配置不含个人下载目录", fresh._download_dir == "", fresh._download_dir)
form, defaults = fresh.get_form()
check("配置文件包含主动洗版",
      "proactive_upgrade" in json.dumps(form, ensure_ascii=False)
      and defaults.get("proactive_upgrade") is False,
      str(defaults.get("proactive_upgrade")))

one_obj = jcd.JavCloudDownload()
one_calls = []
one_obj.scan_proactive = lambda: one_calls.append(True)
one_config = {
    "enabled": True,
    "cd2_host": "cd2.example.com",
    "cd2_token": "x",
    "download_dir": "/cloud/download",
    "emby_library": "成人影片",
    "enable_upgrade": True,
    "proactive_upgrade": True,
}
one_obj.init_plugin(one_config)
time.sleep(0.05)
one_obj.init_plugin(one_config)
time.sleep(0.05)
check("主动洗版：开启后只扫一次",
      len(one_calls) == 1
      and one_obj._store.get("proactive_state", {}).get("enabled") is True,
      f"{one_calls} {one_obj._store.get('proactive_state')}")

missing_dir = jcd.JavCloudDownload()
missing_dir.init_plugin({"enabled": True, "cd2_host": "cd2.example.com", "cd2_token": "x"})
check("缺少下载目录时给出提醒", "缺少云下载目录" in missing_dir._status, missing_dir._status)

missing_host = jcd.JavCloudDownload()
missing_host.init_plugin({"enabled": True, "download_dir": "/cloud/download", "cd2_token": "x"})
check("缺少 CD2 地址时给出提醒", "缺少 CloudDrive2 地址" in missing_host._status, missing_host._status)

no_dir_submit = jcd.JavCloudDownload()
no_dir_submit.init_plugin({"enabled": True, "cd2_host": "cd2.example.com", "cd2_token": "x"})
no_dir_submit.preview = lambda code: {"code": code, "chosen": {"magnet": "magnet:?xt=urn:btih:" + "0" * 40}}
try:
    no_dir_submit._submit_one("ABC-123", 0)
    blocked = False
except jcd.Cd2Error as error:
    blocked = "云下载目录" in str(error)
check("未配置下载目录时拒绝提交", blocked, "未拦截")

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


# ---------- 候选超时换种 ----------

OLD = (datetime.now() - timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")


def _candidate(name, char):
    return {
        "name": name,
        "magnet": "magnet:?xt=urn:btih:" + char * 40,
        "info_hash": char * 40,
        "size_bytes": 1024 ** 3,
        "size_text": "1GB",
    }


def _retry_instance():
    obj = jcd.JavCloudDownload()
    obj._download_dir = "/dl"
    obj._max_candidates = 3
    obj._candidate_timeout_minutes = 5
    obj._candidate_queue_timeout_minutes = 10
    return obj


def _candidate_record(obj, candidates, index=0):
    rec = obj._new_record("ABC-123", "ABC-123")
    rec.update({
        "candidates": candidates,
        "candidate_index": index,
        "state": jcd.STATE_DOWNLOADING,
        "candidate_started_at": OLD,
        "submitted_at": OLD,
        "info_hash": candidates[index]["info_hash"],
        "magnet_name": candidates[index]["name"],
        "task_name": candidates[index]["name"],
    })
    return rec


_mp_db = Path(TMP) / "mp-user.db"
_mp_con = sqlite3.connect(_mp_db)
_mp_con.execute(
    "create table mediaserveritem(server text, library text, item_id text, "
    "title text, original_title text, path text)"
)
_mp_con.execute(
    "insert into mediaserveritem values(?,?,?,?,?,?)",
    ("emby", "3", "117", "SNIS-110 标题", "",
     "/mnt/strm/adult/女优/SNIS-110/SNIS-110.strm"),
)
_mp_con.execute(
    "insert into mediaserveritem values(?,?,?,?,?,?)",
    ("emby", "3", "118", "SNIS-1100 标题", "",
     "/mnt/strm/adult/女优/SNIS-1100/SNIS-1100.strm"),
)
_mp_con.commit()
_mp_con.close()
_mp_rows = media_library.EmbyLibrary("成人影片")._mp_rows("SNIS-110", db_path=_mp_db)
check("MP 索引：命中 SNIS-110 且不误匹配 SNIS-1100",
      len(_mp_rows) == 1 and _mp_rows[0]["item_id"] == "117",
      str(_mp_rows))

check("JavRanking 榜单名：年份榜",
      jcd.JavRankingSource.label_for("javdb_top250_2024") == "JAVDB 2024 Top250")
check("JavRanking 榜单名：DMM 月榜",
      jcd.JavRankingSource.label_for("dmm_monthly_2025_11") == "DMM 月榜 2025-11")
check("JavRanking 封面：按 video_id 拼接",
      jcd.JavRankingSource.cover_url(123)
      == "https://static.javranking.cc/video/cover/123.webp")

# ---------- Emby 库基线 / 洗版 ----------

base_candidate = _candidate("ABC-123 FHD", "a")
q_4k_candidate = {
    "name": "ABC-123 2160P",
    "magnet": "magnet:?xt=urn:btih:" + "e" * 40,
    "info_hash": "e" * 40,
    "size_bytes": 6 * GB,
    "size_text": "6GB",
}

q_4k = media_library.estimate_candidate_quality("ABC-123 2160P", 6 * GB)
q_fake_4k = media_library.estimate_candidate_quality("ABC-123 2160P", 4 * GB)
q_2k = media_library.estimate_candidate_quality("ABC-123 1440P", 4 * GB)
q_1080 = media_library.estimate_candidate_quality("ABC-123 FHD", 5 * GB)
check("候选估算：2160P 达 5GB 算 2160", q_4k["height"] == 2160, str(q_4k))
check("候选估算：2160P 不足 5GB 不算 2160", q_fake_4k["height"] != 2160, str(q_fake_4k))
check("候选估算：1440P 算 1440", q_2k["height"] == 1440, str(q_2k))
check("候选估算：6K 不当作分辨率标签",
      media_library.estimate_candidate_quality(
          "IPX-899_6K-C 高清", 5723043921)["height"] == 0)
check("候选估算：名称不解析码率",
      media_library.estimate_candidate_quality(
          "IPX-899_6K-C 高清", 5723043921)["bitrate"] == 0)
check("洗版判断：同 1080 且体积不足 1.5 倍不洗",
      media_library.candidate_may_be_better(
          {"height": 1080, "bitrate": 5746007, "size": 5345542264},
          media_library.estimate_candidate_quality("IPX-899_6K-C 高清", 5723043921),
      ) is False)
_q8k = media_library.estimate_candidate_quality("X 8K", 6 * GB)
check("候选估算：8K 不猜测分辨率与码率",
      _q8k["height"] == 0 and _q8k["bitrate"] == 0, str(_q8k))
check("候选估算：裸 2K 不猜，不算 1440",
      media_library.estimate_candidate_quality(
          "IPX-899_2K-C 字幕", 1986422374)["height"] == 0)
check("候选估算：裸 4K 达 5GB 算 2160",
      media_library.estimate_candidate_quality("X 4K", 6 * GB)["height"] == 2160)
check("候选估算：裸 4K 不足 5GB 不算 2160",
      media_library.estimate_candidate_quality("X 4K", 4 * GB)["height"] == 0)
check("候选估算：FHD 算 1080", q_1080["height"] == 1080, str(q_1080))

existing_1080 = {"height": 1080, "size": 5 * GB, "bitrate": 8_000_000,
                 "has_chinese_sub": False}
check("洗版判断：4K 高于 1080",
      media_library.candidate_may_be_better(existing_1080, q_4k) is True)
check("洗版判断：同 1080 只大 1.2 倍不洗",
      media_library.candidate_may_be_better(
          existing_1080,
          media_library.estimate_candidate_quality("ABC-123 FHD", int(6 * GB)),
      ) is False)
check("洗版判断：同 1080 大 1.5 倍可洗",
      media_library.candidate_may_be_better(
          existing_1080,
          media_library.estimate_candidate_quality("ABC-123 FHD", int(8 * GB)),
      ) is True)
check("中字标记：-C 保留判断",
      selector.has_chinese_sub("IPX-899_6K-C") is True)

check("分辨率标签：1080",
      media_library.resolution_label(1080) == "1080P")
check("分辨率标签：2160",
      media_library.resolution_label(2160) == "2160P")
check("分辨率标签：未知高度不写",
      media_library.resolution_label(0) == "")

check("真实复核：2160 高于 1080",
      media_library.actual_is_better(existing_1080, {"height": 2160, "size": 6 * GB}) is True)
check("真实复核：1080 不高于 1080",
      media_library.actual_is_better(existing_1080, {"height": 1080, "size": 5 * GB}) is False)

hit = media_library.LibraryHit(
    server="emby", item_id="item-1", path="/lib/adult/ABC-123/ABC-123.mp4",
    quality=existing_1080,
)
skip_obj = _retry_instance()
skip_obj._find_library_item = lambda code, probe_missing=True: hit
skip_obj._enable_upgrade = False
skip_obj.preview = lambda code: (_ for _ in ()).throw(AssertionError("不应搜索"))
skip_result = skip_obj._submit_one("ABC-123", 0)
check("库内有且未开洗版：直接跳过",
      skip_result.get("skipped") is True
      and skip_obj._tasks["ABC-123"].get("state") == jcd.STATE_SKIPPED,
      str(skip_result))

_probe_calls = []


def _record_probe(code, probe_missing=True):
    _probe_calls.append(probe_missing)
    return hit


probe_obj = _retry_instance()
probe_obj._find_library_item = _record_probe
probe_obj._enable_upgrade = False
probe_obj.preview = lambda code: (_ for _ in ()).throw(AssertionError("不应搜索"))
probe_obj._submit_one("ABC-123", 0)
check("普通下载不探测库内文件", _probe_calls == [False], str(_probe_calls))

upgrade_obj = _retry_instance()
upgrade_obj._find_library_item = lambda code, probe_missing=True: hit
upgrade_obj._enable_upgrade = True
upgrade_obj.preview = lambda code: {
    "code": code, "tier": "中文字幕", "chosen": base_candidate,
    "candidates": [base_candidate],
}
upgrade_skip = upgrade_obj._submit_one("ABC-123", 0, upgrade=True)
check("开洗版但候选不够好：跳过",
      upgrade_skip.get("skipped") is True,
      str(upgrade_skip))

upgrade_client = _FakeClient({}, set())
upgrade_download = _retry_instance()
upgrade_download._find_library_item = lambda code, probe_missing=True: hit
upgrade_download._enable_upgrade = True
upgrade_download.preview = lambda code: {
    "code": code, "tier": "4K", "chosen": q_4k_candidate,
    "candidates": [q_4k_candidate],
}
upgrade_download._client = lambda: upgrade_client
upgrade_result = upgrade_download._submit_one("ABC-123", 0, upgrade=True)
check("开洗版且候选更好：提交下载",
      upgrade_result.get("ok") is True
      and upgrade_client.added == [(q_4k_candidate["magnet"], "/dl")],
      str(upgrade_result))

exist_client = _FakeClient(
    {"/dl/ABC-123": [_File("ABC-123.mp4", 6 * GB)]},
    {"/dl/ABC-123"},
)
exist_obj = _retry_instance()
exist_obj._find_library_item = lambda code, probe_missing=True: None
exist_obj._move_enabled = True
exist_obj._library_dir = "/lib"
exist_obj._delete_junk_only = True
exist_obj._enable_filter_rules = False
exist_obj._client = lambda: exist_client
exist_obj.preview = lambda code: {
    "code": code, "tier": "中文字幕", "chosen": base_candidate,
    "candidates": [base_candidate],
}
exist_result = exist_obj._submit_one("ABC-123", 0)
check("重复提交已有成片：继续整理并移动",
      exist_result.get("moved") is True
      and exist_obj._tasks["ABC-123"].get("state") == jcd.STATE_MOVED
      and exist_client.moved[-1][0] == ["/dl/ABC-123"],
      str(exist_result) + " " + str(exist_client.moved))

multi_replace = _FakeClient(
    {
        "/lib/加美": [_File("ipx-899.mp4", 5 * GB), _File("ipx-422.mp4", 8 * GB)],
        "/dl/IPX-899": [_File("IPX-899.mp4", 6 * GB)],
    },
    {"/lib/加美", "/dl/IPX-899"},
)
multi_result = pipeline.replace_existing_files(
    multi_replace, "/dl/IPX-899", "/lib/加美",
    ["/dl/IPX-899/IPX-899.mp4"], old_name="ipx-899.mp4", code="IPX-899",
)
check("洗版替换：目录有多个视频时只删目标视频",
      multi_result.get("ok") is True
      and multi_replace.deleted == ["/lib/加美/ipx-899.mp4"]
      and multi_replace.moved[-1][:2] == (["/dl/IPX-899/ipx-899.mp4"], "/lib/加美"),
      str(multi_result) + " " + str(multi_replace.deleted) + " " + str(multi_replace.moved))

missing_replace = _FakeClient(
    {"/lib/加美": [_File("ipx-422.mp4", 8 * GB)]},
    {"/lib/加美", "/dl/IPX-899"},
)
missing_result = pipeline.replace_existing_files(
    missing_replace, "/dl/IPX-899", "/lib/加美",
    ["/dl/IPX-899/IPX-899.mp4"], old_name="ipx-899.mp4", code="IPX-899",
)
check("洗版替换：找不到旧视频时不删除任何内容",
      missing_result.get("ok") is False and missing_replace.deleted == [],
      str(missing_result))

single_client = _FakeClient(
    {
        "/lib/ABC-123": [_File("ABC-123.mp4", 5 * GB)],
        "/dl/ABC-123": [_File("ABC-123.mp4", 6 * GB)],
    },
    {"/lib/ABC-123", "/dl/ABC-123"},
)
single_result = pipeline.replace_existing_files(
    single_client, "/dl/ABC-123", "/lib/ABC-123",
    ["/dl/ABC-123/ABC-123.mp4"], old_name="ABC-123.mp4", code="ABC-123",
)
check("洗版替换：目录只有一个视频时替换整个目录",
      single_result.get("ok") is True
      and "/lib/ABC-123" in single_client.deleted
      and single_client.moved[-1][:2] == (["/dl/ABC-123"], "/lib"),
      str(single_result) + " " + str(single_client.moved))

stale_client = _FakeClient(
    {"/dl": [_File("ABC-123", is_dir=True)]}, set()
)
check("落盘目录：find 缓存未命中时返回空",
      pipeline.locate_download_dir(stale_client, "/dl", "ABC-123") is None)
check("落盘目录：强制刷新后能找到",
      pipeline.locate_download_dir(
          stale_client, "/dl", "ABC-123", force_refresh=True
      ) == "/dl/ABC-123")

empty_client = _FakeClient(
    {"/dl/ABC-123": [
        _File("main.mp4", 5 * GB),
        _File("2048", 0, True),
        _File("宣傳文件", 0, True),
    ]},
    {"/dl/ABC-123"},
)
empty_removed = pipeline.remove_empty_dirs(empty_client, "/dl/ABC-123")
check("整理后清理空目录",
      set(empty_removed) == {"/dl/ABC-123/2048", "/dl/ABC-123/宣傳文件"},
      str(empty_removed))

# ---------- 移动与替换 ----------

class _RootLibrary:
    def __init__(self, library_id, name, path=""):
        self.id = library_id
        self.name = name
        self.path = path


class _RootInstance:
    def get_librarys(self):
        return [_RootLibrary("3", "成人影片", "")]

    def get_data(self, url):
        assert "VirtualFolders" in url, url
        class _Response:
            @staticmethod
            def json():
                return [{"ItemId": "3", "Locations": ["/mnt/strm/adult"]}]
        return _Response()


check("目录映射：解析 Emby:CD2",
      media_library._parse_path_map("/mnt/strm/adult:/cd2/adult")
      == [("/mnt/strm/adult", "/cd2/adult")])

_map_lib = media_library.EmbyLibrary("成人影片", "/mnt/strm/adult:/cd2/adult")
check("目录映射：旧文件路径换算为 CD2 目录",
      _map_lib._mapped_dir("/mnt/strm/adult/女优/ABC-123/ABC-123.strm")
      == "/cd2/adult/女优/ABC-123")

check("Emby 库根：get_librarys 无路径时回退 VirtualFolders",
      media_library.EmbyLibrary._library_roots(_RootInstance())
      == {"3": "/mnt/strm/adult"})

check("Emby 相对目录：按库根换算子目录",
      media_library.EmbyLibrary._relative_dir(
          "/mnt/strm/adult/女优/ABC-123/ABC-123.strm", "/mnt/strm/adult"
      ) == "女优/ABC-123")

nested_client = _FakeClient({}, {"/dl/ABC-123", "/lib/女优/ABC-123"})
nested_result = pipeline.move_dir(
    nested_client, "/dl/ABC-123", "/lib",
    replace=True, dest_dir="/lib/女优/ABC-123",
)
check("原位替换：删除旧子目录并移回原父目录",
      nested_result.get("ok") is True
      and "/lib/女优/ABC-123" in nested_client.deleted
      and nested_client.moved[-1][:2] == (["/dl/ABC-123"], "/lib/女优"),
      str(nested_result) + " " + str(nested_client.moved))

move_client = _FakeClient({}, {"/dl/ABC-123", "/lib/ABC-123"})
move_result = pipeline.move_dir(move_client, "/dl/ABC-123", "/lib", replace=True)
check("替换移动：先删旧目录",
      "/lib/ABC-123" in move_client.deleted,
      str(move_client.deleted))
check("替换移动：再移动新目录",
      move_result.get("ok") is True
      and move_client.moved
      and move_client.moved[-1][0] == ["/dl/ABC-123"],
      str(move_result))

no_replace_client = _FakeClient({}, {"/dl/ABC-123", "/lib/ABC-123"})
no_replace = pipeline.move_dir(no_replace_client, "/dl/ABC-123", "/lib", replace=False)
check("不替换时目标存在：跳过",
      no_replace.get("ok") is False
      and no_replace_client.deleted == [],
      str(no_replace))


retry = _retry_instance()
cands = [
    _candidate("c0", "a"),
    {"name": "c0-dup", "magnet": "magnet:?xt=urn:btih:" + "a" * 40,
     "info_hash": "a" * 40, "size_bytes": 0, "size_text": ""},
    _candidate("c1", "b"),
    _candidate("c2", "c"),
    _candidate("c3", "d"),
]
prepared = retry._prepare_candidates({"chosen": cands[0], "candidates": cands})
check("候选按 info_hash 去重", len(prepared) == 3, str(prepared))
check("候选最多保留 3 条", [c["name"] for c in prepared] == ["c0", "c1", "c2"], str(prepared))

submit_client = _FakeClient({}, set())
submit_obj = _retry_instance()
submit_obj.preview = lambda code: {
    "code": code,
    "tier": "中文字幕",
    "chosen": cands[0],
    "candidates": cands,
}
submit_obj._client = lambda: submit_client
submitted = submit_obj._submit_one("ABC-123", 0)
submitted_record = submit_obj._tasks["ABC-123"]
check("提交成功并保存候选", submitted.get("ok") and len(submitted_record["candidates"]) == 3,
      str(submitted_record))
check("只提交第一条候选", submit_client.added == [(cands[0]["magnet"], "/dl")],
      str(submit_client.added))
check("提交后 candidate_index=0", submitted_record.get("candidate_index") == 0,
      str(submitted_record))

timeout_client = _FakeClient(
    {}, {"/dl/c0"}, offline=[_FakeOfflineTask("c0", "a" * 40, status=1, percent=42)]
)
timeout_obj = _retry_instance()
timeout_record = _candidate_record(timeout_obj, prepared)
timeout_obj._advance_record(
    timeout_client, timeout_record,
    offline_index=timeout_obj._offline_index(timeout_client.offline),
)
check("超时删除已下载文件",
      timeout_client.deleted == ["/dl/c0"],
      str(timeout_client.deleted))
check("超时后换下一条候选",
      timeout_record.get("candidate_index") == 1
      and timeout_client.added[-1][0] == prepared[1]["magnet"],
      str(timeout_record))
check("超时写入尝试记录",
      (timeout_record.get("candidate_attempts") or [{}])[0].get("result") == "timeout",
      str(timeout_record.get("candidate_attempts")))

finish_client = _FakeClient(
    {}, set(), offline=[_FakeOfflineTask("c0", "a" * 40, status=2, percent=100)]
)
finish_obj = _retry_instance()
finish_record = _candidate_record(finish_obj, prepared[:1])
organize_calls = []
finish_obj._organize = lambda client, record, wait=0: organize_calls.append(True)
finish_obj._advance_record(
    finish_client, finish_record,
    offline_index=finish_obj._offline_index(finish_client.offline),
)
check("完成进入整理", finish_record.get("state") == jcd.STATE_FINISHED and organize_calls,
      str(finish_record))
check("完成只发一次通知", len(finish_obj.messages) == 1
      and "完成" in finish_obj.messages[0].get("title", ""), str(finish_obj.messages))

exhaust_client = _FakeClient(
    {}, {"/dl/c2"}, offline=[_FakeOfflineTask("c2", "c" * 40, status=1, percent=30)]
)
exhaust_obj = _retry_instance()
exhaust_record = _candidate_record(exhaust_obj, prepared, index=2)
exhaust_obj._advance_record(
    exhaust_client, exhaust_record,
    offline_index=exhaust_obj._offline_index(exhaust_client.offline),
)
check("3 条候选耗尽后失败", exhaust_record.get("state") == jcd.STATE_FAILED,
      str(exhaust_record))
check("耗尽后不再提交", exhaust_client.added == [], str(exhaust_client.added))
check("耗尽后发失败通知", len(exhaust_obj.messages) == 1
      and "失败" in exhaust_obj.messages[0].get("title", ""), str(exhaust_obj.messages))

remove_client = _FakeClient(
    {}, {"/dl/c0"}, offline=[_FakeOfflineTask("c0", "a" * 40, status=1, percent=10)]
)
remove_client.delete_result = False
remove_obj = _retry_instance()
remove_record = _candidate_record(remove_obj, prepared)
remove_obj._advance_record(
    remove_client, remove_record,
    offline_index=remove_obj._offline_index(remove_client.offline),
)
check("删除文件失败时不换种",
      remove_record.get("candidate_index") == 0 and remove_client.added == [],
      str(remove_record))
remove_record["remove_attempts"] = jcd.MAX_REMOVE_ATTEMPTS - 1
remove_obj._advance_record(
    remove_client, remove_record,
    offline_index=remove_obj._offline_index(remove_client.offline),
)
check("删除文件连续失败后终止", remove_record.get("state") == jcd.STATE_FAILED,
      str(remove_record))

queue_client = _FakeClient(
    {}, {"/dl/c0"}, offline=[_FakeOfflineTask("c0", "a" * 40, status=0, percent=0)]
)
queue_obj = _retry_instance()
queue_record = _candidate_record(queue_obj, prepared)
queue_record["state"] = jcd.STATE_SUBMITTED
queue_record["candidate_started_at"] = ""
queue_obj._advance_record(
    queue_client, queue_record,
    offline_index=queue_obj._offline_index(queue_client.offline),
)
check("排队超时也换种",
      queue_record.get("candidate_index") == 1
      and queue_client.deleted == ["/dl/c0"],
      str(queue_record))

_original_probe = jcd.probe_media
jcd.probe_media = lambda client, path: {
    "height": 480, "width": 856, "bitrate": 1_100_000, "size": GB,
}
low_client = _FakeClient(
    {"/dl/ABC-123": [_File("ABC-123.mp4", GB)]}, {"/dl/ABC-123"}
)
low_obj = _retry_instance()
low_record = _candidate_record(low_obj, prepared)
low_record["state"] = jcd.STATE_FINISHED
low_record["existing"] = {"quality": {"height": 1080, "size": 2 * GB}}
low_obj._organize(low_client, low_record)
check("低清：删除已下载文件", "/dl/ABC-123" in low_client.deleted,
      str(low_client.deleted))
check("低清：换下一条候选",
      low_record.get("candidate_index") == 1
      and low_client.added[-1][0] == prepared[1]["magnet"],
      str(low_record))

low_only_client = _FakeClient(
    {"/dl/ABC-123": [_File("ABC-123.mp4", GB)]}, {"/dl/ABC-123"}
)
low_only_obj = _retry_instance()
low_only_record = _candidate_record(low_only_obj, prepared[:1])
low_only_record["state"] = jcd.STATE_FINISHED
low_only_record["existing"] = {"quality": {"height": 1080, "size": 2 * GB}}
low_only_obj._organize(low_only_client, low_only_record)
check("低清：所有候选耗尽后失败",
      low_only_record.get("state") == jcd.STATE_FAILED
      and "低于 720P" in (low_only_record.get("message") or ""),
      str(low_only_record))

probe_calls = []
jcd.probe_media = lambda client, path: (
    probe_calls.append(path) or {"height": 480, "width": 856, "size": GB}
)
normal_client = _FakeClient(
    {"/dl/ABC-123": [_File("ABC-123.mp4", GB)]}, {"/dl/ABC-123"}
)
normal_obj = _retry_instance()
normal_record = _candidate_record(normal_obj, prepared)
normal_record["state"] = jcd.STATE_FINISHED
normal_obj._organize(normal_client, normal_record)
check("普通下载不跑 ffprobe",
      probe_calls == [] and normal_record.get("state") == jcd.STATE_ORGANIZED,
      f"{probe_calls} {normal_record.get('state')}")

jcd.probe_media = lambda client, path: {
    "height": 1080, "width": 1920, "bitrate": 5_000_000, "size": GB,
}
cn_client = _FakeClient(
    {"/dl/ABC-123": [_File("ABC-123.mp4", GB)]}, {"/dl/ABC-123"}
)
cn_obj = _retry_instance()
cn_record = _candidate_record(cn_obj, [_candidate("ABC-123-C", "a")])
cn_record["state"] = jcd.STATE_FINISHED
cn_obj._organize(cn_client, cn_record)
check("整理：中文字幕文件名带 -C",
      ("/dl/ABC-123/ABC-123.mp4", "ABC-123-C.mp4") in cn_client.renamed,
      str(cn_client.renamed))
check("整理：文件名不再带分辨率",
      (cn_record.get("final_files") or [""])[0].endswith("/ABC-123-C.mp4"),
      str(cn_record.get("final_files")))
jcd.probe_media = _original_probe

service_obj = _retry_instance()
service_obj._enabled = True
service = service_obj.get_service()[0]
check("定时服务使用秒级间隔", service.get("kwargs") == {"seconds": 10}, str(service))

# ---------- 主动洗版 ----------

pro_obj = _retry_instance()
pro_obj._enabled = True
pro_obj._proactive_enabled = True
pro_obj._enable_upgrade = True
pro_obj._emby_library = "成人影片"
pro_obj._proactive = {}
pro_obj._scan_low_res = lambda: [{
    "code": "SNIS-854", "title": "SNIS-854", "path": "/adult/SNIS-854/SNIS-854.mp4",
    "item_id": "1", "height": 480, "width": 856, "bitrate": 1_100_000, "size": GB,
}]
first_scan = pro_obj.scan_proactive()
check("主动洗版：发现低清并发提醒",
      first_scan.get("notified") == 1
      and pro_obj.messages[-1].get("buttons")
      and pro_obj.messages[-1]["buttons"][0][0].get("callback_data")
      == "[PLUGIN]JavCloudDownload|proactive:upgrade:SNIS-854",
      str(first_scan))
check("主动洗版：记录为已提醒",
      pro_obj._proactive["SNIS-854"].get("status") == "notified",
      str(pro_obj._proactive))
pro_obj.scan_proactive()
check("主动洗版：已提醒不重复发",
      sum("低清扫描" in str(m.get("title")) for m in pro_obj.messages) == 1,
      str(pro_obj.messages))
pro_obj.handle_proactive_action("dismiss", "SNIS-854")
check("主动洗版：忽略后落记录",
      pro_obj._proactive["SNIS-854"].get("status") == "declined",
      str(pro_obj._proactive))
pro_obj.scan_proactive()
check("主动洗版：忽略后不再提醒",
      sum("低清扫描" in str(m.get("title")) for m in pro_obj.messages) == 1,
      str(pro_obj.messages))

up_obj = _retry_instance()
up_obj._enabled = True
up_obj._proactive_enabled = True
up_obj._enable_upgrade = True
up_obj._emby_library = "成人影片"
up_obj._proactive = {}
up_obj._scan_low_res = lambda: [{
    "code": "KAWD-722", "title": "KAWD-722", "path": "/adult/KAWD-722/KAWD-722.mp4",
    "item_id": "2", "height": 404, "width": 700, "bitrate": 1_200_000, "size": GB,
}]
up_obj.scan_proactive()
up_calls = []
up_obj.submit = lambda codes, **kwargs: (
    up_calls.append((codes, kwargs)) or {"ok": True, "items": [{"ok": True, "state": "submitted"}]}
)
up_result = up_obj.handle_proactive_action("upgrade", "KAWD-722")
check("主动洗版：选择洗版后提交",
      up_result.get("status") == "upgrading"
      and up_calls == [(["KAWD-722"], {"wait_seconds": 0, "upgrade": True, "force": True})],
      f"{up_result} {up_calls}")

svc2_obj = _retry_instance()
svc2_obj._enabled = True
svc2_obj._proactive_enabled = True
svc2_obj._enable_upgrade = True
svc2_obj._emby_library = "成人影片"
check("主动洗版：不注册长期扫描服务",
      len(svc2_obj.get_service()) == 1,
      str(svc2_obj.get_service()))

many_obj = _retry_instance()
many_obj._enabled = True
many_obj._proactive_enabled = True
many_obj._enable_upgrade = True
many_obj._emby_library = "成人影片"
many_obj._proactive = {}
many_obj._scan_low_res = lambda: [
    {
        "code": f"AAA-{index:03d}", "title": f"AAA-{index:03d}",
        "path": f"/adult/AAA-{index:03d}/AAA-{index:03d}.mp4",
        "item_id": str(index), "height": 720, "width": 1280,
        "bitrate": 4_000_000, "size": GB,
    }
    for index in range(1, 8)
]
many_obj.scan_proactive()
summary = many_obj.messages[-1]
summary_text = str(summary.get("text") or "")
check("主动洗版：一次汇总通知",
      len(many_obj.messages) == 1
      and "共 7 条" in str(summary.get("title") or ""),
      str(many_obj.messages))
check("主动洗版：只列前5条",
      "5. AAA-005" in summary_text
      and "6. AAA-006" not in summary_text
      and "其余 2 条" in summary_text,
      summary_text)
check("主动洗版：前5条可操作，其余待处理",
      len(summary.get("buttons") or []) == 5
      and many_obj._proactive["AAA-001"].get("status") == "notified"
      and many_obj._proactive["AAA-006"].get("status") == "pending",
      str(many_obj._proactive))

class _FakeEmbyInstance:
    def __init__(self, items):
        self.items = items

    def get_librarys(self):
        return []

    def get_data(self, url):
        return type("_Resp", (), {"json": lambda _self: {"Items": self.items}})()


def _emby_item(code, height):
    return {
        "Id": code,
        "Name": code,
        "Path": f"/adult/{code}/{code}.mp4",
        "MediaSources": [{
            "Height": height, "Width": 1920, "Size": GB,
            "MediaStreams": [{"Type": "Video", "Height": height, "Width": 1920}],
        }],
    }


_original_services = media_library.EmbyLibrary._services
media_library.EmbyLibrary._services = staticmethod(
    lambda: {"emby": type("_Svc", (), {"instance": _FakeEmbyInstance([
        _emby_item("SNIS-854", 480),
        _emby_item("IPZ-127", 404),
        _emby_item("IPZ-127", 404),
        _emby_item("MIX-001", 720),
        _emby_item("MIX-001", 1080),
        _emby_item("SSNI-424", 1080),
    ])})()}
)
low_rows = media_library.EmbyLibrary("", "").list_low_res(1080)
check("Emby 低清扫描：低于1080才返回",
      {row["code"] for row in low_rows} == {"SNIS-854", "IPZ-127"},
      str(low_rows))
check("Emby 低清扫描：同番号取最高版本",
      next(row for row in low_rows if row["code"] == "IPZ-127")["height"] == 404,
      str(low_rows))
media_library.EmbyLibrary._services = _original_services


# ---------- 选种规则：4K 门槛、中字优先、720P ----------

def _magnet(name, gb):
    return {"name": name, "size_text": f"{gb}GB", "size_bytes": int(gb * GB),
            "date": "", "magnet": "magnet:?xt=urn:btih:" + "d" * 40, "info_hash": "d" * 40}

check("2160P 门槛：4GB 不算 2160P", selector.is_uhd("X 2160P", 4 * GB) is False)
check("2160P 门槛：5GB 算 2160P", selector.is_uhd("X 2160P", 5 * GB) is True)
check("4K 门槛：裸 4K 达 5GB 算 4K", selector.is_uhd("X 4K", 6 * GB) is True)
check("4K 门槛：裸 4K 不足 5GB 是假4K", selector.is_fake_uhd("X 4K", 4 * GB) is True)
check("4K 门槛：4K 与 2160P 同档",
      selector.rank(_magnet("X 4K", 6)) == selector.rank(_magnet("X 2160P", 6)))
check("6K 不作 4K 判断", selector.is_uhd("X 6K", 6 * GB) is False)

picked = selector.select([_magnet("X 2160P 4GB", 4), _magnet("X-C 1080p", 6)])
check("假4K被排除", (picked.get("chosen") or {}).get("name") == "X-C 1080p", str(picked))
check("假4K记录原因", any(x.get("reason", "").startswith("假4K") for x in picked.get("excluded", [])), str(picked.get("excluded")))

picked = selector.select([_magnet("X 2160P 6GB", 6), _magnet("X-C 5GB", 5)])
check("中字优先于2160P", (picked.get("chosen") or {}).get("name") == "X-C 5GB", str(picked))

picked = selector.select([_magnet("X 2160P 6GB", 6), _magnet("X 10GB", 10)])
check("2160P优先于普通大体积", (picked.get("chosen") or {}).get("name") == "X 2160P 6GB", str(picked))

picked = selector.select([_magnet("X 720P 10GB", 10), _magnet("X 5GB", 5)])
check("720P被排除", (picked.get("chosen") or {}).get("name") == "X 5GB", str(picked))
check("720P记录原因", any(x.get("reason") == "720P" for x in picked.get("excluded", [])), str(picked.get("excluded")))

picked = selector.select([_magnet("X 480P 10GB", 10), _magnet("X 5GB", 5)])
check("480P被排除", (picked.get("chosen") or {}).get("name") == "X 5GB", str(picked))
check("480P记录原因", any(x.get("reason") == "480P/360P" for x in picked.get("excluded", [])), str(picked.get("excluded")))

picked = selector.select([_magnet("X-C 1GB", 1), _magnet("X 6GB", 6)])
check("中字体积不足一半不优先", (picked.get("chosen") or {}).get("name") == "X 6GB", str(picked))
check("中字体积不足一半不改档位", picked.get("tier") == "体积最大", str(picked))

picked = selector.select([_magnet("第一會所新片@SIS001@X 2GB", 2), _magnet("X 6GB", 6)])
check("来源标签不再优先", (picked.get("chosen") or {}).get("name") == "X 6GB", str(picked))
check("来源标签不再改档位", picked.get("tier") == "体积最大", str(picked))

# ---------- 小视频阈值与规则开关 ----------

picked = selector.select([_magnet("X 200MB", 0.2), _magnet("X 5GB", 5)])
check("小于300MB的候选被排除", (picked.get("chosen") or {}).get("name") == "X 5GB", str(picked))
check("小视频记录原因", any("小于300MB" in x.get("reason", "") for x in picked.get("excluded", [])), str(picked.get("excluded")))

picked = selector.select([_magnet("X-UC 10GB", 10), _magnet("X-C 5GB", 5)], enable_rules=False)
check("规则关闭：直接按体积最大", (picked.get("chosen") or {}).get("name") == "X-UC 10GB", str(picked))
check("规则关闭：档位标记", picked.get("tier") == "体积最大（规则关闭）", str(picked))

tree = {"/dl/small": [_File("main.mp4", 10 * GB), _File("tiny.mp4", 100 * 1024 ** 2)]}
client = _FakeClient(tree, {"/dl/small"})
result = pipeline.tidy_dir(client, "/dl/small", "ABC-123", delete_junk_only=True)
check("整理：小于300MB的视频删除", "/dl/small/tiny.mp4" in client.deleted, str(client.deleted))

tree = {"/dl/all-small": [_File("tiny.mp4", 100 * 1024 ** 2)]}
client = _FakeClient(tree, {"/dl/all-small"})
result = pipeline.tidy_dir(client, "/dl/all-small", "ABC-123", delete_junk_only=True)
check("整理：全部小于阈值时不整理", result.get("ok") is False and "小于" in result.get("reason", ""), str(result))

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

_orig_remote = jcd.JavRankingSource.fetch_remote_detail
jcd.JavRankingSource.fetch_remote_detail = lambda self, code: self.fetch_detail(code)

inst3 = jcd.JavCloudDownload()
inst3._use_javbus = False
inst3._use_javranking = True
inst3._javranking_dir = str(CACHE)
inst3._allow_uncensored = False
inst3._allow_junk = False
preview = inst3.preview("START-257")
check("数据源：javranking 选种", preview.get("source") == "javranking" and preview.get("tier") == "中文字幕", str(preview))
check("数据源：排除 UC 后选中 -C", (preview.get("chosen") or {}).get("name") == "START-257-C.torrent", str(preview.get("chosen")))

try:
    inst3.preview("ABF-394")
    check("数据源：javranking 无磁力报错", False)
except (jcd.JavRankingError, jcd.JavBusError):
    check("数据源：javranking 无磁力报错", True)

inst4 = jcd.JavCloudDownload()
inst4._use_javbus = False
inst4._use_javranking = True
inst4._javranking_dir = str(CACHE)
inst4._allow_uncensored = False
inst4._allow_junk = False
check("数据源：auto 命中缓存", inst4._fetch_detail("START-257").get("source") == "javranking")

_orig_javbus_detail = jcd.source.JavBusSource.fetch_detail
def fake_both_detail(self, code):
    return {
        "code": code, "title": "合并标题", "release_date": "", "duration": "",
        "detail_url": "", "magnets": [
            {"name": "START-257-C.torrent", "size_text": "5.82GB",
             "size_bytes": 6249177415, "date": "",
             "magnet": "magnet:?xt=urn:btih:" + "b" * 40, "info_hash": "b" * 40},
            {"name": "START-257-1080P", "size_text": "6GB",
             "size_bytes": 6442450944, "date": "",
             "magnet": "magnet:?xt=urn:btih:" + "c" * 40, "info_hash": "c" * 40},
        ],
    }
jcd.source.JavBusSource.fetch_detail = fake_both_detail
inst5 = jcd.JavCloudDownload()
inst5._use_javbus = True
inst5._use_javranking = True
inst5._javranking_dir = str(CACHE)
inst5._allow_uncensored = False
inst5._allow_junk = False
merged = inst5._fetch_detail("START-257")
check("数据源：两个来源合并并按 infoHash 去重",
      merged.get("source") == "javranking+javbus"
      and len(merged.get("magnets") or []) == 3,
      str(merged.get("source")) + " " + str(len(merged.get("magnets") or [])))
jcd.source.JavBusSource.fetch_detail = _orig_javbus_detail

_parsed = jcd.javranking.parse_magnet_html(
    '<li class="magnet-item"><a href="magnet:?xt=urn:btih:'
    + "d" * 40 + '&amp;dn=X" title="X-C 4.59GB">X-C 4.59GB</a>'
    '<span class="magnet-tag magnet-tag--size">4.59GB</span></li>'
)
check("JavRanking：解析磁力块",
      len(_parsed) == 1 and _parsed[0]["info_hash"] == "d" * 40
      and _parsed[0]["size_bytes"] > 0,
      str(_parsed))

original = jcd.source.JavBusSource.fetch_detail
def fake_fetch(self, code):
    return {
        "code": code, "title": "JavBus 回退", "release_date": "", "duration": "",
        "detail_url": "", "magnets": [{"name": "X-C", "size_text": "1GB", "size_bytes": 1,
                                       "date": "", "magnet": "magnet:?xt=urn:btih:" + "c" * 40,
                                       "info_hash": "c" * 40}],
    }
jcd.source.JavBusSource.fetch_detail = fake_fetch
inst4._use_javbus = True
try:
    fallback = inst4._fetch_detail("ABF-394")
    check("数据源：auto 无磁力回退 JavBus", fallback.get("source") == "javbus", str(fallback))
finally:
    jcd.source.JavBusSource.fetch_detail = original

print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
