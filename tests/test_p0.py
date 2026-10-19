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

    def list_dir(self, path):
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
check("多段：两个分段都改名", ("/dl/multi/OLD-CD1.mp4", "ABC-123 - cd1.mp4") in client.renamed and ("/dl/multi/OLD-CD2.mp4", "ABC-123 - cd2.mp4") in client.renamed, str(client.renamed))
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
check("多段：part1/part2 识别", ("/dl/part/OLD-part1.mkv", "XYZ-999 - cd1.mkv") in client.renamed and ("/dl/part/OLD-part2.mkv", "XYZ-999 - cd2.mkv") in client.renamed, str(client.renamed))

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

service_obj = _retry_instance()
service_obj._enabled = True
service = service_obj.get_service()[0]
check("定时服务使用秒级间隔", service.get("kwargs") == {"seconds": 10}, str(service))


# ---------- 选种规则：4K 门槛、中字优先、720P ----------

def _magnet(name, gb):
    return {"name": name, "size_text": f"{gb}GB", "size_bytes": int(gb * GB),
            "date": "", "magnet": "magnet:?xt=urn:btih:" + "d" * 40, "info_hash": "d" * 40}

check("4K 门槛：4GB 不算 4K", selector.is_uhd("X 4K", 4 * GB) is False)
check("4K 门槛：5GB 算 4K", selector.is_uhd("X 4K", 5 * GB) is True)

picked = selector.select([_magnet("X 4K 4GB", 4), _magnet("X-C 1080p", 6)])
check("假4K被排除", (picked.get("chosen") or {}).get("name") == "X-C 1080p", str(picked))
check("假4K记录原因", any(x.get("reason", "").startswith("假4K") for x in picked.get("excluded", [])), str(picked.get("excluded")))

picked = selector.select([_magnet("X 4K 6GB", 6), _magnet("X-C 5GB", 5)])
check("中字优先于4K", (picked.get("chosen") or {}).get("name") == "X-C 5GB", str(picked))

picked = selector.select([_magnet("X 4K 6GB", 6), _magnet("X 10GB", 10)])
check("4K优先于普通大体积", (picked.get("chosen") or {}).get("name") == "X 4K 6GB", str(picked))

picked = selector.select([_magnet("X 720P 10GB", 10), _magnet("X 5GB", 5)])
check("720P被排除", (picked.get("chosen") or {}).get("name") == "X 5GB", str(picked))
check("720P记录原因", any(x.get("reason") == "720P" for x in picked.get("excluded", [])), str(picked.get("excluded")))

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
