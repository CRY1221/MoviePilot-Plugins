"""
CloudDrive2 gRPC 客户端。

自行接入 gRPC，不依赖 CloudDriveDisk 插件实例，仅复用其安装的 proto 定义。
支持 API 令牌认证（推荐）与账号密码认证两种方式。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Iterable, List, Optional

import grpc

from app.sdk.logging import logger

try:
    from clouddrive2_client.proto import clouddrive_pb2 as pb
    from clouddrive2_client.proto import clouddrive_pb2_grpc as pb_grpc
except ImportError:  # pragma: no cover - 依赖缺失时由调用方给出可读错误
    pb = None
    pb_grpc = None

DEFAULT_CHANNEL_OPTIONS = (
    ("grpc.keepalive_time_ms", 30000),
    ("grpc.keepalive_timeout_ms", 10000),
    ("grpc.keepalive_permit_without_calls", True),
    ("grpc.http2.max_pings_without_data", 0),
)

# 115 重复提交离线任务的业务错误码
DUPLICATE_OFFLINE_CODE = "10008"

# 账号密码换来的 JWT 缓存，避免 10 秒轮询每次都重新登录
_TOKEN_CACHE: Dict[tuple, tuple[str, float]] = {}
_TOKEN_LOCK = threading.Lock()


class Cd2Error(RuntimeError):
    """CloudDrive2 调用失败。"""


def _require_proto() -> None:
    """校验 proto 依赖是否可用。"""
    if pb is None or pb_grpc is None:
        raise Cd2Error(
            "缺少 clouddrive2-client 依赖，无法连接 CloudDrive2；"
            "请重新安装插件以安装依赖"
        )


def status_name(value: int) -> str:
    """
    把离线任务状态枚举转换为名称。

    :param value: 枚举数值
    :return: 状态名称；未知值返回 UNKNOWN
    """
    _require_proto()
    try:
        return pb.OfflineFileStatus.Name(int(value))
    except Exception:
        return "OFFLINE_UNKNOWN"


class Cd2Client:
    """CloudDrive2 gRPC 客户端，线程内复用单一 channel。"""

    def __init__(
        self,
        host: str,
        port: int = 19798,
        token: str = "",
        username: str = "",
        password: str = "",
        timeout: int = 30,
    ) -> None:
        """
        初始化客户端。

        :param host: 服务地址
        :param port: gRPC 端口
        :param token: API 令牌；为空时改用账号密码认证
        :param username: 账号
        :param password: 密码
        :param timeout: 单次 RPC 超时秒数
        """
        _require_proto()
        self._address = f"{host}:{port}"
        self._token = (token or "").strip()
        self._username = username or ""
        self._password = password or ""
        self._timeout = timeout or 30
        self._lock = threading.RLock()
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[Any] = None

    def __enter__(self) -> "Cd2Client":
        """进入上下文时建立连接。"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """退出上下文时释放连接。"""
        self.close()

    @property
    def address(self) -> str:
        """当前连接地址。"""
        return self._address

    def connect(self) -> None:
        """建立 gRPC channel 并完成认证。"""
        with self._lock:
            if self._stub is not None:
                return
            self._channel = grpc.insecure_channel(
                self._address, options=DEFAULT_CHANNEL_OPTIONS
            )
            self._stub = pb_grpc.CloudDriveFileSrvStub(self._channel)
            if self._username and self._password:
                # 账号密码登录拿到的是完整用户 JWT，权限比只读 API Token 更全
                cached = self._cached_token()
                if cached:
                    self._token = cached
                else:
                    self._login()
            elif not self._token:
                raise Cd2Error("未配置 API 令牌，也未配置账号密码")

    def _cached_token(self) -> str:
        """读取仍有效的账号密码 JWT。"""
        key = (self._address, self._username)
        with _TOKEN_LOCK:
            item = _TOKEN_CACHE.get(key)
        if item and item[1] > time.time() + 60:
            return item[0]
        return ""

    def close(self) -> None:
        """关闭 gRPC channel。"""
        with self._lock:
            self._stub = None
            if self._channel is not None:
                try:
                    self._channel.close()
                except Exception:
                    pass
            self._channel = None

    def _login(self) -> None:
        """使用账号密码换取 JWT 令牌。"""
        if not self._username or not self._password:
            raise Cd2Error("账号密码登录需要同时填写账号和密码")
        response = self._stub.GetToken(
            pb.GetTokenRequest(userName=self._username, password=self._password),
            timeout=self._timeout,
        )
        if not response.success:
            raise Cd2Error(f"CloudDrive2 认证失败：{response.errorMessage or '账号或密码错误'}")
        self._token = response.token
        expires_at = time.time() + 1800
        expiration = getattr(response, "expiration", None)
        if expiration is not None and getattr(expiration, "seconds", 0):
            expires_at = float(expiration.seconds)
        with _TOKEN_LOCK:
            _TOKEN_CACHE[(self._address, self._username)] = (self._token, expires_at)

    def _metadata(self) -> List[tuple]:
        """构造带 Bearer 令牌的调用元数据。"""
        if not self._token:
            return []
        return [("authorization", f"Bearer {self._token}")]

    def _call(self, name: str, method: str, *args, **kwargs):
        """
        执行一次 RPC 并统一转换异常。

        :param name: 便于日志定位的操作名称
        :param method: stub 上的方法名；延迟解析，避免连接前取到 None
        """
        self.connect()
        fn = getattr(self._stub, method)
        try:
            return fn(*args, metadata=self._metadata(), timeout=self._timeout, **kwargs)
        except grpc.RpcError as error:
            code = error.code().name if hasattr(error, "code") else "UNKNOWN"
            detail = str(error.details() if hasattr(error, "details") else error)
            raise Cd2Error(f"{name} 失败：{code} {detail[:200]}") from error

    # ---------- 系统 ----------

    def system_info(self) -> Any:
        """读取服务端系统信息（无需认证）。"""
        _require_proto()
        from google.protobuf import empty_pb2

        self.connect()
        return self._call("GetSystemInfo", "GetSystemInfo", empty_pb2.Empty())

    # ---------- 文件 ----------

    def list_dir(self, path: str, force_refresh: bool = False) -> List[Any]:
        """
        列出目录内容。

        :param path: 目录路径
        :param force_refresh: 是否绕过目录缓存
        :return: CloudDriveFile 列表
        """
        files: List[Any] = []
        stream = self._call(
            "GetSubFiles",
            "GetSubFiles",
            pb.ListSubFileRequest(path=path, forceRefresh=force_refresh),
        )
        for response in stream:
            files.extend(response.subFiles)
        return files

    def find(self, path: str) -> Optional[Any]:
        """
        按路径查询条目。

        :param path: 完整路径
        :return: CloudDriveFile；不存在时返回 None
        """
        try:
            return self._call(
                "FindFileByPath",
                "FindFileByPath",
                pb.FindFileByPathRequest(path=path),
            )
        except Cd2Error as error:
            if "NOT_FOUND" in str(error):
                return None
            raise

    def create_folder(self, parent: str, name: str) -> bool:
        """
        在指定父目录下建目录。

        :param parent: 父目录路径
        :param name: 新目录名
        :return: 是否成功
        """
        result = self._call(
            "CreateFolder",
            "CreateFolder",
            pb.CreateFolderRequest(parentPath=parent, folderName=name),
        )
        inner = getattr(result, "result", None)
        return bool(getattr(inner, "success", False))

    def rename(self, path: str, new_name: str) -> bool:
        """
        重命名文件或目录。

        :param path: 原路径
        :param new_name: 新名称（不含父目录）
        :return: 是否成功
        """
        result = self._call(
            "RenameFile",
            "RenameFile",
            pb.RenameFileRequest(theFilePath=path, newName=new_name),
        )
        return bool(getattr(result, "success", False))

    def move(self, paths: Iterable[str], dest_parent: str, overwrite: bool = True) -> bool:
        """
        移动文件或目录到目标父目录。

        :param paths: 源路径列表
        :param dest_parent: 目标父目录，注意不是完整目标路径
        :param overwrite: 同名时是否覆盖
        :return: 是否成功
        """
        policy = (
            pb.MoveFileRequest.Overwrite if overwrite else pb.MoveFileRequest.Rename
        )
        result = self._call(
            "MoveFile",
            "MoveFile",
            pb.MoveFileRequest(
                theFilePaths=list(paths), destPath=dest_parent, conflictPolicy=policy
            ),
        )
        return bool(getattr(result, "success", False))

    def delete(self, path: str) -> bool:
        """
        删除文件或目录（回收站语义）。

        :param path: 目标路径
        :return: 是否成功
        """
        result = self._call(
            "DeleteFile", "DeleteFile", pb.FileRequest(path=path)
        )
        return bool(getattr(result, "success", False))

    # ---------- 离线下载 ----------

    def quota(self, folder: str) -> Dict[str, int]:
        """
        查询离线下载配额。

        :param folder: 云盘内路径
        :return: total/used/left
        """
        info = self._call(
            "GetOfflineQuotaInfo",
            "GetOfflineQuotaInfo",
            pb.OfflineQuotaRequest(path=folder),
        )
        return {"total": info.total, "used": info.used, "left": info.left}

    def list_offline(self, folder: str, force_refresh: bool = False) -> List[Any]:
        """
        列出该云盘的离线任务。

        :param folder: 云盘内路径
        :param force_refresh: 是否绕过 CD2 目录缓存
        :return: OfflineFile 列表
        """
        result = self._call(
            "ListOfflineFilesByPath",
            "ListOfflineFilesByPath",
            pb.FileRequest(path=folder, forceRefresh=force_refresh),
        )
        return list(result.offlineFiles)

    def find_offline(self, folder: str, info_hash: str = "", name: str = "",
                     force_refresh: bool = False) -> Optional[Any]:
        """
        按 infoHash 或名称定位离线任务。

        :param folder: 云盘内路径
        :param info_hash: 磁力 infoHash
        :param name: 任务名称
        :param force_refresh: 是否绕过 CD2 目录缓存
        :return: 匹配的 OfflineFile；未找到返回 None
        """
        target = (info_hash or "").lower()
        for task in self.list_offline(folder, force_refresh=force_refresh):
            if target and (task.infoHash or "").lower() == target:
                return task
            if name and task.name == name:
                return task
        return None

    def add_offline(self, url: str, folder: str) -> str:
        """
        提交离线下载任务。

        :param url: 磁力或种子链接
        :param folder: 目标目录
        :return: submitted 或 exists
        """
        try:
            result = self._call(
                "AddOfflineFiles",
                "AddOfflineFiles",
                pb.AddOfflineFileRequest(urls=url, toFolder=folder),
            )
        except Cd2Error as error:
            text = str(error)
            if DUPLICATE_OFFLINE_CODE in text or "任务已存在" in text:
                return "exists"
            raise
        if getattr(result, "success", False):
            return "submitted"
        detail = getattr(result, "errorMessage", "") or ""
        if "已存在" in detail or DUPLICATE_OFFLINE_CODE in detail:
            return "exists"
        raise Cd2Error(f"提交离线任务失败：{detail or '未知错误'}")

    def remove_offline(self, folder: str, info_hashes: Iterable[str],
                       delete_files: bool = False) -> bool:
        """
        删除离线任务记录。

        :param folder: 云盘内路径
        :param info_hashes: 任务 infoHash 列表
        :param delete_files: 是否同时删除已落盘文件
        :return: 是否成功
        """
        result = self._call(
            "RemoveOfflineFiles",
            "RemoveOfflineFiles",
            pb.RemoveOfflineFilesRequest(
                infoHashes=list(info_hashes), deleteFiles=delete_files, path=folder
            ),
        )
        return bool(getattr(result, "success", False))


def probe(host: str, port: int, token: str, folder: str) -> Dict[str, Any]:
    """
    连通性与权限连通性探测，供插件配置页与自检使用。

    :param host: 服务地址
    :param port: gRPC 端口
    :param token: API 令牌
    :param folder: 待探测的云下载目录
    :return: 探测结果字典
    """
    client = Cd2Client(host=host, port=port, token=token)
    try:
        info = client.system_info()
        result: Dict[str, Any] = {
            "address": client.address,
            "user": getattr(info, "userName", ""),
            "version": getattr(info, "serverVersion", ""),
        }
        try:
            result["quota"] = client.quota(folder)
        except Cd2Error as error:
            result["quota_error"] = str(error)
        try:
            result["offline_tasks"] = len(client.list_offline(folder))
        except Cd2Error as error:
            result["offline_error"] = str(error)
        return result
    finally:
        client.close()
