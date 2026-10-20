"""番号云下载的 MoviePilot Agent 工具。

工具只做两件事：把番号送进插件的待提交队列、读取任务进度。
实际的取种、提交 115、下载与整理仍由插件的定时服务完成。
"""

from __future__ import annotations

import json
from typing import Any, List, Optional, Type, Union

from pydantic import BaseModel, Field

from app.agent.tools.base import MoviePilotTool
from app.agent.tools.tags import ToolTag
from app.application.plugin.runtime import get_plugin_manager

PLUGIN_ID = "JavCloudDownload"


def _plugin() -> Any:
    """返回正在运行的番号云下载插件实例。"""
    plugin = get_plugin_manager().running_plugins.get(PLUGIN_ID)
    if plugin is None or not plugin.get_state():
        raise RuntimeError("番号云下载插件未运行或未启用")
    return plugin


def _split_codes(value: Union[str, List[str], None]) -> List[str]:
    """把字符串或列表统一成番号列表。"""
    if value is None:
        return []
    if isinstance(value, str):
        raw = value.replace("，", ",").replace("\n", ",")
        return [part.strip() for part in raw.split(",") if part.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


class JavSubmitInput(BaseModel):
    """提交番号的输入参数。"""

    codes: Union[str, List[str]] = Field(
        ...,
        description=(
            "一个或多个番号。多个番号可用逗号、换行分隔，也可以直接传数组，"
            "例如 'ABC-123, DEF-456'。"
        ),
    )


class JavSubmitTool(MoviePilotTool):
    """把番号加入番号云下载队列。"""

    name: str = "javcloud_submit"
    description: str = (
        "把番号提交到番号云下载插件。插件会在后台自动选种、提交 115 离线下载、"
        "整理并入库，调用会立即返回，不需要等待下载完成。"
        "当用户要求下载某个或某批番号时使用。"
        "调用成功后把返回内容里的 message 原样告诉用户；"
        "不要自行编造排队数量或下载结果。"
    )
    tags: list[str] = [ToolTag.Download, ToolTag.Plugin]
    args_schema: Type[BaseModel] = JavSubmitInput

    async def run(  # type: ignore[override]
        self,
        codes: Union[str, List[str]],
    ) -> str:
        """入队并返回结果。"""
        plugin = _plugin()
        parsed = _split_codes(codes)
        if not parsed:
            return json.dumps(
                {"ok": False, "reason": "缺少番号"}, ensure_ascii=False
            )
        result = await self.run_blocking("plugin", plugin.enqueue, parsed)
        return json.dumps(result, ensure_ascii=False)


class JavTasksInput(BaseModel):
    """查询任务进度的输入参数。"""

    code: Optional[str] = Field(
        None,
        description="可选番号；填写后只返回该番号的任务记录。",
    )
    limit: int = Field(
        20,
        ge=1,
        le=100,
        description="最多返回多少条最近任务，默认 20。",
    )


class JavTasksTool(MoviePilotTool):
    """读取番号云下载任务进度。"""

    name: str = "javcloud_tasks"
    description: str = (
        "查询番号云下载插件的任务进度和待提交队列。"
        "当用户询问下载到哪了、是否完成、为什么失败时使用。"
        "返回结果只包含精简字段，不会暴露磁力链接。"
    )
    tags: list[str] = [ToolTag.Read, ToolTag.Download, ToolTag.Plugin]
    args_schema: Type[BaseModel] = JavTasksInput

    async def run(  # type: ignore[override]
        self,
        code: Optional[str] = None,
        limit: int = 20,
    ) -> str:
        """返回精简后的任务与队列状态。"""
        plugin = _plugin()
        payload = await self.run_blocking("plugin", plugin.api_tasks)
        items = payload.get("items") or []
        if code:
            keyword = str(code).strip().upper().replace("_", "-")
            items = [
                item for item in items
                if str(item.get("code") or "").upper() == keyword
            ]
        items = items[:max(1, int(limit or 20))]
        compact = [
            {
                key: item.get(key)
                for key in (
                    "code", "state", "message", "size_text",
                    "percent", "updated_at", "final_dir",
                )
                if item.get(key) not in (None, "")
            }
            for item in items
        ]
        queued = [
            {
                "code": entry.get("code"),
                "position": entry.get("position"),
                "batch_id": entry.get("batch_id"),
            }
            for entry in (payload.get("queued") or [])
        ]
        return json.dumps(
            {
                "ok": True,
                "total": payload.get("total", len(items)),
                "items": compact,
                "queued_count": payload.get("queued_count", len(queued)),
                "queued": queued[:100],
            },
            ensure_ascii=False,
        )
