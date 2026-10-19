"""Explicit tool input contract; all business search is forwarded to MP."""
from typing import Literal
from contextlib import aclosing

from pydantic import BaseModel, ConfigDict, Field, model_validator
from app.agent.tools.base import MoviePilotTool, ToolTag
from app.chain.search.facade import SearchChain
from app.schemas.types import MediaSource, MediaType


class SearchInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = ""
    media_source: MediaSource | None = None
    media_id: str | None = None
    mtype: MediaType | None = None
    area: Literal["title", "imdbid"] = "title"
    season: int | None = Field(default=None, ge=0)
    site_ids: list[int] | None = None
    page: int = Field(default=0, ge=0)
    rule_groups: list[str] | None = None
    include_candidates: bool = False
    top_k: int = Field(default=0, ge=0, description="仅裁剪最终展示，0 表示全部")
    en_title: str = Field(default="", description="历史参数，不再支持；关键词由 MP 管理")
    require_complete: bool = Field(default=False, description="不支持插件自定完整包规则，请使用 MP 规则")

    @model_validator(mode="after")
    def supported(self):
        if self.en_title or self.require_complete:
            raise ValueError("en_title/require_complete 不支持；请在 MP 中设置关键词和规则")
        if bool(self.media_source) != bool(self.media_id):
            raise ValueError("media_source 和 media_id 必须同时提供")
        if self.media_id:
            if self.title or self.page or self.rule_groups is not None:
                raise ValueError("ID 搜索由 MP 管理标题、分页和规则，不接受 title/page/rule_groups")
        elif not self.title.strip() or self.season is not None or self.area != "title" or self.include_candidates:
            raise ValueError("标题模式需要 title；season/area/include_candidates 仅适用于 ID 模式")
        return self


class FastIntersectSearchTool(MoviePilotTool):
    name: str = "fast_intersect_search"
    description: str = "调用 MP 原生搜索与系统规则；支持标题或媒体 ID。仅搜索，不提交下载。"
    tags: list[ToolTag] = [ToolTag.Read, ToolTag.Resource]
    args_schema: type[BaseModel] = SearchInput

    def get_mcp_input_schema(self):
        return SearchInput.model_json_schema()

    async def run(self, **kwargs):
        from . import FastIntersectSearch
        plugin = FastIntersectSearch.get_instance()
        if plugin is None or not plugin.get_state():
            return {"success": False, "message": "极速搜索未启用或接口不兼容"}
        request = SearchInput.model_validate(kwargs)
        chain = SearchChain()
        common = dict(mtype=request.mtype, sites=request.site_ids, cache_local=False)
        if request.media_id:
            events = chain.async_search_by_id_stream(
                media_source=request.media_source, media_id=request.media_id, area=request.area,
                season=request.season, include_candidates=request.include_candidates, **common)
        else:
            events = chain.async_search_by_title_stream(title=request.title, page=request.page,
                                                        rule_groups=request.rule_groups, **common)
        items, complete = [], False
        async with aclosing(events):
            async for event in events:
                if event.get("type") == "error":
                    return {"success": False, "status": "failed", "message": "MP 搜索返回错误，请查看 MP 日志"}
                if event.get("type") in {"replace", "done"} and "items" in event:
                    items = event["items"]
                if event.get("type") == "done":
                    complete = True
        if not complete:
            return {"success": False, "status": "incomplete", "message": "MP 未返回搜索完成事件"}
        selected = items[:request.top_k] if request.top_k else items
        # Search output is for inspection. Native Context serialization also
        # includes download credentials, which must stay inside MoviePilot.
        selected = [{**item, "torrent_info": {
            key: value for key, value in item["torrent_info"].items()
            if key not in {"site_cookie", "site_ua", "enclosure", "page_url"}
        }} if isinstance(item.get("torrent_info"), dict) else dict(item)
                    for item in selected]
        return {"success": True, "status": "results" if items else "empty", "total": len(items),
                "shown": len(selected), "items": selected,
                "site_completeness": "unknown", "message": "结果来自 MP；原生接口不报告所有站点是否成功"}
