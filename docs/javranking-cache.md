# JavRanking 本地缓存

## 怎么抓的

JavRanking 有 Cloudflare 挑战，直接 HTTP（本机或 MP 容器）一律 403。实际可用的方式是
**在 MP 容器里用浏览器会话抓**，和 MP 助手用的 `browse_webpage` 同一套 `cloakbrowser`：

1. 启动无头浏览器，打开 `https://javranking.cc/zh-hans/`；
2. 等待页面标题从 `Just a moment...` 变为 JavRanking；
3. 在页面内执行 `fetch(url, {credentials:'include'})`，请求会带上 Cloudflare 会话；
4. 把返回内容写到 `/config/temp/`，再复制到本项目的 `data/javranking/raw/`。

2026-10-20 实测抓取结果：

| 文件 | 内容 |
|---|---|
| `llms-full.txt` | 3.62MB，15,702 个详情页链接 |
| `llms.txt` | Agent 指南 |
| `main_rank_3380.json` | 全站神作总榜 3380 条（排名/番号/标题/日期） |
| `three_lists.json` | 神作、JavLibrary TOP250、JAVDB TOP250 |
| `pages/*.md` | 各榜单的 Markdown 页面 |

直接 `curl` `llms.txt` / `llms-full.txt` / `*.md` 都是 403；只有浏览器会话内 `fetch` 能拿到。

## 整理后的缓存

`python3 tools/build_javranking_cache.py` 会生成：

- `videos.json`：15,702 部影片，按详情页 ID 去重
- `code_index.json`：15,165 个番号 → 详情页（实测无重复番号）
- `rankings.json`：`main` 3380、`main_top250` 250、`javlibrary_top250` 250、`javdb_top250` 250
- `javranking.db`：SQLite，`videos` / `rankings` 两张表

示例：

```text
START-257 → https://javranking.cc/zh-hans/videos/78/
ABF-394   → https://javranking.cc/zh-hans/videos/3127/
LAFBD-41  → https://javranking.cc/zh-hans/videos/1/
```

## 边界

- `data/` 已加入 `.gitignore`，缓存内容不进公开仓库。
- 只缓存榜单、目录索引和番号映射，**不批量抓详情页**；磁力仍需按需打开详情页。
- 站点 `robots.txt` 明确禁止 `ClaudeBot` / `anthropic-ai`，Cloudflare 也会拦截直接请求；
  这些缓存仅供本机个人使用，不做再分发，也不把抓取逻辑塞进插件主流程。
- 刷新方式：用 MP 浏览器重新抓 `llms-full.txt` 和榜单，覆盖 `data/javranking/raw/` 后重跑
  `build_javranking_cache.py`。

## 插件数据源选择

插件设置页新增「数据源」：

| 值 | 行为 |
|---|---|
| `javbus` | 直连 JavBus，默认 |
| `javranking` | 只读本地缓存；没有详情页磁力时直接报错 |
| `auto` | 先读 JavRanking 缓存，缓存没有磁力或不可用时回退 JavBus |

缓存目录默认是插件数据目录下的 `javranking/javranking.db`，可以在设置页覆盖。

新增 API：

- `GET /api/v1/plugin/JavCloudDownload/sources`：当前数据源与缓存统计
- `GET /api/v1/plugin/JavCloudDownload/ranking?list_name=main&limit=50`：榜单
- `GET /api/v1/plugin/JavCloudDownload/lookup?code=START-257`：番号与磁力缓存状态
- `preview` 返回新增 `source` 与 `has_magnets`

## 刷新工具

`tools/fetch_javranking_cache.py` 必须在 **MP 容器内**运行（依赖 `cloakbrowser`）：

```bash
docker exec -i moviepilot-v3 python3 - \
  --out /config/plugins/JavCloudDownload/javranking/raw \
  --codes START-257,ABF-394 < tools/fetch_javranking_cache.py
```

它会：

1. 打开 JavRanking，等待 Cloudflare 挑战结束；
2. 抓 `llms-full.txt`、`llms.txt`、榜单 Markdown 和 3380 条总榜；
3. 对 `--codes` 里的番号抓详情页，解析磁力并写入 `raw/details/<番号>.json`。

抓完后在本地或容器里运行 `tools/build_javranking_cache.py`，把 `raw/` 整理成
`javranking.db`。详情页按需抓，不要一次抓 1.5 万个。
