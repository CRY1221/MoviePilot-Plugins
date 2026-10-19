# JavRanking 缓存与按需抓取

## 插件怎么用

JavRanking 在插件里是**可选种子来源**，设置页勾选「JavRanking」后生效，默认关闭。
它依赖一份本地 SQLite 缓存（`javranking.db`），缓存提供：

- `videos`：番号、标题、详情页 URL
- `rankings`：榜单条目
- `details`：已抓过的详情页磁力

磁力不要求预先缓存。插件在需要时按详情页 URL 抓取，解析后写入 `details`，缓存 **7 天**；
抓取失败时回退旧缓存，没有旧缓存才报错。

默认缓存路径：插件数据目录下的 `javranking/javranking.db`，可在设置页覆盖。

## 与 JavBus 的关系

| 勾选 | 行为 |
|---|---|
| 只勾 JavBus | 只走 JavBus，默认行为 |
| 只勾 JavRanking | 只走 JavRanking；缺少缓存或详情页没有磁力时直接报错 |
| 两个都勾 | 两个来源**并行**抓取，按 `infoHash` 合并去重；任一失败仍可使用另一个 |

两个来源都勾选时，合并阶段 JavRanking 的磁力排在前面，JavBus 补充缺失的；最终候选仍按
`selector.py` 的选种规则排序。所有站点统一走 `throttle.py` 的 **3 秒最小间隔**，
避免被封和给站点施压。

## 直接 HTTP

2026-10-20 实测：MP 容器内直接请求 JavRanking 详情页可以返回 200，Cloudflare 当前未固定拦截；
这不保证以后一直可用。插件用普通 `RequestUtils` 抓取，不依赖浏览器会话；`robots.txt` 仍声明禁止
`ClaudeBot` / `anthropic-ai`，这些请求仅供本机个人使用，不做再分发。

如果站点以后重新加挑战，按需抓取会失败；插件会回退已缓存的磁力，不会因此中断整个任务
（前提是缓存里有旧磁力）。

## 本地缓存内容

`data/javranking/javranking.db` 是开发机上的样例缓存：

| 表 | 内容 |
|---|---|
| `videos` | 15,702 部影片，按详情页 ID 去重 |
| `rankings` | 当前样例有 `main` 3380 条、`main_top250` / `javlibrary_top250` / `javdb_top250` 各 250 条 |
| `details` | 按需抓取后写入的磁力，带 `fetched_at` |

榜单数量不写死：`lists()` 从 `rankings` 表动态读取，页面显示缓存里实际存在的榜单。

## API

- `GET /api/v1/plugin/JavCloudDownload/sources`：当前来源、缓存统计
- `GET /api/v1/plugin/JavCloudDownload/ranking?list_name=main&limit=50`：榜单
- `GET /api/v1/plugin/JavCloudDownload/lookup?code=START-257`：番号与磁力缓存状态
- `preview` 返回 `source`、`has_magnets`、`candidates`

## 刷新缓存

`tools/fetch_javranking_cache.py` 用于重建整份缓存，必须在 MP 容器内运行
（抓全量目录仍依赖 `cloakbrowser`）：

```bash
docker exec -i moviepilot-v3 python3 - \
  --out /config/plugins/JavCloudDownload/javranking/raw \
  --codes START-257,ABF-394 < tools/fetch_javranking_cache.py
```

抓完后运行 `tools/build_javranking_cache.py`，把 `raw/` 整理成 `javranking.db`。
详情页按需抓，不要一次抓 1.5 万个。

## 边界

- `data/` 已加入 `.gitignore`，缓存内容不进公开仓库。
- 只缓存榜单、目录索引和已抓过的详情；不批量抓详情页。
- 磁力缓存 7 天，过期后下一次使用会重新抓详情页。
- 选种时主要靠中文字幕、分辨率和体积。
