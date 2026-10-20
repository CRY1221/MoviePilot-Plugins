# JavRanking 缓存与按需抓取

## 插件怎么用

JavRanking 在插件里是**可选种子来源**，设置页勾选「JavRanking」后生效，默认关闭。
它依赖一份本地 SQLite 缓存（`javranking.db`），缓存提供：

- `videos`：番号、标题、详情页 URL
- `rankings`：榜单条目
- `details`：已抓过的详情页磁力

磁力不要求预先缓存。插件每次按详情页 URL 抓取，解析后写入 `details` 仅供查询；
**旧磁力不再作为下载兜底**，抓取失败时上层直接回退 JavBus。

默认缓存路径：插件数据目录下的 `javranking/javranking.db`，可在设置页覆盖。

## 与 JavBus 的关系

| 勾选 | 行为 |
|---|---|
| 只勾 JavBus | 只走 JavBus，默认行为 |
| 只勾 JavRanking | 只走 JavRanking；抓取失败时自动回退 JavBus |
| 两个都勾 | 两个来源**并行**抓取，按 `infoHash` 合并去重；任一失败仍可使用另一个 |

两个来源都勾选时，合并阶段 JavRanking 的磁力排在前面，JavBus 补充缺失的；最终候选仍按
`selector.py` 的选种规则排序。所有站点统一走 `throttle.py` 的 **3 秒最小间隔**，
避免被封和给站点施压。

## 直接 HTTP

2026-10-20 晚间复测：站点已启用 Cloudflare 托管挑战，详情页直接返回 **403**
（响应头 `cf-mitigated: challenge`），加请求头或 Cookie 都无法绕过。插件当前的处理：

- 被挑战时用 **cloakbrowser** 打开一次首页过盾，取 `cf_clearance` 和浏览器 UA，
  存到 `javranking/cf_ticket.json`（插件按 **30 分钟**有效期处理）；
- 后续详情页用普通 HTTP + 这张票请求，票失效或再次被拦时重新取票；
- 取票失败才进入冷却（首次 1 分钟、再次 5 分钟），并改用 **JavBus** 取新种。
- 抓取失败不再使用旧磁力缓存兜底。

插件只在取票时启动一次 cloakbrowser（约 9 秒），拿到 `cf_clearance` 后立即关闭，
不会常驻浏览器进程。票失效或再次被拦时才会再启动一次。

2026-10-20 实测：清票后完整取票与抓取共 **14.4 秒**（含两次 403 和 5 秒等待），
复用票 **0.3 秒**拿到 55,541 字符、24 个磁力块；真实队列中来源显示为
`javranking+javbus`，说明取票在插件进程内生效。
`robots.txt` 仍声明禁止 `ClaudeBot` / `anthropic-ai`，这些请求仅供本机个人使用，不做再分发。

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
- 磁力每次重新抓取，`details` 只用于查询展示，不参与下载兜底。
- 选种时主要靠中文字幕、分辨率和体积。
