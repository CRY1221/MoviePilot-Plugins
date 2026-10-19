# 主动洗版

一次性扫描 Emby 成人库，列出低于 1080P 的条目并发通知询问是否洗版；用户选择后落记录，避免重复提醒。
不做长期轮询，也不监听 Emby Webhook。

## 前提

- 插件已启用，且配置了 `emby_library`（成人影片库）。
- 设置页打开「启用洗版」（`enable_upgrade`）。
- 设置页打开「主动洗版提醒」（`proactive_upgrade`）。

只开主动提醒、不开洗版时，不会触发扫描。

## 数据来源

分辨率来自 Emby 的 `/emby/Items` 返回的 `MediaSources` / `MediaStreams`，不是 MoviePilot 的
`mediaserveritem` 同步表——那张表只存标题和路径，没有分辨率。

Emby 已经探测过的条目能直接读到高度；高度为 0 的条目跳过，等 Emby 后续探测后再手动重扫。

## 扫描规则

- 打开「主动洗版提醒」且洗版已启用时，插件初始化后执行**一次**扫库；之后不会自动重复。
- 读取 Emby `/emby/Items`，字段包含 `MediaSources`、`MediaStreams`。
- 高度 `<1080` 才算低清；高度为 0 或读取不到时跳过，不猜。
- 同一番号取库内最高版本：只要存在 1080P 及以上，就不提醒。
- 番号从文件路径的文件夹或文件名提取，不依赖 Emby 标题（标题可能被刮削错）。
- 每次扫描发**一条汇总通知**，写明共发现多少条低清、待处理多少条。
- 列表最多显示前 **5** 条，并给这 5 条附「洗版 / 忽略」按钮；其余记录保持 `pending`。
- 需要继续处理剩余条目时，再次调用 `POST /proactive/scan`。

## 通知与选择

通知标题：`番号云下载低清扫描：共 N 条`，正文先写总数，再列前 5 条（番号、分辨率、体积）。
前 5 条各带一组按钮：

- `洗版 <番号>`：提交 `upgrade=true, force=true` 的洗版任务，仍走下载后的 ffprobe 复核。
- `忽略 <番号>`：记录为 `declined`，以后不再提醒这个番号。

按钮回调格式：

```text
[PLUGIN]JavCloudDownload|proactive:upgrade:<番号>
[PLUGIN]JavCloudDownload|proactive:dismiss:<番号>
```

用户没点按钮时，记录停在 `notified`，也不会重复发同一条提醒。

## 记录状态

`proactive` 数据保存在插件数据目录，按番号索引：

| 状态 | 含义 |
|---|---|
| `pending` | 已发现，尚未发提醒或发送失败待重试 |
| `notified` | 已提醒，等待用户选择 |
| `declined` | 用户选择不洗版，不再提醒 |
| `upgrading` | 用户选择洗版，任务已提交 |
| `skipped` | 没有更好的候选，未提交 |
| `failed` | 提交或洗版失败 |
| `resolved` | 库内已不再低清（已洗版或 Emby 更新） |

## API

- `GET /api/v1/plugin/JavCloudDownload/proactive`：查看记录
- `POST /api/v1/plugin/JavCloudDownload/proactive/scan`：立即扫描
- `POST /api/v1/plugin/JavCloudDownload/proactive/action`：`{"code":"SNIS-854","action":"upgrade"}` 或 `{"code":"SNIS-854","action":"dismiss"}`

## 边界

- 只依赖 Emby 已刮削的分辨率，不主动 ffprobe；Emby 显示 0 的条目不处理。
- 洗版任务失败不会自动重试提醒；需要手动再提交。
- 主动提醒和普通下载是两条独立路径；普通下载不会因为低清被拦截。
