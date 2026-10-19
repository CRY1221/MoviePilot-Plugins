# MoviePilot 插件

本仓库当前维护 **番号云下载**。

## 番号云下载 · JavCloudDownload

按番号从 JavBus / JavRanking 取种，经 CloudDrive2 gRPC 提交 115 离线下载；落盘后清理广告文件、
按番号+分辨率重命名，可选洗版，并按配置移动到 Emby 库目录。

- 代码：`plugins.v3/javclouddownload/`
- 清单：`package.v3.json` 中的 `JavCloudDownload`
- 依赖：`clouddrive2-client==0.3.0`、`grpcio~=1.80.0`、`protobuf>=6.31.1,<7`
- 当前版本：`0.6.8`
- 运行基线：MoviePilot `v3.1.8`

配置项包括 CloudDrive2 地址与令牌、云下载目录、库目录、是否移动、是否允许无码破解资源、
任务推进间隔与最长等待时间。任务由插件注册的定时服务异步推进，也可通过插件 API 手动触发。


## 数据源

设置页用勾选框选择种子来源：

- **JavBus**：默认勾选，直连站点取种。
- **JavRanking**：勾选后按需抓取详情页磁力并写入本地缓存；没有缓存时会自动请求详情页。
- 两个都勾选时并行抓取，按 infoHash 合并去重。
- 所有站点请求共用最小 3 秒间隔，避免触发限流。

本地缓存位于插件数据目录的 `javranking/javranking.db`，也可由
`tools/fetch_javranking_cache.py` 与 `tools/build_javranking_cache.py` 刷新。

## 洗版

可选开启。库内已有番号时默认跳过；只有显式请求洗版才比较质量：

- 下载前按分辨率标签、体积、码率粗筛；
- 下载后用 ffprobe 复核真实分辨率与码率；
- 确认更好才删除旧视频并替换，旧片目录内有多个视频时只删除目标文件；
- 支持 Emby 路径到 CD2 路径的目录映射。

## 已归档

**极速搜索 · FastIntersectSearch** 已于 2026-10-20 归档，不再维护。

代码、历史版本（V2/V3）、清单与文档全部移到 `archive/fastintersectsearch/`。
归档原因见该目录下的 `README.md`：根因在 MoviePilot-Rust 的正则缓存容量，上游修复后
插件失去存在意义。

归档内容不参与 `package.v3.json` 的市场清单，也不会随本仓库当前插件一起发布。
