# MoviePilot 插件

本仓库当前维护 **番号云下载**。

## 番号云下载 · JavCloudDownload

按番号从 JavBus 取种，经 CloudDrive2 gRPC 提交 115 离线下载；落盘后清理广告文件、
重命名，并按配置移动到库目录。

- 代码：`plugins.v3/javclouddownload/`
- 清单：`package.v3.json` 中的 `JavCloudDownload`
- 依赖：`clouddrive2-client==0.3.0`、`grpcio~=1.80.0`、`protobuf>=6.31.1,<7`
- 当前版本：`0.1.2`
- 运行基线：MoviePilot `v3.1.8`

配置项包括 CloudDrive2 地址与令牌、云下载目录、库目录、是否移动、是否允许无码破解资源、
任务推进间隔与最长等待时间。任务由插件注册的定时服务异步推进，也可通过插件 API 手动触发。


## 数据源

插件设置里的「数据源」有三档：

- `javbus`：直连 JavBus，默认
- `javranking`：只读本地缓存；没有详情页磁力的番号会直接报错
- `auto`：优先 JavRanking 缓存，缓存没有磁力或不可用时回退 JavBus

JavRanking 缓存由 `tools/fetch_javranking_cache.py` 在 MP 容器内用浏览器抓取，
再由 `tools/build_javranking_cache.py` 整理成 `javranking.db`。

## 已归档

**极速搜索 · FastIntersectSearch** 已于 2026-10-20 归档，不再维护。

代码、历史版本（V2/V3）、清单与文档全部移到 `archive/fastintersectsearch/`。
归档原因见该目录下的 `README.md`：根因在 MoviePilot-Rust 的正则缓存容量，上游修复后
插件失去存在意义。

归档内容不参与 `package.v3.json` 的市场清单，也不会随本仓库当前插件一起发布。
