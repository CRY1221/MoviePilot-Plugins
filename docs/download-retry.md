# 下载推进、超时与换种

## 轮询

插件通过 CloudDrive2 的 `ListOfflineFilesByPath` 读取 115 离线任务列表，
按 `infoHash` 匹配当前候选，再读 `OfflineFileStatus`：

- `OFFLINE_INIT`：排队中
- `OFFLINE_DOWNLOADING`：下载中，记录百分比
- `OFFLINE_FINISHED`：下载完成，进入整理
- `OFFLINE_ERROR`：失败

默认每 **10 秒** 查一次（`poll_seconds`，最小 5 秒）。每轮只拉一次离线列表，
不是每条任务各拉一次。

## 计时

- **5 分钟超时**：从任务第一次进入 `OFFLINE_DOWNLOADING` 开始算。
  5 分钟还没 `OFFLINE_FINISHED`，删除当前候选，换下一条。
- **10 分钟启动超时**：从提交算起，任务一直查不到或一直停在 `OFFLINE_INIT`，
  也删除并换下一条。
- 115 上传磁力链、确认下载后通常几秒内就能在离线列表看到任务；如果 10 分钟还没有，
  说明这条链接大概率有问题。

相关配置：

| 配置 | 默认 | 说明 |
|---|---:|---|
| `poll_seconds` | 10 | 轮询间隔（秒） |
| `candidate_timeout_minutes` | 5 | 进入下载后的单条候选超时 |
| `candidate_queue_timeout_minutes` | 10 | 从提交到进入下载的启动超时 |
| `max_candidates` | 3 | 一个番号最多尝试候选数 |

## 超时删除与换种

超时后调用 CD2：

```text
RemoveOfflineFiles(path=云下载目录, infoHashes=[当前 infoHash], deleteFiles=True)
```

任务和未完成文件一起删除，然后按选种顺序提交下一条候选。

- 删除失败：不提交下一条，下一轮重试删除；连续 3 次失败才终止。
  这是为了避免两个任务同时下载。
- 3 条候选都超时：任务标记 `failed`，发 MP 失败通知。
- 候选按 `infoHash` 去重，避免同一个种子换名字重复尝试。

## 通知

- 任务进入 `OFFLINE_FINISHED`：发一条 `番号云下载完成` MP 通知。
- 所有候选失败、删除重试耗尽、整理重试耗尽：发一条 `番号云下载失败` 通知。
- 单条候选超时换种只写日志和任务记录，不逐条发通知。

## 旧任务兼容

升级前已存在的进行中任务没有候选列表，继续按旧的 `max_wait_minutes`（240 分钟）
推进，不自动删除、不换种；新提交的任务才走上面的规则。

## 已知边界

- 5 分钟是严格值，大文件在 5 分钟内下不完也会被删除换种。
- 删除接口依赖 CD2 的 `RemoveOfflineFiles`；线上需要确认它确实能同时删除任务和文件。
- `pipeline.py` 里的 `wait_offline` / `run_once` 是历史实现，当前插件主流程未使用。
