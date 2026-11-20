# R86S 监控 API

使用 Java 21。状态接口读 OpenWrt 的采集状态文件。历史接口读取 Oracle 中的 AWS 已确认批次。容器在已有的 Docker `lan` 网络上使用 `192.168.3.22:8080`。

## 准备 Oracle

用现有 `blog` 用户连接 `ORCLPDB1`，在该用户下执行一次 [建表脚本](sql/oracle-schema.sql)。脚本建三个以 `R86S_` 开头的表和相应索引。把 `.env.example` 复制为 Compose 同目录的 `.env`，填写 Oracle 密码。密码存放在部署主机的 `.env` 中，建议文件权限设为 `600`。

构建并推送镜像后，在 R86S 的 Compose 目录执行：

```sh
docker compose up -d
docker compose logs --tail=50 r86s-monitor-api
```

`R86S_IMPORT_SECONDS` 默认 30 秒，控制检查间隔。采集器通常每 5 分钟封存一批，达到封存体积阈值时会提前封存。Java 在批次确认到达 AWS 后导入它。刚发生的事件会在下一次已确认的批次入库后出现在历史查询里。

## 批次确认与清理

每批先检查本地 AWS 确认、压缩包 SHA-256、归档清单和各文件的校验和。Oracle 在同一事务里写入完整日志、30 秒指标和批次确认。按 `event_id` 和 `sample_id` 去重，`batch_id` 记录整批提交结果。暂时无法连接数据库时，后续轮询会重试。

数据库按记录时间保留 180 天，每小时检查一次。可用 `R86S_DB_RETENTION_DAYS` 调整。

启用本地清理前，需要部署带 `oracle_managed_retention` 支持的 Python 采集器，并在其 `/etc/r86s-monitor/config.json` 中设为 `true`。这个选项把 `raw`、`metrics` 和 `batches/sent` 的保留交给 Oracle 导入端。采集器应保留尚有本地归档文件的 AWS 确认。旧版采集器仍会按它自己的保留期限清理这些文件。

用下列查询确认 Oracle 已经接到批次：

```sql
SELECT batch_id, event_count, sample_count
FROM r86s_ingest_batch
ORDER BY imported_ms DESC FETCH FIRST 5 ROWS ONLY;
```

验证批次后，把 Compose 的 `.env` 中 `R86S_PRUNE_LOCAL` 改为 `true`，再运行 `docker compose up -d` 应用。已封存包确认写入 Oracle 后清理。按日保存的 JSONL 还需满足 UTC 日期已经结束、至少 24 小时未写入，以及每条记录的 ID 均能在 Oracle 查到。遇到不完整、校验失败或缺少记录的文件时会保留该文件。

## 查询接口

| 路径 | 查询参数和返回内容 |
| --- | --- |
| `GET /api/v1/status` | 最新采样、WAN 状态、最近的 AWS 上传确认 |
| `GET /api/v1/metrics` | `from`、`to`。返回对齐 UTC 的 5 分钟窗口，包括样本数、采集错误数、CPU、内存、温度、存储、WAN 和网口状态以及连接跟踪。没有样本的窗口返回 `sampleCount: 0` |
| `GET /api/v1/events` | `from`、`to`、`type`、`limit`、`cursor`。默认显示关键事件，使用 `all=true` 可查全部。下一页传回 `nextCursor`，沿用筛选条件 |
| `GET /api/v1/events/{id}` | 单条日志原文和完整归档记录 |

`from` 包含起点，`to` 排除终点，传带时区的 ISO 8601 时间。默认查最近 24 小时。单次指标查询支持最多 31 天，事件列表默认每页 50 条，最多 200 条。缺少数据库配置时历史接口返回 `503 history_disabled`。数据库临时不可用时返回 `503 history_unavailable`。实时状态接口继续读取本地文件。
