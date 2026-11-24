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
| `GET /api/v1/status` | 最新采样、WAN 状态、最近的 AWS 上传确认，以及供 Vigil 显示的 `liveMetrics` |
| `GET /api/v1/metrics` | `from`、`to`。返回对齐 UTC 的 5 分钟窗口，包括样本数、采集错误数、CPU、负载、I/O 等待、内存、Swap、温度、存储、WAN 和网口状态以及连接跟踪。没有样本的窗口返回 `sampleCount: 0` |
| `GET /api/v1/network` | 最新网口、逻辑链路和可用计数器。返回采样状态与时间、互联网逻辑接口、接口状态及累计收发字节 |
| `GET /api/v1/network/traffic` | `interface`、`from`、`to`，最长 7 天。每个 UTC 5 分钟窗口返回观测到的 RX/TX 增量字节和有效采样对数 |
| `GET /api/v1/network/bandwidth` | `interface`。返回采集端缓存的 LuCI 3 秒带宽窗口，适用于逻辑 WAN 和 WireGuard |
| `GET /api/v1/client-traffic` | `from`、`to`、`scope=internet\|internal`、`region=all\|cn\|foreign\|unknown`、`route=all\|direct\|proxy\|unknown`、`category=all\|aws\|cn`、可选设备 `client`，最长 60 天。7 天以内按 5 分钟汇总，更长范围按小时汇总。设备和设备服务行均返回可用的 `deviceName`，设备服务行也返回汇总对应的目标 IP 样本 |
| `GET /api/v1/client-traffic/aws-usage` | `from`、`to`，使用采集到的 Xray `proxy` 出站计数；缺少直接计数的历史样本使用原连接统计推算，标为部分覆盖 |
| `GET /api/v1/traffic-rules` | 列出 Oracle 中服务规则和设备名称，同一个监控主机共享配置 |
| `POST /api/v1/traffic-rules` | 新增规则，提交 `kind`、`target`、`label` |
| `PUT /api/v1/traffic-rules/{id}` | 修改规则，提交 `kind`、`target`、`label` |
| `DELETE /api/v1/traffic-rules/{id}` | 删除规则 |
| `GET /api/v1/events` | `from`、`to`、`type`、`limit`、`cursor`。默认显示关键事件，使用 `all=true` 可查全部。下一页传回 `nextCursor`，沿用筛选条件 |
| `GET /api/v1/events/{id}` | 单条日志原文和完整归档记录 |

`from` 包含起点，`to` 排除终点，传带时区的 ISO 8601 时间。默认查最近 24 小时。单次指标查询支持最多 31 天，事件列表默认每页 50 条，最多 200 条。缺少数据库配置时历史接口返回 `503 history_disabled`。数据库临时不可用时返回 `503 history_unavailable`。实时状态接口继续读取本地文件。

流量历史使用已入库样本的累计计数差分，按采样对的结束时间分桶。重启、接口或计数器重建、计数回退和两分钟以上的采样间隔都跳过。窗口缺少可用采样对时，收发字节为 `null`。有效的零流量返回 `0`。历史随已确认批次写入 Oracle。

互联网口径使用已识别的逻辑 WAN。IPTV 逻辑接口可查看状态，且从流量历史查询排除。物理网口可承载多个逻辑网络，原始接口计数可能含 IPTV。转发口径需要采集端提供已排除 IPTV 的命名计数器。Xray 口径需要采集端提供出站字节计数，访问日志可用于统计访问记录数量。不同口径有流量重叠，仪表分开展示。

实时带宽读取 `latest-bandwidth.json`。采样缺失、超过 20 秒或接口没有可用窗口时返回 `503 bandwidth_unavailable`。此查询直接使用采集端缓存，与 Oracle 归档查询独立。

设备流量查询使用已入库样本中的 `client_traffic`。设备 ID 优先使用 MAC，缺少 MAC 时使用客户端 IP。流量方向以客户端为准。内网互访的地区为 `internal`，可使用默认的 `region=all` 查询。地区总量应用范围、分流和设备筛选，设备选项应用范围、地区和分流筛选。趋势和目的地同时应用所有筛选条件。

设备流量接口要求 `Authorization: Bearer <token>`。在采集端状态目录放置 `client-traffic.token`，内容为 32 至 128 个 base64url 字符，可带结尾换行。令牌缺失、无效或不匹配时返回 `401 client_traffic_unauthorized`。

在 `ORCLPDB1` 使用监控数据库账号 `BLOG` 执行一次 `sql/oracle-traffic-rules.sql`。服务识别和设备名统一存入 `R86S_TRAFFIC_RULE`，已有三张表无需修改。可在 Vigil 的规则管理页维护。规则接口使用同一个流量令牌，运行在受信局域网。数据库表尚未建立时，规则接口返回 `503 rules_schema_missing`，流量仍可使用内置服务分类和 GeoSite。

`ip` 匹配目标 IPv4 或 IPv6，`domain` 匹配域名及其子域，`exact` 只匹配完整域名。设备名称使用 `mac` 匹配设备 MAC，也可以用 `client_ip` 匹配客户端 IPv4 或 IPv6，例如没有 MAC 的 WireGuard 客户端。有两种匹配时优先使用 MAC 名称。规则编辑后会用于下一次查询，包括已入库历史。

已有 `R86S_TRAFFIC_RULE` 的环境需使用 `BLOG` 在 `ORCLPDB1` 执行一次 `sql/oracle-client-ip-rules.sql`，再保存新的设备 IP 规则。已有规则继续保留；如果曾误建同一地址的「服务 IP」规则，可在规则管理页把原行编辑为「设备 IP」。

五分钟窗口缺少采集样本时返回 `null`，有采集样本且筛选后没有流量时返回 `0`。质量字段报告有数据、有部分数据的样本数和缺测窗口数。设备列表最多 1,000 项，目的地摘要最多 250 项，列表限制在质量字段中标记。汇总总量始终包含符合当前筛选条件的所有有效记录。

## 更新 Vigil 所用的指标

发布新镜像后，在 OpenWrt 上更新 `r86s-monitor-api`。现有 Oracle 表可继续使用。历史窗口从已保存的样本 JSON 取 1 分钟负载峰值 `load1Max` 和 Swap 已用峰值 `swapUsedMaxBytes`。只在对应样本包含原始值时返回统计，未配置 Swap 且读到零值时返回 0。

实时负载、CPU、I/O 等待、内存、Swap、温度、磁盘时延和连接跟踪使用最新采样。磁盘容量按实际挂载点提供，Vigil 使用文件系统标识去重。主要目录占用显示实际测量时间。

完整的挂载点容量和目录占用需要同时更新 OpenWrt 的 `/usr/libexec/r86s-monitor/monitor.py`。上传新版脚本到路由器的 `/tmp/monitor.py` 后执行以下命令。

```sh
cp -p /usr/libexec/r86s-monitor/monitor.py /usr/libexec/r86s-monitor/monitor.py.bak
cp /tmp/monitor.py /usr/libexec/r86s-monitor/monitor.py
/etc/init.d/r86s-monitor restart
```

采集器默认测量 `/fourT/oracle/oradata` 和 `/fourT/openwrt-monitor`。路径不同可以在原有 `/etc/r86s-monitor/config.json` 配置 `directory_paths` 数组。采集器启动时安排一次测量，之后每 15 分钟在后台运行一次，单个目录最长等待 10 秒。结果会在下一次状态采样中发布。暂时无法测量时保留最后一次成功值和时间。
