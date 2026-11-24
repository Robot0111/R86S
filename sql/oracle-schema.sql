CREATE TABLE r86s_metric_sample (
    host_id       VARCHAR2(128 CHAR) NOT NULL,
    sample_id     VARCHAR2(32 CHAR) NOT NULL,
    sampled_ms    NUMBER(19) NOT NULL,
    window_ms     NUMBER(19) NOT NULL,
    cpu_pct       NUMBER,
    iowait_pct    NUMBER,
    mem_avail     NUMBER(19),
    temperature   NUMBER,
    overlay_free  NUMBER(19),
    docker_free   NUMBER(19),
    archive_free  NUMBER(19),
    disk_await    NUMBER,
    conntrack_pct NUMBER,
    wan_up        NUMBER(1),
    eth0_up       NUMBER(1),
    eth2_up       NUMBER(1),
    has_errors    NUMBER(1) NOT NULL,
    record_json   CLOB NOT NULL,
    CONSTRAINT r86s_metric_sample_pk PRIMARY KEY (host_id, sample_id)
);

CREATE INDEX r86s_metric_sample_time ON r86s_metric_sample (host_id, sampled_ms);

CREATE TABLE r86s_event (
    host_id     VARCHAR2(128 CHAR) NOT NULL,
    event_id    VARCHAR2(64 CHAR) NOT NULL,
    observed_ms NUMBER(19) NOT NULL,
    category    VARCHAR2(32 CHAR) NOT NULL,
    event_type  VARCHAR2(32 CHAR) NOT NULL,
    severity    VARCHAR2(16 CHAR) NOT NULL,
    log_source  VARCHAR2(512 CHAR),
    preview     VARCHAR2(512 CHAR),
    record_json CLOB NOT NULL,
    CONSTRAINT r86s_event_pk PRIMARY KEY (host_id, event_id)
);

CREATE INDEX r86s_event_time ON r86s_event (host_id, observed_ms, event_id);
CREATE INDEX r86s_event_type_time ON r86s_event (host_id, event_type, observed_ms, event_id);

CREATE TABLE r86s_ingest_batch (
    host_id      VARCHAR2(128 CHAR) NOT NULL,
    batch_id     VARCHAR2(64 CHAR) NOT NULL,
    sha256       VARCHAR2(64 CHAR) NOT NULL,
    sealed_ms    NUMBER(19) NOT NULL,
    imported_ms  NUMBER(19) NOT NULL,
    event_count  NUMBER(10) NOT NULL,
    sample_count NUMBER(10) NOT NULL,
    CONSTRAINT r86s_ingest_batch_pk PRIMARY KEY (host_id, batch_id)
);

CREATE INDEX r86s_ingest_batch_time ON r86s_ingest_batch (host_id, sealed_ms);
COMMENT ON TABLE r86s_metric_sample IS 'R86S 系统指标样本，通常每 30 秒采集一次';
COMMENT ON COLUMN r86s_metric_sample.host_id IS '采集主机标识，与 sample_id 组成主键';
COMMENT ON COLUMN r86s_metric_sample.sample_id IS '采集器生成的样本标识，用于重复导入时去重';
COMMENT ON COLUMN r86s_metric_sample.sampled_ms IS '采样时间，UTC Unix 时间戳，单位毫秒';
COMMENT ON COLUMN r86s_metric_sample.window_ms IS '样本所属的 UTC 五分钟窗口起点，Unix 时间戳，单位毫秒';
COMMENT ON COLUMN r86s_metric_sample.cpu_pct IS '两次采样间的 CPU 忙碌占比，单位百分比，不含 I/O 等待';
COMMENT ON COLUMN r86s_metric_sample.iowait_pct IS '两次采样间的 CPU I/O 等待占比，单位百分比';
COMMENT ON COLUMN r86s_metric_sample.mem_avail IS 'Linux MemAvailable，可用内存估算值，单位字节';
COMMENT ON COLUMN r86s_metric_sample.temperature IS '本次采样中各传感器的最高温度，单位摄氏度';
COMMENT ON COLUMN r86s_metric_sample.overlay_free IS '/overlay 所在文件系统的剩余空间，单位字节';
COMMENT ON COLUMN r86s_metric_sample.docker_free IS '/docker 所在文件系统的剩余空间，单位字节';
COMMENT ON COLUMN r86s_metric_sample.archive_free IS '/fourT 所在文件系统的剩余空间，单位字节';
COMMENT ON COLUMN r86s_metric_sample.disk_await IS '本次采样中各磁盘平均 I/O 耗时的最大值，单位毫秒';
COMMENT ON COLUMN r86s_metric_sample.conntrack_pct IS '连接跟踪表使用率，当前条目数除以上限再乘以 100';
COMMENT ON COLUMN r86s_metric_sample.wan_up IS 'WAN 状态，1 为在线，0 为离线，NULL 为未知';
COMMENT ON COLUMN r86s_metric_sample.eth0_up IS 'eth0 链路状态，1 为 up，0 为 down 或 lowerlayerdown，NULL 为未知';
COMMENT ON COLUMN r86s_metric_sample.eth2_up IS 'eth2 链路状态，1 为 up，0 为 down 或 lowerlayerdown，NULL 为未知';
COMMENT ON COLUMN r86s_metric_sample.has_errors IS '本次采样的错误标志，1 表示 errors 对象非空，0 表示没有上报错误';
COMMENT ON COLUMN r86s_metric_sample.record_json IS '归档中的完整指标样本 JSON，包含各项采集值和错误明细';

COMMENT ON TABLE r86s_event IS 'R86S 归档日志，包含关键事件和普通日志';
COMMENT ON COLUMN r86s_event.host_id IS '采集主机标识，与 event_id 组成主键';
COMMENT ON COLUMN r86s_event.event_id IS '采集器生成的日志标识，用于去重和查询单条日志';
COMMENT ON COLUMN r86s_event.observed_ms IS '采集器读取到该日志的时间，UTC Unix 时间戳，单位毫秒';
COMMENT ON COLUMN r86s_event.category IS '采集器划分的日志类别，例如 kernel、network、dns-dhcp';
COMMENT ON COLUMN r86s_event.event_type IS '导入程序识别的事件类型，例如 oom、pppoe_down，普通日志归为 other';
COMMENT ON COLUMN r86s_event.severity IS '日志严重级别，例如 info、warn、err，无法识别时为 unknown';
COMMENT ON COLUMN r86s_event.log_source IS '采集器记录的日志来源标识';
COMMENT ON COLUMN r86s_event.preview IS '日志原文或启动快照的预览，最多 512 字符';
COMMENT ON COLUMN r86s_event.record_json IS '归档中的完整日志 JSON，包含原文或启动快照以及采集元数据';

COMMENT ON TABLE r86s_ingest_batch IS 'AWS 已确认且 Oracle 已完成事务提交的归档批次';
COMMENT ON COLUMN r86s_ingest_batch.host_id IS '采集主机标识，与 batch_id 组成主键';
COMMENT ON COLUMN r86s_ingest_batch.batch_id IS '采集器生成的归档批次标识，用于识别已提交的批次';
COMMENT ON COLUMN r86s_ingest_batch.sha256 IS '归档压缩包的 SHA-256 摘要，用于与 AWS 确认记录核对';
COMMENT ON COLUMN r86s_ingest_batch.sealed_ms IS '采集器封存该批次的时间，UTC Unix 时间戳，单位毫秒';
COMMENT ON COLUMN r86s_ingest_batch.imported_ms IS '写入该批导入确认记录的时间，UTC Unix 时间戳，单位毫秒';
COMMENT ON COLUMN r86s_ingest_batch.event_count IS '归档内的日志总条数，包含被去重的记录';
COMMENT ON COLUMN r86s_ingest_batch.sample_count IS '归档内的指标样本总数，包含被去重的记录';