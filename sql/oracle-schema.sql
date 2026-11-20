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
