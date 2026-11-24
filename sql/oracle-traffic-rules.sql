-- 使用现有监控数据库账号 BLOG 在 ORCLPDB1 中执行一次。
CREATE TABLE r86s_traffic_rule (
    host_id     VARCHAR2(128 CHAR) NOT NULL,
    rule_id     VARCHAR2(36 CHAR) NOT NULL,
    rule_kind   VARCHAR2(12 CHAR) NOT NULL,
    rule_target VARCHAR2(253 CHAR) NOT NULL,
    rule_label  VARCHAR2(64 CHAR) NOT NULL,
    CONSTRAINT r86s_traffic_rule_pk PRIMARY KEY (host_id, rule_id),
    CONSTRAINT r86s_traffic_rule_key UNIQUE (host_id, rule_kind, rule_target),
    CONSTRAINT r86s_traffic_rule_kind_ck CHECK (
        CASE rule_kind WHEN 'ip' THEN 1 WHEN 'domain' THEN 1 WHEN 'exact' THEN 1 WHEN 'mac' THEN 1
                       WHEN 'client_ip' THEN 1 ELSE 0 END = 1
    )
);

COMMENT ON TABLE r86s_traffic_rule IS '流量监控的手动服务识别规则与设备别名';
COMMENT ON COLUMN r86s_traffic_rule.host_id IS '监控主机标识，用于隔离不同路由器的规则';
COMMENT ON COLUMN r86s_traffic_rule.rule_id IS '规则 UUID 由监控接口创建';
COMMENT ON COLUMN r86s_traffic_rule.rule_kind IS 'ip 匹配目标 IP，domain 包括子域，exact 仅完整域名，mac 匹配设备 MAC，client_ip 匹配客户端 IP';
COMMENT ON COLUMN r86s_traffic_rule.rule_target IS '规范化后的目标 IP、目标域名或设备 MAC 地址';
COMMENT ON COLUMN r86s_traffic_rule.rule_label IS '仪表盘显示的服务名或设备名称';
