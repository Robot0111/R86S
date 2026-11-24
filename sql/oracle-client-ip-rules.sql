-- 用 BLOG 连接 ORCLPDB1 后，对已存在的 R86S_TRAFFIC_RULE 执行一次。
-- 先扩展列和添加新版检查，再移除旧检查。已有规则、主键和唯一索引保留。
ALTER TABLE r86s_traffic_rule MODIFY (rule_kind VARCHAR2(12 CHAR));

ALTER TABLE r86s_traffic_rule ADD CONSTRAINT r86s_traffic_rule_kind_v2_ck CHECK (
    CASE rule_kind WHEN 'ip' THEN 1 WHEN 'domain' THEN 1 WHEN 'exact' THEN 1 WHEN 'mac' THEN 1
                   WHEN 'client_ip' THEN 1 ELSE 0 END = 1
);

ALTER TABLE r86s_traffic_rule DROP CONSTRAINT r86s_traffic_rule_kind_ck;

ALTER TABLE r86s_traffic_rule RENAME CONSTRAINT r86s_traffic_rule_kind_v2_ck TO r86s_traffic_rule_kind_ck;

COMMENT ON COLUMN r86s_traffic_rule.rule_kind IS 'ip 匹配目标 IP，domain 包括子域，exact 仅完整域名，mac 匹配设备 MAC，client_ip 匹配客户端 IP';
