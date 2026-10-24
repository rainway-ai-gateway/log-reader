-- ============================================================================
-- bfe_ai_request_log 建表语句（LR03 集成测试内嵌副本）
--
-- 权威 DDL 归 ai-gateway-api 仓库 db_ddl_report_mysql.sql 明细表（2026-09-27
-- 99 列版，含 ai-cache/流量镜像/ai-intent 10 新列；2026-10-13 起基准另含
-- bfenetworks/bfe#1417：uk_dedup 为五列、logid 收紧为 NOT NULL DEFAULT 0），
-- 本副本以其为基准重新生成，权威 DDL 后续变更时应同步重新生成。
-- 列清单/列序/类型与权威 DDL 明细表保持一致，并有三处测试化调整（权威 DDL
-- 变更时必须同样处理）：
-- 1. 长文本列（origin_uri/final_uri/x_forward_for/authorization/referrer/
--    user_agent/cookie/res_location）使用 TEXT 而非 VARCHAR——utf8mb4 下
--    VARCHAR 长度总和超过 MySQL 65535 字节行上限（Error 1118）；
-- 2. 幂等键中的字符串列 ai_apikey_id / ai_requested_model 允许 NULL：
--    零值规则将空字符串映射为 NULL（未认证请求没有 API Key），而
--    NOT NULL 约束会使这类写入整批失败（Error 1048）；hostid / log_time
--    由 log-reader 构造保证非空，保持 NOT NULL；
-- 3. 去掉表分区子句：RANGE 分区要求插入行的 log_time 落在已建分区内，
--    测试日志使用固定时间戳，预建分区既不必要也会引入额外失败面；
--    写入路径不感知表分区。
-- 要求 MySQL >= 5.7.8（JSON 列），推荐 8.0。
-- ============================================================================
CREATE TABLE IF NOT EXISTS bfe_ai_request_log (
    hostid                  VARCHAR(256)  NOT NULL DEFAULT '',
    log_time                DATETIME      NOT NULL,
    ai_apikey_id            VARCHAR(256)  DEFAULT NULL,
    ai_requested_model      VARCHAR(128)  DEFAULT NULL,
    -- logid：BFE 逐请求唯一标识（非 0）。NOT NULL DEFAULT 0 而非可空：MySQL 唯一键
    -- 视 NULL 互不相等，可空会让缺 id 的行逃出全部去重（同一行重放插入重复行）。
    logid                   BIGINT        NOT NULL DEFAULT 0,
    product                 VARCHAR(64)   DEFAULT NULL,
    log_tag                 VARCHAR(64)   DEFAULT NULL,
    -- 客户端连接
    client_ip               VARCHAR(64)   DEFAULT NULL,
    client_network          VARCHAR(16)   DEFAULT NULL,
    is_trust_src_ip         TINYINT       DEFAULT NULL,
    req_num                 INT           DEFAULT NULL,
    session_id              BIGINT        DEFAULT NULL,
    bfe_ip                  VARCHAR(64)   DEFAULT NULL,
    sock_src_ip             VARCHAR(64)   DEFAULT NULL,
    vip                     VARCHAR(64)   DEFAULT NULL,
    vip6                    VARCHAR(128)  DEFAULT NULL,
    -- 错误
    err_code                VARCHAR(64)   DEFAULT NULL,
    err_msg                 VARCHAR(512)  DEFAULT NULL,
    -- 请求
    proto                   VARCHAR(16)   DEFAULT NULL,
    header_host             VARCHAR(256)  DEFAULT NULL,
    origin_uri              TEXT          DEFAULT NULL,
    final_uri               TEXT          DEFAULT NULL,
    method                  VARCHAR(16)   DEFAULT NULL,
    content_type            VARCHAR(128)  DEFAULT NULL,
    x_forward_for           TEXT          DEFAULT NULL,
    accept_language         VARCHAR(256)  DEFAULT NULL,
    authorization           TEXT          DEFAULT NULL,
    transfer_encoding       VARCHAR(64)   DEFAULT NULL,
    referrer                TEXT          DEFAULT NULL,
    user_agent              TEXT          DEFAULT NULL,
    delegation              VARCHAR(256)  DEFAULT NULL,
    uid                     VARCHAR(256)  DEFAULT NULL,
    cookie                  TEXT          DEFAULT NULL,
    req_headers             JSON          DEFAULT NULL,
    req_header_len          INT           DEFAULT NULL,
    req_body_len            INT           DEFAULT NULL,
    -- 路由
    cluster                 VARCHAR(256)  DEFAULT NULL,
    sub_cluster             VARCHAR(256)  DEFAULT NULL,
    backend_info            VARCHAR(256)  DEFAULT NULL,
    backend_retry           TINYINT       DEFAULT NULL,
    -- 响应
    res_status_code         SMALLINT      DEFAULT NULL,
    res_header_len          INT           DEFAULT NULL,
    res_body_len            INT           DEFAULT NULL,
    res_content_type        VARCHAR(128)  DEFAULT NULL,
    res_location            TEXT          DEFAULT NULL,
    res_transfer_encoding   VARCHAR(64)   DEFAULT NULL,
    res_headers             JSON          DEFAULT NULL,
    -- 耗时（毫秒）
    all_time                INT           DEFAULT NULL,
    read_client_time        INT           DEFAULT NULL,
    cluster_serve_time      INT           DEFAULT NULL,
    backend_serve_time      INT           DEFAULT NULL,
    write_client_time       INT           DEFAULT NULL,
    connect_backend_time    INT           DEFAULT NULL,
    proxy_delay_time        INT           DEFAULT NULL,
    session_offset_time     INT           DEFAULT NULL,
    -- API Key 标签（写时打平）
    level1Name              VARCHAR(128)  DEFAULT NULL,
    level1                  VARCHAR(128)  DEFAULT NULL,
    level2Name              VARCHAR(128)  DEFAULT NULL,
    level2                  VARCHAR(128)  DEFAULT NULL,
    level3Name              VARCHAR(128)  DEFAULT NULL,
    level3                  VARCHAR(128)  DEFAULT NULL,
    level4Name              VARCHAR(128)  DEFAULT NULL,
    level4                  VARCHAR(128)  DEFAULT NULL,
    level5Name              VARCHAR(128)  DEFAULT NULL,
    level5                  VARCHAR(128)  DEFAULT NULL,
    -- AI 可观测
    ai_target_model         VARCHAR(128)  DEFAULT NULL,
    ai_stream               TINYINT       DEFAULT NULL,
    ai_input_tokens         BIGINT        DEFAULT NULL,
    ai_output_tokens        BIGINT        DEFAULT NULL,
    ai_total_tokens         BIGINT        DEFAULT NULL,
    ai_cache_read_tokens    BIGINT        DEFAULT NULL,
    ai_cache_write_tokens   BIGINT        DEFAULT NULL,
    ai_audio_input_tokens   BIGINT        DEFAULT NULL,
    ai_audio_output_tokens  BIGINT        DEFAULT NULL,
    ai_image_count          BIGINT        DEFAULT NULL,
    ai_ttft_us              BIGINT        DEFAULT NULL,
    ai_tpot_us              BIGINT        DEFAULT NULL,
    ai_provider             VARCHAR(64)   DEFAULT NULL,
    ai_protocol             VARCHAR(64)   DEFAULT NULL,
    ai_mode                 VARCHAR(64)   DEFAULT NULL,
    ai_retry_count          INT           DEFAULT NULL,
    ai_cost_value           BIGINT        DEFAULT NULL,
    ai_cost_currency        VARCHAR(16)   DEFAULT NULL,
    ai_route_rule_hits      JSON          DEFAULT NULL,
    ai_cluster_key_names    JSON          DEFAULT NULL,
    ai_rate_limit_hits      JSON          DEFAULT NULL,
    ai_auth_reject_reason   VARCHAR(256)  DEFAULT NULL,
    ai_auth_reject_quota_plans JSON       DEFAULT NULL,
    ai_auth_hit_quota_plans JSON          DEFAULT NULL,
    -- AI 缓存/镜像/意图（2026-09-27 加列，proto v0.3.7/3.8/3.9 字段；列序与
    -- ai-gateway-api 权威 DDL 明细表一致；ai_cache_key、mirror_status~mirror_error
    -- 不进报表库：前者 debug 专用防膨胀，后者走 Prometheus 不回写日志）
    ai_cache_status         VARCHAR(16)   NOT NULL DEFAULT '',
    mirror_hit              TINYINT       NOT NULL DEFAULT 0,
    mirror_cluster          VARCHAR(128)  NOT NULL DEFAULT '',
    ai_intent_question      VARCHAR(64)   NOT NULL DEFAULT '',
    ai_intent_answer        VARCHAR(64)   NOT NULL DEFAULT '',
    ai_intent_confidence    DOUBLE        DEFAULT NULL,
    ai_intent_source        VARCHAR(32)   NOT NULL DEFAULT '',
    ai_intent_latency_us    BIGINT        DEFAULT NULL,
    ai_intent_cache_hit     TINYINT       DEFAULT NULL,
    ai_intent_questions_version VARCHAR(32) NOT NULL DEFAULT '',
    -- 幂等键：含请求级判别列 logid（bfenetworks/bfe#1417）——同秒同 Key 同模型的
    -- 并发请求各自成行；只有同一请求重放（同 logid）才命中覆盖（重发/-b 补读仍安全）
    UNIQUE KEY uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model, logid),
    KEY idx_model_time (ai_target_model, log_time),
    KEY idx_apikey_time (ai_apikey_id, log_time),
    KEY idx_provider_time (ai_provider, log_time),
    KEY idx_host_time (hostid, log_time),
    KEY idx_status_time (res_status_code, log_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;