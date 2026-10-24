# AI Gateway 可观测性链路打通指南

本文档演示如何打通 **LogReader → Kafka → Doris → Grafana** 完整链路，实现 AI Gateway 请求的可观测性（QPS、Token 用量、延迟、限流、认证拒绝等）。

> **本示例仅用于演示链路打通**。本示例中的Doris 聚合表的设计（维度过大、聚合粒度不合理）不一定适用于您的生产环境，请您根据实际业务需求重新设计聚合表。详见 [第 6 节](#6-重要提示关于聚合表设计)。

---

## 1. 架构概览

```
┌──────────┐    PB 日志文件     ┌────────────┐   JSON 消息    ┌─────────┐
│ BFE (AI  │ ────────────────> │ LogReader   │ ────────────> │  Kafka  │
│ Gateway) │   pb_access.log   │ + mod_kafka │   zstd 压缩   │         │
└──────────┘                   └────────────┘               └────┬────┘
                                                                 │
                                                     Routine Load │
                                                                 ▼
┌──────────┐    MySQL 协议      ┌────────────┐   INSERT JOB    ┌─────────┐
│ Grafana  │ <──────────────── │   Doris     │ <────────────── │  Doris  │
│ Dashboard│                   │ (聚合表)    │   定时聚合       │ (明细表) │
│ + Alert  │                   └────────────┘                 └─────────┘
└──────────┘
```

| 组件 | 角色 | 说明 |
|------|------|------|
| BFE | AI Gateway | 处理 AI API 请求，输出 PB 格式访问日志 |
| LogReader | 日志采集 | 读取 PB 日志，提取字段，通过 mod_kafka 发送到 Kafka |
| Kafka | 消息队列 | 缓冲日志消息，解耦采集端和存储端 |
| Doris | 时序存储 + 聚合 | 明细表存储全量日志，聚合表预计算分钟级指标 |
| Grafana | 可视化 + 告警 | 通过 MySQL 协议查询 Doris，展示看板，触发告警 |

---

## 2. 前提条件

本文档假设以下组件已安装并运行：

| 组件 | 版本要求 | 状态 |
|------|---------|------|
| LogReader | 最新 | 需配置 |
| Kafka | 2.8+ | 已安装运行 |
| Doris | 5.0+ | 已安装运行 |
| Grafana | 8.0+ | 已安装运行 |

> 本文档中 Kafka Broker 地址为 `172.18.1.244:9092`，Doris FE 地址为 `<doris_fe_host>:9030`，请替换为实际地址。

---

## 3. 第一步：配置 LogReader

LogReader 是 BFE 的日志采集组件，通过 `mod_kafka` 模块将 PB 格式的访问日志转为 JSON 并发送到 Kafka。

> **LogReader应与BFE 同机部署**，需在部署BFE时，指定Kafka Topic的信息，参考下文。以下mod_kafka输出的字段是的默认配置。

### 3.1. 主配置文件 `config.conf`

```ini
[main]
httpPort=8992
maxCpus = 6
monitorInterval = 60

[PbAccessLogConf]
LogFile = ./../../bfe/log/pb_access3.log
Modules=mod_kafka
MaxSizePerBatch = 128
```

| 参数 | 说明 |
|------|------|
| `LogFile` | BFE 的 PB 访问日志文件路径 |
| `Modules` | 启用的模块，此处为 `mod_kafka` |
| `MaxSizePerBatch` | 每批读取的最大日志条数 |

### 3.2. Kafka 模块配置 `mod_kafka.conf`

```ini
[Basic]
DataPath = ./../conf/mod_kafka/kafka_config.data
OpenDebug = false

[kafka]
Brokers = 172.18.1.244:9092
Topic = bfe_ai_log
DeadLetterTopic = bfe_ai_log_dlq
Compression = zstd
BatchSize = 100
LingerMs = 100
MaxRetries = 3
```

| 参数 | 说明 |
|------|------|
| `Brokers` | Kafka Broker 地址 |
| `Topic` | 正常日志发送的 Topic |
| `DeadLetterTopic` | 发送失败的消息写入死信 Topic |
| `Compression` | 压缩算法，zstd 兼顾压缩率和速度 |
| `BatchSize` | 每批发送的消息数 |
| `LingerMs` | 批次等待时间（毫秒），在延迟和吞吐间平衡 |
| `MaxRetries` | 发送失败最大重试次数 |

### 3.3. 字段配置 `kafka_config.data`

`FieldMode` 决定发送到 Kafka 的字段集：

| FieldMode | 说明 |
|-----------|------|
| `require` | 仅输出必需字段 |
| `default` | 输出默认字段集（向后兼容） |
| `all` | 输出所有可用字段（60+ 字段） |
| `customized` | 输出 `FieldNames` 中列出的字段 + 必需字段 |

本例使用 `customized` 模式，输出以下 48 个字段：

```ini
[ConfFields]
FieldMode = customized
FieldNames= logid
FieldNames= timestamp
FieldNames= product
FieldNames= hostid
FieldNames= client_ip
FieldNames= is_trust_src_ip
FieldNames= err_code
FieldNames= err_msg
FieldNames= req_header_len
FieldNames= req_body_len
FieldNames= proto
FieldNames= header_host
FieldNames= origin_uri
FieldNames= final_uri
FieldNames= method
FieldNames= content_type
FieldNames= x_forward_for
FieldNames= accept_language
FieldNames= authorization
FieldNames= transfer_encoding
FieldNames= cluster
FieldNames= sub_cluster
FieldNames= backend_info
FieldNames= backend_retry
FieldNames= res_status_code
FieldNames= res_header_len
FieldNames= res_body_len
FieldNames= res_content_type
FieldNames= all_time
FieldNames= read_client_time
FieldNames= cluster_serve_time
FieldNames= backend_serve_time
FieldNames= write_client_time
FieldNames= connect_backend_time
FieldNames= proxy_delay_time
FieldNames= ai_apikey_id
FieldNames= ai_apikey_idtags
FieldNames= ai_requested_model
FieldNames= ai_target_model
FieldNames= ai_stream
FieldNames= ai_input_tokens
FieldNames= ai_output_tokens
FieldNames= ai_total_tokens
FieldNames= ai_ttft_us
FieldNames= ai_tpot_us
FieldNames= ai_rate_limit_hits
FieldNames= ai_auth_reject_reason
FieldNames= ai_auth_reject_quota_plans
```

### 3.4. 发送到 Kafka 的 JSON 消息示例

LogReader 将 PB 日志转换为如下 JSON 格式发送到 Kafka：

**正常 AI 请求：**

```json
{
    "logid": 10602749765076101032,
    "timestamp": 1782353290,
    "product": "AI_product",
    "hostid": "bfe-node01_4026532708",
    "client_ip": "10.0.0.1",
    "req_header_len": 189,
    "req_body_len": 512,
    "proto": "HTTP/1.1",
    "header_host": "ai.example.org",
    "origin_uri": "/v1/chat/completions",
    "method": "POST",
    "content_type": "application/json",
    "x_forward_for": "172.27.152.27",
    "authorization": "Bearer sk-xxx",
    "cluster": "ai_cluster_example",
    "sub_cluster": "ai.pool.bj",
    "backend_info": "10.0.0.2:8080",
    "backend_retry": 1,
    "res_status_code": 200,
    "res_header_len": 155,
    "res_body_len": 1687,
    "res_content_type": "text/event-stream",
    "all_time": 40043,
    "read_client_time": 2,
    "cluster_serve_time": 3,
    "backend_serve_time": 3,
    "write_client_time": 40033,
    "connect_backend_time": 1,
    "proxy_delay_time": 3,
    "ai_apikey_id": "YOURAPIKEY",
    "ai_apikey_idtags": [
        {"tagname": "dep0", "tagvalue": "rd"},
        {"tagname": "dep2", "tagvalue": "teama"},
        {"tagname": "dep3", "tagvalue": "yyx"}
    ],
    "ai_requested_model": "test-model",
    "ai_target_model": "gpt-5",
    "ai_stream": true,
    "ai_input_tokens": 61,
    "ai_output_tokens": 485,
    "ai_total_tokens": 546,
    "ai_ttft_us": 3922,
    "ai_tpot_us": 82715
}
```

**限流命中请求：**

```json
{
    "logid": 8877700219231856663,
    "timestamp": 1782353293,
    "product": "AI_product",
    "hostid": "bfe-node01_4026532708",
    "client_ip": "10.0.0.1",
    "req_header_len": 189,
    "proto": "HTTP/1.1",
    "header_host": "ai.example.org",
    "origin_uri": "/v1/chat/completions",
    "method": "POST",
    "content_type": "application/json",
    "authorization": "Bearer sk-xxx",
    "cluster": "ai_cluster_example",
    "sub_cluster": "ai.pool.bj",
    "res_status_code": 429,
    "res_header_len": 137,
    "res_body_len": 103,
    "res_content_type": "application/json",
    "all_time": 4,
    "proxy_delay_time": 3,
    "err_code": "AI_RATE_LIMIT",
    "ai_apikey_id": "YOURAPIKEY",
    "ai_apikey_idtags": [
        {"tagname": "dep", "tagvalue": "op"},
        {"tagname": "team", "tagvalue": "bfe"}
    ],
    "ai_requested_model": "test-model",
    "ai_target_model": "gpt-5",
    "ai_input_tokens": 61,
    "ai_output_tokens": -1,
    "ai_rate_limit_hits": [
        {
            "rate_limit_policy_id": "rlp-0002",
            "rate_limit_type": "tpm",
            "rule_names": ["tpm1"]
        }
    ]
}
```

**认证拒绝请求：**

```json
{
    "logid": 12345678901234567890,
    "timestamp": 1782353300,
    "product": "AI_product",
    "hostid": "bfe-node01_4026532708",
    "client_ip": "10.0.0.3",
    "req_header_len": 150,
    "proto": "HTTP/1.1",
    "header_host": "ai.example.org",
    "origin_uri": "/v1/chat/completions",
    "method": "POST",
    "content_type": "application/json",
    "cluster": "ai_cluster_example",
    "sub_cluster": "ai.pool.bj",
    "res_status_code": 401,
    "res_header_len": 100,
    "res_body_len": 50,
    "res_content_type": "application/json",
    "all_time": 2,
    "proxy_delay_time": 1,
    "err_code": "AI_AUTH_REJECT",
    "err_msg": "invalid api key",
    "ai_apikey_id": "invalid-key",
    "ai_auth_reject_reason": "apikey not found",
    "ai_auth_reject_quota_plans": ["plan_basic", "plan_pro"]
}
```

> **零值字段**：Kafka 消息中零值字段不会出现（被 Go 的 `omitempty` 跳过），Doris 中对应列自动填充 NULL。

---

## 4. 第二步：创建 Kafka Topic

LogReader 需要两个 Kafka Topic：

| Topic | 用途 |
|-------|------|
| `bfe_ai_log` | 正常日志消息 |
| `bfe_ai_log_dlq` | 发送失败的消息（死信队列） |

### 4.1. 场景选择

根据日均请求量选择合适的分区数和保留策略：

| 场景 | 日均请求量 | 峰值 QPS | 日均数据量 |
|------|-----------|---------|-----------|
| 小规模 | 100 万 | 36 | 0.8 GB |
| 中规模 | 1000 万 | 350 | 8 GB |
| 大规模 | 5000 万 | 1740 | 40 GB |
| 超大规模 | 1 亿 | 3480 | 80 GB |

### 4.2. 创建命令（小规模示例）

```bash
# bfe_ai_log
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.18.1.244:9092 \
  --create \
  --topic bfe_ai_log \
  --partitions 2 --replication-factor 2 \
  --config retention.ms=604800000 \
  --config retention.bytes=10737418240 \
  --config compression.type=zstd

# bfe_ai_log_dlq
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server 172.18.1.244:9092 \
  --create \
  --topic bfe_ai_log_dlq \
  --partitions 1 --replication-factor 2 \
  --config retention.ms=2592000000 \
  --config retention.bytes=1073741824
```

> **参数说明**：
> - `retention.ms=604800000`：保留 7 天
> - `retention.bytes=10737418240`：磁盘上限 10GB
> - `compression.type=zstd`：与 LogReader 的 `Compression = zstd` 保持一致

### 4.3. Kafka 配置汇总

| 场景 | bfe_ai_log 分区 | 副本数 | 保留天数 | bfe_ai_log_dlq 分区 | Producer BatchSize | Producer LingerMs |
|------|:---:|:---:|:---:|:---:|:---:|:---:|
| 100 万/天 | 2 | 2 | 7 | 1 | 100 | 100 |
| 1000 万/天 | 4 | 3 | 7 | 1 | 200 | 50 |
| 5000 万/天 | 8 | 3 | 3 | 2 | 500 | 20 |
| 1 亿/天 | 12 | 3 | 3 | 2 | 500 | 10 |

---

## 5. 第三步：Doris 表设计与数据摄入

Doris 采用**明细表 + 聚合表**双层模型：

| 表 | 模型 | 用途 |
|----|------|------|
| `bfe_ai_request_log` | UNIQUE KEY | 明细日志存储、分位数计算、日志回溯。**注意**：Doris 侧唯一键目前仍是四元组 `(hostid, log_time, ai_apikey_id, ai_requested_model)`，而 MySQL 侧（轻量形态）自 bfenetworks/bfe#1417 起为五元组（多 `logid`）——Doris 侧同缺陷待同改 |
| `bfe_ai_metrics_1m` | AGGREGATE KEY | 分钟级预聚合指标 |

> **⚠️ 注意**：`bfe_ai_metrics_1m` 仅为演示用的聚合表，详见 [第 6 节](#6-重要提示关于聚合表设计)。

### 5.1. 创建数据库

```sql
CREATE DATABASE IF NOT EXISTS bfe_observability;
USE bfe_observability;
```

### 5.2. 明细表 `bfe_ai_request_log`

```sql
CREATE TABLE bfe_ai_request_log (
    hostid                  VARCHAR(256)    COMMENT '主机标识，格式 hostname_netns',
    log_time                DATETIME        COMMENT '日志产生时间',
    ai_apikey_id               VARCHAR(256)    COMMENT 'API Key',
    ai_requested_model      VARCHAR(128)    COMMENT '请求模型名',

    logid                   BIGINT          COMMENT 'BFE 请求唯一标识',
    product                 VARCHAR(64)     COMMENT '产品标识',

    -- 客户端连接
    client_ip               VARCHAR(64)     COMMENT '客户端 IP',
    is_trust_src_ip         TINYINT         COMMENT '是否可信源 IP',

    -- 错误信息
    err_code                VARCHAR(64)     COMMENT '错误码',
    err_msg                 VARCHAR(512)    COMMENT '错误详情',

    -- 请求头
    proto                   VARCHAR(16)     COMMENT 'HTTP 协议版本',
    header_host             VARCHAR(256)    COMMENT '请求 Host',
    origin_uri              VARCHAR(2048)   COMMENT '原始请求 URI',
    final_uri               VARCHAR(2048)   COMMENT '最终路由 URI',
    method                  VARCHAR(16)     COMMENT 'HTTP 方法',
    content_type            VARCHAR(128)    COMMENT '请求 Content-Type',
    x_forward_for           VARCHAR(1024)   COMMENT 'X-Forwarded-For',
    accept_language         VARCHAR(256)    COMMENT 'Accept-Language',
    authorization           VARCHAR(1024)   COMMENT 'Authorization 头',
    transfer_encoding       VARCHAR(64)     COMMENT 'Transfer-Encoding',
    req_header_len          INT             COMMENT '请求头长度（字节）',
    req_body_len            INT             COMMENT '请求体长度（字节）',

    -- 路由
    cluster                 VARCHAR(256)    COMMENT '目标集群',
    sub_cluster             VARCHAR(256)    COMMENT '目标子集群',
    backend_info            VARCHAR(256)    COMMENT '后端 IP:Port',
    backend_retry           TINYINT         COMMENT '后端重试次数',

    -- 响应
    res_status_code         SMALLINT        COMMENT '响应状态码',
    res_header_len          INT             COMMENT '响应头长度（字节）',
    res_body_len            INT             COMMENT '响应体长度（字节）',
    res_content_type        VARCHAR(128)    COMMENT '响应 Content-Type',

    -- 耗时（毫秒）
    all_time                INT             COMMENT '请求总耗时',
    read_client_time        INT             COMMENT '读客户端耗时',
    cluster_serve_time      INT             COMMENT '集群层耗时',
    backend_serve_time      INT             COMMENT '后端耗时',
    write_client_time       INT             COMMENT '写客户端耗时',
    connect_backend_time    INT             COMMENT '连接后端耗时',
    proxy_delay_time        INT             COMMENT '代理延迟',

    -- AI 可观测
    ai_apikey_idtags           ARRAY<STRUCT<
        tagname  : VARCHAR(128),
        tagvalue : VARCHAR(128)
    >>                                      COMMENT 'API Key 标签列表',
    ai_target_model         VARCHAR(128)    COMMENT '实际路由模型名',
    ai_stream               TINYINT         COMMENT '是否流式：0=非流式, 1=流式',
    ai_input_tokens        BIGINT          COMMENT '输入 Token 数',
    ai_output_tokens        BIGINT          COMMENT '输出 Token 数',
    ai_total_tokens         BIGINT          COMMENT '总 Token 数',
    ai_ttft_us              BIGINT          COMMENT '首 Token 延迟 TTFT（微秒）',
    ai_tpot_us              BIGINT          COMMENT '每 Token 延迟 TPOT（微秒）',
    ai_rate_limit_hits      ARRAY<STRUCT<
        rate_limit_policy_id : VARCHAR(128),
        rate_limit_type      : VARCHAR(32),
        rule_names           : ARRAY<VARCHAR(128)>
    >>                                      COMMENT '限流命中列表',
    ai_auth_reject_reason   VARCHAR(256)    COMMENT '认证拒绝原因',
    ai_auth_reject_quota_plans ARRAY<VARCHAR(128)> COMMENT '被拒绝的配额计划'
)
UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model)
PARTITION BY RANGE(log_time) (
    PARTITION p_init VALUES LESS THAN ('2026-07-10')
)
DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "compression" = "zstd"
);
```

### 5.3. Routine Load 消费 Kafka

Routine Load 将 Kafka 消息持续写入明细表：

```sql
CREATE ROUTINE LOAD bfe_ai_log_load ON bfe_ai_request_log
COLUMNS(
    logid,
    timestamp,
    log_time         = FROM_UNIXTIME(timestamp),
    product,
    hostid,
    client_ip,
    is_trust_src_ip,
    err_code,
    err_msg,
    req_header_len,
    req_body_len,
    proto,
    header_host,
    origin_uri,
    final_uri,
    method,
    content_type,
    x_forward_for,
    accept_language,
    authorization,
    transfer_encoding,
    cluster,
    sub_cluster,
    backend_info,
    backend_retry,
    res_status_code,
    res_header_len,
    res_body_len,
    res_content_type,
    all_time,
    read_client_time,
    cluster_serve_time,
    backend_serve_time,
    write_client_time,
    connect_backend_time,
    proxy_delay_time,
    ai_apikey_id,
    ai_apikey_idtags,
    ai_requested_model,
    ai_target_model,
    ai_stream,
    ai_input_tokens,
    ai_output_tokens,
    ai_total_tokens,
    ai_ttft_us,
    ai_tpot_us,
    ai_rate_limit_hits,
    ai_auth_reject_reason,
    ai_auth_reject_quota_plans
)
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_batch_rows" = "250000",
    "max_error_number" = "1000",
    "format" = "json"
)
FROM KAFKA (
    "kafka_broker_list" = "172.18.1.244:9092",
    "kafka_topic" = "bfe_ai_log",
    "property.group.id" = "doris_bfe_ai_log",
    "property.client.id" = "doris_bfe_ai_log"
);
```

> **关键列映射**：`log_time = FROM_UNIXTIME(timestamp)` 将 Kafka 中的 Unix 秒级时间戳转换为 DATETIME 类型。

### 5.4. 聚合表 `bfe_ai_metrics_1m`（示例）

> **⚠️ 此表仅为演示用途**。详见 [第 6 节](#6-重要提示关于聚合表设计)。

```sql
CREATE TABLE bfe_ai_metrics_1m (
    ts_min             DATETIME        COMMENT '分钟时间桶',
    hostid             VARCHAR(256)    COMMENT '主机标识',
    ai_apikey_id          VARCHAR(128)    COMMENT 'API Key',
    ai_requested_model VARCHAR(128)    COMMENT '请求模型',
    ai_target_model    VARCHAR(128)    COMMENT '路由模型',
    ai_stream          TINYINT         COMMENT '流式标识',
    product            VARCHAR(64)     COMMENT '产品线',
    cluster            VARCHAR(64)     COMMENT '集群',
    sub_cluster        VARCHAR(64)     COMMENT '子集群',
    backend_info       VARCHAR(256)    COMMENT '后端节点',
    method             VARCHAR(16)     COMMENT 'HTTP 方法',
    res_status_code    SMALLINT        COMMENT '响应状态码',
    err_code           VARCHAR(64)     COMMENT '错误码',
    header_host        VARCHAR(256)    COMMENT '请求 Host',
    tagslot1name       VARCHAR(128)    COMMENT '标签槽位1: tagname',
    tagslot1value      VARCHAR(128)    COMMENT '标签槽位1: tagvalue',
    tagslot2name       VARCHAR(128)    COMMENT '标签槽位2: tagname',
    tagslot2value      VARCHAR(128)    COMMENT '标签槽位2: tagvalue',
    tagslot3name       VARCHAR(128)    COMMENT '标签槽位3: tagname',
    tagslot3value      VARCHAR(128)    COMMENT '标签槽位3: tagvalue',
    tagslot4name       VARCHAR(128)    COMMENT '标签槽位4: tagname',
    tagslot4value      VARCHAR(128)    COMMENT '标签槽位4: tagvalue',
    tagslot5name       VARCHAR(128)    COMMENT '标签槽位5: tagname',
    tagslot5value      VARCHAR(128)    COMMENT '标签槽位5: tagvalue',
    rate_limit_policy_id VARCHAR(128)  COMMENT '限流策略ID',
    rate_limit_type   VARCHAR(32)     COMMENT '限流类型',
    rate_limit_rule_name VARCHAR(128)  COMMENT '限流规则名',
    ai_auth_reject_reason VARCHAR(256) COMMENT '认证拒绝原因',
    ai_auth_reject_quota_plans_slot1 VARCHAR(128) COMMENT '被拒绝配额计划槽位1',
    ai_auth_reject_quota_plans_slot2 VARCHAR(128) COMMENT '被拒绝配额计划槽位2',
    ai_auth_reject_quota_plans_slot3 VARCHAR(128) COMMENT '被拒绝配额计划槽位3',
    ai_auth_reject_quota_plans_slot4 VARCHAR(128) COMMENT '被拒绝配额计划槽位4',
    ai_auth_reject_quota_plans_slot5 VARCHAR(128) COMMENT '被拒绝配额计划槽位5',

    -- 聚合指标（SUM）
    request_count      BIGINT   SUM    COMMENT '请求数',
    error_count        BIGINT   SUM    COMMENT '错误数',
    auth_reject_count  BIGINT   SUM    COMMENT '认证拒绝数',
    prompt_tokens      BIGINT   SUM    COMMENT '输入 Token 累计',
    output_tokens      BIGINT   SUM    COMMENT '输出 Token 累计',
    total_tokens       BIGINT   SUM    COMMENT '总 Token 累计',
    ttft_us_sum        BIGINT   SUM    COMMENT 'TTFT 累计（微秒）',
    tpot_us_sum        BIGINT   SUM    COMMENT 'TPOT 累计（微秒）',
    req_header_bytes   BIGINT   SUM    COMMENT '请求头字节累计',
    req_body_bytes     BIGINT   SUM    COMMENT '请求体字节累计',
    res_header_bytes   BIGINT   SUM    COMMENT '响应头字节累计',
    res_body_bytes     BIGINT   SUM    COMMENT '响应体字节累计',
    rate_limit_hits    BIGINT   SUM    COMMENT '限流命中次数',
    backend_retries    BIGINT   SUM    COMMENT '后端重试总次数',
    all_time_sum       BIGINT   SUM    COMMENT '总耗时累计（毫秒）',
    cluster_serve_sum  BIGINT   SUM    COMMENT '集群层耗时累计',
    backend_serve_sum  BIGINT   SUM    COMMENT '后端耗时累计'
)
AGGREGATE KEY(ts_min, hostid, ai_apikey_id, ai_requested_model, ai_target_model, ai_stream,
              product, cluster, sub_cluster, backend_info, method, res_status_code,
              err_code, header_host,
              tagslot1name, tagslot1value, tagslot2name, tagslot2value,
              tagslot3name, tagslot3value, tagslot4name, tagslot4value,
              tagslot5name, tagslot5value,
              rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
              ai_auth_reject_reason,
              ai_auth_reject_quota_plans_slot1, ai_auth_reject_quota_plans_slot2,
              ai_auth_reject_quota_plans_slot3, ai_auth_reject_quota_plans_slot4,
              ai_auth_reject_quota_plans_slot5)
PARTITION BY RANGE(ts_min) (
    PARTITION p_init VALUES LESS THAN ('2026-07-10')
)
DISTRIBUTED BY HASH(ai_apikey_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "compression" = "zstd"
);
```

### 5.5. INSERT JOB 定时聚合

通过 Doris INSERT JOB 每分钟从明细表增量写入聚合表：

```sql
CREATE JOB bfe_ai_metrics_1m_job
ON SCHEDULE EVERY 1 MINUTE
DO
INSERT INTO bfe_ai_metrics_1m
SELECT
    DATE_TRUNC(log_time, 'minute')       AS ts_min,
    COALESCE(hostid, '')                 AS hostid,
    COALESCE(ai_apikey_id, '')              AS ai_apikey_id,
    COALESCE(ai_requested_model, '')     AS ai_requested_model,
    COALESCE(ai_target_model, '')        AS ai_target_model,
    COALESCE(ai_stream, 0)               AS ai_stream,
    COALESCE(product, '')                AS product,
    COALESCE(cluster, '')                AS cluster,
    COALESCE(sub_cluster, '')            AS sub_cluster,
    COALESCE(backend_info, '')           AS backend_info,
    COALESCE(method, '')                 AS method,
    COALESCE(res_status_code, 0)         AS res_status_code,
    COALESCE(err_code, '')               AS err_code,
    COALESCE(header_host, '')            AS header_host,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 1).tagname, '')  AS tagslot1name,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 1).tagvalue, '') AS tagslot1value,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 2).tagname, '')  AS tagslot2name,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 2).tagvalue, '') AS tagslot2value,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 3).tagname, '')  AS tagslot3name,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 3).tagvalue, '') AS tagslot3value,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 4).tagname, '')  AS tagslot4name,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 4).tagvalue, '') AS tagslot4value,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 5).tagname, '')  AS tagslot5name,
    COALESCE(ELEMENT_AT(ai_apikey_idtags, 5).tagvalue, '') AS tagslot5value,
    COALESCE(ELEMENT_AT(ai_rate_limit_hits, 1).rate_limit_policy_id, '') AS rate_limit_policy_id,
    COALESCE(ELEMENT_AT(ai_rate_limit_hits, 1).rate_limit_type, '')      AS rate_limit_type,
    COALESCE(ELEMENT_AT(ELEMENT_AT(ai_rate_limit_hits, 1).rule_names, 1), '') AS rate_limit_rule_name,
    COALESCE(ai_auth_reject_reason, '')  AS ai_auth_reject_reason,
    COALESCE(ELEMENT_AT(ai_auth_reject_quota_plans, 1), '') AS ai_auth_reject_quota_plans_slot1,
    COALESCE(ELEMENT_AT(ai_auth_reject_quota_plans, 2), '') AS ai_auth_reject_quota_plans_slot2,
    COALESCE(ELEMENT_AT(ai_auth_reject_quota_plans, 3), '') AS ai_auth_reject_quota_plans_slot3,
    COALESCE(ELEMENT_AT(ai_auth_reject_quota_plans, 4), '') AS ai_auth_reject_quota_plans_slot4,
    COALESCE(ELEMENT_AT(ai_auth_reject_quota_plans, 5), '') AS ai_auth_reject_quota_plans_slot5,
    -- metrics
    COUNT(1)                             AS request_count,
    SUM(CASE WHEN err_code != '' AND err_code IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN ai_auth_reject_reason != '' AND ai_auth_reject_reason IS NOT NULL THEN 1 ELSE 0 END) AS auth_reject_count,
    SUM(COALESCE(ai_input_tokens, 0))   AS prompt_tokens,
    SUM(COALESCE(ai_output_tokens, 0))   AS output_tokens,
    SUM(COALESCE(ai_total_tokens, 0))    AS total_tokens,
    SUM(COALESCE(ai_ttft_us, 0))         AS ttft_us_sum,
    SUM(COALESCE(ai_tpot_us, 0))         AS tpot_us_sum,
    SUM(COALESCE(req_header_len, 0))     AS req_header_bytes,
    SUM(COALESCE(req_body_len, 0))       AS req_body_bytes,
    SUM(COALESCE(res_header_len, 0))     AS res_header_bytes,
    SUM(COALESCE(res_body_len, 0))       AS res_body_bytes,
    SUM(CASE WHEN ARRAY_SIZE(ai_rate_limit_hits) > 0 THEN 1 ELSE 0 END) AS rate_limit_hits,
    SUM(COALESCE(backend_retry, 0))      AS backend_retries,
    SUM(COALESCE(all_time, 0))           AS all_time_sum,
    SUM(COALESCE(cluster_serve_time, 0)) AS cluster_serve_sum,
    SUM(COALESCE(backend_serve_time, 0)) AS backend_serve_sum
FROM bfe_ai_request_log
WHERE log_time >= DATE_TRUNC(NOW(), 'minute') - INTERVAL 1 MINUTE
  AND log_time <  DATE_TRUNC(NOW(), 'minute')
GROUP BY ts_min, hostid, ai_apikey_id, ai_requested_model, ai_target_model, ai_stream,
         product, cluster, sub_cluster, backend_info, method, res_status_code,
         err_code, header_host,
         tagslot1name, tagslot1value, tagslot2name, tagslot2value,
         tagslot3name, tagslot3value, tagslot4name, tagslot4value,
         tagslot5name, tagslot5value,
         rate_limit_policy_id, rate_limit_type, rate_limit_rule_name,
         ai_auth_reject_reason,
         ai_auth_reject_quota_plans_slot1, ai_auth_reject_quota_plans_slot2,
         ai_auth_reject_quota_plans_slot3, ai_auth_reject_quota_plans_slot4,
         ai_auth_reject_quota_plans_slot5;
```

### 5.6. 数据流延迟

| 环节 | 延迟 | 说明 |
|------|------|------|
| BFE → PB 日志文件 | < 1s | 请求结束时写入 |
| LogReader tail → Kafka | < 1s | 近实时 tail |
| Kafka → Doris Routine Load | 1~5s | 批量提交间隔 |
| 明细表 → 聚合表 | ≤ 60s | INSERT JOB 每分钟执行 |
| **端到端总延迟** | **< 1 分钟** | 满足分钟级看板需求 |

---

## 6. 第四步：Grafana 集成

### 6.1. 数据源配置

Grafana 通过 **MySQL 数据源**连接 Doris FE（Doris 兼容 MySQL 协议）：

| 配置项 | 值 |
|--------|-----|
| Data source type | MySQL |
| Host | `<doris_fe_host>:9030` |
| Database | `bfe_observability` |
| Username / Password | Doris 用户凭证 |

> 操作路径：Grafana → Configuration → Data Sources → Add data source → MySQL

### 6.2. Dashboard 变量配置

在 Dashboard Settings → Variables 中定义变量，实现面板联动筛选：

| 变量名 | 类型 | 标签 | 查询 SQL |
|--------|------|------|----------|
| `model` | Query | 模型 | `SELECT DISTINCT ai_target_model FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR` |
| `hostid` | Query | 主机 | `SELECT DISTINCT hostid FROM bfe_ai_request_log WHERE log_time >= NOW() - INTERVAL 1 HOUR` |
| `apikey` | Query | API Key | `SELECT DISTINCT ai_apikey_id FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR` |
| `cluster` | Query | 集群 | `SELECT DISTINCT cluster FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR` |
| `product` | Query | 产品 | `SELECT DISTINCT product FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR` |
| `host` | Query | Host | `SELECT DISTINCT header_host FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 1 HOUR AND header_host != ''` |
| `uri` | TextBox | URI 搜索 | （文本框，手动输入模糊匹配） |

> `model` 和 `apikey` 建议开启 Multi-value 和 Include All option。

### 6.3. Dashboard 面板 SQL 示例

#### 看板布局

```
┌──────────────────────────────────────────────────────────────────┐
│  Row 1: 全局概览（聚合表）                                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ QPS      │ │ 错误率    │ │ TPM      │ │ 限流命中  │ │认证拒绝 │ │
│  │ (折线图)  │ │ (折线图)  │ │ (折线图)  │ │ (折线图)  │ │(折线图) │ │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │
├──────────────────────────────────────────────────────────────────┤
│  Row 2: 延迟分布（明细表，分位数计算）                                │
│  ┌──────────────────────┐ ┌──────────────────────┐               │
│  │ P50/P90/P99 全链路   │ │ TTFT 分位延迟         │               │
│  │ (折线图，多系列)      │ │ (折线图，仅流式)      │               │
│  └──────────────────────┘ └──────────────────────┘               │
├──────────────────────────────────────────────────────────────────┤
│  Row 3: Token 与耗时（聚合表）                                      │
│  ┌────────────────────────────┐ ┌────────────────────────────┐   │
│  │ 各阶段耗时堆叠              │ │ Token 吞吐（TPM）          │   │
│  │ read/proxy/backend/write   │ │ prompt / output / total     │   │
│  │ (堆叠柱状图)                │ │ (堆叠折线图)               │   │
│  └────────────────────────────┘ └────────────────────────────┘   │
├──────────────────────────────────────────────────────────────────┤
│  Row 4: 维度下钻（聚合表）                                         │
│  ┌──────────────────────┐ ┌──────────────────────┐               │
│  │ 按模型 QPS Top N      │ │ 按 apikey Token 消耗  │               │
│  │ (表格)                │ │ (柱状图)              │               │
│  └──────────────────────┘ └──────────────────────┘               │
├──────────────────────────────────────────────────────────────────┤
│  Row 5: 明细回溯（明细表）                                         │
│  ┌──────────────────────────────────────────────────┐            │
│  │ 日志搜索：按 logid / hostid / apikey / uri / host 过滤         │            │
│  │ (Table panel)                                      │            │
│  └──────────────────────────────────────────────────┘            │
│  ┌──────────────────────────────────────────────────┐            │
│  │ 限流命中展开 (UNNEST) / 认证拒绝展开 (UNNEST)       │            │
│  │ (Table panel)                                      │            │
│  └──────────────────────────────────────────────────┘            │
└──────────────────────────────────────────────────────────────────┘
```

#### QPS 折线图（聚合表）

```sql
SELECT
  ts_min AS time,
  SUM(request_count) / 60 AS qps
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ai_target_model IN ($model)
  AND product IN ($product)
GROUP BY ts_min
ORDER BY ts_min;
```

#### 错误率折线图（聚合表）

```sql
SELECT
  ts_min AS time,
  SUM(error_count) / SUM(request_count) AS error_rate
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ai_target_model IN ($model)
GROUP BY ts_min
ORDER BY ts_min;
```

#### TPM 折线图（聚合表）

```sql
SELECT
  ts_min AS time,
  SUM(total_tokens) AS tokens_per_minute
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
  AND ai_target_model IN ($model)
GROUP BY ts_min
ORDER BY ts_min;
```

#### P50/P90/P99 全链路延迟（明细表）

```sql
SELECT
  $__timeGroup(log_time, '1m') AS time,
  PERCENTILE_APPROX(all_time, 0.50) AS p50,
  PERCENTILE_APPROX(all_time, 0.90) AS p90,
  PERCENTILE_APPROX(all_time, 0.99) AS p99
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ai_target_model IN ($model)
  AND all_time IS NOT NULL
GROUP BY time
ORDER BY time;
```

#### TTFT 分位延迟（明细表，微秒→毫秒，仅流式）

```sql
SELECT
  $__timeGroup(log_time, '1m') AS time,
  PERCENTILE_APPROX(ai_ttft_us, 0.50) / 1000 AS ttft_p50_ms,
  PERCENTILE_APPROX(ai_ttft_us, 0.90) / 1000 AS ttft_p90_ms,
  PERCENTILE_APPROX(ai_ttft_us, 0.99) / 1000 AS ttft_p99_ms
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ai_stream = 1
  AND ai_target_model IN ($model)
  AND ai_ttft_us IS NOT NULL
GROUP BY time
ORDER BY time;
```

#### 按模型 QPS Top N（聚合表）

```sql
SELECT
  ai_target_model AS model,
  SUM(request_count) / (($__timeTo() - $__timeFrom()) / 1000) AS avg_qps
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
GROUP BY ai_target_model
ORDER BY avg_qps DESC
LIMIT 10;
```

#### 按 apikey Token 消耗排行（聚合表）

```sql
SELECT
  ai_apikey_id,
  SUM(total_tokens) AS total_tokens,
  SUM(request_count) AS requests
FROM bfe_ai_metrics_1m
WHERE ts_min >= $__timeFrom() AND ts_min < $__timeTo()
GROUP BY ai_apikey_id
ORDER BY total_tokens DESC
LIMIT 20;
```

#### 明细日志回溯（明细表）

```sql
SELECT
  log_time, logid, hostid, ai_apikey_id, ai_requested_model, ai_target_model,
  ai_stream, ai_total_tokens, all_time, res_status_code, err_code, err_msg,
  header_host, origin_uri, method, client_ip, res_content_type,
  ai_rate_limit_hits, ai_auth_reject_reason
FROM bfe_ai_request_log
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND (ai_apikey_id = '$apikey' OR '$apikey' = '')
  AND (ai_target_model IN ($model) OR '$model' = '')
  AND (origin_uri LIKE '%$uri%' OR '$uri' = '')
ORDER BY log_time DESC
LIMIT 100;
```

#### 限流命中展开（明细表，UNNEST）

```sql
SELECT
  log_time, logid, hostid, ai_apikey_id, ai_target_model, origin_uri,
  hit.rate_limit_policy_id, hit.rate_limit_type, hit.rule_names
FROM bfe_ai_request_log
CROSS JOIN UNNEST(ai_rate_limit_hits) AS hit
WHERE log_time >= $__timeFrom() AND log_time < $__timeTo()
  AND ARRAY_SIZE(ai_rate_limit_hits) > 0
ORDER BY log_time DESC
LIMIT 100;
```

### 6.4. 告警规则

Grafana 8.0+ 内置告警引擎，支持对 MySQL 数据源的查询结果设置告警规则。

| 告警规则 | 数据源 | 评估间隔 | 条件 | 持续时长 |
|---------|--------|---------|------|---------|
| 错误率过高 | 聚合表 | 1m | `error_rate > 0.05` | 3m |
| 认证拒绝率过高 | 聚合表 | 1m | `auth_reject_rate > 0.1` | 3m |
| P99 延迟过高 | 明细表 | 1m | `p99 > 30000`（30s） | 3m |
| TTFT 延迟过高 | 明细表 | 1m | `ttft_p99_ms > 10000`（10s） | 3m |
| 限流命中率异常 | 聚合表 | 1m | `hit_rate > 0.1` | 5m |
| QPS 突降 | 聚合表 | 1m | 下降 > 50% | 3m |

**错误率告警 SQL 示例**：

```sql
SELECT
  ts_min AS time,
  SUM(error_count) / SUM(request_count) AS error_rate
FROM bfe_ai_metrics_1m
WHERE ts_min >= NOW() - INTERVAL 5 MINUTE
  AND ai_target_model IN ($model)
GROUP BY ts_min
ORDER BY ts_min;
```

**P99 延迟告警 SQL 示例**：

```sql
SELECT
  $__timeGroup(log_time, '1m') AS time,
  PERCENTILE_APPROX(all_time, 0.99) AS p99
FROM bfe_ai_request_log
WHERE log_time >= NOW() - INTERVAL 5 MINUTE
  AND ai_target_model IN ($model)
  AND all_time IS NOT NULL
GROUP BY time
ORDER BY time;
```

---

## 7. ⚠️ 重要提示：关于聚合表设计

> **本节是本文档最重要的部分，在实际业务场景中，请在参考示例后重新设计聚合表。**

### 7.1. 示例聚合表 `bfe_ai_metrics_1m` 的问题

本示例中的聚合表 `bfe_ai_metrics_1m` 包含 **35 个维度列**（AGGREGATE KEY），存在以下潜在问题：

#### 问题一：维度过多导致稀疏表

| 维度 | 问题 |
|------|------|
| `ai_apikey_id` | 最高基数维度，百万级用户，每个用户一个唯一值 |
| `backend_info` | IP:Port 级别，每个后端实例一个值，高基数且频繁变更 |
| `hostid` | 每台 BFE 实例一个值，数百~数千 |
| `header_host` | 域名级别，数百~数千 |
| `tagslot1~5name/value` | 10 列自定义标签，维度可能很高 |
| `err_code` | 仅错误请求有值，99% 请求为空串 |
| `rate_limit_policy_id/type/rule_name` | 仅限流命中时有值，99% 请求为空 |
| `ai_auth_reject_reason/quota_plans_slot1~5` | 仅认证拒绝时有值，99% 请求为空 |

**后果**：高基数维度（如 apikey、backend_info）与稀疏维度（如 err_code、rate_limit_*）产生笛卡尔积组合，导致聚合表行数远超预期，甚至接近明细表。

#### 问题二：1 分钟聚合在 LLM 场景下不合理

LLM 请求耗时特征：
- 非流式请求：10~30 秒
- 流式请求：30~60 秒甚至更长

在 1 分钟内，单个 `(apikey, model)` 组合可能只有 1~2 个请求。大量维度组合的 `request_count = 1`，聚合失去意义。

### 7.2. 合理的聚合表设计建议

#### 原则

1. **按查询场景拆分聚合表**：限流看板不会同时查 Token 用量，认证拒绝看板不会同时查 TTFT
2. **稀疏维度独立成表**：错误码、限流、认证拒绝等维度只在少数请求中出现，应从主表中分离
3. **基础设施维度独立**：`hostid`、`backend_info` 是运维视角，不应与 apikey/model 交叉
4. **聚合粒度适配业务特征**：请根据具体的LLM 场景，确定时间间隔，建议使用 5 分钟或 15 分钟粒度

#### 可能的粗维度聚合表

| 表名 | 粒度 | 维度数 | 用途 |
|------|------|:---:|------|
| `bfe_ai_metrics_core_5m` | 5 分钟 | ~10 | 核心流量：QPS、Token、延迟 |
| `bfe_ai_metrics_error_5m` | 5 分钟 | ~5 | 错误码分布（仅错误请求） |
| `bfe_ai_metrics_ratelimit_5m` | 5 分钟 | ~6 | 限流命中（仅限流请求） |
| `bfe_ai_metrics_auth_5m` | 5 分钟 | ~5 | 认证拒绝（仅拒绝请求） |
| `bfe_ai_metrics_host_5m` | 5 分钟 | ~3 | 主机级别运维（无 apikey 维度） |
| `...` | 5 分钟 | ... | 其它业务需求的聚合 |


### 7.3. 总结

| 维度 | 示例聚合表 | 建议设计 |
|------|----------|---------|
| 维度列数 | 35 | 3~10（按表） |
| 聚合粒度 | 1 分钟 | 5 分钟 |
| 表数量 | 1 | 按场景拆） |
| 稀疏维度处理 | 混在主表（空值） | 独立错误/限流/认证表/... |
| 聚合比 | 可能接近 1:1 | 可达 1:50 ~ 1:300 |

**请务必根据实际业务需求重新设计聚合表**，示例表仅用于演示如何打通 LogReader → Kafka → Doris → Grafana 的完整链路。

---

## 8. 附录：验证清单

完成以上配置后，请按以下顺序验证各环节：

| 序号 | 验证项 | 方法 |
|:---:|--------|------|
| 1 | LogReader 是否正常发送 | 检查 LogReader 日志，确认无 Kafka 发送错误 |
| 2 | Kafka 是否收到消息 | `kafka-console-consumer.sh --bootstrap-server 172.18.1.244:9092 --topic bfe_ai_log --max-messages 1` |
| 3 | Routine Load 是否运行 | `SHOW ROUTINE LOAD FOR bfe_ai_log_load\G` |
| 4 | 明细表是否有数据 | `SELECT COUNT(*) FROM bfe_ai_request_log WHERE log_time >= NOW() - INTERVAL 5 MINUTE` |
| 5 | INSERT JOB 是否运行 | `SHOW JOB FROM bfe_observability` |
| 6 | 聚合表是否有数据 | `SELECT COUNT(*) FROM bfe_ai_metrics_1m WHERE ts_min >= NOW() - INTERVAL 5 MINUTE` |
| 7 | Grafana 数据源是否连通 | Grafana → Data Sources → Save & Test |
| 8 | Dashboard 面板是否显示 | 选择最近 15 分钟时间范围，确认面板有数据 |

