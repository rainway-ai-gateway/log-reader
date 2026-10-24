# mod_log_mysql 输出列说明（bfe_ai_request_log）

## 1. 概述

本文档列出 `mod_log_mysql` 写入 MySQL 明细表 `bfe_ai_request_log` 的全部 99 个列：

- **列名 / MySQL 类型**：与 Doris 明细表 `bfe_ai_request_log` 同名同列（`ARRAY<STRUCT>` 列在本表为 `JSON`）；
- **来源**：绝大多数列与 `mod_fields` 抽取字段（即 mod_kafka JSON 输出字段）**同名直通**；特殊列（`log_time`、level 打平列）在表中单独标注；
- **说明**：字段含义及数据来源。

**DDL 归属**：建表文件权威在 ai-gateway-api 仓库 `db_ddl_report_mysql.sql`（与聚合表 `bfe_ai_metrics_1m` 同文件）。`field_mapper.go` 内的列常量表是写入侧的唯一权威（决定 INSERT 列序），单测校验其与 DDL 列清单一致；本文档与 DDL 同步维护。

## 2. 通用规则

| 规则 | 说明 |
|------|------|
| 字符串零值 | 可空列：空串 `""` 写入 `NULL`（下游聚合侧 `IFNULL/COALESCE` 归一，与 Doris 聚合 JOB 的 COALESCE 口径一致）；NOT NULL 字符串列（AI 缓存/镜像/意图段的 `ai_cache_status`/`mirror_cluster`/`ai_intent_*` 字符串列）：空串原样写入 `''`（写 NULL 会触发 Error 1048） |
| 数值/布尔零值 | 原样写入（`0` 即 `0`，如 `ai_stream=0`、`ai_retry_count=0`）；例外：`ai_intent_confidence`/`ai_intent_latency_us`/`ai_intent_cache_hit` 三个可空意图数值列为 proto optional，**未设置（指针 nil）→ NULL（=未求值，与 report 查询契约一致），显式置 0 → 0** |
| JSON 列 | 抽取值为结构化值，`json.Marshal` 后写入；nil/空数组 → `NULL` |
| 日志类型 | 仅写入 `BfeLogType_Request`；会话日志不写入本表 |
| 幂等 | 唯一键 `(hostid, log_time, ai_apikey_id, ai_requested_model, logid)` 冲突覆盖（`ON DUPLICATE KEY UPDATE`）；`logid` 是请求级判别列（issue #1417），只有**同一请求重放**（同 `logid`）才折叠，故重复写入/补读重放安全 |

## 3. 列完整列表

### 3.1. 主键/基础列

| 列名 | 类型 | 来源 | 说明 |
|------|------|------|------|
| `hostid` | VARCHAR(256) | mod_fields 注入 | 主机标识，格式 `hostname_netns`；唯一键成员，NOT NULL |
| `log_time` | DATETIME | `timestamp`（Unix 秒）转换 | 日志产生时间；唯一键成员，NOT NULL |
| `ai_apikey_id` | VARCHAR(256) | 同名列 | API Key 内部 ID（不记原始 key）；唯一键成员，允许 NULL（未认证请求空串→NULL） |
| `ai_requested_model` | VARCHAR(128) | 同名列 | 客户端请求的模型名；唯一键成员，允许 NULL（空串→NULL） |
| `logid` | BIGINT | 同名列 | BFE 请求唯一标识；**唯一键成员**，`NOT NULL DEFAULT 0`。BFE 侧为十进制值（≤ `2^63−1`），与访问日志/`X-Bfe-Log-Id` 中的字符串 logid **逐值恒等**；写入侧恒写整数、不写 NULL。issue #1417 修复前该列恒 0（BFE 以十进制解析十六进制 logid 且吞掉错误），使唯一键退化为"秒 + Key + 模型"、同秒并发请求互相覆盖；修复后才真正成为判别列 |
| `product` | VARCHAR(64) | 同名列 | 产品标识（优先 `RequestLog.Product`，回退顶层枚举） |
| `log_tag` | VARCHAR(64) | 同名列 | 日志标签：`req_<product>` / `req_err_*` |

### 3.2. 客户端连接列

| 列名 | 类型 | 说明 |
|------|------|------|
| `client_ip` | VARCHAR(64) | 客户端 IP（IPv4 点分或 IPv6 字符串） |
| `client_network` | VARCHAR(16) | 客户端网络类型（Ipv4/Ipv6） |
| `is_trust_src_ip` | TINYINT | 是否可信源 IP |
| `req_num` | INT | 会话内请求序号 |
| `session_id` | BIGINT | 会话 ID |
| `bfe_ip` | VARCHAR(64) | BFE 服务器 IP |
| `sock_src_ip` | VARCHAR(64) | Socket 源 IP |
| `vip` | VARCHAR(64) | 目的 VIP（IPv4） |
| `vip6` | VARCHAR(128) | 目的 VIP6 |

### 3.3. 错误列

| 列名 | 类型 | 说明 |
|------|------|------|
| `err_code` | VARCHAR(64) | 错误码，正常请求为 NULL |
| `err_msg` | VARCHAR(512) | 错误详情 |

### 3.4. 请求列

| 列名 | 类型 | 说明 |
|------|------|------|
| `proto` | VARCHAR(16) | HTTP 协议版本 |
| `header_host` | VARCHAR(256) | 请求 Host |
| `origin_uri` | TEXT | 原始请求 URI |
| `final_uri` | TEXT | 最终路由 URI（重写后才有值） |
| `method` | VARCHAR(16) | HTTP 方法 |
| `content_type` | VARCHAR(128) | 请求 Content-Type |
| `x_forward_for` | TEXT | X-Forwarded-For |
| `accept_language` | VARCHAR(256) | Accept-Language |
| `authorization` | TEXT | Authorization 头（当前 BFE 不填充，恒 NULL） |
| `transfer_encoding` | VARCHAR(64) | Transfer-Encoding |
| `referrer` | TEXT | Referer 头 |
| `user_agent` | TEXT | User-Agent |
| `delegation` | VARCHAR(256) | 委托域名 |
| `uid` | VARCHAR(256) | UID 头 |
| `cookie` | TEXT | Cookie 头 |
| `req_headers` | JSON | 请求头列表 `[{"key","value"}]` |
| `req_header_len` | INT | 请求头长度（字节） |
| `req_body_len` | INT | 请求体长度（字节） |

### 3.5. 路由列

| 列名 | 类型 | 说明 |
|------|------|------|
| `cluster` | VARCHAR(256) | 目标集群 |
| `sub_cluster` | VARCHAR(256) | 目标子集群 |
| `backend_info` | VARCHAR(256) | 后端 `IP:Port`（重试时取最后一次） |
| `backend_retry` | TINYINT | 后端重试次数 |

### 3.6. 响应列

| 列名 | 类型 | 说明 |
|------|------|------|
| `res_status_code` | SMALLINT | 响应状态码 |
| `res_header_len` | INT | 响应头长度（字节） |
| `res_body_len` | INT | 响应体长度（字节） |
| `res_content_type` | VARCHAR(128) | 响应 Content-Type |
| `res_location` | TEXT | 响应 Location（3xx） |
| `res_transfer_encoding` | VARCHAR(64) | 响应 Transfer-Encoding |
| `res_headers` | JSON | 响应头列表 `[{"key","value"}]` |

### 3.7. 耗时列（毫秒）

| 列名 | 类型 | 说明 |
|------|------|------|
| `all_time` | INT | 请求总耗时 |
| `read_client_time` | INT | 读客户端耗时 |
| `cluster_serve_time` | INT | 集群层耗时 |
| `backend_serve_time` | INT | 后端耗时 |
| `write_client_time` | INT | 写客户端耗时 |
| `connect_backend_time` | INT | 连接后端耗时 |
| `proxy_delay_time` | INT | 代理延迟 |
| `session_offset_time` | INT | 会话内时间偏移 |

### 3.8. API Key 标签打平列（10 列）

写入时由 `ai_apikeytags` 打平（Doris 链路由 Routine Load 表达式打平，效果一致）；无标签 → NULL。

| 列名 | 类型 | 说明 |
|------|------|------|
| `level1Name` / `level1` | VARCHAR(128) | Level1 标签名 / 标签值 |
| `level2Name` / `level2` | VARCHAR(128) | Level2 标签名 / 标签值 |
| `level3Name` / `level3` | VARCHAR(128) | Level3 标签名 / 标签值 |
| `level4Name` / `level4` | VARCHAR(128) | Level4 标签名 / 标签值 |
| `level5Name` / `level5` | VARCHAR(128) | Level5 标签名 / 标签值 |

### 3.9. AI 可观测列（标量，19 列）

| 列名 | 类型 | 说明 |
|------|------|------|
| `ai_target_model` | VARCHAR(128) | 实际路由目标模型名 |
| `ai_stream` | TINYINT | 是否流式：0=非流式，1=流式 |
| `ai_input_tokens` | BIGINT | 输入 Token 数（含 cache/audio/image 输入） |
| `ai_output_tokens` | BIGINT | 输出 Token 数 |
| `ai_total_tokens` | BIGINT | 总消耗（=UsedQuota 口径） |
| `ai_cache_read_tokens` | BIGINT | cache 读 Token 数 |
| `ai_cache_write_tokens` | BIGINT | cache 写 Token 数 |
| `ai_audio_input_tokens` | BIGINT | 音频输入 Token 数 |
| `ai_audio_output_tokens` | BIGINT | 音频输出 Token 数 |
| `ai_image_count` | BIGINT | 生成图片张数 |
| `ai_ttft_us` | BIGINT | 首 Token 延迟（微秒） |
| `ai_tpot_us` | BIGINT | 平均每输出 Token 耗时（微秒） |
| `ai_provider` | VARCHAR(64) | 上游厂商（openai/anthropic/baidu/aliyun…） |
| `ai_protocol` | VARCHAR(64) | 协议风格（openai/anthropic） |
| `ai_mode` | VARCHAR(64) | 请求模式（chat/image_generation/embedding/audio_speech…） |
| `ai_retry_count` | INT | 模型调用层（key 级）重试次数 |
| `ai_cost_value` | BIGINT | 估算成本定点整数（RMB 精度 1e-8 元） |
| `ai_cost_currency` | VARCHAR(16) | 成本币种（RMB/USD） |
| `ai_auth_reject_reason` | VARCHAR(256) | 鉴权拒绝原因 |

### 3.10. AI 可观测列（JSON，5 列）

| 列名 | 类型 | 对应 mod_kafka JSON 字段 | 说明 |
|------|------|--------------------------|------|
| `ai_route_rule_hits` | JSON | `ai_route_rule_hits` | 命中路由规则 `[{"rule_owner","rule_owner_type","rule_name"}]` |
| `ai_cluster_key_names` | JSON | `ai_cluster_key_names` | 尝试过的 `[{"cluster_name","key_name"}]` |
| `ai_rate_limit_hits` | JSON | `ai_rate_limit_hits` | 限流命中 `[{"rate_limit_policy_id","rate_limit_type","rule_names":[...]}]` |
| `ai_auth_reject_quota_plans` | JSON | `ai_auth_reject_quota_plans` | 拒绝时余额不足的 Quota Plan 名称数组 |
| `ai_auth_hit_quota_plans` | JSON | `ai_auth_hit_quota_plans` | 成功请求命中的 Quota Plan 名称数组 |

JSON 列的完整结构示例见 [mod_kafka/output-fields.md](../mod_kafka/output-fields.md) 第 5 节（两个模块的字段抽取同源，结构一致）。

### 3.11. AI 缓存/镜像/意图列（10 列，2026-09-27 加列）

proto v0.3.7/3.8/3.9 字段；列序与 ai-gateway-api 权威 DDL 明细表一致。`ai_cache_key`（debug 专用防膨胀）、`mirror_status`~`mirror_error`（走 Prometheus 不回写日志）按设计不进报表库。

| 列名 | 类型 | 说明 |
|------|------|------|
| `ai_cache_status` | VARCHAR(16) | AI 缓存状态（如 `hit`/`miss`）；NOT NULL，未启用 → `''` |
| `mirror_hit` | TINYINT | 是否命中流量镜像：0/1；NOT NULL，未启用 → 0 |
| `mirror_cluster` | VARCHAR(128) | 镜像目标集群名；NOT NULL，未命中 → `''` |
| `ai_intent_question` | VARCHAR(64) | 意图识别命中的问题标识；NOT NULL，未识别 → `''` |
| `ai_intent_answer` | VARCHAR(64) | 意图识别选中的答案选项；NOT NULL，未识别 → `''` |
| `ai_intent_confidence` | DOUBLE | 意图置信度（0~1）；**NULL=未求值**，显式 0=求值为零 |
| `ai_intent_source` | VARCHAR(32) | 意图识别来源（如 `llm`/`rule`）；NOT NULL，未识别 → `''` |
| `ai_intent_latency_us` | BIGINT | 意图识别耗时（微秒）；**NULL=未求值**，显式 0=求值为零 |
| `ai_intent_cache_hit` | TINYINT | 意图答案是否命中缓存：0/1；**NULL=未求值**，显式 0=求值为零 |
| `ai_intent_questions_version` | VARCHAR(32) | 意图问题集版本；NOT NULL，未识别 → `''` |

三个可空数值列的 NULL/0 语义：mapper `kindOptionalNum` 按 proto optional 指针判空——指针 nil（未设置）写 NULL，指针非 nil（含显式置 0）写实际值，与 ai-gateway-api report 查询契约（null=未求值）对齐。

## 4. 特殊列说明

### 4.1. 幂等键（含请求级判别列 `logid`）

`hostid + log_time + ai_apikey_id + ai_requested_model + logid`：冲突时全列覆盖更新，保证 log-reader 重发、`-b` 补读、进程重启重放均不产生重复行。

**为什么必须含 `logid`（issue #1417）**：`log_time` 是秒级 `DATETIME`（见 §4.2），四列键对**同一秒**内同 Key 同模型的并发请求毫无判别力；写入侧又是全列覆盖，于是 n 个并发行只留 1 行——生产实测丢率恰为 (n−1)/n（2026-10-12 窗口 13.5%），且被覆盖行的 tokens/cost 从求和中整体消失（"以聚合可接受为前提"的旧取舍在计费对账场景不成立）。加入 `logid`（BFE 逐请求唯一、非 0）后，同秒并发请求各自成行，只有**同一请求重放**才命中覆盖。

**`logid` 为 0 的行**：`0` 参与键比较，该行退化为旧四元组语义（同秒并发仍会互相覆盖）。bfe 侧已保证非 0；本模块以 `LOGID_EMPTY` 计数器暴露这类行（正常应为 0，非 0 即需排查）。

**与 Doris 的关系**：列契约一致，**唯一键语义待同改**——Doris 明细表当前仍是四元组 `UNIQUE KEY(hostid, log_time, ai_apikey_id, ai_requested_model)`（见 `ai-gateway-observability/doris/sqls/bfe_ai_request_log.sql`），同一缺陷未修；本仓库不再宣称"与 Doris UNIQUE KEY 完全一致"。两侧口径分叉期间，对账统一以 pb3 原始日志为准。

### 4.2. log_time

由 pb 的 `timestamp`（Unix 秒）在写入组装时转为 `DATETIME`；Doris 链路则由 Routine Load 的 `FROM_UNIXTIME(timestamp)` 完成，两链路同口径。

### 4.3. pb 中已存在但本表未纳入的字段

`ai_image_input_tokens`、`ai_video_count`、`ai_cache_write_1h_tokens` 等 pb 字段（及 `client_ip6`、`BfeLog.log_type`、整个 `session_log`）不在本表——与 Doris 明细表保持一致。待 Doris 侧升级加列时，本表由 ai-gateway-api 仓库 DDL 同步加列（两侧同名同列演进）。

## 5. 索引与分区

```sql
UNIQUE KEY uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model, logid),
KEY idx_model_time (ai_target_model, log_time),
KEY idx_apikey_time (ai_apikey_id, log_time),
KEY idx_provider_time (ai_provider, log_time),
KEY idx_host_time (hostid, log_time),
KEY idx_status_time (res_status_code, log_time)
```

- 唯一键长度约 2579 字节（utf8mb4，含 `logid` BIGINT 8 字节），低于 InnoDB 3072 字节上限，无需前缀索引；唯一键必须包含分区列 `log_time`（MySQL 分区表约束）；
- 二级索引面向报表查询的典型过滤维度（模型 / Key / 厂商 / 主机 / 状态码 + 时间）；
- 分区：`PARTITION BY RANGE (TO_DAYS(log_time))`，首个分区由建表 DDL 的 `p_init` 提供；新分区创建与过期分区 DROP 由报表查询侧（ai-gateway-api）的分区管理 JOB 负责，本模块不执行任何 DDL。

## 6. 与 Doris 明细表差异对照

| 项 | Doris `bfe_ai_request_log` | MySQL `bfe_ai_request_log`（本表） |
|----|---------------------------|------------------------------------|
| 列名/列数 | 99 列 | 同名同列，99 列 |
| 复合列类型 | `ARRAY<STRUCT<...>>` / `ARRAY<VARCHAR>` | `JSON` |
| 标签打平 | Routine Load `json_extract` 表达式 | 插件写时打平 |
| log_time 生成 | Routine Load `FROM_UNIXTIME(timestamp)` | 插件写时转换 |
| 幂等 | UNIQUE KEY 模型（同键覆盖） | 唯一键 + `ON DUPLICATE KEY UPDATE` |
| 唯一键 | 四元组 `(hostid, log_time, ai_apikey_id, ai_requested_model)`——**同缺陷未修，待加 `logid`**（issue #1417） | 五元组（上述四列 + `logid`） |
| 分区 | 动态分区（start=-7，日粒度） | RANGE 分区（查询侧 JOB 管理） |

## 7. 统计汇总

| 类别 | 列数 |
|------|------|
| 主键/基础 | 7 |
| 客户端连接 | 9 |
| 错误 | 2 |
| 请求 | 18 |
| 路由 | 4 |
| 响应 | 7 |
| 耗时 | 8 |
| API Key 标签打平 | 10 |
| AI 可观测（标量） | 19 |
| AI 可观测（JSON） | 5 |
| AI 缓存/镜像/意图 | 10 |
| **总计** | **99** |