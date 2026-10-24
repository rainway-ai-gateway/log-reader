# logreader可输出各个字段说明

## 1. 概述

本文档列出了 `log_reader` 的 `mod_kafka` 模块中所有已注册的 JSON 输出字段，包括每个字段的：

- **JSON 字段名**：输出到 Kafka 消息中的 JSON key
- **类型**：Go 类型及对应的 JSON 类型
- **Required**：是否始终输出（无论何种 FieldMode 模式）
- **Default**：是否在默认字段集中（`FieldMode = default` 时输出）
- **说明**：字段含义及数据来源

---

## 2. 字段模式说明

通过 `kafka_config.data` 中的 `FieldMode` 控制输出字段：

| 模式 | 说明 |
|------|------|
| `require` | 仅输出 Required 字段 |
| `default` | 输出 Default 字段集 |
| `all` | 输出所有已注册字段 |
| `customized` | 输出 Required 字段 + `FieldNames` 中指定字段的并集 |

> **注意**：无论选择何种模式，Required 字段始终输出。

---

## 3. 字段完整列表

### 3.1. BfeLog 顶层字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `logid` | uint64 | ✅ | ✅ | 请求唯一标识，BFE 为每个请求分配的唯一 ID |
| `timestamp` | uint64 | ✅ | ✅ | 请求时间戳（Unix 秒） |
| `product` | string | ✅ | ✅ | 产品标识，优先取 `RequestLog.Product`，为空时取 `BfeLog.Product` |
| `log_tag` | string | ❌ | ✅ | 日志分类标签，用于区分正常/错误日志 |
| `hostid` | string | ✅ | ✅ | log reader 的hostid(由 hostname_netns 组成) |

### 3.2. 客户端连接字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `client_ip` | string | ✅ | ✅ | 客户端 IP 地址（IPv4 点分十进制或 IPv6 字符串） |
| `client_network` | string | ❌ | ✅ | 客户端网络类型（IPv4 / IPv6） |
| `req_num` | uint32 | ❌ | ✅ | 连接上的请求序号 |
| `session_id` | uint64 | ❌ | ✅ | 会话 ID |

### 3.3. 请求基础字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `err_code` | string | ✅ | ✅ | 错误码，正常请求为空 |
| `err_msg` | string | ✅ | ✅ | 错误详情 |
| `req_header_len` | uint32 | ✅ | ✅ | 请求头长度（字节） |
| `req_body_len` | uint32 | ✅ | ✅ | 请求体长度（字节） |

### 3.4. 请求头字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `proto` | string | ✅ | ✅ | HTTP 协议版本（如 `HTTP/1.1`、`HTTP/2`） |
| `header_host` | string | ✅ | ✅ | 请求目标 Host |
| `origin_uri` | string | ✅ | ✅ | 原始请求 URI |
| `final_uri` | string | ❌ | ✅ | 最终路由后的 URI（经重写后） |
| `method` | string | ✅ | ✅ | HTTP 方法（GET / POST / PUT / DELETE 等） |
| `content_type` | string | ❌ | ✅ | 请求 Content-Type |
| `referrer` | string | ❌ | ✅ | 请求 Referer |
| `user_agent` | string | ❌ | ✅ | 请求 User-Agent |
| `x_forward_for` | string | ❌ | ✅ | X-Forwarded-For 头 |
| `accept_language` | string | ❌ | ✅ | Accept-Language 头 |
| `authorization` | string | ❌ | ✅ | Authorization 头 |
| `transfer_encoding` | string | ❌ | ✅ | Transfer-Encoding 头 |
| `delegation` | string | ❌ | ✅ | 委托标识 |
| `uid` | string | ❌ | ✅ | 用户 ID |

### 3.5. Cookie 字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `cookie` | string | ❌ | ✅ | 请求 Cookie 原始字符串 |

### 3.6. 请求头列表

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `req_headers` | []object | ❌ | ✅ | 请求头列表，每项为 `{"key": "...", "value": "..."}` |

### 3.7. 路由信息字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `cluster` | string | ❌ | ✅ | 目标集群名称 |
| `sub_cluster` | string | ❌ | ✅ | 目标子集群名称 |
| `backend_info` | string | ❌ | ✅ | 后端实例信息，格式 `"ip:port"` |
| `backend_retry` | uint32 | ❌ | ✅ | 后端重试次数 |

### 3.8. 响应信息字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `res_status_code` | uint32 | ✅ | ✅ | 响应 HTTP 状态码 |
| `res_header_len` | uint32 | ✅ | ✅ | 响应头长度（字节） |
| `res_body_len` | uint32 | ✅ | ✅ | 响应体长度（字节） |
| `res_content_type` | string | ❌ | ✅ | 响应 Content-Type |
| `res_location` | string | ❌ | ✅ | 响应 Location 头（重定向地址） |
| `res_transfer_encoding` | string | ❌ | ✅ | 响应 Transfer-Encoding 头 |

### 3.9. 响应头列表

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `res_headers` | []object | ❌ | ✅ | 响应头列表，每项为 `{"key": "...", "value": "..."}` |

### 3.10. 时间信息字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `all_time` | uint32 | ✅ | ✅ | 请求总耗时（毫秒） |
| `read_client_time` | uint32 | ✅ | ✅ | 读取客户端请求耗时（毫秒） |
| `cluster_serve_time` | uint32 | ✅ | ✅ | 集群层处理耗时（毫秒） |
| `backend_serve_time` | uint32 | ✅ | ✅ | 后端服务处理耗时（毫秒） |
| `write_client_time` | uint32 | ✅ | ✅ | 写响应到客户端耗时（毫秒） |
| `session_offset_time` | uint32 | ❌ | ✅ | 会话偏移时间（毫秒） |
| `connect_backend_time` | uint32 | ❌ | ✅ | 连接后端耗时（毫秒） |
| `proxy_delay_time` | uint32 | ✅ | ✅ | 代理延迟时间（毫秒） |

### 3.11. AI 可观测字段

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `ai_apikey_id` | string | ❌ | ✅ | API Key 内部标识 |
| `ai_apikeytags` | object | ❌ | ✅ | API Key 标签对象，包含 `level1` ~ `level5`，每个 level 非空时为 `{"tagname": "...", "tagvalue": "..."}` |
| `ai_requested_model` | string | ❌ | ✅ | 客户端请求的模型名称 |
| `ai_target_model` | string | ❌ | ✅ | 实际路由目标模型名 |
| `ai_stream` | bool | ❌ | ✅ | 是否为流式请求 |
| `ai_input_tokens` | int64 | ❌ | ✅ | 输入 Token 数 |
| `ai_output_tokens` | int64 | ❌ | ✅ | 输出消耗 Token 数（限流时可能为 -1） |
| `ai_total_tokens` | int64 | ❌ | ✅ | 总消耗 Token 数 |
| `ai_cache_read_tokens` | int64 | ❌ | ✅ | 从 cache 读取的 Token 数 |
| `ai_cache_write_tokens` | int64 | ❌ | ✅ | 写入 cache 的 Token 数 |
| `ai_audio_input_tokens` | int64 | ❌ | ✅ | 音频输入 Token 数，已包含在 `ai_input_tokens` 中 |
| `ai_audio_output_tokens` | int64 | ❌ | ✅ | 音频输出 Token 数，已包含在 `ai_output_tokens` 中 |
| `ai_image_count` | int64 | ❌ | ✅ | 图像生成模式下生成的图像张数 |
| `ai_image_input_tokens` | int64 | ❌ | ✅ | 图片输入 Token 数，已包含在 `ai_input_tokens` 中 |
| `ai_video_count` | int64 | ❌ | ✅ | 视频生成模式下生成的视频数量 |
| `ai_ttft_us` | int64 | ❌ | ✅ | 首 Token 延迟（微秒），Time To First Token |
| `ai_tpot_us` | int64 | ❌ | ✅ | 每 Token 输出延迟（微秒），Time Per Output Token |
| `ai_rate_limit_hits` | []object | ❌ | ✅ | 限流命中记录，每项为 `{"rate_limit_policy_id": "...", "rate_limit_type": "...", "rule_names": [...]}` |
| `ai_auth_reject_reason` | string | ❌ | ✅ | 认证/鉴权拒绝原因 |
| `ai_auth_reject_quota_plans` | []string | ❌ | ✅ | 被拒绝的配额计划（quota plan）名称列表 |
| `ai_protocol` | string | ❌ | ✅ | AI 协议风格，如 `openai`、`anthropic` |
| `ai_provider` | string | ❌ | ✅ | 上游模型提供商 |
| `ai_retry_count` | uint32 | ❌ | ✅ | 模型调用层重试次数 |
| `ai_mode` | string | ❌ | ✅ | AI 请求模式，如 `chat`、`image_generation`、`embedding`、`audio_speech` 等 |
| `ai_cost_value` | int64 | ❌ | ✅ | 成本固定点整数值 |
| `ai_cost_currency` | string | ❌ | ✅ | 成本币种 |
| `ai_route_rule_hits` | []object | ❌ | ✅ | AI 路由规则命中记录 |
| `ai_cluster_key_names` | []object | ❌ | ✅ | 尝试过的 cluster 与 key 名称组合 |
| `ai_auth_hit_quota_plans` | []string | ❌ | ✅ | 成功请求时命中的配额计划名称列表 |
| `ai_cache_status` | string | ❌ | ✅ | AI 缓存状态（如 `hit`/`miss`），未启用缓存时为空串 |
| `ai_intent_question` | string | ❌ | ✅ | AI 意图识别命中的问题标识 |
| `ai_intent_answer` | string | ❌ | ✅ | AI 意图识别选中的答案选项 |
| `ai_intent_confidence` | float64 | ❌ | ✅ | 意图识别置信度（0~1）；proto optional，**未设置输出 null**（= 未求值，与 MySQL 侧 NULL 语义一致） |
| `ai_intent_source` | string | ❌ | ✅ | 意图识别来源（如 `llm`、`rule`） |
| `ai_intent_latency_us` | int64 | ❌ | ✅ | 意图识别耗时（微秒）；proto optional，**未设置输出 null**（= 未求值） |
| `ai_intent_cache_hit` | bool | ❌ | ✅ | 意图答案是否命中意图缓存；proto optional，**未设置输出 null**（= 未求值） |
| `ai_intent_questions_version` | string | ❌ | ✅ | 意图问题集版本 |
| `mirror_hit` | bool | ❌ | ✅ | 请求是否命中流量镜像 |
| `mirror_cluster` | string | ❌ | ✅ | 流量镜像目标集群名，未命中镜像时为空串 |

### 3.12. 地址信息字段（从 ConnAddrInfo 展平）

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `bfe_ip` | string | ❌ | ✅ | BFE 实例 IP 地址 |
| `sock_src_ip` | string | ❌ | ✅ | Socket 源 IP 地址 |
| `is_trust_src_ip` | bool | ❌ | ✅ | 是否为可信源 IP |
| `vip` | string | ❌ | ✅ | VIP 地址（IPv4） |
| `vip6` | string | ❌ | ✅ | VIP 地址（IPv6） |

---

## 4. 统计汇总

| 类别 | 字段数 | Required 数 | Default 数 |
|------|--------|------------|------------|
| BfeLog 顶层 | 5 | 4 | 5 |
| 客户端连接 | 4 | 1 | 4 |
| 请求基础 | 4 | 4 | 4 |
| 请求头 | 14 | 4 | 14 |
| Cookie | 1 | 0 | 1 |
| 请求头列表 | 1 | 0 | 1 |
| 路由信息 | 4 | 0 | 4 |
| 响应信息 | 6 | 3 | 6 |
| 响应头列表 | 1 | 0 | 1 |
| 时间信息 | 8 | 6 | 8 |
| AI 可观测 | 39 | 0 | 39 |
| 地址信息 | 5 | 0 | 5 |
| **总计** | **92** | **22** | **92** |

> Default 集 = 全部注册字段（2026-10-01 起）：`FieldMode=default` 的输出字段集与
> `mod_log_mysql` 的写入字段集（派生列除外）完全对齐，见
> `doc/modifications/2026-10-01-align-mod-kafka-output-with-mod-log-mysql/`。
> 需要裁剪体积的部署可用 `customized` / `require` 模式。

---

## 5. 复合对象结构说明

### 5.1. ai_apikeytags（object）

```json
{
    "level1": {
        "tagname": "dep",
        "tagvalue": "op"
    },
    "level2": {
        "tagname": "team",
        "tagvalue": "bfe"
    },
    "level3": {
        "tagname": "person",
        "tagvalue": "zhangsan"
    },
    "level4": {},
    "level5": {}
}
```

| 子字段 | 类型 | 说明 |
|--------|------|------|
| `level1` ~ `level5` | object | 各级标签对象；非空时包含 `tagname` 和 `tagvalue` |
| `tagname` | string | 标签名 |
| `tagvalue` | string | 标签值 |

### 5.2. ai_rate_limit_hits（[]object）

```json
[
    {
        "rate_limit_policy_id": "rlp-0002",
        "rate_limit_type": "tpm",
        "rule_names": ["tpm1"]
    }
]
```

| 子字段 | 类型 | 说明 |
|--------|------|------|
| `rate_limit_policy_id` | string | 限流策略 ID |
| `rate_limit_type` | string | 限流类型（如 `tpm`、`rpm`） |
| `rule_names` | []string | 命中的规则名称列表 |

### 5.3. ai_route_rule_hits（[]object）

```json
[
    {
        "rule_owner": "ak_user_a",
        "rule_owner_type": "apikey",
        "rule_name": "user_a-rule1"
    }
]
```

| 子字段 | 类型 | 说明 |
|--------|------|------|
| `rule_owner` | string | 路由表 owner |
| `rule_owner_type` | string | 路由表类型（如 `apikey`、`entity`、`global`） |
| `rule_name` | string | 规则名称 |

### 5.4. ai_cluster_key_names（[]object）

```json
[
    {"cluster_name": "cluster-a", "key_name": "key-001"}
]
```

| 子字段 | 类型 | 说明 |
|--------|------|------|
| `cluster_name` | string | 集群名称 |
| `key_name` | string | API Key 名称/标识 |

### 5.5. req_headers / res_headers（[]object）

```json
[
    {"key": "Content-Type", "value": "application/json"},
    {"key": "Authorization", "value": "Bearer sk-xxx"}
]
```

| 子字段 | 类型 | 说明 |
|--------|------|------|
| `key` | string | 头部名称 |
| `value` | string | 头部值 |

---

## 6. 配置示例

### 6.1. 默认模式（输出 Default 字段）

```ini
[ConfFields]
FieldMode = default
```

### 6.2. 自定义模式（输出 22 个 Required + 指定字段）

```ini
[ConfFields]
FieldMode = customized
FieldNames = ai_apikey_id
FieldNames = ai_apikeytags
FieldNames = ai_requested_model
FieldNames = ai_target_model
FieldNames = ai_stream
FieldNames = ai_input_tokens
FieldNames = ai_output_tokens
FieldNames = ai_total_tokens
FieldNames = ai_ttft_us
FieldNames = ai_tpot_us
FieldNames = ai_rate_limit_hits
FieldNames = ai_auth_reject_reason
FieldNames = referrer
FieldNames = user_agent
```

### 6.3. 仅必需字段

```ini
[ConfFields]
FieldMode = require
```

### 6.4. 全部字段

```ini
[ConfFields]
FieldMode = all
```

