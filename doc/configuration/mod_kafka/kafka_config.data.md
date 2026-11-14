# Kafka 输出字段配置

## 配置简介

`kafka_config.data` 是 `mod_kafka` 模块的数据配置文件，用于控制 BFE 访问日志转换为 JSON 后输出到 Kafka 的字段集合。

该文件通过 `mod_kafka.conf` 中的 `Basic.DataPath` 指定，路径相对于 `mod_kafka.conf` 所在目录。

## 配置描述

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| ConfFields.FieldMode | String | 字段输出模式 | N | 默认值 `default` | 取值须为 `require`、`default`、`all`、`customized` 之一 |
| ConfFields.FieldNames | String[] | 自定义输出字段列表 | 条件 | `FieldMode = customized` 时生效；可配置多行；未知字段会被忽略 | 必须是下文列出的有效字段名 |

### FieldMode 说明

| 模式 | 含义 |
| ---- | ---- |
| `require` | 仅输出必需字段（`logid`、`timestamp`、`product`、`hostid`） |
| `default` | 输出默认字段集（包含大部分常用字段，向后兼容） |
| `all` | 输出所有可用字段 |
| `customized` | 输出 `FieldNames` 中配置的字段 + 必需字段 |

## 可用字段列表

### 基础字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| logid | uint64 | 日志 ID | Y | Y |
| timestamp | uint64 | 时间戳 | Y | Y |
| product | string | 产品线 | Y | Y |
| hostid | string | 主机标识 | Y | Y |
| log_tag | string | 日志标签 | N | N |

### 连接 / 客户端字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| client_ip | string | 客户端 IP | Y | Y |
| client_network | string | 客户端网络类型 | N | N |
| req_num | uint32 | 请求序号 | N | N |
| session_id | uint64 | 会话 ID | N | N |

### 请求基础字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| err_code | string | 错误码 | Y | Y |
| err_msg | string | 错误信息 | Y | Y |
| req_header_len | uint32 | 请求头长度 | Y | Y |
| req_body_len | uint32 | 请求体长度 | Y | Y |

### 请求头字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| proto | string | 协议 | Y | Y |
| header_host | string | Host 头 | Y | Y |
| origin_uri | string | 原始 URI | Y | Y |
| final_uri | string | 最终 URI | N | Y |
| method | string | 请求方法 | Y | Y |
| content_type | string | Content-Type | N | Y |
| referrer | string | Referer | N | N |
| user_agent | string | User-Agent | N | N |
| x_forward_for | string | X-Forwarded-For | N | Y |
| accept_language | string | Accept-Language | N | Y |
| authorization | string | Authorization | N | Y |
| transfer_encoding | string | Transfer-Encoding | N | Y |
| delegation | string | Delegation | N | N |
| uid | string | 用户 ID | N | N |

### Cookie 字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| cookie | string | Cookie | N | N |

### 请求头列表

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| req_headers | []object | 所有请求头（键值对列表） | N | N |

### 路由字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| cluster | string | 集群名 | N | Y |
| sub_cluster | string | 子集群名 | N | Y |
| backend_info | string | 后端地址（IP:Port） | N | Y |
| backend_retry | uint32 | 后端重试次数 | N | Y |

### 响应字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| res_status_code | uint32 | 响应状态码 | Y | Y |
| res_header_len | uint32 | 响应头长度 | Y | Y |
| res_body_len | uint32 | 响应体长度 | Y | Y |
| res_content_type | string | 响应 Content-Type | N | Y |
| res_location | string | 响应 Location | N | N |
| res_transfer_encoding | string | 响应 Transfer-Encoding | N | N |

### 响应头列表

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| res_headers | []object | 所有响应头（键值对列表） | N | N |

### 耗时字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| all_time | uint32 | 总耗时 | Y | Y |
| read_client_time | uint32 | 读取客户端耗时 | Y | Y |
| cluster_serve_time | uint32 | 集群处理耗时 | Y | Y |
| backend_serve_time | uint32 | 后端服务耗时 | Y | Y |
| write_client_time | uint32 | 写入客户端耗时 | Y | Y |
| session_offset_time | uint32 | 会话偏移耗时 | N | N |
| connect_backend_time | uint32 | 连接后端耗时 | N | Y |
| proxy_delay_time | uint32 | 代理延迟耗时 | Y | Y |

### AI 可观测字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| ai_apikey_id | string | API Key 内部标识 | N | Y |
| ai_apikeytags | object | API Key 标签对象，包含 `level1` ~ `level5` | N | Y |
| ai_requested_model | string | 请求模型 | N | Y |
| ai_target_model | string | 实际路由目标模型名 | N | Y |
| ai_stream | bool | 是否流式请求 | N | Y |
| ai_input_tokens | int64 | 输入 Token 数 | N | Y |
| ai_output_tokens | int64 | 输出 Token 数 | N | Y |
| ai_total_tokens | int64 | 总 Token 数 | N | Y |
| ai_cache_read_tokens | int64 | 从 cache 读取的 Token 数 | N | Y |
| ai_cache_write_tokens | int64 | 写入 cache 的 Token 数 | N | Y |
| ai_audio_input_tokens | int64 | 音频输入 Token 数 | N | Y |
| ai_audio_output_tokens | int64 | 音频输出 Token 数 | N | Y |
| ai_image_count | int64 | 图像生成模式下生成的图像张数 | N | Y |
| ai_image_input_tokens | int64 | 图片输入 Token 数，已包含在 `ai_input_tokens` 中 | N | Y |
| ai_video_count | int64 | 视频生成模式下生成的视频数量 | N | Y |
| ai_ttft_us | int64 | 首 Token 延迟（微秒） | N | Y |
| ai_tpot_us | int64 | 相邻 Token 生成耗时（微秒） | N | Y |
| ai_rate_limit_hits | []object | 限流命中记录 | N | Y |
| ai_auth_reject_reason | string | 认证拒绝原因 | N | Y |
| ai_auth_reject_quota_plans | []string | 认证拒绝时超限的配额计划 | N | Y |
| ai_protocol | string | AI 协议风格，如 `openai`、`anthropic` | N | Y |
| ai_provider | string | 上游模型提供商 | N | Y |
| ai_retry_count | uint32 | 模型调用层重试次数 | N | Y |
| ai_mode | string | AI 请求模式，如 `chat`、`image_generation` 等 | N | Y |
| ai_cost_value | int64 | 成本固定点整数值 | N | Y |
| ai_cost_currency | string | 成本币种 | N | Y |
| ai_price_miss | bool | 是否发生模型价格缺失 | N | Y |
| ai_price_warn | string | 价格缺失处理形态（fallback/no_entry/reject） | N | Y |
| ai_route_rule_hits | []object | AI 路由规则命中记录 | N | Y |
| ai_cluster_key_names | []object | 尝试过的 cluster 与 key 名称组合 | N | Y |
| ai_auth_hit_quota_plans | []string | 成功请求时命中的配额计划 | N | Y |

### 地址信息字段

| 字段名 | 类型 | 含义 | 必需 | 默认 |
| ------ | ---- | ---- | ---- | ---- |
| bfe_ip | string | BFE 实例 IP | N | N |
| sock_src_ip | string | 套接字源 IP | N | N |
| is_trust_src_ip | bool | 是否信任源 IP | N | Y |
| vip | string | VIP（IPv4） | N | N |
| vip6 | string | VIP（IPv6） | N | N |

## 配置示例

### 输出默认字段集

```ini
[ConfFields]
FieldMode = default
```

### 输出所有字段

```ini
[ConfFields]
FieldMode = all
```

### 自定义输出字段

```ini
[ConfFields]
FieldMode = customized
FieldNames= logid
FieldNames= timestamp
FieldNames= product
FieldNames= hostid
FieldNames= client_ip
FieldNames= err_code
FieldNames= res_status_code
FieldNames= ai_requested_model
FieldNames= ai_input_tokens
FieldNames= ai_total_tokens
```
