# mod_kafka

## 模块简介

`mod_kafka` 是 `log-reader` 的 Kafka 输出模块。它负责消费解析后的 BFE 访问日志（`bfe-access-pb` protobuf 格式），根据配置的字段集合将日志转换为 JSON，并批量发送到 Kafka。

主要功能包括：

- 消费 BFE 访问日志并按配置的字段集合输出 JSON
- 将 JSON 日志批量发送到 Kafka

## 基础配置

模块基础配置文件说明详见 [mod_kafka.conf](../../configuration/mod_kafka/mod_kafka.conf.md)；输出字段集合通过数据配置文件 [kafka_config.data](../../configuration/mod_kafka/kafka_config.data.md) 控制。

## 输出字段

`mod_kafka` 支持将 BFE 访问日志中的以下类别字段输出为 JSON。完整字段说明（含类型、是否必需、是否默认输出、复合对象结构示例）参见 [output-fields.md](./output-fields.md)。

| 字段类别 | 主要字段 |
| -------- | -------- |
| BfeLog 顶层字段 | `logid`、`timestamp`、`product`、`hostid`、`log_tag` |
| 客户端连接字段 | `client_ip`、`client_ip6`、`client_network`、`req_num`、`session_id` |
| 请求基础字段 | `err_code`、`err_msg`、`req_header_len`、`req_body_len` |
| 请求头字段 | `proto`、`header_host`、`origin_uri`、`final_uri`、`method`、`content_type`、`referrer`、`user_agent`、`x_forward_for`、`accept_language`、`authorization`、`transfer_encoding`、`delegation`、`uid` |
| Cookie 字段 | `cookie` |
| 请求头列表 | `req_headers` |
| 路由信息字段 | `cluster`、`sub_cluster`、`backend_info`、`backend_retry` |
| 响应信息字段 | `res_status_code`、`res_header_len`、`res_body_len`、`res_content_type`、`res_location`、`res_transfer_encoding` |
| 响应头列表 | `res_headers` |
| 时间信息字段 | `all_time`、`read_client_time`、`cluster_serve_time`、`backend_serve_time`、`write_client_time`、`session_offset_time`、`connect_backend_time`、`proxy_delay_time` |
| AI 可观测字段 | `ai_apikey_id`、`ai_apikeytags`、`ai_requested_model`、`ai_target_model`、`ai_stream`、`ai_input_tokens`、`ai_output_tokens`、`ai_total_tokens`、`ai_ttft_us`、`ai_tpot_us`、`ai_rate_limit_hits`、`ai_auth_reject_reason`、`ai_auth_reject_quota_plans`、`ai_provider`、`ai_retry_count`、`ai_cost_value`、`ai_cost_currency`、`ai_price_miss`、`ai_price_warn`、`ai_route_rule_hits`、`ai_cluster_key_names`、`ai_auth_hit_quota_plans` |
| 地址信息字段 | `bfe_ip`、`sock_src_ip`、`is_trust_src_ip`、`vip`、`vip6` |

字段输出模式、配置示例及监控指标详见 [mod_kafka.conf](../../configuration/mod_kafka/mod_kafka.conf.md) 与 [kafka_config.data](../../configuration/mod_kafka/kafka_config.data.md)。
