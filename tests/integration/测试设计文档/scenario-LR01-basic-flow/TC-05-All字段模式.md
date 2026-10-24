# TC-05 All 字段模式

## 用例编号与名称

TC-05 All 字段模式

## 所属场景

LR01 基本流程与 JSON 转换正确性

## 版本声明

- `log-reader`：当前源码版本

## 测试目的

验证 `FieldMode = all` 时，log-reader 输出所有已注册字段，包括不常用字段。

## 运行模式

单组件模式：仅启动真实 `log-reader` 进程，Mock Kafka 与日志生成均在测试代码中完成。

## 前置条件

1. 已编译 `log-reader` 可执行文件。
2. `MockKafka` 已启动并监听 `127.0.0.1` 上的随机端口。
3. 临时 `log-reader` 配置已生成并加载。
4. pb 日志文件已创建并处于空状态。

## 配置构造

- `kafka_config.data` 中 `FieldMode = all`，不配置 `FieldNames`。

## 输入数据

写入 1 条 `BfeLog` 请求日志：

| 字段 | 值 |
|------|-----|
| logid | 50001 |
| header_host | all.example.org |
| origin_uri | /v1/chat |
| ai_requested_model | all-model |

## 操作步骤

1. 启动 `MockKafka`。
2. 生成临时配置目录与日志目录。
3. 启动 `log-reader` 进程。
4. 通过 `LogGenerator` 写入 1 条 b2log 记录。
5. 等待最多 10 秒，直到 `MockKafka` 收到 1 条消息。
6. 解析 JSON，验证所有注册字段均存在。

## 预期结果

- `MockKafka` 收到 1 条消息。
- 消息包含所有注册字段，例如：
  - 常用字段：`logid`、`timestamp`、`product`、`header_host`、`origin_uri`、`ai_requested_model`；
  - 不常用字段：`log_tag`、`client_network`、`req_num`、`session_id`、`referrer`、`user_agent`、`delegation`、`uid`、`cookie`、`req_headers`、`res_location`、`res_transfer_encoding`、`res_headers`、`session_offset_time`、`bfe_ip`、`sock_src_ip`、`vip` 等。
- 对齐改造（2026-10-01）后 Default 集已扩至全部 92 个注册字段，与 All 集字段名集合一致（两者仅模式语义不同）；`ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit` 未设置时输出 JSON `null`。
- `logid = 50001`，`ai_requested_model = "all-model"`。

## 清理

停止 `log-reader` 进程，关闭 `MockKafka`，删除临时目录。
