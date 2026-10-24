# TC-08 optional 字段未设置输出 null

## 用例编号与名称

TC-08 optional 字段未设置输出 null

## 所属场景

LR01 基本流程与 JSON 转换正确性

## 版本声明

- `log-reader`：当前源码版本（2026-10-01 对齐改造后）

## 测试目的

验证 `ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit` 三个
proto optional 字段的 JSON 输出语义：

- 日志中**未设置**（指针为 nil）时输出 JSON `null`，与 mod_log_mysql 写入侧
  NULL（"未求值"）语义对齐；
- 日志中**已设置**时输出原值。

## 运行模式

单组件模式：仅启动真实 `log-reader` 进程，Mock Kafka 与日志生成均在测试代码中完成。

## 前置条件

1. 已编译 `log-reader` 可执行文件。
2. `MockKafka` 已启动并监听 `127.0.0.1` 上的随机端口。
3. 临时 `log-reader` 配置已生成并加载。
4. pb 日志文件已创建并处于空状态。

## 配置构造

- `kafka_config.data` 中 `FieldMode = customized`，字段包含
  `ai_intent_confidence`、`ai_intent_latency_us`、`ai_intent_cache_hit`（加必需字段）。

## 输入数据

写入 2 条 `BfeLog` 请求日志：

| 字段 | 第 1 条 | 第 2 条 |
|------|---------|---------|
| logid | 80001 | 80002 |
| header_host | intent-unset.example.org | intent-set.example.org |
| ai_intent_confidence | 未设置 | 0.95 |
| ai_intent_latency_us | 未设置 | 1234 |
| ai_intent_cache_hit | 未设置 | true |

## 操作步骤

1. 启动 `MockKafka`。
2. 生成临时配置目录与日志目录。
3. 启动 `log-reader` 进程。
4. 通过 `LogGenerator` 依次写入 2 条 b2log 记录（第 1 条 intent 三字段指针为 nil，
   第 2 条按上表赋值）。
5. 等待最多 10 秒，直到 `MockKafka` 收到 2 条消息。
6. 解析 JSON，分别验证两条消息的 intent 三字段取值。

## 预期结果

- `MockKafka` 收到 2 条消息。
- 第 1 条消息：`ai_intent_confidence`、`ai_intent_latency_us`、`ai_intent_cache_hit`
  三个字段均存在且值为 JSON `null`（不是 0 / false）。
- 第 2 条消息：`ai_intent_confidence = 0.95`，`ai_intent_latency_us = 1234`，
  `ai_intent_cache_hit = true`。

## 清理

停止 `log-reader` 进程，关闭 `MockKafka`，删除临时目录。
