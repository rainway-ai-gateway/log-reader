# log-reader 集成测试

本目录承载 `log-reader` 的真实进程级集成测试。

与单元测试不同的是：

- 启动真实的 `log-reader` 进程；
- 不依赖真实的 BFE，而是通过测试代码直接生成 protobuf 访问日志；
- 不依赖真实的 Kafka，而是通过 `MockKafka` 模拟 Kafka Broker；
- 验证 log-reader 是否能正确读取日志、解析字段并输出到 Kafka。

## 目录结构

```text
log-reader/tests/integration/
├── README.md                                  # 本文档
├── common/                                    # 公共 harness
│   ├── mock_kafka.go                          # 模拟 Kafka Broker
│   ├── mysql_env.go                           # MySQL 测试环境（LR_MYSQL_DSN / testcontainers / skip）
│   ├── log_generator.go                       # 生成 b2log 格式日志
│   ├── process_env.go                         # 编译/启动/停止 log-reader 进程
│   ├── config_builder.go                      # 生成临时 log-reader 配置
│   └── util.go                                # 工具函数
├── implementation/                            # Go 实现代码
│   └── scenario-LR01-basic-flow/
│       ├── lr01_basic_flow_test.go
│       └── testdata/                          # 静态配置模板
└── 测试设计文档/                               # 中文测试设计文档（可选）
```

## 运行方式

在 `log-reader/` 目录下执行：

```bash
# 运行全部集成测试
go test ./tests/integration/... -v

# 运行单个场景
go test ./tests/integration/implementation/scenario-LR01-basic-flow/... -v

# 运行单个测试例
go test ./tests/integration/implementation/scenario-LR01-basic-flow/ -run TestLR01_BasicFlow -v
```

首次运行会自动编译 `log-reader` 二进制并缓存到 `log-reader/tests/integration/.integration-test-bin/`。

## 当前覆盖

| 场景 | 说明 |
|------|------|
| LR01 基本流程 | 验证 log-reader 读取 protobuf 日志、按 `conf/mod_kafka/kafka_config.data` 中开启的 60 个字段输出 JSON 到 Kafka，并逐字段（含 AI 对象数组与字符串数组）验证 JSON 内容与输入 protobuf 一致 |
| LR01 批次拆分 | 验证日志数量超过 `MaxSizePerBatch` 时，模块能正确拆分批次处理，且不丢失/不错误转换字段内容 |
| LR01 Customized 字段模式 | 验证 `customized` 模式自动包含必需字段 |
| LR01 Default 字段模式 | 验证 `default` 模式输出默认字段集（92 个注册字段，与 mod_log_mysql 写入列对齐，2026-10-01 起含原 non-default 的 18 个字段） |
| LR01 All 字段模式 | 验证 `all` 模式输出所有注册字段 |
| LR01 Require 字段模式 | 验证 `require` 模式仅输出 22 个必需字段 |
| LR01 JSON 结构稳定性 | 验证同一条消息多次解析结果一致 |
| LR01 optional 字段未设置输出 null | 验证 `ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit` 未设置时输出 JSON null（与 MySQL NULL 未求值语义对齐），设置时输出原值 |
| LR02 CLI 工具 | 验证 bfe-pblog-tool 的 cat/tail 子命令在各种参数组合下的输出正确性，包括行号显示、指定条数、跟随模式、空文件与错误路径 |
| LR03 基本写入 | 验证 mod_log_mysql 将请求日志按 99 列清单写入 MySQL：逐列断言标量/布尔 TINYINT/JSON 列/标签打平/log_time，覆盖字符串零值→NULL 规则 |
| LR03 幂等重放 | 验证同唯一键日志重发与 log-reader 重启 `-b` 补读时不产生重复行、冲突值被覆盖 |
| LR03 零值规则 | 验证字符串零值与空结构→NULL、布尔/数值零值→0，以及未认证请求（ai_apikey_id 为 NULL）可正常写入 |
| LR03 批次拆分 | 验证日志量超过 MaxSizePerBatch(10) 与 BatchSize(5) 时多批次处理不丢行、不串值 |
| LR03 新字段写入与零值规则 | 验证 ai-cache/流量镜像/ai-intent 10 新列全字段写入精确取值（浮点列、bool→TINYINT 转换）与缺省形态（NOT NULL 字符串列→空串，mirror_hit→0，三个可空意图数值列→NULL=未求值；显式置 0→写 0 可区分） |
| LR04 启动不可达后台重试 | 验证 MySQL 不可达时 log-reader 不退出：`MYSQL_CONN_STATE=DOWN` 且 `MYSQL_CONN_RETRY` 增长，日志照常入队缓冲（`WRITE_BATCH_SIZE=0`）；MySQL 恢复后自动建连（`STATE=UP`、`MYSQL_CONN_OK=1`）并排空积压（行数与 logid 一致）。依赖 Docker（testcontainers Pause/Unpause）；外部 `LR_MYSQL_DSN` 环境 skip |
| LR04 配置错误仍 fail-fast | 验证 `Mysql.Addr` 置空（配置错误）时进程退出且退出码非 0，守护"配置错误立即退出、连通性错误不退出"的分界。无需 MySQL |
| LR04 运行期断连自动重连 | 验证已建连后 MySQL 挂掉：DOWN 在 `BatchTimeoutMs` 决定的有限窗口内出现（进程不挂死），断连期行入队缓冲；恢复后自动重连（`MYSQL_CONN_LOST=1`、`SEND_MYSQL_FAILED=0`）并排空落库。依赖 Docker（testcontainers Pause/Unpause）；外部 `LR_MYSQL_DSN` 环境 skip |

## Mock 说明

### Mock 日志生成

`common.LogGenerator` 使用 `b2log.HeaderWrite` + `proto.Marshal` 将 `bfe_access_pb.BfeLog` 写入文件，格式与 BFE `mod_access_pb3` 输出的日志一致。

### Mock Kafka

`common.MockKafka` 是一个轻量级 TCP 服务器，使用 `segmentio/kafka-go/protocol` 处理以下 Kafka 请求：

- `ApiVersions`
- `Metadata`
- `Produce`

收到的 `Produce` 消息会被解析并保存，测试代码通过 `WaitForMessages` 等待并校验消息内容。

### MySQL 环境（LR03）

LR03 使用真实 MySQL（不用 SQLite 替代：`ON DUPLICATE KEY UPDATE` 与表分区均为 MySQL 专有，方言差异会造成假阳性）。`common.MysqlEnv` 按优先级选择实例：

1. 环境变量 `LR_MYSQL_DSN`（如 `root:****@tcp(127.0.0.1:3306)/`，凭据需具备 CREATE/DROP DATABASE 权限）；测试创建专用随机名数据库并在结束后 DROP，不使用 DSN 中自带的数据库名；
2. testcontainers 启动 `mysql:8.0` 容器（需本机 Docker 可用）；
3. 都不可用则 `t.Skip`。

表结构由场景 testdata 内嵌 DDL 副本初始化（`scenario-LR03-mysql-write/testdata/ddl/bfe_ai_request_log.sql`，副本头注释注明与权威 DDL 的偏差）。

## 参考文档

- `测试设计文档/测试场景总体说明.md`
- `测试设计文档/scenario-LR01-basic-flow/场景说明.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-02-批次拆分.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-03-Customized字段模式包含必需字段.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-04-Default字段模式.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-05-All字段模式.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-06-Require字段模式.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-07-JSON结构稳定性.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-08-optional字段未设置输出null.md`
- `测试设计文档/scenario-LR02-cli-tool/场景说明.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-01-cat基本输出.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-02-cat带行号.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-03-cat不存在的文件.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-04-cat空文件.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-05-tail默认输出.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-06-tail指定条数.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-07-tail不存在的文件.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-08-tail空文件.md`
- `测试设计文档/scenario-LR02-cli-tool/TC-09-tail跟随新增数据.md`
- `测试设计文档/scenario-LR03-mysql-write/场景说明.md`
- `测试设计文档/scenario-LR03-mysql-write/TC-01-基本写入全字段校验.md`
- `测试设计文档/scenario-LR03-mysql-write/TC-02-幂等重放.md`
- `测试设计文档/scenario-LR03-mysql-write/TC-03-零值规则.md`
- `测试设计文档/scenario-LR03-mysql-write/TC-04-批次拆分.md`
- `测试设计文档/scenario-LR03-mysql-write/TC-05-新字段写入与零值规则.md`
- `../doc/configuration/config.md`
- `../doc/configuration/mod_kafka/mod_kafka.conf.md`
- `../doc/configuration/mod_kafka/kafka_config.data.md`
- `../../bfe/tests/integration/README.md`

