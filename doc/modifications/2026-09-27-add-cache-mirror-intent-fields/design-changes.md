# log-reader 输出与入库字段增加 ai-cache / 流量镜像 / ai-intent 字段

## 背景

BFE 三个模块的访问日志字段已随 `bfe-access-pb` 发布并回填，log-reader 目前
均未注册，导致字段无法流入 Kafka 输出与 MySQL 报表库：

| 模块 | 字段（proto 编号） | bfe-access-pb |
|------|--------------------|---------------|
| ai-cache | `ai_cache_status`(789)、`mirror_cluster` 无关；本组只加 `ai_cache_status` | v0.3.7 |
| 流量镜像 | `mirror_hit`(842)、`mirror_cluster`(843) | v0.3.8 |
| ai-intent | `ai_intent_question`~`ai_intent_questions_version`(803–809) | v0.3.9 |

消费侧（ai-gateway-api）已完成配套：`db_ddl_report_mysql.sql` 明细表已扩为
99 列（本次 +10 列），report 查询层已实现过滤与聚合消费（mileszhang2016/
ai-gateway-api `d884f3a`）。**log-reader 是链路上剩余的一环**：`mod_log_mysql`
的 `field_mapper.go` 是写入侧 89→99 列清单与列序的唯一权威，列序必须与
ai-gateway-api DDL 完全一致，否则报表库写入错位。

明确不注册的字段（与消费侧口径一致）：

- `ai_cache_key`(790)：debug 专用，避免日志膨胀，报表库也不收；
- `mirror_status`~`mirror_error`(844–850)：异步镜像结果走 Prometheus，
  BFE 设计即不回写日志语义，报表库不收。

## 修改目标

1. `mod_fields`（Kafka JSON 输出）：注册 10 个新字段；
2. `mod_log_mysql`（MySQL 报表库写入）：`columnDefs` 89 → 99 列，列序与
   ai-gateway-api `db_ddl_report_mysql.sql` 严格一致；
3. `mod_kafka`：如输出走显式字段注册则同步注册（沿用
   `2026-08-22-add-cache-token-fields` 先例，实际以代码现状为准）；
4. 文档同步：`doc/modules/mod_kafka/output-fields.md` 字段表与统计汇总；
5. 依赖升级：`go.mod` 中 `bfe-access-pb` v0.3.5 → v0.3.9。

## 详细改动

### 0. 依赖升级

`log-reader/go.mod`：`github.com/bfenetworks/bfe-access-pb v0.3.5` → `v0.3.9`，
`go mod tidy && go build ./...`。注意：v0.3.9 刚发布到
`bfenetworks/bfe-access-pb`，goproxy.cn 收录可能有延迟；若拉取失败，用
`GOPROXY=direct GOSUMDB=off go mod tidy`（bfe 仓库升级时同款处理），
**不要**启用 go.mod 中注释掉的本地 replace 提交。

### 1. mod_fields 字段注册（`reader_modules/mod_fields/field_registry.go`）

沿用既有 `registerField(name, type, required, default, ...)` 模式，新增 10 项
（位置建议放在 `ai_route_rule_hits` 之前/之后均可，本表内按 proto 编号顺序
插入 789、803–809、842–843 对应位置）：

| 字段 | 类型 | Default | 取值 |
|------|------|---------|------|
| `ai_cache_status` | string | ✅ | `reqLog.GetAiCacheStatus()` |
| `ai_intent_question` | string | ✅ | `reqLog.GetAiIntentQuestion()` |
| `ai_intent_answer` | string | ✅ | `reqLog.GetAiIntentAnswer()` |
| `ai_intent_confidence` | float64 | ✅ | `reqLog.GetAiIntentConfidence()`（proto 为 optional double，注意空值不输出） |
| `ai_intent_source` | string | ✅ | `reqLog.GetAiIntentSource()` |
| `ai_intent_latency_us` | int64 | ✅ | `reqLog.GetAiIntentLatencyUs()` |
| `ai_intent_cache_hit` | bool | ✅ | `reqLog.GetAiIntentCacheHit()` |
| `ai_intent_questions_version` | string | ✅ | `reqLog.GetAiIntentQuestionsVersion()` |
| `mirror_hit` | bool | ✅ | `reqLog.GetMirrorHit()` |
| `mirror_cluster` | string | ✅ | `reqLog.GetMirrorCluster()` |

零值/空值语义与既有字段一致（optional 字段未设置不输出；空字符串不输出）。

### 2. mod_log_mysql 列映射（`reader_modules/mod_log_mysql/field_mapper.go`）

`columnDefs` 从 89 列扩为 **99 列**，新增 10 列**追加在既有 AI 列段末尾**，
顺序与 ai-gateway-api `db_ddl_report_mysql.sql` 明细表新列顺序**逐字一致**
（消费侧 DDL 已发布，此处不得擅自调序）：

```
ai_cache_status             VARCHAR(16)   来源 reqLog.GetAiCacheStatus()
mirror_hit                  TINYINT(1)    来源 reqLog.GetMirrorHit()
mirror_cluster              VARCHAR(128)  来源 reqLog.GetMirrorCluster()
ai_intent_question          VARCHAR(64)   来源 reqLog.GetAiIntentQuestion()
ai_intent_answer            VARCHAR(64)   来源 reqLog.GetAiIntentAnswer()
ai_intent_confidence        DOUBLE        来源 reqLog.GetAiIntentConfidence()
ai_intent_source            VARCHAR(32)   来源 reqLog.GetAiIntentSource()
ai_intent_latency_us        BIGINT        来源 reqLog.GetAiIntentLatencyUs()
ai_intent_cache_hit         TINYINT(1)    来源 reqLog.GetAiIntentCacheHit()
ai_intent_questions_version VARCHAR(32)   来源 reqLog.GetAiIntentQuestionsVersion()
```

类型/长度与消费侧 DDL 对齐；`mirror_hit`/`ai_intent_cache_hit` 的 proto bool
→ MySQL TINYINT(1) 转换沿用既有 bool 列的处理方式。文件头注释
"写入侧 89 列清单与列序的唯一权威"同步改为 99 列。

### 3. mod_kafka 输出字段（`reader_modules/mod_kafka/field_registry.go`）

若 mod_kafka 为显式注册（参照 2026-08-22 先例），同步注册上述 10 字段，
并更新 `doc/modules/mod_kafka/output-fields.md`：

- §3.11 AI 可观测字段表新增 10 行（proto 编号序）；
- 底部统计汇总：AI 可观测字段数、总字段数、Default 字段总数相应 +10；
- `field_registry_test.go` 中 Default 字段计数期望值同步更新。

### 4. 测试

#### 4.1 单元测试

- `mod_fields`/`mod_kafka` 注册表测试：新字段名/类型/Default 断言、计数；
- `mod_log_mysql`：列数 89→99 断言、新列名与列序断言（建议按列名清单做
  快照式断言，与 ai-gateway-api DDL 防漂移）、`ToRow` 行宽与取值断言
  （构造含 intent 结果的 BfeLog，验证 10 个新槽位取值正确）。

#### 4.2 集成测试（`tests/integration`，原 LR03 场景扩展）

LR03（scenario-LR03-mysql-write）是"生成 BfeLog → 真实 log-reader 进程 →
真实 MySQL → 逐列断言"的端到端场景，本次必须同步扩展，否则新列的列序/
取值错误无法被端到端捕获：

1. **testdata DDL 副本再生**：`scenario-LR03-mysql-write/testdata/ddl/
   bfe_ai_request_log.sql` 是 ai-gateway-api 权威 DDL 的内嵌副本（文件头注释
   已注明"权威 DDL 发布后以其为准重新生成"），须用 ai-gateway-api 当前的
   99 列版 `db_ddl_report_mysql.sql` 明细表重新生成（保留副本头注释的偏差
   说明，如分区数等测试裁剪）；
2. **`common/log_generator.go`**：日志构造 helper 增加 ai_cache_status /
   mirror_hit / mirror_cluster / ai_intent_* 字段的填充能力；
3. **新 TC-05（新字段写入与零值规则）**：
   - 构造含全部 10 个新字段的 BfeLog → 断言 MySQL 各列取值精确相等
     （`ai_intent_confidence` 浮点、`mirror_hit`/`ai_intent_cache_hit` bool→
     TINYINT 转换是重点）；
   - 零值形态：未启用意图/缓存/镜像的最小日志 → 新列为 mapper 定义的
     缺省值（空串/0，与 TC-03 零值规则同风格断言，具体缺省以实现为准
     并写入用例文档）；
4. **`requestLogRow` 视图与断言 helper**：扩展新列的 scan 与断言方法；
5. **测试设计文档**：`测试设计文档/scenario-LR03-mysql-write/` 新增
   `TC-05-新字段写入与零值规则.md`，`场景说明.md` 的 TC 列表、涉及配置、
   字段统计（如有列数表述）同步更新；
6. **回归**：TC-01~TC-04 全绿（89→99 列变更后，TC-01 全字段校验的列清单
   如有显式计数需同步）。

## 验证步骤

1. `go build ./...` 与 `go test ./...`（含单元测试）全绿；
2. 集成测试：`go test ./tests/integration/implementation/scenario-LR03-mysql-write/... -v`
   （需本机 MySQL，参照 `tests/integration/README.md`；TC-01~TC-04 回归 +
   新 TC-05 断言 10 新列取值与零值规则）；
3. 列序一致性核对（关键）：

```bash
# log-reader 侧最后 10 列
grep -o '"[a-z_0-9]*"' reader_modules/mod_log_mysql/field_mapper.go | ...
# 与 ai-gateway-api db_ddl_report_mysql.sql 明细表末尾 10 列逐一比对
```

3. 文档统计：`output-fields.md` 字段数与实际注册数一致；
4. LR03 testdata DDL 副本与 ai-gateway-api 权威 DDL 列集一致（列名集合 diff 为空）。

## 部署与顺序约束

**INSERT 组装方式（实现期已确认）**：`mod_log_mysql` 按**显式列名清单**
INSERT（`record_writer.go`，`INSERT INTO <table> (col1,...,col99) VALUES ...
ON DUPLICATE KEY UPDATE`），列清单由 `columnDefs` 生成，非固定表列序。因此：

- 旧 log-reader（89 列）写新表（99 列）安全：新列走 DDL DEFAULT；
- **新 log-reader（99 列）写旧表（89 列）会因未知列名报错**——部署顺序：
  先对报表库执行 ALTER（新部署直接用 99 列 DDL 建表），再升级 log-reader；
- 无需两侧同版本切换。

DDL ALTER 指引（存量表，列序即上文 §2 顺序）：

```sql
ALTER TABLE bfe_ai_request_log
  ADD COLUMN ai_cache_status             VARCHAR(16)  NOT NULL DEFAULT '' AFTER ai_route_rule_hits,
  ADD COLUMN mirror_hit                  TINYINT(1)   NOT NULL DEFAULT 0  AFTER ai_cache_status,
  ADD COLUMN mirror_cluster              VARCHAR(128) NOT NULL DEFAULT '' AFTER mirror_hit,
  ADD COLUMN ai_intent_question          VARCHAR(64)  NOT NULL DEFAULT '' AFTER mirror_cluster,
  ADD COLUMN ai_intent_answer            VARCHAR(64)  NOT NULL DEFAULT '' AFTER ai_intent_question,
  ADD COLUMN ai_intent_confidence        DOUBLE                DEFAULT NULL AFTER ai_intent_answer,
  ADD COLUMN ai_intent_source            VARCHAR(32)  NOT NULL DEFAULT '' AFTER ai_intent_confidence,
  ADD COLUMN ai_intent_latency_us        BIGINT                DEFAULT NULL AFTER ai_intent_source,
  ADD COLUMN ai_intent_cache_hit         TINYINT(1)            DEFAULT NULL AFTER ai_intent_latency_us,
  ADD COLUMN ai_intent_questions_version VARCHAR(32)  NOT NULL DEFAULT '' AFTER ai_intent_cache_hit;
```

（AFTER 位置以权威 DDL `db_ddl_report_mysql.sql` 实际列序为准执行时核对。）

## 实现期确认与偏差（实现后补充）

1. **mod_kafka 无独立字段注册表**：现行实现经 `json_converter.go` 共用
   mod_fields 全局注册表，10 字段注册进 mod_fields 即自动流入 Kafka 输出；
   mod_kafka 侧仅同步 `output-fields.md` 文档（§3 的"如显式注册"按现状落地为
   只改文档）。
2. **新增 `kindNotNullStr` 写入类型**：DDL 中 7 个新字符串列为
   `NOT NULL DEFAULT ''`，既有 `kindScalar` 会把空串映射为 NULL（触发
   Error 1048），故 mapper 新增该 kind，空串原样写入。
3. **零值语义（最终决策，与 ai-gateway-api report.md 契约对齐，null=未求值）**：
   新字符串列空串写 `''`（不写 NULL）；`ai_intent_confidence`/
   `ai_intent_latency_us`/`ai_intent_cache_hit` 三个可空数值列改为 **proto
   指针判空**（mapper 新增 `kindOptionalNum`，`extractOptionalNum` 直接检查
   RequestLog 的 optional 指针）：未设置（指针 nil）→ 写 NULL（= 未求值），
   显式置 0 → 写 0，消费侧可区分"求值为零"与"未求值"。ttft/tpot 等既有数值列
   保持历史 0 口径不动。LR03 TC-05 已按此锁定：Part 2 缺省形态断言 NULL，
   Part 3 显式置 0 断言写 0。
4. **output-fields.md 统计修正存量偏差**：修正后 BfeLog 顶层 5/4/4、客户端
   连接 4/1/1（删除未注册的 `client_ip6` 行）、请求头 14/4/10、时间信息
   8/6/7、AI 可观测 39、总 92 字段 / 74 Default（非机械 +10）。
5. **集成测试缓存注意**：LR 场景共用 `.integration-test-bin/log-reader.exe`
   且仅当文件不存在才重建——改源码后须删除该缓存二进制再跑集成测试。
