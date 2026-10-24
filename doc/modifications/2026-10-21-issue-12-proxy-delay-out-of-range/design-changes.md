# log-reader 修复 `mod_log_mysql` 数值越界毒化整批写入（issue #12）

> 版本：v1.5.x 设计稿　日期：2026-10-21
> 状态：**已实施**（2026-10-21；§5/§6/§7 生产代码改动 + 文档同步 + 单测/集成测试，见 §13 实施记录）
> 涉及组件：log-reader（`reader_modules/mod_log_mysql`，重点 `field_mapper.go` / `record_writer.go`）
> 上游：[rainway-ai-gateway/log-reader#12](https://github.com/rainway-ai-gateway/log-reader/issues/12)（P1，数据完整性缺陷 + 监控语义失真）
> 测试条款：`ai-gateway-e2e-test/docs/test-design/SC2201/SC2201-TC007.md` 的 `A-09`（越界值进入 SQL 前被拒绝或规范化，不得生成 MySQL 1264）与 `A-10`（单行异常不得导致同批正常行丢失）

---

## 目录

- [1. 背景与缺陷复现](#1-背景与缺陷复现)
- [2. 根因](#2-根因)
- [3. 目标与非目标](#3-目标与非目标)
- [4. 方案总览](#4-方案总览)
- [5. 改动一：写入前范围归一化（`field_mapper.go`）](#5-改动一写入前范围归一化field_mappergo)
- [6. 改动二：行级隔离（`record_writer.go`）](#6-改动二行级隔离record_writergo)
- [7. 监控与计数器](#7-监控与计数器)
- [8. 受影响列全量清单](#8-受影响列全量清单)
- [9. 测试计划](#9-测试计划)
- [10. 配置影响、上线顺序与回滚](#10-配置影响上线顺序与回滚)
- [11. 风险与开放问题](#11-风险与开放问题)
- [12. 参考](#12-参考)
- [13. 实施记录（2026-10-21）](#13-实施记录2026-10-21)

## 1. 背景与缺陷复现

proto `BfeLog` 中 `proxy_delay_time` 为 `uint32`，可携带 `2147483648`、`4294967295` 等超出 signed `INT` 上限的值。`mod_log_mysql` 原样写入报表库明细表 `bfe_ai_request_log.proxy_delay_time INT` 列，MySQL 返回 `Error 1264 (22003): Out of range value`，整个多值 `INSERT` 语句失败：

- 退避重试 3 次（200ms/400ms/800ms）均为同一语句、同一错误，无收敛可能；
- 重试耗尽触发 `markDown` 断连，重连后补写再次命中同一错误，最终整批（实测 4 行、6 行两种批大小）被丢弃并计入 `SEND_MYSQL_FAILED`；
- 同批的正常 200 行与两条 401 鉴权拒绝行随探针行一起丢失。

由此同时违反两条已批准的产品设计条款（`doc/sys_design/mod_log_mysql.md` §4.1/§4.2/§4.6）：

| 条款 | 要求 | 实际 |
|------|------|------|
| `A-09` | `0`、`2147483647` 可安全写入；`2147483648`、`4294967295` 在进入 SQL 前被拒绝或规范化，不产生 MySQL 1264 | 连续 4 次 `Error 1264`，探针行与同批行全部未落库 |
| `A-10` | 单行异常不得导致同批正常/鉴权拒绝行丢失；`dropped_rows`/`db_write_failed` 不因此增加 | 整批丢弃，`SEND_MYSQL_FAILED=1`、`MYSQL_CONN_LOST=1` |
| `§4.6` 计数语义 | `SEND_MYSQL_FAILED` 仅统计"重试耗尽且重连后补写仍失败"的**结构性错误**（表被 DROP、权限被收回），`>0` 即告警 | 非结构性错误（单行越界）污染该计数器，监控语义失效 |

## 2. 根因

根因链（issue #12 根因分析节，与本地源码核对一致）：

1. pb 字段为 `uint32`，值域大于 DDL 列类型；
2. `reader_modules/mod_log_mysql/field_mapper.go:116` 把 `proxy_delay_time` 声明为 `kindScalar`，`ToRow`（`:324-333`）原样透传；`columnKind` 枚举（`:29-38`）只有 8 种取值方式，**无任何范围约束**；
3. `record_writer.go:396-423` `writeBatch` 用单事务多值 `INSERT`（99 列）写入，命中 `ai-gateway-api/db_ddl_report_mysql.sql:141` 的 `proxy_delay_time INT DEFAULT NULL`；
4. MySQL 整语句失败 → `writeBatchWithRetry`（`record_writer.go:354-391`）以**同一语句**退避重试 → 耗尽 → `markDown` → 补写同错 → `SEND_MYSQL_FAILED+1`、整批丢弃；
5. 丢弃路径本只应为结构性错误预留（§4.2），越界属**可定位到行的数据错误**，却被当作连接/结构性故障处理，故 `A-09`/`A-10` 同时失败。

## 3. 目标与非目标

| # | 目标 | 验证标准 |
|---|------|----------|
| 1 | 越界数值在进入 SQL 前被规范化，不再产生 MySQL 1264 | `A-09` 四边界值（`0`、`2147483647`、`2147483648`、`4294967295`）均产生"可回读"结果，`SEND_MYSQL_FAILED` 不增加 |
| 2 | 单行数据异常不再毒化整批 | 同批正常/401 行全部回读成功，`dropped_rows=0`、`db_write_failed=0`（`A-10`） |
| 3 | 越界事件可观测 | 新增计数器 + 限频日志，能回答"哪列、多少行被规范化" |
| 4 | 结构性错误的既有语义不回归 | 表被 DROP、权限收回等仍走 `markDown` → 补写 → `SEND_MYSQL_FAILED` 既有路径 |

**非目标**：

- **不改 DDL 列类型**（issue 建议方向 3：`*_time` 列扩 `BIGINT`）。涉及已上线表结构与 ai-gateway-api 仓库 DDL 归属，需产品 owner 单独裁定，另立卡片；且扩列不能替代行级隔离（方向 2）对其它结构性错误的兜底；
- 不改 pb `uint32` 字段本身（bfe-access-pb / bfe 侧非缺陷来源，issue 已明确不建议开卡）；
- 不做历史数据修复（已被丢弃的批次无法从本模块找回，只能靠 `bfe-pblog-tool -b` 重放 pb 源文件补读）；
- 不引入配置项（归一化阈值由列定义硬编码，见 §5）；
- 不改 `Enqueue` 背压、攒批、`ON DUPLICATE KEY UPDATE` 全列覆盖等已批准语义。

## 4. 方案总览

采纳 issue 建议方向 1 + 方向 2 叠加（方向 3 另立任务）：

| 层 | 改动 | 文件 | 满足条款 |
|----|------|------|---------|
| 写入前 | 数值列声明写入范围，越界 clamp 到列上限（`kindScalar` 扩展，不改列清单与列序） | `field_mapper.go` | `A-09` |
| 写入时 | 捕获可定位到行的数据类错误（1264/1406 等），剔除报错行后重试剩余行 | `record_writer.go` | `A-10`（兜底） |

**为什么两层都要**：方向 1 使正常路径不再产生 1264（性能与日志干净）；方向 2 是 `A-10` 的硬要求——即使未来出现未纳入范围表的列/新错误码，单行异常也不再毒化整批。两层独立生效、互为兜底。

## 5. 改动一：写入前范围归一化（`field_mapper.go`）

### 5.1 设计

`columnDef` 增加可选的范围元数据（不新增 `columnKind`，避免 8 种 kind 爆炸；`kindScalar` 的数值直通分支统一过一遍范围校验）：

```go
type columnDef struct {
    name  string
    field string
    kind  columnKind
    level int
    max   int64 // > 0 时表示该列写入上限（按 DDL 列类型）；0 = 无约束
}
```

`ToRow` 的 `kindScalar` 分支（`:324-333`）在透传前检查：值为数值类型（`uint32`/`uint64`/`int64` 等）且 `max > 0` 时，若超过 `max` 则写入 `max`（clamp 到列上限），并上报一次归一化事件。

**clamp 到上限、不写 NULL 的取舍**：`A-09` 允许"拒绝或规范化"两种形态。选 clamp 因为：① 这些延迟/长度字段语义上"很大"优于"缺失"（NULL 会把求和/审计行从聚合里整体摘掉，clamp 只影响单字段）；② 与 `kindScalar` 既有"数值恒写整数"口径一致（参考 issue #1417 修改说明中 `logid` 的同类取舍）；③ 事件有计数器与日志，不静默。

### 5.2 范围表（按权威 DDL 逐列核对）

| 列 | proto 类型 | DDL 类型（`db_ddl_report_mysql.sql`） | max |
|----|-----------|--------------------------------------|-----|
| `proxy_delay_time` | `uint32` | `INT`（`:141`） | 2147483647 |
| `all_time` | `uint32` | `INT`（`:135`） | 2147483647 |
| `read_client_time` | `uint32` | `INT`（`:136`） | 2147483647 |
| `cluster_serve_time` | `uint32` | `INT`（`:137`） | 2147483647 |
| `backend_serve_time` | `uint32` | `INT`（`:138`） | 2147483647 |
| `write_client_time` | `uint32` | `INT`（`:139`） | 2147483647 |
| `connect_backend_time` | `uint32` | `INT`（`:140`） | 2147483647 |
| `session_offset_time` | `uint32` | `INT`（`:142`） | 2147483647 |
| `req_header_len` | `uint32` | `INT`（`:119`） | 2147483647 |
| `req_body_len` | `uint32` | `INT`（`:120`） | 2147483647 |
| `res_header_len` | `uint32` | `INT`（`:128`） | 2147483647 |
| `res_body_len` | `uint32` | `INT`（`:129`） | 2147483647 |
| `req_num` | `uint32` | `INT`（`:93`） | 2147483647 |
| `ai_retry_count` | `uint32` | `INT`（`:170`） | 2147483647 |
| `res_status_code` | `uint32` | `SMALLINT`（`:127`） | 32767 |
| `backend_retry` | `uint32` | `TINYINT`（`:125`） | 127 |

`logid` 为 pb `uint64` 对 `BIGINT`（issue #1417 后为 `NOT NULL DEFAULT 0`），值域在 int64 内的部分不会越界，本次不加约束；`err_code`、`ai_input_tokens` 等同族列在 proto/DDL 中类型一致或值域天然受控，不加约束（评审时可逐列复核）。

### 5.3 事件上报

`FieldMapper` 是无状态值对象，`ToRow` 返回 `(row, error)` 无计数通道。事件上报方式：

- `FieldMapper` 增加可选回调 `onNormalized func(col string)`（`NewFieldMapper()` 默认 nil，保持既有构造兼容）；
- `mod_log_mysql.go` 组装 mapper 时注入回调：`Inc("VALUE_NORMALIZED", 1)` + 限频日志（如每列每 60s 一条 Warn，记录列名与 clamp 前值——**记录值会打大数日志，仅记录列名与次数**）；
- 单测直接断言 `ToRow` 的 clamp 结果，不依赖回调。

## 6. 改动二：行级隔离（`record_writer.go`）

### 6.1 触发条件

`writeBatchWithRetry`（`record_writer.go:354-391`）在每次 `writeBatch` 失败后、退避重试之前，先判断错误是否为**可定位到行的数据类错误**：

- go-sql-driver/mysql 的 `*mysql.MySQLError`，`Number == 1264`（22003 out of range）或 `1406`（22001 data too long）；
- 错误文本含 `at row N`（N 为 1 起始的行号）。

满足则进入行剔除流程；不满足保持既有路径（退避 → markDown → 补写 → `SEND_MYSQL_FAILED`）。

### 6.2 行剔除流程

1. 从错误中解析行号 `N`（解析失败则放弃隔离、走既有路径——保守优先）；
2. 记录被剔除行的主键摘要（`logid`/`log_time`/`ai_apikey_id`/`ai_requested_model`，用于事后追查），`Inc("ROW_DROPPED_DATA", 1)`，Warn 限频日志；
3. 从批中剔除第 N 行，**以剩余行重建语句**（`buildStmt(len(remaining))` + 重建 args）后立即重试，不占 `MaxRetries` 次数、不 sleep（数据错误重试无收敛可能）；
4. 若剔除后批为空 → 视为本批处理完成，返回；
5. 若剔除后再次命中同类错误 → 重复剔除，设单批剔除上限（如批大小本身，天然有界）；
6. 剩余行重试中若出现**非**行定位类错误（连接断开等）→ 转既有路径（markDown/补写）。

### 6.3 语义边界

- **整批单语句写入不变**（§4.1 批准条款）：正常路径仍是单事务多值 `INSERT`，只有命中行定位错误时才拆批重建；
- `WRITE_BATCH_SIZE` 计数改为按**实际成功写入的行数**累计（剔除行不计），保持 `§4.6` 中"`SENT_TO_MYSQL`/`WRITE_BATCH_SIZE` 反映真实结果"的口径；
- `SEND_MYSQL_FAILED` 语义收窄回"结构性错误"：行剔除路径**不**触碰该计数器，修复其"`>0` 即告警（结构性错误）"的监控语义；
- `markDown`/`MYSQL_CONN_LOST` 不因数据错误触发（数据错误与连接健康无关），消除 issue 中"`MYSQL_CONN_LOST=1`"的误报。

### 6.4 实现要点

- `writeBatch` 返回错误时保留 `*mysql.MySQLError`（现状直接透传，无需改）；
- 行号 → 批内下标映射注意：MySQL 多值 `INSERT` 的 `at row N` 按 `VALUES` 列表 1 起始计数，与 `batch[N-1]` 对应；
- args 重建：剔除行后 `args` 不再是简单切片拼接，需按剩余行重新 append（`writeBatch` 内按 `batch` 生成 args 的逻辑需参数化或下沉，供"子批"复用）；
- 单测用 stub `connector`（既有注入缝）返回可控的 `*mysql.MySQLError`，验证：剔除行、剩余行成功、计数器正确、不再 `markDown`。

## 7. 监控与计数器

`COUNTER_KEYS`（`mod_log_mysql.go`）新增：

| 计数器 | 语义 | 告警建议 |
|--------|------|---------|
| `VALUE_NORMALIZED` | 因超界被 clamp 到列上限的字段值个数（按字段值计，非按行） | 稳态恒 0；非 0 说明上游产生超界值，需排查 BFE 侧 |
| `ROW_DROPPED_DATA` | 因行定位类数据错误（1264/1406）被剔除丢弃的行数 | 正常路径（§5 归一化生效后）恒 0；非 0 说明出现了范围表未覆盖的列/错误，需补范围表 |

两处监控文档同步：`doc/configuration/mod_log_mysql/mod_log_mysql.conf.md` 指标表、`doc/modules/mod_log_mysql/mod_log_mysql.md` §监控指标；`doc/sys_design/mod_log_mysql.md` §4.6 补充这两个计数器及"行剔除不改变 `SEND_MYSQL_FAILED` 结构性错误语义"的说明。

## 8. 受影响列全量清单

issue 影响范围节已核实：风险不限于 `proxy_delay_time` 一列。`field_registry.go` 中 `uint32` 声明而 DDL 为 `INT` 的列共 13 列，`res_status_code`（`uint32`→`SMALLINT`）、`backend_retry`（`uint32`→`TINYINT`）越界阈值更低。§5.2 范围表已全量覆盖。`mod_kafka` 侧输出不经过 MySQL 列类型约束（Doris 侧另仓），不在本次范围。

## 9. 测试计划

### 9.1 单元测试（`reader_modules/mod_log_mysql/`）

| 用例 | 断言 |
|------|------|
| `TestFieldMapper_RangeClamp`（`field_mapper_test.go`） | §5.2 全表逐列：`max` 边界值原样、`max+1` clamp 到 `max`；字符串/NULL 分支不受影响；回调触发次数正确 |
| `TestRecordWriter_RowEviction`（`record_writer_test.go`） | stub 返回含 `at row N` 的 1264：剔除第 N 行、剩余行写成功；`ROW_DROPPED_DATA`/`WRITE_BATCH_SIZE` 正确；不 `markDown`、`SEND_MYSQL_FAILED`/`MYSQL_CONN_LOST` 不增加 |
| 行剔除上限/连续多行越界 | 批内 2 行均越界：两次剔除后批空、正常返回；无死循环 |
| 非行定位错误回归 | 连接类错误仍走 markDown/补写路径（既有用例继续通过） |
| `TestModuleLogMysql_Update_...` 计数器一致性 | `SENT_TO_MYSQL` 等既有计数不受新计数器影响 |

### 9.2 集成测试（`tests/integration/implementation/scenario-LR03-mysql-write/`）

新增用例 `TestLR03_OutOfRangeValueSurvives`（真实 MySQL，沿用 `newTestEnv`/`logGen`/`waitRows` 骨架）：

- 构造同批 4 行：`proxy_delay_time` 分别为 `0`、`2147483647`、`2147483648`、`4294967295`，并混入一条正常行与一条 401 行；
- 断言：5 行全部落库且可回读（越界行读回 `2147483647`）；`SEND_MYSQL_FAILED=0`、`MYSQL_CONN_LOST=0`、`VALUE_NORMALIZED=2`；
- **红/绿口径**：在旧代码上该用例红（1264 → 整批丢弃）；本修复后绿。

### 9.3 E2E 回归

重跑 `SC2201-TC007`（`go test -tags=e2e -count=1 -v -run '^TestSC2201TC007_LogMySQLSink$' ./test-cases/SC2201`），`A-09`/`A-10` 断言应转绿。

## 10. 配置影响、上线顺序与回滚

- **配置影响**：无（不改 `mod_log_mysql.conf`、不加配置项）。
- **上线顺序**：单仓库改动，先发版 log-reader 即可；与 ai-gateway-api DDL 无依赖（本次不改 DDL）。
- **回滚**：回滚代码即回到旧行为（越界整批丢弃）；无数据迁移、无配置残留。

## 11. 风险与开放问题

| # | 项 | 说明 | 需谁裁定 |
|---|----|------|---------|
| 1 | clamp 值（上限）vs NULL 的取舍 | §5.1 已选 clamp；若报表口径要求"超界即未知"可改 NULL，但会摘除整行聚合参与 | 评审 |
| 2 | `at row N` 行号解析的可靠性 | 依赖 go-sql-driver 错误文本格式；解析失败保守回落到既有路径（可能仍丢批，但不引入新错误） | 实施时以驱动 v1.9.0 实测确认 |
| 3 | `err_code`、`ai_input_tokens` 等未加约束列 | 复核 proto/DDL 类型后如确认值域受控则不加；如发现同类风险应并入 §5.2 范围表 | 评审 |
| 4 | `mod_kafka`/Doris 链路同问题 | Doris 列类型约束不同，另仓（`ai-gateway-observability`）评估 | 另立任务 |
| 5 | DDL 扩列（issue 方向 3） | 根治越界但涉及已上线表结构与 ai-gateway-api DDL 归属；即使采纳也不替代行级隔离 | 产品 owner |

## 12. 参考

- 缺陷：[rainway-ai-gateway/log-reader#12](https://github.com/rainway-ai-gateway/log-reader/issues/12)（含根因链、运行证据 `run_id=qa-20261021-5dfdf34e`、三条建议方向）
- 设计条款：`doc/sys_design/mod_log_mysql.md` §4.1 写入语义、§4.2 背压/重试/连接生命周期、§4.6 监控；断言条款 `ai-gateway-e2e-test/docs/test-design/SC2201/SC2201-TC007.md` `A-09`/`A-10`
- 本仓库代码：`reader_modules/mod_log_mysql/field_mapper.go:26-47,116,260-337`、`record_writer.go:30-31,350-423`、`mod_log_mysql.go`（`COUNTER_KEYS`）
- 权威 DDL：`ai-gateway-api/db_ddl_report_mysql.sql:93,119-129,135-142,170`（列类型以该文件为准）
- 测试副本 DDL：`tests/integration/implementation/scenario-LR03-mysql-write/testdata/ddl/bfe_ai_request_log.sql`（本次不改，范围校验在写入侧，副本保持与权威 DDL 同步即可）
- 同类取舍先例：`doc/modifications/2026-10-13-issue-1417-dedup-key-logid/design-changes.md`（`LOGID_EMPTY` 计数器、"数值恒写整数"口径）

## 13. 实施记录（2026-10-21）

**结论**：§5 / §6 / §7 全部落地；两层独立生效——写入前归一化让正常路径不再产生 1264，行级隔离作为 `A-10` 的兜底。不改 pb、不改 DDL、不改 conf 结构、不引入新依赖（`github.com/go-sql-driver/mysql v1.9.0` 已在 `go.mod`，本次首次被 `record_writer.go` 直接 import）。

| 项 | 状态 |
|----|------|
| §5 写入前范围归一化 | ✅ `field_mapper.go`：`columnDef` 增 `max int64`；按 §5.2 给 17 列赋值（14 列 `INT` → 2147483647、`res_status_code` → 32767、`backend_retry` → 127，另 `req_num`/`ai_retry_count` 同 `INT`）；`clampNumeric` 通用处理 `uint32/uint64/int64/int32/int`（mod_fields 抽取类型），超界写 `max`；`FieldMapper.onNormalized` 可选回调（`SetOnNormalized`，nil 静默），仅作用于 `kindScalar` 数值分支，字符串 NULL 规则不变 |
| §6 行级隔离 | ✅ `record_writer.go`：新增 `rowErrorIndex`（`errors.As` → `*mysql.MySQLError` 且 `Number ∈ {1264,1406}` 且消息含 `at row N`，N 越界/解析失败返回 false）；`writeBatchWithRetry` 拆为首尝试 + 行剔除循环 + `retryWriteBatch` 退避路径：剔除行后立即以剩余行重建语句重写（不占 `MaxRetries`、不 sleep），批空即返回，出现非行定位错误转既有路径；`evictRow`/`rowDigest` 记主键摘要（`logid`/`log_time`/`ai_apikey_id`/`ai_requested_model`）+ 限频 Warn（60s）+ `Inc("ROW_DROPPED_DATA",1)`；`WRITE_BATCH_SIZE` 按实际成功写入行数计数；行剔除不触发 markDown、不计 `SEND_MYSQL_FAILED`/`MYSQL_CONN_LOST` |
| §7 计数器 | ✅ `COUNTER_KEYS` 增 `VALUE_NORMALIZED` / `ROW_DROPPED_DATA`；`Init` 组装 mapper 时注入 `normalizeMonitor.onNormalized`（Inc + 每列每 60s 一条 Warn，只记列名与累计次数） |
| §7 文档同步 | ✅ `doc/configuration/mod_log_mysql/mod_log_mysql.conf.md`（指标表 +2 行）、`doc/modules/mod_log_mysql/mod_log_mysql.md`（§监控指标 +2 行、§写入语义要点 +2 条：范围归一化/行级隔离）、`doc/sys_design/mod_log_mysql.md`（§4.1 范围归一化行、§4.2 行级隔离条目、§4.6 指标表 +2 行并显式记录"行剔除不改变 `SEND_MYSQL_FAILED` 结构性错误语义"）。**grep 复核**：三份文档中原先并无"非结构性错误也计入 `SEND_MYSQL_FAILED`"的表述（§1 表格中的该句是对缺陷现状的描述），故无需修正错误表述，只做新增 |
| §9.1 单元测试 | ✅ `field_mapper_test.go` `TestFieldMapper_RangeClamp`（§5.2 全表逐列：边界值原样 + max+1 clamp）、`TestFieldMapper_RangeClampCallback`（回调 2 次、nil 回调静默、字符串 NULL 不受影响）；`record_writer_test.go` `TestRecordWriter_RowEviction`（剔 1 行、留 2 行成功、`ROW_DROPPED_DATA=1`、`SEND_MYSQL_FAILED=0`、`MYSQL_CONN_LOST=0`、仍 UP）、`TestRecordWriter_RowEvictionAllRows`（连续剔除至批空、正常返回）、`TestRecordWriter_RowEvictionRowNumberUnparseable`（无 `at row N` → 回落 markDown 既有路径）；`mod_log_mysql_test.go` `TestModuleLogMysql_Update_ValueNormalized`（越界行计数 1、正常行不计数、既有计数器不受影响）。既有用例（含 `TestRecordWriter_SQLShape`）全绿 |
| §9.2 集成测试 | ✅ `tests/integration/implementation/scenario-LR03-mysql-write/lr03_mysql_write_test.go` 新增 `TestLR03_OutOfRangeValueSurvives`（4 探针 + 正常行 + 401 行，见下"偏离"第 2 条） |
| §9.3 E2E 回归 | ❌ **未运行**：`SC2201-TC007` 位于 `ai-gateway-e2e-test` 仓库，不在本工作区，无该仓库源码可跑；等价断言已由 §9.2 新增用例在真实 MySQL 上覆盖 |
| CHANGELOG | 经复核**不加条目**（本次不改列数/配置结构，行为变更为新增两个计数键与写入侧容错；与 issue #1417 先例一致） |

**验证结果**（env：本机 MySQL 8.4.9 临时实例，`mysqld --initialize-insecure` 独立数据目录 + 端口 3307，root 空密码；未读取/未使用既有实例的凭据文件）：

| 项 | 命令 | 结果 |
|----|------|------|
| 构建 | `go build ./...` | 通过 |
| 静态检查 | `go vet ./...` | 通过 |
| 模块单测 | `go test ./reader_modules/mod_log_mysql/` | ok（1.5s，含 6 个新增用例） |
| 新增用例定位 | `go test -run 'TestFieldMapper_RangeClamp\|TestRecordWriter_RowEviction\|TestModuleLogMysql_Update_ValueNormalized\|TestRecordWriter_SQLShape\|TestRecordWriter_Retry' -v` | 9/9 PASS（含既有 SQLShape/Retry 回归） |
| LR03 全场景 | `LR_MYSQL_DSN='root:@tcp(127.0.0.1:3307)/' go test -v ./tests/integration/implementation/scenario-LR03-mysql-write/` | **7/7 PASS**（新用例 5.79s；最终代码在删除缓存二进制重建后复跑一次，结果一致） |
| **红证据**（旧代码上该用例失败） | 首次运行命中了集成测试缓存的**旧二进制**（`tests/integration/.integration-test-bin/log-reader.exe`，未重新编译） | **FAIL**：`mod_log_mysql: write batch failed (attempt 4/4): Error 1264 (22003): Out of range value for column 'proxy_delay_time' at row 3` → `connection lost (write retry exhausted)` → `rewrite after reconnect failed, dropped 5 rows` → `row count = 1, want >= 6 (timeout 30s)`——与 issue #12 的复现形态一致；删除缓存二进制重新编译后 PASS |

**偏离设计稿之处及原因**：

1. **`writeBatchWithRetry` 结构调整**：为保持"非行定位错误的执行次数与退避节奏完全不变"（`MaxRetries+1` 次语句执行、首次不 sleep），把既有退避路径原样下沉为 `retryWriteBatch`，由首尝试 + 行剔除循环调用；行剔除后的重写走循环内联路径，不占 `MaxRetries`、不 sleep，与 §6.2 一致。行为对既有路径逐字保持（既有用例全绿为证）。
2. **集成用例行数**：§9.2 文字为"4 行探针 + 一条正常行 + 一条 401 行"却写"断言 5 行全部落库"（自相矛盾）。按"探针行 + 正常行 + 401 行"实现为 **6 行**，断言 6 行全部落库；越界探针仍为 2 条（`2147483648`、`4294967295`），`VALUE_NORMALIZED=2` 不变。
3. **LR03 集成测试监控端口改为动态分配**：计数器断言需要轮询 `/monitor/mod_log_mysql`，原 `testdata/config.conf` 固定 `httpPort=8993` 无法预先得知端口。改为 `httpPort={{HTTP_PORT}}` 占位 + `newTestEnv` 用 `common.FreePort` 固定端口（与 LR04 同做法），并同步更新 `测试设计文档/scenario-LR03-mysql-write/场景说明.md`（TC 列表新增 TC-07、关键断言点新增范围归一化、监控端口说明）与 `TC-01-基本写入全字段校验.md`；新增 `TC-07-越界值归一化与整批存活.md`。既有 6 个 LR03 用例全部复跑通过。
4. **`clampNumeric` 类型覆盖**：除设计稿点名的 `uint32/uint64/int64` 外，一并处理 `int32/int`（mod_fields 抽取类型集合内的整数类型），非整数类型（含 `float64`）一律原样透传——避免误伤 `ai_intent_confidence` 等浮点/字符串列。
5. **文档改动范围略超 §7 要求**：除三处监控指标章节外，另在 sys_design §4.1/§4.2 与模块文档 §写入语义要点补记归一化与行级隔离（否则文档会描述改造前的写入语义）。

**未做**：E2E `SC2201-TC007`（外仓，见上表）；DDL 扩列（§3 非目标）；`mod_kafka`/Doris 侧同问题（另仓另立任务）。

**顺带说明**：`reader_modules/mod_log_mysql/field_mapper.go` 存在**既存** gofmt 对齐偏差（`columnKind` 常量块的注释对齐，`git show HEAD:` 版本同样不满足 gofmt），本次未顺带重排该无关代码块（仅新增代码保持 gofmt 合规）；本次新增的 `field_mapper_test.go` 代码已 `gofmt -w`。
