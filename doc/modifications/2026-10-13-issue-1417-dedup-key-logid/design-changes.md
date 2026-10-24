# log-reader 配合修复 `bfe_ai_request_log` 幂等键粒度（issue #1417）

> 版本：v0.8 设计稿　日期：2026-10-13
> 状态：**方案待评审**（本文件只做设计；§6 计数器已按评审采纳并实施）　核心结论：修复机制本身 **零代码改动**（写入语句与列清单已自动适配新键）；另含 1 项经评审采纳的观测增强（§6 `LOGID_EMPTY` 计数器），以及文档与集成测试的修订
> 涉及组件：log-reader（`mod_log_mysql` 模块 + `doc/` + `tests/integration/`）；本仓库仅"文档 + 测试副本 + 新增用例"
> 上游：
> - 总方案：`document-ai-gateway/迭代系统设计/v0.8/bfe_ai_request_log秒级覆盖问题/bfe_ai_request_log秒级覆盖问题修复方案.md`
> - `bfe`（**已交付**）：pb `logid` 由恒 0 改为逐请求唯一非 0（commit `89defa14`）
> - `ai-gateway-api`（**已交付**）：`uk_dedup` 补 `logid` + `logid` 收 `NOT NULL DEFAULT 0`（commit `64f278e`）

---

## 目录

- [1. 背景与现状](#1-背景与现状)
- [2. 目标与非目标](#2-目标与非目标)
- [3. 核心结论：写入侧零代码改动](#3-核心结论写入侧零代码改动)
- [4. 文档修订清单](#4-文档修订清单)
- [5. 集成测试与测试副本 DDL](#5-集成测试与测试副本-ddl)
- [6. `LOGID_EMPTY` 计数器（已采纳）](#6-logid_empty-计数器已采纳)
- [7. 配置影响、上线顺序与回滚](#7-配置影响上线顺序与回滚)
- [8. 风险与开放问题](#8-风险与开放问题)
- [9. 参考](#9-参考)

## 1. 背景与现状

### 1.1 缺陷

本模块负责把 pb 访问日志**幂等写入**报表库明细表 `bfe_ai_request_log`（`INSERT ... ON DUPLICATE KEY UPDATE`，全列覆盖）。该表的唯一键 `uk_dedup` 原为四列 `(hostid, log_time, ai_apikey_id, ai_requested_model)`，其中 `log_time` 是由 pb `timestamp`（Unix **秒**）转换来的秒级 `DATETIME`——于是**同一秒内同一 API Key、同一模型的并发请求互相覆盖**：n 个请求只留 1 行，被覆盖行的 tokens/cost 从求和中整体消失。生产实测（2026-10-12 13:00–13:59 CST）：pb3 解码 2113 行 vs 明细表 1827 行，丢 286 行 = 13.5%，同秒 1/2/3-4 并发的丢率 1%/50%/67%——**恰为 (n−1)/n**。

设计上用于区分请求的判别列 `logid` 在该窗口内**恒为 0**（BFE 侧以十进制解析十六进制 logid 且吞掉错误），判别能力从未兑现。两侧修复既已交付（见文档头），本仓库需要处理的是"写入侧是否要改"与"文档/测试是否与新语义一致"。

### 1.2 本仓库的位置

| 层 | 位置 | 与本缺陷的关系 |
|----|------|---------------|
| 写入语句 | `reader_modules/mod_log_mysql/record_writer.go:84,100-112` | `ON DUPLICATE KEY UPDATE <每列>=VALUES(<每列>)`（全列覆盖）。**语句与列清单均由 `Columns()` 动态生成**，不含任何唯一键信息 |
| 列定义 | `reader_modules/mod_log_mysql/field_mapper.go:41,49-56` | `columnDefs` 是写入侧 99 列的**唯一权威**；`logid` 已在第 5 位（`:55`），并注明"列序与 ai-gateway-api 仓库 `db_ddl_report_mysql.sql` 一致" |
| 唯一键 | 仅存在于 DDL（权威在 ai-gateway-api 仓库） | 全仓库 `uk_dedup` 只出现在文档与测试副本 `.sql`，**无任何 Go 符号编码它** |
| 文档承诺 | `doc/modules/mod_log_mysql/output-columns.md`、`doc/sys_design/mod_log_mysql.md` 等 | 承诺"幂等键与 Doris UNIQUE KEY 完全一致""唯一键不含 logid""同一**分钟**内只保留一条"——与实现（同一**秒**）矛盾，且 `logid` 契约（"消费侧以 `log_time`+`logid` 为准"）在修复前从未成立 |
| 测试 | `tests/integration/implementation/scenario-LR03-mysql-write/`、`LR04` | LR03/LR04 各有一份**内嵌 DDL 副本**（二者逐字节相同，已 `diff` 核实），键仍是四元组 |

### 1.3 三处关键事实（决定本方案形态）

1. 写入列清单与覆盖子句都由 `Columns()` 生成，`logid` 已在其中 → **DDL 加列后写入侧自动生效，无需发版**（上一节已给出位置）。
2. 本仓库不持有唯一键 → 加键只能在 ai-gateway-api 的 DDL 做（已完成）。
3. LR03 的既有断言**没有被钉死**：`TestLR03_IdempotentReplay` 两次投递使用**同一个 logid**（80001），属于"同一请求重放"的**正当**幂等路径；仓库内**没有任何用例**断言"不同 logid 但同一四元组被覆盖"（`MakeRequestLog` 的 `logid` 由调用方给定，既有用例都用不同 model 或不同 token 区分）。

## 2. 目标与非目标

| # | 目标 | 验证标准 |
|---|------|----------|
| 1 | 写入行为与新语义一致 | **无需代码改动**（见 §3）；上线后同秒并发请求各自落库 |
| 2 | 文档不再与实现矛盾 | §4 清单逐条修订完成，且不再出现"唯一键不含 logid""同一分钟内只保留一条"等表述 |
| 3 | 集成测试 DDL 副本与权威 DDL 同步 | LR03/LR04 两处副本的 `uk_dedup` 为五列、`logid` 为 `NOT NULL DEFAULT 0`；副本头部注明基准版本 |
| 4 | 新增"报表库侧不丢行"回归用例 | LR03 新增用例：同秒同 Key 同模型、不同 `logid` 的 2 条 → 2 行（修复前为 1 行，红→绿） |

**非目标**：

- 不动 `record_writer.go` 的列清单、攒批、重试、连接生命周期；**不动覆盖语义**（它是批准设计：与 Doris UNIQUE KEY 一致、重发/`-b` 补读幂等）；
- 不改 `field_mapper.go` 的 99 列与零值规则（含 `logid` 的 `kindScalar` 行为：**恒写整数**，这正是新键 `NOT NULL DEFAULT 0` 前提）；
- **不做历史数据修复**：旧 pb3 行自身的 `logid` 就是 0，`-b` 重放只会在新键下继续互相覆盖，历史补偿需另立任务；
- 不同步改 Doris 明细表（`ai-gateway-observability` 仓库，另立任务；见 §7）；
- 不引入配置项。

## 3. 核心结论：修复机制零代码改动

**结论：本机制修复不需要改动写入路径**——唯一的生产代码改动是按评审采纳的观测计数器（`LOGID_EMPTY`，见 §6），它与写入语句、列清单、唯一键均无关。依据：

| 依据 | 位置 | 说明 |
|------|------|------|
| 列清单动态生成 | `record_writer.go:100-102` | `cols := Columns()` → `INSERT INTO <table> (cols...)`；`logid` 已在其中 |
| 覆盖子句动态生成 | `record_writer.go:104-112` | 逐列 `col=VALUES(col)`；新增/变更键不需要改语句 |
| 唯一键不在代码中 | 全仓库检索 | `uk_dedup` 仅出现在 `doc/` 与 `tests/.../testdata/ddl/*.sql`，无 Go 引用 |
| 单测已钉住 SQL 形状 | `record_writer_test.go:167-196`（`TestRecordWriter_SQLShape`） | 断言语句与 `Columns()`/`ddlColumns` 一致；DDL 变更不影响该断言 |
| `logid` 值形态 | `field_mapper.go:55,324-333` | `kindScalar` + pb `uint64` → **恒写整数、不写 NULL**，满足新键 `NOT NULL` 前提 |

因此本次配合工作 = **文档修订 + 测试副本同步 + 新增用例**（§4、§5）+ **已采纳的观测增强**（§6）。

## 4. 文档修订清单

以下为逐条核实的现存表述（行号为当前 HEAD），均需按"新键语义"改写：

### 4.1 `doc/modules/mod_log_mysql/output-columns.md`

| 行 | 现状 | 建议 |
|----|------|------|
| `:21` | `幂等 \| 唯一键 (hostid, log_time, ai_apikey_id, ai_requested_model) 冲突覆盖...` | 键改五列，并在描述里点明"`logid` 是请求级判别列（issue #1417）" |
| `:33` | `logid \| BIGINT \| 同名列 \| BFE 请求唯一标识` | 补口径：十进制值（≤2^63−1）、与字符串 logid 逐值恒等、**修复前恒 0** 的诚实注记与修复时点 |
| `:186` | `...与 Doris 明细表 UNIQUE KEY 完全一致。...注意唯一键**不含 logid**——同一**分钟**内同 Key 同模型的多条请求只保留一条...` | **必须重写**：① 键为五列；② 删除"不含 logid"；③ 删除"分钟级"（实现一直是秒级，且新键下同秒并发各自成行）；④ 与 Doris 的关系改为"列契约一致，唯一键语义待 Doris 侧同改"；⑤ 说明修复前"同秒并发丢 (n−1)/n"是缺陷而非设计承诺 |
| `:199` | DDL 片段 `UNIQUE KEY uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model),` | 同步为五列 |
| `:207` | `唯一键长度约 2565 字节...` | 改 `≈2579`（含 `logid` BIGINT 8 字节），仍 < 3072 |
| `:211-220`（§6 与 Doris 差异对照） | `幂等 \| UNIQUE KEY 模型（同键覆盖） \| 唯一键 + ON DUPLICATE KEY UPDATE` | 补一行差异说明：**Doris 侧仍是四元组、同缺陷未修**（`ai-gateway-observability/doris/sqls/bfe_ai_request_log.sql:144`） |

### 4.2 `doc/modules/mod_log_mysql/mod_log_mysql.md`

| 行 | 现状 | 建议 |
|----|------|------|
| `:53` | `...消费侧以 log_time+logid 为准，分钟聚合报表不受影响。` | 保留但补一句：该契约自 issue #1417 修复后**才真正成立**（此前 `logid` 恒 0） |
| `:89` | `- **幂等**：唯一键 (hostid, log_time, ai_apikey_id, ai_requested_model)，冲突覆盖更新——...` | 五列 |

### 4.3 `doc/sys_design/mod_log_mysql.md`

| 行 | 现状 | 建议 |
|----|------|------|
| `:28` | `...消费侧以 log_time+logid 为准；` | 同 §4.2 `:53` 的补注 |
| `:35` | `| 幂等键 | 唯一键 (hostid, log_time, ai_apikey_id, ai_requested_model)，冲突覆盖更新 |` | 五列 |
| `:73` | `| 幂等 | 唯一键四元组与 Doris UNIQUE KEY 完全一致；重发/重启/-b 补读均不产生重复行 |` | 改为"键含请求级判别列 `logid`（issue #1417）；同 `logid` 重放命中覆盖 → 重发/重启/`-b` 补读仍不产生重复行；Doris 侧待同改" |
| `:74` | `| 唯一键取舍 | 唯一键**不含 logid**：同一分钟内同 Key 同模型的多条请求只留一条（...以聚合可接受为前提） |` | **整行重写**：原取舍已被证伪（计费对账场景聚合不可接受，实测窗口低估 13.5%）；改为"唯一键含 `logid`：同秒并发各自成行，仅同一请求重放折叠" |
| `:110` | `- 唯一键 uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model)（约 2565 字节...）` | 五列 + `≈2579` |

### 4.4 其它

| 文件 | 行 | 现状 | 建议 |
|------|----|------|------|
| `doc/sys_design/overview.md` | `:157`、`:164-165` | `下游幂等性由输出模块保证（Kafka 侧靠 Doris 表 UNIQUE KEY，MySQL 侧靠唯一键覆盖）`、`① 下游写入幂等（Doris UNIQUE KEY / MySQL 唯一键覆盖），使「重放补数」安全` | 保留（结论仍成立），但把"MySQL 侧靠唯一键覆盖"补注为"键含 `logid`，覆盖只发生于同一请求重放"；Doris 侧标注"待同改" |
| `doc/howto/01AI Gateway 可观测性链路打通指南.md` | `:368`、`:389` | Doris 表 `bfe_ai_request_log` 为 `UNIQUE KEY`、`logid BIGINT COMMENT 'BFE 请求唯一标识'` | 保留；补注 Doris 侧键仍是四元组、与 MySQL 的差异及待办 |
| `CHANGELOG.md` | `:25` | `batch idempotent write (INSERT ... ON DUPLICATE KEY UPDATE) … table bfe_ai_request_log (**89 columns**)` | **经复核不改**：该行位于 `[v1.4.0]` 历史条目，描述该版本当时状态（含当时的 89 列），改动会伪造历史；本次变更不改列数，故也不新增 Changelog 条目（模块行为变更仅新增 `LOGID_EMPTY` 计数键） |
| `doc/modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md` | `:55,57,68,209,223,361-362` | 初版设计里的四元组与"不含 logid"叙述 | **历史文档不改**，在本方案与新版文档中明确"该取舍已被 issue #1417 推翻"即可（保留决策留痕） |

## 5. 集成测试与测试副本 DDL

### 5.1 测试副本 DDL 同步（必做）

| 文件 | 现状 | 改动 |
|------|------|------|
| `tests/integration/implementation/scenario-LR03-mysql-write/testdata/ddl/bfe_ai_request_log.sql` | 头部注明"权威 DDL 归 ai-gateway-api 仓库…以其为基准重新生成"；`:132-133` 为四元组键 | `logid` 列改 `BIGINT NOT NULL DEFAULT 0`；`uk_dedup` 改五列；头部注明新的基准版本（含 issue #1417） |
| `tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/testdata/ddl/bfe_ai_request_log.sql` | 与 LR03 副本**逐字节相同**（已 `diff` 核实） | 同步同上（两份必须一起改，否则分叉） |

副本与生产 DDL 的既有三处偏差（TEXT 列、幂等键字符串列允许 NULL、去掉分区子句）保持不变——本次不顺带调整。

### 5.2 新增用例（LR03）

在 `tests/integration/implementation/scenario-LR03-mysql-write/lr03_mysql_write_test.go` 增加一个用例（沿用既有 `newTestEnv`/`logGen`/`waitRows`/`mustFind...` 骨架）：

- **用例名**：`TestLR03_ConcurrentRowsSameSecondSurvive`（同秒同 Key 同模型的并发行各自落库）
- **数据构造**：使用 `common.MakeRequestLog` 生成**两条**日志——`hostid` 由 `reader_util.GetHostId()` 注入、`Timestamp` 固定为 `1782353290`（`log_generator.go:84`）、`ai_apikey_id` 固定 `key-id-123`（`:155`），因此两条日志天然共享 `(hostid, log_time, ai_apikey_id, ai_requested_model)`；只需**同一 model、不同 logid**（`logid` 由调用方给定，如 91001/91002，并给不同 `AiInputTokens` 以便区分先后）。
- **断言**：
  1. `waitRows(2, …)` —— 报表库出现 **2 行**（修复前为 1 行，红）；
  2. 两行的 `logid` 分别为 91001/91002，`ai_input_tokens` 各自保留（无覆盖）；
  3. 可选：再投递同一 `logid` 的第二条（值变更）→ 行数仍 2、值被覆盖（与既有重放用例互补）。
- **红/绿口径**：把副本 DDL 的键回退为四元组（或 `git stash` 该 DDL 文件）→ 本用例红（1 行）；同步为新键后绿。

### 5.3 既有用例的影响（已逐条核实）

| 用例 | 是否受影响 | 理由 |
|------|-----------|------|
| `TestLR03_IdempotentReplay` | **仍通过** | 两次投递使用**同一 logid**（80001）→ 五元组相同 → 命中覆盖、仍 1 行；这正是需要保留的"正当幂等"路径 |
| `TestLR03_BasicWrite`、`TestLR03_BatchSplit` | 仍通过 | 各自使用**不同 model**（`-01`/`-02`…）→ 五元组天然不同 |
| `TestLR03_ZeroValueToNull` | 仍通过 | 单行；`logid=81001` 非 0 |
| `TestLR03_NewFieldsWriteAndZeroRules` | 仍通过 | 单行 |
| `tests/integration/common/log_generator.go` | **无需改** | `MakeRequestLog` 已支持调用方指定 `logid`；但需注意**其产物不覆盖生产"logid 恒 0"形态**——该形态由 bfe 侧单测与 ai-gateway-api 侧用例覆盖 |

### 5.4 运行方式

```bash
cd log-reader
go test ./tests/integration/implementation/scenario-LR03-mysql-write/... -v      # 需要 MySQL（LR_MYSQL_DSN 或 testcontainers）
go test ./tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/...
```

## 6. `LOGID_EMPTY` 计数器（已采纳）

**实施状态（2026-10-13）**：已落地并验证——`COUNTER_KEYS` 增 `LOGID_EMPTY`、`Update` 内计数、单测 `TestModuleLogMysql_Update_LogidEmpty` 通过、演示文档三处同步（见下）。**本文 §2 目标表第 1 行的"无需代码改动"应读作"修复机制无需改动"**（唯一的生产代码改动即本计数器，与写入语句/列清单/唯一键无关）；**§8 风险 2 已结案为"采纳"**。

**动机**：新唯一键下 `logid=0` 的行与旧行为等价（`0` 参与比较 → 同秒同 Key 同模型继续互相覆盖）。bfe 侧已保证非 0、生产预期不会出现，但"出现时静默"与本次缺陷的教训相悖，故采纳。

**实现**（`reader_modules/mod_log_mysql/mod_log_mysql.go`）：

```go
var COUNTER_KEYS = []string{
	// ...
	"LOGID_EMPTY", // logid 为 0 的行数（bfenetworks/bfe#1417）：该行无法按请求级判别列去重，退化为旧四元组语义
}

// Update() 内，过滤出 request 类型之后：
if _, isZero := mod_fields.Extract("logid", bfeLog); isZero {
	m.state.Inc("LOGID_EMPTY", 1)
}
```

- **语义**：明细表 **request 行**中 `logid` 为 0 或缺失的条数（session 日志不计——它不进明细表）。`isZero` 用 `mod_fields` 的零值判定，与 `field_mapper` 写库时的判零口径一致（`logid` 走 `kindScalar` + pb `uint64`）。
- **代价**：每行一次字段抽取（O(1)）；不改写入语句、队列与重试行为，**不影响写入路径**（单测断言 `SENT_TO_MYSQL` 不变）。
- **监控**：已登记进 `COUNTER_KEYS`（web monitor 自动带出），并同步文档：
  - `doc/configuration/mod_log_mysql/mod_log_mysql.conf.md` 指标表；
  - `doc/modules/mod_log_mysql/mod_log_mysql.md` §监控指标（异常信号列：**非 0 即需排查**）。
- **建议告警阈值**：`0`（稳态恒 0；非 0 说明有行退化为旧四元组语义）。
- **单测**：`reader_modules/mod_log_mysql/mod_log_mysql_test.go` 的 `TestModuleLogMysql_Update_LogidEmpty`——覆盖"非 0 不计 / 显式 0 计 / 缺省（nil）计 / session 日志不计 / `SENT_TO_MYSQL` 不受影响"。

## 7. 配置影响、上线顺序与回滚

- **配置影响**：无（不改 `mod_log_mysql.conf`、不加字段）。
- **上线顺序**：与 `bfe`、`ai-gateway-api` 的改动**顺序不敏感**——本模块不持有键，DDL 生效即行为生效；两仓库均已交付（`89defa14` / `64f278e`）。
- **回滚**：本仓库回滚 = 回滚文档/测试副本（生产行为不受影响）；若 ai-gateway-api 侧 DDL 回滚为四元组，本模块行为自动回到旧语义，无需发版。
- **Doris 链路（另仓任务，非本仓库）**：Doris 明细表 `ai-gateway-observability/doris/sqls/bfe_ai_request_log.sql:144` 仍是四元组 `UNIQUE KEY`，同缺陷未修；`mod_kafka` 输出的列已含 `logid`，Doris 侧加键后本模块同样无需改动。两侧口径分叉期间，对账统一以 pb3 原始日志为准。

## 8. 风险与开放问题

| # | 项 | 说明 | 需谁裁定 |
|---|----|------|---------|
| 1 | 文档"分钟级"表述的取舍 | 原文档承诺分钟级、实现是秒级。本次按"实际语义"重写（同秒并发各自成行；只有同 `logid` 重放折叠），不再承诺任何"保留粒度" | 评审 |
| 2 | `LOGID_EMPTY` 计数器是否采纳 | 见 §6；不采纳则该形态静默（但不影响正确性——`logid=0` 行为与修复前逐行等价） | 评审 |
| 3 | 历史丢失不可回收 | 旧 pb3 行 `logid` 本身为 0，`-b` 重放无法补回；历史补偿另立任务 | 已确认 |
| 4 | 新增用例的样本量 | 2 行足以判别 (n−1)/n 的覆盖行为；如需覆盖 n=3/4 可扩展为表驱动 | 可选 |
| 5 | 与 Doris 的契约表述 | 两仓文档此前互称"完全一致"；Doris 未同改前，本仓库改称"列契约一致、唯一键语义待同改" | 评审 |

## 9. 参考

- 缺陷：[bfenetworks/bfe#1417](https://github.com/bfenetworks/bfe/issues/1417)；总方案与另两份仓库方案见文档头
- 本仓库代码：`reader_modules/mod_log_mysql/record_writer.go:84,100-112,393-423`、`field_mapper.go:41,49-56,324-333`、`mod_log_mysql.go:150-176`
- 权威 DDL：`ai-gateway-api` 仓库 `db_ddl_report_mysql.sql`（`logid NOT NULL DEFAULT 0` + 五列 `uk_dedup`）
- 测试副本：`tests/integration/implementation/scenario-LR03-mysql-write/testdata/ddl/bfe_ai_request_log.sql`、`scenario-LR04-mysql-startup-connect-retry/testdata/ddl/bfe_ai_request_log.sql`
- 既有设计留痕（不改，仅标注被推翻）：`doc/modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md`

## 10. 实施记录（2026-10-13）

**结论**：§4 / §5 / §6 全部实施完成；本模块**生产代码改动仅 1 处**（§6 计数器），写入路径、列清单、覆盖语义均未变。

| 项 | 状态 |
|----|------|
| §4 文档修订 | ✅ `output-columns.md`（§2 幂等行、§3.1 `logid` 行、§4.1 重写、§5 键与键长、§6 差异表新增"唯一键"行）、`mod_log_mysql.md`（行序/幂等两处）、`sys_design/mod_log_mysql.md`（文档头、非目标、术语、§3 流程图、§4.1 三行（幂等/取舍/`logid=0`）、§4.3、§4.4、§4.6 指标表、§5 兼容性、§6 文件索引）、`overview.md`（进程重启/缓解两处）、`howto/01…md`（Doris 表 UNIQUE KEY 行） |
| §4.4 CHANGELOG 行 | **经复核不改**：`(89 columns)` 位于 `CHANGELOG.md` 的 `[v1.4.0]` 历史条目，描述该版本当时状态，改动会伪造历史；本次变更不改列数，无需新增 Changelog 条目（模块行为变更仅新增 `LOGID_EMPTY` 计数键） |
| §5.1 测试副本 DDL | ✅ LR03 与 LR04 两份内嵌副本同步为五列键 + `logid NOT NULL DEFAULT 0`（二者逐字节一致，已 `diff` 校验）；头部基准说明补注 issue #1417 |
| §5.2 新增用例 | ✅ `TestLR03_ConcurrentRowsSameSecondSurvive`（`lr03_mysql_write_test.go`）：同秒同 Key 同模型、不同 `logid` 的 2 条 → 2 行（逐 `logid` 校验）；再同 `logid` 重放 → 仍 2 行且值被覆盖 |
| §6 `LOGID_EMPTY` 计数器 | ✅ `COUNTER_KEYS` + `Update` 计数 + 单测 `TestModuleLogMysql_Update_LogidEmpty` + 两处监控文档（`conf.md` 指标表、模块文档 §监控指标） |
| §8 风险 1（"分钟级"表述） | 已按"不承诺保留粒度、按实际语义描述"重写（同秒并发各自成行；仅同 `logid` 重放折叠） |
| §8 风险 2（计数器是否采纳） | 已结案：**采纳**（§6） |

**验证结果**（真实 MySQL 8.4.9 临时实例；非默认端口 13306，验证后实例关闭、目录删除）：

| 项 | 命令 | 结果 |
|----|------|------|
| 模块单测 | `go test ./reader_modules/mod_log_mysql/...` | ok（含新计数器用例） |
| 静态检查 | `go vet`（模块 + LR03/LR04） | 通过 |
| LR03（含新用例） | `LR_MYSQL_DSN=… go test -v ./…/scenario-LR03-mysql-write/...` | **6/6 PASS**（新用例 6.24s） |
| **红证据**（新用例真回归锁） | 将 LR03 副本 DDL 回退为四列键（`git stash`）后重跑新用例 | **FAIL**：`table bfe_ai_request_log row count = 1, want >= 2 (timeout 30s)`——同秒两行只剩 1 行，即 (n−1)/n 丢行在库侧精确复现；恢复五列键后 PASS |
| LR04 | `LR_MYSQL_DSN=… go test -v ./…/scenario-LR04-mysql-startup-connect-retry/...` | 退出码 0：`ConfigErrorStillFailFast` PASS；两个需 Docker 暂停/恢复实例的用例 SKIP（DSN-only 环境的预期行为，CHANGELOG 已声明） |

**顺带说明**：`lr03_mysql_write_test.go` 内存在**既存** gofmt 对齐偏差（非本次引入），本次因编辑该文件一并 `gofmt -w` 清理（约 25 行纯格式改动，无逻辑变化）；`reader_modules/mod_log_mysql/field_mapper.go` 的同款既存偏差未顺带修改（未编辑该文件）。

**未做（另仓任务）**：Doris 明细表加 `logid`（`ai-gateway-observability`，同缺陷同修）；历史丢失行不可回收（旧 pb3 行的 `logid` 本身为 0）。