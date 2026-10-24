# mod_kafka 输出与 mod_log_mysql 写入对齐

## 背景

对 log-reader 两个输出模块（Kafka JSON 输出与 MySQL 报表库写入）做系统对齐性
分析后，结论：

| 维度 | mod_log_mysql（写入侧权威，99 列） | mod_kafka（Kafka JSON 输出） | 是否对齐 |
|------|-----------------------------------|------------------------------|----------|
| 字段注册 | mapper 88 个直接字段全部经 `mod_fields.Extract` 取值 | 全部 88 字段均已注册（92 字段表） | ✅ |
| 派生列 | `log_time`=timestamp→UTC 墙钟（driver 无 `loc` 参数默认 UTC 渲染）；`level1Name~level5` 写时打平 | 输出 `timestamp` epoch + `ai_apikeytags` 对象，由 Doris Routine Load 侧解析/打平（observability 已修 UTC 写入） | ✅（Doris 侧 2026-10-01 时区修复后） |
| JSON/布尔列 | 空→NULL；bool→0/1 | 空→JSON null；bool→显式 0/1 | ✅ |
| **Default 字段集** | 99 列**全量写入**，与字段注册无关 | `FieldMode=default`（observability 默认配置）只输出 **Default 集 74 字段**，18 个字段不输出 | ❌ **见 §1** |
| **optional 三列零值** | `kindOptionalNum` 做 proto 指针判空：未设置→**NULL**（report.md 契约"null=未求值"） | extractor 对 unset 返回零值，`json_converter.go` 的 `isZero continue` 已注释（2026-08-26 起全字段输出）→ 输出显式 **0** | ❌ **见 §2** |
| 字符串零值（旧列） | `kindScalar` 空串→**NULL** | 显式输出 **""**（既定决策，不调整，见 §3） | ⚠️ 已接受差异 |

**§1 的 18 个缺口字段**（mapper 有列、Default 集之外、`FieldMode=default`
下 Doris 明细表恒 NULL 而 MySQL 有值）：

```
bfe_ip, vip, vip6, client_network, sock_src_ip, req_num, session_id,
session_offset_time, log_tag, referrer, cookie, uid, user_agent,
delegation, req_headers, res_headers, res_location, res_transfer_encoding
```

注意 HOWTO v1.3（ai-gateway-observability）曾把这 18 列补进 Doris 表结构，但
Kafka 默认输出从未携带它们——表结构能力与数据能力错位，正是本方案要消除的。

## 修改目标

1. `FieldMode=default` 的输出字段集 = mod_log_mysql 的写入字段集（88 直接字段
   + 派生源字段），两链路在任何默认配置下字段对齐；
2. `ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit`
   三列在未设置时输出 JSON `null`，与 MySQL 侧 NULL（未求值）语义一致；
3. 同步文档与测试，防止后续字段再加挂时两类不对齐复发。

**非目标**：

- 不调整字符串零值语义（"" vs NULL，见 §3 决策）；
- 不改变 `FieldMode=all/require/customized` 语义；
- 不改动 mod_log_mysql 写入行为（它是对齐基准）；
- 不要求两侧消息/行体积一致（Kafka JSON 与 SQL 行本来形态不同）。

## 关键决策

| 决策 | 说明 |
|------|------|
| 对齐方向：kafka 向 mysql 看齐 | mod_log_mysql 的 `columnDefs` 是"写入侧 99 列清单与列序的唯一权威"（文件头注释），且两仓 DDL 以它为列序基准；Default 集扩到 88 字段使**默认配置**即对齐，size 敏感部署仍可用 `customized`/`require` 裁剪 |
| 18 字段整体进 Default（含 req_headers/res_headers） | 体积评估：18 字段多为短 VARCHAR，消息增量约几百字节/条；`req_headers`/`res_headers` 在大多数部署为空数组（`[]`），增量可忽略。当初注册为 non-default 未见文档化理由，属于历史遗留而非既定取舍 |
| optional 三列用 JSON `null` 表达 unset | JSON 无法表达 proto optional 的"字段未设置"，唯一可行的对齐方式是让 extractor 返回 nil → `null`；不引入 `has_xxx` 标记字段（消费侧改动面大、且 Routine Load COLUMNS 需同步加列） |
| 字符串零值维持现状 | `json_converter` 的全字段输出是 2026-08-26 既定决策（demo 样例、SC21 断言、HOWTO 均依赖）；报表查询层用 `IFNULL(col,'')` 归一两边语义（unknown 桶/空值排除），`ai_apikey_id` 幂等键差异（NULL vs ''）无实际影响。差异在消费文档注明即可 |

## 详细改动

### 1. 18 个字段纳入 Default 集（`reader_modules/mod_fields/field_registry.go`）

将下列 18 个 `registerField(...)` 调用的第 4 个参数（Default）由 `false`
改为 `true`（Required 保持 `false`）：

```
bfe_ip, client_network, cookie, delegation, log_tag, referrer,
req_headers, req_num, res_headers, res_location, res_transfer_encoding,
session_id, session_offset_time, sock_src_ip, uid, user_agent, vip, vip6
```

改动后 Default 集 = 92 字段（全部注册字段），`require` / `all` / `customized`
模式行为不变。

### 2. optional 三列 unset 输出 null（`reader_modules/mod_fields/field_registry.go`）

`ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit` 三个
extractor 改为 proto 指针判空（与 `mod_log_mysql/field_mapper.go`
`extractOptionalNum` 同逻辑）：

```go
// 例：ai_intent_confidence
func(bfeLog *bfe_access_pb.BfeLog) interface{} {
    if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
        if reqLog.AiIntentConfidence == nil {
            return nil // 未设置 -> JSON null（= MySQL NULL，未求值）
        }
        return *reqLog.AiIntentConfidence
    }
    return nil
},
```

`json_converter.go` 无需改动：`m[fieldName] = val` 对 nil 值自然产出 JSON
`null`；Doris Routine Load 与下游消费侧对 null 的现有处理（落 NULL /
`IFNULL` 归一）均兼容。

### 3. 测试更新

- `reader_modules/mod_fields/field_registry_test.go`：
  - Default 字段计数期望值 74 → 92（及分类统计如有）；
  - 新增三列 unset 断言：nil RequestLog / 指针 nil → `Extract` 返回 `(nil, *)`，
    非 nil → 原值；
- `reader_modules/mod_kafka/json_converter_test.go`：全字段输出断言中这三列
  的零值形态由 `0/false` 改为 `null`（如有）；
- 回归 `go test ./...`；SC21（scenario-SC21-bfe-log-reader-kafka）用
  `FieldMode=all` + 存在性断言，不受 Default 集扩大影响，仍须跑绿。

### 4. 文档更新

- `doc/modules/mod_kafka/output-fields.md`：Default 集统计（74→92）、分类
  计数、§3 中 18 个字段的"Default"列标记、三列 optional 字段的空值说明
  （"未设置输出 null"）；
- 本修改说明落地后，在 `doc/modules/mod_log_mysql/` 模块文档加一行
  对齐性约定："mod_log_mysql 写入列集 == mod_kafka Default 输出字段集
  （派生列除外），新增字段两侧同 PR 落地"。

## 下游影响

| 组件 | 影响 |
|------|------|
| ai-gateway-observability | Doris 链路默认配置（`FieldMode=default`）下 18 列由 NULL 变为有值，与 Doris 表全列映射匹配；`bfe_ai_log_load_routine.sql` COLUMNS 无需改动；`demo/normal_request.json` 本就全字段，无需改动 |
| ai-gateway-api 报表 | 无改动；两后端空值归一口径不变 |
| Kafka 消息体积 | 每条约 +200~400B（18 字段零值形态为主）；`req_headers`/`res_headers` 常规为空数组，影响可忽略 |
| 既有消费方 | 仅"多字段"与"三列零值变 null"，均为兼容性增量（旧版 log-reader 消息更少的场景不受影响） |

## 验证步骤

1. `go build ./... && go test ./...` 全绿（含更新后的计数/零值断言）；
2. 字段集对齐核对（关键）：

```bash
# mapper 直接字段（88）应全部落入 Default 集
go run ./cmd/... 或按 2026-09-27 §验证步骤的 grep 方式比对
```

3. `go test ./tests/integration/implementation/scenario-SC21-bfe-log-reader-kafka/... -v`
   （删除 `.integration-test-bin/log-reader.exe` 缓存后回归）；
4. （可选）Doris 报表全链路集成场景将 log-reader 的 `FieldMode=all` 改回
   `default` 复跑，验证默认配置下 Doris 明细表 18 列有值、用例仍绿。

## 部署与顺序约束

纯输出侧扩展（多输出字段 + 三列零值形态变化），对下游：

- Doris Routine Load / MySQL 表结构：均已是全列映射，**无顺序要求**；
- 唯一行为变化：optional 三列的 Kafka 消息由显式 `0` 变为 `null`——若某
  部署的旧消费方把"0 当未求值"用，升级后应改按 null 判断；报表链路无此
  问题（聚合维度是 `ai_intent_answer`，confidence 未进聚合）。

## 实现期确认（实现后补充）

1. Default 计数按方案落地为 **92**（全部注册字段），Required 仍为 22；
   `field_registry_test.go` 新增 `TestFieldRegistry_DefaultFieldsAlignedWithMysqlWriter`
   用 90 个字段名（88 直接字段 + `timestamp`/`ai_apikeytags` 派生源）锁定
   "Default∪Required ⊇ 写入字段集"的对齐关系，防止后续加挂字段时复发；
2. 三个 optional 字段的 unset 断言同时锁定 **值为 nil** 与 **isZero 标志**
   （`isZeroFloat64/Int64/Bool` 对 nil 返回 true，历史断言不受影响）；
3. `mod_log_mysql` 单测零改动即通过——mapper 的指针判空逻辑本就是本次
   对齐的基准；
4. SC21 回归通过（`FieldMode=all` + 存在性断言，不受 Default 集扩大影响）；
5. `output-fields.md` 统计修正为 92/22/92，并在 §4 注明对齐约定与
   `mod_log_mysql.md` 的"与 mod_kafka 的关系"一节互相引用。
