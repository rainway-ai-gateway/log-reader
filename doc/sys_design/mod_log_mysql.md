# mod_log_mysql 模块设计（MySQL 输出）

> 状态：已随 v1.4.0 发布；连接容错分两期演进——一期（2026-09-29）启动连接失败后台重试、二期（2026-09-30）运行期断连自动重连，均已实施。实施细节与变更清单见：
> - [doc/modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md](../modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md)（模块引入）
> - [doc/modifications/2026-09-29-mod-log-mysql-startup-connect-retry/design-changes.md](../modifications/2026-09-29-mod-log-mysql-startup-connect-retry/design-changes.md)（一期）
> - [doc/modifications/2026-09-30-mod-log-mysql-runtime-reconnect/design-changes.md](../modifications/2026-09-30-mod-log-mysql-runtime-reconnect/design-changes.md)（二期）
> - [doc/modifications/2026-10-13-issue-1417-dedup-key-logid/design-changes.md](../modifications/2026-10-13-issue-1417-dedup-key-logid/design-changes.md)（幂等键补请求级判别列 `logid`，issue #1417）

## 1. 背景与目标

### 1.1 背景

报表标准形态依赖 Kafka/Doris/Grafana 三个外部组件，对小规模/私有化部署过重。新增「轻量形态」：访问日志由 log-reader 直接写入 MySQL，报表查询由 ai-gateway-api 提供 API、ai-gateway-web 展示，全程无需 Kafka/Doris/Grafana。

### 1.2 目标

1. 消费 pb 访问日志，批量幂等写入 MySQL 明细表 `bfe_ai_request_log`（与 Doris 明细表同名同列）；
2. 写路径异步化：缓冲队列 + 单写协程攒批，事务批插，对读取主流程零阻塞；
3. 背压保护：队列满丢弃计数；写失败退避重试；
4. 复用 mod_kafka 的字段抽取能力（公共包 `mod_fields`），保证两链路字段口径一致；
5. **连接容错**：MySQL 不可达（启动或运行期）不导致 log-reader 退出——后台重试建连、断连期行数据缓冲、恢复后自动排空补写；写路径超时有界（网络分区下秒级失败，不挂死）；
6. 完整监控计数（含连接状态机）与排障指引。

### 1.3 非目标

- 不做聚合计算——分钟级聚合由 ai-gateway-api 侧 JOB 完成（与本模块解耦）；
- 不做自动建表/改表——DDL 归 ai-gateway-api 仓库持有，本模块账号无 DDL 权限；
- 不做死信落盘——重试耗尽且重连后补写仍失败的批丢弃计数，依赖幂等键支持 `-b` 补读重放；
- 不保证断连窗口内的严格行序——pending 批优先补写 + 队列重排使窗口内行序与到达序可能不一致，消费侧以 `log_time`+`logid` 为准（该契约自 issue #1417 修复后才真正成立：此前 `logid` 恒 0，不具备定位能力）；
- v1 仅 MySQL；PostgreSQL 预留 `database/sql` 驱动替换点，不实现。

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| 幂等键 | 唯一键 `(hostid, log_time, ai_apikey_id, ai_requested_model, logid)`，冲突覆盖更新；`logid` 为请求级判别列（issue #1417） |
| 打平列 | `level1Name/level1 … level5Name/level5`，写入时由 `ai_apikeytags` 打平生成 |
| 零值规则 | 字符串空串写 NULL、数值/布尔原样、空 JSON 写 NULL（下游聚合 IFNULL 归一） |
| 补读重放 | 以 `log_reader -b` 从头重读 pb 日志，依赖幂等键安全覆盖（同 `logid` 命中覆盖） |
| 分区管理 JOB | ai-gateway-api 侧的按天 RANGE 分区创建/清理任务（本模块不参与） |
| DOWN/UP | 连接状态机的两个态：`MYSQL_CONN_STATE` 键值；DOWN = 连接池已摘除（`db` 为 nil），写协程挂起缓冲 |
| markDown | 置 DOWN 的唯一入口（写重试耗尽或探活失败触发）：摘除并关闭连接池、`MYSQL_CONN_LOST+1`、唤醒 supervisor 重连 |
| connSupervisor | 常驻连接监管协程：UP 期等待 markDown 触发（可选周期探活），DOWN 期按 `ConnectRetryIntervalMs` 重试建连 |
| 持有补写 | 运行期写失败的批不立即丢弃，由写协程持有，待重连成功后优先补写；补写仍失败才计数 `SEND_MYSQL_FAILED` |

## 3. 在系统中的位置

```
PbLogReader 批次分发
   └─► mod_log_mysql.Update(batch)
         ├─ 过滤：仅 BfeLogType_Request
         ├─ LOGID_EMPTY 计数（logid 为 0 的行，issue #1417）
         ├─ mod_fields 抽取（与 mod_kafka 共用）→ field_mapper 组装行（99 列固定列序）
         │    （字符串零值→NULL；apikeytags 打平；JSON 列 marshal）
         └─ RecordWriter.Enqueue(row)
               └─ recordCh（容量 QueueSize，非阻塞）
                     └─ writeLoop（UP 期）：攒批（BatchSize 或 FlushIntervalMs）
                           → 事务批插（BatchTimeoutMs 超时约束）
                           INSERT ... ON DUPLICATE KEY UPDATE（幂等覆盖）
                           ├─ 失败 → 指数退避重试 MaxRetries 次
                           └─ 耗尽 → markDown（DOWN）+ 持有该批
Init: 尽力建连一次 → 失败非致命（STATE=DOWN）
Start: writeLoop + connSupervisor（常驻）
connSupervisor: DOWN 期按 ConnectRetryIntervalMs 重试 Connect；
                成功 → STATE=UP，换代 connCh 唤醒 writeLoop → 持有批优先补写 → 排空积压
```

## 4. 详细设计

### 4.1 写入语义

| 项 | 设计 |
|----|------|
| 语句 | 单事务多值 `INSERT INTO <table> (99 列) VALUES (...),(...) ON DUPLICATE KEY UPDATE col=VALUES(col), ...`（全列覆盖） |
| 幂等 | 唯一键含请求级判别列 `logid`：只有**同一请求重放**（同 `logid`）才命中覆盖 → 重发/重启/`-b` 补读均不产生重复行（issue #1417） |
| 唯一键取舍 | 唯一键**必须含 `logid`**：四列键下 `log_time` 是秒级（见下），同秒同 Key 同模型的并发请求被全列覆盖压成 1 行——生产实测丢率恰为 (n−1)/n（窗口 13.5%），被覆盖行的 tokens/cost 从求和中整体消失。原设计"唯一键不含 `logid`、以聚合可接受为前提"的取舍**已被 issue #1417 推翻**；含 `logid` 后并发请求各自成行 |
| `logid=0` 的行 | `0` 参与键比较 → 退化为旧四元组语义（同秒并发仍互相覆盖）。bfe 侧已保证非 0；本模块以 `LOGID_EMPTY` 计数暴露这类行（正常应为 0） |
| 零值规则 | 字符串空串 → NULL；数值/布尔原样（`ai_stream=0`、`ai_retry_count=0`）；`logid` 恒写整数、不写 NULL（列已 `NOT NULL DEFAULT 0`）；nil/空 JSON → NULL |
| 范围归一化 | 17 个数值列按 DDL 列类型声明写入上限（`INT` 2147483647、`res_status_code` SMALLINT 32767、`backend_retry` TINYINT 127），超界值写入前 clamp 到列上限并计数 `VALUE_NORMALIZED`（issue #12：pb `uint32` 值域大于 DDL 列类型，原样写入触发 MySQL 1264 毒化整批） |
| 时间列 | `log_time` 由 pb `timestamp`（Unix 秒）写时转 DATETIME（Doris 链路为 Routine Load `FROM_UNIXTIME`，同口径） |
| 标签打平 | `levelNName/levelN` 写时从 `ai_apikeytags` 对象打平；无标签 → NULL |

### 4.2 背压、重试与连接生命周期

- **入队非阻塞**：`recordCh` 容量 `QueueSize`（默认 2000），满则丢弃并计数 `SENT_MYSQL_CHN_FULL`——与 mod_kafka 同策略，读取主流程永不被下游拖住；
- **攒批**：单写协程，条数 `BatchSize`（默认 200）或超时 `FlushIntervalMs`（默认 2000ms）先到先发；
- **写失败重试**：失败按 200ms/400ms/800ms 指数退避重试至 `MaxRetries`（默认 3）；耗尽**不再直接丢批**，而是 `markDown` 进入 DOWN 并持有该批（不占 `QueueSize`、保序优于 re-enqueue）；
- **连接监管（connSupervisor，常驻）**：启动未连通与运行期 `markDown` 共用同一条重试路径——UP 期等待触发（可选 `HealthPingIntervalMs` 周期探活，失败即 markDown，面向半开连接场景）；DOWN 期按 `ConnectRetryIntervalMs` 重试 `Connect`（幂等可反复调用），成功则换代 `connCh` 唤醒写协程，`STATE=UP`、`MYSQL_CONN_OK+1`；写协程与持有批经 `waitConnected`（双重检查防换代竞态）恢复，持有批**优先补写**，随后按批排空积压；
- **补写仍失败**（结构性错误：表被 DROP、权限被收回）才计数 `SEND_MYSQL_FAILED` 丢弃该批（不落盘死信，恢复后靠 `-b` 补读重放）；
- **行级隔离（issue #12）**：批写入命中**可定位到行的数据类错误**（`*mysql.MySQLError` 1264/1406 且消息含 `at row N`）时，剔除第 N 行并立即以剩余行重建语句重写（不占退避重试次数、不 sleep、不 `markDown`），计数 `ROW_DROPPED_DATA`；剔除后批空视为本批处理完成；行号解析失败或出现非行定位错误则保守回落上面的退避/补写路径——单行数据异常不再毒化整批，`SEND_MYSQL_FAILED` 语义收窄回结构性错误；
- **超时控制（写路径有界）**：建连受 `ConnectTimeoutMs` 约束（`PingContext` + DSN `timeout` 拨号参数）；单批事务受 `BatchTimeoutMs` 约束（`BeginTx`/`ExecContext` 截止时间 + DSN `writeTimeout`，后者兼约束 Commit/Rollback）——网络分区（DROP）下写路径秒级失败而非挂死到 OS 级 TCP 超时。`BatchTimeoutMs` 必须大于正常 P99 批执行时间，否则慢批会被误判为断连；
- **关闭**：`Close()` 停两协程（supervisor 直接退出，writeLoop drain 残留批后最终 flush）→ 关连接池；从未连通则丢弃缓冲行并记 Info 日志（进程退出边界）；
- **panic 防护**：写协程 recover；停滞可通过 `SENT_TO_MYSQL` 计数停增发现。

**状态机**：

```
UP ──(写重试耗尽 / 探活失败)──► markDown ──► DOWN ──(supervisor 重连成功)──► UP
```

`markDown` 与 `Connect` 通过 `atomic.Pointer` 的 Swap 互斥——任何时刻至多一个有效连接池，先到者生效，幂等不重复计数。

### 4.3 表结构（归 ai-gateway-api 仓库持有）

`bfe_ai_request_log` 99 列，与 Doris 同名同列；两侧差异：复合列类型（`ARRAY<STRUCT>` → `JSON`，共 7 个 JSON 列）与**唯一键**（本侧含请求级判别列 `logid`，Doris 侧仍是四元组、待同改，issue #1417）。**DDL 权威文件在 ai-gateway-api 仓库 `db_ddl_report_mysql.sql`**（与聚合表 `bfe_ai_metrics_1m` 同文件），归属理由：

1. ai-gateway-api 依赖面最广：聚合 JOB 按列取数、分区管理 JOB 执行 `ALTER TABLE`、明细查询，且运行时持有 DDL 权限；本模块仅按固定列清单 INSERT，账号无 DDL 权限；
2. ai-gateway-api 是代码库中唯一有 MySQL DDL 管理传统的仓库（`db_ddl.sql` 体系），两表与其同发布、同演进；
3. 发布顺序 schema-first：先执行 DDL 再启用插件。

列清单与字段语义见 [output-columns.md](../modules/mod_log_mysql/output-columns.md)；`field_mapper` 代码内列常量表是写入侧列序的唯一权威，单测校验其与 DDL 一致。

### 4.4 索引与分区

- 唯一键 `uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model, logid)`（约 2579 字节 < 3072 InnoDB 上限；含分区列满足 MySQL 分区表约束）；
- 二级索引面向报表查询过滤维度：`idx_model_time`、`idx_apikey_time`、`idx_provider_time`、`idx_host_time`、`idx_status_time`；
- 按天 `RANGE (TO_DAYS(log_time))` 分区；首个分区由建表 DDL 提供，新分区创建与过期分区 DROP 由 ai-gateway-api 分区管理 JOB 负责（对齐 Doris 动态分区语义，保留期默认 7 天）。

### 4.5 配置

`conf/mod_log_mysql/mod_log_mysql.conf`（INI，gcfg）：

| 节 | 配置项 | 默认 | 说明 |
|----|--------|------|------|
| `[Basic]` | `OpenDebug` | false | 打印每行组装结果 |
| `[mysql]` | `Addr` / `User` / `Password` | 必填 | DSN 三要素 |
| `[mysql]` | `DBName` / `Table` | 必填 | 目标库表（默认 `bfe_report` / `bfe_ai_request_log`） |
| `[mysql]` | `ConnectTimeoutMs` | 3000 | 单次建连超时（`PingContext` + DSN `timeout` 拨号参数），防黑洞 IP 挂死 |
| `[mysql]` | `ConnectRetryIntervalMs` | 3000 | 建连重试间隔：启动未连通与运行期 markDown 共用 |
| `[mysql]` | `HealthPingIntervalMs` | 0（关） | UP 期周期探活；>0 时探活失败即 markDown，面向半开连接场景 |
| `[Writer]` | `QueueSize` | 2000 | 缓冲队列容量（断连期可缓存行数上限） |
| `[Writer]` | `BatchSize` | 200 | 攒批条数 |
| `[Writer]` | `FlushIntervalMs` | 2000 | 攒批超时 |
| `[Writer]` | `MaxRetries` | 3 | 写失败重试次数；耗尽触发 markDown + 持有补写 |
| `[Writer]` | `BatchTimeoutMs` | 30000 | 单批事务超时（`ExecContext` + DSN `writeTimeout`），须大于正常 P99 批执行时间 |
| `[Writer]` | `MaxOpenConns` / `MaxIdleConns` | 10 / 5 | 连接池 |

**凭据安全**：部署上为插件建专用最小权限 MySQL 账号——仅目标库目标表 INSERT/UPDATE 权限（`ON DUPLICATE KEY UPDATE` 需要 UPDATE），无 DDL/DELETE/CREATE。

启用（`config.conf`）：`Modules = mod_log_mysql`（与 mod_kafka 并存语法：`mod_kafka:false, mod_log_mysql:true`）。

### 4.6 监控指标

`mod_log_mysql(_diff)` 计数：

| 指标 | 含义 | 异常信号 |
|------|------|----------|
| `RECEIVED_LOGS` / `RECEIVED_REQ` | 收到条数 / request 条数 | 与 `bfe_reader.SUM_PB_RECORD` 对不上 |
| `CONVERT_FAILED` | 行组装失败 | 持续增长排查字段映射 |
| `SENT_TO_MYSQL` | 入队成功 | 停滞 = 写管道异常 |
| `SENT_MYSQL_CHN_FULL` | 队列满丢弃（背压） | 持续增长 = 断连超窗或 MySQL 写入跟不上 |
| `SEND_MYSQL_FAILED` | 重试耗尽且重连后补写仍失败、丢弃的批 | >0 即告警（结构性错误）；运行期断连本身不再贡献（二期起） |
| `WRITE_BATCH_SIZE` | 每批条数累计 | diff 均值观测批大小 |
| `MYSQL_CONN_STATE` | 连接状态 `UP`/`DOWN`（state 键值） | `UP→DOWN` 跳变或长期 `DOWN` 建议告警 |
| `MYSQL_CONN_RETRY` | 建连重试累计次数 | 按重试间隔节奏增长即 supervisor 工作正常 |
| `MYSQL_CONN_OK` | 建连成功累计次数 | 正常运行恒为 1，每次运行期重连 +1 |
| `MYSQL_CONN_LOST` | 运行期断连次数（markDown 触发） | 持续增长 = 链路抖动或 MySQL 不稳定 |
| `LOGID_EMPTY` | `logid` 为 0 的明细行数（issue #1417） | **非 0 即需排查**：该类行无法按请求级判别列去重，退化为旧四元组语义（同秒同 Key 同模型的并发请求仍会互相覆盖）；bfe 侧已保证 `logid` 非 0 |
| `VALUE_NORMALIZED` | 因超界被 clamp 到列上限的字段值个数（issue #12，按字段值计） | **稳态恒 0；非 0 即需排查**：上游产生了超出 DDL 列类型的值，写入侧已归一化保证落库，取值来源需在 BFE 侧核查 |
| `ROW_DROPPED_DATA` | 因行定位类数据错误（1264/1406 + `at row N`）被剔除丢弃的行数（issue #12） | **正常路径恒 0；非 0 即需排查**：出现范围表未覆盖的列或新错误码，需补范围表。剔除不触发 `markDown`（`MYSQL_CONN_LOST` 不增）、不改变 `SEND_MYSQL_FAILED` 的结构性错误语义 |

## 5. 降级与兼容性

| 场景 | 行为 |
|------|------|
| MySQL 启动不可达 | 进程不退出：`STATE=DOWN`，supervisor 后台重试；断连期行入队缓冲，恢复后自动建连排空补写（幂等），不丢批（缓冲容量内） |
| MySQL 运行期断连 | 写重试耗尽 → `markDown`（`MYSQL_CONN_LOST+1`）→ supervisor 自动重连 → 持有批优先补写 + 积压排空；`SEND_MYSQL_FAILED` 不随断连时长增长（二期行为） |
| 持续断连超窗 | 缓冲超出 `QueueSize` 后背压丢弃（`SENT_MYSQL_CHN_FULL`）；恢复后 `-b` 补读重放缺口 |
| 配置错误 | 配置文件缺失/必填字段为空仍 **fail-fast**：`Init` 返回错误，进程退出（连通性错误不退出，二者分界见模块文档） |
| 认证失败/库不存在（1045/1049） | 后台重试不自愈：进程存活、`STATE=DOWN` 常驻、节流日志提示根因；修复方式是改配置重启 |
| 表不存在（写时 1146） | 重连后补写仍失败，该批计数 `SEND_MYSQL_FAILED` 丢弃（结构性错误不重试） |
| 唯一键仍为四元组（DDL 未升级） | 行为退化为修复前语义（同秒并发丢 (n−1)/n）；本模块无需发版，DDL 升级后自动生效（键不在代码中） |
| pb 加字段 | 本模块列清单不变（与 Doris 表对齐演进，两侧同步加列）；未纳入字段不落库 |
| 与 mod_kafka 并存 | 支持（Modules 多模块），但单集群建议只启用一种落库形态，避免两条链路数据缺口窗口不一致 |
| 进程重启 | 从文件尾读（丢窗口）或 `-b` 全量重放（幂等覆盖），同框架语义 |
| 回滚 | 一期/二期配置均为可选带默认值；回滚二进制需同步回退 conf（gcfg 对未知字段报错，见各期变更文档）；`LOGID_EMPTY` 为新增计数键，旧二进制回滚后该计数自然消失，无配置影响 |

## 6. 关键文件索引

| 文件 | 内容 |
|------|------|
| `reader_modules/mod_log_mysql/mod_log_mysql.go` | ModuleLogMysql：接口实现、Init（尽力建连、连通性错误非致命）、Update/Close（含 `LOGID_EMPTY` 计数）、监控注册 |
| `reader_modules/mod_log_mysql/field_mapper.go` | 99 列固定列序行组装、零值规则、标签打平、JSON marshal |
| `reader_modules/mod_log_mysql/record_writer.go` | 缓冲队列、攒批、事务批插（超时约束）、退避重试、连接状态机（connSupervisor/markDown/epoch connCh/持有补写）、drain |
| `reader_modules/mod_log_mysql/conf_mod_log_mysql.go` | INI 配置解析与校验 |
| `reader_modules/mod_fields/` | 公共字段抽取包（自 mod_kafka 上移，两模块共用） |
| `reader_modules/all_modules.go` | 注册新模块 |
| `tests/integration/implementation/scenario-LR03-mysql-write/` | 集成测试：列映射/幂等重放/零值规则/批次拆分/同秒并发不丢行（issue #1417） |
| `tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/` | 集成测试：启动不可达后台重试（TC-04-01）、配置错误 fail-fast（TC-04-02）、运行期断连自动重连（TC-04-03） |

关联文档：[overview.md](./overview.md)（系统总体设计）、[mod_log_mysql.md](../modules/mod_log_mysql/mod_log_mysql.md)（模块参考）、[output-columns.md](../modules/mod_log_mysql/output-columns.md)（列清单）、实施变更文档（[模块引入](../modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md)、[一期·启动重试](../modifications/2026-09-29-mod-log-mysql-startup-connect-retry/design-changes.md)、[二期·运行期重连](../modifications/2026-09-30-mod-log-mysql-runtime-reconnect/design-changes.md)、[issue #1417 幂等键](../modifications/2026-10-13-issue-1417-dedup-key-logid/design-changes.md)）。