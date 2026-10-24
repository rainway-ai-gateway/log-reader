# mod_log_mysql

## 模块简介

`mod_log_mysql` 是 `log-reader` 的 MySQL 输出模块。它负责消费解析后的 BFE 访问日志（`bfe-access-pb` protobuf 格式），将日志组装为固定列序的数据行，**批量幂等写入 MySQL 明细表 `bfe_ai_request_log`**，为 ai-gateway-api 的报表查询 API 提供数据源。

与 `mod_kafka`（转 JSON 发 Kafka，供 Doris 链路）不同，本模块面向「轻量形态」部署：无需 Kafka/Doris/Grafana，日志直接落 MySQL。

主要功能包括：

- 消费 BFE 访问日志（仅 `Request` 类型，会话日志丢弃），按固定列清单组装数据行
- 批量写入 MySQL：缓冲队列 + 单写协程攒批（条数/超时双阈值），单事务多值 `INSERT ... ON DUPLICATE KEY UPDATE` 幂等覆盖
- **启动连接容错**：MySQL 不可达时进程不退出，后台按 `ConnectRetryIntervalMs` 持续重试；建连成功前新日志在缓冲队列堆积，连通后自动排空补写
- 背压保护：队列满非阻塞丢弃并计数，写失败指数退避重试
- web-monitor 监控：`mod_log_mysql` / `mod_log_mysql_diff`

## 数据流

```
Init: 尽力建连一次（Connect：open + 超时 ping）
  ├─ 成功 → MYSQL_CONN_STATE=UP（与常规启动完全一致）
  └─ 失败 → Error 日志一次，STATE=DOWN（非致命）
Start: 起 writeLoop + 常驻 connSupervisor
connSupervisor: UP 期等待 markDown 触发（可选周期探活 HealthPingIntervalMs）；
                DOWN 期按 ConnectRetryIntervalMs 重试 Connect，成功 → STATE=UP 并唤醒写协程
writeLoop: DOWN 期挂起不消费（行数据堆积）；UP 后按批攒批
           （BatchSize 或 FlushIntervalMs）→ 事务批插（BatchTimeoutMs 超时约束）
Update: 过滤仅 BfeLogType_Request → mod_fields 抽取 → field_mapper 组装行
  → recordCh（容量 QueueSize，满丢弃计数 SENT_MYSQL_CHN_FULL）
写失败: 退避重试 MaxRetries 耗尽 → markDown（STATE=DOWN、MYSQL_CONN_LOST+1、
        持有该批）→ 重连成功后优先补写；补写仍失败才计 SEND_MYSQL_FAILED 丢弃
Close: 两协程退出；writeLoop drain 残留批后最终 flush；
       从未连通则丢弃缓冲行并记 Info 日志
```

## 连接容错行为（启动重试 + 运行期断连自动重连）

启动时 MySQL（`bfe_report`）未就绪**不再导致 log-reader 退出**；运行期 MySQL 挂掉后**自动重连**，不再线性丢批。行为分界：

| 错误类别 | 示例 | 行为 |
| -------- | ---- | ---- |
| 配置错误 | 配置文件缺失/格式错误；`Mysql.Addr`/`User`/`DBName`/`Table` 为空 | **仍 fail-fast**：`Init` 返回错误，进程退出 |
| 连通性错误 | 网络不可达、MySQL 未启动/被杀、认证失败（1045）、库不存在（1049） | **后台重试**：`Init`/写路径不失败退出，`connSupervisor` 按 `ConnectRetryIntervalMs` 持续建连 |

断连期间（启动 DOWN 或运行期 markDown 后）的语义：

- **缓冲**：`Update` 照常入队，行数据在 `QueueSize` 容量的队列中堆积；重连后 writeLoop 按批排空补写，运行期断连时失败批**持有优先补写**（不占 `QueueSize`）。幂等键保证补写/重放不产生重复行；
- **背压**：队列满时 `Enqueue` 丢弃并计 `SENT_MYSQL_CHN_FULL`，与连通期语义一致。可容忍的断连窗口 ≈ `QueueSize` ÷ 日志速率，超窗即丢，需按部署速率评估 `QueueSize`；
- **超时控制**：建连受 `ConnectTimeoutMs` 约束（`PingContext` + DSN `timeout` 拨号参数）；单批事务受 `BatchTimeoutMs` 约束（`ExecContext` + DSN `writeTimeout`）——网络分区（DROP）下写路径秒级失败而非挂死到 OS 级 TCP 超时；
- **日志节流**：启动/重连失败第 1–3 次 Error、之后每 10 次 Warn 一条；建连成功 Info 一次；`markDown` 时 Error 一条（含原因）；
- **认证失败/库不存在**靠重试不自愈：重试日志持续提示根因，修复方式是改配置重启（log-reader 无配置热加载）；
- **可选探活**：`HealthPingIntervalMs > 0` 时 supervisor 在 UP 期周期 `PingContext`，失败即 `markDown`——面向半开连接（防火墙断链、NAT 超时）场景更早发现断连，默认关闭；
- **行序**：断连窗口内 pending 批优先补写 + 队列重排，窗口内行序与到达序可能不一致；消费侧以 `log_time`+`logid` 为准，分钟聚合报表不受影响（该契约自 bfenetworks/bfe#1417 修复后才真正成立——此前 `logid` 恒 0，不具备定位能力；修复后唯一键含 `logid`，同秒并发各自成行）。

## 基础配置

模块配置文件说明详见 [mod_log_mysql.conf](../../configuration/mod_log_mysql/mod_log_mysql.conf.md)。

启用方式（`config.conf`）：

```ini
[PbAccessLogConf]
LogFile = /home/work/bfe/log/pb_access3.log
Modules = mod_log_mysql
```

> 表结构 DDL 不在本仓库：随 ai-gateway-api 仓库 `db_ddl_report_mysql.sql` 发布（与聚合表 `bfe_ai_metrics_1m` 同文件），部署时 schema-first 先建表再启用插件。列清单与字段语义见 [output-columns.md](./output-columns.md)。

## 输出列

`bfe_ai_request_log` 共 99 列，与 Doris 明细表**同名同列**（差异仅 `ARRAY<STRUCT>` 列在本表为 `JSON` 类型）。完整列清单（含类型、来源、零值规则、JSON 结构）参见 [output-columns.md](./output-columns.md)。

| 列类别 | 主要列 |
| ------ | ------ |
| 主键/基础 | `hostid`、`log_time`、`ai_apikey_id`、`ai_requested_model`、`logid`、`product`、`log_tag` |
| 客户端连接 | `client_ip`、`client_network`、`is_trust_src_ip`、`req_num`、`session_id`、`bfe_ip`、`sock_src_ip`、`vip`、`vip6` |
| 错误 | `err_code`、`err_msg` |
| 请求 | `proto`、`header_host`、`origin_uri`、`final_uri`、`method`、`content_type`、`x_forward_for`、`accept_language`、`authorization`、`transfer_encoding`、`referrer`、`user_agent`、`delegation`、`uid`、`cookie`、`req_headers`(JSON)、`req_header_len`、`req_body_len` |
| 路由 | `cluster`、`sub_cluster`、`backend_info`、`backend_retry` |
| 响应 | `res_status_code`、`res_header_len`、`res_body_len`、`res_content_type`、`res_location`、`res_transfer_encoding`、`res_headers`(JSON) |
| 耗时（毫秒） | `all_time`、`read_client_time`、`cluster_serve_time`、`backend_serve_time`、`write_client_time`、`connect_backend_time`、`proxy_delay_time`、`session_offset_time` |
| API Key 标签（打平） | `level1Name`/`level1` … `level5Name`/`level5` |
| AI 可观测（标量） | `ai_target_model`、`ai_stream`、`ai_input_tokens`、`ai_output_tokens`、`ai_total_tokens`、`ai_cache_read_tokens`、`ai_cache_write_tokens`、`ai_audio_input_tokens`、`ai_audio_output_tokens`、`ai_image_count`、`ai_ttft_us`、`ai_tpot_us`、`ai_provider`、`ai_protocol`、`ai_mode`、`ai_retry_count`、`ai_cost_value`、`ai_cost_currency`、`ai_auth_reject_reason` |
| AI 可观测（JSON） | `ai_route_rule_hits`、`ai_cluster_key_names`、`ai_rate_limit_hits`、`ai_auth_reject_quota_plans`、`ai_auth_hit_quota_plans` |
| AI 缓存/镜像/意图 | `ai_cache_status`、`mirror_hit`、`mirror_cluster`、`ai_intent_question`、`ai_intent_answer`、`ai_intent_confidence`、`ai_intent_source`、`ai_intent_latency_us`、`ai_intent_cache_hit`、`ai_intent_questions_version` |

## 写入语义要点

- **幂等**：唯一键 `(hostid, log_time, ai_apikey_id, ai_requested_model, logid)`，冲突覆盖更新——只有**同一请求重放**（同 `logid`）才命中覆盖，故 log-reader 重启补读（`-b`）或重发不会产生重复行；同秒同 Key 同模型的并发请求各自成行（bfenetworks/bfe#1417 修复前唯一键缺 `logid`，这类请求会互相覆盖、丢 (n−1)/n 行）；
- **零值规则**：可空字符串空串写 `NULL`（下游聚合 `IFNULL/COALESCE` 归一，与 Doris 口径一致）；NOT NULL 字符串列（AI 缓存/镜像/意图段字符串列）空串原样写 `''`；数值/布尔原样写入；空 JSON 值写 `NULL`；`ai_intent_confidence`/`ai_intent_latency_us`/`ai_intent_cache_hit` 三个可空意图数值列为 proto optional，未设置（指针 nil）写 `NULL`（=未求值），显式置 0 写 0（与 report 查询契约一致）；
- **标签打平**：`levelNName/levelN` 在写入时由 `ai_apikeytags` 打平，不等价于 Doris 侧由 Routine Load 表达式打平，效果一致；
- **范围归一化**：17 个数值列按 DDL 列类型声明写入上限（`INT` 2147483647、`res_status_code` SMALLINT 32767、`backend_retry` TINYINT 127），超界值写入前 clamp 到列上限并计数 `VALUE_NORMALIZED`（issue #12：pb `uint32` 值域大于列类型，原样写入会触发 MySQL 1264 使整批失败）；
- **行级隔离**：批写入命中可定位到行的数据错误（1264/1406 + `at row N`）时剔除该行、立即重写剩余行（不占退避重试次数、不断连），计数 `ROW_DROPPED_DATA`——单行异常不再毒化整批（issue #12）；
- **分区分区**：表按天 RANGE 分区（`TO_DAYS(log_time)`），新分区创建与过期分区 DROP 由报表查询侧（ai-gateway-api）的分区管理 JOB 负责，本模块不参与。

## 监控指标

监控入口：`http://<host>:<port>/monitor/mod_log_mysql` 及 `/monitor/mod_log_mysql_diff`。

| 指标 | 含义 | 异常信号 |
| ---- | ---- | -------- |
| `RECEIVED_LOGS` | Update 收到的日志条数 | 与 `bfe_reader` 的 `SUM_PB_RECORD` 长期不同步 |
| `RECEIVED_REQ` | 其中 request 类型条数 | — |
| `CONVERT_FAILED` | 行组装失败条数 | 持续增长需排查字段映射 |
| `SENT_TO_MYSQL` | 成功入队条数 | 停滞 = 写入管道异常 |
| `SENT_MYSQL_CHN_FULL` | 队列满丢弃条数（背压） | 持续增长说明 MySQL 写入跟不上，调大 QueueSize/BatchSize 或排查库端 |
| `SEND_MYSQL_FAILED` | 重试耗尽且重连后补写仍失败、丢弃的批数 | >0 即需告警（结构性错误：表被删/权限收回）；运行期断连本身不再贡献该计数（二期起） |
| `WRITE_BATCH_SIZE` | 每批实际条数累计 | diff 均值观测批大小是否符合预期 |
| `MYSQL_CONN_STATE` | 建连状态 `UP`/`DOWN` | `DOWN` = MySQL 不可达（启动未连通或运行期断连），supervisor 在后台重试；`UP→DOWN` 跳变或长期 `DOWN` 建议告警 |
| `MYSQL_CONN_RETRY` | 建连重试累计次数 | 随 `ConnectRetryIntervalMs` 节奏增长即重试循环正常 |
| `MYSQL_CONN_OK` | 建连成功累计次数 | 启动以来成功建连次数（正常运行恒为 1，每次运行期重连 +1） |
| `MYSQL_CONN_LOST` | 运行期断连次数 | 写重试耗尽或探活失败触发；持续增长说明链路抖动或 MySQL 不稳定 |
| `LOGID_EMPTY` | `logid` 为 0 的明细行数（bfenetworks/bfe#1417） | **非 0 即需排查**：该行无法按请求级判别列去重，退化为旧四元组语义（同秒同 Key 同模型的并发请求仍会互相覆盖）。bfe 侧已保证 `logid` 非 0 |
| `VALUE_NORMALIZED` | 因超界被 clamp 到列上限的字段值个数（issue #12，按字段值计） | **稳态恒 0；非 0 即需排查**：说明上游产生了超出 DDL 列类型的值（如 `proxy_delay_time` 超 `INT` 上限），写入侧已 clamp 到列上限保证落库，但取值来源需在 BFE 侧核查 |
| `ROW_DROPPED_DATA` | 因行定位类数据错误（1264/1406 + `at row N`）被剔除丢弃的行数（issue #12） | **正常路径恒 0；非 0 即需排查**：说明出现了范围表未覆盖的列或新错误码，需补充列范围表。剔除按行进行：不触发断连（`MYSQL_CONN_LOST` 不增）、不计 `SEND_MYSQL_FAILED` |

## 排障要点

1. 启动报连接/权限错误：模块**不再退出**，进程保持运行并在后台重试（`MYSQL_CONN_STATE=DOWN`）——检查 `mod_log_mysql.conf` 的 DSN 与专用账号权限（仅需目标表 INSERT/UPDATE，无 DDL/DELETE），修复后重启进程；若 `DOWN` 且重试日志持续为认证失败（1045）/库不存在（1049），改配置重启，重试本身不会自愈；
2. `MYSQL_CONN_STATE=DOWN` 期间 `SENT_TO_MYSQL` 仍在增长：日志在缓冲队列堆积，关注 `SENT_MYSQL_CHN_FULL` 是否持续增长（超窗丢弃），连通后积压自动补写；
3. `SENT_TO_MYSQL` 不增长但 `RECEIVED_LOGS` 增长：写协程异常，查进程日志与 pprof；
4. `SEND_MYSQL_FAILED` 增长：先 `SHOW ENGINE INNODB STATUS` / 检查唯一键冲突外的错误（死锁、超时、磁盘）；该计数在运行期断连下应接近 0（断连触发的是 DOWN/重连而非丢批），若随断连增长需排查 `BatchTimeoutMs` 是否小于正常批执行时间。恢复后对 pb 日志 `bfe-pblog-tool` 导出比对缺口，停插件后 `-b` 补读重放；
5. `MYSQL_CONN_LOST` 增长 / `MYSQL_CONN_STATE` 反复跳变：链路抖动或 MySQL 不稳定，结合 `MYSQL_CONN_RETRY` 增长速率判断；持续为认证失败（1045）则改配置重启；
6. 表不存在错误：部署流程漏执行 ai-gateway-api 仓库的 `db_ddl_report_mysql.sql`。

## 与 mod_kafka 的关系

两个模块共用 `reader_modules/mod_fields` 的字段抽取与类型转换逻辑（IP 转点分、`backend_info` 拼 `IP:Port`、apikeytags 打平、hostid 注入等）：mod_kafka 在其上做 `json.Marshal`，mod_log_mysql 在其上做行数组组装（其中三个可空意图数值列按 proto optional 指针判空直接取 RequestLog 字段）。`FieldMode/FieldNames` 字段裁剪配置仅属于 mod_kafka；mod_log_mysql 始终写全量 99 列。

**对齐性约定**（2026-10-01 起，见 `doc/modifications/2026-10-01-align-mod-kafka-output-with-mod-log-mysql/`）：

- mod_kafka 的 Default 输出字段集（`FieldMode=default`）= mod_log_mysql 的写入字段集（88 个直接字段 + `timestamp`/`ai_apikeytags` 两个派生源；`log_time`、`level1Name~level5` 为两侧各自派生，不单独输出）；
- `ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit` 三个 proto optional 列：未设置时两侧均为空值（MySQL NULL / Kafka JSON `null`），可区分"未求值"与"显式置零"；
- 新增报表字段必须两侧同 PR 落地（注册表 Default + `columnDefs` 列 + 消费侧 DDL），并以 `field_registry_test.go` 的 `TestFieldRegistry_DefaultFieldsAlignedWithMysqlWriter` 锁定对齐关系。
