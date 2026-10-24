# mod_log_mysql 基础配置

## 配置简介

`mod_log_mysql.conf` 是 `mod_log_mysql` 模块的基础配置文件，用于配置目标 MySQL 连接参数与批量写入参数。模块行为详见 [mod_log_mysql](../../modules/mod_log_mysql/mod_log_mysql.md)。

## 配置描述

### 基础配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| Basic.OpenDebug | Boolean | 是否开启 Debug 模式 | N | 默认值 `false`；开启后会在日志中输出每行组装结果与每次 flush 的批大小 | - |

### MySQL 连接配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| Mysql.Addr | String | MySQL 地址 | Y | 如 `127.0.0.1:3306`；**为空属于配置错误，启动 fail-fast**；地址不可达属于连通性错误，启动不退出、后台重试 | 非空 |
| Mysql.User | String | 用户名 | Y | 专用最小权限账号（仅目标表 INSERT/UPDATE） | 非空 |
| Mysql.Password | String | 密码 | N | 仅存在于本配置文件 | - |
| Mysql.DBName | String | 目标库名 | Y | 如 `bfe_report` | 非空 |
| Mysql.Table | String | 目标表名 | Y | 如 `bfe_ai_request_log`；表由 ai-gateway-api 仓库 `db_ddl_report_mysql.sql` 创建，插件不自动建表 | 非空 |
| Mysql.ConnectTimeoutMs | Integer | 单次 connect/ping 超时（毫秒） | N | 默认值 `3000`；约束单次建连尝试上限（`PingContext` + DSN `timeout` 拨号参数），防止目标 IP 被黑洞时卡在 OS 级 TCP 超时 | > 0 |
| Mysql.ConnectRetryIntervalMs | Integer | 建连重试间隔（毫秒） | N | 默认值 `3000`；启动未连通或运行期断连时后台重试的间隔，直至建连成功 | > 0 |
| Mysql.HealthPingIntervalMs | Integer | UP 期健康探活间隔（毫秒） | N | 默认值 `0`（关闭）；>0 时 supervisor 按间隔 `PingContext`，失败即置 DOWN 并触发重连——面向半开连接（防火墙断链、NAT 超时）场景更早发现断连 | >= 0 |

### 批量写入配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| Writer.QueueSize | Integer | 缓冲队列容量（条） | N | 默认值 `2000`；断连期可缓存的行数上限，超窗丢弃（计 `SENT_MYSQL_CHN_FULL`） | > 0 |
| Writer.BatchSize | Integer | 攒批条数 | N | 默认值 `200` | > 0 |
| Writer.FlushIntervalMs | Integer | 攒批超时（毫秒） | N | 默认值 `2000`；与 BatchSize 先到先发 | > 0 |
| Writer.MaxRetries | Integer | 写失败重试次数 | N | 默认值 `3`；退避间隔 200ms/400ms/800ms；耗尽即触发断连重连，持有该批待重连后优先补写，补写仍失败才丢弃（计 `SEND_MYSQL_FAILED`） | > 0 |
| Writer.BatchTimeoutMs | Integer | 单批事务执行超时（毫秒） | N | 默认值 `30000`；`ExecContext` 截止时间 + DSN `writeTimeout`，约束网络分区下的写路径挂起时长。**必须大于正常 P99 批执行时间**，否则大批/索引抖动会被误判为断连 | > 0 |
| Writer.MaxOpenConns | Integer | 连接池最大打开连接数 | N | 默认值 `10` | > 0 |
| Writer.MaxIdleConns | Integer | 连接池最大空闲连接数 | N | 默认值 `5` | > 0 |

## 配置示例

```ini
[Basic]
OpenDebug = false

[mysql]
# MySQL address, e.g. 127.0.0.1:3306
Addr = 127.0.0.1:3306

# dedicated minimal-privilege account (INSERT/UPDATE on target table only)
User = report
Password = ******

# target database and table (table created by ai-gateway-api repo DDL, see db_ddl_report_mysql.sql)
DBName = bfe_report
Table = bfe_ai_request_log

# single connect/ping timeout in milliseconds (default 3000)
ConnectTimeoutMs = 3000

# interval between connect retries in milliseconds (default 3000)
# (log-reader stays up and retries in background when mysql is unreachable
#  at startup or lost at runtime)
ConnectRetryIntervalMs = 3000

# UP-period health ping interval in milliseconds (default 0 = disabled)
HealthPingIntervalMs = 0

[Writer]
# record buffer queue size (records)
QueueSize = 2000

# batch size (records)
BatchSize = 200

# batch flush interval in milliseconds
FlushIntervalMs = 2000

# max retries on write failure (backoff 200ms/400ms/800ms)
# (after exhaustion the batch is held and rewritten once reconnect succeeds)
MaxRetries = 3

# single batch transaction timeout in milliseconds (default 30000)
# (ExecContext deadline and driver writeTimeout; must exceed the normal P99
#  batch execution time)
BatchTimeoutMs = 30000

# connection pool
MaxOpenConns = 10
MaxIdleConns = 5
```

## 监控指标

`mod_log_mysql` 模块注册以下监控指标（入口 `/monitor/mod_log_mysql`、`/monitor/mod_log_mysql_diff`）：

| 指标名 | 含义 |
| ------ | ---- |
| RECEIVED_LOGS | Update 收到的日志条数 |
| RECEIVED_REQ | 其中 request 类型条数 |
| CONVERT_FAILED | 行组装失败条数 |
| SENT_TO_MYSQL | 成功入队条数（断连期照常增长，行在缓冲队列） |
| SENT_MYSQL_CHN_FULL | 队列满丢弃条数（背压） |
| SEND_MYSQL_FAILED | 重试耗尽且重连后补写仍失败、丢弃的批数（运行期断连本身不贡献） |
| WRITE_BATCH_SIZE | 每批实际条数累计 |
| MYSQL_CONN_STATE | 建连状态 `UP`/`DOWN`（state 键值） |
| MYSQL_CONN_RETRY | 建连重试累计次数 |
| MYSQL_CONN_OK | 建连成功累计次数 |
| MYSQL_CONN_LOST | 运行期断连次数（写重试耗尽/探活失败触发） |
| LOGID_EMPTY | `logid` 为 0 的明细行数（bfenetworks/bfe#1417）：该行无法按请求级判别列去重，退化为旧四元组语义（同秒同 Key 同模型的并发请求仍会互相覆盖）。正常应为 0——bfe 侧已保证 `logid` 非 0，非 0 即需排查 |
| VALUE_NORMALIZED | 因超界被 clamp 到列上限的字段值个数（按字段值计，非按行；issue #12）。稳态恒 0；非 0 说明上游（BFE 侧）产生了超出 DDL 列类型的值，需排查取值来源 |
| ROW_DROPPED_DATA | 因行定位类数据错误（MySQL 1264/1406 + `at row N`）被剔除丢弃的行数（issue #12）。正常路径（写入前归一化生效后）恒 0；非 0 说明出现了范围表未覆盖的列或新错误码，需补范围表（剔除按行进行，不触发断连、不计 `SEND_MYSQL_FAILED`） |
