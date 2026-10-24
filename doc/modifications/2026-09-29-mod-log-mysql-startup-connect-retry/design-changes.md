# log-reader mod_log_mysql 启动连接失败后台重试（去 fail-fast）

## 背景

`mod_log_mysql` 是 fail-fast 的：`Init` 里 `sql.Open` + `db.Ping()` 失败即返回
error，经 `BfeLogReader.Start` 上抛至 `main` 后 `Exit(1)`，log-reader 进程直接退出
（`reader_modules/mod_log_mysql/mod_log_mysql.go:114-126`）。

实际部署中 MySQL（`bfe_report`）尚未就绪是常见瞬态：启动顺序、网络未通、主从切换
等。进程退出后无人拉起即造成日志链路缺口，且退出不解决任何问题。而写入侧
（`record_writer.go`）本身是健壮的：缓冲队列 + 退避重试 + 幂等批插，运行期写失败
并不会让进程退出——"连不上就退出"只发生在启动一个点。

本改动把"连接生命周期"从 `Init` 挪进 `RecordWriter`：启动时 MySQL 不可达不再退出，
由模块后台持续重试，直到建连成功、自动排空积压。

## 修改目标

1. `mod_log_mysql.Init` 对**连通性错误**返回 nil（后台重试），对**配置错误**
   （配置文件缺失/必填字段为空）仍 fail-fast。
2. `RecordWriter` 自建连管理：`Connect()`（超时控制）+ 重试协程 `connectLoop` +
   建连信号 `connCh`；`writeLoop` 未连通时挂起不消费。
3. 断连期行为复用既有背压语义：`Update` 照常入队缓冲（容量 `QueueSize`），队列满
   丢弃计 `SENT_MYSQL_CHN_FULL`；连通后积压自动排空，幂等写保证补写安全。
4. 新增监控：`MYSQL_CONN_STATE`（UP/DOWN）、`MYSQL_CONN_RETRY`、
   `MYSQL_CONN_OK`；重试日志节流（防刷爆日志文件）。
5. 配置新增 `ConnectTimeoutMs` / `ConnectRetryIntervalMs`（可选带默认值，旧配置
   零改动可用）。
6. 补充单元测试；更新模块文档、配置文档与 CHANGELOG。

## 总体设计

### 启动时序

```
Init：
  LoadConfig（失败仍 return err，fail-fast 边界不变）
  → NewRecordWriter(conf, state)          // 不再传 db；ch 照常创建
  → writer.Connect()                      // 尽力同步建连一次
       成功：state MYSQL_CONN_STATE=UP    // 与今天行为完全一致
       失败：Error 日志一次，STATE=DOWN   // 非致命，Init 继续
  → writer.Start()                        // 起 writeLoop；未连通时同时起 connectLoop
  → RegisterHandlers（不变）

connectLoop（仅启动未连通时存在）：
  每 ConnectRetryIntervalMs tick → Connect()
    失败：MYSQL_CONN_RETRY+1，节流日志（前 3 次 Error，之后每 10 次 Warn）
    成功：MYSQL_CONN_OK+1，STATE=UP，close(connCh) 唤醒 writeLoop，协程退出

writeLoop：
  入口闸：select connCh / stopCh          // 未连通前挂起，ch 中积压行
  连通后：现有攒批/flush/drain 逻辑全部不变
```

### 断连期数据流（与现有背压机制的关系）

```
BFE pb 日志 → Update → ToRow → Enqueue → ch（容量 QueueSize）
                                              │ 未连通：writeLoop 不消费，行堆积
                                              │ 满：Enqueue=false，计 SENT_MYSQL_CHN_FULL
MySQL 恢复 → connectLoop 建连成功 → writeLoop 开始消费 → 按 BatchSize/FlushIntervalMs
             排空积压 → INSERT ... ON DUPLICATE KEY UPDATE（幂等，补写安全）
```

### 错误分类（本改动的关键分界）

| 类别 | 示例 | 处理 |
|------|------|------|
| 配置错误 | 配置文件缺失/格式错误；`Mysql.Addr`/`User`/`DBName`/`Table` 为空 | **仍 fail-fast**：`Init` 返回 error，进程退出 |
| 连通性错误 | `sql.Open`/`PingContext` 失败：网络不可达、MySQL 未启动、认证失败（1045）、库不存在（1049） | `Init` 返回 nil，后台持续重试 |

认证失败/库不存在靠重试不自愈，修复方式是改配置重启；持续重试 + 节流日志可持续
提示根因，进程留在场上等待（log-reader 无配置热加载，重启本已是常规操作）。

## 详细改动

### 1. `reader_modules/mod_log_mysql/record_writer.go`

**结构改动**：

```go
type RecordWriter struct {
    db       atomic.Pointer[sql.DB] // nil = 未连通；跨协程（connectLoop 写 / writeLoop 读）
    conf     *ConfModLogMysql
    ch       chan []interface{}     // 容量 QueueSize，NewRecordWriter 即创建（与连通与否无关）
    state    *module_state2.State
    stopCh   chan struct{}
    connCh   chan struct{}          // 建连成功信号，close 一次（sync.Once 保护）
    connOnce sync.Once
    wg       sync.WaitGroup
    // stmtHead/stmtRow/stmtTail 构造逻辑不变
}
```

`NewRecordWriter` 签名去掉 `db *sql.DB` 参数（db 改由 `Connect` 注入）。

**新增方法**：

```go
// Connect 建立连接并验证连通性；失败不残留句柄，可反复调用（二期运行期重连预留）
func (w *RecordWriter) Connect() error {
    dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4",
        w.conf.Mysql.User, w.conf.Mysql.Password, w.conf.Mysql.Addr, w.conf.Mysql.DBName)
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return err
    }
    // PingContext 超时：目标 IP 被黑洞时 db.Ping() 可能卡到 OS 级 TCP 超时（分钟级），
    // 用 ConnectTimeoutMs 约束单次尝试上限，重试节奏才真正可控
    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(w.conf.Mysql.ConnectTimeoutMs)*time.Millisecond)
    defer cancel()
    if err = db.PingContext(ctx); err != nil {
        db.Close()
        return err
    }
    db.SetMaxOpenConns(w.conf.Writer.MaxOpenConns)
    db.SetMaxIdleConns(w.conf.Writer.MaxIdleConns)
    w.db.Store(db)
    w.connOnce.Do(func() { close(w.connCh) }) // 唤醒 writeLoop
    return nil
}

func (w *RecordWriter) Connected() bool { return w.db.Load() != nil }
```

**`Start` 按状态起协程**：

```go
func (w *RecordWriter) Start() {
    w.wg.Add(1)
    go w.writeLoop()
    if !w.Connected() {          // 仅启动未连通时启动重试
        w.wg.Add(1)
        go w.connectLoop()
    }
}
```

**`writeLoop` 入口加闸**（攒批/flush/drain 逻辑不变）：

```go
func (w *RecordWriter) writeLoop() {
    // defer recover / wg.Done 不变

    // 闸：未连通前不消费 ch，行数据在缓冲队列中堆积
    select {
    case <-w.connCh: // 建连成功（Init 前已连通则 connCh 已关闭，立即通过）
    case <-w.stopCh: // 未连通即 Close：直接退出
        return
    }
    // 以下 ticker 攒批逻辑不变 ...
}
```

**新增重试协程**：

```go
func (w *RecordWriter) connectLoop() {
    defer w.wg.Done()
    ticker := time.NewTicker(time.Duration(w.conf.Mysql.ConnectRetryIntervalMs) * time.Millisecond)
    defer ticker.Stop()

    var attempts int
    for {
        select {
        case <-w.stopCh:
            return
        case <-ticker.C:
        }
        attempts++
        if err := w.Connect(); err != nil {
            w.state.Inc("MYSQL_CONN_RETRY", 1)
            // 日志节流：第 1-3 次 Error，第 4 次起每 10 次 Warn 一次（约每 30s 一条）
            logRetryFailure(attempts, err)
            continue
        }
        w.state.Inc("MYSQL_CONN_OK", 1)
        w.state.Set("MYSQL_CONN_STATE", "UP")
        log.Logger.Info("mod_log_mysql: connected to mysql, draining buffered records")
        return
    }
}
```

**`Close` 语义**：

- `close(stopCh)`：`connectLoop` 若在运行直接退出（`wg.Wait()` 收割）；
- `writeLoop`：未连通（`connCh` 未关闭）则走 `stopCh` 分支退出——`ch` 中积有的未
  写出行记一条 Info 日志（"dropped N buffered records"）后丢弃（进程退出边界，
  是既有 drain 语义的自然延伸）；已连通则走现有 drain + final flush +
  `db.Close()` 路径，**不变**。

### 2. `reader_modules/mod_log_mysql/mod_log_mysql.go`

**`Init` 改造**（`LoadConfig` 失败仍 return err，fail-fast 边界不变）：

```go
// 原 fail-fast 段（114-128 行：sql.Open + db.Ping + return err）整体移除，替换为：
m.mapper = NewFieldMapper()
m.writer = NewRecordWriter(m.conf, &m.state) // 签名变化，不再传 db
if err := m.writer.Connect(); err != nil {
    // 非致命：记一次 Error，交由 writer 后台重试
    log.Logger.Error("%s.Init(): mysql not reachable (%v), will retry in background", m.name, err)
    m.state.Set("MYSQL_CONN_STATE", "DOWN")
} else {
    m.state.Set("MYSQL_CONN_STATE", "UP")
}
m.writer.Start()
```

`Update` 路径一行不改（`mapper.ToRow` + `writer.Enqueue` 断连期照常工作）。

**`recordWriter` 接口扩展**：

```go
type recordWriter interface {
    Enqueue(row []interface{}) bool
    Connect() error     // 新增
    Connected() bool    // 新增
    Start()
    Close()
}
```

**`COUNTER_KEYS` 扩展**：

```go
var COUNTER_KEYS = []string{
    // ... 既有 7 项不变（RECEIVED_LOGS / RECEIVED_REQ / CONVERT_FAILED /
    //                      SENT_TO_MYSQL / SENT_MYSQL_CHN_FULL / SEND_MYSQL_FAILED /
    //                      WRITE_BATCH_SIZE）
    "MYSQL_CONN_RETRY", // 建连重试累计次数（每次失败 +1）
    "MYSQL_CONN_OK",    // 建连成功累计次数
}
// 另新增 state 键值（非 counter）：MYSQL_CONN_STATE = UP / DOWN，state.Set 用法
// 与 bfe_reader 的 SERVER_READY 同款，web monitor 自动带出
```

### 3. `reader_modules/mod_log_mysql/conf_mod_log_mysql.go`

`ConfMysql` 新增两个可选字段，`ConfModLogMysqlCheck` 按现有风格校验
（`<= 0` 打 Warn 填默认值）：

```go
type ConfMysql struct {
    Addr                   string
    User                   string
    Password               string
    DBName                 string
    Table                  string
    ConnectTimeoutMs       int // 新增；单次 connect/ping 超时，默认 3000
    ConnectRetryIntervalMs int // 新增；建连重试间隔，默认 3000
}
```

### 4. 配置样例 `conf/mod_log_mysql/mod_log_mysql.conf`

`[mysql]` 段追加两行（其余不变）：

```ini
[mysql]
Addr = 127.0.0.1:3306
User = report
Password = ******
DBName = bfe_report
Table = bfe_ai_request_log

# single connect/ping timeout in milliseconds (default 3000)
ConnectTimeoutMs = 3000

# interval between connect retries in milliseconds (default 3000)
ConnectRetryIntervalMs = 3000
```

### 5. 单元测试

`recordWriter` 接口的现有 fake 同步补 `Connect`/`Connected` 两个方法。为覆盖重试
时序，`RecordWriter` 预留建连注入缝（实现细节建议：包级 `var connectFunc = sql.Open`
或持有可替换 `connector`），测试替换为"前 N 次失败、第 N+1 次成功"的桩：

| 用例 | 断言 |
|------|------|
| `Init`：MySQL 不可达（如 `127.0.0.1:1`） | 返回 nil；`MYSQL_CONN_STATE=DOWN`；monitor handlers 已注册 |
| `Init`：配置缺 `Mysql.Addr` | 仍返回 error（fail-fast 边界回归） |
| 断连期 `Update` 入队 | 行在 `ch` 缓冲；超 `QueueSize` 后 `Enqueue=false`，计 `SENT_MYSQL_CHN_FULL` |
| 重试桩第 N+1 次成功 | `Connected()=true`；`MYSQL_CONN_RETRY=N`、`MYSQL_CONN_OK=1`、STATE 转 UP；积压行被排空写出 |
| 未连通即 `Close` | 两协程限时退出，无 goroutine 泄漏；缓冲行记日志丢弃 |
| 连通后 `Close` | 现有 drain + final flush + `db.Close()` 行为不变（回归） |
| 连通后 `writeBatch` 持续失败 | 维持现状：退避重试后丢弃计 `SEND_MYSQL_FAILED`（回归） |
| `ConfModLogMysqlCheck`：新字段 `<=0`/缺省 | 填默认值 3000，不报错 |

### 6. 集成测试（新增 scenario-LR04）

新增场景 `scenario-LR04-mysql-startup-connect-retry`，与 LR03 同目录约定：

```text
tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/
├── lr04_mysql_startup_retry_test.go
└── testdata/                        # conf 模板，自 LR03 模板拷贝适配
tests/integration/测试设计文档/scenario-LR04-mysql-startup-connect-retry/
└── 场景说明.md + TC 文档            # 体例对齐 LR03
```

#### 6.1 common harness 扩展

| 文件 | 扩展 | 理由 |
|------|------|------|
| `common/mysql_env.go` | `MysqlEnv` 新增 `Pause()` / `Unpause()`：仅 testcontainers 后端可用（底层 `container.Pause/Unpause`，Docker pause 冻结容器进程，真实模拟 MySQL 不可达）；外部 `LR_MYSQL_DSN` 后端调用即 `t.Skip` | 需要"先不可达、后恢复"的闭环，未监听端口只能模拟拒绝、无法模拟恢复 |
| `common/process_env.go` | 新增退出探测：`StartLogReader` 保持旧签名不变，内部转调新增的 `StartLogReaderEx(...)`，后者额外返回 `waitExit(timeout) (exited bool, code int)`；另加 monitor 抓取工具 `WaitMonitorValue(t, port, handler, key, want, timeout)`——轮询 `http://127.0.0.1:<port>/monitor/<handler>` 至目标键值出现 | "进程不退出"是本改动的核心断言，现有 harness 只返回 `stop func`，无法感知进程退出；`MYSQL_CONN_STATE` 等键值只能经 monitor HTTP 观测 |
| `common/config_builder.go` | `MysqlAddr` 注入已具备（指向被 Pause 的容器地址即可）；导出 `FreePort(t)`（现 `freePort` 为小写，供 LR04 取"必失败"地址场景复用） | LR04 的 conf 生成与 LR03 完全同路 |

测试库名前缀现硬编码为 `bfe_report_lr03_*`，LR04 用时序上不会冲突，可复用；如需区分可加后缀参数（可选，不强制）。

#### 6.2 测试用例

**TC-04-01 启动时 MySQL 不可达：进程存活 + 后台重试 + 恢复自动补写（核心用例）**

前置：testcontainers 起 MySQL → 建库 → `ApplyDDL` → `mysqlEnv.Pause()`。步骤与断言：

1. 按真实容器地址生成 conf，写 N 条日志（`MakeRequestLog`，N 取 3，对齐 LR03 习惯），启动 log-reader（`-b`）；
2. **不退出**：`waitExit(3s)` 返回 `exited=false`；`/monitor/mod_log_mysql` 可访问（WebServer 正常启动）；
3. **重试进行中**：`WaitMonitorValue` 断言 `MYSQL_CONN_STATE = DOWN` 且 `MYSQL_CONN_RETRY` 持续增长（间隔 ≈`ConnectRetryIntervalMs`）；`dumpLogs` 抽查重试日志节流（前 3 次 Error、之后每 10 次一条 Warn）；
4. **断连期背压**：表行数 = 0（未写出）；`SENT_TO_MYSQL = N`（入队照常，行在缓冲队列）；进程持续存活；
5. `mysqlEnv.Unpause()`；
6. **自动恢复**：`WaitMonitorValue(..., "MYSQL_CONN_STATE", "UP", 30s)` 成功（耗时 ≈ 重试间隔 + Connect）；随后 `waitRows(N)`，积压排空、逐行值正确（复用 LR03 的行断言方式，可下沉 `common` 共用）；
7. **计数**：`MYSQL_CONN_OK = 1`，`MYSQL_CONN_RETRY >= 1`。

说明：Pause 状态下新连接 TCP 挂起而非拒绝，该用例同时覆盖了 `PingContext`
超时路径（无超时控制时此场景会卡到 OS 级 TCP 超时，重试节奏失效）。

**TC-04-02 配置错误仍 fail-fast（负向回归）**

1. 生成 conf 后将 `mod_log_mysql.conf` 的 `Mysql.Addr` 置空（直接改写文件，模拟配置错误）；
2. 启动 log-reader，`waitExit(10s)` 断言 `exited=true` 且退出码非 0；
3. 守护 fail-fast 边界：配置错误立即退出、连通性错误不退出的分界不被本改动破坏。

**TC-04-03 运行期断连（暂不纳入自动化，列二期）**

正常建连写入后 `Pause()`：进程存活、`SEND_MYSQL_FAILED` 增长；`Unpause()` 后
`database/sql` 连接池自行更换底层连接恢复写入（现状能力，非本改动引入）。不纳入
LR04 自动化，原因：Pause 下连接池拨号无超时（DSN 未配 `timeout`），写路径挂起时长
不可控，断言窗口不稳定；一期改用手工验证（见"验证步骤"第 2.4 步），自动化方案见
`../2026-09-30-mod-log-mysql-runtime-reconnect/design-changes.md`（写路径超时 +
重试耗尽触发重连，落地后回补本用例）。

#### 6.3 README 与文档更新

`tests/integration/README.md` "当前覆盖"表新增两行：LR04 启动重连（核心）与 LR04
配置错误 fail-fast（负向）。

### 7. 文档更新

| 文件 | 改动 |
|------|------|
| `doc/modules/mod_log_mysql/mod_log_mysql.md` | 启动行为（非 fail-fast + 后台重试）、断连期缓冲/背压语义、新监控项与排障（`MYSQL_CONN_STATE` 告警口径） |
| `doc/configuration/mod_log_mysql/mod_log_mysql.conf.md` | 新增两个配置项逐项说明（**注意：该文件尚不存在，需新建**，对齐 `doc/configuration/mod_kafka/mod_kafka.conf.md` 体例） |
| `CHANGELOG.md` | 新增条目（行为变更：启动时 MySQL 不可达由进程退出改为后台重试） |
| `README.md` / `readme.txt` | 如涉及 mod_log_mysql 特性/监控项描述则同步 |

## 代码变更说明

- 【修改】`log-reader/reader_modules/mod_log_mysql/record_writer.go`（db 原子指针 +
  `Connect`/`Connected` + `connCh`/`connectLoop` + `writeLoop` 入口闸 + `Close` 双路径）
- 【修改】`log-reader/reader_modules/mod_log_mysql/mod_log_mysql.go`（Init 去 fail-fast、
  `recordWriter` 接口扩展、`COUNTER_KEYS` 扩展 + `MYSQL_CONN_STATE`）
- 【修改】`log-reader/reader_modules/mod_log_mysql/conf_mod_log_mysql.go`（`ConfMysql`
  新增 2 字段 + 校验默认值）
- 【修改】`log-reader/conf/mod_log_mysql/mod_log_mysql.conf`（新增 2 配置样例行）
- 【修改】`log-reader/reader_modules/mod_log_mysql/*_test.go`（fake 补接口方法 +
  新增用例）
- 【修改】`log-reader/tests/integration/common/mysql_env.go`（`Pause`/`Unpause`）
- 【修改】`log-reader/tests/integration/common/process_env.go`（`StartLogReaderEx`
  退出探测 + `WaitMonitorValue` monitor 抓取）
- 【修改】`log-reader/tests/integration/common/config_builder.go`（导出
  `FreePort`）
- 【新增】`log-reader/tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/`
  （用例 + testdata）
- 【新增】`log-reader/tests/integration/测试设计文档/scenario-LR04-mysql-startup-connect-retry/`
- 【修改】`log-reader/tests/integration/README.md`（场景清单加 LR04）
- 【修改】`doc/modules/mod_log_mysql/mod_log_mysql.md`、`CHANGELOG.md`、
  `README.md`/`readme.txt`（如涉及）
- 【新增】`doc/configuration/mod_log_mysql/mod_log_mysql.conf.md`（配置项说明，体例
  对齐 mod_kafka）
- 【新增】`doc/modifications/2026-09-29-mod-log-mysql-startup-connect-retry/design-changes.md`（本文档）

无 `go.mod` 依赖变化（`context`/`sync/atomic` 均标准库）。

## 验证步骤

```bash
cd log-reader

# 1. 静态检查与单测（全绿，含新增重试时序用例）
make check
go test ./reader_modules/mod_log_mysql/... -race

# 1.5 集成测试 LR04（需 Docker 提供 testcontainers；LR_MYSQL_DSN 模式下 LR04 自动 skip）
go test ./tests/integration/implementation/scenario-LR03-mysql-write/... -v   # 回归
go test ./tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/... -v

# 2. 手工端到端（开发机）
#    2.1 停掉 MySQL，启动 ./log_reader -c ../conf -l ../log -s
#        → 进程存活；monitor 页 mod_log_mysql_MYSQL_CONN_STATE=DOWN，
#          MYSQL_CONN_RETRY 递增；重试日志节流（前 3 次 Error，之后约每 30s 一条 Warn）
#    2.2 持续产生访问日志 → SENT_TO_MYSQL 停在 0；队列满时 SENT_MYSQL_CHN_FULL 增长；
#        进程始终存活
#    2.3 启动 MySQL → 日志出现 connected Info；积压排空，SENT_TO_MYSQL 跳涨；
#        bfe_ai_request_log 数据补齐（幂等：重投递无重复冲突）
#    2.4 回归：运行中 kill MySQL → 写失败重试后丢弃计 SEND_MYSQL_FAILED，进程存活
#        （与改动前一致）；MySQL 恢复后观察写入自行恢复（TC-04-03 的手工对照）
#    2.5 回归：MySQL 就绪后正常启动 → 行为与改动前完全一致（无重试协程、同步建连）
#    2.6 回归：Init 时把 mod_log_mysql.conf 的 Mysql.Addr 置空 → 进程退出（配置错误
#        仍 fail-fast）
```

## 影响范围

| 维度 | 影响 |
|------|------|
| 存量部署 | MySQL 正常时启动路径与现状逐行等价（同步建连成功，不起重试协程）；仅"MySQL 不可达"场景行为变化（退出 → 后台重试） |
| 配置 | 新增 2 个可选字段带默认值，旧 conf 零改动可用；`[Writer]` 段语义不变 |
| 框架 | `ReaderModule` 接口、`BfeLogReader.Start`、`main` 零改动；`mod_kafka` 等其他模块零改动 |
| 数据正确性 | 断连期积压行靠唯一键 + `ON DUPLICATE KEY UPDATE` 幂等补写；重复执行安全 |
| 测试 | 新增集成场景 LR04（核心：启动不可达→存活→重试→恢复补写；负向：配置错误仍退出）；`common` harness 增 Pause/Unpause 与进程退出探测，不影响 LR01–LR03 既有用例 |
| 观测 | monitor 新增 `MYSQL_CONN_STATE`/`MYSQL_CONN_RETRY`/`MYSQL_CONN_OK`；`SERVER_READY` 语义不变（MySQL 侧健康度看模块自身 STATE） |

## 依赖与兼容性

- **MySQL**：无新要求（驱动 `go-sql-driver/mysql` 不变，`PingContext` 为
  `database/sql` 标准 API）。
- **集成测试环境**：LR04 依赖 Docker（testcontainers 的 Pause/Unpause 模拟 MySQL
  不可达与恢复）；仅设置 `LR_MYSQL_DSN` 时 LR04 用例自动 skip（外部实例不可随意
  停启），不影响 LR01–LR03 运行。
- **前向兼容**：新二进制 + 旧配置 = 直接可用（新字段走默认值）。
- **回滚**：旧二进制 + 新配置 = **Init 失败**——gcfg.v1 对配置中结构体不存在的字段
  返回 warning 级 `extraData` 错误，现有 `LoadConfig` 未用 `gcfg.FatalOnly` 过滤，
  会把 warning 当错误返回。因此回滚二进制时必须同步回退配置文件。
  （备选小改进，可另起改动：`LoadConfig` 改 `gcfg.FatalOnly(gcfg.ReadFileInto(...))`，
  让未知字段只告警不失败。）
- **已知限制**（维持现状，非本改动引入）：断连期可保存行数上限 = `QueueSize`
  （默认 2000 条），超出即丢（既有背压语义）；认证失败/库不存在靠重试不自愈，
  需改配置重启；运行期断连仍为"重试后丢弃"，自动重连列为可选二期（`Connect`
  已设计为可反复调用，结构已预留）。

---

*文档生成日期：2026-09-29*
