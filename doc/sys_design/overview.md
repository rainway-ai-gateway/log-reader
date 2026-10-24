# log-reader 系统总体设计

## 1. 背景与目标

### 1.1 背景

BFE 数据面的 `mod_access_pb3` 模块把访问日志以 protobuf + b2log 二进制帧写入本地文件（`pb_access3.log`，详见 bfe 仓库 `docs/zh_cn/sys_design/ai_access_log_fields.md`）。需要一个独立进程把这些日志**实时读取、解析并分发给下游存储/管道**，支撑报表等可观测能力——该进程即 log-reader。

下游输出形态有两种：

- **标准形态**：JSON 写 Kafka → Doris → Grafana（现有 `mod_kafka`）；
- **轻量形态**：直接写 MySQL → ai-gateway-api 报表 API → ai-gateway-web（新增 `mod_log_mysql`，见 [mod_log_mysql.md](./mod_log_mysql.md)）。

### 1.2 目标

1. 实时 tail BFE pb 日志文件，秒级送达下游；
2. 感知 BFE 日志轮转（文件改名），无感切换不丢不重；
3. 解析容错：脏数据/半截记录自恢复，单条失败不影响后续；
4. 可插拔的输出模块框架：新增下游只需实现 `ReaderModule` 接口并注册；
5. 输出模块故障隔离：任一模块 panic/慢不影响读取主流程与其他模块；
6. 可观测：读取/解析/各模块独立计数，HTTP 监控页 + pprof。

### 1.3 非目标

- **不做持久化位点（断点续传）**：重启默认从文件尾读，`-b` 从头全量重读作为补救（取舍理由见 §5.2）；
- 不管理旧日志文件的生命周期（备份清理由 BFE 侧 `BackupCount` 决定）；
- 不做流量整形/限流以外的数据转换（字段抽取归各输出模块）；
- 不内置复杂存储后端逻辑——存储细节归输出模块，本框架只负责「读文件 → 解析 → 分发批次」。

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| b2log | BFE 访问日志的二进制封装格式：24 字节头（magic `0xB0AEBEA7`、version、长度、时间戳）+ protobuf 负载，小端序（定义在 `bfe-access-pb` 仓库 `b2log/`） |
| `BfeLog` | pb 顶层消息，含 `log_type`（Request/Session）、`RequestLog`、`SessionLog` |
| LogFileReader | 单文件 reader：打开、按偏移读取、轮转检测（`bfe_log_reader/log_reader.go`） |
| PbLogReader | 读取编排器：驱动读循环、攒缓冲、切帧解析、拆批、分发模块（`bfe_log_reader/pb_log_reader.go`） |
| ReaderModule | 输出模块接口：`Name/Init/Start/Update/Close`（`reader_module/reader_module.go`） |
| hostid | 实例标识，`hostname_netns`（hostname + 网络命名空间 inode），由 log-reader 本机生成并注入每条日志 |
| 轮转（rotate） | BFE log4go 到点把 `pb_access3.log` rename 为带时间戳的备份文件并新建同名文件 |
| DLQ | 死信队列（Kafka topic），发送重试耗尽的消息整批落入 |

## 3. 总体架构

### 3.1 数据流

```
pb_access3.log (b2log 二进制帧)
   │  LogFileReader：每次最多读 1MB，EOF sleep 10ms 轮询
   │  轮转检测：os.Stat 与已打开 fd 做 os.SameFile（inode 比较）
   │    → 读完旧文件残留 → logRelocate() 关旧开新
   ▼
DataBuffer ── PbBuffParse：b2log.BuffParse 切帧 → proto.Unmarshal → []*BfeLog
   │            （magic 失配 tryFindNextStart 重同步；单条失败计数跳过）
   ▼
按 MaxSizePerBatch 拆批
   │  每批 × 每个绑定模块：go module.Update(batch)（panic recover）
   ├─► mod_kafka.Update  → JSON → KafkaProducer → Kafka
   └─► mod_log_mysql.Update → 行组装 → RecordWriter → MySQL
                                  （模块设计见各自 sys_design 文档）
```

### 3.2 核心组件

| 组件 | 位置 | 职责 |
|------|------|------|
| 进程入口 | `main/log_reader.go` | flag 解析、配置加载、模块装配、Web 监控启动、信号处理、优雅退出 |
| 文件读取 | `bfe_log_reader/log_reader.go` | `LogFileOpen`（启动定位到文件尾，`-b` 到头）、`isLogCut`（inode 比较判轮转）、`logRelocate` |
| 解析编排 | `bfe_log_reader/pb_log_reader.go` | `logRead` 循环、`PbBuffParse` 调度、拆批、并发分发各模块 |
| 帧解析 | `bfe_log_reader/pb_buff_parse.go`（及 b2log 包） | 切帧、unmarshal、magic 重同步、错误计数 |
| 模块框架 | `reader_module/reader_module.go` | `ReaderModule` 接口、全局注册表 `AddModule`、`ModConfPath` 约定 |
| 内置模块 | `reader_modules/` | `all_modules.go` 注册；`mod_kafka/`、`mod_log_mysql/`（新增） |
| 配置 | `reader_conf/` | INI（gcfg）加载与校验：`main` / `PbAccessLogConf` 两节 |
| 工具 | `reader_util/` | `GetHostId()`（hostid 生成）、信号处理 |
| CLI 工具 | `cmd/bfe-pblog-tool/` | 离线查看 pb 日志（cat / tail -n N [-f]），复用同一解析内核 |
| 监控 | go-lib `web-monitor` | `bfe_reader(_diff)`、各模块 `mod_xxx(_diff)`、SERVER_READY；端口 `Main.HttpPort`（默认 8992），含 pprof |

装配顺序（`main/log_reader.go`）：flag/日志初始化 → `ReaderConfigLoad` → `reader_modules.SetModules()`（注册全部内置模块）→ `NewBfeLogReader` → 按配置 `Modules` 逐个 `RegisterModule` → 各模块 `Init` + `go Start` → `go` 各 logReader `Start` → Web server → `SetReady` → 阻塞等 SIGINT/SIGTERM → `Exit`（逐个 `module.Close()`）。

## 4. 详细设计

### 4.1 文件读取与轮转感知

- **tail 单文件**：`PbAccessLogConf.LogFile` 固定路径，非目录扫描；首次启动 `Seek(0, 2)` 定位文件尾（只读新数据），`-b` 时 `Seek(0, 0)` 从头全量重读；
- **读取节奏**：每次最多 `MAX_BUFF_SIZE = 1MB`；EOF 视为正常，空转 sleep 10ms 轮询；
- **轮转检测**：EOF 时 `os.Stat(logPath)` 与已打开 fd 的 fileInfo 做 `os.SameFile` 比较，inode 变化即判定 BFE 已切新文件。处理顺序：**先把旧文件剩余数据全部读出解析**，再 `logRelocate()` 关旧 fd、开新 fd，有新一轮转时清空 DataBuffer（防半帧拼接错误）；
- **位点即 fd 偏移**：无持久化位点，位点只存在于进程内存（取舍见 §5.2）；
- 文件读写错误计数（`ERR_PB_OPEN/SEEK/STAT/CLOSE/READ`）后 sleep 10ms 重试，不退出。

### 4.2 帧解析与容错

- 单条记录 = 24 字节头 + pb 负载；单条上限 100KB；
- DataBuffer 中不完整记录保留待下次读取补齐后再切帧；
- magic 不符/头解码失败：`tryFindNextStart` 按 magic 字节重新同步，丢弃中间脏字节；
- 压缩记录（`compress_len != 0`）不支持：跳过并报错计数；
- 单条 `proto.Unmarshal` 失败：计数 `ERR_PB_DECODE` 后跳过，不影响后续记录。

### 4.3 模块框架与分发

`ReaderModule` 接口（`reader_module/reader_module.go:27`）：

```go
type ReaderModule interface {
    Name() string                                                // 模块名，如 mod_kafka
    Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error
    Start()                                                      // 异步工作协程在此启动
    Update([]*bfe_access_pb.BfeLog)                              // 接收一批解析后的日志
    Close() error                                                // 清理；进程退出前调用
}
```

- **注册**：实现方在 `reader_modules/all_modules.go` 的 `SetModules()` 中 `reader_module.AddModule(NewXxx())`；按配置 `Modules` 列表 `RegisterModule` 绑定到 logReader；
- **配置路径约定**：`ModConfPath(cr, modName)` = `<confRoot>/<modName>/<modName>.conf`，模块在此加载自己的 INI 配置；
- **并发模型**：每批日志对每个绑定模块起一个 goroutine 调 `Update`（同一模块内串行、模块间并发），`Update` 内 panic 被 recover 捕获计数，读取循环不受影响；
- **批次**：按 `MaxSizePerBatch`（样例 128）拆批，控制单次分发粒度。

### 4.4 配置体系

`conf/config.conf`（INI，gcfg 解析，**不支持热加载**，修改需重启）：

| 节 | 配置项 | 说明 |
|----|--------|------|
| `[main]` | `HttpPort` / `HttpAddr` | 监控 HTTP 端口（默认 8992）/ 监听地址 |
| `[main]` | `MaxCpus` | 必填，>0 |
| `[main]` | `MonitorInterval` | diff 统计周期秒，默认 20，[20,60] 且整除 60 |
| `[PbAccessLogConf]` | `LogFile` | pb 日志路径，必填 |
| `[PbAccessLogConf]` | `Modules` | 模块列表，逗号分隔，支持 `mod_name[:true\|false]`；与 LogFile 必须同时配置 |
| `[PbAccessLogConf]` | `MaxSizePerBatch` | 每批最大条数，≤0 不限制 |

启动参数：`-c`（conf 根目录）、`-l`（日志目录）、`-s`（日志同时输出 stdout）、`-d`（debug）、`-b`（从头读）、`-h`。

### 4.5 监控体系

- 框架：go-lib `web-monitor`，每个关注对象注册两个 handler：`<name>`（累计值）、`<name>_diff`（按 `MonitorInterval` 的周期增量）；
- 内置：`bfe_reader(_diff)`——`SUM_PB_RECORD`（解析成功条数）、`ERR_PB_*`（OPEN/SEEK/STAT/CLOSE/READ/DECODE）、`SUM_READ_DATA`、`PB_LOG_RELOCATE`（轮转次数）、`SERVER_READY`；各输出模块各自的计数（见模块文档）；
- pprof：随监控端口暴露（`import _ "net/http/pprof"`）；
- 进程日志：log4go，按午夜切分保留 5 份，缓冲 10000 非阻塞（满丢弃）；
- 优雅退出：SIGINT/SIGTERM 触发 `Exit` → 逐个 `module.Close()`（模块 flush 残留数据）→ 进程退出；其他信号 ignore。

### 4.6 部署形态

| 形态 | 说明 |
|------|------|
| 裸机 | 与 BFE 同机部署，`LogFile` 指向 BFE 的 `pb_access3.log` |
| 容器（ai-gateway 一体镜像） | log-reader 与 BFE/EPP/Conf-Agent 同容器并行，conf 由镜像挂载，监控端口 8992 |
| 发布物 | `log_reader`（daemon）+ `bfe-pblog-tool`（CLI），`make release` 交叉编译四平台 tar.gz |

## 5. 容错与一致性设计

### 5.1 故障场景行为

| 场景 | 行为 |
|------|------|
| 半帧/脏数据 | 保留待补齐；magic 失配重同步；坏条计数跳过，后续记录不受影响 |
| BFE 轮转 | inode 检测 → 读完旧文件残留 → 切换新文件，无感 |
| 输出模块 panic | `Update` recover 兜底，读取循环与其他模块不受影响 |
| 进程重启 | 从文件尾读（`-b` 从头全量重放）；下游幂等性由输出模块保证（Kafka 侧靠 Doris 表 UNIQUE KEY，MySQL 侧靠唯一键覆盖——MySQL 侧唯一键自 bfenetworks/bfe#1417 起含 `logid`，覆盖只发生于同一请求重放；Doris 侧键仍是四元组、待同改） |
| 下游（Kafka/MySQL）不可用 | 模块内重试 + 背压丢弃（各模块策略见模块文档）。MySQL 侧（mod_log_mysql）：启动不可达/运行期断连进程不退出，断连期入队缓冲，恢复后自动重连并排空补写；仅缓冲超窗（队列满）或重连后补写仍失败的批丢弃，不追补 |
| 监控页不可用 | 不影响读取主流程 |

### 5.2 关键取舍：无持久化位点

- **收益**：实现极简、无外部状态依赖、无位点损坏风险；与「日志即缓冲」的管道定位一致；
- **代价**：进程重启丢失停机窗口内写入的日志；`-b` 全量重放虽可补，但依赖下游幂等；
- **缓解**：① 下游写入幂等（Doris UNIQUE KEY / MySQL 唯一键覆盖——MySQL 侧自 bfenetworks/bfe#1417 起键含 `logid`，覆盖仅发生于同一请求重放），使「重放补数」安全；② 读取解析与发送解耦，正常发布（先起新进程再停旧）窗口很小；
- **结论**：当前数据量级（报表可容忍分钟级缺口）下可接受；若未来做长保留/对账级报表，再评估位点机制。

## 6. 已知限制

1. 无持久化位点（见 §5.2）；
2. 输出模块背压策略为丢弃（计数可观测），不做磁盘 spill；
3. 配置不支持热加载；
4. 压缩 b2log 记录不支持；
5. `Modules` 仅支持内置已注册模块，配置未知模块名会启动报错（fail-fast）。

## 7. 关键文件索引

| 文件 | 内容 |
|------|------|
| `main/log_reader.go` | 进程入口、装配、信号处理、优雅退出 |
| `bfe_log_reader/log_reader.go` | 文件打开/读取/轮转检测/重定位 |
| `bfe_log_reader/pb_log_reader.go` | 读循环、拆批、模块分发（recover） |
| `bfe_log_reader/pb_buff_parse.go` | b2log 切帧与 pb 解析容错 |
| `reader_module/reader_module.go` | 模块接口、注册表、`ModConfPath` |
| `reader_modules/all_modules.go` | 内置模块注册入口 |
| `reader_conf/conf_load.go` | 主配置加载与校验 |
| `reader_util/` | hostid 生成、信号注册 |
| `cmd/bfe-pblog-tool/` | 离线查看 CLI |

关联文档：[mod_kafka.md](./mod_kafka.md)、[mod_log_mysql.md](./mod_log_mysql.md)、模块参考 [doc/modules/](../modules/)、配置文档 [doc/configuration/](../configuration/)、变更记录 [doc/modifications/](../modifications/)。
