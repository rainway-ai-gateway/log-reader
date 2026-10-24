# log-reader mod_log_mysql 运行期断连自动重连（二期设计方案）

> 版本：二期设计稿　日期：2026-09-30
> 状态：**已按本方案实施**（2026-09-30，与一期同分支未发布）；实现与设计的唯一偏差见 §4.2 ⑥ 注（`sql.Tx` 无 `CommitContext`，Commit/Rollback 超时由 DSN `writeTimeout` 同值约束）
> 涉及组件：log-reader（仅 `mod_log_mysql` 模块），其余组件零改动
> 上游：`../2026-09-29-mod-log-mysql-startup-connect-retry/design-changes.md`（一期：启动连接失败后台重试，已交付；本方案为其"可选二期——运行期断连重连"）

---

## 目录

- [1. 背景与现状](#1-背景与现状)
- [2. 目标与非目标](#2-目标与非目标)
- [3. 方案选型](#3-方案选型)
- [4. 详细设计](#4-详细设计)
- [5. 配置与监控变更](#5-配置与监控变更)
- [6. 兼容性与回滚](#6-兼容性与回滚)
- [7. 测试计划](#7-测试计划)
- [8. 风险与开放问题](#8-风险与开放问题)

## 1. 背景与现状

### 1.1 一期已交付

启动时 MySQL 不可达不再退出：`Init` 尽力建连一次，失败则置 `MYSQL_CONN_STATE=DOWN` 并由 `connectLoop` 后台重试；`writeLoop` 挂起不消费，行数据在 `QueueSize` 队列缓冲，连通后自动排空。`Connect()` 设计为幂等可反复调用，`connCh`/`connOnce` 信号机制——这些是一期为二期预留的结构。

### 1.2 运行期断连的三个缺口（本方案要解决）

一期刻意保留了运行期断连的原语义：写失败按 `MaxRetries` 退避重试（200ms/400ms/800ms），耗尽丢弃该批计 `SEND_MYSQL_FAILED`；进程不退出；网络恢复后 `database/sql` 连接池自行换连接恢复写入。该语义在持续断连下有三个缺口：

| # | 缺口 | 机理 | 后果 |
|---|------|------|------|
| 1 | **写路径可能挂死** | DSN 未配 `timeout` 参数，连接池拨号无超时；网络分区（DROP，非 REJECT）时 `db.Begin()` 挂在 TCP 握手直到 OS 级超时（分钟级） | 写协程卡死期间：无错误日志、无计数，队列堆满后 `SENT_MYSQL_CHN_FULL` 静默丢光；重试逻辑根本得不到执行机会 |
| 2 | **持续断连线性丢批** | 单次 flush 的重试窗口仅约 1.4s（3 次退避 + 执行耗时），断连每多持续一个 flush 周期（默认 2s）就丢一批 | `SEND_MYSQL_FAILED` 随断连时长线性增长；数据缺口依赖 `-b` 补读重放兜底，而补读本身又受"分区是否已恢复"牵制 |
| 3 | **断连不可观测** | `MYSQL_CONN_STATE` 只在启动路径设置；运行期断连无任何状态信号，只能靠 `SEND_MYSQL_FAILED`/`SENT_MYSQL_CHN_FULL` 副作用间接推断 | 告警滞后、定位困难；"进程活着但写不动"与"进程健康"无法区分 |

说明缺口 2 时值得量化：默认配置下断连 1 分钟约丢 30 批（按 FlushIntervalMs=2000 计），即每分钟最多 6000 行（BatchSize=200）。

### 1.3 为什么连接池自愈不够

`database/sql` 确实会在检出坏连接（`driver.ErrBadConn`）后换新连接，这能覆盖**瞬断**（重启、短抖动）；但：拨号本身无超时（缺口 1），且对**持续断连**它无能为力——每次取连接都要现场拨号、现场失败，重试窗口内恢复不了就丢批（缺口 2）。观测（缺口 3）则完全不在其职责内。

## 2. 目标与非目标

| # | 目标 | 验证标准 |
|---|------|----------|
| 1 | 写路径单次操作有界：dial 与批事务执行均有超时，网络分区下写协程不挂死 | DROP 型分区中，≤`BatchTimeoutMs` + 拨号超时内出现 DOWN 信号与重试日志（而非静默卡死数分钟） |
| 2 | 运行期断连可检测、可重连：写重试耗尽即置 DOWN 并自动重连，重连期间行缓冲不丢（在预算内） | 断连恢复后表内行数与输入一一对应，`SEND_MYSQL_FAILED` 不随断连时长增长 |
| 3 | 重连成功后自动恢复写入并排空（复用一期排空语义） | DOWN→UP 转换后积压自动落库，无需重启进程 |
| 4 | 可观测：DOWN/UP 转换可见，断连次数可计数 | monitor 可见 `MYSQL_CONN_STATE` 转换与新增 `MYSQL_CONN_LOST` 计数 |

**非目标**：

- **不重做启动逻辑**：一期启动路径（同步尽力建连 + DOWN 后台重试）原样复用，supervisor 只把一次性 `connectLoop` 常驻化；
- **不改队列与批参数语义**：`QueueSize`/`BatchSize`/`FlushIntervalMs`/`MaxRetries` 含义不变；
- **不保证断连窗口内的严格行序**：pending 批优先补写 + 队列重排会使窗口内行序变化；消费侧以 `log_time`+`logid` 为准，分钟级聚合报表不受影响（幂等键保证不重复）；
- **不做 MySQL 故障转移的专门适配**：主从切换、VIP 漂移等场景按"TCP 目标恢复"自然覆盖，由重试循环等待；
- **认证失效（密码被改）不追求自愈**：与一期结论一致——重连持续失败、节流日志提示根因，修复方式是改配置重启；
- **周期 ping 探活**作为可选增强（配置默认关闭），不构成本期验收条件。

## 3. 方案选型

| 方案 | 做法 | 评价 |
|------|------|------|
| A. 仅写路径加超时 | DSN 加 `timeout`、`writeBatch` 改 `ExecContext` 带Deadline | 解决挂死（缺口 1），但持续断连照样丢批（缺口 2）、仍无 DOWN 信号（缺口 3）。✗ 不充分 |
| B. 超时 + 重试耗尽触发重连（选中） | A 的基础上：`writeBatchWithRetry` 耗尽后 `markDown()`（置 DOWN、关闭旧池、触发常驻 supervisor 重连），失败批持有待重连后优先补写 | 三个缺口全解；结构复用一期（`Connect` 幂等、`connCh` 信号、计数器），变更收敛在 `record_writer.go`。✓ |
| C. B + 周期 ping 探活 | supervisor 在 UP 空闲期按 `HealthPingIntervalMs` 周期 `PingContext`，失败即 `markDown` | 能更早发现"半开连接"（防火墙断链、NAT 超时等写不立刻报错场景），但有误判成本（网络抖动→无谓重连）。作为 B 之上的可选开关，默认关闭。 |

选中 **B**，C 留作配置开关（见 §5）。

## 4. 详细设计

### 4.1 状态机

```
                    ┌──────────── write 重试耗尽 / ping 失败 ───────────┐
                    │                                                    │
                    ▼                                                    │
  UP ──────────────────────────────────────────────────────────► DOWN ──┘
  │  supervisor 重连成功（Connect + epoch 换代）                            │
  └──────────────────────────────────────────────────────────────► UP
```

- `UP`：正常攒批写入（一期语义）；
- `DOWN`：`db` 为 nil，写协程挂起不消费，`Enqueue` 照常缓冲（既有背压语义）；
- 转换动作全部落在 `Connect()`（UP 方向）与 `markDown()`（DOWN 方向）两个函数，便于审计与测试。

### 4.2 结构改动（`record_writer.go`）

**① `connectLoop` → `connSupervisor`（常驻）**

一期 `connectLoop` 建连成功即退出；二期常驻，UP 期阻塞等待触发信号，DOWN 期按间隔重试：

```go
// Start 中：无条件启动（替代原"仅未连通时启动"）
go w.connSupervisor()

func (w *RecordWriter) connSupervisor() {
	defer w.wg.Done()
	ticker := time.NewTicker(retryInterval)   // ConnectRetryIntervalMs
	defer ticker.Stop()

	for {
		if w.Connected() {
			// UP：等待 markDown 触发；可选 ping 探活
			select {
			case <-w.stopCh:
				return
			case <-w.discCh:        // markDown 触发（带 epoch 防并发风暴）
				continue
			case <-pingTicker.C:    // 仅 HealthPingIntervalMs > 0 时启用
				ctx, cancel := context.WithTimeout(..., ConnectTimeoutMs)
				err := w.db.Load().PingContext(ctx)
				cancel()
				if err != nil {
					w.markDown("health ping failed")
				}
			}
			continue
		}

		// DOWN：重试 Connect 直至成功（节流日志/计数沿用一期）
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
		}
		if err := w.Connect(); err != nil {
			w.state.Inc("MYSQL_CONN_RETRY", 1)
			logRetryThrottled(err)   // 一期节流规则
			continue
		}
		w.state.Inc("MYSQL_CONN_OK", 1)
		w.state.Set("MYSQL_CONN_STATE", "UP")
		log.Logger.Info("mod_log_mysql: reconnected to mysql, draining buffered records")
	}
}
```

**② `markDown()`（DOWN 方向的唯一入口）**

```go
func (w *RecordWriter) markDown(reason string) {
	db := w.db.Swap(nil)
	if db == nil {
		return              // 已被其他路径置 DOWN，幂等
	}
	db.Close()              // 旧池立即关闭：检出坏连接、释放 FD
	w.state.Set("MYSQL_CONN_STATE", "DOWN")
	w.state.Inc("MYSQL_CONN_LOST", 1)
	log.Logger.Error("mod_log_mysql: connection lost (%s), reconnecting in background", reason)
	select {
	case w.discCh <- struct{}{}:   // 非阻塞唤醒 supervisor
	default:
	}
}
```

要点：`db.Swap(nil)` 与 `Connect()` 的 `db.Swap(newDB)` 构成互斥——任何时刻至多一个有效池；`markDown` 与 `Connect` 并发时后到的生效，先到者的旧池 `Close()` 幂等（`sql.DB.Close` 可多次调用）。

**③ `Connect()` 换代语义**

在一期基础上增加"连接代"（epoch）：每次成功建连生成新一代 `connCh`（替换字段而非 `once.Close`），`writeLoop` 挂起时取"当前代"的 channel。这样 DOWN→UP 的唤醒可反复发生（一期 `connOnce` 只支持一次）。`Connect` 成功时若存在旧池（如 markDown 竞态后的补偿性重连）先关闭旧池再发布新代。

**④ `writeLoop` 每轮检查连通（闸门常态化）**

一期闸门只在协程入口检查一次；二期改为**每个写周期前检查**：

```go
for {
	if !w.Connected() {
		// DOWN：挂起在当前代 connCh 或 stopCh（与一期闸同语义，可反复进入）
		select {
		case <-w.currentConnCh():
			continue
		case <-w.stopCh:
			return
		}
	}
	select {
	case <-w.stopCh:
		drainAndFinalFlush()   // 仅连通时可 flush；未连通丢弃并记 Info（一期语义）
		return
	case row := <-w.ch:
		batch = append(batch, row)
		if len(batch) >= w.conf.Writer.BatchSize {
			flush("batch")
		}
	case <-ticker.C:
		flush("timeout")
	}
}
```

flush 内部 `writeBatchWithRetry` 失败后将触发 `markDown`，写循环下一轮自然进入 DOWN 挂起分支——不需要额外状态标志。

**⑤ 失败批处理：持有待补写（S1），不直接丢弃**

`writeBatchWithRetry` 耗尽 `MaxRetries` 后：

```go
// 常规重试耗尽 → 判定连接失效，进入 DOWN 并重连
w.markDown("write retry exhausted")

// 持有该批，等待重连成功后优先补写（S1；不占用 QueueSize、保序优于 re-enqueue）
select {
case <-w.stopCh:
	return            // 进程退出：批随进程消亡（既有语义）
case <-w.currentConnCh():
}

// 重连成功：优先补写该批；单次失败即丢弃计数（不再递归重连，防叠加）
if err := w.writeBatch(batch); err == nil {
	w.state.Inc("WRITE_BATCH_SIZE", len(batch))
	return
}
w.state.Inc("SEND_MYSQL_FAILED", 1)
```

选 S1（持有）而非 S2（re-enqueue 到队尾）的理由：不占 `QueueSize`（断连期队列容量全留给新行）、窗口内保序较好、语义简单可预测。代价是写协程在 DOWN 期被 pending 批占住——这正是期望行为（写不动时不再消费队列，背压自然传导到 `Enqueue` 丢弃计数）。

**⑥ 写路径超时（缺口 1 的前提）**

- **拨号超时**：DSN 增加 `timeout=<ConnectTimeoutMs>ms`（go-sql-driver 拨号超时参数），与建连超时同一配置，分区时 `Begin` 取连接在秒级失败而非挂死分钟级；
- **批事务超时**：`writeBatch` 改 `ExecContext`，ctx 超时 = 新增 `[Writer] BatchTimeoutMs`（默认 30000ms）：

```go
// 注：database/sql 的 sql.Tx 无 CommitContext，实现采用 BeginTx(ctx) 约束拨号、
// ExecContext(ctx) 约束执行；Commit/Rollback 由 DSN writeTimeout（同 BatchTimeoutMs 值）约束
ctx, cancel := context.WithTimeout(context.Background(),
	time.Duration(w.conf.Writer.BatchTimeoutMs)*time.Millisecond)
defer cancel()
txn, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
_, err = txn.ExecContext(ctx, w.buildStmt(len(batch)), args...)
if err != nil {
	txn.Rollback()
	return err
}
return txn.Commit()
```

幂等键保证半成品事务回滚后重试安全。

### 4.3 `Init` 与接口变化

`ModuleLogMysql.Init` **零改动**（仍是一次尽力建连 + DOWN 后台等待），`recordWriter` 接口不增方法（`Connect`/`Connected` 已在一期加入）。`Start()` 简化为无条件起 `writeLoop` + `connSupervisor`。

### 4.4 错误分类与行为

| 场景 | 一期行为 | 二期行为 |
|------|----------|----------|
| 网络分区（DROP） | 挂死至 OS 超时，期间静默丢光队列 | 拨号/执行秒级超时 → DOWN → 重连，行缓冲不丢（恢复后补写） |
| MySQL 进程被杀（REJECT/复位） | 重试窗口内恢复则无碍，否则丢批 | DOWN → 重连 → 补写；不丢批 |
| 认证被撤销（REVOKE） mid-run | 写失败丢批 | 重连持续失败，DOWN 常驻 + 节流日志提示 1045；改配置重启 |
| 表被 DROP mid-run | 写失败丢批（error 1146） | 重连成功但补写仍失败 → 该批计 `SEND_MYSQL_FAILED` 丢弃，恢复写新行（与一期同语义：结构性错误不重试无意义——supervisor 重试的是**连接**，不是** schema**） |
| 慢查询超过 BatchTimeoutMs | 不存在该超时 | 该批失败 → DOWN → 重连 → 补写；需按 §8 配置指引避让 |

## 5. 配置与监控变更

### 配置新增（均为可选带默认值）

| 配置项 | 段 | 默认 | 含义 |
|--------|----|------|------|
| `BatchTimeoutMs` | `[Writer]` | 30000 | 单批事务（Exec/Commit）执行超时，`ExecContext` ctx；须大于正常 P99 批执行时间（见 §8） |
| `HealthPingIntervalMs` | `[mysql]` | 0（关闭） | UP 期周期探活间隔；>0 时 supervisor 按间隔 `PingContext`，失败即 DOWN。面向半开连接场景，默认关闭 |

`ConnectTimeoutMs` 同时约束建连 ping 与拨号超时（DSN `timeout` 参数），语义不变、复用不新增。

### 监控变更

| 键 | 类型 | 含义 |
|----|------|------|
| `MYSQL_CONN_STATE` | state | 语义扩展：启动外新增运行期 DOWN/UP 转换 |
| `MYSQL_CONN_LOST` | counter（新增） | 运行期断连次数（`markDown` 触发，含写耗尽与 ping 失败） |
| `MYSQL_CONN_RETRY` / `MYSQL_CONN_OK` | counter | 语义不变；运行期重连同样累计 |
| `SEND_MYSQL_FAILED` | counter | 语义收窄为"最终丢弃的批"（断连不再线性贡献；重连后补写失败才计入） |

## 6. 兼容性与回滚

- **行为变更面**：仅"运行期断连"场景——丢批变为缓冲补写、`SEND_MYSQL_FAILED` 增长率下降（告警阈值语义变化，需在部署说明中提示：该指标从"断连时长代理"回归"真实数据缺口"）；
- **前向兼容**：新配置可选带默认值，旧 conf 零改动可用；DSN 新增 `timeout` 参数对既有部署无影响（原依赖 OS 默认超时的极端场景行为变化：从挂死分钟级变为秒级失败，这正是目的）；
- **回滚**：与一期相同——旧二进制 + 含新字段的 conf 会因 gcfg 对未知字段报 warning 错误导致 Init 失败，回滚需二进制与 conf 同步回退；
- **与一期的组合**：本方案落地后一期行为全部保留（启动语义、缓冲排空、监控键），一期测试用例应原样通过（回归网）。

## 7. 测试计划

### 7.1 单元测试（`mod_log_mysql` 包，复用一期的 connector 注入缝）

| # | 用例 | 断言 |
|---|------|------|
| 1 | 写重试耗尽 → DOWN → 重连成功 → pending 批补写 | `MYSQL_CONN_LOST=1`、`STATE` DOWN→UP、批落库（假 db 断言）、`SEND_MYSQL_FAILED=0`、缓冲行随后排空 |
| 2 | DOWN 期新行入队 | `Enqueue` 照常；`WRITE_BATCH_SIZE` 不增长；重连后全部落库、次序为 pending 批在前 |
| 3 | 重连后补写仍失败（如表被 DROP） | pending 批计 `SEND_MYSQL_FAILED` 丢弃，写循环继续消费队列 |
| 4 | `markDown` 与 `Connect` 并发 | 无 double-close、无 panic、终态一致（`-race` 由 CI 跑） |
| 5 | `markDown` 幂等 | 连续两次调用仅 `MYSQL_CONN_LOST+1` 一次（第二次 `Swap` 得 nil 直接返回） |
| 6 | 批事务超时 | 假 `db.Exec` 挂起超过 `BatchTimeoutMs` → ctx 取消 → 进入 markDown 路径 |
| 7 | 未连通即 `Close` / 连通后 `Close` | 一期语义回归（supervisor 一并退出，无 goroutine 泄漏） |
| 8 | `HealthPingIntervalMs>0` 且 ping 失败 | 触发 `markDown`；ping 正常时无 `MYSQL_CONN_LOST` 增长 |
| 9 | `ConfModLogMysqlCheck`：`BatchTimeoutMs`/`HealthPingIntervalMs` 缺省与非法 | 填默认值，不报错 |

### 7.2 集成测试（LR04 扩展 TC-04-03，解除一期暂缓）

一期因"连接池拨号无超时、断言窗口不稳定"暂缓的 TC-04-03 在本方案落地后具备自动化条件（`BatchTimeoutMs` 提供确定性窗口）：

1. 正常建连写入 N 条；
2. `Pause`：`WaitMonitor` 断言 `BatchTimeoutMs`+余量内出现 `STATE=DOWN`（替代一期的"挂死无信号"）；进程存活；`SENT_TO_MYSQL` 增长而 `WRITE_BATCH_SIZE` 停滞；
3. `Unpause`：断言自动 `UP`、`MYSQL_CONN_LOST=1`、全部行落库且 logid 齐全、`SEND_MYSQL_FAILED=0`；
4. （可选，若实现 ping 探活）开启 `HealthPingIntervalMs` 后重复步骤 2，断言 DOWN 出现时间 ≈ 探活间隔。

`tests/integration/测试设计文档/scenario-LR04-mysql-startup-connect-retry/` 增补 `TC-04-03-运行期断连自动重连.md`，`场景说明.md` 同步更新。

## 8. 风险与开放问题

- **`BatchTimeoutMs` 与慢查询冲突**：超时必须大于正常 P99 批执行时间，否则大批/索引抖动会被误判为断连引发无谓重连。落地时需在部署文档给出配置指引（如：观测 `WRITE_BATCH_SIZE` diff 均值与慢查询日志，取 P99 的 3 倍）；一期遗留备选同样适用——后续可把 retryBackoffMs 硬编码一并配置化，不在本方案范围。
- **抖动导致的重连风暴（flap）**：当前设计沿用固定重试间隔（`ConnectRetryIntervalMs`），与一期一致；MySQL 边界态（反复 up/down）下会按间隔交替 DOWN/UP。是否需要指数退避或"重连成功冷静期"，按上线后观察决策（开放问题，默认不做）。
- **ping 探活误判**：网络抖动会把瞬断放大为 DOWN 事件（`MYSQL_CONN_LOST` 虚高）。默认关闭；启用时建议配合 `MYSQL_CONN_LOST` 速率告警而非单次数告警。
- **行序变化**：断连窗口内 pending 批优先 + 队列重排，窗口内行序与到达序不完全一致。下游分钟聚合报表按 `log_time` 分桶不受影响；如需严格顺序消费的用途（当前无）需另行评估。
- **事务被 ctx 取消的半成品**：驱动保证取消即回滚，幂等键保证重试安全——该路径已被一期幂等设计覆盖，无新增数据正确性风险。

---

*文档生成日期：2026-09-30*
