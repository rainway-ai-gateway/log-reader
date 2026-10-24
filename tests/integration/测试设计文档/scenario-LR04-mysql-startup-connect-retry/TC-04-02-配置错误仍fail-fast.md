# TC-04-02 配置错误仍 fail-fast

## 用例编号与名称

TC-04-02 配置错误仍 fail-fast（`TestLR04_ConfigErrorStillFailFast`）

## 所属场景

LR04 MySQL 启动连接失败后台重试场景（scenario-LR04-mysql-startup-connect-retry）

## 版本声明

- `log-reader`：v1.5.0-dev 起（`mod_log_mysql` 启动连接容错改造后）

## 测试目的

守护 fail-fast 分界：启动连接容错**只降级连通性错误**，配置错误（本用例取 `Mysql.Addr` 为空，被 `ConfModLogMysqlCheck` 拒绝）仍必须立即退出且退出码非 0。防止后续演进把"配置写错"这类应快速暴露的问题一并吞掉。

本用例**不需要 MySQL**（配置错误在 `LoadConfig` 阶段即失败，不触及任何连接逻辑），因此在无 Docker/无 `LR_MYSQL_DSN` 的环境同样执行，作为负向回归的保底网。

## 运行模式

单组件模式：真实 `log-reader` 进程。无外部依赖。

## 前置条件

- 自动编译的 `log-reader` 二进制（首次运行缓存至 `tests/integration/.integration-test-bin/`）。

## 配置构造

- 先生成一份合法配置（与 TC-04-01 同模板：`{{HTTP_PORT}}` 注入、monitor 端口预占、`Mysql.Addr = 127.0.0.1:1` 占位）。
- 再将生成后的 `mod_log_mysql.conf` 中 `Addr = 127.0.0.1:1` 改写为 `Addr = `（空值），模拟配置错误。

## 输入数据

无（进程应在读取到任何日志前退出）。

## 操作步骤

1. 准备临时 conf/log 目录，构建配置并改写 `Mysql.Addr` 为空。
2. `StartLogReaderOnPort` 启动 log-reader。
3. `waitExit(10s)` 等待进程退出。
4. 断言已退出（`exited = true`）。
5. 断言退出码非 0。

## 预期结果

- 步骤 4：10 秒内进程退出（配置错误 → `Init` 返回 error → `BfeLogReader.Start` 上抛 → `main` `Exit(1)`）。
- 步骤 5：退出码非 0。
- 对照意义：同样启动条件下，若仅把 `Addr` 指向不可达地址（连通性错误），进程应存活（TC-04-01 与单测 `TestModuleLogMysql_InitMysqlUnreachable` 覆盖）——一进一退界定行为分界。

## 清理

`waitExit` 已确认进程退出，`stopReader`（defer）对退出进程为空操作；删除临时工作目录。

## 历史记录

- 新增于 v1.5.0-dev（mod_log_mysql 启动连接失败后台重试改造），与改造同 PR 落地。
