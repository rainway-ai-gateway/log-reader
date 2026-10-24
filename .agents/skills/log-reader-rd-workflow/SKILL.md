---
name: log-reader-rd-workflow
description: 引导用户在 log-reader 代码库中完成一次完整的功能研发流程，包括需求对齐、modifications 修改说明、代码实现、集成测试与回归验证。
---

# log-reader 代码库研发流程

本 Skill 适用于在 `log-reader/` 目录中新增或修改日志读取/转发功能（例如 `mod_log_mysql`、`mod_kafka`、`mod_fields` 新字段、pb 解码、可观测计数器等）的完整研发流程。

## 触发语

当用户提出以下类型请求时启用本流程：

- “我要实现 xxx 功能”
- “请帮我完成 xxx 的代码与测试”
- “请在 log-reader 中增加/修改 xxx”

## 执行原则

1. **分阶段暂停确认**：本流程将研发拆分为多个 Phase。每个 Phase 执行完毕后，必须暂停并等待用户确认“可以继续”后，再进入下一个 Phase。不要在未获得用户确认的情况下自动推进到下一阶段。
2. **Git 提交前确认**：在任何 `git commit`、`git push` 或其他会改变 Git 仓库状态的操作之前，必须先向用户说明变更内容并取得明确同意。禁止自动执行 Git 提交或推送。
3. **Git push 默认目标为 origin**：如果获得用户授权执行 `git push`，默认推送到 `origin` 远程仓库，而不是 `upstream`。除非用户明确指定其他 remote，否则不使用 `upstream`。

## 研发阶段

### Phase 0. 需求澄清与范围界定

1. 让用户明确：
   - 功能目标（一句话描述）
   - 验收标准（必须通过的测试/行为；如涉及 `bfe_ai_request_log` 写入，明确对应设计条款如 `A-09`/`A-10`）
   - 影响范围（是否改 pb 字段契约、是否改列清单、是否改配置协议、是否影响既有写路径）
2. 判断是否为“非平凡改动”（多文件、有架构选择、用户偏好影响实现）：
   - 是 → 调用 `EnterPlanMode`，先出设计文档/plan 再执行。
   - 否 → 直接进入 Phase 1。

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 1. 修改 modifications 文档

log-reader 侧的改动必须在 `log-reader/doc/modifications/` 留下修改说明，便于后续维护与审计。

1. 在 `log-reader/doc/modifications/` 下新建日期化目录，例如：
   ```
   log-reader/doc/modifications/2026-10-21-issue-12-proxy-delay-out-of-range/
   ```
2. 在该目录下创建 `design-changes.md`，包含：
   - 背景与缺陷/需求（如有上游 issue，附链接与根因链）
   - 目标与非目标
   - 主要改动点（按文件/函数给出位置与行为变化）
   - 监控与计数器影响
   - 配置影响、上线顺序与回滚
   - 风险与开放问题
3. 如已有同主题目录，则更新其中的文档，不要重复创建。

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 2. 更新 configuration 文档

如果改动涉及配置文件（`config.conf`、`mod_*.conf` 等），必须同步更新 `log-reader/doc/configuration/`：

- 模块配置：`doc/configuration/<mod>/<mod>.conf.md`
- 新增字段要说明类型、默认值、是否必填、示例。
- 修改字段要说明前后行为差异。
- **新增监控计数器时**，除配置文档指标表外，还必须同步 `doc/modules/<mod>/<mod>.md` 的监控指标章节。

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 3. 更新 modules / sys_design 文档

如果改动涉及模块行为、写入语义、协议交互，需要更新：

1. `log-reader/doc/modules/<mod>/`：模块行为、输出列契约（如 `output-columns.md`）。
2. `log-reader/doc/sys_design/`：系统设计章节（如 `mod_log_mysql.md` 的写入语义/背压/监控条款）。

关键约束（写文档与改代码都要遵守）：

- `mod_log_mysql` 的 **99 列清单与列序唯一权威**是 `reader_modules/mod_log_mysql/field_mapper.go` 的 `columnDefs`，列序必须与 `ai-gateway-api` 仓库 `db_ddl_report_mysql.sql` 的 `CREATE TABLE` 列序一致；
- 零值规则：`kindScalar` 字符串零值写 `NULL`、数值原样；`kindNotNullStr` 空串原样写入；`kindOptionalNum` proto 未设置写 `NULL`——文档表述须与之一致；
- 写入语句、覆盖子句由 `Columns()` 动态生成，唯一键不编码在任何 Go 代码中。

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 4. 代码实现

1. 阅读相关源码，确认：
   - 入口与模块注册：`main/log_reader.go`、`reader_modules/all_modules.go`（模块注册顺序即执行顺序）
   - 模块框架接口：`reader_module/reader_module.go`
   - 模块实现：`reader_modules/mod_<name>/`（含 `conf_mod_<name>.go`、`field_mapper.go`、`record_writer.go` 等）
   - 字段抽取：`reader_modules/mod_fields/`（`field_registry.go`）
   - 配置加载：`reader_conf/`
2. 做最小改动，优先匹配现有代码风格：
   - 不引入未使用的依赖。
   - 不修改与本次需求无关的逻辑。
   - 新源文件需要 Apache 2.0 License 头（与现有文件一致）。
3. 关键实现完成后，先跑模块级单元测试：
   ```bash
   cd log-reader
   go test ./reader_modules/<相关模块>/... ./reader_util/...
   go vet ./...
   ```

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 5. 编写集成测试设计文档

log-reader 的集成测试设计文档位于 `log-reader/tests/integration/测试设计文档/`。在写测试代码之前，先补充对应场景的设计文档，便于后续维护与评审。

1. 在 `tests/integration/测试设计文档/` 下新建 scenario 目录，命名规范：
   ```
   scenario-LR<序号>-<简短中文描述>
   ```
2. 编写 `场景说明.md`，包含：
   - 场景背景与目的
   - 运行模式与前置依赖（如 MySQL DSN、Docker）
   - 涉及的配置与数据构造方式
   - 测试例列表
   - 公共基础设施
   - 与其他场景的依赖关系
3. 为每个测试例编写独立的 `TC-<序号>-<测试例名称>.md`，结构与现有 TC 文档保持一致（目的、前置条件、数据构造、执行步骤、预期结果、清理）。
4. 更新 `tests/integration/测试设计文档/测试场景总体说明.md`：
   - 在“场景清单”中新增一行；
   - 在“场景与测试例对应关系”中新增该 scenario 的 TC 列表。

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 6. 编写集成测试代码

log-reader 的集成测试代码位于 `log-reader/tests/integration/implementation/`。

1. 选择最接近的现有 scenario 作为模板（如 MySQL 写入参考 `scenario-LR03-mysql-write`，连接重试参考 `scenario-LR04-mysql-startup-connect-retry`）。
2. 新建 scenario 目录，命名规范：
   ```
   scenario-LR<序号>-<简短英文描述>
   ```
3. 复用 `tests/integration/common/` 的公共基础设施（`log_generator.go` 的 `MakeRequestLog` 等）；如需扩展公共代码，确认不影响既有 scenario。
4. 如 scenario 需要内嵌 DDL 副本（如 `testdata/ddl/bfe_ai_request_log.sql`）：**LR03 与 LR04 的两份副本必须逐字节一致**（改一处必须同步另一处，改完用 `diff` 校验），且与 ai-gateway-api 权威 DDL 保持同步；副本头部注明基准版本。
5. 编写 `_test.go`，覆盖：
   - 正常路径
   - 边界场景（零值/NULL 规则、越界值、重试耗尽、重启/断连恢复）
   - 红/绿口径：新用例应能在旧行为上失败（红），在修复后通过（绿）
6. 运行集成测试（需要 MySQL，可用 `LR_MYSQL_DSN` 指定已有实例）：
   ```bash
   cd log-reader
   LR_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' go test ./tests/integration/implementation/scenario-LR<NN>-<描述>/... -v
   ```

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 7. 回归验证

1. 运行本次新增 scenario 的测试。
2. 运行与被改动模块相关的既有 scenario：
   - MySQL 写入：`scenario-LR03-mysql-write`
   - 启动建连重试：`scenario-LR04-mysql-startup-connect-retry`
3. 运行相关单元测试：
   ```bash
   go test ./reader_modules/... ./reader_module/... ./reader_util/...
   go vet ./...
   ```
4. 如有失败，优先修复；修复后再次回归，直到全部通过。

完成本阶段后暂停，等待用户确认后再进入下一阶段。

### Phase 8. 收尾与总结

1. 检查是否有注释/文档描述的是旧行为，及时同步更新（重点：`doc/modules/`、`doc/sys_design/`、`doc/configuration/`、CHANGELOG 中尚在预览版本的条目）。
2. 向用户汇报：
   - 改动了哪些文件
   - 新增/修改了哪些测试
   - 验证结果
   - 仍存在的风险或待决策点（如有）

3. **Git 提交前必须人工确认**：如果用户要求或流程需要执行 `git commit`、`git push` 等 Git 操作，必须先向用户清晰说明本次提交内容（包含文件清单与主要变更摘要），并取得明确同意后再执行。禁止在未获授权的情况下自动提交或推送。获得授权后，`git push` 默认推送到 `origin`，不要推送到 `upstream`，除非用户明确指定。

## 常见陷阱

- **列清单/列序漂移**：`field_mapper.go` 加列后，`record_writer_test.go` 的 `ddlColumns` 等断言要同步；列序必须与 ai-gateway-api `db_ddl_report_mysql.sql` 一致。
- **DDL 测试副本分叉**：LR03 与 LR04 的 `testdata/ddl/bfe_ai_request_log.sql` 必须逐字节一致，改一处忘另一处会导致两 scenario 行为分叉。
- **计数器文档遗漏**：`COUNTER_KEYS` 新增计数器后，`doc/configuration/mod_log_mysql/mod_log_mysql.conf.md` 指标表与 `doc/modules/mod_log_mysql/mod_log_mysql.md` §监控指标必须同步登记。
- **零值规则混淆**：`kindScalar` 数值零值写 `0`、字符串零值写 `NULL`；改 `kind` 前先读 `ToRow` 的 switch 分支与既有单测。
- **重试/连接语义回归**：`writeBatchWithRetry` 的 `markDown` → 重连 → 补写 → `SEND_MYSQL_FAILED` 路径是批准设计（§4.2），改动时不得让非结构性错误（如单行越界）进入该路径。
- **模块注册顺序**：`reader_modules/all_modules.go` 中的顺序即模块执行顺序，新增模块要放在正确的管道位置。
- **集成测试端口/DSN**：不要写死端口与 DSN；LR04 中需 Docker 暂停/恢复实例的用例在 DSN-only 环境下 SKIP 属预期行为。

## 推荐命令速查

```bash
# 模块单元测试 + 静态检查
cd log-reader
go test ./reader_modules/... ./reader_module/... ./reader_util/...
go vet ./...

# 单个集成测试场景（详细日志，需 MySQL）
LR_MYSQL_DSN='user:pass@tcp(127.0.0.1:3306)/test' \
  go test ./tests/integration/implementation/scenario-LR03-mysql-write/... -v

# 相关场景回归
LR_MYSQL_DSN='...' go test ./tests/integration/implementation/scenario-LR04-mysql-startup-connect-retry/... -v
```

## 参考

- 模块设计：`doc/sys_design/mod_log_mysql.md`、`doc/modules/mod_log_mysql/`
- 修改说明先例：`doc/modifications/2026-10-13-issue-1417-dedup-key-logid/design-changes.md`
- 权威 DDL（另仓）：`ai-gateway-api/db_ddl_report_mysql.sql`
