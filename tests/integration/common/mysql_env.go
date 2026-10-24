// Copyright(c) 2026 The Rainway AI Gateway (壬远AI网关) Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package common

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/client"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// MysqlEnv 管理集成测试使用的 MySQL 实例。
//
// 实例来源按优先级：
//  1. 环境变量 LR_MYSQL_DSN 指向的外部实例（格式 user:pass@tcp(host:port)/dbname，
//     凭据需具备 CREATE/DROP DATABASE 权限）；测试会创建专用随机名数据库并在结束时
//     DROP，绝不使用 DSN 中自带的数据库名，避免触碰用户数据；
//  2. testcontainers 启动 mysql:8.0 容器（需要本机 Docker 可用）；
//  3. 两者都不可用则 t.Skip。
type MysqlEnv struct {
	t         *testing.T
	db        *sql.DB // 已选中测试数据库的连接（parseTime=true&loc=UTC）
	addr      string  // host:port，用于生成 log-reader 配置
	user      string
	passwd    string
	dbName    string
	container testcontainers.Container // 仅 testcontainers 后端非空
	cleanup   func()                   // 结束时释放外部资源（drop 测试库 / 终止容器）
}

// NewMysqlEnv 创建 MySQL 测试环境。
func NewMysqlEnv(t *testing.T) *MysqlEnv {
	t.Helper()

	dbName := fmt.Sprintf("bfe_report_lr03_%d", time.Now().UnixNano())

	if dsn := os.Getenv("LR_MYSQL_DSN"); dsn != "" {
		return newExternalMysqlEnv(t, dsn, dbName)
	}
	if dockerAvailable() {
		if env, err := newContainerMysqlEnv(t, dbName); err == nil {
			return env
		} else {
			t.Logf("start mysql container failed, skip: %v", err)
		}
	} else {
		t.Logf("docker unavailable and LR_MYSQL_DSN not set")
	}
	t.Skipf("MySQL not available: set LR_MYSQL_DSN (e.g. root:****@tcp(127.0.0.1:3306)/) " +
		"or start a Docker daemon for testcontainers")
	return nil
}

func newExternalMysqlEnv(t *testing.T, dsn string, dbName string) *MysqlEnv {
	t.Helper()

	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse LR_MYSQL_DSN failed: %v", err)
	}
	if cfg.User == "" {
		t.Fatalf("LR_MYSQL_DSN has no user")
	}

	env := &MysqlEnv{t: t, user: cfg.User, passwd: cfg.Passwd, dbName: dbName, addr: cfg.Addr}

	// 连接 server（不带数据库名），创建专用测试库。
	serverDSN := fmt.Sprintf("%s:%s@tcp(%s)/?parseTime=true&loc=UTC",
		cfg.User, cfg.Passwd, cfg.Addr)
	serverDB, err := sql.Open("mysql", serverDSN)
	if err != nil {
		t.Fatalf("open mysql server failed: %v", err)
	}
	if _, err := serverDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
		serverDB.Close()
		t.Fatalf("create test database failed (check LR_MYSQL_DSN account privileges): %v", err)
	}
	serverDB.Close()

	env.db = env.openTestDB(t)
	env.cleanup = func() {
		serverDB, err := sql.Open("mysql", serverDSN)
		if err != nil {
			return
		}
		defer serverDB.Close()
		serverDB.Exec("DROP DATABASE IF EXISTS " + dbName)
	}
	return env
}

func newContainerMysqlEnv(t *testing.T, dbName string) (*MysqlEnv, error) {
	t.Helper()

	ctx := context.Background()
	container, err := tcmysql.Run(ctx, "mysql:8.0",
		tcmysql.WithDatabase(dbName),
		tcmysql.WithUsername("root"),
		tcmysql.WithPassword("root"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("port: 3306").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		return nil, err
	}

	connStr, err := container.ConnectionString(ctx, "parseTime=true", "loc=UTC")
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}
	cfg, err := gomysql.ParseDSN(connStr)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}

	env := &MysqlEnv{
		t:         t,
		addr:      cfg.Addr,
		user:      cfg.User,
		passwd:    cfg.Passwd,
		dbName:    dbName,
		container: container,
	}
	env.db = env.openTestDB(t)
	env.cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		container.Terminate(ctx)
	}
	return env, nil
}

func (e *MysqlEnv) openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=UTC",
		e.user, e.passwd, e.addr, e.dbName))
	if err != nil {
		t.Fatalf("open test database failed: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping test database failed: %v", err)
	}
	return db
}

// DB 返回测试数据库连接。
func (e *MysqlEnv) DB() *sql.DB {
	return e.db
}

// Config 返回生成 log-reader mod_log_mysql 配置所需的连接参数。
func (e *MysqlEnv) Config() (addr string, user string, password string, dbName string) {
	return e.addr, e.user, e.passwd, e.dbName
}

// ApplyDDL 在测试库中执行 DDL 文件（单语句）。
func (e *MysqlEnv) ApplyDDL(t *testing.T, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read ddl file failed: %v", err)
	}
	stmts := splitSQLStatements(string(data))
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		if _, err := e.db.Exec(stmt); err != nil {
			t.Fatalf("apply ddl failed: %v\nstmt: %s", err, stmt)
		}
	}
}

// TableCount 查询目标表行数。
func (e *MysqlEnv) TableCount(t *testing.T, table string) int {
	t.Helper()

	var n int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count table %s failed: %v", table, err)
	}
	return n
}

// WaitTableCount 轮询等待目标表行数达到期望值。
func (e *MysqlEnv) WaitTableCount(t *testing.T, table string, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.TableCount(t, table) >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("table %s row count = %d, want >= %d (timeout %v)",
		table, e.TableCount(t, table), want, timeout)
}

// Close 释放环境资源：关闭连接、清理测试库/容器。
func (e *MysqlEnv) Close() {
	if e.db != nil {
		e.db.Close()
	}
	if e.cleanup != nil {
		e.cleanup()
	}
}

// Pause 冻结 MySQL 容器进程，模拟 MySQL 不可达（新连接挂起）。
// 仅 testcontainers 后端可用；外部 LR_MYSQL_DSN 后端调用即 skip。
func (e *MysqlEnv) Pause(t *testing.T) {
	t.Helper()
	cli := e.pauseClient(t)
	ctx := context.Background()
	if err := cli.ContainerPause(ctx, e.container.GetContainerID()); err != nil {
		t.Fatalf("pause mysql container failed: %v", err)
	}
}

// Unpause 恢复被 Pause 的 MySQL 容器。
// 仅 testcontainers 后端可用；外部 LR_MYSQL_DSN 后端调用即 skip。
func (e *MysqlEnv) Unpause(t *testing.T) {
	t.Helper()
	cli := e.pauseClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cli.ContainerUnpause(ctx, e.container.GetContainerID()); err != nil {
		t.Fatalf("unpause mysql container failed: %v", err)
	}
}

// pauseClient 返回 docker client；非 testcontainers 后端（外部 DSN）调用即 skip
func (e *MysqlEnv) pauseClient(t *testing.T) *client.Client {
	t.Helper()
	if e.container == nil {
		t.Skip("Pause/Unpause requires testcontainers mysql (LR_MYSQL_DSN backend unsupported)")
	}
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf("create docker client failed: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// splitSQLStatements 按分号切分 SQL 脚本（去除 -- 行注释），供 ApplyDDL 逐条执行。
func splitSQLStatements(script string) []string {
	var stmts []string
	var sb strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		sb.WriteString(line)
		sb.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			stmts = append(stmts, strings.TrimSpace(sb.String()))
			sb.Reset()
		}
	}
	if rest := strings.TrimSpace(sb.String()); rest != "" {
		stmts = append(stmts, rest)
	}
	return stmts
}

// dockerAvailable 检查本机 Docker daemon 是否可用。
func dockerAvailable() bool {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return false
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Ping(ctx)
	return err == nil
}
