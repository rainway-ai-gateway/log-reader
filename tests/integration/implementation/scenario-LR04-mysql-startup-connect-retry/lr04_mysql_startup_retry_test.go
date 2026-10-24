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

package lr04

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"

	"github.com/rainway-ai-gateway/log-reader/tests/integration/common"
)

const (
	tableName = "bfe_ai_request_log"
)

// TestLR04_StartupMysqlDownThenRecover (TC-04-01): when MySQL is unreachable
// at startup, log-reader must stay up, keep retrying in background, buffer the
// incoming rows, and drain the backlog automatically after MySQL recovers.
//
// Requires Docker (testcontainers Pause/Unpause simulate mysql down/up);
// external LR_MYSQL_DSN backends skip via Pause.
func TestLR04_StartupMysqlDownThenRecover(t *testing.T) {
	mysqlEnv := common.NewMysqlEnv(t)
	addr, user, passwd, dbName := mysqlEnv.Config()
	mysqlEnv.ApplyDDL(t, filepath.Join("testdata", "ddl", "bfe_ai_request_log.sql"))

	// Simulate "mysql not ready at startup" before log-reader starts.
	mysqlEnv.Pause(t)

	processEnv := common.NewProcessEnv(t)
	processEnv.Build()

	confDir := filepath.Join(processEnv.WorkDir(), "conf")
	logDir := filepath.Join(processEnv.WorkDir(), "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("create log dir failed: %v", err)
	}
	logFilePath := filepath.Join(logDir, "pb_access3.log")
	monitorPort := common.FreePort(t)

	builder := &common.LogReaderConfigBuilder{
		TemplateDir:   "testdata",
		TargetConfDir: confDir,
		LogFilePath:   logFilePath,
		HttpPort:      monitorPort,
		MysqlAddr:     addr,
		MysqlUser:     user,
		MysqlPassword: passwd,
		MysqlDBName:   dbName,
	}
	if err := builder.Build(); err != nil {
		t.Fatalf("build log-reader config failed: %v", err)
	}

	stopReader, waitExit := processEnv.StartLogReaderOnPort(confDir, logDir, monitorPort)
	defer func() {
		stopReader()
		mysqlEnv.Close()
		processEnv.Cleanup()
	}()

	// 1. Process must NOT exit while mysql is down; monitor must be serving.
	if exited, code := waitExit(3 * time.Second); exited {
		t.Fatalf("log-reader exited (code %d) while mysql was down; expected to stay up", code)
	}
	down := common.WaitMonitor(t, monitorPort, "mod_log_mysql", 15*time.Second, func(m *common.MonitorState) bool {
		return m.States["MYSQL_CONN_STATE"] == "DOWN"
	})

	// 2. Retry loop is running: MYSQL_CONN_RETRY keeps growing.
	base := down.SCounters["MYSQL_CONN_RETRY"]
	common.WaitMonitor(t, monitorPort, "mod_log_mysql", 15*time.Second, func(m *common.MonitorState) bool {
		return m.SCounters["MYSQL_CONN_RETRY"] > base
	})

	// 3. Logs arriving while disconnected are buffered, not written.
	logGen := common.NewLogGenerator(t, logFilePath)
	defer logGen.Close()
	logs := []*bfe_access_pb.BfeLog{
		common.MakeRequestLog(40001, bfe_access_pb.ProductID_BFE, "retry.example.org", "/v1/chat", "retry-model-a"),
		common.MakeRequestLog(40002, bfe_access_pb.ProductID_BFE, "retry.example.org", "/v1/chat", "retry-model-b"),
		common.MakeRequestLog(40003, bfe_access_pb.ProductID_BFE, "retry.example.org", "/v1/embed", "retry-model-c"),
	}
	for _, log := range logs {
		logGen.MustWriteBfeLog(t, log)
	}
	common.WaitMonitor(t, monitorPort, "mod_log_mysql", 15*time.Second, func(m *common.MonitorState) bool {
		return m.SCounters["SENT_TO_MYSQL"] == int64(len(logs)) &&
			m.SCounters["WRITE_BATCH_SIZE"] == 0
	})

	// 4. MySQL recovers: retry loop connects, state flips to UP, backlog drains.
	mysqlEnv.Unpause(t)
	up := common.WaitMonitor(t, monitorPort, "mod_log_mysql", 60*time.Second, func(m *common.MonitorState) bool {
		return m.States["MYSQL_CONN_STATE"] == "UP"
	})
	if got := up.SCounters["MYSQL_CONN_OK"]; got != 1 {
		t.Errorf("MYSQL_CONN_OK = %d, want 1", got)
	}
	if got := up.SCounters["MYSQL_CONN_RETRY"]; got < 1 {
		t.Errorf("MYSQL_CONN_RETRY = %d, want >= 1", got)
	}

	mysqlEnv.WaitTableCount(t, tableName, len(logs), 60*time.Second)

	// Rows actually landed and match the written logids.
	rows, err := mysqlEnv.DB().Query(
		`SELECT COUNT(DISTINCT logid) FROM ` + tableName + ` WHERE logid BETWEEN 40001 AND 40003`)
	if err != nil {
		t.Fatalf("query landed rows failed: %v", err)
	}
	defer rows.Close()
	var distinct int
	for rows.Next() {
		if err := rows.Scan(&distinct); err != nil {
			t.Fatalf("scan landed rows failed: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate landed rows failed: %v", err)
	}
	if distinct != len(logs) {
		t.Errorf("distinct logids = %d, want %d", distinct, len(logs))
	}
}

// TestLR04_RuntimeDisconnectThenRecover (TC-04-03): after a successful
// startup, killing mysql at runtime must be detected (write timeout bounded by
// BatchTimeoutMs), the connection marked down and reconnected automatically;
// rows arriving while down are buffered and drained after recovery without any
// SEND_MYSQL_FAILED drops.
//
// Requires Docker (testcontainers Pause/Unpause); external LR_MYSQL_DSN
// backends skip via Pause.
func TestLR04_RuntimeDisconnectThenRecover(t *testing.T) {
	mysqlEnv := common.NewMysqlEnv(t)
	addr, user, passwd, dbName := mysqlEnv.Config()
	mysqlEnv.ApplyDDL(t, filepath.Join("testdata", "ddl", "bfe_ai_request_log.sql"))

	processEnv := common.NewProcessEnv(t)
	processEnv.Build()

	confDir := filepath.Join(processEnv.WorkDir(), "conf")
	logDir := filepath.Join(processEnv.WorkDir(), "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("create log dir failed: %v", err)
	}
	logFilePath := filepath.Join(logDir, "pb_access3.log")
	monitorPort := common.FreePort(t)

	builder := &common.LogReaderConfigBuilder{
		TemplateDir:   "testdata",
		TargetConfDir: confDir,
		LogFilePath:   logFilePath,
		HttpPort:      monitorPort,
		MysqlAddr:     addr,
		MysqlUser:     user,
		MysqlPassword: passwd,
		MysqlDBName:   dbName,
	}
	if err := builder.Build(); err != nil {
		t.Fatalf("build log-reader config failed: %v", err)
	}

	stopReader, waitExit := processEnv.StartLogReaderOnPort(confDir, logDir, monitorPort)
	defer func() {
		stopReader()
		mysqlEnv.Close()
		processEnv.Cleanup()
	}()

	logGen := common.NewLogGenerator(t, logFilePath)
	defer logGen.Close()

	// 1. Connected startup: two rows land.
	up := common.MakeRequestLog(41001, bfe_access_pb.ProductID_BFE, "rt.example.org", "/v1/chat", "rt-model-a")
	up2 := common.MakeRequestLog(41002, bfe_access_pb.ProductID_BFE, "rt.example.org", "/v1/chat", "rt-model-b")
	logGen.MustWriteBfeLog(t, up)
	logGen.MustWriteBfeLog(t, up2)
	mysqlEnv.WaitTableCount(t, tableName, 2, 60*time.Second)
	common.WaitMonitor(t, monitorPort, "mod_log_mysql", 15*time.Second, func(m *common.MonitorState) bool {
		return m.States["MYSQL_CONN_STATE"] == "UP"
	})

	// 2. Runtime disconnect: writes fail within BatchTimeoutMs per attempt,
	// the connection is marked down and reconnect starts.
	mysqlEnv.Pause(t)
	logGen.MustWriteBfeLog(t, common.MakeRequestLog(41003, bfe_access_pb.ProductID_BFE, "rt.example.org", "/v1/chat", "rt-model-c"))
	logGen.MustWriteBfeLog(t, common.MakeRequestLog(41004, bfe_access_pb.ProductID_BFE, "rt.example.org", "/v1/embed", "rt-model-d"))

	down := common.WaitMonitor(t, monitorPort, "mod_log_mysql", 90*time.Second, func(m *common.MonitorState) bool {
		return m.States["MYSQL_CONN_STATE"] == "DOWN"
	})
	if got := down.SCounters["SENT_TO_MYSQL"]; got != 4 {
		t.Errorf("SENT_TO_MYSQL = %d, want 4 (rows buffered while down)", got)
	}
	if got := down.SCounters["WRITE_BATCH_SIZE"]; got != 2 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 2 (no writes land while down)", got)
	}
	if exited, code := waitExit(100 * time.Millisecond); exited {
		t.Fatalf("log-reader exited (code %d) on runtime disconnect; expected to stay up", code)
	}

	// 3. Recovery: automatic reconnect, backlog drained, zero drops.
	mysqlEnv.Unpause(t)
	upAgain := common.WaitMonitor(t, monitorPort, "mod_log_mysql", 60*time.Second, func(m *common.MonitorState) bool {
		return m.States["MYSQL_CONN_STATE"] == "UP"
	})
	if got := upAgain.SCounters["MYSQL_CONN_LOST"]; got != 1 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 1", got)
	}
	if got := upAgain.SCounters["SEND_MYSQL_FAILED"]; got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0 (held batch rewritten after reconnect)", got)
	}

	mysqlEnv.WaitTableCount(t, tableName, 4, 60*time.Second)
	rows, err := mysqlEnv.DB().Query(
		`SELECT COUNT(DISTINCT logid) FROM ` + tableName + ` WHERE logid BETWEEN 41001 AND 41004`)
	if err != nil {
		t.Fatalf("query landed rows failed: %v", err)
	}
	defer rows.Close()
	var distinct int
	for rows.Next() {
		if err := rows.Scan(&distinct); err != nil {
			t.Fatalf("scan landed rows failed: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate landed rows failed: %v", err)
	}
	if distinct != 4 {
		t.Errorf("distinct logids = %d, want 4", distinct)
	}
}

// TestLR04_ConfigErrorStillFailFast (TC-04-02): config errors (empty Mysql.Addr)
// must still fail fast — the fail-fast boundary is only moved for connectivity
// errors. Needs no MySQL at all.
func TestLR04_ConfigErrorStillFailFast(t *testing.T) {
	processEnv := common.NewProcessEnv(t)
	processEnv.Build()

	confDir := filepath.Join(processEnv.WorkDir(), "conf")
	logDir := filepath.Join(processEnv.WorkDir(), "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("create log dir failed: %v", err)
	}
	monitorPort := common.FreePort(t)

	builder := &common.LogReaderConfigBuilder{
		TemplateDir:   "testdata",
		TargetConfDir: confDir,
		LogFilePath:   filepath.Join(logDir, "pb_access3.log"),
		HttpPort:      monitorPort,
		MysqlAddr:     "127.0.0.1:1",
		MysqlUser:     "report",
		MysqlPassword: "x",
		MysqlDBName:   "bfe_report",
	}
	if err := builder.Build(); err != nil {
		t.Fatalf("build log-reader config failed: %v", err)
	}

	// Break the config: empty Addr is rejected by ConfModLogMysqlCheck.
	confPath := filepath.Join(confDir, "mod_log_mysql", "mod_log_mysql.conf")
	content, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("read built conf failed: %v", err)
	}
	broken := strings.Replace(string(content), "Addr = 127.0.0.1:1", "Addr = ", 1)
	if err := os.WriteFile(confPath, []byte(broken), 0644); err != nil {
		t.Fatalf("write broken conf failed: %v", err)
	}

	stopReader, waitExit := processEnv.StartLogReaderOnPort(confDir, logDir, monitorPort)
	defer stopReader()

	exited, code := waitExit(10 * time.Second)
	if !exited {
		t.Fatal("log-reader stayed up with an invalid config; expected fail-fast exit")
	}
	if code == 0 {
		t.Errorf("exit code = %d, want non-zero", code)
	}
}
