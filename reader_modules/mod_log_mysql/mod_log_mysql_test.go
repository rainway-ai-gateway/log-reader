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

package mod_log_mysql

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/reader_conf"
)

func TestModuleLogMysql_Name(t *testing.T) {
	m := NewModuleLogMysql()
	if m.Name() != "mod_log_mysql" {
		t.Errorf("Name() = %q, want mod_log_mysql", m.Name())
	}
}

func TestModuleLogMysql_Close_NilWriter(t *testing.T) {
	m := NewModuleLogMysql()
	if err := m.Close(); err != nil {
		t.Errorf("Close with nil writer should not error, got %v", err)
	}
}

// fakeRecordWriter implements recordWriter for Update tests.
type fakeRecordWriter struct {
	enqueueOk  bool
	enqueued   int
	started    bool
	closed     bool
	connected  bool
	connectErr error
}

func (f *fakeRecordWriter) Enqueue(row []interface{}) bool {
	if f.enqueueOk {
		f.enqueued++
	}
	return f.enqueueOk
}
func (f *fakeRecordWriter) Connect() error  { return f.connectErr }
func (f *fakeRecordWriter) Connected() bool { return f.connected }
func (f *fakeRecordWriter) Start()          { f.started = true }
func (f *fakeRecordWriter) Close()          { f.closed = true }

// failMapper implements fieldMapper returning an error.
type failMapper struct{}

func (failMapper) ToRow(log *bfe_access_pb.BfeLog) ([]interface{}, error) {
	return nil, errors.New("row assemble failed")
}

func newTestModule() *ModuleLogMysql {
	m := NewModuleLogMysql()
	m.state.Init()
	m.state.CountersInit(COUNTER_KEYS)
	m.mapper = NewFieldMapper()
	m.writer = &fakeRecordWriter{enqueueOk: true}
	return m
}

func TestModuleLogMysql_Update(t *testing.T) {
	m := newTestModule()

	logid := uint64(1)
	ts := uint64(100_000_000_000) // 100s，Unix 纳秒
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
		{
			Logid:   &logid,
			LogType: bfe_access_pb.BfeLogType_Session.Enum(),
		},
	}

	m.Update(logs)

	if got := m.state.GetCounter("RECEIVED_LOGS"); got != 2 {
		t.Errorf("RECEIVED_LOGS = %d, want 2", got)
	}
	if got := m.state.GetCounter("RECEIVED_REQ"); got != 1 {
		t.Errorf("RECEIVED_REQ = %d, want 1 (session log filtered out)", got)
	}
	if got := m.state.GetCounter("SENT_TO_MYSQL"); got != 1 {
		t.Errorf("SENT_TO_MYSQL = %d, want 1", got)
	}
	if got := m.state.GetCounter("SENT_MYSQL_CHN_FULL"); got != 0 {
		t.Errorf("SENT_MYSQL_CHN_FULL = %d, want 0", got)
	}
}

func TestModuleLogMysql_Update_ChannelFull(t *testing.T) {
	m := newTestModule()
	m.writer = &fakeRecordWriter{enqueueOk: false}

	logid := uint64(1)
	ts := uint64(100_000_000_000) // 100s，Unix 纳秒
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
	}

	m.Update(logs)

	if got := m.state.GetCounter("SENT_TO_MYSQL"); got != 0 {
		t.Errorf("SENT_TO_MYSQL = %d, want 0", got)
	}
	if got := m.state.GetCounter("SENT_MYSQL_CHN_FULL"); got != 1 {
		t.Errorf("SENT_MYSQL_CHN_FULL = %d, want 1", got)
	}
}

func TestModuleLogMysql_Update_ConvertFailed(t *testing.T) {
	m := newTestModule()
	m.mapper = failMapper{}

	logid := uint64(1)
	ts := uint64(100_000_000_000) // 100s，Unix 纳秒
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
	}

	m.Update(logs)

	if got := m.state.GetCounter("CONVERT_FAILED"); got != 1 {
		t.Errorf("CONVERT_FAILED = %d, want 1", got)
	}
	if got := m.state.GetCounter("SENT_TO_MYSQL"); got != 0 {
		t.Errorf("SENT_TO_MYSQL = %d, want 0", got)
	}
}

func TestModuleLogMysql_MonitorHandlers(t *testing.T) {
	m := newTestModule()
	m.state.SetKeyPrefix(m.name)
	m.state.SetProgramName("log_reader")

	handlers := m.monitorHandlers()
	if _, ok := handlers["mod_log_mysql"]; !ok {
		t.Error("monitorHandlers should contain mod_log_mysql")
	}
	if _, ok := handlers["mod_log_mysql_diff"]; !ok {
		t.Error("monitorHandlers should contain mod_log_mysql_diff")
	}

	getState, ok := handlers["mod_log_mysql"].(func(url.Values) ([]byte, error))
	if !ok {
		t.Fatal("mod_log_mysql handler should be func(url.Values) ([]byte, error)")
	}
	body, err := getState(url.Values{})
	if err != nil {
		t.Fatalf("getState failed: %v", err)
	}
	for _, key := range COUNTER_KEYS {
		if !strings.Contains(string(body), key) {
			t.Errorf("getState output should contain counter %q, got:\n%s", key, body)
		}
	}

	getStateDiff, ok := handlers["mod_log_mysql_diff"].(func(url.Values) ([]byte, error))
	if !ok {
		t.Fatal("mod_log_mysql_diff handler should be func(url.Values) ([]byte, error)")
	}
	if _, err := getStateDiff(url.Values{}); err != nil {
		t.Fatalf("getStateDiff failed: %v", err)
	}
}

// TestModuleLogMysql_InitMysqlUnreachable: a connectivity error at Init must be
// non-fatal (Init returns nil, state DOWN, writer created unconnected).
func TestModuleLogMysql_InitMysqlUnreachable(t *testing.T) {
	// address that refuses connections
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	confRoot := writeInitConf(t, fmt.Sprintf(`
[mysql]
Addr = %s
User = report
DBName = bfe_report
Table = bfe_ai_request_log
ConnectTimeoutMs = 1000
ConnectRetryIntervalMs = 50
`, addr))

	m := NewModuleLogMysql()
	if err := m.Init(newInitReaderConfig(), web_monitor.NewWebHandlers(), confRoot); err != nil {
		t.Fatalf("Init should be non-fatal on unreachable mysql, got: %v", err)
	}
	defer m.Close()

	if got := m.state.GetState("MYSQL_CONN_STATE"); got != "DOWN" {
		t.Errorf("MYSQL_CONN_STATE = %q, want DOWN", got)
	}
	if m.writer == nil {
		t.Fatal("writer should be created")
	}
	if m.writer.Connected() {
		t.Error("writer should not be connected")
	}
}

// TestModuleLogMysql_InitInvalidConf: config errors stay fail-fast.
func TestModuleLogMysql_InitInvalidConf(t *testing.T) {
	// empty Addr: rejected by ConfModLogMysqlCheck
	confRoot := writeInitConf(t, `
[mysql]
User = report
DBName = bfe_report
Table = bfe_ai_request_log
`)

	m := NewModuleLogMysql()
	if err := m.Init(newInitReaderConfig(), web_monitor.NewWebHandlers(), confRoot); err == nil {
		t.Fatal("Init should fail-fast on config error (empty Addr)")
	}
}

func newInitReaderConfig() *reader_conf.ReaderConfig {
	return &reader_conf.ReaderConfig{
		Main: reader_conf.ConfBasic{ProgramName: "log-reader", MonitorInterval: 20},
	}
}

func writeInitConf(t *testing.T, content string) string {
	t.Helper()
	confRoot := t.TempDir()
	modDir := filepath.Join(confRoot, "mod_log_mysql")
	if err := os.MkdirAll(modDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modDir, "mod_log_mysql.conf"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return confRoot
}
