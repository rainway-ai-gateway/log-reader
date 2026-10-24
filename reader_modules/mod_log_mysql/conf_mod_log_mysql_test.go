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
	"os"
	"path/filepath"
	"testing"
)

func writeTempLogMysqlConf(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mod_log_mysql.conf")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func validConf() *ConfModLogMysql {
	return &ConfModLogMysql{
		Mysql: ConfMysql{
			Addr:   "127.0.0.1:3306",
			User:   "report",
			DBName: "bfe_report",
			Table:  "bfe_ai_request_log",
		},
	}
}

func TestConfModLogMysqlCheck_Defaults(t *testing.T) {
	cfg := validConf()
	if err := ConfModLogMysqlCheck(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Writer.QueueSize != 2000 {
		t.Errorf("QueueSize should default to 2000, got %d", cfg.Writer.QueueSize)
	}
	if cfg.Writer.BatchSize != 200 {
		t.Errorf("BatchSize should default to 200, got %d", cfg.Writer.BatchSize)
	}
	if cfg.Writer.FlushIntervalMs != 2000 {
		t.Errorf("FlushIntervalMs should default to 2000, got %d", cfg.Writer.FlushIntervalMs)
	}
	if cfg.Writer.MaxRetries != 3 {
		t.Errorf("MaxRetries should default to 3, got %d", cfg.Writer.MaxRetries)
	}
	if cfg.Writer.MaxOpenConns != 10 {
		t.Errorf("MaxOpenConns should default to 10, got %d", cfg.Writer.MaxOpenConns)
	}
	if cfg.Writer.MaxIdleConns != 5 {
		t.Errorf("MaxIdleConns should default to 5, got %d", cfg.Writer.MaxIdleConns)
	}
	if cfg.Mysql.ConnectTimeoutMs != 3000 {
		t.Errorf("ConnectTimeoutMs should default to 3000, got %d", cfg.Mysql.ConnectTimeoutMs)
	}
	if cfg.Mysql.ConnectRetryIntervalMs != 3000 {
		t.Errorf("ConnectRetryIntervalMs should default to 3000, got %d", cfg.Mysql.ConnectRetryIntervalMs)
	}
	if cfg.Mysql.HealthPingIntervalMs != 0 {
		t.Errorf("HealthPingIntervalMs should default to 0, got %d", cfg.Mysql.HealthPingIntervalMs)
	}
	if cfg.Writer.BatchTimeoutMs != 30000 {
		t.Errorf("BatchTimeoutMs should default to 30000, got %d", cfg.Writer.BatchTimeoutMs)
	}
}

func TestConfModLogMysqlCheck_EmptyAddr(t *testing.T) {
	cfg := validConf()
	cfg.Mysql.Addr = ""
	if err := ConfModLogMysqlCheck(cfg); err == nil {
		t.Error("expected error for empty Addr")
	}
}

func TestConfModLogMysqlCheck_EmptyUser(t *testing.T) {
	cfg := validConf()
	cfg.Mysql.User = ""
	if err := ConfModLogMysqlCheck(cfg); err == nil {
		t.Error("expected error for empty User")
	}
}

func TestConfModLogMysqlCheck_EmptyDBName(t *testing.T) {
	cfg := validConf()
	cfg.Mysql.DBName = ""
	if err := ConfModLogMysqlCheck(cfg); err == nil {
		t.Error("expected error for empty DBName")
	}
}

func TestConfModLogMysqlCheck_EmptyTable(t *testing.T) {
	cfg := validConf()
	cfg.Mysql.Table = ""
	if err := ConfModLogMysqlCheck(cfg); err == nil {
		t.Error("expected error for empty Table")
	}
}

func TestConfModLogMysqlCheck_InvalidWriterValues(t *testing.T) {
	cfg := validConf()
	cfg.Writer.QueueSize = -1
	cfg.Writer.BatchSize = 0
	cfg.Writer.FlushIntervalMs = -100
	cfg.Writer.MaxRetries = 0
	cfg.Writer.MaxOpenConns = -5
	cfg.Writer.MaxIdleConns = 0
	cfg.Mysql.ConnectTimeoutMs = 0
	cfg.Mysql.ConnectRetryIntervalMs = -1
	cfg.Mysql.HealthPingIntervalMs = -5
	cfg.Writer.BatchTimeoutMs = 0

	if err := ConfModLogMysqlCheck(cfg); err != nil {
		t.Fatalf("invalid writer values should be replaced by defaults, got error: %v", err)
	}
	if cfg.Writer.QueueSize != 2000 || cfg.Writer.BatchSize != 200 ||
		cfg.Writer.FlushIntervalMs != 2000 || cfg.Writer.MaxRetries != 3 ||
		cfg.Writer.MaxOpenConns != 10 || cfg.Writer.MaxIdleConns != 5 ||
		cfg.Mysql.ConnectTimeoutMs != 3000 || cfg.Mysql.ConnectRetryIntervalMs != 3000 ||
		cfg.Mysql.HealthPingIntervalMs != 0 || cfg.Writer.BatchTimeoutMs != 30000 {
		t.Errorf("invalid values not replaced by defaults: %+v", cfg)
	}
}

func TestLoadConfig(t *testing.T) {
	content := `
[Basic]
OpenDebug = true

[mysql]
Addr = 10.0.0.1:3306
User = report
Password = secret123
DBName = bfe_report
Table = bfe_ai_request_log
ConnectTimeoutMs = 1500
ConnectRetryIntervalMs = 500
HealthPingIntervalMs = 10000

[Writer]
QueueSize = 3000
BatchSize = 100
FlushIntervalMs = 500
MaxRetries = 5
MaxOpenConns = 20
MaxIdleConns = 8
BatchTimeoutMs = 45000
`
	path := writeTempLogMysqlConf(t, content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if !cfg.Basic.OpenDebug {
		t.Error("OpenDebug should be true")
	}
	if cfg.Mysql.Addr != "10.0.0.1:3306" {
		t.Errorf("Addr = %q", cfg.Mysql.Addr)
	}
	if cfg.Mysql.User != "report" {
		t.Errorf("User = %q", cfg.Mysql.User)
	}
	if cfg.Mysql.Password != "secret123" {
		t.Errorf("Password = %q", cfg.Mysql.Password)
	}
	if cfg.Mysql.DBName != "bfe_report" {
		t.Errorf("DBName = %q", cfg.Mysql.DBName)
	}
	if cfg.Mysql.Table != "bfe_ai_request_log" {
		t.Errorf("Table = %q", cfg.Mysql.Table)
	}
	if cfg.Mysql.ConnectTimeoutMs != 1500 {
		t.Errorf("ConnectTimeoutMs = %d", cfg.Mysql.ConnectTimeoutMs)
	}
	if cfg.Mysql.ConnectRetryIntervalMs != 500 {
		t.Errorf("ConnectRetryIntervalMs = %d", cfg.Mysql.ConnectRetryIntervalMs)
	}
	if cfg.Mysql.HealthPingIntervalMs != 10000 {
		t.Errorf("HealthPingIntervalMs = %d", cfg.Mysql.HealthPingIntervalMs)
	}
	if cfg.Writer.QueueSize != 3000 {
		t.Errorf("QueueSize = %d", cfg.Writer.QueueSize)
	}
	if cfg.Writer.BatchSize != 100 {
		t.Errorf("BatchSize = %d", cfg.Writer.BatchSize)
	}
	if cfg.Writer.FlushIntervalMs != 500 {
		t.Errorf("FlushIntervalMs = %d", cfg.Writer.FlushIntervalMs)
	}
	if cfg.Writer.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d", cfg.Writer.MaxRetries)
	}
	if cfg.Writer.MaxOpenConns != 20 {
		t.Errorf("MaxOpenConns = %d", cfg.Writer.MaxOpenConns)
	}
	if cfg.Writer.MaxIdleConns != 8 {
		t.Errorf("MaxIdleConns = %d", cfg.Writer.MaxIdleConns)
	}
	if cfg.Writer.BatchTimeoutMs != 45000 {
		t.Errorf("BatchTimeoutMs = %d", cfg.Writer.BatchTimeoutMs)
	}
}

func TestLoadConfig_Invalid(t *testing.T) {
	path := writeTempLogMysqlConf(t, "[mysql]\nAddr = \nUser = \n")
	if _, err := LoadConfig(path); err == nil {
		t.Error("expected error for invalid config")
	}
}
