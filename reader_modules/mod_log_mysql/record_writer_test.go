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
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	gomysql "github.com/go-sql-driver/mysql"
)

func newTestConf() *ConfModLogMysql {
	return &ConfModLogMysql{
		Mysql: ConfMysql{
			Addr:                   "127.0.0.1:3306",
			User:                   "report",
			DBName:                 "bfe_report",
			Table:                  "test_table",
			ConnectTimeoutMs:       3000,
			ConnectRetryIntervalMs: 50, // fast retry in tests
			HealthPingIntervalMs:   0,  // health ping disabled by default
		},
		Writer: ConfLogMysqlWriter{
			QueueSize:       16,
			BatchSize:       3,
			FlushIntervalMs: 60000, // long interval: count-based flush in most tests
			MaxRetries:      1,
			MaxOpenConns:    2,
			MaxIdleConns:    1,
			BatchTimeoutMs:  5000, // generous: no interference unless overridden
		},
	}
}

func newTestState() *module_state2.State {
	state := &module_state2.State{}
	state.Init()
	state.CountersInit(COUNTER_KEYS)
	return state
}

// newTestWriterWithConf creates a writer wired to a sqlmock db: the connector
// seam is replaced so Connect() returns the mock db directly (ping assumed).
// Connect() is already called; the writer is not started.
func newTestWriterWithConf(t *testing.T, conf *ConfModLogMysql) (*RecordWriter, sqlmock.Sqlmock, *module_state2.State) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) { return db, nil }
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	return w, mock, state
}

func newTestWriter(t *testing.T) (*RecordWriter, sqlmock.Sqlmock, *module_state2.State) {
	t.Helper()
	return newTestWriterWithConf(t, newTestConf())
}

// makeRow returns a row with one value per column.
func makeRow() []interface{} {
	return make([]interface{}, len(Columns()))
}

// expectedInsertSQL builds the expected INSERT statement independently from the
// DDL column list hardcoded in field_mapper_test.go.
func expectedInsertSQL(table string, cols []string, n int) string {
	row := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
	rows := make([]string, n)
	for i := range rows {
		rows[i] = row
	}

	updates := make([]string, len(cols))
	for i, c := range cols {
		updates[i] = c + "=VALUES(" + c + ")"
	}

	return "INSERT INTO " + table + " (" + strings.Join(cols, ",") + ") VALUES " +
		strings.Join(rows, ",") + " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
}

func waitForCounter(t *testing.T, state *module_state2.State, key string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if state.GetCounter(key) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting counter %s == %d, current %d", key, want, state.GetCounter(key))
}

func TestRecordWriter_BatchByCount(t *testing.T) {
	w, mock, state := newTestWriter(t)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 3))).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	w.Start()

	for i := 0; i < 3; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 3, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_BatchByTimeout(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 100
	conf.Writer.FlushIntervalMs = 50
	w, mock, state := newTestWriterWithConf(t, conf)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 1))).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w.Start()

	if !w.Enqueue(makeRow()) {
		t.Fatal("Enqueue failed")
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 1, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_SQLShape(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 2
	w, mock, state := newTestWriterWithConf(t, conf)

	// exact statement: placeholder count == 2 * 89, ON DUPLICATE KEY UPDATE covers all columns
	stmt := w.buildStmt(2)
	expected := expectedInsertSQL("test_table", ddlColumns, 2)
	if stmt != expected {
		t.Fatalf("buildStmt mismatch:\n got: %s\nwant: %s", stmt, expected)
	}
	if strings.Count(stmt, "?") != 2*len(ddlColumns) {
		t.Errorf("placeholder count = %d, want %d", strings.Count(stmt, "?"), 2*len(ddlColumns))
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expected)).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	w.Start()
	w.Enqueue(makeRow())
	w.Enqueue(makeRow())

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 2, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_RetryThenSuccess(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1 // flush on every enqueue
	w, mock, state := newTestWriterWithConf(t, conf)

	// first attempt fails, retry (after 200ms backoff) succeeds
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("connection lost"))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO test_table").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	w.Start()
	w.Enqueue(makeRow())

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 1, 5*time.Second)
	w.Close()

	if got := state.GetCounter("SEND_MYSQL_FAILED"); got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_RetryExhausted(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1
	conf.Writer.MaxRetries = 2 // initial attempt + 2 retries (200ms + 400ms backoff)
	w, mock, state := newTestWriterWithConf(t, conf)

	for i := 0; i < 3; i++ {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("db down"))
		mock.ExpectRollback()
	}

	w.Start()
	w.Enqueue(makeRow())

	waitForCounter(t, state, "SEND_MYSQL_FAILED", 1, 5*time.Second)
	w.Close()

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 0", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_DrainOnClose(t *testing.T) {
	w, mock, state := newTestWriter(t)

	// batch not full (2 < 3), Close must drain and flush the residual rows
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 2))).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	w.Start()
	w.Enqueue(makeRow())
	w.Enqueue(makeRow())
	w.Close() // drain + final flush happens before Close returns

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 2 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 2 after drain", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestRecordWriter_EnqueueFull(t *testing.T) {
	conf := newTestConf()
	conf.Writer.QueueSize = 1
	w := NewRecordWriter(conf, newTestState()) // not started: channel never drains

	if !w.Enqueue(makeRow()) {
		t.Error("first Enqueue should succeed")
	}
	if w.Enqueue(makeRow()) {
		t.Error("second Enqueue should fail when channel full")
	}
	w.Close()
}

// TestRecordWriter_ConnectIdempotent: a second Connect on an already connected
// writer must be a no-op (the connector is not invoked again).
func TestRecordWriter_ConnectIdempotent(t *testing.T) {
	var calls int32
	w := NewRecordWriter(newTestConf(), newTestState())
	// fail twice, then swap in a fake db
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if atomic.AddInt32(&calls, 1) <= 2 {
			return nil, errors.New("mysql down")
		}
		db, _, err := sqlmock.New()
		return db, err
	}
	if err := w.Connect(); err == nil {
		t.Fatal("first Connect should fail")
	}
	if err := w.Connect(); err == nil {
		t.Fatal("second Connect should fail")
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("third Connect should succeed: %v", err)
	}
	if !w.Connected() {
		t.Fatal("Connected() should be true")
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect on connected writer should be no-op: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("connector called %d times, want 3 (no-op on connected)", got)
	}
}

// TestRecordWriter_BufferThenConnectDrain: while mysql is unreachable the
// writeLoop must not consume the queue (rows accumulate in ch); after the
// retry loop connects, the backlog drains in batches as before.
func TestRecordWriter_BufferThenConnectDrain(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 2           // flush by count once both rows arrived
	conf.Writer.FlushIntervalMs = 60000 // disable ticker flush: keep the batch atomic

	var failing atomic.Bool
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if failing.Load() {
			return nil, errors.New("mysql down")
		}
		return db, nil
	}

	failing.Store(true)
	if err := w.Connect(); err == nil {
		t.Fatal("initial Connect should fail while failing is set")
	}
	w.Start() // writeLoop parks; connSupervisor retries every 50ms

	// rows enqueue into the buffered queue while disconnected
	for i := 0; i < 2; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	// retry loop is running; nothing is written yet
	waitForCounter(t, state, "MYSQL_CONN_RETRY", 2, 5*time.Second)
	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Fatalf("WRITE_BATCH_SIZE = %d, want 0 while disconnected", got)
	}

	// expect a single 2-row batch for the buffered backlog
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 2))).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	failing.Store(false)
	waitForCounter(t, state, "MYSQL_CONN_OK", 1, 5*time.Second)
	if !w.Connected() {
		t.Fatal("Connected() should be true after retry")
	}
	if got := state.GetState("MYSQL_CONN_STATE"); got != "UP" {
		t.Errorf("MYSQL_CONN_STATE = %q, want UP", got)
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 2, 5*time.Second)
	w.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_CloseBeforeConnected: Close while the retry loop is still
// running must shut down both goroutines promptly and drop buffered rows.
func TestRecordWriter_CloseBeforeConnected(t *testing.T) {
	conf := newTestConf()
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		return nil, errors.New("mysql down")
	}

	w.Start()
	w.Enqueue(makeRow())
	w.Enqueue(makeRow())

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked with unconnected writer")
	}

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 0 (buffered rows dropped)", got)
	}
	if got := state.GetCounter("MYSQL_CONN_OK"); got != 0 {
		t.Errorf("MYSQL_CONN_OK = %d, want 0", got)
	}
}

// TestRecordWriter_WriteExhaustedThenReconnect: when write retries are
// exhausted the writer marks the connection down, the supervisor reconnects,
// the held batch is rewritten first, and rows enqueued while down are drained
// afterwards. Nothing is counted as SEND_MYSQL_FAILED.
func TestRecordWriter_WriteExhaustedThenReconnect(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1 // flush on every enqueue
	conf.Writer.MaxRetries = 1

	db1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}

	var useSecond atomic.Bool
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if useSecond.Load() {
			return db2, nil
		}
		return db1, nil
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// db1: both write attempts fail
	mock1.ExpectBegin()
	mock1.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("connection lost"))
	mock1.ExpectRollback()
	mock1.ExpectBegin()
	mock1.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("connection lost"))
	mock1.ExpectRollback()

	// db2: pending-batch rewrite + three rows buffered while down
	for i := 0; i < 4; i++ {
		mock2.ExpectBegin()
		mock2.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 1))).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock2.ExpectCommit()
	}

	useSecond.Store(true)
	w.Start()

	for i := 0; i < 4; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 4, 5*time.Second)
	w.Close()

	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 1 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 1", got)
	}
	if got := state.GetCounter("MYSQL_CONN_OK"); got != 1 {
		t.Errorf("MYSQL_CONN_OK = %d, want 1 (supervisor reconnect)", got)
	}
	if got := state.GetCounter("SEND_MYSQL_FAILED"); got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0 (held batch rewritten after reconnect)", got)
	}
	if got := state.GetState("MYSQL_CONN_STATE"); got != "UP" {
		t.Errorf("MYSQL_CONN_STATE = %q, want UP", got)
	}
	if err := mock1.ExpectationsWereMet(); err != nil {
		t.Errorf("mock1 unfulfilled expectations: %v", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Errorf("mock2 unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_RewriteAfterReconnectStillFails: if the rewrite after a
// reconnect still fails (e.g. table dropped), the held batch is dropped and
// counted as SEND_MYSQL_FAILED, and the write loop keeps consuming.
func TestRecordWriter_RewriteAfterReconnectStillFails(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1
	conf.Writer.MaxRetries = 0 // single failing attempt on db1

	db1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}

	var useSecond atomic.Bool
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if useSecond.Load() {
			return db2, nil
		}
		return db1, nil
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	mock1.ExpectBegin()
	mock1.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("table dropped"))
	mock1.ExpectRollback()

	// reconnect succeeds at connection level, but the rewrite still fails
	mock2.ExpectBegin()
	mock2.ExpectExec("INSERT INTO test_table").WillReturnError(errors.New("table dropped"))
	mock2.ExpectRollback()

	useSecond.Store(true)
	w.Start()

	if !w.Enqueue(makeRow()) {
		t.Fatal("Enqueue failed")
	}

	waitForCounter(t, state, "SEND_MYSQL_FAILED", 1, 5*time.Second)
	w.Close()

	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 1 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 1", got)
	}
	if got := state.GetCounter("MYSQL_CONN_OK"); got != 1 {
		t.Errorf("MYSQL_CONN_OK = %d, want 1", got)
	}
	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 0", got)
	}
	if err := mock1.ExpectationsWereMet(); err != nil {
		t.Errorf("mock1 unfulfilled expectations: %v", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Errorf("mock2 unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_MarkDownIdempotent: repeated markDown calls count a single
// connection loss and keep the writer down.
func TestRecordWriter_MarkDownIdempotent(t *testing.T) {
	w, _, state := newTestWriter(t)

	w.markDown("test reason 1")
	w.markDown("test reason 2")

	if w.Connected() {
		t.Error("Connected() should be false after markDown")
	}
	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 1 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 1 (idempotent)", got)
	}
	if got := state.GetState("MYSQL_CONN_STATE"); got != "DOWN" {
		t.Errorf("MYSQL_CONN_STATE = %q, want DOWN", got)
	}
	w.Close()
}

// TestRecordWriter_BatchTimeoutTriggersReconnect: a batch execution hanging
// beyond BatchTimeoutMs is cancelled by context deadline, which drives the
// writer into markDown and reconnect; the held batch is rewritten on the new
// connection.
func TestRecordWriter_BatchTimeoutTriggersReconnect(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1
	conf.Writer.MaxRetries = 0 // single attempt
	conf.Writer.BatchTimeoutMs = 50

	db1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	db2, mock2, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}

	var useSecond atomic.Bool
	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) {
		if useSecond.Load() {
			return db2, nil
		}
		return db1, nil
	}
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// execution hangs far beyond BatchTimeoutMs
	mock1.ExpectBegin()
	mock1.ExpectExec("INSERT INTO test_table").
		WillDelayFor(500 * time.Millisecond).
		WillReturnError(errors.New("db down"))
	mock1.ExpectRollback()

	mock2.ExpectBegin()
	mock2.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 1))).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock2.ExpectCommit()

	useSecond.Store(true)
	w.Start()

	if !w.Enqueue(makeRow()) {
		t.Fatal("Enqueue failed")
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 1, 10*time.Second)
	w.Close()

	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 1 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 1", got)
	}
	if got := state.GetCounter("SEND_MYSQL_FAILED"); got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0", got)
	}
	if err := mock1.ExpectationsWereMet(); err != nil {
		t.Errorf("mock1 unfulfilled expectations: %v", err)
	}
	if err := mock2.ExpectationsWereMet(); err != nil {
		t.Errorf("mock2 unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_PingFailureTriggersMarkDown: with HealthPingIntervalMs > 0,
// a failed health ping marks the connection down and counts MYSQL_CONN_LOST.
func TestRecordWriter_PingFailureTriggersMarkDown(t *testing.T) {
	conf := newTestConf()
	conf.Mysql.HealthPingIntervalMs = 20

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	mock.ExpectPing() // first ping succeeds; the next one errors (no expectation left)

	state := newTestState()
	w := NewRecordWriter(conf, state)
	w.connect = func(*ConfModLogMysql) (*sql.DB, error) { return db, nil }
	if err := w.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	w.Start()
	defer w.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if state.GetCounter("MYSQL_CONN_LOST") >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("MYSQL_CONN_LOST still 0 after 5s (health ping should have failed)")
}

// outOfRangeErr builds the driver error observed in issue #12: Error 1264 with
// the row locator "at row N" (1-based over the multi-value VALUES list).
func outOfRangeErr(n int) error {
	return &gomysql.MySQLError{
		Number:  1264,
		Message: fmt.Sprintf("Out of range value for column 'proxy_delay_time' at row %d", n),
	}
}

// TestRecordWriter_RowEviction (issue #12): a batch write failing with a
// row-locatable data error (1264 + "at row N") must drop only that row, rewrite
// the remainder immediately (no backoff, no markDown), count the dropped row
// in ROW_DROPPED_DATA and the landed rows in WRITE_BATCH_SIZE.
func TestRecordWriter_RowEviction(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 3
	w, mock, state := newTestWriterWithConf(t, conf)

	// batch of 3: first attempt fails locating row 2, rewrite of 2 rows succeeds
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 3))).
		WillReturnError(outOfRangeErr(2))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 2))).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	w.Start()
	for i := 0; i < 3; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	waitForCounter(t, state, "WRITE_BATCH_SIZE", 2, 5*time.Second)
	w.Close()

	if got := state.GetCounter("ROW_DROPPED_DATA"); got != 1 {
		t.Errorf("ROW_DROPPED_DATA = %d, want 1", got)
	}
	if got := state.GetCounter("SEND_MYSQL_FAILED"); got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0 (row eviction is not a structural error)", got)
	}
	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 0 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 0 (no markDown on data errors)", got)
	}
	if !w.Connected() {
		t.Error("writer should stay connected after row eviction")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_RowEvictionAllRows: every row of the batch is out of range;
// each eviction retries the remainder until the batch is empty, which is a
// normal completion (no markDown, no SEND_MYSQL_FAILED, bounded loop).
func TestRecordWriter_RowEvictionAllRows(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 2
	w, mock, state := newTestWriterWithConf(t, conf)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 2))).
		WillReturnError(outOfRangeErr(1))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(expectedInsertSQL("test_table", ddlColumns, 1))).
		WillReturnError(outOfRangeErr(1))
	mock.ExpectRollback()

	w.Start()
	for i := 0; i < 2; i++ {
		if !w.Enqueue(makeRow()) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}

	waitForCounter(t, state, "ROW_DROPPED_DATA", 2, 5*time.Second)
	w.Close()

	if got := state.GetCounter("WRITE_BATCH_SIZE"); got != 0 {
		t.Errorf("WRITE_BATCH_SIZE = %d, want 0 (no row landed)", got)
	}
	if got := state.GetCounter("SEND_MYSQL_FAILED"); got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0", got)
	}
	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 0 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 0", got)
	}
	if !w.Connected() {
		t.Error("writer should stay connected after dropping the whole batch")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// TestRecordWriter_RowEvictionRowNumberUnparseable: a 1264 error without a
// parseable "at row N" locator conservatively falls back to the existing
// backoff/markDown path (the batch is held and rewritten after reconnect).
func TestRecordWriter_RowEvictionRowNumberUnparseable(t *testing.T) {
	conf := newTestConf()
	conf.Writer.BatchSize = 1
	conf.Writer.MaxRetries = 0 // single failing attempt on db1
	w, mock, state := newTestWriterWithConf(t, conf)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO test_table").
		WillReturnError(&gomysql.MySQLError{Number: 1264, Message: "Out of range value for column 'c'"})
	mock.ExpectRollback()

	w.Start()
	if !w.Enqueue(makeRow()) {
		t.Fatal("Enqueue failed")
	}

	waitForCounter(t, state, "ROW_DROPPED_DATA", 0, 100*time.Millisecond)
	w.Close()

	if got := state.GetCounter("ROW_DROPPED_DATA"); got != 0 {
		t.Errorf("ROW_DROPPED_DATA = %d, want 0 (unparseable row number: fallback path)", got)
	}
	if got := state.GetCounter("MYSQL_CONN_LOST"); got != 1 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 1 (fallback to markDown)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
