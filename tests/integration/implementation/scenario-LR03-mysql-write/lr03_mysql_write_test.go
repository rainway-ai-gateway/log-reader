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

package lr03

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"google.golang.org/protobuf/proto"

	"github.com/rainway-ai-gateway/log-reader/tests/integration/common"
)

const (
	tableName = "bfe_ai_request_log"
	testTS    = uint64(1782353290) // fixed timestamp from common.MakeRequestLog
)

// testEnv holds all resources for a single LR03 integration test.
type testEnv struct {
	t           *testing.T
	mysqlEnv    *common.MysqlEnv
	processEnv  *common.ProcessEnv
	confDir     string
	logDir      string
	logFilePath string
	logGen      *common.LogGenerator
	monitorPort int
	stopReader  func()
}

func newTestEnv(t *testing.T) *testEnv {
	e := &testEnv{t: t}

	// Prepare MySQL (external LR_MYSQL_DSN or testcontainers).
	e.mysqlEnv = common.NewMysqlEnv(t)
	addr, user, passwd, dbName := e.mysqlEnv.Config()

	// Prepare directories.
	e.processEnv = common.NewProcessEnv(t)
	e.processEnv.Build()

	e.confDir = filepath.Join(e.processEnv.WorkDir(), "conf")
	e.logDir = filepath.Join(e.processEnv.WorkDir(), "log")
	if err := os.MkdirAll(e.logDir, 0755); err != nil {
		t.Fatalf("create log dir failed: %v", err)
	}
	e.logFilePath = filepath.Join(e.logDir, "pb_access3.log")

	// Pin the monitor port up front: tests asserting on counters poll
	// /monitor/mod_log_mysql while the process runs.
	e.monitorPort = common.FreePort(t)

	// Build log-reader config.
	builder := &common.LogReaderConfigBuilder{
		TemplateDir:   "testdata",
		TargetConfDir: e.confDir,
		LogFilePath:   e.logFilePath,
		HttpPort:      e.monitorPort,
		MysqlAddr:     addr,
		MysqlUser:     user,
		MysqlPassword: passwd,
		MysqlDBName:   dbName,
	}
	if err := builder.Build(); err != nil {
		t.Fatalf("build log-reader config failed: %v", err)
	}

	// Create the target table from the embedded DDL copy.
	e.mysqlEnv.ApplyDDL(t, filepath.Join("testdata", "ddl", "bfe_ai_request_log.sql"))

	// Start log-reader process.
	e.startReader()

	// Open log generator after process starts so the file can be created.
	e.logGen = common.NewLogGenerator(t, e.logFilePath)

	return e
}

func (e *testEnv) startReader() {
	e.stopReader, _ = e.processEnv.StartLogReaderOnPort(e.confDir, e.logDir, e.monitorPort)
}

func (e *testEnv) Close() {
	if e.logGen != nil {
		e.logGen.Close()
	}
	if e.stopReader != nil {
		e.stopReader()
	}
	if e.mysqlEnv != nil {
		e.mysqlEnv.Close()
	}
	e.processEnv.Cleanup()
}

func (e *testEnv) dumpLogs() {
	entries, err := os.ReadDir(e.logDir)
	if err != nil {
		e.t.Logf("read log dir failed: %v", err)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(e.logDir, entry.Name()))
		if err != nil {
			continue
		}
		if len(data) > 0 {
			e.t.Logf("=== %s ===\n%s", entry.Name(), string(data))
		}
	}
}

// requestLogRow is a typed view of the columns asserted in LR03.
type requestLogRow struct {
	logid                    int64
	product                  string
	logTag                   sql.NullString
	errCode                  sql.NullString
	errMsg                   sql.NullString
	protoCol                 sql.NullString
	headerHost               sql.NullString
	originURI                sql.NullString
	aiRequestedModel         sql.NullString
	aiTargetModel            sql.NullString
	clientIP                 sql.NullString
	isTrustSrcIP             sql.NullInt64
	reqNum                   sql.NullInt64
	aiStream                 sql.NullInt64
	aiInputTokens            sql.NullInt64
	aiOutputTokens           sql.NullInt64
	allTime                  sql.NullInt64
	backendRetry             sql.NullInt64
	resStatusCode            sql.NullInt64
	reqHeaders               sql.NullString
	aiRateLimitHits          sql.NullString
	aiAuthRejectPlans        sql.NullString
	aiAuthHitPlans           sql.NullString
	level1Name               sql.NullString
	level1                   sql.NullString
	level3Name               sql.NullString
	level3                   sql.NullString
	aiCacheStatus            string
	mirrorHit                int64
	mirrorCluster            string
	aiIntentQuestion         string
	aiIntentAnswer           string
	aiIntentConfidence       sql.NullFloat64
	aiIntentSource           string
	aiIntentLatencyUs        sql.NullInt64
	aiIntentCacheHit         sql.NullInt64
	aiIntentQuestionsVersion string
	logTime                  time.Time
}

func (e *testEnv) queryRowByLogid(logid uint64) *requestLogRow {
	e.t.Helper()

	r := &requestLogRow{}
	err := e.mysqlEnv.DB().QueryRow(
		`SELECT logid, product, log_tag, err_code, err_msg, proto, header_host, origin_uri,
			ai_requested_model, ai_target_model, client_ip, is_trust_src_ip, req_num,
			ai_stream, ai_input_tokens, ai_output_tokens, all_time, backend_retry,
			res_status_code, req_headers, ai_rate_limit_hits,
			ai_auth_reject_quota_plans, ai_auth_hit_quota_plans,
			level1Name, level1, level3Name, level3,
			ai_cache_status, mirror_hit, mirror_cluster,
			ai_intent_question, ai_intent_answer, ai_intent_confidence, ai_intent_source,
			ai_intent_latency_us, ai_intent_cache_hit, ai_intent_questions_version,
			log_time
		FROM `+tableName+` WHERE logid = ?`, logid).Scan(
		&r.logid, &r.product, &r.logTag, &r.errCode, &r.errMsg, &r.protoCol, &r.headerHost, &r.originURI,
		&r.aiRequestedModel, &r.aiTargetModel, &r.clientIP, &r.isTrustSrcIP, &r.reqNum,
		&r.aiStream, &r.aiInputTokens, &r.aiOutputTokens, &r.allTime, &r.backendRetry,
		&r.resStatusCode, &r.reqHeaders, &r.aiRateLimitHits,
		&r.aiAuthRejectPlans, &r.aiAuthHitPlans,
		&r.level1Name, &r.level1, &r.level3Name, &r.level3,
		&r.aiCacheStatus, &r.mirrorHit, &r.mirrorCluster,
		&r.aiIntentQuestion, &r.aiIntentAnswer, &r.aiIntentConfidence, &r.aiIntentSource,
		&r.aiIntentLatencyUs, &r.aiIntentCacheHit, &r.aiIntentQuestionsVersion,
		&r.logTime)
	if err != nil {
		e.dumpLogs()
		e.t.Fatalf("query row by logid %d failed: %v", logid, err)
	}
	return r
}

// waitTokens waits until the row with the given logid has ai_output_tokens == want.
func (e *testEnv) waitTokens(logid uint64, want int64, timeout time.Duration) {
	e.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		row := e.queryRowByLogid(logid)
		if row.aiOutputTokens.Valid && row.aiOutputTokens.Int64 == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.dumpLogs()
	e.t.Fatalf("logid %d ai_output_tokens != %d after %v", logid, want, timeout)
}

// waitRows waits until the target table reaches the expected row count,
// dumping log-reader logs on timeout for diagnosis.
func (e *testEnv) waitRows(want int, timeout time.Duration) {
	e.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.mysqlEnv.TableCount(e.t, tableName) >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.dumpLogs()
	e.t.Fatalf("table %s row count = %d, want >= %d (timeout %v)",
		tableName, e.mysqlEnv.TableCount(e.t, tableName), want, timeout)
}

// assertJSONColumn checks that a JSON column value deeply equals want after unmarshalling.
func assertJSONColumn(t *testing.T, col sql.NullString, name string, want interface{}) {
	t.Helper()

	if !col.Valid {
		t.Errorf("column %s should not be NULL", name)
		return
	}
	var got interface{}
	if err := json.Unmarshal([]byte(col.String), &got); err != nil {
		t.Errorf("column %s is not valid JSON: %v (%q)", name, err, col.String)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("column %s = %v, want %v", name, got, want)
	}
}

// assertNull asserts that a nullable column is NULL.
func assertNull(t *testing.T, valid bool, name string) {
	t.Helper()

	if valid {
		t.Errorf("column %s should be NULL", name)
	}
}

// assertNullString asserts a nullable string column equals want.
func assertNullString(t *testing.T, col sql.NullString, name string, want string) {
	t.Helper()

	if !col.Valid {
		t.Errorf("column %s should not be NULL (want %q)", name, want)
		return
	}
	if col.String != want {
		t.Errorf("column %s = %q, want %q", name, col.String, want)
	}
}

// assertNullInt asserts a nullable integer column equals want.
func assertNullInt(t *testing.T, col sql.NullInt64, name string, want int64) {
	t.Helper()

	if !col.Valid {
		t.Errorf("column %s should not be NULL (want %d)", name, want)
		return
	}
	if col.Int64 != want {
		t.Errorf("column %s = %d, want %d", name, col.Int64, want)
	}
}

// assertNullFloat asserts a nullable float column equals want.
func assertNullFloat(t *testing.T, col sql.NullFloat64, name string, want float64) {
	t.Helper()

	if !col.Valid {
		t.Errorf("column %s should not be NULL (want %v)", name, want)
		return
	}
	if col.Float64 != want {
		t.Errorf("column %s = %v, want %v", name, col.Float64, want)
	}
}

func TestLR03_BasicWrite(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	// Three logs with distinct models so all three land as separate rows
	// (unique key: hostid + log_time + ai_apikey_id + ai_requested_model).
	logs := []*bfe_access_pb.BfeLog{
		common.MakeRequestLog(10001, bfe_access_pb.ProductID_BFE, "api.example.org", "/v1/chat", "gpt-4"),
		common.MakeRequestLog(10002, bfe_access_pb.ProductID_BFE, "api.example.org", "/v1/chat", "gpt-4o"),
		common.MakeRequestLog(10003, bfe_access_pb.ProductID_BFE, "other.example.org", "/v1/embed", "text-embedding-3"),
	}
	for _, log := range logs {
		e.logGen.MustWriteBfeLog(t, log)
	}

	e.waitRows(len(logs), 20*time.Second)
	if got := e.mysqlEnv.TableCount(t, tableName); got != len(logs) {
		t.Fatalf("table %s row count = %d, want %d", tableName, got, len(logs))
	}

	for _, log := range logs {
		req := log.GetRequestLog()
		r := e.queryRowByLogid(log.GetLogid())

		// Basic columns.
		if r.logid != int64(log.GetLogid()) {
			t.Errorf("logid = %d, want %d", r.logid, log.GetLogid())
		}
		if r.product != req.GetProduct() {
			t.Errorf("product = %q, want %q", r.product, req.GetProduct())
		}
		assertNullString(t, r.logTag, "log_tag", "integration-test")

		// Empty string maps to NULL (err_code is "" in the fixture).
		assertNull(t, r.errCode.Valid, "err_code")
		assertNullString(t, r.errMsg, "err_msg", "ok")

		// Request / routing / response scalars.
		assertNullString(t, r.headerHost, "header_host", req.GetHeaderHost())
		assertNullString(t, r.aiRequestedModel, "ai_requested_model", req.GetAiRequestedModel())
		assertNullString(t, r.aiTargetModel, "ai_target_model", req.GetAiTargetModel())
		assertNullString(t, r.clientIP, "client_ip", "10.0.0.1")
		assertNullInt(t, r.isTrustSrcIP, "is_trust_src_ip", 1)
		assertNullInt(t, r.reqNum, "req_num", 1)
		assertNullInt(t, r.aiStream, "ai_stream", 1)
		assertNullInt(t, r.aiInputTokens, "ai_input_tokens", 1000)
		assertNullInt(t, r.aiOutputTokens, "ai_output_tokens", 200)
		assertNullInt(t, r.allTime, "all_time", 11)
		assertNullInt(t, r.backendRetry, "backend_retry", 1)
		assertNullInt(t, r.resStatusCode, "res_status_code", 200)

		// JSON columns.
		assertJSONColumn(t, r.reqHeaders, "req_headers", []interface{}{
			map[string]interface{}{"key": "X-Request-Id", "value": "req-123"},
			map[string]interface{}{"key": "X-Test-Header", "value": "test-value"},
		})
		assertJSONColumn(t, r.aiRateLimitHits, "ai_rate_limit_hits", []interface{}{
			map[string]interface{}{
				"rate_limit_policy_id": "rlp-0001",
				"rate_limit_type":      "tpm",
				"rule_names":           []interface{}{"win1m", "win5m"},
			},
		})
		assertJSONColumn(t, r.aiAuthRejectPlans, "ai_auth_reject_quota_plans",
			[]interface{}{"plan-A", "plan-B"})
		assertJSONColumn(t, r.aiAuthHitPlans, "ai_auth_hit_quota_plans",
			[]interface{}{"hit-plan-A", "hit-plan-B"})

		// Flattened API key tags: level1 set, level3 absent -> NULL.
		assertNullString(t, r.level1Name, "level1Name", "dep")
		assertNullString(t, r.level1, "level1", "ops")
		assertNull(t, r.level3Name.Valid, "level3Name")
		assertNull(t, r.level3.Valid, "level3")

		// AI cache / mirroring / intent columns (filled by MakeRequestLog).
		if r.aiCacheStatus != "hit" {
			t.Errorf("ai_cache_status = %q, want hit", r.aiCacheStatus)
		}
		if r.mirrorHit != 1 {
			t.Errorf("mirror_hit = %d, want 1", r.mirrorHit)
		}
		if r.mirrorCluster != "mirror-cluster-A" {
			t.Errorf("mirror_cluster = %q, want mirror-cluster-A", r.mirrorCluster)
		}
		if r.aiIntentQuestion != "intent-q-001" {
			t.Errorf("ai_intent_question = %q, want intent-q-001", r.aiIntentQuestion)
		}
		if r.aiIntentAnswer != "intent-a-1" {
			t.Errorf("ai_intent_answer = %q, want intent-a-1", r.aiIntentAnswer)
		}
		assertNullFloat(t, r.aiIntentConfidence, "ai_intent_confidence", 0.95)
		if r.aiIntentSource != "llm" {
			t.Errorf("ai_intent_source = %q, want llm", r.aiIntentSource)
		}
		assertNullInt(t, r.aiIntentLatencyUs, "ai_intent_latency_us", 1234)
		assertNullInt(t, r.aiIntentCacheHit, "ai_intent_cache_hit", 1)
		if r.aiIntentQuestionsVersion != "v2026-09-27" {
			t.Errorf("ai_intent_questions_version = %q, want v2026-09-27", r.aiIntentQuestionsVersion)
		}

		// log_time is the fixed timestamp (compared in UTC).
		if got, want := r.logTime.UTC().Unix(), int64(testTS); got != want {
			t.Errorf("log_time = %d (%v), want unix %d", got, r.logTime, want)
		}
	}
}

func TestLR03_IdempotentReplay(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	log := common.MakeRequestLog(80001, bfe_access_pb.ProductID_BFE, "replay.example.org", "/v1/chat", "replay-model")
	e.logGen.MustWriteBfeLog(t, log)
	e.waitRows(1, 20*time.Second)
	e.waitTokens(80001, 200, 20*time.Second)

	// Re-delivery of the same log (same unique key) with a changed value:
	// row count must stay 1 and the value must be overwritten.
	replay := proto.Clone(log).(*bfe_access_pb.BfeLog)
	replay.RequestLog.AiOutputTokens = proto.Int64(999)
	e.logGen.MustWriteBfeLog(t, replay)
	e.waitTokens(80001, 999, 20*time.Second)

	if got := e.mysqlEnv.TableCount(t, tableName); got != 1 {
		t.Fatalf("row count after re-delivery = %d, want 1", got)
	}

	// Simulate log-reader restart with -b (read from begin): the whole log
	// file is replayed, both entries collapse into the single row again.
	e.stopReader()
	e.startReader()
	time.Sleep(3 * time.Second)

	if got := e.mysqlEnv.TableCount(t, tableName); got != 1 {
		e.dumpLogs()
		t.Fatalf("row count after -b replay = %d, want 1", got)
	}
	e.waitTokens(80001, 999, 20*time.Second)
}

func TestLR03_ZeroValueToNull(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	// Minimal request log: fields set to proto-required but semantically zero values.
	logType := bfe_access_pb.BfeLogType_Request
	product := bfe_access_pb.ProductID_BFE
	zeroIP := uint32(0)
	notTrust := false
	log := &bfe_access_pb.BfeLog{
		Logid:     proto.Uint64(81001),
		Timestamp: proto.Uint64(testTS),
		Product:   &product,
		LogType:   &logType,
		RequestLog: &bfe_access_pb.RequestLog{
			ErrCode:          proto.String(""),
			ErrMsg:           proto.String(""),
			AddrInfo:         &bfe_access_pb.ConnAddrInfo{BfeIp: &zeroIP, SockSrcIp: &zeroIP, IsTrustSrcIp: &notTrust},
			ClientIp:         proto.Uint32(0),
			ReqNum:           proto.Uint32(0),
			Proto:            proto.String(""),
			HeaderHost:       proto.String(""),
			OriginUri:        proto.String(""),
			Method:           proto.String(""),
			AllTime:          proto.Uint32(0),
			AiRequestedModel: proto.String("zero-model"),
		},
	}
	e.logGen.MustWriteBfeLog(t, log)
	e.waitRows(1, 20*time.Second)

	r := e.queryRowByLogid(81001)

	// String zero values -> NULL.
	assertNull(t, r.errCode.Valid, "err_code")
	assertNull(t, r.errMsg.Valid, "err_msg")
	assertNull(t, r.protoCol.Valid, "proto")
	assertNull(t, r.headerHost.Valid, "header_host")
	assertNull(t, r.originURI.Valid, "origin_uri")
	assertNull(t, r.level1.Valid, "level1")

	// Empty structured fields -> NULL.
	assertNull(t, r.reqHeaders.Valid, "req_headers")
	assertNull(t, r.aiAuthHitPlans.Valid, "ai_auth_hit_quota_plans")

	// Boolean / numeric zero values are written as-is (0, not NULL).
	assertNullInt(t, r.isTrustSrcIP, "is_trust_src_ip", 0)
	assertNullInt(t, r.aiStream, "ai_stream", 0)
	assertNullInt(t, r.aiInputTokens, "ai_input_tokens", 0)

	// Set fields still land.
	assertNullString(t, r.aiRequestedModel, "ai_requested_model", "zero-model")
}

func TestLR03_BatchSplit(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	// 25 logs > MaxSizePerBatch(10) > writer BatchSize(5): exercises reader
	// batch splitting and writer multi-batch flush.
	const total = 25
	for i := 0; i < total; i++ {
		log := common.MakeRequestLog(uint64(90001+i), bfe_access_pb.ProductID_BFE,
			"batch.example.org", "/v1/chat", fmt.Sprintf("batch-model-%02d", i))
		e.logGen.MustWriteBfeLog(t, log)
	}

	e.waitRows(total, 30*time.Second)
	if got := e.mysqlEnv.TableCount(t, tableName); got != total {
		t.Fatalf("table %s row count = %d, want %d", tableName, got, total)
	}

	rows, err := e.mysqlEnv.DB().Query(
		`SELECT logid, ai_requested_model FROM ` + tableName + ` WHERE logid BETWEEN 90001 AND 90025`)
	if err != nil {
		t.Fatalf("query batch rows failed: %v", err)
	}
	defer rows.Close()

	seen := make(map[int64]string)
	for rows.Next() {
		var logid int64
		var model string
		if err := rows.Scan(&logid, &model); err != nil {
			t.Fatalf("scan batch row failed: %v", err)
		}
		seen[logid] = model
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate batch rows failed: %v", err)
	}
	if len(seen) != total {
		t.Fatalf("distinct logids = %d, want %d", len(seen), total)
	}
	for i := 0; i < total; i++ {
		logid := int64(90001 + i)
		wantModel := fmt.Sprintf("batch-model-%02d", i)
		if seen[logid] != wantModel {
			t.Errorf("logid %d model = %q, want %q", logid, seen[logid], wantModel)
		}
	}
}

// TestLR03_NewFieldsWriteAndZeroRules (TC-05) verifies the 10 ai-cache /
// mirroring / intent columns end to end: exact values when the fields are
// filled, and the mapper-defined default form ("" / 0, never NULL) when unset.
func TestLR03_NewFieldsWriteAndZeroRules(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	// Part 1: all 10 new fields filled (MakeRequestLog sets them).
	full := common.MakeRequestLog(82001, bfe_access_pb.ProductID_BFE,
		"intent.example.org", "/v1/chat", "intent-model")
	e.logGen.MustWriteBfeLog(t, full)
	e.waitRows(1, 20*time.Second)

	r := e.queryRowByLogid(82001)
	if r.aiCacheStatus != "hit" {
		t.Errorf("ai_cache_status = %q, want hit", r.aiCacheStatus)
	}
	if r.mirrorHit != 1 {
		t.Errorf("mirror_hit = %d, want 1", r.mirrorHit)
	}
	if r.mirrorCluster != "mirror-cluster-A" {
		t.Errorf("mirror_cluster = %q, want mirror-cluster-A", r.mirrorCluster)
	}
	if r.aiIntentQuestion != "intent-q-001" {
		t.Errorf("ai_intent_question = %q, want intent-q-001", r.aiIntentQuestion)
	}
	if r.aiIntentAnswer != "intent-a-1" {
		t.Errorf("ai_intent_answer = %q, want intent-a-1", r.aiIntentAnswer)
	}
	assertNullFloat(t, r.aiIntentConfidence, "ai_intent_confidence", 0.95)
	if r.aiIntentSource != "llm" {
		t.Errorf("ai_intent_source = %q, want llm", r.aiIntentSource)
	}
	assertNullInt(t, r.aiIntentLatencyUs, "ai_intent_latency_us", 1234)
	assertNullInt(t, r.aiIntentCacheHit, "ai_intent_cache_hit", 1)
	if r.aiIntentQuestionsVersion != "v2026-09-27" {
		t.Errorf("ai_intent_questions_version = %q, want v2026-09-27", r.aiIntentQuestionsVersion)
	}

	// Part 2: minimal log without cache/mirror/intent fields -> mapper defaults.
	zeroType := bfe_access_pb.BfeLogType_Request
	zeroProduct := bfe_access_pb.ProductID_BFE
	zeroIP := uint32(0)
	notTrust := false
	zero := &bfe_access_pb.BfeLog{
		Logid:     proto.Uint64(82002),
		Timestamp: proto.Uint64(testTS),
		Product:   &zeroProduct,
		LogType:   &zeroType,
		RequestLog: &bfe_access_pb.RequestLog{
			ErrCode:          proto.String(""),
			ErrMsg:           proto.String(""),
			AddrInfo:         &bfe_access_pb.ConnAddrInfo{BfeIp: &zeroIP, SockSrcIp: &zeroIP, IsTrustSrcIp: &notTrust},
			ClientIp:         proto.Uint32(0),
			ReqNum:           proto.Uint32(0),
			Proto:            proto.String("HTTP/1.1"),
			HeaderHost:       proto.String("zero.example.org"),
			OriginUri:        proto.String("/v1/chat"),
			Method:           proto.String("POST"),
			AllTime:          proto.Uint32(0),
			AiRequestedModel: proto.String("zero-intent-model"),
		},
	}
	e.logGen.MustWriteBfeLog(t, zero)
	e.waitRows(2, 20*time.Second)

	zr := e.queryRowByLogid(82002)

	// NOT NULL string columns: empty string, never NULL.
	if zr.aiCacheStatus != "" {
		t.Errorf("ai_cache_status = %q, want \"\"", zr.aiCacheStatus)
	}
	if zr.mirrorCluster != "" {
		t.Errorf("mirror_cluster = %q, want \"\"", zr.mirrorCluster)
	}
	if zr.aiIntentQuestion != "" {
		t.Errorf("ai_intent_question = %q, want \"\"", zr.aiIntentQuestion)
	}
	if zr.aiIntentAnswer != "" {
		t.Errorf("ai_intent_answer = %q, want \"\"", zr.aiIntentAnswer)
	}
	if zr.aiIntentSource != "" {
		t.Errorf("ai_intent_source = %q, want \"\"", zr.aiIntentSource)
	}
	if zr.aiIntentQuestionsVersion != "" {
		t.Errorf("ai_intent_questions_version = %q, want \"\"", zr.aiIntentQuestionsVersion)
	}

	// bool TINYINT and numeric columns: mirror_hit (NOT NULL) is 0; the three
	// nullable optional columns are NULL when unset (proto pointer nil = 未求值).
	if zr.mirrorHit != 0 {
		t.Errorf("mirror_hit = %d, want 0", zr.mirrorHit)
	}
	assertNull(t, zr.aiIntentLatencyUs.Valid, "ai_intent_latency_us")
	assertNull(t, zr.aiIntentCacheHit.Valid, "ai_intent_cache_hit")
	assertNull(t, zr.aiIntentConfidence.Valid, "ai_intent_confidence")

	// Part 3: optional columns explicitly set to zero values -> written as 0
	// (not NULL), so the consumer can distinguish "evaluated to zero" from
	// "not evaluated" (NULL).
	explicit := common.MakeRequestLog(82003, bfe_access_pb.ProductID_BFE,
		"intent.example.org", "/v1/chat", "intent-explicit-zero-model")
	explicit.RequestLog.AiIntentConfidence = proto.Float64(0)
	explicit.RequestLog.AiIntentLatencyUs = proto.Int64(0)
	explicit.RequestLog.AiIntentCacheHit = proto.Bool(false)
	e.logGen.MustWriteBfeLog(t, explicit)
	e.waitRows(3, 20*time.Second)

	er := e.queryRowByLogid(82003)
	assertNullFloat(t, er.aiIntentConfidence, "ai_intent_confidence", 0)
	assertNullInt(t, er.aiIntentLatencyUs, "ai_intent_latency_us", 0)
	assertNullInt(t, er.aiIntentCacheHit, "ai_intent_cache_hit", 0)
}

// TestLR03_ConcurrentRowsSameSecondSurvive (bfenetworks/bfe#1417) verifies the
// report table keeps every concurrent row of one API key and model inside one
// second. The legacy dedup key (hostid, log_time, ai_apikey_id,
// ai_requested_model) had a second-granularity log_time, while the writer
// overwrites the whole row on conflict — so such concurrent rows collapsed into
// one (n requests -> 1 row, loss (n-1)/n). MakeRequestLog fixes
// hostid / log_time (1782353290) / ai_apikey_id ("key-id-123") for every call,
// so passing one model with two logids reproduces exactly that precondition.
func TestLR03_ConcurrentRowsSameSecondSurvive(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	const logidA, logidB = uint64(91001), uint64(91002)
	logA := common.MakeRequestLog(logidA, bfe_access_pb.ProductID_BFE,
		"dedup.example.org", "/v1/chat", "same-second-model")
	logB := common.MakeRequestLog(logidB, bfe_access_pb.ProductID_BFE,
		"dedup.example.org", "/v1/chat", "same-second-model")
	e.logGen.MustWriteBfeLog(t, logA)
	e.logGen.MustWriteBfeLog(t, logB)

	e.waitRows(2, 30*time.Second)
	if got := e.mysqlEnv.TableCount(t, tableName); got != 2 {
		t.Fatalf("row count = %d, want 2: rows sharing (hostid, log_time, ai_apikey_id, ai_requested_model) must all survive", got)
	}

	// Both rows must be individually present, keyed by their own logid.
	rows, err := e.mysqlEnv.DB().Query(
		`SELECT logid, ai_input_tokens FROM ` + tableName + ` ORDER BY logid`)
	if err != nil {
		t.Fatalf("query rows failed: %v", err)
	}
	defer rows.Close()

	got := make(map[int64]int64)
	for rows.Next() {
		var logid, inTokens int64
		if err := rows.Scan(&logid, &inTokens); err != nil {
			t.Fatalf("scan row failed: %v", err)
		}
		got[logid] = inTokens
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rows failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("distinct logids = %d, want 2 (a concurrent row was collapsed into another)", len(got))
	}
	for _, want := range []int64{int64(logidA), int64(logidB)} {
		if _, ok := got[want]; !ok {
			t.Errorf("logid %d missing: its row was overwritten by the other concurrent row", want)
		}
	}

	// Re-delivering one and the same request (same logid) must hit the overwrite,
	// not append a duplicate row.
	replay := proto.Clone(logA).(*bfe_access_pb.BfeLog)
	replay.RequestLog.AiOutputTokens = proto.Int64(777)
	e.logGen.MustWriteBfeLog(t, replay)
	e.waitTokens(logidA, 777, 20*time.Second)

	if n := e.mysqlEnv.TableCount(t, tableName); n != 2 {
		t.Fatalf("row count after replay = %d, want 2 (same logid must be overwritten, not appended)", n)
	}
}

// TestLR03_OutOfRangeValueSurvives (issue #12, SC2201-TC007 A-09/A-10) verifies
// end to end that a uint32 value above the DDL INT range no longer poisons its
// whole write batch: every row of the batch must land (the out-of-range ones
// normalized to the column max), no connection is marked down and no batch is
// counted as failed.
//
// Rows: 4 proxy_delay_time probes (0, INT max, INT max+1, uint32 max) plus a
// normal row and a 401 auth-reject row, written as one batch.
func TestLR03_OutOfRangeValueSurvives(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	type probe struct {
		logid  uint64
		model  string
		delay  uint32
		wantDB int64
	}
	probes := []probe{
		{logid: 93001, model: "probe-delay-zero", delay: 0, wantDB: 0},
		{logid: 93002, model: "probe-delay-int-max", delay: 2147483647, wantDB: 2147483647},
		{logid: 93003, model: "probe-delay-int-max-plus-1", delay: 2147483648, wantDB: 2147483647},
		{logid: 93004, model: "probe-delay-uint32-max", delay: 4294967295, wantDB: 2147483647},
	}

	for _, p := range probes {
		log := common.MakeRequestLog(p.logid, bfe_access_pb.ProductID_BFE,
			"oor.example.org", "/v1/chat", p.model)
		log.RequestLog.ProxyDelayTime = proto.Uint32(p.delay)
		e.logGen.MustWriteBfeLog(t, log)
	}

	// Normal row and 401 auth-reject row mixed into the same write batch.
	normal := common.MakeRequestLog(93005, bfe_access_pb.ProductID_BFE,
		"oor.example.org", "/v1/chat", "probe-normal")
	normal.RequestLog.ProxyDelayTime = proto.Uint32(3)
	e.logGen.MustWriteBfeLog(t, normal)

	unauthorized := common.MakeRequestLog(93006, bfe_access_pb.ProductID_BFE,
		"oor.example.org", "/v1/chat", "probe-401")
	unauthorized.RequestLog.ResStatusCode = proto.Uint32(401)
	e.logGen.MustWriteBfeLog(t, unauthorized)

	const total = 6
	e.waitRows(total, 30*time.Second)
	if got := e.mysqlEnv.TableCount(t, tableName); got != total {
		e.dumpLogs()
		t.Fatalf("table %s row count = %d, want %d (an out-of-range value must not drop the batch)",
			tableName, got, total)
	}

	// In-range values land as-is; out-of-range values are read back as the
	// column max (2147483647), never as MySQL 1264 / dropped rows.
	for _, p := range probes {
		var delay sql.NullInt64
		if err := e.mysqlEnv.DB().QueryRow(
			`SELECT proxy_delay_time FROM `+tableName+` WHERE logid = ?`, p.logid).Scan(&delay); err != nil {
			e.dumpLogs()
			t.Fatalf("query proxy_delay_time for logid %d failed: %v", p.logid, err)
		}
		if !delay.Valid {
			t.Errorf("logid %d proxy_delay_time is NULL, want %d", p.logid, p.wantDB)
			continue
		}
		if delay.Int64 != p.wantDB {
			t.Errorf("logid %d proxy_delay_time = %d, want %d (source value %d)",
				p.logid, delay.Int64, p.wantDB, p.delay)
		}
	}

	// The normal row and the 401 auth-reject row survive alongside the probes.
	var normalDelay sql.NullInt64
	if err := e.mysqlEnv.DB().QueryRow(
		`SELECT proxy_delay_time FROM `+tableName+` WHERE logid = ?`, uint64(93005)).Scan(&normalDelay); err != nil {
		e.dumpLogs()
		t.Fatalf("query normal row failed: %v", err)
	}
	if !normalDelay.Valid || normalDelay.Int64 != 3 {
		t.Errorf("normal row proxy_delay_time = %v, want 3", normalDelay)
	}

	var status sql.NullInt64
	if err := e.mysqlEnv.DB().QueryRow(
		`SELECT res_status_code FROM `+tableName+` WHERE logid = ?`, uint64(93006)).Scan(&status); err != nil {
		e.dumpLogs()
		t.Fatalf("query 401 row failed: %v", err)
	}
	if !status.Valid || status.Int64 != 401 {
		t.Errorf("401 row res_status_code = %v, want 401", status)
	}

	// Counters: two field values normalized, no structural failure, no markDown.
	state := common.WaitMonitor(t, e.monitorPort, "mod_log_mysql", 10*time.Second,
		func(m *common.MonitorState) bool {
			return m.SCounters["VALUE_NORMALIZED"] >= 2
		})
	if got := state.SCounters["VALUE_NORMALIZED"]; got != 2 {
		t.Errorf("VALUE_NORMALIZED = %d, want 2 (two out-of-range probe values)", got)
	}
	if got := state.SCounters["SEND_MYSQL_FAILED"]; got != 0 {
		t.Errorf("SEND_MYSQL_FAILED = %d, want 0 (data errors are not structural failures)", got)
	}
	if got := state.SCounters["MYSQL_CONN_LOST"]; got != 0 {
		t.Errorf("MYSQL_CONN_LOST = %d, want 0 (no markDown on data errors)", got)
	}
	if got := state.SCounters["ROW_DROPPED_DATA"]; got != 0 {
		t.Errorf("ROW_DROPPED_DATA = %d, want 0 (normalization prevents row eviction)", got)
	}
}
