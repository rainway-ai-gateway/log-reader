// Copyright (c) 2026 The BFE Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lr01

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/tests/integration/common"
)

// testEnv holds all resources for a single LR01 integration test.
type testEnv struct {
	t           *testing.T
	processEnv  *common.ProcessEnv
	mockKafka   *common.MockKafka
	logFilePath string
	logGen      *common.LogGenerator
	stopReader  func()
}

func newTestEnv(t *testing.T, fieldMode string, fieldNames []string) *testEnv {
	e := &testEnv{t: t}

	// Start mock Kafka server.
	e.mockKafka = common.NewMockKafka(t, "bfe_ai_log", "bfe_ai_log_dlq")

	// Prepare directories.
	e.processEnv = common.NewProcessEnv(t)
	e.processEnv.Build()

	confDir := filepath.Join(e.processEnv.WorkDir(), "conf")
	logDir := filepath.Join(e.processEnv.WorkDir(), "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("create log dir failed: %v", err)
	}
	e.logFilePath = filepath.Join(logDir, "pb_access3.log")

	// Build log-reader config.
	builder := &common.LogReaderConfigBuilder{
		TemplateDir:   "testdata",
		TargetConfDir: confDir,
		KafkaBroker:   e.mockKafka.Addr(),
		LogFilePath:   e.logFilePath,
	}
	if err := builder.Build(); err != nil {
		t.Fatalf("build log-reader config failed: %v", err)
	}

	// Override kafka_config.data if requested.
	if fieldMode != "" {
		if err := builder.WriteKafkaDataConfig(fieldMode, fieldNames); err != nil {
			t.Fatalf("write kafka data config failed: %v", err)
		}
	}

	// Start log-reader process.
	_, e.stopReader = e.processEnv.StartLogReader(confDir, logDir)

	// Open log generator after process starts so the file can be created.
	e.logGen = common.NewLogGenerator(t, e.logFilePath)

	return e
}

func (e *testEnv) Close() {
	if e.logGen != nil {
		e.logGen.Close()
	}
	if e.stopReader != nil {
		e.stopReader()
	}
	e.mockKafka.Close()
	e.processEnv.Cleanup()
}

func (e *testEnv) dumpLogs() {
	logDir := filepath.Join(e.processEnv.WorkDir(), "log")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		e.t.Logf("read log dir failed: %v", err)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(logDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if len(data) > 0 {
			e.t.Logf("=== %s ===\n%s", entry.Name(), string(data))
		}
	}
}

// assertMessagesReceived waits for expected message count and returns them.
func (e *testEnv) assertMessagesReceived(expected int, timeout time.Duration) [][]byte {
	msgs, ok := e.mockKafka.WaitForMessages(expected, timeout)
	if !ok {
		e.dumpLogs()
		e.t.Fatalf("expected %d messages, got %d", expected, len(msgs))
	}
	return msgs
}

// parseJSON parses a single Kafka message into a map.
func parseJSON(t *testing.T, msg []byte) map[string]interface{} {
	t.Helper()
	var payload map[string]interface{}
	if err := json.Unmarshal(msg, &payload); err != nil {
		t.Fatalf("message is not valid JSON: %v", err)
	}
	return payload
}

// assertFieldEquals checks that payload[field] equals want (for string/number/bool).
func assertFieldEquals(t *testing.T, payload map[string]interface{}, field string, want interface{}) {
	t.Helper()
	got, ok := payload[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	if got != want {
		t.Errorf("field %q = %v (%T), want %v (%T)", field, got, got, want, want)
	}
}

// assertFieldAbsent checks that payload does not contain field.
func assertFieldAbsent(t *testing.T, payload map[string]interface{}, field string) {
	t.Helper()
	if _, ok := payload[field]; ok {
		t.Errorf("field %q should not be present", field)
	}
}

// assertFieldStringNonEmpty checks that payload[field] is a non-empty string.
func assertFieldStringNonEmpty(t *testing.T, payload map[string]interface{}, field string) {
	t.Helper()
	got, ok := payload[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	s, ok := got.(string)
	if !ok {
		t.Errorf("field %q is not a string (%T)", field, got)
		return
	}
	if s == "" {
		t.Errorf("field %q should not be empty", field)
	}
}

// assertFieldStringSliceEquals checks that payload[field] equals want as a string slice.
func assertFieldStringSliceEquals(t *testing.T, payload map[string]interface{}, field string, want []string) {
	t.Helper()
	got, ok := payload[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	gotSlice, ok := got.([]interface{})
	if !ok {
		t.Errorf("field %q is not an array (%T)", field, got)
		return
	}
	if len(gotSlice) != len(want) {
		t.Errorf("field %q length = %d, want %d", field, len(gotSlice), len(want))
		return
	}
	for i, v := range gotSlice {
		s, ok := v.(string)
		if !ok {
			t.Errorf("field %q[%d] is not a string (%T)", field, i, v)
			continue
		}
		if s != want[i] {
			t.Errorf("field %q[%d] = %q, want %q", field, i, s, want[i])
		}
	}
}

// assertFieldObjectEquals checks that payload[field] deeply equals want as an object.
func assertFieldObjectEquals(t *testing.T, payload map[string]interface{}, field string, want map[string]interface{}) {
	t.Helper()
	got, ok := payload[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("field %q = %v, want %v", field, got, want)
	}
}

// assertFieldObjectArrayEquals checks that payload[field] equals want as an object array.
// Each want element is a map of field names to expected values. Only the listed keys are checked.
// Values of type []string are compared as string slices against the nested JSON array.
func assertFieldObjectArrayEquals(t *testing.T, payload map[string]interface{}, field string, want []map[string]interface{}) {
	t.Helper()
	got, ok := payload[field]
	if !ok {
		t.Errorf("missing field %q", field)
		return
	}
	gotSlice, ok := got.([]interface{})
	if !ok {
		t.Errorf("field %q is not an array (%T)", field, got)
		return
	}
	if len(gotSlice) != len(want) {
		t.Errorf("field %q length = %d, want %d", field, len(gotSlice), len(want))
		return
	}
	for i, item := range gotSlice {
		gotMap, ok := item.(map[string]interface{})
		if !ok {
			t.Errorf("field %q[%d] is not an object (%T)", field, i, item)
			continue
		}
		for k, v := range want[i] {
			switch wantVal := v.(type) {
			case []string:
				gotSliceVal, ok := gotMap[k].([]interface{})
				if !ok {
					t.Errorf("field %q[%d].%q is not an array (%T)", field, i, k, gotMap[k])
					continue
				}
				if len(gotSliceVal) != len(wantVal) {
					t.Errorf("field %q[%d].%q length = %d, want %d", field, i, k, len(gotSliceVal), len(wantVal))
					continue
				}
				for j, gotElem := range gotSliceVal {
					gotStr, ok := gotElem.(string)
					if !ok {
						t.Errorf("field %q[%d].%q[%d] is not a string (%T)", field, i, k, j, gotElem)
						continue
					}
					if gotStr != wantVal[j] {
						t.Errorf("field %q[%d].%q[%d] = %q, want %q", field, i, k, j, gotStr, wantVal[j])
					}
				}
			default:
				if gotMap[k] != v {
					t.Errorf("field %q[%d].%q = %v (%T), want %v (%T)", field, i, k, gotMap[k], gotMap[k], v, v)
				}
			}
		}
	}
}

func TestLR01_BasicFlow(t *testing.T) {
	e := newTestEnv(t, "", nil)
	defer e.Close()

	// Write three BFE access logs to the pb log file.
	logs := []*bfe_access_pb.BfeLog{
		common.MakeRequestLog(10001, bfe_access_pb.ProductID_BFE, "api.example.org", "/v1/chat", "gpt-4"),
		common.MakeRequestLog(10002, bfe_access_pb.ProductID_BFE, "api.example.org", "/v1/chat", "gpt-4o"),
		common.MakeRequestLog(10003, bfe_access_pb.ProductID_BFE, "other.example.org", "/v1/embed", "text-embedding-3"),
	}
	for _, log := range logs {
		e.logGen.MustWriteBfeLog(t, log)
	}

	msgs := e.assertMessagesReceived(len(logs), 10*time.Second)

	// Verify each message contains the configured fields with correct values.
	for i, msg := range msgs {
		payload := parseJSON(t, msg)
		req := logs[i].GetRequestLog()

		// --- BfeLog top-level fields (configured) ---
		assertFieldEquals(t, payload, "logid", float64(logs[i].GetLogid()))
		assertFieldEquals(t, payload, "timestamp", float64(logs[i].GetTimestamp()))
		assertFieldEquals(t, payload, "product", req.GetProduct())
		assertFieldStringNonEmpty(t, payload, "hostid")

		// --- Connection / client fields (configured) ---
		assertFieldEquals(t, payload, "client_ip", "10.0.0.1")
		assertFieldEquals(t, payload, "is_trust_src_ip", true)

		// --- Request basic fields (configured) ---
		assertFieldEquals(t, payload, "err_code", "")
		assertFieldEquals(t, payload, "err_msg", "ok")
		assertFieldEquals(t, payload, "req_header_len", float64(189))
		assertFieldEquals(t, payload, "req_body_len", float64(256))
		assertFieldEquals(t, payload, "proto", "HTTP/1.1")
		assertFieldEquals(t, payload, "header_host", req.GetHeaderHost())
		assertFieldEquals(t, payload, "origin_uri", req.GetOriginUri())
		assertFieldEquals(t, payload, "final_uri", req.GetFinalUri())
		assertFieldEquals(t, payload, "method", "POST")
		assertFieldEquals(t, payload, "content_type", "application/json")
		assertFieldEquals(t, payload, "x_forward_for", "10.0.0.2, 10.0.0.3")
		assertFieldEquals(t, payload, "accept_language", "en-US,zh-CN")
		assertFieldEquals(t, payload, "authorization", "Bearer sk-test")
		assertFieldEquals(t, payload, "transfer_encoding", "chunked")

		// --- Routing fields (configured) ---
		assertFieldEquals(t, payload, "cluster", "cluster-A")
		assertFieldEquals(t, payload, "sub_cluster", "subcluster-A1")
		assertFieldEquals(t, payload, "backend_info", "10.0.0.200:8080")
		assertFieldEquals(t, payload, "backend_retry", float64(1))

		// --- Response fields (configured) ---
		assertFieldEquals(t, payload, "res_status_code", float64(200))
		assertFieldEquals(t, payload, "res_header_len", float64(154))
		assertFieldEquals(t, payload, "res_body_len", float64(459))
		assertFieldEquals(t, payload, "res_content_type", "application/json")

		// --- Timing fields (configured) ---
		assertFieldEquals(t, payload, "all_time", float64(11))
		assertFieldEquals(t, payload, "read_client_time", float64(2))
		assertFieldEquals(t, payload, "cluster_serve_time", float64(5))
		assertFieldEquals(t, payload, "backend_serve_time", float64(4))
		assertFieldEquals(t, payload, "write_client_time", float64(1))
		assertFieldEquals(t, payload, "connect_backend_time", float64(1))
		assertFieldEquals(t, payload, "proxy_delay_time", float64(3))

		// --- AI observability fields (configured) ---
		assertFieldEquals(t, payload, "ai_apikey_id", "key-id-123")
		assertFieldObjectEquals(t, payload, "ai_apikeytags", map[string]interface{}{
			"level1": map[string]interface{}{"tagname": "dep", "tagvalue": "ops"},
			"level2": map[string]interface{}{"tagname": "team", "tagvalue": "bfe"},
			"level3": map[string]interface{}{},
			"level4": map[string]interface{}{},
			"level5": map[string]interface{}{},
		})
		assertFieldEquals(t, payload, "ai_requested_model", req.GetAiRequestedModel())
		assertFieldEquals(t, payload, "ai_target_model", req.GetAiTargetModel())
		assertFieldEquals(t, payload, "ai_stream", true)
		assertFieldEquals(t, payload, "ai_input_tokens", float64(1000))
		assertFieldEquals(t, payload, "ai_output_tokens", float64(200))
		assertFieldEquals(t, payload, "ai_total_tokens", float64(1200))
		assertFieldEquals(t, payload, "ai_cache_read_tokens", float64(500))
		assertFieldEquals(t, payload, "ai_cache_write_tokens", float64(100))
		assertFieldEquals(t, payload, "ai_audio_input_tokens", float64(80))
		assertFieldEquals(t, payload, "ai_audio_output_tokens", float64(20))
		assertFieldEquals(t, payload, "ai_image_count", float64(1))
		assertFieldEquals(t, payload, "ai_image_input_tokens", float64(200))
		assertFieldEquals(t, payload, "ai_video_count", float64(3))
		assertFieldEquals(t, payload, "ai_ttft_us", float64(50000))
		assertFieldEquals(t, payload, "ai_tpot_us", float64(2500))
		assertFieldObjectArrayEquals(t, payload, "ai_rate_limit_hits", []map[string]interface{}{
			{
				"rate_limit_policy_id": "rlp-0001",
				"rate_limit_type":      "tpm",
				"rule_names":           []string{"win1m", "win5m"},
			},
		})
		assertFieldEquals(t, payload, "ai_auth_reject_reason", "quota_exceeded")
		assertFieldStringSliceEquals(t, payload, "ai_auth_reject_quota_plans", []string{"plan-A", "plan-B"})
		assertFieldEquals(t, payload, "ai_provider", "openai")
		assertFieldEquals(t, payload, "ai_protocol", "openai")
		assertFieldEquals(t, payload, "ai_mode", "chat")
		assertFieldEquals(t, payload, "ai_retry_count", float64(1))
		assertFieldEquals(t, payload, "ai_cost_value", float64(5000))
		assertFieldEquals(t, payload, "ai_cost_currency", "USD")
		assertFieldObjectArrayEquals(t, payload, "ai_route_rule_hits", []map[string]interface{}{
			{
				"rule_owner":      "ak_user_a",
				"rule_owner_type": "apikey",
				"rule_name":       "user_a-rule1",
			},
		})
		assertFieldObjectArrayEquals(t, payload, "ai_cluster_key_names", []map[string]interface{}{
			{
				"cluster_name": "cluster-a",
				"key_name":     "key-001",
			},
		})
		assertFieldStringSliceEquals(t, payload, "ai_auth_hit_quota_plans", []string{"hit-plan-A", "hit-plan-B"})

		// Non-required, non-configured fields should NOT be emitted.
		assertFieldAbsent(t, payload, "req_num")
		assertFieldAbsent(t, payload, "session_id")
		assertFieldAbsent(t, payload, "client_network")
		assertFieldAbsent(t, payload, "user_agent")
		assertFieldAbsent(t, payload, "cookie")
		assertFieldAbsent(t, payload, "req_headers")
		assertFieldAbsent(t, payload, "res_headers")
		assertFieldAbsent(t, payload, "res_location")
		assertFieldAbsent(t, payload, "res_transfer_encoding")
		assertFieldAbsent(t, payload, "log_tag")
		assertFieldAbsent(t, payload, "bfe_ip")
		assertFieldAbsent(t, payload, "sock_src_ip")
		assertFieldAbsent(t, payload, "vip")
		assertFieldAbsent(t, payload, "vip6")
		assertFieldAbsent(t, payload, "ai_apikey")
		assertFieldAbsent(t, payload, "ai_mapped_model")
		assertFieldAbsent(t, payload, "ai_prompt_tokens")
	}
}

func TestLR01_BatchSplit(t *testing.T) {
	e := newTestEnv(t, "", nil)
	defer e.Close()

	// Write more logs than MaxSizePerBatch to exercise batching.
	const total = 12
	logs := make([]*bfe_access_pb.BfeLog, total)
	for i := 0; i < total; i++ {
		logs[i] = common.MakeRequestLog(uint64(20000+i), bfe_access_pb.ProductID_BFE, "batch.example.org", "/v1/chat", "batch-model")
		e.logGen.MustWriteBfeLog(t, logs[i])
	}

	msgs := e.assertMessagesReceived(total, 10*time.Second)

	// Verify all messages contain expected fields and distinct logids.
	// Note: messages may arrive out of order because multiple batches are
	// processed concurrently.
	seen := make(map[uint64]bool)
	for _, msg := range msgs {
		payload := parseJSON(t, msg)

		logid := uint64(payload["logid"].(float64))
		seen[logid] = true

		assertFieldEquals(t, payload, "header_host", "batch.example.org")
		assertFieldEquals(t, payload, "origin_uri", "/v1/chat")
		assertFieldEquals(t, payload, "ai_requested_model", "batch-model")
	}
	if len(seen) != total {
		t.Fatalf("expected %d distinct logids, got %d", total, len(seen))
	}
	for i := 0; i < total; i++ {
		if !seen[uint64(20000+i)] {
			t.Fatalf("missing logid %d", 20000+i)
		}
	}
}

func TestLR01_CustomizedFieldModeIncludesRequiredFields(t *testing.T) {
	e := newTestEnv(t, "customized", []string{"ai_requested_model"})
	defer e.Close()

	log := common.MakeRequestLog(30001, bfe_access_pb.ProductID_BFE, "required.example.org", "/v1/chat", "required-model")
	e.logGen.MustWriteBfeLog(t, log)

	msgs := e.assertMessagesReceived(1, 10*time.Second)
	payload := parseJSON(t, msgs[0])

	// Customized mode must always include required fields.
	if _, ok := payload["logid"]; !ok {
		t.Errorf("required field logid missing")
	}
	if _, ok := payload["timestamp"]; !ok {
		t.Errorf("required field timestamp missing")
	}
	if _, ok := payload["product"]; !ok {
		t.Errorf("required field product missing")
	}
	if _, ok := payload["hostid"]; !ok {
		t.Errorf("required field hostid missing")
	}

	// And the explicitly customized field.
	assertFieldEquals(t, payload, "ai_requested_model", "required-model")
}

func TestLR01_DefaultFieldMode(t *testing.T) {
	e := newTestEnv(t, "default", nil)
	defer e.Close()

	log := common.MakeRequestLog(40001, bfe_access_pb.ProductID_BFE, "default.example.org", "/v1/chat", "default-model")
	e.logGen.MustWriteBfeLog(t, log)

	msgs := e.assertMessagesReceived(1, 10*time.Second)
	payload := parseJSON(t, msgs[0])

	// Default mode should include many common fields.
	expectedFields := []string{
		"logid", "timestamp", "product", "hostid",
		"client_ip", "err_code", "err_msg", "req_header_len", "req_body_len",
		"proto", "header_host", "origin_uri", "method",
		"res_status_code", "res_header_len", "res_body_len",
		"all_time", "read_client_time", "cluster_serve_time", "backend_serve_time", "write_client_time",
	}
	// Aligned with mod_log_mysql write columns (2026-10-01): these 18 fields
	// were promoted from non-default to the default set.
	alignedFields := []string{
		"log_tag", "client_network", "req_num", "session_id",
		"referrer", "user_agent", "delegation", "uid", "cookie", "req_headers",
		"res_location", "res_transfer_encoding", "res_headers",
		"session_offset_time", "bfe_ip", "sock_src_ip", "vip", "vip6",
	}
	for _, f := range append(expectedFields, alignedFields...) {
		if _, ok := payload[f]; !ok {
			t.Errorf("default mode missing field %q", f)
		}
	}

	// MakeRequestLog sets the intent fields (0.95/1234/true), so in default
	// mode they must appear with real values (not null).
	assertFieldEquals(t, payload, "ai_intent_confidence", 0.95)
	assertFieldEquals(t, payload, "ai_intent_latency_us", float64(1234))
	assertFieldEquals(t, payload, "ai_intent_cache_hit", true)

	// Verify a few values.
	assertFieldEquals(t, payload, "logid", float64(40001))
	assertFieldEquals(t, payload, "header_host", "default.example.org")
	assertFieldEquals(t, payload, "origin_uri", "/v1/chat")
	assertFieldEquals(t, payload, "ai_requested_model", "default-model")
}

func TestLR01_OptionalIntentFieldsUnsetNull(t *testing.T) {
	e := newTestEnv(t, "customized", []string{
		"ai_intent_confidence", "ai_intent_latency_us", "ai_intent_cache_hit",
	})
	defer e.Close()

	// Log 1: intent fields unset (nil pointers) -> JSON null.
	logUnset := common.MakeRequestLog(80001, bfe_access_pb.ProductID_BFE, "intent-unset.example.org", "/v1/chat", "unset-model")
	reqUnset := logUnset.GetRequestLog()
	reqUnset.AiIntentConfidence = nil
	reqUnset.AiIntentLatencyUs = nil
	reqUnset.AiIntentCacheHit = nil

	// Log 2: intent fields set -> original values.
	logSet := common.MakeRequestLog(80002, bfe_access_pb.ProductID_BFE, "intent-set.example.org", "/v1/chat", "set-model")

	e.logGen.MustWriteBfeLog(t, logUnset)
	e.logGen.MustWriteBfeLog(t, logSet)

	msgs := e.assertMessagesReceived(2, 10*time.Second)

	// Message order matches write order.
	payloadUnset := parseJSON(t, msgs[0])
	for _, f := range []string{"ai_intent_confidence", "ai_intent_latency_us", "ai_intent_cache_hit"} {
		got, ok := payloadUnset[f]
		if !ok {
			t.Errorf("unset log missing field %q", f)
			continue
		}
		if got != nil {
			t.Errorf("unset log field %q = %v (%T), want JSON null", f, got, got)
		}
	}

	payloadSet := parseJSON(t, msgs[1])
	assertFieldEquals(t, payloadSet, "ai_intent_confidence", 0.95)
	assertFieldEquals(t, payloadSet, "ai_intent_latency_us", float64(1234))
	assertFieldEquals(t, payloadSet, "ai_intent_cache_hit", true)
}

func TestLR01_AllFieldMode(t *testing.T) {
	e := newTestEnv(t, "all", nil)
	defer e.Close()

	log := common.MakeRequestLog(50001, bfe_access_pb.ProductID_BFE, "all.example.org", "/v1/chat", "all-model")
	e.logGen.MustWriteBfeLog(t, log)

	msgs := e.assertMessagesReceived(1, 10*time.Second)
	payload := parseJSON(t, msgs[0])

	// All mode should include all registered fields, including rarely used ones.
	expectedFields := []string{
		"log_tag", "client_network", "req_num", "session_id",
		"referrer", "user_agent", "delegation", "uid", "cookie", "req_headers",
		"res_location", "res_transfer_encoding", "res_headers",
		"session_offset_time", "bfe_ip", "sock_src_ip", "vip",
	}
	for _, f := range expectedFields {
		if _, ok := payload[f]; !ok {
			t.Errorf("all mode missing field %q", f)
		}
	}

	assertFieldEquals(t, payload, "logid", float64(50001))
	assertFieldEquals(t, payload, "ai_requested_model", "all-model")
}

func TestLR01_RequireFieldMode(t *testing.T) {
	e := newTestEnv(t, "require", nil)
	defer e.Close()

	log := common.MakeRequestLog(60001, bfe_access_pb.ProductID_BFE, "require.example.org", "/v1/chat", "require-model")
	e.logGen.MustWriteBfeLog(t, log)

	msgs := e.assertMessagesReceived(1, 10*time.Second)
	payload := parseJSON(t, msgs[0])

	// Require mode should include all 22 required fields.
	requiredFields := []string{
		"logid", "timestamp", "product", "hostid",
		"client_ip", "err_code", "err_msg",
		"req_header_len", "req_body_len",
		"proto", "header_host", "origin_uri", "method",
		"res_status_code", "res_header_len", "res_body_len",
		"all_time", "read_client_time", "cluster_serve_time", "backend_serve_time", "write_client_time", "proxy_delay_time",
	}
	for _, f := range requiredFields {
		if _, ok := payload[f]; !ok {
			t.Errorf("require mode missing field %q", f)
		}
	}

	// Non-required fields should be absent.
	assertFieldAbsent(t, payload, "ai_requested_model")
	assertFieldAbsent(t, payload, "ai_target_model")
	assertFieldAbsent(t, payload, "req_num")
	assertFieldAbsent(t, payload, "session_id")
	assertFieldAbsent(t, payload, "user_agent")
	assertFieldAbsent(t, payload, "cookie")
	assertFieldAbsent(t, payload, "res_location")
	assertFieldAbsent(t, payload, "log_tag")
	assertFieldAbsent(t, payload, "bfe_ip")
	assertFieldAbsent(t, payload, "vip")
}

func TestLR01_JSONStructureIsStable(t *testing.T) {
	e := newTestEnv(t, "customized", []string{"logid", "timestamp", "product", "ai_requested_model"})
	defer e.Close()

	log := common.MakeRequestLog(70001, bfe_access_pb.ProductID_BFE, "stable.example.org", "/v1/chat", "stable-model")
	e.logGen.MustWriteBfeLog(t, log)

	msgs := e.assertMessagesReceived(1, 10*time.Second)

	// Re-parse the same message twice and ensure identical output.
	payload1 := parseJSON(t, msgs[0])
	payload2 := parseJSON(t, msgs[0])
	if fmt.Sprintf("%v", payload1) != fmt.Sprintf("%v", payload2) {
		t.Error("JSON payload should be stable across re-parses")
	}
}
