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
	"testing"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

// ddlColumns 是 ai-gateway-api 仓库 db_ddl_report_mysql.sql 明细表（99 列，变更
// 文档 2026-09-27 评审基准）的列清单硬编码，用于校验 field_mapper 列数/列序与 DDL
// 一致（快照式断言，防两侧漂移）。
var ddlColumns = []string{
	// 主键/基础列
	"hostid", "log_time", "ai_apikey_id", "ai_requested_model", "logid", "product", "log_tag",
	// 客户端连接列
	"client_ip", "client_network", "is_trust_src_ip", "req_num", "session_id",
	"bfe_ip", "sock_src_ip", "vip", "vip6",
	// 错误列
	"err_code", "err_msg",
	// 请求列
	"proto", "header_host", "origin_uri", "final_uri", "method", "content_type",
	"x_forward_for", "accept_language", "authorization", "transfer_encoding",
	"referrer", "user_agent", "delegation", "uid", "cookie",
	"req_headers", "req_header_len", "req_body_len",
	// 路由列
	"cluster", "sub_cluster", "backend_info", "backend_retry",
	// 响应列
	"res_status_code", "res_header_len", "res_body_len", "res_content_type",
	"res_location", "res_transfer_encoding", "res_headers",
	// 耗时列（毫秒）
	"all_time", "read_client_time", "cluster_serve_time", "backend_serve_time",
	"write_client_time", "connect_backend_time", "proxy_delay_time", "session_offset_time",
	// API Key 标签打平列
	"level1Name", "level1", "level2Name", "level2", "level3Name", "level3",
	"level4Name", "level4", "level5Name", "level5",
	// AI 可观测列（标量）
	"ai_target_model", "ai_stream", "ai_input_tokens", "ai_output_tokens", "ai_total_tokens",
	"ai_cache_read_tokens", "ai_cache_write_tokens", "ai_audio_input_tokens", "ai_audio_output_tokens",
	"ai_image_count", "ai_ttft_us", "ai_tpot_us", "ai_provider", "ai_protocol", "ai_mode",
	"ai_retry_count", "ai_cost_value", "ai_cost_currency",
	// AI 可观测列（JSON）
	"ai_route_rule_hits", "ai_cluster_key_names", "ai_rate_limit_hits",
	"ai_auth_reject_reason", "ai_auth_reject_quota_plans", "ai_auth_hit_quota_plans",
	// AI 缓存/镜像/意图列（2026-09-27 加列，与 ai-gateway-api DDL 明细表列序一致）
	"ai_cache_status", "mirror_hit", "mirror_cluster",
	"ai_intent_question", "ai_intent_answer", "ai_intent_confidence", "ai_intent_source",
	"ai_intent_latency_us", "ai_intent_cache_hit", "ai_intent_questions_version",
}

// makeBfeLog builds a BfeLog with full request fields for mapper tests.
func makeBfeLog() *bfe_access_pb.BfeLog {
	logid := uint64(12345)
	timestamp := uint64(1782353290)
	product := bfe_access_pb.ProductID_BFE
	logTag := "req_BFE"

	return &bfe_access_pb.BfeLog{
		Product:   &product,
		Timestamp: &timestamp,
		Logid:     &logid,
		LogTag:    &logTag,
		LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
		RequestLog: &bfe_access_pb.RequestLog{
			ErrCode:      strPtr(""),
			ErrMsg:       strPtr(""),
			ReqHeaderLen: uint32Ptr(189),
			ReqBodyLen:   uint32Ptr(0),
			ClientIp:     uint32Ptr(0x0A000001), // 10.0.0.1
			ReqNum:       uint32Ptr(1),
			Proto:        strPtr("HTTP/1.1"),
			HeaderHost:   strPtr("example.com"),
			OriginUri:    strPtr("/api/v1/test"),
			Method:       strPtr("POST"),
			AddrInfo: &bfe_access_pb.ConnAddrInfo{
				BfeIp:        uint32Ptr(0x0A000064), // 10.0.0.100
				SockSrcIp:    uint32Ptr(0xC0A80164), // 192.168.1.100
				IsTrustSrcIp: boolPtr(true),
				Vip:          uint32Ptr(0x0A0000C8), // 10.0.0.200
			},
			ReqHeaders: []*bfe_access_pb.HttpHeader{
				{Key: strPtr("X-Test"), Value: strPtr("abc")},
			},
			Cluster:            strPtr("cluster_ai"),
			SubCluster:         strPtr("pool_bj"),
			BackendInfo:        &bfe_access_pb.InstanceInfo{IpAddr: uint32Ptr(0x0A000002), Port: uint32Ptr(8080)},
			BackendRetry:       uint32Ptr(0),
			ResStatusCode:      uint32Ptr(200),
			ResHeaderLen:       uint32Ptr(154),
			ResBodyLen:         uint32Ptr(459),
			ResContentType:     strPtr("application/json"),
			AllTime:            uint32Ptr(11),
			ReadClientTime:     uint32Ptr(2),
			ClusterServeTime:   uint32Ptr(5),
			BackendServeTime:   uint32Ptr(4),
			WriteClientTime:    uint32Ptr(1),
			SessionOffsetTime:  uint32Ptr(9),
			ConnectBackendTime: uint32Ptr(1),
			ProxyDelayTime:     uint32Ptr(3),
			AiApikeyId:         strPtr("key-id-123"),
			AiApikeytags: []*bfe_access_pb.ApikeyTag{
				{Tagname: strPtr("dep"), Tagvalue: strPtr("ops"), Taglevel: int32Ptr(1)},
				{Tagname: strPtr("team"), Tagvalue: strPtr("bfe"), Taglevel: int32Ptr(2)},
			},
			AiRequestedModel: strPtr("test-model"),
			AiTargetModel:    strPtr("gpt-5"),
			AiStream:         boolPtr(true),
			AiInputTokens:    int64Ptr(34),
			AiOutputTokens:   int64Ptr(182),
			AiTotalTokens:    int64Ptr(216),
			AiTtftUs:         int64Ptr(5486),
			AiTpotUs:         int64Ptr(3),
			AiProvider:       strPtr("openai"),
			AiProtocol:       strPtr("openai"),
			AiMode:           strPtr("chat"),
			AiRetryCount:     uint32Ptr(0),
			AiCostValue:      int64Ptr(5000),
			AiCostCurrency:   strPtr("USD"),
			AiRouteRuleHits: []*bfe_access_pb.AIRouteRuleHit{
				{
					RuleOwner:     strPtr("ak_user_a"),
					RuleOwnerType: strPtr("apikey"),
					RuleName:      strPtr("user_a-rule1"),
				},
			},
			AiAuthHitQuotaPlans: []string{"hit-plan-a"},
			// AI cache / traffic mirroring / AI intent fields (v0.3.7/3.8/3.9).
			AiCacheStatus:            strPtr("hit"),
			MirrorHit:                boolPtr(true),
			MirrorCluster:            strPtr("mirror-cluster-a"),
			AiIntentQuestion:         strPtr("intent-q-001"),
			AiIntentAnswer:           strPtr("intent-a-1"),
			AiIntentConfidence:       float64Ptr(0.95),
			AiIntentSource:           strPtr("llm"),
			AiIntentLatencyUs:        int64Ptr(1234),
			AiIntentCacheHit:         boolPtr(true),
			AiIntentQuestionsVersion: strPtr("v2026-09-27"),
		},
	}
}

func strPtr(s string) *string    { return &s }
func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }
func int64Ptr(v int64) *int64    { return &v }
func int32Ptr(v int32) *int32    { return &v }
func boolPtr(v bool) *bool       { return &v }
func float64Ptr(v float64) *float64 {
	return &v
}

func columnIndex(t *testing.T) map[string]int {
	t.Helper()
	cols := Columns()
	if len(cols) != len(ddlColumns) {
		t.Fatalf("Columns() count = %d, want %d (DDL)", len(cols), len(ddlColumns))
	}
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		idx[c] = i
	}
	return idx
}

func TestColumns_MatchDDL(t *testing.T) {
	cols := Columns()
	for i, want := range ddlColumns {
		if cols[i] != want {
			t.Fatalf("Columns()[%d] = %q, want %q (DDL order)", i, cols[i], want)
		}
	}
	if len(cols) != 99 {
		t.Fatalf("expected 99 columns, got %d", len(cols))
	}
}

func TestToRow_ColumnCountAndOrder(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	if len(row) != 99 {
		t.Fatalf("row length = %d, want 99", len(row))
	}
}

func TestToRow_StringZeroToNil(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	// err_code is "" in fixture -> NULL
	if row[idx["err_code"]] != nil {
		t.Errorf("err_code should be NULL for empty string, got %v", row[idx["err_code"]])
	}
	// authorization never filled -> NULL
	if row[idx["authorization"]] != nil {
		t.Errorf("authorization should be NULL, got %v", row[idx["authorization"]])
	}
	// non-empty string stays
	if row[idx["proto"]] != "HTTP/1.1" {
		t.Errorf("proto = %v, want HTTP/1.1", row[idx["proto"]])
	}
}

func TestToRow_NumericZeroKept(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	// req_body_len is 0 -> kept as 0 (NOT NULL)
	if v := row[idx["req_body_len"]]; v != uint32(0) {
		t.Errorf("req_body_len should be uint32(0), got %v (%T)", v, v)
	}
	// ai_retry_count is 0 -> kept as 0
	if v := row[idx["ai_retry_count"]]; v != uint32(0) {
		t.Errorf("ai_retry_count should be uint32(0), got %v (%T)", v, v)
	}
	// non-zero numeric stays
	if v := row[idx["res_status_code"]]; v != uint32(200) {
		t.Errorf("res_status_code = %v, want 200", v)
	}
}

func TestToRow_BoolToTinyInt(t *testing.T) {
	mapper := NewFieldMapper()
	log := makeBfeLog()
	log.RequestLog.AiStream = boolPtr(true)
	log.RequestLog.AddrInfo.IsTrustSrcIp = boolPtr(false)

	row, err := mapper.ToRow(log)
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	// ai_stream=true -> 1 (TINYINT)
	if v := row[idx["ai_stream"]]; v != int64(1) {
		t.Errorf("ai_stream should be int64(1) for true, got %v (%T)", v, v)
	}
	// is_trust_src_ip=false -> 0 (TINYINT)
	if v := row[idx["is_trust_src_ip"]]; v != int64(0) {
		t.Errorf("is_trust_src_ip should be int64(0) for false, got %v (%T)", v, v)
	}
}

func TestToRow_ApikeyTagsFlatten(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	if v := row[idx["level1Name"]]; v != "dep" {
		t.Errorf("level1Name = %v, want dep", v)
	}
	if v := row[idx["level1"]]; v != "ops" {
		t.Errorf("level1 = %v, want ops", v)
	}
	if v := row[idx["level2Name"]]; v != "team" {
		t.Errorf("level2Name = %v, want team", v)
	}
	if v := row[idx["level2"]]; v != "bfe" {
		t.Errorf("level2 = %v, want bfe", v)
	}
	// levels 3-5 not set -> NULL
	for _, col := range []string{"level3Name", "level3", "level4Name", "level4", "level5Name", "level5"} {
		if row[idx[col]] != nil {
			t.Errorf("%s should be NULL, got %v", col, row[idx[col]])
		}
	}
}

func TestToRow_NoTagsAllNil(t *testing.T) {
	mapper := NewFieldMapper()
	log := makeBfeLog()
	log.RequestLog.AiApikeytags = nil

	row, err := mapper.ToRow(log)
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	for _, col := range []string{"level1Name", "level1", "level2Name", "level2",
		"level3Name", "level3", "level4Name", "level4", "level5Name", "level5"} {
		if row[idx[col]] != nil {
			t.Errorf("%s should be NULL without tags, got %v", col, row[idx[col]])
		}
	}
}

func TestToRow_JSONColumns(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	// req_headers with entries -> marshaled JSON string
	want := `[{"key":"X-Test","value":"abc"}]`
	if v := row[idx["req_headers"]]; v != want {
		t.Errorf("req_headers = %v, want %s", v, want)
	}
	// ai_route_rule_hits -> marshaled JSON string
	want = `[{"rule_owner":"ak_user_a","rule_owner_type":"apikey","rule_name":"user_a-rule1"}]`
	if v := row[idx["ai_route_rule_hits"]]; v != want {
		t.Errorf("ai_route_rule_hits = %v, want %s", v, want)
	}
	// ai_auth_hit_quota_plans -> marshaled JSON string
	want = `["hit-plan-a"]`
	if v := row[idx["ai_auth_hit_quota_plans"]]; v != want {
		t.Errorf("ai_auth_hit_quota_plans = %v, want %s", v, want)
	}
	// res_headers empty -> NULL
	if v := row[idx["res_headers"]]; v != nil {
		t.Errorf("res_headers should be NULL when empty, got %v", v)
	}
	// ai_cluster_key_names empty -> NULL
	if v := row[idx["ai_cluster_key_names"]]; v != nil {
		t.Errorf("ai_cluster_key_names should be NULL when empty, got %v", v)
	}
	// ai_auth_reject_quota_plans empty -> NULL
	if v := row[idx["ai_auth_reject_quota_plans"]]; v != nil {
		t.Errorf("ai_auth_reject_quota_plans should be NULL when empty, got %v", v)
	}
}

func TestToRow_LogTime(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	v, ok := row[idx["log_time"]].(time.Time)
	if !ok {
		t.Fatalf("log_time should be time.Time, got %T", row[idx["log_time"]])
	}
	want := time.Unix(1782353290, 0) // local timezone, same as FROM_UNIXTIME
	if !v.Equal(want) {
		t.Errorf("log_time = %v, want %v", v, want)
	}
}

func TestToRow_NilRequestLog(t *testing.T) {
	mapper := NewFieldMapper()
	logid := uint64(1)
	ts := uint64(100)
	bfeLog := &bfe_access_pb.BfeLog{
		Logid:     &logid,
		Timestamp: &ts,
		LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
	}

	row, err := mapper.ToRow(bfeLog)
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	if len(row) != 99 {
		t.Fatalf("row length = %d, want 99", len(row))
	}
	idx := columnIndex(t)

	if row[idx["err_code"]] != nil {
		t.Errorf("err_code should be NULL without RequestLog, got %v", row[idx["err_code"]])
	}
	if row[idx["req_body_len"]] != uint32(0) {
		t.Errorf("req_body_len should be uint32(0) without RequestLog, got %v", row[idx["req_body_len"]])
	}
	// NOT NULL string columns stay "" (never NULL) without RequestLog.
	if v := row[idx["ai_cache_status"]]; v != "" {
		t.Errorf("ai_cache_status should be \"\" without RequestLog, got %v", v)
	}
	// bool TINYINT columns default to 0 without RequestLog.
	if v := row[idx["mirror_hit"]]; v != int64(0) {
		t.Errorf("mirror_hit should be int64(0) without RequestLog, got %v", v)
	}
	// optional numeric columns -> NULL without RequestLog.
	if v := row[idx["ai_intent_confidence"]]; v != nil {
		t.Errorf("ai_intent_confidence should be NULL without RequestLog, got %v", v)
	}
	if v := row[idx["ai_intent_cache_hit"]]; v != nil {
		t.Errorf("ai_intent_cache_hit should be NULL without RequestLog, got %v", v)
	}
	if v := row[idx["log_time"]]; !v.(time.Time).Equal(time.Unix(100, 0)) {
		t.Errorf("log_time = %v, want %v", v, time.Unix(100, 0))
	}
}

func TestToRow_HostIdInjected(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	v, ok := row[idx["hostid"]].(string)
	if !ok || v == "" {
		t.Errorf("hostid should be non-empty string, got %v", row[idx["hostid"]])
	}
}

// TestToRow_CacheMirrorIntentFields asserts the 10 ai-cache / mirroring / intent
// slots: NOT NULL string columns keep "", bool columns become TINYINT 0/1,
// numeric columns keep their values.
func TestToRow_CacheMirrorIntentFields(t *testing.T) {
	mapper := NewFieldMapper()
	row, err := mapper.ToRow(makeBfeLog())
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	if v := row[idx["ai_cache_status"]]; v != "hit" {
		t.Errorf("ai_cache_status = %v, want hit", v)
	}
	if v := row[idx["mirror_hit"]]; v != int64(1) {
		t.Errorf("mirror_hit = %v (%T), want int64(1)", v, v)
	}
	if v := row[idx["mirror_cluster"]]; v != "mirror-cluster-a" {
		t.Errorf("mirror_cluster = %v, want mirror-cluster-a", v)
	}
	if v := row[idx["ai_intent_question"]]; v != "intent-q-001" {
		t.Errorf("ai_intent_question = %v, want intent-q-001", v)
	}
	if v := row[idx["ai_intent_answer"]]; v != "intent-a-1" {
		t.Errorf("ai_intent_answer = %v, want intent-a-1", v)
	}
	if v := row[idx["ai_intent_confidence"]]; v != float64(0.95) {
		t.Errorf("ai_intent_confidence = %v (%T), want float64(0.95)", v, v)
	}
	if v := row[idx["ai_intent_source"]]; v != "llm" {
		t.Errorf("ai_intent_source = %v, want llm", v)
	}
	if v := row[idx["ai_intent_latency_us"]]; v != int64(1234) {
		t.Errorf("ai_intent_latency_us = %v (%T), want int64(1234)", v, v)
	}
	if v := row[idx["ai_intent_cache_hit"]]; v != int64(1) {
		t.Errorf("ai_intent_cache_hit = %v (%T), want int64(1)", v, v)
	}
	if v := row[idx["ai_intent_questions_version"]]; v != "v2026-09-27" {
		t.Errorf("ai_intent_questions_version = %v, want v2026-09-27", v)
	}
}

// TestToRow_CacheMirrorIntentZeroForm asserts the default form when the new
// fields are unset: NOT NULL string columns are "" (not NULL), mirror_hit is
// 0 (NOT NULL TINYINT), and the three nullable optional columns
// (confidence/latency_us/cache_hit) are NULL (proto pointer nil = 未设置).
func TestToRow_CacheMirrorIntentZeroForm(t *testing.T) {
	mapper := NewFieldMapper()
	log := makeBfeLog()
	log.RequestLog.AiCacheStatus = nil
	log.RequestLog.MirrorHit = nil
	log.RequestLog.MirrorCluster = nil
	log.RequestLog.AiIntentQuestion = nil
	log.RequestLog.AiIntentAnswer = nil
	log.RequestLog.AiIntentConfidence = nil
	log.RequestLog.AiIntentSource = nil
	log.RequestLog.AiIntentLatencyUs = nil
	log.RequestLog.AiIntentCacheHit = nil
	log.RequestLog.AiIntentQuestionsVersion = nil

	row, err := mapper.ToRow(log)
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	for _, col := range []string{
		"ai_cache_status", "mirror_cluster", "ai_intent_question", "ai_intent_answer",
		"ai_intent_source", "ai_intent_questions_version",
	} {
		if v := row[idx[col]]; v != "" {
			t.Errorf("%s = %v (%T), want \"\" (NOT NULL column)", col, v, v)
		}
	}
	// mirror_hit is a NOT NULL TINYINT column: unset -> 0.
	if v := row[idx["mirror_hit"]]; v != int64(0) {
		t.Errorf("mirror_hit = %v (%T), want int64(0)", v, v)
	}
	// Nullable optional columns: unset -> NULL.
	for _, col := range []string{"ai_intent_confidence", "ai_intent_latency_us", "ai_intent_cache_hit"} {
		if v := row[idx[col]]; v != nil {
			t.Errorf("%s = %v (%T), want nil (NULL, optional unset)", col, v, v)
		}
	}
}

// TestFieldMapper_RangeClamp 覆盖 §5.2 范围表全量逐列断言（issue #12）：
// 边界值原样透传，max+1 clamp 到 max；字符串/NULL 规则不受影响；
// onNormalized 回调按 clamp 次数触发，nil 回调静默。
func TestFieldMapper_RangeClamp(t *testing.T) {
	idx := columnIndex(t)

	// §5.2 范围表（proto uint32 -> DDL 列类型）：15 列 INT + res_status_code
	// SMALLINT(32767) + backend_retry TINYINT(127)。列名 -> RequestLog 字段 setter
	intCols := []string{
		"proxy_delay_time", "all_time", "read_client_time", "cluster_serve_time",
		"backend_serve_time", "write_client_time", "connect_backend_time",
		"session_offset_time", "req_header_len", "req_body_len",
		"res_header_len", "res_body_len", "req_num", "ai_retry_count",
	}
	setters := map[string]func(rl *bfe_access_pb.RequestLog, v uint32){
		"proxy_delay_time":     func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ProxyDelayTime = uint32Ptr(v) },
		"all_time":             func(rl *bfe_access_pb.RequestLog, v uint32) { rl.AllTime = uint32Ptr(v) },
		"read_client_time":     func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ReadClientTime = uint32Ptr(v) },
		"cluster_serve_time":   func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ClusterServeTime = uint32Ptr(v) },
		"backend_serve_time":   func(rl *bfe_access_pb.RequestLog, v uint32) { rl.BackendServeTime = uint32Ptr(v) },
		"write_client_time":    func(rl *bfe_access_pb.RequestLog, v uint32) { rl.WriteClientTime = uint32Ptr(v) },
		"connect_backend_time": func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ConnectBackendTime = uint32Ptr(v) },
		"session_offset_time":  func(rl *bfe_access_pb.RequestLog, v uint32) { rl.SessionOffsetTime = uint32Ptr(v) },
		"req_header_len":       func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ReqHeaderLen = uint32Ptr(v) },
		"req_body_len":         func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ReqBodyLen = uint32Ptr(v) },
		"res_header_len":       func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ResHeaderLen = uint32Ptr(v) },
		"res_body_len":         func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ResBodyLen = uint32Ptr(v) },
		"req_num":              func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ReqNum = uint32Ptr(v) },
		"ai_retry_count":       func(rl *bfe_access_pb.RequestLog, v uint32) { rl.AiRetryCount = uint32Ptr(v) },
		"res_status_code":      func(rl *bfe_access_pb.RequestLog, v uint32) { rl.ResStatusCode = uint32Ptr(v) },
		"backend_retry":        func(rl *bfe_access_pb.RequestLog, v uint32) { rl.BackendRetry = uint32Ptr(v) },
	}

	// case {col, max}：边界值（max 原样）与 max+1（clamp 到 int64(max)）
	cases := make([]struct {
		col string
		max int64
	}, 0, len(intCols)+2)
	for _, col := range intCols {
		cases = append(cases, struct {
			col string
			max int64
		}{col, 2147483647})
	}
	cases = append(cases,
		struct {
			col string
			max int64
		}{"res_status_code", 32767},
		struct {
			col string
			max int64
		}{"backend_retry", 127})

	for _, tc := range cases {
		set, ok := setters[tc.col]
		if !ok {
			t.Fatalf("no setter for column %s", tc.col)
		}

		// 边界值原样透传（保持抽取类型 uint32）
		log := makeBfeLog()
		set(log.RequestLog, uint32(tc.max))
		mapper := NewFieldMapper()
		row, err := mapper.ToRow(log)
		if err != nil {
			t.Fatalf("ToRow failed for %s: %v", tc.col, err)
		}
		if v := row[idx[tc.col]]; v != uint32(tc.max) {
			t.Errorf("%s = %v (%T), want uint32(%d) (boundary value kept)", tc.col, v, v, tc.max)
		}

		// max+1 clamp 到列上限
		log = makeBfeLog()
		set(log.RequestLog, uint32(tc.max)+1)
		row, err = mapper.ToRow(log)
		if err != nil {
			t.Fatalf("ToRow failed for %s: %v", tc.col, err)
		}
		if v := row[idx[tc.col]]; v != tc.max {
			t.Errorf("%s = %v (%T), want int64(%d) (max+1 clamped)", tc.col, v, v, tc.max)
		}
	}
}

// TestFieldMapper_RangeClampCallback 断言 onNormalized 回调按 clamp 次数触发
// （nil 回调静默），且字符串 NULL 规则不受影响。
func TestFieldMapper_RangeClampCallback(t *testing.T) {
	idx := columnIndex(t)

	log := makeBfeLog()
	log.RequestLog.ProxyDelayTime = uint32Ptr(4294967295) // uint32 max -> clamp
	log.RequestLog.AllTime = uint32Ptr(2147483648)        // INT max+1 -> clamp

	var cols []string
	mapper := NewFieldMapper()
	mapper.SetOnNormalized(func(col string) { cols = append(cols, col) })
	row, err := mapper.ToRow(log)
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}

	if len(cols) != 2 {
		t.Fatalf("onNormalized called %d times, want 2", len(cols))
	}
	if row[idx["proxy_delay_time"]] != int64(2147483647) {
		t.Errorf("proxy_delay_time = %v, want 2147483647", row[idx["proxy_delay_time"]])
	}
	if row[idx["all_time"]] != int64(2147483647) {
		t.Errorf("all_time = %v, want 2147483647", row[idx["all_time"]])
	}

	// nil 回调静默，不 panic、行为不变
	silent := NewFieldMapper()
	row, err = silent.ToRow(log)
	if err != nil {
		t.Fatalf("ToRow with nil callback failed: %v", err)
	}
	if row[idx["proxy_delay_time"]] != int64(2147483647) {
		t.Errorf("nil callback: proxy_delay_time = %v, want 2147483647", row[idx["proxy_delay_time"]])
	}

	// 字符串 NULL 规则不受 clamp 影响：err_code "" 仍为 NULL
	if row[idx["err_code"]] != nil {
		t.Errorf("err_code should stay NULL for empty string, got %v", row[idx["err_code"]])
	}
}

// TestToRow_OptionalNumSetZeroKept asserts explicitly-set zero values on the
// optional columns are written as 0 (not NULL): pointer set -> value, so the
// consumer can distinguish "evaluated to zero" from "not evaluated" (NULL).
func TestToRow_OptionalNumSetZeroKept(t *testing.T) {
	mapper := NewFieldMapper()
	log := makeBfeLog()
	log.RequestLog.AiIntentConfidence = float64Ptr(0)
	log.RequestLog.AiIntentLatencyUs = int64Ptr(0)
	log.RequestLog.AiIntentCacheHit = boolPtr(false)

	row, err := mapper.ToRow(log)
	if err != nil {
		t.Fatalf("ToRow failed: %v", err)
	}
	idx := columnIndex(t)

	if v := row[idx["ai_intent_confidence"]]; v != float64(0) {
		t.Errorf("ai_intent_confidence = %v (%T), want float64(0) (set, not NULL)", v, v)
	}
	if v := row[idx["ai_intent_latency_us"]]; v != int64(0) {
		t.Errorf("ai_intent_latency_us = %v (%T), want int64(0) (set, not NULL)", v, v)
	}
	if v := row[idx["ai_intent_cache_hit"]]; v != int64(0) {
		t.Errorf("ai_intent_cache_hit = %v (%T), want int64(0) (set false, not NULL)", v, v)
	}
}
