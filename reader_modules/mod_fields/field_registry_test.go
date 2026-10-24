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

package mod_fields

import (
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

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
			ErrCode:            strPtr(""),
			ErrMsg:             strPtr(""),
			ReqHeaderLen:       uint32Ptr(189),
			ReqBodyLen:         uint32Ptr(0),
			ClientIp:           uint32Ptr(0x0A000001), // 10.0.0.1
			ReqNum:             uint32Ptr(1),
			Proto:              strPtr("HTTP/1.1"),
			HeaderHost:         strPtr("example.com"),
			OriginUri:          strPtr("/api/v1/test"),
			Method:             strPtr("POST"),
			ContentType:        strPtr("application/json"),
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
			AiRequestedModel:   strPtr("test-model"),
			AiTargetModel:      strPtr("gpt-5"),
			AiStream:           boolPtr(false),
			AiInputTokens:      int64Ptr(34),
			AiOutputTokens:     int64Ptr(182),
			AiTotalTokens:      int64Ptr(216),
			AiTtftUs:           int64Ptr(5486),
			AiTpotUs:           int64Ptr(3),
			AiProvider:         strPtr("openai"),
			AiRetryCount:       uint32Ptr(1),
			AiCostValue:        int64Ptr(5000),
			AiCostCurrency:     strPtr("USD"),
			AiRouteRuleHits: []*bfe_access_pb.AIRouteRuleHit{
				{
					RuleOwner:     strPtr("ak_user_a"),
					RuleOwnerType: strPtr("apikey"),
					RuleName:      strPtr("user_a-rule1"),
				},
			},
			AiClusterKeyNames: []*bfe_access_pb.ClusterKeyName{
				{
					ClusterName: strPtr("cluster-a"),
					KeyName:     strPtr("key-001"),
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

func TestFieldRegistry_DefaultFieldsCount(t *testing.T) {
	def := DefaultFields()
	if len(def) != 92 {
		t.Fatalf("expected 92 default fields, got %d: %v", len(def), def)
	}
}

// TestFieldRegistry_DefaultFieldsAlignedWithMysqlWriter pins the alignment
// contract: the mod_kafka default output set covers every direct field the
// mod_log_mysql writer persists (see modifications/2026-10-01-align-mod-kafka-
// output-with-mod-log-mysql). Derived columns (log_time, level1Name~level5)
// are produced from timestamp / ai_apikeytags and are not separate fields.
func TestFieldRegistry_DefaultFieldsAlignedWithMysqlWriter(t *testing.T) {
	mysqlWriterFields := []string{
		"hostid", "ai_apikey_id", "ai_requested_model", "logid", "product", "log_tag",
		"client_ip", "client_network", "is_trust_src_ip", "req_num", "session_id",
		"bfe_ip", "sock_src_ip", "vip", "vip6",
		"err_code", "err_msg",
		"proto", "header_host", "origin_uri", "final_uri", "method", "content_type",
		"x_forward_for", "accept_language", "authorization", "transfer_encoding",
		"referrer", "user_agent", "delegation", "uid", "cookie", "req_headers",
		"req_header_len", "req_body_len",
		"cluster", "sub_cluster", "backend_info", "backend_retry",
		"res_status_code", "res_header_len", "res_body_len", "res_content_type",
		"res_location", "res_transfer_encoding", "res_headers",
		"all_time", "read_client_time", "cluster_serve_time", "backend_serve_time",
		"write_client_time", "connect_backend_time", "proxy_delay_time", "session_offset_time",
		"ai_target_model", "ai_stream", "ai_input_tokens", "ai_output_tokens", "ai_total_tokens",
		"ai_cache_read_tokens", "ai_cache_write_tokens", "ai_audio_input_tokens",
		"ai_audio_output_tokens", "ai_image_count", "ai_ttft_us", "ai_tpot_us",
		"ai_provider", "ai_protocol", "ai_mode", "ai_retry_count", "ai_cost_value", "ai_cost_currency",
		"ai_route_rule_hits", "ai_cluster_key_names", "ai_rate_limit_hits",
		"ai_auth_reject_reason", "ai_auth_reject_quota_plans", "ai_auth_hit_quota_plans",
		"ai_cache_status", "mirror_hit", "mirror_cluster",
		"ai_intent_question", "ai_intent_answer", "ai_intent_confidence", "ai_intent_source",
		"ai_intent_latency_us", "ai_intent_cache_hit", "ai_intent_questions_version",
		"timestamp", "ai_apikeytags",
	}
	def := map[string]bool{}
	for _, name := range DefaultFields() {
		def[name] = true
	}
	req := map[string]bool{}
	for _, name := range RequiredFields() {
		req[name] = true
	}
	for _, name := range mysqlWriterFields {
		if !def[name] && !req[name] {
			t.Errorf("field %s is written by mod_log_mysql but not in the mod_kafka default/required output set", name)
		}
	}
}

func TestFieldRegistry_RequiredFieldsCount(t *testing.T) {
	req := RequiredFields()
	if len(req) != 22 {
		t.Fatalf("expected 22 required fields, got %d: %v", len(req), req)
	}
}

func TestFieldRegistry_AllFieldsCount(t *testing.T) {
	all := AllFields()
	if len(all) != 92 {
		t.Fatalf("expected 92 fields, got %d", len(all))
	}
}

// TestFieldRegistry_CacheMirrorIntentFields asserts the ai-cache / traffic
// mirroring / ai-intent fields (bfe-access-pb v0.3.7/3.8/3.9) are registered
// with the expected type and Default membership.
func TestFieldRegistry_CacheMirrorIntentFields(t *testing.T) {
	want := map[string]string{
		"ai_cache_status":           "string",
		"ai_intent_question":        "string",
		"ai_intent_answer":          "string",
		"ai_intent_confidence":      "float64",
		"ai_intent_source":          "string",
		"ai_intent_latency_us":      "int64",
		"ai_intent_cache_hit":       "bool",
		"ai_intent_questions_version": "string",
		"mirror_hit":                "bool",
		"mirror_cluster":            "string",
	}

	defs := make(map[string]FieldDef)
	for _, d := range AllFields() {
		defs[d.Name] = d
	}
	for name, typ := range want {
		d, ok := defs[name]
		if !ok {
			t.Errorf("field %s should be registered", name)
			continue
		}
		if d.Type != typ {
			t.Errorf("field %s type = %s, want %s", name, d.Type, typ)
		}
		if d.Required {
			t.Errorf("field %s should not be required", name)
		}
		if !d.Default {
			t.Errorf("field %s should be in default set", name)
		}
	}
}

func TestFieldRegistry_IsValidField(t *testing.T) {
	if !IsValidField("logid") {
		t.Error("logid should be valid")
	}
	if !IsValidField("hostid") {
		t.Error("hostid should be valid")
	}
	if !IsValidField("ai_apikey_id") {
		t.Error("ai_apikey_id should be valid")
	}
	if IsValidField("ai_apikey") {
		t.Error("ai_apikey should not be valid (renamed to ai_apikey_id)")
	}
	if !IsValidField("referrer") {
		t.Error("referrer should be valid")
	}
	if IsValidField("nonexistent") {
		t.Error("nonexistent should not be valid")
	}
}

func TestFieldRegistry_Extract(t *testing.T) {
	log := makeBfeLog()

	tests := []struct {
		name     string
		expected interface{}
	}{
		{"logid", uint64(12345)},
		{"timestamp", uint64(1782353290)},
		{"hostid", "TODO"},
		{"product", "BFE"},
		{"log_tag", "req_BFE"},
		{"client_ip", "10.0.0.1"},
		{"err_code", ""},
		{"proto", "HTTP/1.1"},
		{"header_host", "example.com"},
		{"origin_uri", "/api/v1/test"},
		{"method", "POST"},
		{"content_type", "application/json"},
		{"cluster", "cluster_ai"},
		{"sub_cluster", "pool_bj"},
		{"backend_info", "10.0.0.2:8080"},
		{"res_status_code", uint32(200)},
		{"all_time", uint32(11)},
		{"read_client_time", uint32(2)},
		{"cluster_serve_time", uint32(5)},
		{"backend_serve_time", uint32(4)},
		{"ai_apikey_id", "key-id-123"},
		{"ai_requested_model", "test-model"},
		{"ai_target_model", "gpt-5"},
		{"ai_stream", false},
		{"ai_input_tokens", int64(34)},
		{"ai_output_tokens", int64(182)},
		{"ai_total_tokens", int64(216)},
		{"ai_ttft_us", int64(5486)},
		{"ai_tpot_us", int64(3)},
		{"ai_provider", "openai"},
		{"ai_retry_count", uint32(1)},
		{"ai_cost_value", int64(5000)},
		{"ai_cost_currency", "USD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isZero := Extract(tt.name, log)
			if tt.name == "hostid" {
				if isZero {
					t.Errorf("hostid should not be zero")
				}
				if _, ok := val.(string); !ok {
					t.Errorf("hostid should be string, got %T", val)
				}
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
			_ = isZero
		})
	}
}

func TestFieldRegistry_ExtractAIRouteRuleHits(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_route_rule_hits", log)
	if isZero {
		t.Fatal("ai_route_rule_hits should not be zero")
	}
	hits, ok := val.([]AIRouteRuleHitJSON)
	if !ok {
		t.Fatalf("expected []AIRouteRuleHitJSON, got %T", val)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	if hits[0].RuleOwner != "ak_user_a" || hits[0].RuleOwnerType != "apikey" || hits[0].RuleName != "user_a-rule1" {
		t.Errorf("unexpected hit: %+v", hits[0])
	}
}

func TestFieldRegistry_ExtractAIClusterKeyNames(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_cluster_key_names", log)
	if isZero {
		t.Fatal("ai_cluster_key_names should not be zero")
	}
	pairs, ok := val.([]ClusterKeyNameJSON)
	if !ok {
		t.Fatalf("expected []ClusterKeyNameJSON, got %T", val)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
	if pairs[0].ClusterName != "cluster-a" || pairs[0].KeyName != "key-001" {
		t.Errorf("unexpected pair: %+v", pairs[0])
	}
}

func TestFieldRegistry_ExtractAIAuthHitQuotaPlans(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_auth_hit_quota_plans", log)
	if isZero {
		t.Fatal("ai_auth_hit_quota_plans should not be zero")
	}
	plans, ok := val.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", val)
	}
	if len(plans) != 1 || plans[0] != "hit-plan-a" {
		t.Errorf("unexpected plans: %v", plans)
	}
}

func TestFieldRegistry_ExtractCacheMirrorIntentFields(t *testing.T) {
	log := makeBfeLog()

	tests := []struct {
		name     string
		expected interface{}
	}{
		{"ai_cache_status", "hit"},
		{"mirror_hit", true},
		{"mirror_cluster", "mirror-cluster-a"},
		{"ai_intent_question", "intent-q-001"},
		{"ai_intent_answer", "intent-a-1"},
		{"ai_intent_confidence", float64(0.95)},
		{"ai_intent_source", "llm"},
		{"ai_intent_latency_us", int64(1234)},
		{"ai_intent_cache_hit", true},
		{"ai_intent_questions_version", "v2026-09-27"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isZero := Extract(tt.name, log)
			if isZero {
				t.Errorf("%s should not be zero", tt.name)
			}
			if val != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
		})
	}

	// Unset optional fields extract as nil (JSON null), matching the
	// mod_log_mysql kindOptionalNum NULL=unset caliber
	// (modifications/2026-10-01-align-mod-kafka-output-with-mod-log-mysql).
	log.RequestLog.AiIntentConfidence = nil
	log.RequestLog.AiIntentLatencyUs = nil
	log.RequestLog.AiIntentCacheHit = nil
	log.RequestLog.MirrorHit = nil
	for _, name := range []string{"ai_intent_confidence", "ai_intent_latency_us", "ai_intent_cache_hit"} {
		val, isZero := Extract(name, log)
		if val != nil {
			t.Errorf("%s should extract as nil when unset, got %v", name, val)
		}
		if !isZero {
			t.Errorf("%s should be flagged zero when unset", name)
		}
	}
	// Non-optional fields keep the historical zero-value caliber.
	if _, isZero := Extract("mirror_hit", log); !isZero {
		t.Error("mirror_hit should be zero when unset")
	}
}

func TestFieldRegistry_ExtractAiApikeytags(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_apikeytags", log)
	if isZero {
		t.Fatal("ai_apikeytags should not be zero")
	}
	tags, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map[string]interface{}, got %T", val)
	}
	if len(tags) != 5 {
		t.Fatalf("expected 5 levels, got %d", len(tags))
	}
	lvl1, ok := tags["level1"].(ApikeyTagJSON)
	if !ok || lvl1.Tagname != "dep" || lvl1.Tagvalue != "ops" {
		t.Errorf("unexpected level1: %+v", tags["level1"])
	}
	lvl2, ok := tags["level2"].(ApikeyTagJSON)
	if !ok || lvl2.Tagname != "team" || lvl2.Tagvalue != "bfe" {
		t.Errorf("unexpected level2: %+v", tags["level2"])
	}
	for _, lvl := range []string{"level3", "level4", "level5"} {
		m, ok := tags[lvl].(map[string]interface{})
		if !ok || len(m) != 0 {
			t.Errorf("expected %s to be empty object, got %+v", lvl, tags[lvl])
		}
	}
}

func TestFieldRegistry_ExtractZeroValues(t *testing.T) {
	log := makeBfeLog()

	val, isZero := Extract("err_code", log)
	if !isZero {
		t.Error("err_code should be zero")
	}
	_ = val

	val, isZero = Extract("req_body_len", log)
	if !isZero {
		t.Error("req_body_len should be zero")
	}
	_ = val

	val, isZero = Extract("ai_stream", log)
	if !isZero {
		t.Error("ai_stream should be zero (false)")
	}
	_ = val
}

func TestFieldRegistry_ExtractNilRequestLog(t *testing.T) {
	logid := uint64(1)
	ts := uint64(100)
	bfeLog := &bfe_access_pb.BfeLog{
		Logid:     &logid,
		Timestamp: &ts,
	}

	val, isZero := Extract("logid", bfeLog)
	if isZero || val.(uint64) != 1 {
		t.Errorf("expected logid=1, got %v", val)
	}

	_, isZero = Extract("err_code", bfeLog)
	if !isZero {
		t.Error("err_code should be zero when RequestLog is nil")
	}
}

func TestFieldRegistry_IpConversion(t *testing.T) {
	log := makeBfeLog()
	log.RequestLog.ClientIp = uint32Ptr(0xC0A80101) // 192.168.1.1

	val, _ := Extract("client_ip", log)
	if val != "192.168.1.1" {
		t.Errorf("expected 192.168.1.1, got %v", val)
	}
}

func TestFieldRegistry_AddrInfoFlattened(t *testing.T) {
	log := makeBfeLog()
	log.RequestLog.AddrInfo = &bfe_access_pb.ConnAddrInfo{
		BfeIp:        uint32Ptr(0x0A000064), // 10.0.0.100
		SockSrcIp:    uint32Ptr(0xC0A80164), // 192.168.1.100
		IsTrustSrcIp: boolPtr(true),
		Vip:          uint32Ptr(0x0A0000C8), // 10.0.0.200
	}

	if val, _ := Extract("bfe_ip", log); val != "10.0.0.100" {
		t.Errorf("bfe_ip: expected 10.0.0.100, got %v", val)
	}
	if val, _ := Extract("sock_src_ip", log); val != "192.168.1.100" {
		t.Errorf("sock_src_ip: expected 192.168.1.100, got %v", val)
	}
	if val, _ := Extract("is_trust_src_ip", log); val != true {
		t.Errorf("is_trust_src_ip: expected true, got %v", val)
	}
	if val, _ := Extract("vip", log); val != "10.0.0.200" {
		t.Errorf("vip: expected 10.0.0.200, got %v", val)
	}
}
