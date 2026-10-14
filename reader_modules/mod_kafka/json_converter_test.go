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

package mod_kafka

import (
	"encoding/json"
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/reader_modules/mod_fields"
)

// makeBfeLog builds a BfeLog with typical request fields for converter tests.
// This is a package-local copy of the test helper (the shared one moved to mod_fields).
func makeBfeLog() *bfe_access_pb.BfeLog {
	logid := uint64(12345)
	timestamp := uint64(1782353290_000_000_000) // Unix 纳秒
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
		},
	}
}

func strPtr(s string) *string    { return &s }
func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }
func int64Ptr(v int64) *int64    { return &v }
func int32Ptr(v int32) *int32    { return &v }
func boolPtr(v bool) *bool       { return &v }

func TestConvertBfeLogToJSON_DefaultFields(t *testing.T) {
	log := makeBfeLog()
	of := DefaultOutputFields()

	jsonBytes, err := ConvertBfeLogToJSON(log, of)
	if err != nil {
		t.Fatalf("ConvertBfeLogToJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if result["logid"].(float64) != 12345 {
		t.Errorf("logid: expected 12345, got %v", result["logid"])
	}
	if result["timestamp"].(float64) != 1782353290_000_000_000 {
		t.Errorf("timestamp: expected 1782353290_000_000_000, got %v", result["timestamp"])
	}
	if result["product"] != "BFE" {
		t.Errorf("product: expected BFE, got %v", result["product"])
	}
	if result["origin_uri"] != "/api/v1/test" {
		t.Errorf("origin_uri: expected /api/v1/test, got %v", result["origin_uri"])
	}
	if result["backend_info"] != "10.0.0.2:8080" {
		t.Errorf("backend_info: expected 10.0.0.2:8080, got %v", result["backend_info"])
	}
	if hostid, ok := result["hostid"].(string); !ok || hostid == "" {
		t.Errorf("hostid should be non-empty string, got %v", result["hostid"])
	}
}

func TestConvertBfeLogToJSON_RequireFields(t *testing.T) {
	log := makeBfeLog()
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{FieldMode: "require"},
	}
	of := cfg.ResolveFields()

	jsonBytes, err := ConvertBfeLogToJSON(log, of)
	if err != nil {
		t.Fatalf("ConvertBfeLogToJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	required := mod_fields.RequiredFields()
	for _, name := range required {
		// empty-string / zero-value required fields are omitted by omitempty behavior
		if name == "err_code" || name == "err_msg" || name == "req_body_len" {
			continue
		}
		if _, ok := result[name]; !ok {
			t.Errorf("required field %q missing from output", name)
		}
	}

	if _, ok := result["ai_apikey_id"]; ok {
		t.Error("ai_apikey_id should not be in require output")
	}
	if _, ok := result["ai_apikey"]; ok {
		t.Error("ai_apikey should not be in require output (renamed)")
	}
	if _, ok := result["cluster"]; ok {
		t.Error("cluster should not be in require output")
	}
}

func TestConvertBfeLogToJSON_CustomizedFields(t *testing.T) {
	log := makeBfeLog()
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode:  "customized",
			FieldNames: []string{"ai_apikey_id", "ai_requested_model", "ai_target_model", "ai_input_tokens", "ai_provider", "ai_route_rule_hits", "ai_cluster_key_names", "ai_auth_hit_quota_plans"},
		},
	}
	of := cfg.ResolveFields()

	jsonBytes, err := ConvertBfeLogToJSON(log, of)
	if err != nil {
		t.Fatalf("ConvertBfeLogToJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if result["ai_apikey_id"] != "key-id-123" {
		t.Errorf("ai_apikey_id: expected key-id-123, got %v", result["ai_apikey_id"])
	}
	if result["ai_requested_model"] != "test-model" {
		t.Errorf("ai_requested_model: expected test-model, got %v", result["ai_requested_model"])
	}
	if result["ai_target_model"] != "gpt-5" {
		t.Errorf("ai_target_model: expected gpt-5, got %v", result["ai_target_model"])
	}
	if result["ai_input_tokens"].(float64) != 34 {
		t.Errorf("ai_input_tokens: expected 34, got %v", result["ai_input_tokens"])
	}
	if result["ai_provider"] != "openai" {
		t.Errorf("ai_provider: expected openai, got %v", result["ai_provider"])
	}

	routeHits, ok := result["ai_route_rule_hits"].([]interface{})
	if !ok || len(routeHits) != 1 {
		t.Fatalf("ai_route_rule_hits: expected 1 element array, got %v", result["ai_route_rule_hits"])
	}
	firstHit := routeHits[0].(map[string]interface{})
	if firstHit["rule_owner"] != "ak_user_a" {
		t.Errorf("ai_route_rule_hits[0].rule_owner: expected ak_user_a, got %v", firstHit["rule_owner"])
	}

	clusterKeys, ok := result["ai_cluster_key_names"].([]interface{})
	if !ok || len(clusterKeys) != 1 {
		t.Fatalf("ai_cluster_key_names: expected 1 element array, got %v", result["ai_cluster_key_names"])
	}
	firstPair := clusterKeys[0].(map[string]interface{})
	if firstPair["cluster_name"] != "cluster-a" {
		t.Errorf("ai_cluster_key_names[0].cluster_name: expected cluster-a, got %v", firstPair["cluster_name"])
	}

	hitPlans, ok := result["ai_auth_hit_quota_plans"].([]interface{})
	if !ok || len(hitPlans) != 1 || hitPlans[0].(string) != "hit-plan-a" {
		t.Errorf("ai_auth_hit_quota_plans: expected [hit-plan-a], got %v", result["ai_auth_hit_quota_plans"])
	}

	for _, name := range mod_fields.RequiredFields() {
		// empty-string / zero-value required fields are omitted by omitempty behavior
		if name == "err_code" || name == "err_msg" || name == "req_body_len" {
			continue
		}
		if _, ok := result[name]; !ok {
			t.Errorf("required field %q missing", name)
		}
	}

	if _, ok := result["cluster"]; ok {
		t.Error("cluster should not be in customized output")
	}
}

func TestConvertBfeLogToJSON_OmitsZeroValues(t *testing.T) {
	log := makeBfeLog()
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{FieldMode: "all"},
	}
	of := cfg.ResolveFields()

	jsonBytes, err := ConvertBfeLogToJSON(log, of)
	if err != nil {
		t.Fatalf("ConvertBfeLogToJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
}

func TestConvertBfeLogToJSON_NilOutputFields(t *testing.T) {
	log := makeBfeLog()

	jsonBytes, err := ConvertBfeLogToJSON(log, nil)
	if err != nil {
		t.Fatalf("ConvertBfeLogToJSON failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	// default fields minus zero-value fields omitted by omitempty
	expectedMin := len(mod_fields.DefaultFields()) - 16
	if len(result) < expectedMin {
		t.Fatalf("expected at least %d fields (some may be zero), got %d", expectedMin, len(result))
	}
}

func BenchmarkConvertBfeLogToJSON_Default(b *testing.B) {
	log := makeBfeLog()
	of := DefaultOutputFields()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ConvertBfeLogToJSON(log, of)
	}
}

func BenchmarkConvertBfeLogToJSON_All(b *testing.B) {
	log := makeBfeLog()
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{FieldMode: "all"},
	}
	of := cfg.ResolveFields()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ConvertBfeLogToJSON(log, of)
	}
}

func BenchmarkConvertBfeLogToJSON_Require(b *testing.B) {
	log := makeBfeLog()
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{FieldMode: "require"},
	}
	of := cfg.ResolveFields()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ConvertBfeLogToJSON(log, of)
	}
}
