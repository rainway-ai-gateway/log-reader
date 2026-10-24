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

package common

import (
	"os"
	"testing"

	"github.com/bfenetworks/bfe-access-pb/b2log"
	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"google.golang.org/protobuf/proto"
)

// LogGenerator writes protobuf access logs to a file in b2log format.
type LogGenerator struct {
	file *os.File
}

// NewLogGenerator creates a generator that appends to the given path.
func NewLogGenerator(t *testing.T, path string) *LogGenerator {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open log file failed: %v", err)
	}
	return &LogGenerator{file: f}
}

// Close closes the underlying file.
func (g *LogGenerator) Close() error {
	return g.file.Close()
}

// WriteBfeLog marshals a BfeLog and appends it to the file.
func (g *LogGenerator) WriteBfeLog(log *bfe_access_pb.BfeLog) error {
	msize := proto.Size(log)
	buf := make([]byte, b2log.HEADER_SIZE+msize)
	if err := b2log.HeaderWrite(buf, msize); err != nil {
		return err
	}
	raw, err := proto.Marshal(log)
	if err != nil {
		return err
	}
	copy(buf[b2log.HEADER_SIZE:], raw)
	_, err = g.file.Write(buf)
	return err
}

// MustWriteBfeLog is a convenience wrapper that fails the test on error.
func (g *LogGenerator) MustWriteBfeLog(t *testing.T, log *bfe_access_pb.BfeLog) {
	t.Helper()
	if err := g.WriteBfeLog(log); err != nil {
		t.Fatalf("write bfe log failed: %v", err)
	}
}

// MakeRequestLog builds a BfeLog whose request log contains every field enabled
// in conf/mod_kafka/kafka_config.data (customized mode). All non-zero values are
// deterministic so integration tests can verify the JSON conversion field by field.
func MakeRequestLog(logid uint64, product bfe_access_pb.ProductID, host, uri, model string) *bfe_access_pb.BfeLog {
	logType := bfe_access_pb.BfeLogType_Request
	clientIP := uint32(0x0A000001)   // 10.0.0.1
	bfeIP := uint32(0x0A000064)      // 10.0.0.100
	sockSrcIP := uint32(0x0A000001)  // 10.0.0.1
	vip := uint32(0xC0A80101)        // 192.168.1.1
	backendIP := uint32(0x0A0000C8)  // 10.0.0.200
	isTrustSrcIP := true
	clientNetwork := bfe_access_pb.NetType_Ipv4
	return &bfe_access_pb.BfeLog{
		Logid:     &logid,
		Timestamp: uint64Ptr(1782353290),
		Product:   &product,
		LogType:   &logType,
		LogTag:    strPtr("integration-test"),
		RequestLog: &bfe_access_pb.RequestLog{
			// Required / connection fields.
			ErrCode:       strPtr(""),
			ErrMsg:        strPtr("ok"),
			ReqHeaderLen:  uint32Ptr(189),
			ReqBodyLen:    uint32Ptr(256),
			SessionId:     uint64Ptr(123456789),
			ReqNum:        uint32Ptr(1),
			ClientIp:      &clientIP,
			ClientNetwork: &clientNetwork,
			AddrInfo: &bfe_access_pb.ConnAddrInfo{
				BfeIp:        &bfeIP,
				SockSrcIp:    &sockSrcIP,
				IsTrustSrcIp: &isTrustSrcIP,
				Vip:          &vip,
			},

			// Request line / header fields.
			Proto:            strPtr("HTTP/1.1"),
			HeaderHost:       strPtr(host),
			OriginUri:        strPtr(uri),
			FinalUri:         strPtr(uri + "/final"),
			Method:           strPtr("POST"),
			ContentType:      strPtr("application/json"),
			Referrer:         strPtr("https://example.org"),
			XForwardFor:      strPtr("10.0.0.2, 10.0.0.3"),
			AcceptLanguage:   strPtr("en-US,zh-CN"),
			Authorization:    strPtr("Bearer sk-test"),
			UserAgent:        strPtr("log-reader-test-agent/1.0"),
			TransferEncoding: strPtr("chunked"),
			Delegation:       strPtr("delegation.example.org"),
			Uid:              strPtr("user-123"),
			Cookie:           strPtr("session=abc123"),
			ReqHeaders: []*bfe_access_pb.HttpHeader{
				{Key: strPtr("X-Request-Id"), Value: strPtr("req-123")},
				{Key: strPtr("X-Test-Header"), Value: strPtr("test-value")},
			},

			// Routing fields.
			Product:      strPtr(product.String()),
			Cluster:      strPtr("cluster-A"),
			SubCluster:   strPtr("subcluster-A1"),
			BackendInfo:  &bfe_access_pb.InstanceInfo{IpAddr: &backendIP, Port: uint32Ptr(8080)},
			BackendRetry: uint32Ptr(1),

			// Response fields.
			ResStatusCode:       uint32Ptr(200),
			ResHeaderLen:        uint32Ptr(154),
			ResBodyLen:          uint32Ptr(459),
			ResContentType:      strPtr("application/json"),
			ResLocation:         strPtr("https://example.org/redirect"),
			ResTransferEncoding: strPtr("chunked"),
			ResHeaders: []*bfe_access_pb.HttpHeader{
				{Key: strPtr("Content-Type"), Value: strPtr("application/json")},
				{Key: strPtr("X-Response-Id"), Value: strPtr("res-456")},
			},

			// Timing fields.
			AllTime:            uint32Ptr(11),
			ReadClientTime:     uint32Ptr(2),
			ClusterServeTime:   uint32Ptr(5),
			BackendServeTime:   uint32Ptr(4),
			WriteClientTime:    uint32Ptr(1),
			SessionOffsetTime:  uint32Ptr(100),
			ConnectBackendTime: uint32Ptr(1),
			ProxyDelayTime:     uint32Ptr(3),

			// AI observability fields.
			AiApikeyId:       strPtr("key-id-123"),
			AiApikeytags: []*bfe_access_pb.ApikeyTag{
				{Tagname: strPtr("dep"), Tagvalue: strPtr("ops"), Taglevel: int32Ptr(1)},
				{Tagname: strPtr("team"), Tagvalue: strPtr("bfe"), Taglevel: int32Ptr(2)},
			},
			AiRequestedModel: strPtr(model),
			AiTargetModel:    strPtr(model + "-mapped"),
			AiStream:         boolPtr(true),
			AiInputTokens:    int64Ptr(1000),
			AiOutputTokens:   int64Ptr(200),
			AiTotalTokens:        int64Ptr(1200),
			AiCacheReadTokens:    int64Ptr(500),
			AiCacheWriteTokens:   int64Ptr(100),
			AiAudioInputTokens:   int64Ptr(80),
			AiAudioOutputTokens:  int64Ptr(20),
			AiImageCount:         int64Ptr(1),
			AiImageInputTokens:   int64Ptr(200),
			AiVideoCount:         int64Ptr(3),
			AiTtftUs:             int64Ptr(50000),
			AiTpotUs:         int64Ptr(2500),
			AiRateLimitHits: []*bfe_access_pb.RateLimitHit{
				{
					RateLimitPolicyId: strPtr("rlp-0001"),
					RateLimitType:     strPtr("tpm"),
					RuleNames:         []string{"win1m", "win5m"},
				},
			},
			AiAuthRejectReason:       strPtr("quota_exceeded"),
			AiAuthRejectQuotaPlans:   []string{"plan-A", "plan-B"},

			// AI observability new fields (v0.2.0).
			AiProvider:      strPtr("openai"),
			AiProtocol:      strPtr("openai"),
			AiMode:          strPtr("chat"),
			AiRetryCount:    uint32Ptr(1),
			AiCostValue:     int64Ptr(5000),
			AiCostCurrency:  strPtr("USD"),
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
			AiAuthHitQuotaPlans: []string{"hit-plan-A", "hit-plan-B"},

			// AI cache / traffic mirroring / AI intent fields (v0.3.7/3.8/3.9).
			AiCacheStatus:            strPtr("hit"),
			MirrorHit:                boolPtr(true),
			MirrorCluster:            strPtr("mirror-cluster-A"),
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

func boolPtr(v bool) *bool       { return &v }
func strPtr(s string) *string    { return &s }
func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }
func int64Ptr(v int64) *int64    { return &v }
func int32Ptr(v int32) *int32    { return &v }
func float64Ptr(v float64) *float64 {
	return &v
}
