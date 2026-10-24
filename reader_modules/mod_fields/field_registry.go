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
	"fmt"
	"sync"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/reader_util"
)

// ApikeyTagJSON API Key tag
type ApikeyTagJSON struct {
	Tagname  string `json:"tagname"`
	Tagvalue string `json:"tagvalue"`
}

// AiRateLimitHitJSON AI rate limit hit record
type AiRateLimitHitJSON struct {
	RateLimitPolicyID string   `json:"rate_limit_policy_id"`
	RateLimitType     string   `json:"rate_limit_type"`
	RuleNames         []string `json:"rule_names"`
}

// AIRouteRuleHitJSON AI routing rule hit record
type AIRouteRuleHitJSON struct {
	RuleOwner     string `json:"rule_owner"`
	RuleOwnerType string `json:"rule_owner_type"`
	RuleName      string `json:"rule_name"`
}

// ClusterKeyNameJSON cluster and key name pair
type ClusterKeyNameJSON struct {
	ClusterName string `json:"cluster_name"`
	KeyName     string `json:"key_name"`
}

// HttpHeaderJSON HTTP request or response header
type HttpHeaderJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// FieldDef describes a JSON output field
type FieldDef struct {
	Name     string // JSON field name
	Type     string // field type: "string", "uint64", "uint32", "int64", "bool", "object", "[]object", "[]string"
	Required bool   // whether this field is always output
	Default  bool   // whether this field is in the default set
}

// FieldExtractor extracts a field value from BfeLog
type FieldExtractor func(bfeLog *bfe_access_pb.BfeLog) interface{}

type fieldEntry struct {
	def     FieldDef
	extract FieldExtractor
	isZero  func(v interface{}) bool
}

// FieldRegistry manages all registered JSON output fields
type FieldRegistry struct {
	mu       sync.RWMutex
	fields   map[string]*fieldEntry
	allNames []string
}

var globalRegistry = &FieldRegistry{
	fields: make(map[string]*fieldEntry),
}

func init() {
	registerAllFields()
}

func registerField(name, typ string, required, def bool, extract FieldExtractor, isZero func(interface{}) bool) {
	globalRegistry.fields[name] = &fieldEntry{
		def: FieldDef{
			Name:     name,
			Type:     typ,
			Required: required,
			Default:  def,
		},
		extract: extract,
		isZero:  isZero,
	}
	globalRegistry.allNames = append(globalRegistry.allNames, name)
}

// AllFields returns all registered field definitions
func AllFields() []FieldDef {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	result := make([]FieldDef, 0, len(globalRegistry.fields))
	for _, name := range globalRegistry.allNames {
		if entry, ok := globalRegistry.fields[name]; ok {
			result = append(result, entry.def)
		}
	}
	return result
}

// DefaultFields returns the default field name list
func DefaultFields() []string {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	result := make([]string, 0)
	for _, name := range globalRegistry.allNames {
		if entry, ok := globalRegistry.fields[name]; ok && entry.def.Default {
			result = append(result, name)
		}
	}
	return result
}

// RequiredFields returns the required field name list
func RequiredFields() []string {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	result := make([]string, 0)
	for _, name := range globalRegistry.allNames {
		if entry, ok := globalRegistry.fields[name]; ok && entry.def.Required {
			result = append(result, name)
		}
	}
	return result
}

// IsValidField checks if a field name is registered
func IsValidField(name string) bool {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	_, ok := globalRegistry.fields[name]
	return ok
}

// Extract extracts a field value from BfeLog, returns (value, isZero)
func Extract(fieldName string, bfeLog *bfe_access_pb.BfeLog) (interface{}, bool) {
	globalRegistry.mu.RLock()
	entry, ok := globalRegistry.fields[fieldName]
	globalRegistry.mu.RUnlock()

	if !ok {
		return nil, true
	}

	v := entry.extract(bfeLog)
	return v, entry.isZero(v)
}

func ipUint32ToString(ip uint32) string {
	if ip == 0 {
		return ""
	}
	return fmt.Sprintf("%d.%d.%d.%d",
		(ip>>24)&0xFF,
		(ip>>16)&0xFF,
		(ip>>8)&0xFF,
		ip&0xFF,
	)
}

func isZeroString(v interface{}) bool {
	s, ok := v.(string)
	return !ok || s == ""
}

func isZeroUint64(v interface{}) bool {
	n, ok := v.(uint64)
	return !ok || n == 0
}

func isZeroUint32(v interface{}) bool {
	n, ok := v.(uint32)
	return !ok || n == 0
}

func isZeroInt64(v interface{}) bool {
	n, ok := v.(int64)
	return !ok || n == 0
}

func isZeroFloat64(v interface{}) bool {
	n, ok := v.(float64)
	return !ok || n == 0
}

func isZeroBool(v interface{}) bool {
	b, ok := v.(bool)
	return !ok || !b
}

func isZeroObject(v interface{}) bool {
	return v == nil
}

func isZeroSlice(v interface{}) bool {
	if v == nil {
		return true
	}
	switch s := v.(type) {
	case []AiRateLimitHitJSON:
		return len(s) == 0
	case []AIRouteRuleHitJSON:
		return len(s) == 0
	case []ClusterKeyNameJSON:
		return len(s) == 0
	case []HttpHeaderJSON:
		return len(s) == 0
	case []string:
		return len(s) == 0
	}
	return true
}

func registerAllFields() {
	// === BfeLog top-level fields ===
	registerField("logid", "uint64", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} { return bfeLog.GetLogid() },
		isZeroUint64,
	)
	registerField("timestamp", "uint64", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} { return bfeLog.GetTimestamp() },
		isZeroUint64,
	)
	registerField("product", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			reqLog := bfeLog.GetRequestLog()
			if reqLog != nil {
				if p := reqLog.GetProduct(); p != "" {
					return p
				}
			}
			return bfeLog.GetProduct().String()
		},
		isZeroString,
	)
	registerField("hostid", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} { return reader_util.GetHostId() },
		isZeroString,
	)
	registerField("log_tag", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} { return bfeLog.GetLogTag() },
		isZeroString,
	)

	// === Connection / Client fields ===
	registerField("client_ip", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			reqLog := bfeLog.GetRequestLog()
			if reqLog == nil {
				return ""
			}
			if reqLog.GetClientNetwork() == bfe_access_pb.NetType_Ipv6 {
				return reqLog.GetClientIp6()
			}
			return ipUint32ToString(reqLog.GetClientIp())
		},
		isZeroString,
	)
	registerField("client_network", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if reqLog.ClientNetwork != nil {
					return reqLog.GetClientNetwork().String()
				}
			}
			return ""
		},
		isZeroString,
	)
	registerField("req_num", "uint32", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetReqNum()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("session_id", "uint64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetSessionId()
			}
			return uint64(0)
		},
		isZeroUint64,
	)

	// === Request basic fields ===
	registerField("err_code", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetErrCode()
			}
			return ""
		},
		isZeroString,
	)
	registerField("err_msg", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetErrMsg()
			}
			return ""
		},
		isZeroString,
	)
	registerField("req_header_len", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetReqHeaderLen()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("req_body_len", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetReqBodyLen()
			}
			return uint32(0)
		},
		isZeroUint32,
	)

	// === Request header fields ===
	registerField("proto", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetProto()
			}
			return ""
		},
		isZeroString,
	)
	registerField("header_host", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetHeaderHost()
			}
			return ""
		},
		isZeroString,
	)
	registerField("origin_uri", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetOriginUri()
			}
			return ""
		},
		isZeroString,
	)
	registerField("final_uri", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetFinalUri()
			}
			return ""
		},
		isZeroString,
	)
	registerField("method", "string", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetMethod()
			}
			return ""
		},
		isZeroString,
	)
	registerField("content_type", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetContentType()
			}
			return ""
		},
		isZeroString,
	)
	registerField("referrer", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetReferrer()
			}
			return ""
		},
		isZeroString,
	)
	registerField("user_agent", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetUserAgent()
			}
			return ""
		},
		isZeroString,
	)
	registerField("x_forward_for", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetXForwardFor()
			}
			return ""
		},
		isZeroString,
	)
	registerField("accept_language", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAcceptLanguage()
			}
			return ""
		},
		isZeroString,
	)
	registerField("authorization", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAuthorization()
			}
			return ""
		},
		isZeroString,
	)
	registerField("transfer_encoding", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetTransferEncoding()
			}
			return ""
		},
		isZeroString,
	)
	registerField("delegation", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetDelegation()
			}
			return ""
		},
		isZeroString,
	)
	registerField("uid", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetUid()
			}
			return ""
		},
		isZeroString,
	)

	// === Cookie fields ===
	registerField("cookie", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetCookie()
			}
			return ""
		},
		isZeroString,
	)

	// === Request headers list ===
	registerField("req_headers", "[]object", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				headers := reqLog.GetReqHeaders()
				result := make([]HttpHeaderJSON, 0, len(headers))
				for _, h := range headers {
					result = append(result, HttpHeaderJSON{
						Key:   h.GetKey(),
						Value: h.GetValue(),
					})
				}
				return result
			}
			return []HttpHeaderJSON{}
		},
		isZeroSlice,
	)

	// === Routing fields ===
	registerField("cluster", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetCluster()
			}
			return ""
		},
		isZeroString,
	)
	registerField("sub_cluster", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetSubCluster()
			}
			return ""
		},
		isZeroString,
	)
	registerField("backend_info", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if bi := reqLog.GetBackendInfo(); bi != nil {
					return fmt.Sprintf("%s:%d", ipUint32ToString(bi.GetIpAddr()), bi.GetPort())
				}
			}
			return ""
		},
		isZeroString,
	)
	registerField("backend_retry", "uint32", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetBackendRetry()
			}
			return uint32(0)
		},
		isZeroUint32,
	)

	// === Response fields ===
	registerField("res_status_code", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetResStatusCode()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("res_header_len", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetResHeaderLen()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("res_body_len", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetResBodyLen()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("res_content_type", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetResContentType()
			}
			return ""
		},
		isZeroString,
	)
	registerField("res_location", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetResLocation()
			}
			return ""
		},
		isZeroString,
	)
	registerField("res_transfer_encoding", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetResTransferEncoding()
			}
			return ""
		},
		isZeroString,
	)

	// === Response headers list ===
	registerField("res_headers", "[]object", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				headers := reqLog.GetResHeaders()
				result := make([]HttpHeaderJSON, 0, len(headers))
				for _, h := range headers {
					result = append(result, HttpHeaderJSON{
						Key:   h.GetKey(),
						Value: h.GetValue(),
					})
				}
				return result
			}
			return []HttpHeaderJSON{}
		},
		isZeroSlice,
	)

	// === Timing fields ===
	registerField("all_time", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAllTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("read_client_time", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetReadClientTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("cluster_serve_time", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetClusterServeTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("backend_serve_time", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetBackendServeTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("write_client_time", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetWriteClientTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("session_offset_time", "uint32", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetSessionOffsetTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("connect_backend_time", "uint32", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetConnectBackendTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("proxy_delay_time", "uint32", true, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetProxyDelayTime()
			}
			return uint32(0)
		},
		isZeroUint32,
	)

	// === AI observability fields ===
	registerField("ai_apikey_id", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiApikeyId()
			}
			return ""
		},
		isZeroString,
	)

	registerField("ai_apikeytags", "object", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			result := map[string]interface{}{
				"level1": map[string]interface{}{},
				"level2": map[string]interface{}{},
				"level3": map[string]interface{}{},
				"level4": map[string]interface{}{},
				"level5": map[string]interface{}{},
			}
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				for _, t := range reqLog.GetAiApikeytags() {
					lvl := t.GetTaglevel()
					if lvl < 1 || lvl > 5 {
						continue
					}
					key := fmt.Sprintf("level%d", lvl)
					result[key] = ApikeyTagJSON{
						Tagname:  t.GetTagname(),
						Tagvalue: t.GetTagvalue(),
					}
				}
			}
			return result
		},
		isZeroObject,
	)

	registerField("ai_requested_model", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiRequestedModel()
			}
			return ""
		},
		isZeroString,
	)

	registerField("ai_target_model", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiTargetModel()
			}
			return ""
		},
		isZeroString,
	)

	registerField("ai_stream", "bool", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiStream()
			}
			return false
		},
		isZeroBool,
	)

	registerField("ai_input_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiInputTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_output_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiOutputTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_total_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiTotalTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_cache_read_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiCacheReadTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_cache_write_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiCacheWriteTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_audio_input_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiAudioInputTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_audio_output_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiAudioOutputTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_image_count", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiImageCount()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_image_input_tokens", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiImageInputTokens()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_video_count", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiVideoCount()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_ttft_us", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiTtftUs()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_tpot_us", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiTpotUs()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_rate_limit_hits", "[]object", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				hits := reqLog.GetAiRateLimitHits()
				result := make([]AiRateLimitHitJSON, 0, len(hits))
				for _, h := range hits {
					result = append(result, AiRateLimitHitJSON{
						RateLimitPolicyID: h.GetRateLimitPolicyId(),
						RateLimitType:     h.GetRateLimitType(),
						RuleNames:         h.GetRuleNames(),
					})
				}
				return result
			}
			return []AiRateLimitHitJSON{}
		},
		isZeroSlice,
	)
	registerField("ai_auth_reject_reason", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiAuthRejectReason()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_auth_reject_quota_plans", "[]string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				qls := reqLog.GetAiAuthRejectQuotaPlans()
				return qls
			}
			return []string{}
		},
		isZeroSlice,
	)

	// === AI observability new fields (v0.2.0) ===
	registerField("ai_provider", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiProvider()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_protocol", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiProtocol()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_retry_count", "uint32", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiRetryCount()
			}
			return uint32(0)
		},
		isZeroUint32,
	)
	registerField("ai_mode", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiMode()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_cost_value", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiCostValue()
			}
			return int64(0)
		},
		isZeroInt64,
	)
	registerField("ai_cost_currency", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiCostCurrency()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_route_rule_hits", "[]object", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				hits := reqLog.GetAiRouteRuleHits()
				result := make([]AIRouteRuleHitJSON, 0, len(hits))
				for _, h := range hits {
					result = append(result, AIRouteRuleHitJSON{
						RuleOwner:     h.GetRuleOwner(),
						RuleOwnerType: h.GetRuleOwnerType(),
						RuleName:      h.GetRuleName(),
					})
				}
				return result
			}
			return []AIRouteRuleHitJSON{}
		},
		isZeroSlice,
	)
	registerField("ai_cluster_key_names", "[]object", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				pairs := reqLog.GetAiClusterKeyNames()
				result := make([]ClusterKeyNameJSON, 0, len(pairs))
				for _, p := range pairs {
					result = append(result, ClusterKeyNameJSON{
						ClusterName: p.GetClusterName(),
						KeyName:     p.GetKeyName(),
					})
				}
				return result
			}
			return []ClusterKeyNameJSON{}
		},
		isZeroSlice,
	)
	registerField("ai_auth_hit_quota_plans", "[]string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiAuthHitQuotaPlans()
			}
			return []string{}
		},
		isZeroSlice,
	)

	// === AI cache / traffic mirroring / AI intent fields (v0.3.7/3.8/3.9) ===
	registerField("ai_cache_status", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiCacheStatus()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_intent_question", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiIntentQuestion()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_intent_answer", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiIntentAnswer()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_intent_confidence", "float64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			// proto optional 指针判空：未设置返回 nil（JSON null），
			// 与 mod_log_mysql kindOptionalNum 的 NULL=未求值语义对齐。
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil && reqLog.AiIntentConfidence != nil {
				return *reqLog.AiIntentConfidence
			}
			return nil
		},
		isZeroFloat64,
	)
	registerField("ai_intent_source", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiIntentSource()
			}
			return ""
		},
		isZeroString,
	)
	registerField("ai_intent_latency_us", "int64", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil && reqLog.AiIntentLatencyUs != nil {
				return *reqLog.AiIntentLatencyUs
			}
			return nil
		},
		isZeroInt64,
	)
	registerField("ai_intent_cache_hit", "bool", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil && reqLog.AiIntentCacheHit != nil {
				return *reqLog.AiIntentCacheHit
			}
			return nil
		},
		isZeroBool,
	)
	registerField("ai_intent_questions_version", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetAiIntentQuestionsVersion()
			}
			return ""
		},
		isZeroString,
	)
	registerField("mirror_hit", "bool", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetMirrorHit()
			}
			return false
		},
		isZeroBool,
	)
	registerField("mirror_cluster", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				return reqLog.GetMirrorCluster()
			}
			return ""
		},
		isZeroString,
	)

	// === Address info fields (flattened from ConnAddrInfo) ===
	registerField("bfe_ip", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if addr := reqLog.GetAddrInfo(); addr != nil {
					return ipUint32ToString(addr.GetBfeIp())
				}
			}
			return ""
		},
		isZeroString,
	)
	registerField("sock_src_ip", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if addr := reqLog.GetAddrInfo(); addr != nil {
					return ipUint32ToString(addr.GetSockSrcIp())
				}
			}
			return ""
		},
		isZeroString,
	)
	registerField("is_trust_src_ip", "bool", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if addr := reqLog.GetAddrInfo(); addr != nil {
					return addr.GetIsTrustSrcIp()
				}
			}
			return false
		},
		isZeroBool,
	)
	registerField("vip", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if addr := reqLog.GetAddrInfo(); addr != nil {
					return ipUint32ToString(addr.GetVip())
				}
			}
			return ""
		},
		isZeroString,
	)
	registerField("vip6", "string", false, true,
		func(bfeLog *bfe_access_pb.BfeLog) interface{} {
			if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
				if addr := reqLog.GetAddrInfo(); addr != nil {
					return addr.GetVip6()
				}
			}
			return ""
		},
		isZeroString,
	)
}
