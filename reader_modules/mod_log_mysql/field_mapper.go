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
	"encoding/json"
	"fmt"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/reader_modules/mod_fields"
)

// columnKind 列取值方式
type columnKind int

const (
	kindScalar     columnKind = iota // mod_fields 抽取直通；字符串零值 -> NULL
	kindNotNullStr                   // NOT NULL 字符串列直通；空串原样写入 ""（不映射 NULL）
	kindBoolTiny                     // 布尔抽取值 -> TINYINT（false->0, true->1）
	kindOptionalNum                  // proto optional 数值列：指针 nil -> NULL（未设置），非 nil -> 实际值
	kindJSON                         // 结构化抽取值 json.Marshal 后写入；空 -> NULL
	kindLogTime                      // timestamp（Unix 秒）-> time.Time（本地时区）
	kindTagName                      // ai_apikeytags 打平：标签名
	kindTagValue                     // ai_apikeytags 打平：标签值
)

// columnDef 单列定义。本表是写入侧 99 列清单与列序的唯一权威，
// 列序与 ai-gateway-api 仓库 db_ddl_report_mysql.sql 的 CREATE TABLE 列序一致。
type columnDef struct {
	name  string // 表列名
	field string // mod_fields 抽取字段名（特殊列为空）
	kind  columnKind
	level int   // kindTagName/kindTagValue 的标签层级
	max   int64 // kindScalar 数值列写入上限（按 DDL 列类型）；0 = 无约束
}

var columnDefs = []columnDef{
	// === 主键/基础列 ===
	{name: "hostid", field: "hostid", kind: kindScalar},
	{name: "log_time", kind: kindLogTime},
	{name: "ai_apikey_id", field: "ai_apikey_id", kind: kindScalar},
	{name: "ai_requested_model", field: "ai_requested_model", kind: kindScalar},
	{name: "logid", field: "logid", kind: kindScalar},
	{name: "product", field: "product", kind: kindScalar},
	{name: "log_tag", field: "log_tag", kind: kindScalar},

	// === 客户端连接列 ===
	{name: "client_ip", field: "client_ip", kind: kindScalar},
	{name: "client_network", field: "client_network", kind: kindScalar},
	{name: "is_trust_src_ip", field: "is_trust_src_ip", kind: kindBoolTiny},
	{name: "req_num", field: "req_num", kind: kindScalar, max: 2147483647},
	{name: "session_id", field: "session_id", kind: kindScalar},
	{name: "bfe_ip", field: "bfe_ip", kind: kindScalar},
	{name: "sock_src_ip", field: "sock_src_ip", kind: kindScalar},
	{name: "vip", field: "vip", kind: kindScalar},
	{name: "vip6", field: "vip6", kind: kindScalar},

	// === 错误列 ===
	{name: "err_code", field: "err_code", kind: kindScalar},
	{name: "err_msg", field: "err_msg", kind: kindScalar},

	// === 请求列 ===
	{name: "proto", field: "proto", kind: kindScalar},
	{name: "header_host", field: "header_host", kind: kindScalar},
	{name: "origin_uri", field: "origin_uri", kind: kindScalar},
	{name: "final_uri", field: "final_uri", kind: kindScalar},
	{name: "method", field: "method", kind: kindScalar},
	{name: "content_type", field: "content_type", kind: kindScalar},
	{name: "x_forward_for", field: "x_forward_for", kind: kindScalar},
	{name: "accept_language", field: "accept_language", kind: kindScalar},
	{name: "authorization", field: "authorization", kind: kindScalar},
	{name: "transfer_encoding", field: "transfer_encoding", kind: kindScalar},
	{name: "referrer", field: "referrer", kind: kindScalar},
	{name: "user_agent", field: "user_agent", kind: kindScalar},
	{name: "delegation", field: "delegation", kind: kindScalar},
	{name: "uid", field: "uid", kind: kindScalar},
	{name: "cookie", field: "cookie", kind: kindScalar},
	{name: "req_headers", field: "req_headers", kind: kindJSON},
	{name: "req_header_len", field: "req_header_len", kind: kindScalar, max: 2147483647},
	{name: "req_body_len", field: "req_body_len", kind: kindScalar, max: 2147483647},

	// === 路由列 ===
	{name: "cluster", field: "cluster", kind: kindScalar},
	{name: "sub_cluster", field: "sub_cluster", kind: kindScalar},
	{name: "backend_info", field: "backend_info", kind: kindScalar},
	{name: "backend_retry", field: "backend_retry", kind: kindScalar, max: 127},

	// === 响应列 ===
	{name: "res_status_code", field: "res_status_code", kind: kindScalar, max: 32767},
	{name: "res_header_len", field: "res_header_len", kind: kindScalar, max: 2147483647},
	{name: "res_body_len", field: "res_body_len", kind: kindScalar, max: 2147483647},
	{name: "res_content_type", field: "res_content_type", kind: kindScalar},
	{name: "res_location", field: "res_location", kind: kindScalar},
	{name: "res_transfer_encoding", field: "res_transfer_encoding", kind: kindScalar},
	{name: "res_headers", field: "res_headers", kind: kindJSON},

	// === 耗时列（毫秒） ===
	{name: "all_time", field: "all_time", kind: kindScalar, max: 2147483647},
	{name: "read_client_time", field: "read_client_time", kind: kindScalar, max: 2147483647},
	{name: "cluster_serve_time", field: "cluster_serve_time", kind: kindScalar, max: 2147483647},
	{name: "backend_serve_time", field: "backend_serve_time", kind: kindScalar, max: 2147483647},
	{name: "write_client_time", field: "write_client_time", kind: kindScalar, max: 2147483647},
	{name: "connect_backend_time", field: "connect_backend_time", kind: kindScalar, max: 2147483647},
	{name: "proxy_delay_time", field: "proxy_delay_time", kind: kindScalar, max: 2147483647},
	{name: "session_offset_time", field: "session_offset_time", kind: kindScalar, max: 2147483647},

	// === API Key 标签打平列（写时从 ai_apikeytags 打平） ===
	{name: "level1Name", kind: kindTagName, level: 1},
	{name: "level1", kind: kindTagValue, level: 1},
	{name: "level2Name", kind: kindTagName, level: 2},
	{name: "level2", kind: kindTagValue, level: 2},
	{name: "level3Name", kind: kindTagName, level: 3},
	{name: "level3", kind: kindTagValue, level: 3},
	{name: "level4Name", kind: kindTagName, level: 4},
	{name: "level4", kind: kindTagValue, level: 4},
	{name: "level5Name", kind: kindTagName, level: 5},
	{name: "level5", kind: kindTagValue, level: 5},

	// === AI 可观测列（标量） ===
	{name: "ai_target_model", field: "ai_target_model", kind: kindScalar},
	{name: "ai_stream", field: "ai_stream", kind: kindBoolTiny},
	{name: "ai_input_tokens", field: "ai_input_tokens", kind: kindScalar},
	{name: "ai_output_tokens", field: "ai_output_tokens", kind: kindScalar},
	{name: "ai_total_tokens", field: "ai_total_tokens", kind: kindScalar},
	{name: "ai_cache_read_tokens", field: "ai_cache_read_tokens", kind: kindScalar},
	{name: "ai_cache_write_tokens", field: "ai_cache_write_tokens", kind: kindScalar},
	{name: "ai_audio_input_tokens", field: "ai_audio_input_tokens", kind: kindScalar},
	{name: "ai_audio_output_tokens", field: "ai_audio_output_tokens", kind: kindScalar},
	{name: "ai_image_count", field: "ai_image_count", kind: kindScalar},
	{name: "ai_ttft_us", field: "ai_ttft_us", kind: kindScalar},
	{name: "ai_tpot_us", field: "ai_tpot_us", kind: kindScalar},
	{name: "ai_provider", field: "ai_provider", kind: kindScalar},
	{name: "ai_protocol", field: "ai_protocol", kind: kindScalar},
	{name: "ai_mode", field: "ai_mode", kind: kindScalar},
	{name: "ai_retry_count", field: "ai_retry_count", kind: kindScalar, max: 2147483647},
	{name: "ai_cost_value", field: "ai_cost_value", kind: kindScalar},
	{name: "ai_cost_currency", field: "ai_cost_currency", kind: kindScalar},

	// === AI 可观测列（JSON） ===
	{name: "ai_route_rule_hits", field: "ai_route_rule_hits", kind: kindJSON},
	{name: "ai_cluster_key_names", field: "ai_cluster_key_names", kind: kindJSON},
	{name: "ai_rate_limit_hits", field: "ai_rate_limit_hits", kind: kindJSON},
	{name: "ai_auth_reject_reason", field: "ai_auth_reject_reason", kind: kindScalar},
	{name: "ai_auth_reject_quota_plans", field: "ai_auth_reject_quota_plans", kind: kindJSON},
	{name: "ai_auth_hit_quota_plans", field: "ai_auth_hit_quota_plans", kind: kindJSON},

	// === AI 缓存/镜像/意图列（2026-09-27 加列，proto v0.3.7/3.8/3.9） ===
	// 列序与 ai-gateway-api db_ddl_report_mysql.sql 明细表新列顺序逐字一致；
	// ai_cache_key、mirror_status~mirror_error 不进报表库（设计文档 2026-09-27）。
	{name: "ai_cache_status", field: "ai_cache_status", kind: kindNotNullStr},
	{name: "mirror_hit", field: "mirror_hit", kind: kindBoolTiny},
	{name: "mirror_cluster", field: "mirror_cluster", kind: kindNotNullStr},
	{name: "ai_intent_question", field: "ai_intent_question", kind: kindNotNullStr},
	{name: "ai_intent_answer", field: "ai_intent_answer", kind: kindNotNullStr},
	{name: "ai_intent_confidence", field: "ai_intent_confidence", kind: kindOptionalNum},
	{name: "ai_intent_source", field: "ai_intent_source", kind: kindNotNullStr},
	{name: "ai_intent_latency_us", field: "ai_intent_latency_us", kind: kindOptionalNum},
	{name: "ai_intent_cache_hit", field: "ai_intent_cache_hit", kind: kindOptionalNum},
	{name: "ai_intent_questions_version", field: "ai_intent_questions_version", kind: kindNotNullStr},
}

// FieldMapper 将 BfeLog 组装为固定列序的数据行
type FieldMapper struct {
	// onNormalized 在数值列值超界被 clamp 到列上限时回调（参数为列名）；
	// 可为 nil（静默归一化）
	onNormalized func(col string)
}

// NewFieldMapper 创建 FieldMapper
func NewFieldMapper() *FieldMapper {
	return &FieldMapper{}
}

// SetOnNormalized 注入归一化事件回调（模块组装时注入计数与限频日志）
func (m *FieldMapper) SetOnNormalized(f func(col string)) {
	m.onNormalized = f
}

// clampNumeric 将 kindScalar 抽取值收敛到列写入上限：仅处理整数数值类型
// （uint32/uint64/int64 等 mod_fields 可能返回的类型），超界写 max；
// 非数值类型原样返回。max <= 0 表示该列无范围约束。
func clampNumeric(v interface{}, max int64) (interface{}, bool) {
	if max <= 0 {
		return v, false
	}
	switch n := v.(type) {
	case uint32:
		if uint64(n) > uint64(max) {
			return max, true
		}
	case uint64:
		if n > uint64(max) {
			return max, true
		}
	case int64:
		if n > max {
			return max, true
		}
	case int32:
		if int64(n) > max {
			return max, true
		}
	case int:
		if int64(n) > max {
			return max, true
		}
	}
	return v, false
}

// Columns 返回表列名清单（按写入列序），供测试与 RecordWriter 共用
func Columns() []string {
	cols := make([]string, 0, len(columnDefs))
	for _, c := range columnDefs {
		cols = append(cols, c.name)
	}
	return cols
}

// boolToTinyInt 将布尔抽取值转为 TINYINT 写入值：false->0, true->1
func boolToTinyInt(v interface{}) interface{} {
	b, ok := v.(bool)
	if !ok {
		return v
	}
	if b {
		return int64(1)
	}
	return int64(0)
}

// extractOptionalNum 取 proto optional 数值列的写入值：RequestLog 或字段指针为
// nil 时返回 (nil, false)（未设置 -> 写 NULL，与 report 契约 null=未求值对齐）；
// 非 nil 时返回解引用值（bool 列转 TINYINT 0/1）。
func extractOptionalNum(col columnDef, log *bfe_access_pb.BfeLog) (interface{}, bool) {
	reqLog := log.GetRequestLog()
	if reqLog == nil {
		return nil, false
	}
	switch col.field {
	case "ai_intent_confidence":
		if reqLog.AiIntentConfidence == nil {
			return nil, false
		}
		return *reqLog.AiIntentConfidence, true
	case "ai_intent_latency_us":
		if reqLog.AiIntentLatencyUs == nil {
			return nil, false
		}
		return *reqLog.AiIntentLatencyUs, true
	case "ai_intent_cache_hit":
		if reqLog.AiIntentCacheHit == nil {
			return nil, false
		}
		return boolToTinyInt(*reqLog.AiIntentCacheHit), true
	}
	return nil, false
}

// flattenTag 从 ai_apikeytags 抽取值（map，键 level1..level5）打平某级标签；
// 缺失或空标签返回 (nil, nil)
func flattenTag(tags map[string]interface{}, level int) (name interface{}, value interface{}) {
	if tags == nil {
		return nil, nil
	}
	raw, ok := tags[fmt.Sprintf("level%d", level)]
	if !ok {
		return nil, nil
	}
	tag, ok := raw.(mod_fields.ApikeyTagJSON)
	if !ok {
		// 未设置的层级是空 map，视为缺失
		return nil, nil
	}
	if tag.Tagname != "" {
		name = tag.Tagname
	}
	if tag.Tagvalue != "" {
		value = tag.Tagvalue
	}
	return name, value
}

// ToRow 将 BfeLog 组装为一行数据（固定 99 列列序）。
// 字符串零值 -> NULL（kindNotNullStr 列除外：空串原样写入，匹配 NOT NULL 列）；
// 数值/布尔原样（布尔 TINYINT 列转 0/1；kindOptionalNum 列：proto 未设置 -> NULL）；
// 声明了 max 的数值列超界 clamp 到列上限并触发 onNormalized 回调；
// JSON 列 marshal 后写入、空 -> NULL；标签缺失 -> NULL。
func (m *FieldMapper) ToRow(log *bfe_access_pb.BfeLog) ([]interface{}, error) {
	row := make([]interface{}, 0, len(columnDefs))

	var tags map[string]interface{}
	tagsExtracted := false
	ensureTags := func() {
		if tagsExtracted {
			return
		}
		tagsExtracted = true
		v, _ := mod_fields.Extract("ai_apikeytags", log)
		tags, _ = v.(map[string]interface{})
	}

	for _, col := range columnDefs {
		switch col.kind {
		case kindLogTime:
			v, _ := mod_fields.Extract("timestamp", log)
			ts, ok := v.(uint64)
			if !ok {
				row = append(row, nil)
				continue
			}
			row = append(row, time.Unix(int64(ts), 0))

		case kindBoolTiny:
			v, _ := mod_fields.Extract(col.field, log)
			row = append(row, boolToTinyInt(v))

		case kindNotNullStr:
			v, _ := mod_fields.Extract(col.field, log)
			s, _ := v.(string)
			row = append(row, s)

		case kindOptionalNum:
			v, set := extractOptionalNum(col, log)
			if !set {
				row = append(row, nil)
				continue
			}
			row = append(row, v)

		case kindJSON:
			v, isZero := mod_fields.Extract(col.field, log)
			if isZero || v == nil {
				row = append(row, nil)
				continue
			}
			b, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("marshal column %s: %v", col.name, err)
			}
			row = append(row, string(b))

		case kindTagName:
			ensureTags()
			name, _ := flattenTag(tags, col.level)
			row = append(row, name)

		case kindTagValue:
			ensureTags()
			_, value := flattenTag(tags, col.level)
			row = append(row, value)

		default: // kindScalar
			v, isZero := mod_fields.Extract(col.field, log)
			if isZero {
				if _, ok := v.(string); ok {
					// 字符串零值（""）写 NULL
					row = append(row, nil)
					continue
				}
			}
			// 数值列超界 clamp 到列上限（DDL 列类型），不产生 MySQL 1264
			if nv, clamped := clampNumeric(v, col.max); clamped {
				if m.onNormalized != nil {
					m.onNormalized(col.name)
				}
				v = nv
			}
			row = append(row, v)
		}
	}

	return row, nil
}
