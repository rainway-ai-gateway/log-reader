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
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/reader_conf"
	"github.com/rainway-ai-gateway/log-reader/reader_module"
	"github.com/rainway-ai-gateway/log-reader/reader_modules/mod_fields"

	_ "github.com/go-sql-driver/mysql"
)

var COUNTER_KEYS = []string{
	"RECEIVED_LOGS",       // Update 收到的日志条数
	"RECEIVED_REQ",        // 其中 request 类型条数
	"CONVERT_FAILED",      // 行组装失败条数
	"SENT_TO_MYSQL",       // 成功入队条数
	"SENT_MYSQL_CHN_FULL", // 队列满丢弃条数（背压）
	"SEND_MYSQL_FAILED",   // 重试耗尽丢弃批数
	"WRITE_BATCH_SIZE",    // 每批实际条数累计（diff 求均值观测批大小）
	"MYSQL_CONN_RETRY",    // 建连重试累计次数（重试协程每次失败 +1）
	"MYSQL_CONN_OK",       // 建连成功累计次数
	"MYSQL_CONN_LOST",     // 运行期断连次数（写重试耗尽/探活失败触发 markDown 计数）
	"LOGID_EMPTY",         // logid 为 0 的行数（bfenetworks/bfe#1417）：该行无法按请求级判别列去重，退化为旧四元组语义
	"VALUE_NORMALIZED",    // 因超界被 clamp 到列上限的字段值个数（按字段值计，非按行；issue #12）
	"ROW_DROPPED_DATA",    // 因行定位类数据错误（1264/1406）被剔除丢弃的行数（issue #12）
}

// valueNormalizedWarnInterval 同一列归一化 Warn 日志的最小间隔（限频）
const valueNormalizedWarnInterval = time.Minute

// normalizeMonitor VALUE_NORMALIZED 事件的计数与限频日志：每列每
// valueNormalizedWarnInterval 最多一条 Warn，只记列名与累计次数，不记 clamp 前值
type normalizeMonitor struct {
	state *module_state2.State

	mu       sync.Mutex
	lastWarn map[string]time.Time
}

func newNormalizeMonitor(state *module_state2.State) *normalizeMonitor {
	return &normalizeMonitor{state: state, lastWarn: make(map[string]time.Time)}
}

// onNormalized FieldMapper 归一化事件回调：计数 + 限频 Warn
func (n *normalizeMonitor) onNormalized(col string) {
	n.state.Inc("VALUE_NORMALIZED", 1)

	n.mu.Lock()
	defer n.mu.Unlock()
	if now := time.Now(); now.Sub(n.lastWarn[col]) >= valueNormalizedWarnInterval {
		n.lastWarn[col] = now
		log.Logger.Warn("mod_log_mysql: column %s value out of range, normalized to column max (total %d)",
			col, n.state.GetCounter("VALUE_NORMALIZED"))
	}
}

// state 键值（非 counter）：MYSQL_CONN_STATE = UP / DOWN，
// 与 bfe_reader 的 SERVER_READY 同为 state.Set 用法，web monitor 自动带出

// recordWriter is a small interface to make ModuleLogMysql testable
type recordWriter interface {
	Enqueue(row []interface{}) bool
	Connect() error
	Connected() bool
	Start()
	Close()
}

// fieldMapper is a small interface to make ModuleLogMysql testable
type fieldMapper interface {
	ToRow(log *bfe_access_pb.BfeLog) ([]interface{}, error)
}

// ModuleLogMysql MySQL 输出模块：批量幂等写入明细表 bfe_ai_request_log
type ModuleLogMysql struct {
	name      string
	conf      *ConfModLogMysql
	state     module_state2.State
	stateDiff module_state2.CounterSlice
	mapper    fieldMapper
	writer    recordWriter
}

// NewModuleLogMysql 创建 ModuleLogMysql
func NewModuleLogMysql() *ModuleLogMysql {
	m := new(ModuleLogMysql)
	m.name = "mod_log_mysql"
	return m
}

// Name 返回模块名
func (m *ModuleLogMysql) Name() string {
	return m.name
}

func (m *ModuleLogMysql) getState(query url.Values) ([]byte, error) {
	s := m.state.GetAll()
	return s.FormatOutput(query)
}

func (m *ModuleLogMysql) getStateDiff(query url.Values) ([]byte, error) {
	s := m.stateDiff.Get()
	return s.FormatOutput(query)
}

func (m *ModuleLogMysql) monitorHandlers() map[string]interface{} {
	return map[string]interface{}{
		m.name:           m.getState,
		m.name + "_diff": m.getStateDiff,
	}
}

// Init 初始化模块
func (m *ModuleLogMysql) Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error {
	var err error

	m.state.Init()
	m.state.CountersInit(COUNTER_KEYS)
	m.state.SetKeyPrefix(m.name)
	m.state.SetProgramName(conf.Main.ProgramName)

	m.stateDiff.Init(&m.state, conf.Main.MonitorInterval)
	m.stateDiff.SetKeyPrefix(m.name + "_diff")
	m.stateDiff.SetProgramName(conf.Main.ProgramName)

	confPath := reader_module.ModConfPath(cr, m.name)
	m.conf, err = LoadConfig(confPath)
	if err != nil {
		log.Logger.Error("%s.Init(): LoadConfig(): %v", m.name, err)
		return fmt.Errorf("LoadConfig(): %v", err)
	}

	// connect to mysql and verify connectivity.
	// 连通性错误非致命：Init 只记日志并置 MYSQL_CONN_STATE=DOWN，
	// 由 writer 的后台重试协程持续重连直到成功；配置错误仍在上方 fail-fast
	// 注入归一化事件回调：计数 VALUE_NORMALIZED + 限频 Warn（不记 clamp 前值）
	mapper := NewFieldMapper()
	mapper.SetOnNormalized(newNormalizeMonitor(&m.state).onNormalized)
	m.mapper = mapper
	m.writer = NewRecordWriter(m.conf, &m.state)
	if err := m.writer.Connect(); err != nil {
		log.Logger.Error("%s.Init(): mysql not reachable (%v), will retry in background", m.name, err)
		m.state.Set("MYSQL_CONN_STATE", "DOWN")
	} else {
		m.state.Set("MYSQL_CONN_STATE", "UP")
	}
	m.writer.Start()

	err = web_monitor.RegisterHandlers(whs, web_monitor.WebHandleMonitor, m.monitorHandlers())
	if err != nil {
		log.Logger.Error("%s.Init(): RegisterHandlers(): %v", m.name, err)
		return fmt.Errorf("RegisterHandlers(): %v", err)
	}

	log.Logger.Info("%s.Init(): success", m.name)
	return nil
}

// Start 启动模块（写协程已在 Init 启动，此处无操作，与 mod_kafka 一致）
func (m *ModuleLogMysql) Start() {
	log.Logger.Info("%s.Start(): started", m.name)
}

// Update 处理 BfeLog 批次：仅处理 Request 类型，组装行后非阻塞入队
func (m *ModuleLogMysql) Update(bfeLogs []*bfe_access_pb.BfeLog) {
	for _, bfeLog := range bfeLogs {
		m.state.Inc("RECEIVED_LOGS", 1)

		if bfeLog.GetLogType() != bfe_access_pb.BfeLogType_Request {
			continue
		}
		m.state.Inc("RECEIVED_REQ", 1)

		// logid 为 0 的行无法按请求级判别列去重（bfenetworks/bfe#1417）：新唯一键
		// (hostid, log_time, ai_apikey_id, ai_requested_model, logid) 下 0 会参与比较，
		// 该行退化为旧四元组语义——同秒同 Key 同模型的并发请求仍会互相覆盖。
		// bfe 侧已保证 logid 非 0；此处计数使该形态不再静默（建议阈值 0、超阈告警）。
		if _, isZero := mod_fields.Extract("logid", bfeLog); isZero {
			m.state.Inc("LOGID_EMPTY", 1)
		}

		row, err := m.mapper.ToRow(bfeLog)
		if err != nil {
			m.state.Inc("CONVERT_FAILED", 1)
			log.Logger.Error("%s.Update(): ToRow failed: %v", m.name, err)
			continue
		}
		if openDebug {
			log.Logger.Debug("%s.Update(): ToRow, row: %v", m.name, row)
		}

		if !m.writer.Enqueue(row) {
			m.state.Inc("SENT_MYSQL_CHN_FULL", 1)
			log.Logger.Warn("%s.Update(): record channel full, dropping log", m.name)
			continue
		}
		m.state.Inc("SENT_TO_MYSQL", 1)
	}
}

// Close 清理模块资源
func (m *ModuleLogMysql) Close() error {
	if m.writer != nil {
		m.writer.Close()
	}
	return nil
}
