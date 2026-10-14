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
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

func TestModuleKafka_Name(t *testing.T) {
	m := NewModuleKafka()
	if m.Name() != "mod_kafka" {
		t.Errorf("Name() = %q, want mod_kafka", m.Name())
	}
}

func TestModuleKafka_Close_NilProducer(t *testing.T) {
	m := NewModuleKafka()
	if err := m.Close(); err != nil {
		t.Errorf("Close with nil producer should not error, got %v", err)
	}
}

func TestModuleKafka_Update(t *testing.T) {
	m := NewModuleKafka()
	m.state.Init()
	m.state.CountersInit(COUNTER_KEYS)
	m.outputFields = DefaultOutputFields()

	// use a fake producer to avoid real Kafka connection
	m.producer = &fakeProducer{}

	logid := uint64(1)
	ts := uint64(100_000_000_000) // 100s，Unix 纳秒
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
	}

	m.Update(logs)

	if m.state.GetCounter("RECEIVED_LOGS") != 1 {
		t.Errorf("RECEIVED_LOGS = %d, want 1", m.state.GetCounter("RECEIVED_LOGS"))
	}
	if m.state.GetCounter("RECEIVED_REQ") != 1 {
		t.Errorf("RECEIVED_REQ = %d, want 1", m.state.GetCounter("RECEIVED_REQ"))
	}
	if m.state.GetCounter("SENT_TO_KAFKA") != 1 {
		t.Errorf("SENT_TO_KAFKA = %d, want 1", m.state.GetCounter("SENT_TO_KAFKA"))
	}
}

type fakeProducer struct{}

func (p *fakeProducer) Send(msg []byte) {}
func (p *fakeProducer) Start()          {}
func (p *fakeProducer) Close() error    { return nil }
