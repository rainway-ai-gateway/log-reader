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

package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// MonitorState is the JSON view of module_state2.StateData, which is the
// default output format of the log-reader web monitor handlers
// (e.g. GET /monitor/mod_log_mysql).
type MonitorState struct {
	SCounters map[string]int64  `json:"SCounters"`
	States    map[string]string `json:"States"`
}

// FetchMonitor GETs http://127.0.0.1:<port>/monitor/<handler> and parses the
// JSON body. It fails the test on transport/parse errors.
func FetchMonitor(t *testing.T, port int, handler string) *MonitorState {
	t.Helper()

	url := fmt.Sprintf("http://127.0.0.1:%d/monitor/%s", port, handler)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d", url, resp.StatusCode)
	}

	var m MonitorState
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode monitor response from %s failed: %v", url, err)
	}
	return &m
}

// WaitMonitor polls FetchMonitor every 200ms until cond is satisfied or
// timeout elapses; it returns the last fetched state and fails on timeout.
func WaitMonitor(t *testing.T, port int, handler string, timeout time.Duration, cond func(*MonitorState) bool) *MonitorState {
	t.Helper()

	var last *MonitorState
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		last = FetchMonitor(t, port, handler)
		if cond(last) {
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout %v waiting monitor %s condition; last state: %+v", timeout, handler, last)
	return last
}
