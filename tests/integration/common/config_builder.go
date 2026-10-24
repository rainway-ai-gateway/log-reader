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

package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WriteKafkaDataConfig overwrites mod_kafka/kafka_config.data with the given
// field mode and field names. This allows a single test scenario to exercise
// different output field configurations.
func (b *LogReaderConfigBuilder) WriteKafkaDataConfig(mode string, fields []string) error {
	path := filepath.Join(b.TargetConfDir, "mod_kafka", "kafka_config.data")
	var sb strings.Builder
	sb.WriteString("# kafka_config.data\n\n")
	sb.WriteString("[ConfFields]\n")
	sb.WriteString(fmt.Sprintf("FieldMode = %s\n", mode))
	for _, f := range fields {
		sb.WriteString(fmt.Sprintf("FieldNames= %s\n", f))
	}
	return os.WriteFile(path, []byte(sb.String()), 0644)
}

// LogReaderConfigBuilder builds a temporary log-reader configuration directory.
type LogReaderConfigBuilder struct {
	TemplateDir   string
	TargetConfDir string
	KafkaBroker   string
	LogFilePath   string

	// HttpPort optionally replaces {{HTTP_PORT}} in config.conf, for
	// scenarios that poll the monitor endpoint and must pin the port up front.
	HttpPort int

	// MySQL connection placeholders, used by mod_log_mysql scenarios.
	MysqlAddr     string
	MysqlUser     string
	MysqlPassword string
	MysqlDBName   string
}

// Build copies the static testdata templates into TargetConfDir and injects
// dynamic values (Kafka broker, log file path, MySQL connection).
func (b *LogReaderConfigBuilder) Build() error {
	if err := copyDirContents(b.TemplateDir, b.TargetConfDir); err != nil {
		return fmt.Errorf("copy testdata failed: %w", err)
	}

	// Replace placeholders in config files.
	// Use forward slashes for paths to avoid gcfg interpreting backslashes as escapes.
	replacements := map[string]string{
		"{{KAFKA_BROKER}}":   b.KafkaBroker,
		"{{LOG_FILE}}":       filepath.ToSlash(b.LogFilePath),
		"{{MYSQL_ADDR}}":     b.MysqlAddr,
		"{{MYSQL_USER}}":     b.MysqlUser,
		"{{MYSQL_PASSWORD}}": b.MysqlPassword,
		"{{MYSQL_DB}}":       b.MysqlDBName,
	}
	if b.HttpPort != 0 {
		replacements["{{HTTP_PORT}}"] = fmt.Sprintf("%d", b.HttpPort)
	}

	if err := replacePlaceholders(b.TargetConfDir, replacements); err != nil {
		return fmt.Errorf("replace placeholders failed: %w", err)
	}

	return nil
}

func replacePlaceholders(dir string, replacements map[string]string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		content := string(data)
		for old, new := range replacements {
			content = strings.ReplaceAll(content, old, new)
		}

		if content != string(data) {
			if err := os.WriteFile(path, []byte(content), info.Mode()); err != nil {
				return err
			}
		}
		return nil
	})
}
