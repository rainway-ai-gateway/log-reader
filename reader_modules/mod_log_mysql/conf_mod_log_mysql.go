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

	"github.com/bfenetworks/go-lib/log"
	gcfg "gopkg.in/gcfg.v1"
)

var openDebug bool

// ConfModLogMysql mod_log_mysql 模块配置
type ConfModLogMysql struct {
	Basic  ModLogMysqlBasic
	Mysql  ConfMysql
	Writer ConfLogMysqlWriter
}

// ConfMysql MySQL 连接配置
type ConfMysql struct {
	Addr                   string // MySQL 地址，如 127.0.0.1:3306
	User                   string // 用户名（专用最小权限账号）
	Password               string // 密码
	DBName                 string // 目标库名
	Table                  string // 目标表名
	ConnectTimeoutMs       int    // 单次 connect/ping 超时（毫秒）
	ConnectRetryIntervalMs int    // 建连重试间隔（毫秒），启动未连通时后台重试
	HealthPingIntervalMs   int    // UP 期健康探活间隔（毫秒），0 = 关闭；探活失败触发断连重连
}

// ConfLogMysqlWriter 批量写入配置
type ConfLogMysqlWriter struct {
	QueueSize       int // 缓冲队列容量（条）
	BatchSize       int // 攒批条数
	FlushIntervalMs int // 攒批超时（毫秒）
	MaxRetries      int // 写失败重试次数
	MaxOpenConns    int // 连接池最大打开连接数
	MaxIdleConns    int // 连接池最大空闲连接数
	BatchTimeoutMs  int // 单批事务执行超时（毫秒），ExecContext ctx 与驱动 writeTimeout
}

// ModLogMysqlBasic 基础配置
type ModLogMysqlBasic struct {
	OpenDebug bool // 是否开启 Debug 模式（打印每行组装结果）
}

// LoadConfig 加载 mod_log_mysql 配置文件
func LoadConfig(filePath string) (*ConfModLogMysql, error) {
	var cfg ConfModLogMysql

	if err := gcfg.ReadFileInto(&cfg, filePath); err != nil {
		return &cfg, err
	}

	if err := ConfModLogMysqlCheck(&cfg); err != nil {
		return &cfg, err
	}

	openDebug = cfg.Basic.OpenDebug

	return &cfg, nil
}

// ConfModLogMysqlCheck 校验配置并填充默认值
func ConfModLogMysqlCheck(cfg *ConfModLogMysql) error {
	if cfg.Mysql.Addr == "" {
		return fmt.Errorf("Mysql.Addr is empty")
	}

	if cfg.Mysql.User == "" {
		return fmt.Errorf("Mysql.User is empty")
	}

	if cfg.Mysql.DBName == "" {
		return fmt.Errorf("Mysql.DBName is empty")
	}

	if cfg.Mysql.Table == "" {
		return fmt.Errorf("Mysql.Table is empty")
	}

	if cfg.Mysql.ConnectTimeoutMs <= 0 {
		log.Logger.Warn("mod_log_mysql: Mysql.ConnectTimeoutMs[%d] <= 0, use default value(3000)", cfg.Mysql.ConnectTimeoutMs)
		cfg.Mysql.ConnectTimeoutMs = 3000
	}

	if cfg.Mysql.ConnectRetryIntervalMs <= 0 {
		log.Logger.Warn("mod_log_mysql: Mysql.ConnectRetryIntervalMs[%d] <= 0, use default value(3000)", cfg.Mysql.ConnectRetryIntervalMs)
		cfg.Mysql.ConnectRetryIntervalMs = 3000
	}

	if cfg.Mysql.HealthPingIntervalMs < 0 {
		log.Logger.Warn("mod_log_mysql: Mysql.HealthPingIntervalMs[%d] < 0, use default value(0, disabled)", cfg.Mysql.HealthPingIntervalMs)
		cfg.Mysql.HealthPingIntervalMs = 0
	}

	if cfg.Writer.QueueSize <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.QueueSize[%d] <= 0, use default value(2000)", cfg.Writer.QueueSize)
		cfg.Writer.QueueSize = 2000
	}

	if cfg.Writer.BatchSize <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.BatchSize[%d] <= 0, use default value(200)", cfg.Writer.BatchSize)
		cfg.Writer.BatchSize = 200
	}

	if cfg.Writer.FlushIntervalMs <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.FlushIntervalMs[%d] <= 0, use default value(2000)", cfg.Writer.FlushIntervalMs)
		cfg.Writer.FlushIntervalMs = 2000
	}

	if cfg.Writer.MaxRetries <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.MaxRetries[%d] <= 0, use default value(3)", cfg.Writer.MaxRetries)
		cfg.Writer.MaxRetries = 3
	}

	if cfg.Writer.MaxOpenConns <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.MaxOpenConns[%d] <= 0, use default value(10)", cfg.Writer.MaxOpenConns)
		cfg.Writer.MaxOpenConns = 10
	}

	if cfg.Writer.MaxIdleConns <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.MaxIdleConns[%d] <= 0, use default value(5)", cfg.Writer.MaxIdleConns)
		cfg.Writer.MaxIdleConns = 5
	}

	if cfg.Writer.BatchTimeoutMs <= 0 {
		log.Logger.Warn("mod_log_mysql: Writer.BatchTimeoutMs[%d] <= 0, use default value(30000)", cfg.Writer.BatchTimeoutMs)
		cfg.Writer.BatchTimeoutMs = 30000
	}

	return nil
}
