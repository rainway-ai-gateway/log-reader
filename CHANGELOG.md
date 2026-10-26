<!--
This changelog should always be read on `main` branch. Its contents on other branches
does not necessarily reflect the changes.
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.5.0] - 2026-10-24

### Changed
- `mod_log_mysql` startup is no longer fail-fast on MySQL connectivity errors: if `bfe_report` is unreachable at startup, log-reader stays up and the writer keeps retrying in background (new `[mysql]` options `ConnectTimeoutMs` / `ConnectRetryIntervalMs`, both default 3000). Config errors (missing/empty `Addr`/`User`/`DBName`/`Table`) still fail fast. While disconnected, rows buffer in the queue (existing `QueueSize` backpressure) and drain automatically after connect; new monitor keys `MYSQL_CONN_STATE` (UP/DOWN), `MYSQL_CONN_RETRY`, `MYSQL_CONN_OK`.
- `mod_log_mysql` runtime disconnect now auto-reconnects instead of dropping batches: write retries exhaustion marks the connection down (`MYSQL_CONN_LOST`), a supervisor goroutine reconnects in background, and the held batch is rewritten first after reconnect — `SEND_MYSQL_FAILED` no longer grows with disconnect duration. Write path is bounded by new timeouts (`BatchTimeoutMs` for batch transactions via ExecContext + driver writeTimeout, dial timeout via DSN `timeout`), so network partitions fail within seconds instead of hanging on OS-level TCP timeouts. Optional UP-period health ping via `HealthPingIntervalMs` (default 0 = disabled).
- `mod_kafka` output is aligned with the `mod_log_mysql` column set: the default field set grows from 74 to 92, so the 18 columns that used to be always NULL under `FieldMode=default` (`log_tag`, `client_network`, `req_num`, `session_id`, `referrer`, `user_agent`, `delegation`, `uid`, `cookie`, `req_headers`, `res_location`, `res_transfer_encoding`, `res_headers`, `session_offset_time`, `bfe_ip`, `sock_src_ip`, `vip`, `vip6`) now carry values, matching what `mod_log_mysql` writes in full. The three optional `ai_intent_*` columns emit JSON `null` when unset. `require` / `all` / `customized` modes are unchanged.
- `mod_log_mysql` column set grows from 89 to 99, ordered to match `db_ddl_report_mysql.sql`, to carry the fields added below.
- The sample `mod_kafka` config template (`conf/mod_kafka/kafka_config.data`) now emits 12 fields that were registered but missing from the shipped default field list: `ai_cache_read_tokens`, `ai_cache_write_tokens`, `ai_cache_status`, `mirror_hit`, `mirror_cluster`, and the `ai_intent_*` group (`ai_intent_question`, `ai_intent_answer`, `ai_intent_confidence`, `ai_intent_source`, `ai_intent_latency_us`, `ai_intent_cache_hit`, `ai_intent_questions_version`).

### Added
- Register 10 new access-log fields from `bfe-access-pb v0.3.9`: `ai_cache_status`, `mirror_hit`, `mirror_cluster` and the `ai_intent_*` group, with `docs/zh_cn/output-fields.md` updated accordingly.
- New monitor counter `LOGID_EMPTY` for rows written without a logid (see Fixed).
- Integration test scenario LR04 (mysql down at startup: process stays up, retries, recovers and drains; config error still exits; runtime disconnect auto-reconnect with zero drops). Requires Docker; `LR_MYSQL_DSN`-only environments skip the pause-based cases.

### Fixed
- Unset optional intent numerics (`ai_intent_confidence` / `ai_intent_latency_us` / `ai_intent_cache_hit`) are now written as `NULL` instead of `0`, matching the published report contract; an explicit zero still writes `0`. The `ttft` / `tpot` historical zero semantics are unchanged.
- Rows without a logid are now surfaced instead of silently collapsing: the report table's dedup key gained the request-level `logid` discriminator (`hostid`, `log_time`, `ai_apikey_id`, `ai_requested_model`, `logid`, with `logid NOT NULL DEFAULT 0` on the control-plane side), so n concurrent requests of one API key and model inside the same second no longer merge into a single row under the full-row `ON DUPLICATE KEY UPDATE` — previously the discarded rows' tokens and cost vanished from every sum (bfenetworks/bfe#1417). Such rows are counted by `LOGID_EMPTY`.
- Out-of-range numeric values no longer fail the whole batch with MySQL Error 1264: protobuf uint32 fields are wider than the report column types, so writing them as-is aborted the multi-row INSERT, the backoff retries never converged, and the entire batch (including healthy rows) ended up dropped into `SEND_MYSQL_FAILED`. 17 numeric columns now declare a max bound in `field_mapper` per the DDL types (INT 2147483647, `res_status_code` 32767, `backend_retry` 127), over-range values are clamped to the column max (counted by new monitor counter `VALUE_NORMALIZED` with rate-limited warning logs), and row-located data errors (MySQLError 1264/1406 with row position) drop only the offending row and immediately rewrite the remaining rows — no backoff sleep, no markDown, no `SEND_MYSQL_FAILED`; new monitor counter `ROW_DROPPED_DATA` tracks dropped rows, and `WRITE_BATCH_SIZE` now counts actually persisted rows (rainway-ai-gateway/log-reader#12).
- The binary now reports its version: package main declares a `version` symbol so the Makefile's `-ldflags "-X main.version=$(LOG_READER_VERSION)"` takes effect (previously silently ignored, leaving the monitor page version empty), a `-v` flag prints `version <ver>` and exits (default `debug` when built without ldflags), and the version is now included in the startup log and shown on the web monitor page.

## [v1.4.0] - 2026-09-24

### Added
- Add `mod_log_mysql` module: batch idempotent write (`INSERT ... ON DUPLICATE KEY UPDATE`) of AI access logs to the MySQL table `bfe_ai_request_log` (89 columns), with a queue + single-writer goroutine and backoff retry.
- Extract shared protobuf field extraction into `reader_modules/mod_fields`, reused by `mod_kafka` (API kept via type aliases) and `mod_log_mysql`.
- Add integration test scenario LR03 (basic write, idempotent replay, zero-value-to-null, batch split) against real MySQL via `LR_MYSQL_DSN` or testcontainers; add module doc, sys_design docs, and `conf/mod_log_mysql/mod_log_mysql.conf` template.

### Changed
- Rename the module from `github.com/bfenetworks/log-reader` to `github.com/rainway-ai-gateway/log-reader`.


## [v1.3.0] - 2026-08-31

### Added
- Register AI observability fields from bfe-access-pb v0.3.5: `ai_image_input_tokens`, `ai_video_count`.

### Changed
- Upgrade `bfe-access-pb` dependency from v0.3.4 to v0.3.5.


## [v1.2.0] - 2026-08-29

### Added
- Register missing AI observability fields: `ai_protocol`, `ai_mode`, `ai_audio_input_tokens`, `ai_audio_output_tokens`, `ai_image_count`.
- Support `ai_cache_read_tokens` and `ai_cache_write_tokens` output fields.

### Changed
- Update `ai_apikeytags` output format from array of objects to level-based object (`level1`~`level5`).
- Upgrade `bfe-access-pb` dependency from v0.2.0 to v0.3.4.
- Upgrade `bfe` dependency to v1.8.6.


## [v1.1.0] - 2026-08-21

### Added
- Add AI observability fields from bfe-access-pb v0.2.0: `ai_provider`, `ai_retry_count`, `ai_cost_value`, `ai_cost_currency`, `ai_route_rule_hits`, `ai_cluster_key_names`, and `ai_auth_hit_quota_plans`.
- Add `HttpAddr` config option to bind the web monitor server to a specific listen address.
- Add integration test suite with a mock Kafka broker, covering the end-to-end access log flow (scenario LR01 basic flow).
- Add configuration documentation (`doc/configuration/`) and output field reference (`doc/fields/`).

### Changed
- Upgrade `bfe-access-pb` from v0.1.0 to v0.2.0.
- Rename AI observability fields to align with bfe-access-pb v0.2.0: `ai_apikey` → `ai_apikey_id`, `ai_mapped_model` → `ai_target_model`, `ai_prompt_tokens` → `ai_input_tokens`.
- Upgrade `bfe` dependency to the latest v1.8.5 branch version.
- Migrate `github.com/baidu/go-lib` dependency to `github.com/bfenetworks/go-lib`.

## [v1.0.0] - 2026-07-20

### Added

- Core log reader with protobuf-encoded BFE access log parsing and tailing support.
- Module framework (`reader_module`) for extensible log processing pipelines.
- Config loading system (`reader_conf`) with support for basic and access-pb config types.
- Built-in Kafka output module (`mod_kafka`) for forwarding parsed access logs to Kafka.

[v1.5.0]: https://github.com/rainway-ai-gateway/log-reader/compare/v1.4.0...v1.5.0
[v1.4.0]: https://github.com/rainway-ai-gateway/log-reader/compare/v1.3.0...v1.4.0
[v1.3.0]: https://github.com/bfenetworks/log-reader/compare/v1.2.0...v1.3.0
[v1.2.0]: https://github.com/bfenetworks/log-reader/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/bfenetworks/log-reader/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/bfenetworks/log-reader/releases/tag/v1.0.0
