# Executor Phase-2B Slice 463 — Post Bring-Up Hardening Batch

Date: 2026-03-03

## Scope

1. `crates/adapter/src/main.rs`
2. `crates/execution/src/submitter.rs`
3. `crates/app/src/main.rs`

## Change

1. Adapter graceful shutdown:
   1. Added `with_graceful_shutdown(shutdown_signal())` to server start path.
   2. Added signal handling for `Ctrl+C` and `SIGTERM`.
2. Execution submitter endpoint redaction:
   1. Added `redacted_endpoint_label(endpoint)` helper (`scheme://host[:port]`).
   2. Replaced raw endpoint usage in submitter error details with redacted label for send/http/parse/response-contract errors.
3. App JSON sanitization hardening:
   1. `sanitize_json_value` now escapes JSON control characters (`\n`, `\r`, `\t`, `\b`, `\f`, `\u00XX`) in addition to `\\` and `\"`.

## Tests

1. `redacted_endpoint_label_drops_path_and_query`.
2. `adapter_submitter_redacts_endpoint_on_http_rejected`.
3. `sanitize_json_value_escapes_control_characters`.

## Validation

1. `cargo test -p copybot-execution -q redacted_endpoint_label_drops_path_and_query` — PASS
2. `cargo test -p copybot-execution -q adapter_submitter_redacts_endpoint_on_http_rejected` — PASS
3. `cargo test -p copybot-app -q sanitize_json_value_escapes_control_characters` — PASS
4. `cargo check -p copybot-adapter -q` — PASS
5. `cargo check -p copybot-execution -q` — PASS
6. `cargo check -p copybot-app -q` — PASS

## Mapping

1. Post bring-up gate closure items:
   1. adapter graceful shutdown,
   2. redacted submitter endpoint labels,
   3. control-character-safe JSON sanitization.
