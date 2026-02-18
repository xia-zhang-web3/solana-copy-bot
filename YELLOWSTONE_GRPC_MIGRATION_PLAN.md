# Yellowstone gRPC Migration Plan for `solana-copy-bot`

Date: 2026-02-18
Owner: copybot runtime team
Status: Pre-implementation (Phase A gates pending)

## Readiness Status

Current state: **Pre-implementation**.

- Allowed now: Phase A contract/probe work.
- Blocked now: Phase B+ coding until all Hard Gates in section 0 are closed and committed.

## 0) Hard Gates Before Coding

No implementation work in `crates/ingestion` starts until all gates below are complete:

1. **Protobuf mapping spec is frozen**:
   - exact `SubscribeUpdateTransaction` paths used for:
     - signature
     - slot
     - success/fail status
     - account keys / signer
     - token balance deltas
     - block time / event time
   - commitment level policy is fixed (target: `confirmed`).
   - behavior for partial/empty meta is fixed (`drop` + reason counter, never panic).
   - mapping template in section 8.1 contains **no `TBD` values** before Phase B.
2. **Telemetry contract decision is frozen**:
   - For migration v1, `IngestionRuntimeSnapshot` stays backward-compatible with current app risk guard contract (`ts_utc`, `ws_notifications_enqueued`, `ws_notifications_replaced_oldest`, `rpc_429`, `rpc_5xx`, `ingestion_lag_ms_p95`).
   - Additional p50/p99 and gRPC-specific metrics are emitted via structured logs/counters, not required in snapshot struct for v1.
3. **Comparison methodology is frozen**:
   - KPI validation must include same-input replay comparison (not only separate live DB canary).
4. **Dependency set and versions are frozen**:
   - `yellowstone-grpc-client = 12.0.0`
   - `yellowstone-grpc-proto = 12.0.0`
   - `tonic = 0.14.4`
   - `prost = 0.14.3`
   - versions pinned in `crates/ingestion/Cargo.toml` before coding to avoid moving target.
   - dependency probe passes and is attached to Phase A checklist:
     - `cargo check -p copybot-ingestion`
     - `cargo tree -p copybot-ingestion -i tonic`
     - single tonic major line (`0.14.x`) in dependency graph
5. **Queue overflow contract is frozen**:
   - `yellowstone_grpc` reuses existing `ingestion.queue_overflow_policy` semantics (`block` / `drop_oldest`).
   - paper/prod default remains `drop_oldest` to preserve replaced-ratio observability.
6. **Failover mechanism is frozen**:
   - auto-failover is implemented as external watchdog/supervisor restart with profile switch (`yellowstone_grpc` -> `helius_ws`), not in-process hot source swap.

## 1) Why This Migration Is Needed

Current production snapshot shows ingestion saturation, not strategy logic, as the dominant bottleneck:

- `ingestion_lag_ms_p95 ~= 9.1s`
- `ingestion_lag_ms_p99 ~= 18.3s`
- `ws_to_fetch_queue_depth = 501` with capacity `512`
- `ws_notifications_replaced_oldest / ws_notifications_enqueued ~= 0.8989`
- `fetch_concurrency_inflight = 4`
- `rpc_429 = 0`, `rpc_5xx = 0`

Interpretation:

- The system is dropping/replacing a large share of incoming events before they are fully processed.
- The main pressure point is the current `logsSubscribe -> getTransaction` HTTP pipeline.
- Yellowstone gRPC can reduce this pressure by delivering richer stream data directly and removing the per-event HTTP fetch dependency from the hot path.

## 2) Scope and Non-Goals

In scope:

1. Add a new ingestion source: `yellowstone_grpc`.
2. Keep `discovery`, `shadow`, and `storage` interfaces stable (`SwapEvent` remains core contract).
3. Reduce event drop ratio and ingestion lag under current traffic.
4. Preserve risk controls, followlist semantics, and shadow processing behavior.

Out of scope:

1. Full execution engine implementation.
2. Strategy changes in scoring/risk thresholds (except temporary rollout tuning).
3. Rewriting schema model or moving away from SQLite in this migration.

## 3) Target End State

The bot supports two production-grade ingestion paths:

1. `helius_ws` (legacy fallback)
2. `yellowstone_grpc` (primary path)

Primary mode (`yellowstone_grpc`) should:

1. Avoid per-swap `getTransaction` HTTP fetch in normal flow.
2. Maintain bounded queues and deterministic ordering safeguards.
3. Feed the same `SwapEvent` contract into app loop.
4. Expose equivalent or better telemetry than current ingestion path.

## 4) Current Code Surfaces to Change

Main files:

- `crates/config/src/lib.rs`
- `configs/dev.toml`
- `configs/paper.toml`
- `configs/prod.toml`
- `crates/ingestion/Cargo.toml`
- `crates/ingestion/src/lib.rs`
- `crates/ingestion/src/source.rs`
- `crates/storage/src/lib.rs` (sqlite contention counters for rollout KPIs)
- `crates/app/src/main.rs` (startup fail-fast validation hook, if enforced there)
- `tools/ingestion_failover_watchdog.sh` (external failover bridge)
- `README.md`

Expected unchanged business logic for first cut:

- `crates/discovery/src/lib.rs` (scoring logic unchanged)
- `crates/shadow/src/lib.rs` (signal/risk semantics unchanged)

Operational caveat:

- `discovery` / `shadow` token-quality refresh still rely on HTTP endpoints. When `ingestion.source=yellowstone_grpc`, set either:
  - `discovery.helius_http_url` and `shadow.helius_http_url`, or
  - keep `ingestion.helius_http_url` populated as fallback for those roles.
- For paper/prod with quality gates enabled, missing effective HTTP endpoint is a startup validation error (fail fast, no silent degrade).

## 5) Migration Strategy (Low-Risk)

Use additive migration, not replacement-first:

1. Implement `yellowstone_grpc` as a new source mode.
2. Keep `helius_ws` fully functional as fallback.
3. Validate on same-input replay harness before production cutover.
4. Canary with real traffic into separate DB.
5. Compare ingestion KPIs and signal capture.
6. Promote to primary after acceptance criteria pass.
7. Only then deprecate legacy hot-path pieces.

## 6) Phase Plan and Estimates

Estimated total: 11-17 engineering days.

### Phase A: Foundation and Contracts (1 day)

1. Freeze baseline metrics for at least 24h.
2. Freeze exact protobuf mapping from Yellowstone events to `RawSwapObservation` / `SwapEvent`.
3. Confirm gRPC endpoint/auth model and TLS settings for QuickNode add-on.
4. Freeze telemetry contract decision (snapshot compatibility vs struct expansion).
5. Freeze comparison methodology (replay + live canary).
6. Freeze queue overflow policy semantics for gRPC path.
7. Freeze failover execution mechanism (external supervisor + restart profile).

Deliverables:

- Field mapping spec section in this document updated with exact protobuf paths and commitment/filter policy.
- Baseline metric snapshot committed in ops notes.
- Dependency probe output attached to checklist (`cargo check` + `cargo tree`).
- Signed Phase A checklist; coding blocked until signed.

### Phase B: Config and Dependency Wiring (0.5-1 day)

1. Add new config fields in `IngestionConfig`:
   - `yellowstone_grpc_url`
   - `yellowstone_x_token`
   - `yellowstone_connect_timeout_ms`
   - `yellowstone_subscribe_timeout_ms`
   - `yellowstone_stream_buffer_capacity`
   - `yellowstone_reconnect_initial_ms`
   - `yellowstone_reconnect_max_ms`
   - `yellowstone_program_ids` (optional explicit override)
   - fallback policy for `yellowstone_program_ids`:
     - if set -> use it
     - else if `subscribe_program_ids` non-empty -> use it
     - else use `raydium_program_ids ∪ pumpswap_program_ids`
     - if resulting set is empty -> startup error
2. Reuse existing `ingestion.queue_overflow_policy` for gRPC source (no extra policy key in v1).
3. Add environment overrides for all new fields.
4. Add crate dependencies for Yellowstone gRPC client/proto stack with pinned versions:
   - `yellowstone-grpc-client = 12.0.0`
   - `yellowstone-grpc-proto = 12.0.0`
   - `tonic = 0.14.4`
   - `prost = 0.14.3`

Deliverables:

- `crates/config/src/lib.rs` defaults + env parsing.
- `configs/*.toml` templates updated (placeholders only).
- `crates/ingestion/Cargo.toml` dependency set updated.

### Phase C: New Source Implementation (3-5 days)

1. Extend `IngestionSource` enum with `YellowstoneGrpc(...)`.
2. Implement dedicated module (recommended split):
   - `crates/ingestion/src/yellowstone.rs`
3. Core runtime loop in new module:
   - connect
   - subscribe
   - receive stream updates
   - parse swap observations
   - bounded queue push
   - apply `queue_overflow_policy` (`block` / `drop_oldest`) identically to current semantics
   - reconnect/backoff
4. Reimplement dedupe explicitly for gRPC path (signature TTL + bounded LRU) because current Helius dedupe is local to `ws_reader_loop`.
5. Reimplement reorder semantics `(slot, arrival_seq, signature)` for deterministic processing.
6. Extract shared helpers for dedupe/reorder into reusable ingestion module to avoid logic drift between `helius_ws` and `yellowstone_grpc`.

Deliverables:

- `ingestion.source = "yellowstone_grpc"` operational in local/paper.
- Existing `helius_ws` path untouched and testable.

### Phase D: Parsing and Normalization Hardening (2-3 days)

1. Map Yellowstone transaction updates to swap legs robustly.
2. Ensure Raydium/PumpSwap detection parity with current parser logic.
3. Handle edge cases:
   - failed tx
   - null/partial metadata
   - multi-leg ambiguity
   - non-SOL pairs when unsupported
4. Add strict metrics for parse rejects by reason.

Deliverables:

- Parser conformance tests.
- Drop reason visibility for gRPC parse failures.

### Phase E: Telemetry and Guard Integration (1 day)

1. Keep existing ingestion metrics names where possible.
2. Add gRPC-specific counters:
   - reconnect_count
   - stream_gap_detected
   - parse_rejected_total
   - grpc_message_rate
   - grpc_decode_errors
3. Add SQLite contention counters (currently not explicitly exposed):
   - `sqlite_write_retry_total`
   - `sqlite_busy_error_total`
4. Keep `IngestionRuntimeSnapshot` backward-compatible in v1 for risk infra guard.
5. Add process memory monitoring (`rss_mb`) to telemetry stream.

Deliverables:

- Ops dashboards can compare `helius_ws` and `yellowstone_grpc` on same dimensions.

### Phase E.5: Watchdog and Supervisor Wiring (0.5-1 day)

1. Implement watchdog runtime:
   - `tools/ingestion_failover_watchdog.sh` (or equivalent service binary).
2. Wire watchdog trigger evaluation against section 13.1 conditions.
3. Implement atomic fallback writes:
   - `state/ingestion_source_override.env`
   - `state/ingestion_failover_cooldown.json`
4. Wire supervisor restart flow (systemd unit/timer or wrapper) and cooldown behavior.
5. Add smoke tests on staging/canary host:
   - forced degradation trigger -> fallback profile written -> service restart -> source switches to `helius_ws`.

Deliverables:

- Watchdog executable + policy/config files committed.
- Supervisor wiring docs with exact commands and restart policy.
- Canary readiness gate signed: watchdog protection active before Phase F.

### Phase F: Canary Rollout (1-2 days)

1. Prerequisite: Phase E.5 completed; watchdog protection active on canary host.
2. Run gRPC source in canary service instance and separate DB.
3. Keep same `discovery` and `shadow` settings as control.
4. Run same-input replay benchmark (recorded update stream) against both adapters before live canary decision.
5. Collect side-by-side KPIs:
   - lag p95/p99
   - replaced/drop ratio
   - signals buy/sell
   - buy drop reasons
   - queue depth stability
6. Validate no regression in risk/event consistency.

Deliverables:

- Canary report with go/no-go recommendation.

### Phase G: Production Cutover and Cleanup (1-2 days)

1. Switch prod ingestion source to `yellowstone_grpc`.
2. Keep `helius_ws` config hot and ready for immediate rollback.
3. After stable window, remove or simplify legacy HTTP fetch internals.

Deliverables:

- Production change record.
- Post-cutover health report at 1h, 6h, 24h.

## 7) Detailed Implementation Tasks by File

### `crates/config/src/lib.rs`

Tasks:

1. Extend `IngestionConfig` with Yellowstone fields.
2. Add defaults that fail closed with explicit `REPLACE_ME`.
3. Add env overrides:
   - `SOLANA_COPY_BOT_YELLOWSTONE_GRPC_URL`
   - `SOLANA_COPY_BOT_YELLOWSTONE_X_TOKEN`
   - `SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS`
   - `SOLANA_COPY_BOT_YELLOWSTONE_SUBSCRIBE_TIMEOUT_MS`
   - `SOLANA_COPY_BOT_YELLOWSTONE_STREAM_BUFFER_CAPACITY`
   - `SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_INITIAL_MS`
   - `SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_MAX_MS`
   - `SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS` (CSV)
4. Resolve `yellowstone_program_ids` fallback deterministically:
   - explicit `SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS` / config value
   - else `subscribe_program_ids`
   - else `raydium_program_ids ∪ pumpswap_program_ids`
5. Keep old env keys unchanged for backward compatibility.

Nuance:

- Do not overload existing Helius env keys to avoid accidental mixed setup.

### `configs/paper.toml`, `configs/prod.toml`, `configs/dev.toml`

Tasks:

1. Add commented `yellowstone_grpc` section with placeholders.
2. Keep default source unchanged until rollout gate passes.
3. For `dev`, optionally keep `mock` as default and add tested sample block for gRPC.

Nuance:

- No secrets in repo. Token only from environment.

### `crates/ingestion/Cargo.toml`

Tasks:

1. Add Yellowstone client/proto deps.
2. Ensure TLS and async runtime features are explicit.
3. Pin versions conservatively to avoid proto/API churn.

Nuance:

- Keep dependency footprint isolated to ingestion crate.

### `crates/ingestion/src/lib.rs`

Tasks:

1. Wire new source variant in service builder.
2. Keep `runtime_snapshot()` semantics unchanged.

Nuance:

- App/risk code depends on snapshot contract stability.

### `crates/ingestion/src/source.rs` and/or `src/yellowstone.rs`

Tasks:

1. Add `IngestionSource::YellowstoneGrpc`.
2. Implement stream consumer with:
   - bounded internal channels
   - reconnect with exponential backoff
   - decode + parse + normalize
3. Reimplement dedupe for gRPC source and keep behavioral parity with existing TTL/LRU semantics.
4. Reimplement reorder handling for gRPC source and keep `(slot, arrival_seq, signature)` ordering parity.
5. Report telemetry under existing metric names + gRPC extras.

Nuance:

- Keep per-path code separated to reduce complexity in already large `source.rs`.

### `README.md`

Tasks:

1. Add startup examples for `yellowstone_grpc`.
2. Add troubleshooting section:
   - auth/token errors
   - stream reconnect storms
   - lag diagnosis checklist

## 8) Parser Mapping and Data Quality Nuances

Critical nuance: Yellowstone stream gives richer data, but parser correctness still defines signal quality.

### 8.1 Mandatory Protobuf Mapping Spec (Hard Gate)

This table must be completed and reviewed before Phase B starts (coding remains blocked until then):

1. `SubscribeUpdateTransaction` field path for transaction signature.
2. Field path for slot.
3. Field path(s) for error/success transaction status.
4. Field path(s) for account keys and signer derivation policy.
5. Field path(s) for pre/post token balances.
6. Field path(s) for pre/post SOL balances (fallback leg inference).
7. Field path for block time and fallback policy when absent.
8. Commitment level used in subscription (`confirmed` target).
9. Program filter semantics (exact include list for Raydium/PumpSwap IDs).
10. Partial/empty meta policy:
    - if required fields missing -> drop with explicit reason counter.
    - no best-effort synthetic fill for unknown legs.

Mapping template (must be filled during Phase A):

| Logical Field | Proto Path | Required | Fallback Rule | Drop Reason Key | Notes |
| --- | --- | --- | --- | --- | --- |
| signature | TBD | yes | none | `missing_signature` | |
| slot | TBD | yes | none | `missing_slot` | |
| tx_status | TBD | yes | none | `missing_status` | success/failed normalization |
| signer | TBD | yes | none | `missing_signer` | derivation policy |
| pre_token_balances | TBD | no | fallback to SOL legs | `missing_pre_token_balances` | |
| post_token_balances | TBD | no | fallback to SOL legs | `missing_post_token_balances` | |
| pre_sol_balances | TBD | no | none | `missing_pre_sol_balances` | |
| post_sol_balances | TBD | no | none | `missing_post_sol_balances` | |
| block_time | TBD | no | `Utc::now()` for lag baseline | `missing_block_time` | |
| program_ids | TBD | yes | none | `missing_program_ids` | filter contract |

Required invariants:

1. One emitted `SwapEvent` must represent one clear SOL buy/sell leg or supported pair.
2. Ambiguous multi-output transactions must be dropped deterministically.
3. Failed transactions must never generate swaps.
4. Program filters must remain explicit and auditable.

Suggested approach:

1. Keep existing `SwapParser` validation criteria intact.
2. Create adapter that transforms gRPC tx update -> `RawSwapObservation`.
3. Reuse existing parser for final accept/reject path to preserve behavior.

## 9) Risk Guard and Shadow Nuances

Risk behavior to preserve:

1. `ShadowRiskGuard` should continue using ingestion snapshot deltas.
2. Soft/hard exposure behavior unchanged.
3. Timed pauses unchanged.

Nuances from current runtime:

1. Your `max_signal_lag_seconds` in paper is high (`1200s`), so missed BUY is likely from ingestion drops and/or other gates, not strict lag gate alone.
2. Historical repeated `shadow_risk_pause` spam was observed earlier; treat it as legacy context and keep focus on ingestion path KPIs for this migration.

## 10) Telemetry Requirements Before Cutover

### 10.1 Snapshot Contract (v1 Decision)

`IngestionRuntimeSnapshot` stays compatible with current app/risk guard usage in v1:

1. `ts_utc`
2. `ws_notifications_enqueued`
3. `ws_notifications_replaced_oldest`
4. `rpc_429`
5. `rpc_5xx`
6. `ingestion_lag_ms_p95`

Additional distribution metrics stay in structured logs/counters unless Phase A reopens this decision.

Semantic mapping in `yellowstone_grpc` mode must be explicitly equivalent:

1. `ts_utc`:
   - `Utc::now()` at the moment snapshot is created.
2. `ws_notifications_enqueued`:
   - count of source notifications accepted into the pre-parse queue (field name preserved for compatibility; semantics are transport-agnostic).
3. `ws_notifications_replaced_oldest`:
   - count of queue-overflow replacements under `drop_oldest` policy in the source queue.
4. `rpc_429` / `rpc_5xx`:
   - stay zero in pure gRPC path; increment only if auxiliary HTTP fallback calls are actually executed.
5. `ingestion_lag_ms_p95`:
   - computed from emitted validated swaps as `now_utc - swap.ts_utc`, same as current path.
6. replaced-ratio interpretation:
   - if `queue_overflow_policy=drop_oldest`, `replaced_oldest / enqueued` remains a valid infra pressure signal.
   - if `queue_overflow_policy=block`, ratio is expected near zero; lag thresholds remain the primary guard signal and decode-reject ratio is tracked as rollout telemetry.

### 10.2 Must-Have Metrics

1. `ingestion_lag_ms_p95` in runtime snapshot, plus `p50/p99` in logs/counters
2. `ws_to_fetch_queue_depth` or equivalent stream queue depth
3. `fetch_to_output_queue_depth` or equivalent decode/output queue depth
4. `notifications_enqueued`, `notifications_dropped`, `notifications_replaced_oldest`
5. parse reject counters by reason
6. `rpc_429`, `rpc_5xx` (legacy path only, to confirm offload effect)
7. inbound update counters (`grpc_updates_inbound_total`) to detect quiet-vs-stalled stream
8. decode/parse reject ratio over sliding windows
9. `sqlite_write_retry_total` and `sqlite_busy_error_total`
10. process memory (`rss_mb`) and growth trend

### 10.3 Numeric Acceptance Targets (Hard)

Window definitions:

1. Replay benchmark window: fixed recorded dataset >= 60 minutes.
2. Live canary window: continuous >= 6 hours.

Targets:

1. Replay benchmark:
   - BUY capture delta (`grpc` vs `helius_ws`) >= +15%
   - BUY capture delta is measured only on validated `SwapEvent` outputs (post-parser, deduped by signature), never on raw observations.
   - SELL capture delta does not degrade by more than 3%
2. Live canary:
   - `ingestion_lag_ms_p95 <= 3000ms` for >= 95% of 1-minute buckets
   - `ingestion_lag_ms_p99 <= 6000ms` for >= 90% of 1-minute buckets
   - drop/replaced ratio `< 0.05` sustained (vs current ~0.9 baseline), where:
     - `replaced_ratio = delta(ws_notifications_replaced_oldest) / max(delta(ws_notifications_enqueued), 1)`
     - evaluated only when `queue_overflow_policy=drop_oldest` and `delta(ws_notifications_enqueued) >= 500` per window
3. Stability:
   - no reconnect storm (`> 6 reconnects in 5 minutes`)
   - no DB lock amplification beyond +20% vs baseline (`sqlite_write_retry_total`, `sqlite_busy_error_total`)
4. Memory:
   - RSS growth slope not positive beyond warmup window (after first 30 min)

### 10.4 Replay Reproducibility Contract

Replay A/B must be deterministic and reproducible:

1. Fixture format:
   - canonical NDJSON stream of normalized inbound updates (one update per line).
2. Fixture versioning:
   - dataset stored with immutable ID and checksum (`sha256`) in repo-local manifest.
3. Run configuration:
   - same config except ingestion source under test.
   - fixed random seeds (if any), UTC-only timestamps.
4. Dedup/ordering normalization:
   - compare only post-parser, signature-deduped `SwapEvent` outputs.
   - ordering comparisons use `(slot, signature)` stable keys.
5. Reporting:
   - report includes fixture ID, commit SHA, config hash, output counts by side, drop reasons.

## 11) Test Plan

### Unit Tests

1. Config defaults + env parsing for Yellowstone fields.
2. gRPC message to `RawSwapObservation` mapping.
3. Dedupe TTL/LRU behavior under repeated signatures.
4. Reorder behavior under out-of-order slot arrival.
5. Parser ambiguity handling.

### Integration Tests

1. Synthetic gRPC stream replay fixture -> ingestion -> `SwapEvent` output.
2. Failure/reconnect scenarios with interrupted stream.
3. Backpressure scenarios with bounded channels.

### Regression Tests

1. `cargo test --workspace` remains green.
2. Existing shadow/discovery behavior unchanged on fixed replay dataset.
3. `helius_ws` ingestion path regression test passes after shared dedupe/reorder extraction.

### Soak Test (paper)

Duration: at least 6-24h before production cutover.

Checks:

1. No steady growth in queue depth.
2. No reconnect storm.
3. Stable signal throughput and lower lag.
4. Memory stable after warmup (`rss_mb` plateau).

## 12) Rollout Playbook

### Step 1: Prepare

1. Deploy code with both ingestion sources.
2. Keep default source unchanged.
3. Validate startup with `yellowstone_grpc` in staging.

### Step 2: Canary

1. Start canary instance on separate SQLite file.
2. Enable `yellowstone_grpc` only there.
3. Run replay-based same-input A/B benchmark before evaluating live KPI outcomes.
4. Compare canary vs control every 30 minutes.
5. Keep dual-ingest observation window for at least 14 days after first production enablement.

### Step 3: Partial Production

1. Shift primary runtime to `yellowstone_grpc`.
2. Keep a warm standby config for `helius_ws`.
3. Monitor first hour closely.

### Step 4: Full Promotion

1. Run 24h stable window.
2. Sign off on KPI thresholds.
3. Mark legacy path as fallback-only.

## 13) Rollback Plan

### 13.1 Auto-Failover Policy (Graceful Degradation)

Implement supervised fallback to `helius_ws` when any trigger is hit.
This is **not** in-process source hot-swap; it is external-watchdog restart-based failover.

Trigger conditions (evaluated by watchdog from telemetry/log stream):

1. `ingestion_lag_ms_p95 > 10000ms` for 5 consecutive minutes, or
2. reconnect storm (`>= 6 reconnects in 5 minutes`), or
3. decode/parse reject rate > 20% over 5-minute window with denominator >= 500 inbound updates, or
4. no processed swaps for 120 seconds while stream is connected **and** inbound subscribed transaction updates are non-trivial (>= 200 updates over the same 120-second window).

Failover behavior:

1. Watchdog marks source degraded and emits `ingestion_source_degraded` event.
2. Watchdog writes fallback profile (`ingestion.source=helius_ws`).
3. Watchdog triggers service restart via supervisor (`systemd` Restart policy / watchdog wrapper).
4. Restart picks up fallback profile.
5. Enforce cooldown: do not retry gRPC promotion for 15 minutes.

Operational artifacts (required deliverables):

1. Policy file:
   - `ops/ingestion_failover_policy.toml`
   - stores trigger thresholds and cooldown duration.
2. Watchdog executable:
   - `tools/ingestion_failover_watchdog.sh` (or equivalent service binary).
3. Fallback profile file:
   - `state/ingestion_source_override.env`
   - written atomically: write temp file -> `fsync` -> atomic rename.
4. Cooldown state file:
   - `state/ingestion_failover_cooldown.json`
   - contains `last_failover_ts`, `cooldown_until_ts`, `reason`.
5. Service wiring:
   - systemd unit/timer docs with exact command, restart policy, and log location.

Immediate rollback trigger conditions:

1. gRPC stream instability causing lag/drops worse than baseline.
2. Parser mismatch causing large signal regression.
3. Unexpected risk/discovery regressions.

Rollback actions:

1. Set `ingestion.source=helius_ws`.
2. Restart service.
3. Preserve gRPC metrics/logs for root-cause analysis.
4. Open incident note with exact timestamps and counters.

Recovery objective:

- Rollback should complete in one config change + restart cycle.

## 14) Post-Migration Cleanup Plan

Only after 1-2 weeks stable production:

1. Remove dead code paths tied to HTTP hot fetch if no longer used.
2. Simplify ingestion telemetry labels for single primary path.
3. Document final operational runbook for gRPC mode.

## 15) Pre-Coding Decisions (Locked)

1. Auth contract:
   - required gRPC auth headers and token rotation policy are finalized in Phase A checklist before coding starts.
2. Historical replay mode:
   - deferred, out of scope for v1 migration implementation.
3. Dual-ingest observation duration:
   - fixed to minimum 14 days for first production enablement.
4. Yellowstone major line:
   - v1 migration uses `yellowstone-grpc-client/proto = 12.0.0` baseline.
5. Failover ownership:
   - trigger evaluation and restart action are owned by external watchdog/supervisor.

## 16) Success Criteria (Definition of Done)

Migration is complete when all are true:

1. Production runs on `yellowstone_grpc` as primary for >= 7 days.
2. `ingestion_lag_ms_p95 <= 3000ms` for >= 95% buckets over that window.
3. If `queue_overflow_policy=drop_oldest`, `replaced_ratio < 0.05` with `delta(enqueued) >= 500` in >=95% windows; if policy=`block`, replaced-ratio criterion is N/A and lag/reject-rate criteria must pass.
4. Replay A/B shows BUY capture improvement >= +15% with no strategy-rule loosening.
5. No increase in critical risk incidents or data integrity regressions.

---

## Appendix A: Immediate Temporary Mitigations Before gRPC Cutover

These are temporary and should be removed/reviewed after migration:

1. Increase `fetch_concurrency` carefully if CPU/RPC headroom exists.
2. Add more ingestion HTTP endpoints/keys for current path.
3. Increase queue capacities only with strict memory monitoring.
4. Keep `queue_overflow_policy=drop_oldest` for freshness under overload.

These mitigations do not replace migration; they only reduce pain during transition.
