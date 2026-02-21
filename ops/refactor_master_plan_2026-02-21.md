# Master Plan: Full-Repo Refactor (2026-02-21)

Branch: `feat/yellowstone-grpc-migration`  
Status: Draft for auditor review/corrections  
Goal: Reduce risk and maintenance cost from oversized files (`3k-6k LOC`) without changing runtime behavior.

---

## 1) Why Do This Now

Current codebase has multiple oversized "god files" that already passed functional hardening, but remain expensive to maintain and audit.

Largest files (current snapshot):

1. `crates/app/src/main.rs` — ~5748 LOC
2. `crates/storage/src/lib.rs` — ~3789 LOC
3. `crates/ingestion/src/source.rs` — ~3598 LOC
4. `crates/execution/src/lib.rs` — ~3550 LOC
5. `crates/adapter/src/main.rs` — ~3380 LOC
6. `crates/execution/src/submitter.rs` — ~2028 LOC
7. `crates/config/src/lib.rs` — ~1527 LOC
8. `tools/ops_scripts_smoke_test.sh` — ~1511 LOC
9. `tools/execution_adapter_preflight.sh` — ~1221 LOC
10. `tools/execution_fee_calibration_report.sh` — ~1196 LOC

Main risk today is **change safety and auditability**, not missing functionality.

---

## 2) Refactor Objectives

1. Keep behavior 1:1 (no intentional contract changes in refactor phases).
2. Split large files into coherent modules by domain responsibility.
3. Make audit points explicit and local (smaller files, fewer cross-cutting branches).
4. Reduce blast radius of future changes.
5. Preserve all fail-closed guarantees already implemented.

Non-goals for this plan:

1. No broad feature rewrite.
2. No schema redesign (except when separately approved as performance hardening).
3. No runtime policy changes disguised as "refactor".

---

## 3) Global Guardrails

Each refactor slice must satisfy all:

1. `cargo fmt --all`
2. `cargo test --workspace -q`
3. `tools/ops_scripts_smoke_test.sh`
4. No behavior change in scripted outputs unless explicitly declared.
5. No silent config/contract drift.

Process guardrails:

1. Max scope per commit: one domain slice.
2. Prefer move+wire first, then tiny cleanup in follow-up commits.
3. Keep backportability to older server heads in mind (avoid unnecessary coupling).

---

## 4) Target Architecture (By Crate)

## 4.1 `copybot-adapter` (`crates/adapter/src/main.rs`)

Current issue: HTTP handlers, config parsing, auth, upstream failover, send-RPC, verify-RPC, and helpers in one file.

Target split:

1. `crates/adapter/src/main.rs` — bootstrap + router wiring only.
2. `crates/adapter/src/config.rs` — `AdapterConfig::from_env`, env parsing/validation.
3. `crates/adapter/src/auth.rs` — bearer/hmac verify, nonce/ttl logic.
4. `crates/adapter/src/handlers.rs` — `simulate`, `submit`, JSON response shaping.
5. `crates/adapter/src/upstream.rs` — upstream forwarding + outcome normalization.
6. `crates/adapter/src/send_rpc.rs` — sendTransaction path + signature checks/classification.
7. `crates/adapter/src/submit_verify.rs` — post-submit visibility checks.
8. `crates/adapter/src/contract.rs` — contract validators (`validate_*`, common invariants).
9. `crates/adapter/src/http_utils.rs` — endpoint identity/redaction/error classification.

Acceptance:

1. Existing adapter tests unchanged in semantics.
2. Same error codes and retryability classifications.
3. Same response contract fields.

---

## 4.2 `copybot-app` (`crates/app/src/main.rs`)

Current issue: startup, runtime contracts, task orchestration, risk logic, queueing, stale close, telemetry in one file.

Target split:

1. `crates/app/src/main.rs` — startup/bootstrap only.
2. `crates/app/src/config_contract.rs` — execution/risk/shadow contract validators.
3. `crates/app/src/secrets.rs` — adapter secret file resolution/loading.
4. `crates/app/src/app_loop.rs` — main event loop orchestration.
5. `crates/app/src/infra_risk_gate.rs` — outage/parser-stall/no-progress logic.
6. `crates/app/src/shadow_queue.rs` — shadow task queue/overflow/scheduling.
7. `crates/app/src/stale_close.rs` — stale lot close + risk event writing.
8. `crates/app/src/task_spawns.rs` — worker/discovery/shadow/execution spawn helpers.
9. `crates/app/src/telemetry.rs` — formatting/aggregation helpers.

Acceptance:

1. Existing risk-gate tests remain green.
2. Stale-close behavior unchanged.
3. Runtime startup validation errors text-compatible where feasible.

---

## 4.3 `copybot-execution`

Files in scope:

1. `crates/execution/src/lib.rs` (~3550)
2. `crates/execution/src/submitter.rs` (~2028)
3. `crates/execution/src/simulator.rs` (~991)

Target split:

1. `crates/execution/src/lib.rs` -> facade + minimal `ExecutionRuntime`.
2. New modules:
   1. `runtime_init.rs` (mode wiring)
   2. `batch_processor.rs` (process_batch orchestration)
   3. `risk_gates.rs` (daily loss / drawdown / infra interactions)
   4. `route_policy.rs` (route order + tip/counters helpers)
   5. `telemetry.rs` (batch report counters/sums)
3. `submitter.rs` split:
   1. `submitter/http.rs`
   2. `submitter/policy_echo.rs`
   3. `submitter/fee_hints.rs`
   4. `submitter/parsing.rs`
4. `simulator.rs` split:
   1. request/response parser
   2. endpoint retry/fallback loop
   3. redaction/log helpers

Acceptance:

1. Existing unit tests in execution crate all pass unchanged.
2. No change in submit/simulate fail-open/fail-closed behavior.
3. Same error code taxonomy.

---

## 4.4 `copybot-storage` (`crates/storage/src/lib.rs`)

Current issue: migrations, lifecycle writes, analytics queries, pricing helpers, drawdown/unrealized logic all in one file.

Target split:

1. `crates/storage/src/lib.rs` — public exports + `SqliteStore` struct + open/migration entry.
2. `crates/storage/src/migrations.rs` — migration discovery/apply.
3. `crates/storage/src/execution_orders.rs` — order lifecycle CRUD.
4. `crates/storage/src/fills_positions.rs` — fill/position finalize paths.
5. `crates/storage/src/shadow.rs` — shadow lots/trades/snapshots.
6. `crates/storage/src/pricing.rs` — reliable price helpers.
7. `crates/storage/src/risk_metrics.rs` — pnl/drawdown/unrealized calculations.
8. `crates/storage/src/discovery.rs` — discovery wallets/metrics paths.
9. `crates/storage/src/risk_events.rs` — risk event insert/query helpers.
10. `crates/storage/src/sqlite_retry.rs` — contention retry utilities/counters.

Acceptance:

1. No SQL semantic drift (same SQL text unless intentional and reviewed).
2. Migration behavior unchanged.
3. Existing storage tests plus workspace tests pass.

---

## 4.5 `copybot-ingestion` (`crates/ingestion/src/source.rs`)

Current issue: source abstraction + Helius + Yellowstone + queues + telemetry in one huge file.

Target split:

1. `crates/ingestion/src/source/mod.rs` — public source API.
2. `crates/ingestion/src/source/mock.rs`
3. `crates/ingestion/src/source/helius_ws.rs`
4. `crates/ingestion/src/source/yellowstone_grpc.rs`
5. `crates/ingestion/src/source/overflow_queue.rs`
6. `crates/ingestion/src/source/telemetry.rs`
7. `crates/ingestion/src/source/reorder.rs`

Acceptance:

1. Runtime snapshot fields unchanged.
2. Fallback counters and parse counters unchanged.
3. No throughput regression in smoke/local runs.

---

## 4.6 `copybot-config` (`crates/config/src/lib.rs`)

Current issue: schema definitions + defaulting + env parsing + validation helpers all in one file.

Target split:

1. `crates/config/src/lib.rs` — public API exports.
2. `crates/config/src/schema.rs` — config structs/defaults.
3. `crates/config/src/load.rs` — file load + merge entrypoints.
4. `crates/config/src/env_parsers.rs` — typed env parsing helpers.
5. `crates/config/src/env_overrides.rs` — override application per section.
6. `crates/config/src/validation.rs` — shared validation constants/helpers.

Acceptance:

1. Same env precedence semantics.
2. Same fail-closed parsing behavior.
3. Existing env tests preserved.

---

## 4.7 `copybot-discovery` and `copybot-shadow`

Files:

1. `crates/discovery/src/lib.rs` (~1141)
2. `crates/shadow/src/lib.rs` (~895)

Target split:

1. `discovery`: window state, scoring, quality/rpc cache, followlist sync.
2. `shadow`: candidate conversion, quality gates, signal recording, snapshot extraction.

Acceptance:

1. Same drop reasons and follow semantics.
2. Same quality gate enforcement.

---

## 4.8 Ops Scripts (`tools/*.sh`)

Largest script risk is readability and duplicated parsing helpers.

Target split (incremental, safe):

1. Add `tools/lib/common.sh` for shared helpers (`trim`, `extract_field`, bool normalization, artifact write).
2. Add `tools/lib/toml_env.sh` for shared key/value parsing utilities.
3. Keep behavior identical; only de-duplicate helper logic.
4. Keep `tools/ops_scripts_smoke_test.sh` as integration harness, but move fixture writers into sourced helper file when stable.

Acceptance:

1. `tools/ops_scripts_smoke_test.sh` PASS before/after.
2. Script outputs stable for key fields used by automation.

---

## 5) Phased Execution Plan

## Phase 0 — Baseline Freeze (1-2 days)

1. Snapshot current behavior:
   1. store canonical outputs from:
      1. `tools/execution_go_nogo_report.sh`
      2. `tools/execution_devnet_rehearsal.sh`
      3. `tools/adapter_rollout_evidence_report.sh`
2. Lock golden tests and smoke in CI expectations.
3. Define refactor "no-behavior-change" checklist template for auditors.

Deliverable:

1. `ops/refactor_baseline_evidence_YYYY-MM-DD.md` (optional but recommended).

## Phase 1 — Adapter Modularization (high ROI, low cross-crate blast)

1. Split `crates/adapter/src/main.rs` into modules listed in 4.1.
2. Preserve public HTTP behavior exactly.
3. Keep `main.rs` as thin bootstrap (router + state init).

Exit criteria:

1. Adapter crate tests pass unchanged.
2. Workspace + smoke pass.
3. Audit check on error code parity.

## Phase 2 — App Runtime Decomposition

1. Split `crates/app/src/main.rs` per 4.2.
2. Move risk/outage/parser-stall logic into dedicated module.
3. Move queue mechanics out of top-level file.

Exit criteria:

1. Existing risk tests green.
2. No telemetry key regressions.
3. Audit on gate precedence.

## Phase 3 — Execution Runtime Decomposition

1. Split `crates/execution/src/lib.rs` and `submitter.rs`.
2. Isolate policy echo + fee hint parsing.
3. Keep retry/terminal taxonomy stable.

Exit criteria:

1. Existing submitter/simulator tests green.
2. No contract drift against adapter.
3. Audit on fail-closed paths.

## Phase 4 — Storage Decomposition

1. Split `crates/storage/src/lib.rs` by domain modules.
2. Keep SQL constants and statements intact where possible.
3. Add light SQL string snapshot tests for critical queries (optional but valuable).

Exit criteria:

1. Workspace green.
2. No migration changes.
3. Audit on pricing/risk query parity.

## Phase 5 — Ingestion + Config Decomposition

1. Split ingestion source monolith per 4.5.
2. Split config monolith per 4.6.
3. Ensure env override behavior unchanged.

Exit criteria:

1. Smoke + workspace pass.
2. Ingestion runtime snapshot contract unchanged.
3. Audit on parser/fallback counters.

## Phase 6 — Script Library Extraction

1. Deduplicate shell helper logic into `tools/lib/*`.
2. Keep top-level script interfaces stable.
3. Re-run full smoke after each helper extraction step.

Exit criteria:

1. Smoke PASS.
2. No changed output fields used by automation.

---

## 6) Priority Order (Recommended)

1. Adapter
2. App
3. Execution
4. Storage
5. Ingestion
6. Config
7. Scripts
8. Discovery/Shadow polish

Reason: highest-risk runtime paths first, but with already strong test coverage.

---

## 7) Risk Register

1. Silent contract drift in fail-closed paths
   1. Mitigation: golden tests + explicit audit checklist per phase.
2. Regression from move-only commits mixed with logic changes
   1. Mitigation: strict two-step pattern:
      1. move/wire
      2. cleanup
3. Cross-crate coupling breakage
   1. Mitigation: phase boundaries by crate + workspace test after each slice.
4. Script output drift breaking ops automation
   1. Mitigation: smoke assertions on exact key lines.

---

## 8) Definition of Done (Whole Plan)

1. No file > 2000 LOC in runtime-critical crates (`app`, `execution`, `adapter`, `storage`, `ingestion`, `config`).
2. All existing tests + smoke remain green.
3. All rollout/audit helper outputs stable and documented.
4. Refactor packages reviewed by auditors in phase batches.
5. No unresolved behavior-drift findings.

---

## 9) Auditor Review Checklist (For This Plan)

Please validate:

1. Phase order minimizes production risk.
2. Proposed module boundaries are practical for Rust visibility/ownership.
3. No hidden behavior changes are implied by structure moves.
4. Missing critical refactor targets (if any).
5. Whether to add strict LOC budget thresholds per phase.

---

## 10) Immediate Next Step (If Approved)

Start Phase 1 with a strict "move-only" slice:

1. Extract `send_rpc.rs` from `crates/adapter/src/main.rs`.
2. Extract `submit_verify.rs`.
3. Keep `main.rs` routing + handler dispatch.

Then run:

```bash
cargo fmt --all
cargo test -p copybot-adapter -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

And publish a dedicated audit package for Phase 1.
