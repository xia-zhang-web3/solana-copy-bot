# Master Plan: Runtime and Ops Refactor Program (2026-02-21)

Version: `rev-3`  
Branch: `feat/yellowstone-grpc-migration`  
Status: Ready for auditor review and execution

---

## 1) Scope and Intent

This plan is for **structural refactoring** of runtime-critical and ops-critical code paths.

Primary objective:

1. Improve change safety, auditability, and maintenance speed.

Hard constraints:

1. No intentional behavior changes in refactor phases.
2. Preserve fail-closed semantics across runtime and ops gates.
3. Keep production rollout/backport safety for server heads.

In-scope domains:

1. `copybot-adapter`
2. `copybot-app`
3. `copybot-execution`
4. `copybot-storage`
5. `copybot-ingestion`
6. `copybot-config`
7. `copybot-discovery`
8. `copybot-shadow`
9. Ops scripts in `tools/`

Out-of-scope for this program:

1. Feature redesign.
2. Schema redesign unless explicitly approved in a separate hardening task.
3. Policy/contract changes disguised as refactor.

---

## 2) Current Monolith Inventory (Verified)

Current snapshot:

1. `crates/app/src/main.rs` — `5748` LOC
2. `crates/storage/src/lib.rs` — `3789` LOC
3. `crates/ingestion/src/source.rs` — `3598` LOC
4. `crates/execution/src/lib.rs` — `3550` LOC
5. `crates/adapter/src/main.rs` — `3380` LOC
6. `crates/execution/src/submitter.rs` — `2028` LOC
7. `crates/config/src/lib.rs` — `1527` LOC
8. `tools/ops_scripts_smoke_test.sh` — `1511` LOC
9. `tools/execution_adapter_preflight.sh` — `1221` LOC
10. `tools/execution_fee_calibration_report.sh` — `1196` LOC
11. `tools/ingestion_ab_report.sh` — `785` LOC

---

## 3) Program Governance

Roles:

1. Refactor owner: developer implementing slices.
2. Runtime sign-off owner: maintainer responsible for production behavior parity.
3. Ops sign-off owner: maintainer responsible for script/report contract parity.
4. Audit sign-off authority: two independent auditors per major phase.

SLA and sign-off:

1. Every phase requires:
   1. Internal green verification.
   2. Two-auditor review package.
   3. Explicit sign-off before next phase starts.
2. Critical findings (`High`) block phase closure.
3. `Medium` may pass only with documented follow-up owner and deadline.

---

## 4) Global Guardrails (Mandatory)

### 4.1 Commit discipline

1. One domain slice per commit.
2. `Move + wire` first.
3. Cleanup and local polish only in follow-up commit.
4. No mixed semantic refactor plus behavior change.

### 4.2 Mandatory checks for every slice

1. `cargo fmt --all`
2. `cargo test --workspace -q`
3. `tools/ops_scripts_smoke_test.sh`
4. Phase-specific targeted regression pack (defined in phase cards).

### 4.3 Script safety gates

For any changed shell script:

1. `bash -n <script>` for syntax.
2. `shellcheck -x <script>` for compatibility and `set -euo pipefail` hazards.
3. Smoke pass must include the touched path.

Shellcheck execution policy:

1. CI is the source of truth and must run `shellcheck -x`.
2. Local execution must run either:
   1. native `shellcheck`, or
   2. dockerized shellcheck fallback.
3. Missing local shellcheck is not a reason to skip CI shellcheck gate.

### 4.4 Module size budgets

Budgets apply to runtime code only, excluding `#[cfg(test)]` blocks.

1. Soft target: `<= 800` LOC per module.
2. Hard cap: `<= 1200` LOC per module.
3. Exception policy:
   1. Explicitly documented in phase evidence.
   2. Must include reduction plan and deadline.

### 4.5 SQL parity policy

Do not require byte-identical SQL text.
Use behavior parity:

1. Query result parity on deterministic fixture DB.
2. Invariant parity for critical paths (pricing, drawdown, finalize).
3. Query-plan/perf sanity only where relevant.

### 4.6 LOC measurement protocol

Single method for auditor repeatability:

1. Phase evidence must report:
   1. raw file LOC (`wc -l`)
   2. runtime LOC excluding `#[cfg(test)]` blocks
2. Runtime LOC measurement command must come from one shared helper script:
   1. `tools/refactor_loc_report.sh`
3. If helper script changes, include before/after sample output in phase evidence.

---

## 5) Baseline Evidence (Phase 0, Mandatory)

This baseline is **required**, not optional.

Deliverable file:

1. `ops/refactor_baseline_evidence_YYYY-MM-DD.md`

Evidence folder convention:

1. `ops/evidence/refactor/baseline/<short_sha>/`

Mandatory baseline content:

1. Git commit SHA.
2. Exact commands executed.
3. Exit codes.
4. Artifact checksum manifest.
5. Normalized outputs for deterministic comparison.
6. Storage location for raw artifacts.

Artifact storage policy:

1. Commit to git:
   1. summary evidence markdown
   2. checksum manifest
   3. normalized text snippets needed for audit traceability
2. Do not commit large raw captures by default.
3. Store raw captures in:
   1. `ops/evidence/refactor/...` if small and reviewable, or
   2. CI artifact storage with retention policy
4. Evidence markdown must include stable pointers to raw artifact location.

Mandatory baseline commands:

```bash
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh

bash -n tools/adapter_secret_rotation_report.sh
bash -n tools/execution_devnet_rehearsal.sh
bash -n tools/adapter_rollout_evidence_report.sh
bash -n tools/execution_go_nogo_report.sh

# shellcheck policy:
# - CI: mandatory
# - Local: mandatory via native shellcheck or docker fallback
shellcheck -x tools/adapter_secret_rotation_report.sh
shellcheck -x tools/execution_devnet_rehearsal.sh
shellcheck -x tools/adapter_rollout_evidence_report.sh
shellcheck -x tools/execution_go_nogo_report.sh
# docker fallback example when local shellcheck is unavailable:
# docker run --rm -v "$PWD:/work" -w /work koalaman/shellcheck:stable \
#   shellcheck -x tools/adapter_secret_rotation_report.sh tools/execution_devnet_rehearsal.sh \
#   tools/adapter_rollout_evidence_report.sh tools/execution_go_nogo_report.sh

# deterministic orchestrator captures (required):
# Preconditions:
# - fixture config/env are prepared under tmp/refactor-baseline/
# - deterministic test-mode flags are set
GO_NOGO_TEST_MODE=true GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
  CONFIG_PATH=tmp/refactor-baseline/devnet_rehearsal.toml OUTPUT_DIR=tmp/refactor-baseline/go_nogo \
  bash tools/execution_go_nogo_report.sh 24 60 > tmp/refactor-baseline/go_nogo_stdout.txt

RUN_TESTS=false DEVNET_REHEARSAL_TEST_MODE=true GO_NOGO_TEST_MODE=true \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
  CONFIG_PATH=tmp/refactor-baseline/devnet_rehearsal.toml OUTPUT_DIR=tmp/refactor-baseline/rehearsal \
  bash tools/execution_devnet_rehearsal.sh 24 60 > tmp/refactor-baseline/rehearsal_stdout.txt

ADAPTER_ENV_PATH=tmp/refactor-baseline/adapter.env CONFIG_PATH=tmp/refactor-baseline/devnet_rehearsal.toml \
  RUN_TESTS=false DEVNET_REHEARSAL_TEST_MODE=true GO_NOGO_TEST_MODE=true \
  GO_NOGO_TEST_FEE_VERDICT_OVERRIDE=PASS GO_NOGO_TEST_ROUTE_VERDICT_OVERRIDE=PASS \
  OUTPUT_DIR=tmp/refactor-baseline/rollout \
  bash tools/adapter_rollout_evidence_report.sh 24 60 > tmp/refactor-baseline/rollout_stdout.txt

sha256sum tmp/refactor-baseline/go_nogo_stdout.txt \
  tmp/refactor-baseline/rehearsal_stdout.txt \
  tmp/refactor-baseline/rollout_stdout.txt > tmp/refactor-baseline/orchestrators.sha256
```

Deterministic output strategy:

1. Use fixture-driven smoke outputs as golden source.
2. Do not use production live outputs as baseline canon.
3. Orchestrator canonical baseline must include the three direct script captures above.
4. Normalize volatile fields in generated artifacts:
   1. timestamps
   2. absolute temp paths
   3. durations
5. Fixture preparation is part of Phase 0 and must be documented step-by-step in baseline evidence.

---

## 6) Measurable Parity Criteria

### 6.1 Functional parity

1. No new failing tests.
2. No changed verdict semantics in gate scripts.
3. No changed fail-closed/fail-open classifications.

### 6.2 Telemetry and output parity

Required key fields must remain present and parseable.

Runtime/report keys:

1. `adapter_rollout_verdict`
2. `rotation_readiness_verdict`
3. `devnet_rehearsal_verdict`
4. `overall_go_nogo_verdict`
5. `preflight_verdict`
6. `fee_decomposition_verdict`
7. `route_profile_verdict`

Ingestion/risk keys and counters (where applicable):

1. `ws_notifications_enqueued`
2. `parse_rejected_total`
3. `grpc_decode_errors`
4. `grpc_transaction_updates_total`

### 6.3 Performance sanity thresholds

On the same machine and same fixture:

1. `tools/ops_scripts_smoke_test.sh` median wall-time regression must be `<= 25%` vs baseline (3-run median).
2. No single targeted test may regress by `> 50%` unless explained and accepted in phase evidence.

---

## 7) Refactor Strategy by Domain

### 7.1 Adapter (`crates/adapter/src/main.rs`)

Target split:

1. `main.rs` bootstrap and router only.
2. `config.rs`
3. `auth.rs`
4. `handlers.rs`
5. `upstream.rs`
6. `send_rpc.rs`
7. `submit_verify.rs`
8. `contract.rs`
9. `http_utils.rs`

Test strategy:

1. Keep handler-level integration tests close to handler modules.
2. Keep shared HTTP fixtures in dedicated test helper module.
3. Avoid widening visibility beyond `pub(crate)` unless required.

### 7.2 App (`crates/app/src/main.rs`)

Refactor in three sub-phases to control blast radius.

Phase 2a (safe extraction, no core loop redesign):

1. `config_contract.rs`
2. `secrets.rs`
3. `telemetry.rs`
4. `stale_close.rs`
5. `task_spawns.rs`
6. `swap_classification.rs`

Phase 2b (state consolidation, moderate risk):

1. Introduce `shadow_scheduler.rs` to consolidate queue state.
2. Extract operator emergency-stop state and helpers.

Phase 2c (optional, high risk, defer unless needed):

1. Event-loop orchestration redesign.
2. Requires dedicated RFC and separate audits.

### 7.3 Execution (`crates/execution`)

Existing modular structure must be preserved and leveraged.

Focus areas:

1. `lib.rs` decomposition into:
   1. `runtime.rs`
   2. `pipeline.rs`
   3. `confirmation.rs`
   4. `batch_report.rs`
2. `submitter.rs` extraction of parser/policy helpers where ROI is real.
3. Keep `simulator.rs` stable unless clear gain; avoid fragmentation for its own sake.

### 7.4 Storage (`crates/storage/src/lib.rs`)

Target split:

1. `migrations.rs`
2. `execution_orders.rs`
3. `fills_positions.rs`
4. `shadow.rs`
5. `pricing.rs`
6. `risk_metrics.rs`
7. `discovery.rs`
8. `risk_events.rs`
9. `sqlite_retry.rs`
10. `transactions.rs` for cross-domain atomic operations.

Critical rule:

1. Cross-domain atomic flow (e.g. finalize confirmed order) must remain coordinated in one transaction boundary.

### 7.5 Ingestion (`crates/ingestion/src/source.rs`)

Target split:

1. `source/mod.rs`
2. `source/mock.rs`
3. `source/helius_ws.rs`
4. `source/yellowstone_grpc.rs`
5. `source/reorder.rs`
6. `source/overflow_queue.rs`
7. `source/telemetry.rs`

Explicit dedup goal:

1. Reorder-buffer logic parity and de-dup between Helius/Yellowstone paths.

### 7.6 Config (`crates/config/src/lib.rs`)

Target split:

1. `schema.rs`
2. `load.rs`
3. `env_parsers.rs`
4. `env_overrides.rs`
5. `validation.rs`

Optional reduction opportunity:

1. Consider macro-driven env override mapping after move-only phase stabilizes.

### 7.7 Discovery and Shadow

Target split:

1. Discovery: windows, scoring, quality cache, followlist sync.
2. Shadow: candidate conversion, quality gates, signal recording, snapshots.

### 7.8 Ops scripts (`tools/*.sh`)

Target split:

1. `tools/lib/common.sh`
2. `tools/lib/toml_env.sh`

Rules:

1. Keep top-level interfaces stable.
2. No output contract drift.
3. Enforce shellcheck and smoke on each helper extraction slice.

---

## 8) Phase Order (Risk-Minimized)

1. Phase 0: Baseline evidence freeze.
2. Phase 1: Adapter modularization.
3. Phase 2a: App safe extraction.
4. Phase 3: Storage decomposition.
5. Phase 4: Execution decomposition.
6. Phase 5A: Ingestion decomposition.
7. Phase 5B: Config decomposition.
8. Phase 6: App state consolidation (2b).
9. Phase 7: Ops script library extraction.
10. Phase 8: Discovery/Shadow polish.
11. Phase 9: App event-loop redesign (2c, optional/deferred).

Rationale:

1. Start with adapter (isolated binary, strong tests).
2. De-risk app by splitting safe and stateful work.
3. Place storage before execution due to dependency surface.
4. Split ingestion/config instead of one combined phase.

---

## 9) Phase Cards with Mandatory Regression Packs

## Phase 0: Baseline Freeze (mandatory)

Scope:

1. Build deterministic baseline evidence.

Mandatory regression pack:

```bash
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Exit criteria:

1. Baseline evidence file committed.
2. Checksum manifest committed.
3. Raw artifact storage location documented and reachable.
4. Auditor acknowledgment of baseline completeness.

## Phase 1: Adapter modularization

Scope:

1. Split `crates/adapter/src/main.rs` into modules from section 7.1.
2. No contract-level behavior changes.

Mandatory regression pack:

```bash
cargo test -p copybot-adapter -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Targeted checks:

```bash
cargo test -p copybot-adapter -q send_signed_transaction_via_rpc_rejects_signature_mismatch
cargo test -p copybot-adapter -q send_signed_transaction_via_rpc_uses_fallback_auth_token_when_retrying
cargo test -p copybot-adapter -q verify_submit_signature_rejects_when_onchain_error_seen
```

Exit criteria:

1. Error code/retryability parity confirmed.
2. HTTP response schema parity confirmed.
3. No unresolved High/Medium findings.

## Phase 2a: App safe extraction

Scope:

1. Extract non-core-loop domains only.
2. Keep core loop behavior unchanged.

Mandatory regression pack:

```bash
cargo test -p copybot-app -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Targeted checks:

```bash
cargo test -p copybot-app -q risk_guard_infra_blocks_when_parser_stall_detected
cargo test -p copybot-app -q risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold
cargo test -p copybot-app -q risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary
cargo test -p copybot-app -q stale_lot_cleanup_ignores_micro_swap_outlier_price
cargo test -p copybot-app -q stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing
cargo test -p copybot-app -q risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades
```

Exit criteria:

1. Gate precedence unchanged.
2. Stale-close behavior unchanged.
3. No telemetry key drift.

## Phase 3: Storage decomposition

Scope:

1. Split storage by domain.
2. Preserve transaction boundaries and migration behavior.

Mandatory regression pack:

```bash
cargo test -p copybot-storage -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Targeted checks:

```bash
cargo test -p copybot-storage -q live_unrealized
cargo test -p copybot-storage -q live_unrealized_pnl_sol_ignores_micro_swap_outlier_price
cargo test -p copybot-storage -q live_unrealized_pnl_sol_counts_missing_when_only_micro_quotes_exist
```

Exit criteria:

1. Deterministic query-result parity evidence generated.
2. Migration and finalize flows unchanged.
3. No unresolved High findings.

## Phase 4: Execution decomposition

Scope:

1. Decompose `lib.rs` and targeted `submitter.rs` areas.
2. Preserve fail-closed semantics.

Mandatory regression pack:

```bash
cargo test -p copybot-execution -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Targeted checks:

```bash
cargo test -p copybot-execution -q adapter_intent_simulator_does_not_fallback_on_invalid_json_terminal_reject
cargo test -p copybot-execution -q adapter_intent_simulator_redacts_endpoint_on_retryable_send_error
```

Exit criteria:

1. Retryable/terminal taxonomy parity verified.
2. Submit/simulate contract parity verified.
3. No adapter/execution drift in strict echo behavior.

## Phase 5A: Ingestion decomposition

Scope:

1. Split source monolith.
2. Centralize reorder logic.

Mandatory regression pack:

```bash
cargo test -p copybot-ingestion -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Targeted checks:

```bash
cargo test -p copybot-app -q risk_guard_infra_no_progress_does_not_block_when_grpc_transaction_updates_advance
cargo test -p copybot-app -q risk_guard_infra_no_progress_still_blocks_when_only_grpc_ping_total_advances
```

Exit criteria:

1. Runtime snapshot key parity.
2. Reorder behavior parity with fixture-driven checks.

## Phase 5B: Config decomposition

Scope:

1. Split schema/load/parsers/overrides/validation.
2. Preserve override precedence and fail-closed parse semantics.

Mandatory regression pack:

```bash
cargo test -p copybot-config -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Targeted checks:

1. Existing env override and parser tests must pass unchanged.
2. Execution preflight and go/no-go smoke cases remain stable.

Exit criteria:

1. Env precedence unchanged.
2. Route map/env parse fail-closed behavior unchanged.

## Phase 6: App state consolidation (2b)

Scope:

1. Introduce `ShadowScheduler` style state holder.
2. Reduce mutable local sprawl in loop without redesigning orchestration model.

Mandatory regression pack:

```bash
cargo test -p copybot-app -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Exit criteria:

1. No queueing/risk behavior drift.
2. Local-state coupling reduced and documented.

## Phase 7: Ops scripts extraction

Scope:

1. Extract shared helpers to `tools/lib/*`.
2. Keep script interfaces stable.

Mandatory regression pack:

```bash
bash -n tools/adapter_secret_rotation_report.sh
bash -n tools/execution_devnet_rehearsal.sh
bash -n tools/adapter_rollout_evidence_report.sh
bash -n tools/execution_go_nogo_report.sh
shellcheck -x tools/adapter_secret_rotation_report.sh
shellcheck -x tools/execution_devnet_rehearsal.sh
shellcheck -x tools/adapter_rollout_evidence_report.sh
shellcheck -x tools/execution_go_nogo_report.sh
tools/ops_scripts_smoke_test.sh
```

Exit criteria:

1. Output contract parity for automation keys.
2. No shell compatibility regressions.

## Phase 8: Discovery and Shadow polish

Scope:

1. Split remaining large files in discovery/shadow where ROI is clear.

Mandatory regression pack:

```bash
cargo test -p copybot-discovery -q
cargo test -p copybot-shadow -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Exit criteria:

1. Quality gate and followlist behavior unchanged.
2. No new audit findings above Low.

## Phase 9 (Optional): App loop redesign (2c)

Scope:

1. Only if size/maintainability still unacceptable after Phase 6.
2. Requires dedicated RFC and separate approval.

Mandatory regression pack:

```bash
cargo test -p copybot-app -q
cargo test --workspace -q
tools/ops_scripts_smoke_test.sh
```

Exit criteria:

1. Explicit architecture review approved.
2. Separate audits completed.

---

## 10) Rollback Matrix (Per Phase)

### 10.1 Phase branch and merge protocol (mandatory)

To make rollback predictable for multi-commit phases:

1. Each phase executes on dedicated branch:
   1. `refactor/phase-<id>-<topic>`
2. Create tags:
   1. `phase-<id>-start`
   2. `phase-<id>-end`
3. Merge to main branch as single `--no-ff` merge commit.
4. Default rollback path for a completed phase:
   1. revert the phase merge commit
5. If emergency hotfix lands mid-phase:
   1. rebase phase branch on hotfix
   2. regenerate phase evidence before merge

Rollback command model:

1. Never use destructive reset.
2. Preferred: `git revert <phase_merge_commit>`.
3. Fallback (unmerged phase): `git revert <phase_commit_or_range>`.
4. Re-run phase regression pack post-revert.

| Phase | Rollback trigger | Rollback path | Max rollback window | Owner |
|---|---|---|---|---|
| 0 | Baseline missing/incomplete | Revert baseline commit, regenerate evidence | Same day | Refactor owner |
| 1 | Any contract drift in adapter responses/error codes | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 2a | Risk gate or stale-close drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 3 | Pricing/finalize/migration parity drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 4 | Retryable/terminal taxonomy drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 5A | Snapshot/ingestion counter drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 5B | Env precedence or parse drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 6 | Queue/scheduler behavior drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 7 | Script output contract drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Ops owner |
| 8 | Discovery/shadow behavior drift | Revert phase merge commit (or latest slice commits if unmerged) | 24h | Runtime owner |
| 9 | Any event-loop orchestration drift | Revert full phase branch merge | 12h | Runtime owner |

---

## 11) Program DoD (Updated)

Program done when all are true:

1. All mandatory phases completed and signed off.
2. Baseline and per-phase evidence packages archived.
3. No unresolved High findings.
4. Runtime-critical module sizes comply with budget policy, with explicit exceptions documented.
5. Workspace tests + smoke stable across phases.

LOC outcome target:

1. Runtime files should trend to `< 2000 LOC` where practical.
2. For exceptional files, enforce documented cap and follow-up milestone.
3. LOC checks exclude `#[cfg(test)]` sections.

---

## 12) Immediate Execution Start

First executable slice after plan approval:

1. Phase 0 baseline evidence (mandatory).
2. Phase 1 adapter move-only micro-slice #1:
   1. Extract `http_utils` only.
3. Phase 1 adapter move-only micro-slice #2:
   1. Extract `send_rpc` only.
4. Phase 1 adapter move-only micro-slice #3:
   1. Extract `submit_verify` only.

Each micro-slice must run full mandatory pack before next slice.

---

## 13) Auditor Checklist (for this plan revision)

Please verify:

1. Baseline is mandatory and deterministic.
2. Rollback matrix is explicit and operational.
3. Phase order reduces blast radius.
4. App split (`2a/2b/2c`) is realistic.
5. Ingestion/config split into `5A/5B` is explicit.
6. Regression packs are mandatory per phase.
7. Telemetry/perf criteria are measurable.
8. Script compatibility gates include `shellcheck`.
9. LOC budget policy and exceptions are clear.
10. Ownership and sign-off authority are explicit.
