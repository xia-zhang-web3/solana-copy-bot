# Executor Backend Master Plan (Rev-4)

Date: 2026-02-24  
Owner: execution-dev  
Scope owner: runtime-ops + execution-dev

## 1) Goal

Реализовать production-grade `copybot-executor` как upstream backend для `copybot-adapter`, чтобы закрыть обязательный code-gap Stage C/C.5:

1. реальный `/simulate` + `/submit` backend за adapter-контрактом;
2. route-aware исполнение (`rpc`, `jito`, `fastlane`) с fail-closed fallback-policy;
3. Solana transaction execution core (build -> sign -> send -> normalize response);
4. auditable evidence-chain для devnet rehearsal и rollout.

## 2) Current State (2026-02-24)

1. Stage A (Yellowstone observation) закрыт и архивирован: `ops/yellowstone_observation_closure_2026-02-24.md`.
2. Runtime + adapter контракт готовы и протестированы.
3. `copybot-adapter` ожидает upstream:
   1. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL`
   2. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL`
4. Отсутствует собственный executor service (в workspace нет `crates/executor`).

## 3) Non-Goals

1. Не переписываем execution runtime архитектуру.
2. Не меняем ingestion/Yellowstone pipeline.
3. Не включаем live submit до закрытия Stage B + Stage C.5 gates.
4. Не даем profit-гарантий; цель — корректный и безопасный execution backend.

## 4) Mandatory Constraints

1. Backward compatibility с текущим adapter/runtime контрактом (`v1`).
2. Fail-closed semantics для unknown/malformed statuses и контрактных mismatch.
3. Error-code compatibility для guarded fallback policy (`jito|fastlane -> rpc`).
4. Durable idempotency обязательна; in-memory-only модель запрещена.
5. Custody/signing policy обязательна; “raw private key in repo/env” запрещено.
6. No destructive rollout operations (`git reset --hard`/forced DB hacks запрещены).

## 5) Definition of Done

Проект по executor считается закрытым только если одновременно:

1. В workspace добавлен `crates/executor` бинарь `copybot-executor`.
2. Executor реализует:
   1. `POST /simulate`
   2. `POST /submit`
   3. `GET /healthz`
3. Реализован Solana execution core:
   1. swap tx build
   2. compute-budget/tip wiring
   3. ATA handling
   4. blockhash lifecycle
   5. signing + submission
4. Реализована custody/signing модель c rotation policy и fail-closed startup checks.
5. Реализована durable idempotency-модель с restart safety.
6. Adapter успешно работает с executor upstream в devnet rehearsal.
7. Route behavior:
   1. `rpc` path — production-ready
   2. `jito` path — production-ready (required)
   3. `fastlane` path — optional, behind explicit feature flag
8. Error taxonomy и retryability подтверждены тестами и ops evidence.
9. Новые ops helpers (preflight/rehearsal/evidence) и runbook-документация добавлены.
10. Итоговый пакет доказательств приложен в `ops/evidence/` + summary markdown.

## 6) Governance / Sign-off

Роли:

1. Implementation owner: execution-dev
2. Runtime contract sign-off: runtime-ops
3. Ops rollout sign-off: runtime-ops
4. Audit authority: 2 independent auditors

Правила:

1. High finding блокирует закрытие фазы.
2. Medium finding допускается только с owner+deadline и отдельным follow-up task.
3. Каждая major phase закрывается только после:
   1. test pack PASS,
   2. smoke PASS,
   3. evidence markdown + hashes.

## 7) Canonical Contract Source

Canonical source:

1. `ops/executor_contract_v1.md` — единственный источник истины.
2. Section 8 этого документа — только audit summary contract и не должен расходиться с `ops/executor_contract_v1.md`.

Contract governance:

1. Любые изменения contract версии требуют:
   1. explicit version bump (`v1` -> `v2`),
   2. compatibility matrix,
   3. migration note для adapter/runtime.

## 8) Contract Freeze (summary)

## 8.1 Request contract (adapter -> executor)

`/simulate`:

1. `action=simulate`
2. `dry_run=true`
3. `contract_version`
4. `request_id`, `signal_id`, `side`, `token`, `notional_sol`, `signal_ts`, `route`

`/submit`:

1. `contract_version`
2. `request_id`, `signal_id`, `client_order_id`
3. `side`, `token`, `notional_sol`, `signal_ts`, `route`
4. `slippage_bps`, `route_slippage_cap_bps`, `tip_lamports`
5. `compute_budget.cu_limit`, `compute_budget.cu_price_micro_lamports`

## 8.2 Response contract (executor -> adapter)

`/simulate` success:

1. `status=ok`, `ok=true`, `accepted=true`
2. optional: `route`, `contract_version`
3. `detail` string рекомендуется всегда и должен отражать simulation outcome reason.

`/submit` success:

1. `status=ok`, `ok=true`, `accepted=true`
2. MUST include one of:
   1. `tx_signature`
   2. `signed_tx_base64`
3. optional policy-echo:
   1. `route`, `contract_version`, `client_order_id`, `request_id`
4. optional time:
   1. `submitted_at` RFC3339 (если отсутствует, adapter подставляет `Utc::now()`).
5. optional fee-hints:
   1. `network_fee_lamports`
   2. `base_fee_lamports`
   3. `priority_fee_lamports`
   4. `ata_create_rent_lamports`
6. fee consistency rule:
   1. если передан `network_fee_lamports`, он MUST равняться `base_fee_lamports + priority_fee_lamports`,
   2. mismatch трактуется adapter как terminal reject.

Reject response:

1. `status=reject|error|failed` (или `ok=false` / `accepted=false`)
2. `retryable` boolean
3. `code`
4. `detail`

## 8.3 Error taxonomy and responsibility boundaries

Runtime guarded allowlist содержит коды нескольких слоев. Для executor плана границы такие:

1. Executor-emitted reject codes:
   1. executor SHOULD эмитить собственные коды с префиксом `executor_` (например `executor_build_failed`, `executor_blockhash_expired`, `executor_route_rejected`) для pure business logic,
   2. compatibility transport codes (`upstream_*`, `send_rpc_*`, `submit_adapter_*`) допускаются в forwarding/broadcast path для adapter/runtime interoperability,
   3. retryability задается полем `retryable` в reject payload.
2. Adapter-generated transport/forwarding codes:
   1. `upstream_*` коды генерируются adapter при транспортных/HTTP проблемах между adapter и executor,
   2. `send_rpc_*` коды генерируются adapter в его signed-tx broadcast path,
   3. `submit_adapter_*` коды формируются на runtime/adapter boundary.
3. Runtime fallback allowlist использует adapter-layer classification, а не “сырые” executor code names.
4. Cross-route guarded fallback policy (`jito|fastlane -> rpc`) intentionally:
   1. разрешается только по adapter-layer transport/service-class событиям (`upstream_*`/`send_rpc_*`),
   2. `executor_*` retryable коды не предназначены для автоматического guarded cross-route switch; они обрабатываются как business-level retry/reject по политике caller.

## 8.4 Auth boundary (adapter -> executor)

1. Текущий adapter при upstream forwarding использует Bearer token, HMAC-forwarding отсутствует.
2. Поэтому Phase 1-6 обязательный auth mode для adapter->executor:
   1. Bearer (mandatory),
   2. HMAC для этого hop — out-of-scope до отдельного adapter enhancement.
3. Optional future item:
   1. `Phase 4B` — добавить upstream HMAC forwarding в adapter и только после этого включить HMAC mandatory для executor ingress.

## 8.5 HTTP status convention (mandatory)

1. Business-level outcomes (both success and reject) MUST return HTTP 200 with JSON payload.
2. Executor service-level failures (overload/unavailable/unhandled) MAY return HTTP `429/5xx`.
3. Guarded cross-route fallback relies on adapter transport/service classification; therefore executor MUST NOT encode business rejects as HTTP 5xx.

## 8.6 `/healthz` summary schema

Executor health response should include at minimum:

1. `status` (`ok|degraded`)
2. `contract_version`
3. `enabled_routes`
4. `signer_source` (`kms|file`)
5. optional: `idempotency_store_status`
6. optional: `send_rpc_enabled_routes`
7. optional: `send_rpc_fallback_routes`

## 9) Solana Execution Core Design (mandatory)

## 9.1 Transaction building pipeline

Каждый submit должен проходить pipeline:

1. route-specific quote/build input resolve;
2. instruction set compose:
   1. swap instruction(s),
   2. compute budget instructions,
   3. optional tip instruction (для `jito`/`fastlane` route policy);
3. optional ATA create instruction (если target ATA отсутствует);
4. recent blockhash fetch;
5. sign transaction;
6. route-specific send;
7. compute fee-hints from built/sent tx context:
   1. `base_fee_lamports` (Solana base fee; currently expected 5000),
   2. `priority_fee_lamports = cu_limit * cu_price_micro_lamports / 1_000_000`,
   3. `network_fee_lamports = base_fee_lamports + priority_fee_lamports`,
   4. `ata_create_rent_lamports` если ATA create instruction была реально применена;
8. normalize upstream response в adapter contract.

Route-specific slippage/tip semantics:

1. `slippage_bps` from request MUST be forwarded into quote/build path as route slippage bound.
2. `tip_lamports` handling:
   1. `jito`/`fastlane`: apply tip according to route policy/instruction model,
   2. `rpc`: tip treated as non-applicable (ignored or forced to zero by policy with explicit audit trace).

Compute-budget passthrough policy:

1. Executor MUST использовать `compute_budget.cu_limit` и `compute_budget.cu_price_micro_lamports` из request как source-of-truth.
2. Executor MUST NOT самостоятельно переопределять CU limit/price.
3. Если route/backend не может выполнить запрос с переданным compute budget, executor возвращает terminal reject (`executor_compute_budget_unsupported` или эквивалентный `executor_*` код).

Integration policy:

1. Primary build backend: Jupiter/Metis class quote+swap API (или эквивалентный агрегатор).
2. Direct DEX-specific builders (Raydium/PumpSwap) допускаются как route plugin, но не обязательны для V1 closure.
3. Все build backends должны вернуть единый normalized internal model.

## 9.2 Custody / signer key management

Primary model (required for production closure):

1. signer source:
   1. external KMS/HSM signer API OR
   2. file-mounted keypair outside repo (`/etc/solana-copy-bot/secrets/...`) with strict permissions.
2. Startup fail-closed rules:
   1. key source must be configured,
   2. file source requires `owner-only` perms,
   3. inline raw secret in env/config is rejected.
3. Rotation policy:
   1. atomic replace,
   2. post-rotation health check,
   3. evidence capture.
4. Stage B dependency:
   1. R2P-03 must be closed before enabling real submit.

## 9.3 ATA handling

1. Before submit, executor checks signer ATA existence for target token.
2. If ATA missing:
   1. add create-ATA instruction (if policy allows),
   2. include rent in `ata_create_rent_lamports` hint.
3. If ATA creation disabled by policy, return terminal reject with explicit code/detail.

## 9.4 Blockhash lifecycle

1. Executor fetches recent blockhash per submit attempt.
2. On retryable send failure, refresh blockhash before retry.
3. Expired blockhash errors are classified as terminal `executor_blockhash_expired` for the current submit attempt (no in-attempt auto-resend on stale blockhash in this path).
4. Any bounded retry policy for blockhash-expired must be orchestrated by caller-level retry flow with a fresh submit attempt and rebuilt transaction context.
5. Submit path must not reuse stale blockhash across retry boundary.

## 9.5 Simulation semantics

1. `/simulate` runs transaction simulation using the same build pipeline as submit path.
2. `detail` should include actionable reason class (success/risk/cap/compute/slippage).
3. Simulation reject classes must be deterministic and auditable.

## 10) Durable Idempotency Model (mandatory)

1. Idempotency storage must be persistent (SQLite/Redis/DB), not in-memory only.
2. Required idempotency key:
   1. `client_order_id` as primary key,
   2. `request_id` as correlation index.
3. Required state machine:
   1. `received`
   2. `built`
   3. `signed`
   4. `submitted`
   5. `confirmed|failed`
4. Restart behavior:
   1. executor restart must reload idempotency state,
   2. re-submission of completed key returns cached normalized result,
   3. partial state (`signed` without `submitted`) handled by explicit reconcile path:
      1. lookup signature on-chain via `getSignatureStatuses`,
      2. if found -> transition to `submitted|confirmed` based on status,
      3. if not found and blockhash still valid -> allow controlled re-submit,
      4. if not found and blockhash expired -> transition to `failed` with explicit `executor_*` reason code.
5. TTL/retention:
   1. closed rows retained for fixed window (e.g., 7d) for duplicate defense + audit.

## 11) Phase Plan

## Phase 0 — Spec + dependencies + deterministic baseline (mandatory)

Scope:

1. Freeze canonical contract in `ops/executor_contract_v1.md`.
2. Add fixtures (success/reject/invalid cases).
3. Add dependency inventory and version pin policy:
   1. Solana SDK/client libs
   2. quote/build backend SDK/API client
   3. Jito client
4. Prepare deterministic baseline commands.

Deliverables:

1. `ops/executor_contract_v1.md` (canonical)
2. `ops/evidence/executor_contract_fixtures_manifest_YYYY-MM-DD.txt`
3. `ops/executor_dependency_inventory_YYYY-MM-DD.md`
4. `tools/executor_contract_smoke_test.sh`

Mandatory regression pack:

1. `cargo test --workspace -q`
2. `bash tools/ops_scripts_smoke_test.sh`
3. `bash -n tools/executor_contract_smoke_test.sh`

Exit criteria:

1. Contract document freeze approved by 2 auditors.
2. Fixtures deterministic (hash stable across two runs).
3. Dependency pins approved.

---

## Phase 1 — `crates/executor` scaffold + custody bootstrap

Scope:

1. Add `copybot-executor` binary crate.
2. Implement:
   1. `GET /healthz`
   2. `POST /simulate`
   3. `POST /submit`
3. Auth for adapter->executor:
   1. Bearer mandatory path,
   2. HMAC optional (non-mandatory) until adapter forwards it.
4. Signer custody bootstrap:
   1. key source loading,
   2. fail-closed startup validation,
   3. rotation hooks.

Deliverables:

1. `crates/executor/Cargo.toml`
2. `crates/executor/src/main.rs`
3. `ops/executor_backend_runbook.md`
4. `tools/executor_signer_rotation_report.sh`

Mandatory regression pack:

1. `cargo test -p copybot-executor -q`
2. `cargo test --workspace -q`
3. `bash tools/ops_scripts_smoke_test.sh`

Exit criteria:

1. Executor starts locally.
2. Health endpoint stable.
3. Bearer ingress auth fail-closed behavior covered.
4. Signer source missing/malformed cases fail startup.

---

## Phase 2 — Transaction build core + route adapters

Scope:

1. Implement build pipeline:
   1. quote/build integration,
   2. instruction composition,
   3. compute budget + tip wiring,
   4. ATA handling.
2. Implement route adapter abstraction:
   1. `RpcRouteExecutor` (required)
   2. `JitoRouteExecutor` (required)
   3. `FastlaneRouteExecutor` (optional, behind `submit_fastlane_enabled=true`)
3. Route policy:
   1. allowlist,
   2. per-route endpoint validation,
   3. fallback endpoint identity guard (primary != fallback).

Mandatory regression pack:

1. `cargo test -p copybot-executor -q route_`
2. `cargo test -p copybot-executor -q tx_build_`
3. `cargo test -p copybot-executor -q ata_`
4. `cargo test --workspace -q`
5. `bash tools/ops_scripts_smoke_test.sh`

Exit criteria:

1. Route selection deterministic.
2. Guarded fallback behavior соответствует runtime policy.
3. Fastlane route blocked when flag is false.
4. Jito route operational and evidence-backed.

---

## Phase 3 — Submit/Simulate hardening + durable idempotency + latency budgets

Scope:

1. Durable idempotency storage and lifecycle.
2. Retry taxonomy:
   1. transport retryable
   2. terminal contract rejects
3. Blockhash refresh policy on retries.
4. Strict response normalization:
   1. no unknown status pass-through
   2. no malformed success payload
5. Latency budget enforcement.

Mandatory regression pack:

1. `cargo test -p copybot-executor -q submit_`
2. `cargo test -p copybot-executor -q simulate_`
3. `cargo test -p copybot-executor -q idempotency_`
4. `cargo test -p copybot-executor -q blockhash_`
5. `cargo test --workspace -q`
6. `bash tools/ops_scripts_smoke_test.sh`

Exit criteria:

1. Duplicate submit does not create duplicate on-chain send after restart.
2. Unknown status always fail-closed.
3. Retryable vs terminal codes stable and documented.
4. End-to-end submit latency (`adapter -> executor -> route send -> executor response`) fits adapter timeout budget with measurable margin.

---

## Phase 4 — Adapter integration + service topology

Scope:

1. Wire adapter upstream to executor:
   1. `COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=http://127.0.0.1:<port>/submit`
   2. `COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=http://127.0.0.1:<port>/simulate`
2. Add `tools/executor_preflight.sh`:
   1. contract checks
   2. endpoint checks
   3. auth checks
3. Add adapter<->executor integration scenarios.
4. Define service dependency chain:
   1. systemd ordering (`executor` before `adapter` before `app`),
   2. health chain and startup failure behavior.

Mandatory regression pack:

1. `cargo test -p copybot-adapter -q`
2. `cargo test -p copybot-executor -q`
3. `cargo test --workspace -q`
4. `bash tools/ops_scripts_smoke_test.sh`

Exit criteria:

1. Adapter + executor work end-to-end in local rehearsal mode.
2. Failure taxonomy does not break runtime fallback logic.
3. Service topology documented and reproducible.

---

## Phase 5 — Ops evidence + runbooks

Scope:

1. Add evidence helper:
   1. `tools/executor_rollout_evidence_report.sh`
2. Add final package helper:
   1. `tools/executor_final_evidence_report.sh`
3. Add checksum manifests + `artifacts_written` contract.
4. Update runbooks:
   1. `ops/executor_backend_runbook.md`
   2. `ops/adapter_backend_runbook.md` (executor wiring)
   3. `ops/execution_devnet_rehearsal_runbook.md`
   4. `ROAD_TO_PRODUCTION.md` next-code-queue mapping

Mandatory regression pack:

1. `bash -n tools/executor_preflight.sh tools/executor_rollout_evidence_report.sh tools/executor_final_evidence_report.sh`
2. `bash tools/ops_scripts_smoke_test.sh`
3. `cargo test --workspace -q`

Exit criteria:

1. Evidence chain deterministic and auditable.
2. Rehearsal + rollout artifacts self-contained (summary + manifest + captures).

---

## Phase 6 — Devnet rehearsal + Go/No-Go

Scope:

1. Run Stage C.5 with executor in-path.
2. Collect 1h/6h/24h route/fee windows.
3. Close P0/P1 findings.
4. Validate fastlane flag enforcement negative path.

Mandatory commands:

1. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml OUTPUT_DIR=state/executor-preflight ./tools/executor_preflight.sh`
2. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml OUTPUT_DIR=state/devnet-rehearsal ./tools/execution_devnet_rehearsal.sh 24 60`
3. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml OUTPUT_ROOT=state/executor-final ./tools/executor_final_evidence_report.sh 1,6,24 60`
4. `ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env CONFIG_PATH=/etc/solana-copy-bot/live.server.toml OUTPUT_ROOT=state/rollout-final ./tools/adapter_rollout_final_evidence_report.sh 24 60`

Exit criteria:

1. Stage C.5 verdict = GO.
2. Route/fee signoff verdict = GO.
3. Executor final evidence package = GO.
4. Fastlane disabled negative test = PASS.
5. No unresolved High findings.

## 12) Rollback Matrix (by phase)

| Phase | Trigger | Rollback Path | Window | Owner |
| --- | --- | --- | --- | --- |
| 0 | contract ambiguity / unstable fixtures | revert phase merge commit | 24h | execution-dev |
| 1 | startup/auth/custody regression | revert phase merge commit | 24h | execution-dev |
| 2 | tx build / route adapter drift | revert phase merge commit | 24h | execution-dev |
| 3 | duplicate submits / blockhash/idempotency drift | revert phase merge commit | 24h | execution-dev |
| 4 | adapter integration regressions | revert phase merge commit | 24h | runtime-ops |
| 5 | evidence artifacts invalid | revert phase merge commit | 24h | runtime-ops |
| 6 | rehearsal verdict NO_GO/P0 | revert phase merge commit + disable execution | 12h | runtime-ops |

Branch protocol:

1. Phase branch: `executor/phase-<id>-<topic>`
2. Merge strategy: `--no-ff`
3. Tags: `executor-phase-<id>-start-<short_sha>-<utc_date>` and `...-end-...`
4. Rollback default: `git revert <phase-merge-commit>` (never destructive reset)

## 13) Risk Register

1. Error-code drift breaks guarded fallback.
   1. Mitigation: compatibility tests vs runtime allowlist.
2. Route misconfiguration causes wrong path.
   1. Mitigation: strict startup validation + endpoint identity checks.
3. Auth miswiring between app->adapter->executor.
   1. Mitigation: preflight script + explicit auth diagnostics.
4. Timeout budget inflation on fallback chains.
   1. Mitigation: total-budget enforcement + measured latency assertions.
5. Custody misconfiguration.
   1. Mitigation: signer source fail-closed startup + rotation evidence.
6. Idempotency state loss.
   1. Mitigation: durable storage + restart recovery tests.
7. Observability gaps in top-level artifacts.
   1. Mitigation: stable reason_code fields + checksum manifests + smoke asserts.

## 14) External Dependencies (operator checklist)

Before Phase 6:

1. QuickNode devnet RPC endpoint provisioned.
2. QuickNode mainnet RPC endpoint provisioned.
3. Lil' JIT endpoint + auth provisioned (required for DoD closure).
4. Fastlane endpoint + auth provisioned only if `submit_fastlane_enabled=true`.
5. Optional Priority Fee API endpoint/token provisioned (runtime dynamic CU hints; not mandatory for executor start).

## 15) Immediate Next Step (execution order)

1. Phase 0: contract + dependency freeze.
2. Phase 1: scaffold + custody bootstrap.
3. Phase 2: tx build core + route adapters.
4. Phase 3: durable idempotency + hardening.
5. Phase 4: adapter integration + service topology.
6. Phase 5: evidence/runbooks.
7. Phase 6: devnet rehearsal + audit sign-off.

---

Auditor Review Checklist:

1. Canonical contract source single and explicit?
2. Contract summary includes `submitted_at` and fee-hint consistency?
3. Transaction building pipeline explicit (build/sign/send)?
4. Custody/signing model explicit and auditable?
5. Durable idempotency model explicit and restart-safe?
6. Guarded fallback code compatibility bounded by responsibility layer?
7. Route gates (`jito/fastlane`) fail-closed and feature-gated?
8. Per-phase rollback operational?
9. Evidence artifacts deterministic and hashed?
10. Stage C.5 path reproducible and includes executor final package?
