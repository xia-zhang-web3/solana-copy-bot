# Server Templates (6.1 Bring-up)

These files are repository-side templates for the test-server bring-up tracked by `ops/test_server_rollout_6_1_tracker.md`.
They are synced with the current staging server snapshot (`52.28.0.218`, `2026-03-03`).

## Security note

1. `live.server.toml.example` contains placeholder-only RPC values; populate real credentials only in the server-local copy or via env overrides.
2. `live.server.toml.example` also contains a placeholder-only `recent_raw_gap_fill.helius_http_url`; restore operators must replace it with the historical raw source URL used for bounded gap-fill.
3. `live.server.toml.example` also contains a placeholder-only `recent_raw_gap_fill_helius.helius_http_url`; this must point at a Helius endpoint that supports `getTransactionsForAddress`.
4. `live.server.toml.example` also contains a placeholder-only `program_history_validation.http_url`; this must point at the QuickNode RPC endpoint that will be used for bounded program-history validation through `getSlot`, `getBlockTime`, `getBlocks`, and `getBlock`.
5. bootstrap signer values are for non-live contour testing only.
6. rotate endpoint/token/signer before any tiny-live or production stage.

## Files

1. `live.server.toml.example`
2. `copybot-executor.service`
3. `copybot-adapter.service`
4. `solana-copy-bot.service`
5. `copybot-execution-mock-upstream.service`
6. `executor.env.example`
7. `adapter.env.example`
8. `app.env.example`
9. `copybot-discovery-runtime-export.service`
10. `copybot-discovery-runtime-export.timer`
11. `copybot-discovery-recent-raw-snapshot.service`
12. `copybot-discovery-recent-raw-snapshot.timer`

## Recent Raw Snapshot Timer Contract

1. `copybot-discovery-recent-raw-snapshot.service` now treats exit code `75` as
   an expected transient outcome for snapshot contention.
2. Operators must inspect the JSON `state` emitted by
   `discovery_recent_raw_snapshot`, not just the systemd success/failure bit.
3. The service template now sets `TimeoutStartSec=10min`. The binary still
   self-bounds the SQLite backup attempt internally, but large-journal finalize
   and full-set retention cleanup can legitimately outlive the old `3min`
   systemd kill window.
4. Operators should inspect `terminal_reason`, `attempt_duration_ms`,
   `backup_total_page_count`, `backup_copied_page_count`, `source_db_bytes`, and
   `source_wal_bytes` before deciding the timer is healthy on a large journal.
5. The snapshot path now pins the source journal inside a read transaction for
   the duration of the online backup, so live writes no longer force the
   snapshot to chase a moving target forever.
6. Latest surface publication now prefers a hard link from the fresh archive
   snapshot to `latest.sqlite`, falling back to an atomic copy only if linking
   is unavailable.
7. Expected non-fatal states:
   - `written`
   - `self_healed_latest_surface`
   - `skipped_not_due`
   - `deferred`
   - `retryable_busy`
8. `deferred` means the service exited cleanly with a transient non-success
   reason such as bounded attempt-duration exhaustion or, if a healthy latest
   surface existed, retained that surface after the failed attempt.
9. Scheduled runs now stage the new archive set under hidden temp names and
   enforce full-set retention on every invocation, including `skipped_not_due`
   and `self_healed_latest_surface`. Failed or interrupted runs must not leave
   behind promoted archive sets that count against retention.
10. After the practical-completion rollout, repeated `deferred` with tiny
   `backup_copied_page_count` relative to `backup_total_page_count` should be
   treated as abnormal and investigated before leaving the timer disabled.
11. `retryable_busy` means retryable contention happened without a healthy latest
   surface to defer onto; rerun or investigate before calling the snapshot
   surface healthy again.
11. `hard_failure` remains a real failure and should leave the service in the
   normal non-zero failed state.

## Stage 3 Wallet Freshness Capture Contract

1. The primary Stage 3 evidence path is now in-band inside
   `solana-copy-bot.service`, not the standalone timer.
2. On each discovery publish-due refresh cycle, the runtime now reuses already
   computed exact truth and appends one persisted Stage 3 capture:
   - exact publication truth
   - exact active follow truth
   - exact current raw-truth top-N
   - exact selected-wallet shadow/raw evidence
3. This is the accepted cheap path because it avoids a second standalone
   current-raw rebuild against the live runtime DB. The capture is a fail-open
   evidence sidecar inside refresh/publication, not a new correctness
   dependency for runtime health.
4. Operators should inspect in-band capture accumulation with:
   - `journalctl -u solana-copy-bot.service -n 50 --no-pager | rg 'wallet_freshness_capture_'`
5. The important in-band capture log fields are:
   - `wallet_freshness_capture_state`
   - `wallet_freshness_capture_reason`
   - `wallet_freshness_capture_id`
   - `wallet_freshness_capture_captured_at`
6. `wallet_freshness_capture_state=persisted` means one exact capture was
   appended on that refresh. `skipped_due_cadence` means the current refresh did
   not owe a publication/capture tick. `persistence_failed` means Stage 3
   evidence did not append, but discovery refresh/publication still stayed
   truthful and live-safe.
7. Operators should inspect the accumulated Stage 3 verdict with:
   - `discovery_wallet_freshness_report --config /etc/solana-copy-bot/live.server.toml --limit 5 --json`
8. For recent-cycle validation, the important report fields are:
   - `latest_capture_age_seconds`
   - `captures_within_recent_horizon`
   - `recent_horizon_seconds`
   - `stale_captures_excluded_from_verdict`
9. The standalone scheduled capture timer/service were removed from the repo
   after the in-band architecture was accepted. They are no longer part of the
   production or debug operator contract.
10. The standalone manual/debug command remains available for explicit deep
    spot checks:
   - `discovery_wallet_freshness_capture --config /etc/solana-copy-bot/live.server.toml --recent-cycles 1 --shadow-evidence-lookback-seconds 960 --json`
11. This manual/debug command is secondary only:
   - it is useful when operators want an explicit one-off persisted capture
     outside the normal refresh loop
   - it is not the primary Stage 3 accumulation path
   - its output now reports `mode=manual_debug`
   - the `--shadow-evidence-lookback-seconds 960` example keeps exact
     selected-wallet raw/shadow evidence aligned with the former 15 minute
     standalone cadence envelope without reviving the old timer architecture
12. `execution.enabled = false` remains unchanged. In-band Stage 3 capture and
   the manual/debug command both collect evidence only; neither implies
   execution activation.

## Stage 4 Execution Readiness Audit

1. Stage 4 preparation now has a dedicated operator command:
   - `copybot_execution_readiness_audit --config /etc/solana-copy-bot/live.server.toml --json`
2. This command does not enable execution and does not submit real trades.
3. It validates the execution-side contract separately from discovery:
   - static execution config completeness
   - signer / route / policy contract
   - RPC reachability for the configured mode
   - adapter dry-run contract via the existing `action=simulate` path
4. The important top-level fields are:
   - `verdict`
   - `config_valid`
   - `connectivity_valid`
   - `adapter_contract_valid`
   - `signer_contract_valid`
   - `policy_contract_valid`
   - `ready_for_dry_run`
   - `blocked_for_activation`
   - `activation_blockers`
5. `ready_for_execution_dry_run` means the execution-side wiring is ready for a
   later dress rehearsal, but it does not override the current Stage 3 gate:
   `execution.enabled` must still remain `false` until discovery is trusted.
6. `config_valid_but_connectivity_blocked` means static config is coherent but
   RPC and/or adapter reachability is still not good enough even for a dry-run.
7. `adapter_contract_incomplete`, `signer_contract_incomplete`, and
   `policy_contract_incomplete` are pre-activation blockers and should be fixed
   before any later tiny-live decision is even discussed.
8. Stage 4 preparation now also has a persisted dry-run rehearsal surface:
   - run and persist one rehearsal:
     `copybot_execution_dry_run_rehearsal --config /etc/solana-copy-bot/live.server.toml --json`
   - inspect recent persisted rehearsal history:
     `copybot_execution_dry_run_rehearsal --config /etc/solana-copy-bot/live.server.toml --history --limit 10 --json`
9. The rehearsal command stays in safe pre-activation mode:
   - it does not enable `execution.enabled`
   - it does not submit real swaps
   - it only uses RPC read/preflight checks plus adapter `action=simulate`
10. Each persisted rehearsal records:
    - deterministic intent summary (`route`, `token`, `notional_sol`)
    - route/policy envelope chosen from current config
    - RPC preflight result (`slot`, `blockhash`, signer balance if available)
    - adapter simulate classification and policy-echo result
    - overall rehearsal verdict, blockers, and warnings
11. Important rehearsal verdicts:
    - `rehearsal_green`
    - `rehearsal_green_with_business_reject`
    - `rehearsal_blocked_by_connectivity`
    - `rehearsal_blocked_by_adapter_contract`
    - `rehearsal_blocked_by_policy_echo`
12. `rehearsal_green_with_business_reject` means wiring and simulate contract
    look valid, but the fixed synthetic probe intent was rejected business-wise.
    That is still useful evidence for later tiny-live preparation, but it does
    not authorize activation.
13. `rehearsal_green` and `rehearsal_green_with_business_reject` still do not
    override Stage 3. Discovery freshness evidence remains the gate before any
    activation discussion.

## Devnet Dress Rehearsal

1. Stage 4 now also has a first-class non-production dress-rehearsal command:
   - run and persist one devnet rehearsal:
     `copybot_devnet_dress_rehearsal --config /etc/solana-copy-bot/devnet.server.toml --route jito --token So11111111111111111111111111111111111111112 --notional-sol 0.01 --json`
   - inspect recent persisted devnet rehearsal history:
     `copybot_devnet_dress_rehearsal --config /etc/solana-copy-bot/devnet.server.toml --history --limit 10 --json`
2. This command is non-prod only:
   - it explicitly refuses production-like `system.env` profiles such as
     `prod`, `production`, `prod-live`, or `production_canary`
   - the same refusal now applies to `--history`; production-like configs are
     not allowed to read or append devnet rehearsal history accidentally
   - it uses `execution.rpc_devnet_http_url` as the RPC target for the
     rehearsal instead of the production execution RPC
   - it requires an explicit non-production adapter endpoint in
     `execution.submit_adapter_devnet_http_url` or
     `execution.submit_adapter_devnet_fallback_http_url`
   - it refuses any devnet adapter endpoint that resolves to the normal
     `execution.submit_adapter_http_url` /
     `execution.submit_adapter_fallback_http_url`
   - it does not enable `execution.enabled` and does not submit real trades on
     production
3. The dress rehearsal reuses the accepted Stage 4 surfaces instead of
   re-implementing them:
   - execution readiness/preflight contract
   - tiny-live policy boundedness contract
   - safe dry-run rehearsal contract with adapter `simulate`
4. Persisted devnet history includes an explicit environment label so
   non-production rehearsal evidence does not get mixed into the production
   pre-activation trail.
5. Important devnet rehearsal verdicts:
   - `devnet_rehearsal_green`
   - `devnet_rehearsal_green_with_business_reject`
   - `devnet_rehearsal_blocked_by_connectivity`
   - `devnet_rehearsal_blocked_by_adapter_contract`
   - `devnet_rehearsal_blocked_by_policy_contract`
   - `devnet_rehearsal_refused_for_prod_profile`
6. A green devnet result means the execution-side contract can be exercised
   end-to-end on non-production infrastructure. It still does not authorize
   production activation, and it does not override Stage 3.
7. For a safe non-live contour, keep using the existing executor and mock
   upstream templates:

## Devnet Activation/Rollback Drill

1. Stage 4 now also has a non-production drill over the accepted tiny-live
   launch dossier:
   - run and persist one activation/rollback drill:
     `copybot_devnet_activation_drill --config /etc/solana-copy-bot/devnet.server.toml --route jito --token So11111111111111111111111111111111111111112 --notional-sol 0.01 --json`
   - inspect recent persisted drill history:
     `copybot_devnet_activation_drill --config /etc/solana-copy-bot/devnet.server.toml --history --limit 10 --json`
2. This command is also non-prod only:
   - it refuses production-like `system.env`
   - it does not enable production execution
   - it does not write `/etc/solana-copy-bot/live.server.toml`
   - it does not submit real trades on production
3. The drill reuses accepted Stage 4 truth instead of inventing a parallel
   activation path:
   - `copybot_tiny_live_activation_plan`
   - `copybot_devnet_dress_rehearsal`
   - `copybot_tiny_live_guardrail_audit`
4. It rehearses both sides of the accepted dossier on a derived non-prod config:
   - activation overlay applied in-memory only
   - rollback overlay applied back to that derived config
   - persisted verdicts for both activation drill and rollback drill
5. Important top-level verdicts:
   - `devnet_activation_drill_green`
   - `devnet_activation_drill_blocked_by_launch_dossier`
   - `devnet_activation_drill_blocked_by_non_prod_contract`
   - `devnet_activation_drill_blocked_by_guardrails`
   - `devnet_rollback_drill_failed`
   - `devnet_activation_drill_refused_for_prod_profile`
6. A green drill means the accepted bounded launch dossier remained internally
   coherent under non-production rehearsal. It still does not authorize
   production activation and it does not override the Stage 3 production gate.

## Devnet Consolidated Readiness

1. Stage 4 non-production evidence now also has a consolidated read-only report:
   - `copybot_devnet_readiness_report --config /etc/solana-copy-bot/devnet.server.toml --json`
2. The command is non-prod only and refuses production-like `system.env`
   profiles. It does not rerun drills, mutate config, restart services, or
   submit trades.
3. It summarizes the persisted recent-history truth from:
   - `copybot_devnet_dress_rehearsal`
   - `copybot_devnet_activation_drill`
4. Important top-level verdicts:
   - `devnet_readiness_green`
   - `devnet_readiness_insufficient_recent_evidence`
   - `devnet_readiness_blocked_by_dress_rehearsal_history`
   - `devnet_readiness_blocked_by_activation_drill_history`
   - `devnet_readiness_stale_history`
   - `devnet_readiness_refused_for_prod_profile`
5. A green readiness report means recent non-prod dress-rehearsal evidence and
   recent non-prod activation-drill evidence are both fresh and acceptable.
   It still does not authorize production activation and it does not override
   the Stage 3 production gate.
   - `ops/server_templates/executor.env.example`
   - `ops/server_templates/copybot-execution-mock-upstream.service`

## Consolidated Pre-Activation Gate

1. Operators now also have one consolidated pre-activation gate surface:
   - `copybot_pre_activation_gate_report --config /etc/solana-copy-bot/live.server.toml --json`
2. This command stays read-only and pre-activation only:
   - it does not enable `execution.enabled`
   - it does not submit real trades
   - it reuses existing Stage 3 / Stage 4 truth surfaces instead of running a
     second heavy decision path
3. The command consolidates:
   - Stage 3 recent-cycle discovery truth from `discovery_wallet_freshness_report`
   - Stage 4 readiness/preflight truth from `copybot_execution_readiness_audit`
   - Stage 4 persisted dry-run rehearsal history
   - explicit tiny-live bounded policy truth from `copybot_tiny_live_policy_audit`
4. Important top-level fields:
   - `verdict`
   - `reason`
   - `blockers`
   - `stage3_verdict`
   - `stage4_readiness_verdict`
   - `stage4_rehearsal_history_verdict`
   - `execution_enabled`
   - `planning_safe_only`
5. Important top-level verdicts:
   - `pre_activation_gates_green`
   - `blocked_by_stage3`
   - `blocked_by_stage4_readiness`
   - `blocked_by_dry_run_history`
   - `blocked_by_tiny_live_policy`
   - `insufficient_recent_evidence`
6. `pre_activation_gates_green` means:
   - Stage 3 recent evidence is currently green
   - Stage 4 readiness/preflight is currently green
   - recent dry-run rehearsal history is sufficient for planning-safe
     tiny-live discussion
   - the explicit tiny-live policy envelope is bounded
7. `pre_activation_gates_green` does not mean:
   - execution is enabled
   - activation permission has been granted
   - Stage 3 can be skipped later
8. Stage 3 remains the primary gate. Even perfect Stage 4 readiness or
   rehearsal history cannot override a non-green Stage 3 verdict.

## Tiny-Live Policy Audit

1. Stage 4 preparation now also has an explicit tiny-live policy audit:
   - `copybot_tiny_live_policy_audit --config /etc/solana-copy-bot/live.server.toml --json`
2. This command stays read-only and pre-activation only:
   - it does not enable `execution.enabled`
   - it does not submit real trades
   - it does not override Stage 3 or the consolidated pre-activation gate
3. The audit compares the current config against the explicit
   `[tiny_live_policy]` envelope in `live.server.toml`.
4. Important policy areas checked:
   - trade-size bounds (`shadow.copy_notional_sol`, `risk.max_position_sol`)
   - batch/concurrency bounds (`execution.batch_size`,
     `risk.max_concurrent_positions`)
   - daily loss guardrail (`risk.daily_loss_limit_pct`)
   - allowed routes and default route
   - per-route slippage / tip / CU-price bounds
   - policy-echo requirement
   - pretrade fee-overhead and priority-fee caps
5. Important top-level verdicts:
   - `tiny_live_policy_bounded`
   - `tiny_live_policy_too_open`
   - `tiny_live_policy_incomplete`
   - `tiny_live_policy_route_risk_unbounded`
   - `tiny_live_policy_fee_risk_unbounded`
   - `tiny_live_policy_not_applicable_current_mode`
6. `tiny_live_policy_bounded` means the current config is explicitly narrow
   enough for later tiny-live discussion. It still does not authorize
   activation and does not override Stage 3.
7. If the verdict is non-green, operators should read:
   - `blockers`
   - `current_allowed_routes`
   - `policy_allowed_routes`
   - the numeric `current_*` versus `policy_*` cap fields
8. The repo template now contains an explicit `[tiny_live_policy]` block. That
   block is the source of truth for the future tiny-live envelope; the audit
   does not rely on hidden defaults in code.

## Tiny-Live Activation Plan

1. Operators now also have a planning-only tiny-live activation package:
   - `copybot_tiny_live_activation_plan --config /etc/solana-copy-bot/live.server.toml --json`
2. This command stays strictly pre-activation:
   - it does not enable `execution.enabled`
   - it does not write into `/etc/solana-copy-bot/live.server.toml`
   - it does not restart services
   - it does not submit trades
3. It reuses the accepted Stage 4 truth surfaces instead of inventing a second
   decision path:
   - `copybot_pre_activation_gate_report`
   - `copybot_tiny_live_policy_audit`
   - `copybot_tiny_live_guardrail_audit`
   - current execution/risk/shadow config truth
4. The report renders:
   - the current safe execution state
   - the future bounded activation overlay
   - the explicit rollback delta back to safe mode
   - the service restart contract for activation and rollback
   - the future rollback-trigger and monitoring-threshold envelope
5. Important top-level verdicts:
   - `activation_plan_ready_when_stage_gate_allows`
   - `blocked_by_pre_activation_gate`
   - `blocked_by_policy_contract`
   - `blocked_by_guardrail_contract`
   - `activation_overlay_incomplete`
   - `rollback_plan_incomplete`
   - `service_restart_contract_incomplete`
6. `activation_plan_ready_when_stage_gate_allows` means the later tiny-live
   config delta, rollback delta, and rollback-trigger envelope are already
   explicit. It still does not authorize activation, and it does not override
   Stage 3.
7. If `--output <path>` is provided, the command writes the full JSON planning
   artifact to disk without mutating the live config.

## Tiny-Live Guardrail Audit

1. Operators now also have a planning-only tiny-live guardrail audit:
   - `copybot_tiny_live_guardrail_audit --config /etc/solana-copy-bot/live.server.toml --json`
2. This command stays read-only and pre-activation only:
   - it does not enable `execution.enabled`
   - it does not write the live config
   - it does not restart services
   - it does not submit trades
3. Guardrails are intentionally separate from the tiny-live activation envelope:
   - `copybot_tiny_live_policy_audit` answers whether the future activation
     envelope is bounded
   - `copybot_tiny_live_guardrail_audit` answers whether future stop conditions
     and rollback triggers are explicit and bounded
4. Important top-level verdicts:
   - `tiny_live_guardrails_bounded`
   - `tiny_live_guardrails_incomplete`
   - `tiny_live_guardrails_too_open`
   - `tiny_live_guardrails_rollback_contract_incomplete`
   - `tiny_live_guardrails_monitoring_contract_incomplete`
5. Important fields:
   - `evaluation_window_seconds`
   - `max_execution_error_rate_pct`
   - `max_adapter_contract_failure_rate_pct`
   - `max_policy_echo_mismatch_rate_pct`
   - `max_fee_or_slippage_breach_rate_pct`
   - `max_connectivity_degraded_window_seconds`
   - `max_daily_realized_loss_sol`
   - `max_consecutive_hard_failures`
   - `rollback_triggers`
6. A bounded verdict means the future tiny-live rollback envelope is explicit
   and operator-readable. It still does not authorize activation and does not
   override Stage 3.

## Final Activation Checklist

1. Operators now also have one final production-facing synthesis surface:
   - `copybot_activation_checklist_report --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --json`
2. This command stays read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not restart services
   - it does not rerun heavy drills by default
   - it does not submit trades
3. It consolidates accepted prod and non-prod truth into one operator-visible
   checklist:
   - `copybot_pre_activation_gate_report`
   - `copybot_tiny_live_activation_plan`
   - `copybot_tiny_live_guardrail_audit`
   - `copybot_devnet_readiness_report`
4. Important top-level verdicts:
   - `activation_checklist_blocked_by_prod_stage3`
   - `activation_checklist_blocked_by_prod_gate`
   - `activation_checklist_blocked_by_launch_dossier`
   - `activation_checklist_blocked_by_non_prod_readiness`
   - `activation_checklist_discussion_ready_but_not_authorized`
   - `activation_checklist_refused_for_prod_profile_mismatch`
5. `activation_checklist_discussion_ready_but_not_authorized` means the
   production planning-safe gate, bounded launch dossier, bounded guardrails,
   and recent non-prod readiness evidence are all green enough for later
   manual discussion.
6. It still does not mean:
   - production activation is authorized
   - Stage 3 can be skipped later
   - non-prod evidence overrides production truth

## Server target paths

1. `/etc/solana-copy-bot/live.server.toml`
2. `/etc/systemd/system/copybot-executor.service`
3. `/etc/systemd/system/copybot-adapter.service`
4. `/etc/systemd/system/solana-copy-bot.service`
5. `/etc/systemd/system/copybot-execution-mock-upstream.service`
6. `/etc/solana-copy-bot/executor.env`
7. `/etc/solana-copy-bot/adapter.env`
8. `/etc/solana-copy-bot/app.env`
9. `/etc/systemd/system/copybot-discovery-runtime-export.service`
10. `/etc/systemd/system/copybot-discovery-runtime-export.timer`
11. `/etc/systemd/system/copybot-discovery-recent-raw-snapshot.service`
12. `/etc/systemd/system/copybot-discovery-recent-raw-snapshot.timer`

## Apply sequence

1. Copy templates to server target paths and replace placeholders.
2. Ensure secret files exist and are owner-only (`0600`/`0400`).
3. Run `sudo systemctl daemon-reload`.
4. Enable/start in dependency order (non-live contour):
   1. `copybot-execution-mock-upstream.service`
   2. `copybot-executor.service`
   3. `copybot-adapter.service`
   4. `solana-copy-bot.service`
   5. `copybot-discovery-runtime-export.timer`
   6. `copybot-discovery-recent-raw-snapshot.timer`
5. Run preflight sequence from `ROAD_TO_PRODUCTION.md` section `6.1`.
6. Keep `ops/discovery_runtime_restore_runbook.md` on hand for the restore path and drill procedure.
