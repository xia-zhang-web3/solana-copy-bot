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
13. There is one production-critical operator caveat for the bounded raw
    snapshot sidecar used by Stage 3:
   - if `copybot-discovery-recent-raw-snapshot.service` repeatedly reports
     `state=deferred`
   - and `latest_surface_action=deferred_due_to_attempt_budget`
   - and `terminal_reason=attempt_duration_budget_exhausted`
   then the usable bounded `recent_raw` surface is stalled even if live swap
   ingest is still running
14. In that incident shape, Stage 3 will not recover by waiting for “more days”
    alone. Operators must inspect:
   - `source_db_bytes`
   - `source_page_count`
   - `backup_copied_page_count`
   - `backup_remaining_page_count`
   - `last_batch_completed_at`
   - `covered_through_cursor.ts_utc`
15. Current live incident pattern observed on `2026-03-26`:
   - source raw DB had grown beyond `11G`
   - adaptive snapshot policy stayed capped at `pages_per_step=1024` and
     `max_attempt_duration=120000ms`
   - each scheduled attempt copied about `2.0M / 2.72M` pages, then returned
     `Deferred`
   - the old healthy `latest.sqlite` surface was intentionally retained, so
     usable raw coverage froze at the last successful promotion instead of
     advancing toward 5 days
16. The bounded snapshot path now preserves one hidden staged snapshot between
    deferred runs instead of restarting from zero on every timer tick:
   - a deferred run may still return
     `latest_surface_action=deferred_due_to_attempt_budget`
   - but operators must now also inspect:
     - `staged_progress_resumed`
     - `staged_progress_preserved_for_retry`
     - `staged_progress_advanced`
     - `staged_row_count_before_attempt`
     - `staged_row_count_after_attempt`
     - `staged_snapshot_path`
17. Operator interpretation for the fixed convergence contract:
   - `state=deferred` plus
     `staged_progress_preserved_for_retry=true` and
     `staged_progress_advanced=true`
     means the bounded snapshot is still time-bounded but useful forward
     progress was preserved for the next scheduled run
   - `state=deferred` plus
     `staged_progress_preserved_for_retry=true` and
     `staged_progress_advanced=false`
     means the service resumed an older staged snapshot but made no new bounded
     progress this run and should be treated as a real stall
   - `state=written` plus `archive_promoted=true` means the resumed staged
     frontier completed and a newer `latest.sqlite` was promoted again
18. The staged snapshot remains bounded:
   - only one staged snapshot + metadata pair is preserved inside the explicit
     recent-raw snapshot dir
   - successful promotion removes that staged pair
   - archive retention still prunes rotated snapshots without touching the
     protected current staged pair during deferred convergence
19. Current post-rollout production status on `2026-03-26 22:15 UTC`:
   - the emergency livelock fix has already been deployed
   - the bounded snapshot path is no longer restarting from zero on each timer tick
   - first observed resumed attempts on the production host showed:
     - first post-rollout run:
       - `state=deferred`
       - `staged_progress_resumed=false`
       - `staged_progress_preserved_for_retry=true`
       - `staged_progress_advanced=true`
       - `staged_row_count_after_attempt=483328`
     - second post-rollout run:
       - `state=deferred`
       - `staged_progress_resumed=true`
       - `staged_progress_preserved_for_retry=true`
       - `staged_progress_advanced=true`
       - `staged_row_count_before_attempt=483328`
       - `staged_row_count_after_attempt=753664`
20. Operator meaning of that live status:
   - the liveness bug is fixed in production
   - the current state is now “recovering via bounded preserved progress”, not
     “dead stall”
   - Stage 3 is still blocked until a full resumed completion promotes a newer
     `latest.sqlite`
   - the next healthy milestone to watch for is:
     - `state=written`
     - `archive_promoted=true`
     - a newer `covered_through_cursor.ts_utc` in `latest.json`

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

## Tiny-Live Activation Executor

1. Operators now also have a bounded activation/rollback executor path:
   - review current executor truth:
     `copybot_tiny_live_activation_execute --config /etc/solana-copy-bot/live.server.toml --plan --json`
   - render a bounded activation candidate into an explicit temp path:
     `copybot_tiny_live_activation_execute --config /etc/solana-copy-bot/live.server.toml --render-activation-config --output /tmp/tiny-live.activation.toml --expected-source-fingerprint <sha256> --json`
   - render a bounded rollback candidate into an explicit temp path:
     `copybot_tiny_live_activation_execute --config /etc/solana-copy-bot/live.server.toml --render-rollback-config --output /tmp/tiny-live.rollback.toml --expected-source-fingerprint <sha256> --json`
   - verify a rendered config plus its sidecar metadata:
     `copybot_tiny_live_activation_execute --config /tmp/tiny-live.activation.toml --verify-rendered-config --json`
2. This command is the next production-moving step because it turns accepted
   planning truth into a deterministic render/verify executor path without
   mutating the real live server config in this batch.
3. The command still remains bounded and non-authorizing:
   - it does not write `/etc/solana-copy-bot/live.server.toml`
   - it does not restart services
   - it does not submit trades
   - it does not bypass Stage 3 or the consolidated pre-activation gate
4. Activation render reuses the accepted planning truth instead of inventing a
   second planner:
   - `copybot_pre_activation_gate_report`
   - `copybot_tiny_live_activation_plan`
   - the bounded tiny-live policy / guardrail truth embedded in that plan
5. Important executor verdicts:
   - `tiny_live_activation_plan_ready`
   - `tiny_live_activation_rendered`
   - `tiny_live_activation_refused_by_stage3`
   - `tiny_live_activation_refused_by_pre_activation_gate`
   - `tiny_live_activation_refused_by_config_drift`
   - `tiny_live_activation_verify_ok`
   - `tiny_live_activation_verify_invalid`
   - `tiny_live_rollback_rendered`
   - `tiny_live_rollback_verify_ok`
6. The rendered artifacts include only the bounded overlay fields plus a sidecar
   metadata file:
   - input config fingerprint
   - exact rendered field expectations
   - pre-activation gate verdict used
   - activation-plan verdict used
7. Rollback render is deterministic and explicitly forces `execution.enabled=false`
   even if the source config has already drifted into an activated posture.
8. A rendered or verified config is still not production authorization. Stage 3
   remaining non-green must continue to block any future production-facing
   activation apply step.

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

## Activation Decision Packet

1. Operators now also have a final archival-ready export surface:
   - `copybot_activation_decision_packet --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --json --output /var/www/solana-copy-bot/state/activation_decision_packet/latest.json`
2. This command stays read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not rerun heavy drills by default
   - it does not submit trades
3. It reuses the accepted final checklist instead of inventing another
   decision layer:
   - `copybot_activation_checklist_report`
   - current prod and non-prod config truth only for fingerprinting and
     packet metadata
4. The packet includes:
   - final checklist verdict, blockers, warnings, and nested summaries
   - prod and non-prod config paths
   - execution state
   - optional operator note
   - build/git metadata when available
   - redacted config fingerprints suitable for later change review
5. Important packet verdicts:
   - `decision_packet_blocked`
   - `decision_packet_discussion_ready_but_not_authorized`
   - `decision_packet_refused_for_profile_mismatch`
6. A discussion-ready packet is still not activation authorization. It is an
   archival decision artifact for later manual review, change discussion, and
   incident/postmortem comparison.

## Activation Runbook

1. Operators now also have a final planning-only handoff/runbook generator:
   - `copybot_activation_runbook --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --json --output /var/www/solana-copy-bot/state/activation_runbook/latest.json --markdown-output /var/www/solana-copy-bot/state/activation_runbook/latest.md`
2. This command stays read-only and planning-safe:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not rerun heavy drills by default
   - it does not submit trades
3. It reuses the accepted activation decision packet and launch dossier
   instead of inventing another approval layer:
   - `copybot_activation_decision_packet`
   - `copybot_tiny_live_activation_plan`
4. The exported runbook is meant for later manual handoff/review and includes:
   - current state and preflight checks
   - explicit stop-here conditions
   - bounded activation candidate steps
   - post-change verification commands
   - rollback triggers and rollback procedure
   - explicit not-authorized disclaimer
5. Important runbook verdicts:
   - `runbook_blocked`
   - `runbook_discussion_ready_but_not_authorized`
   - `runbook_refused_for_profile_mismatch`
6. Difference from the decision packet:
   - the decision packet is the archival summary artifact
   - the runbook is the human-usable ordered handoff package built from that
     packet and the accepted launch dossier
7. A discussion-ready runbook is still not activation authorization. It is a
   planning-only operator handoff for a later explicit manual change review.

## Activation Decision History

1. Operators now also have a final artifact-analysis surface over exported
   decision packets:
   - history summary:
     `copybot_activation_decision_history_report --history-dir /var/www/solana-copy-bot/state/activation_decision_packet/archive --json`
   - diff mode:
     `copybot_activation_decision_history_report --compare /var/www/solana-copy-bot/state/activation_decision_packet/archive/older.json /var/www/solana-copy-bot/state/activation_decision_packet/archive/newer.json --json`
2. This command stays read-only and archival-only:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not mutate packet artifacts
   - it does not rerun heavy prod or non-prod drills by default
   - it does not submit trades
3. History mode answers:
   - latest verdict and recent verdict progression
   - blocked vs discussion-ready packet counts
   - latest prod gate / non-prod readiness summaries
   - whether prod or non-prod config fingerprints changed over time
   - whether blockers are narrowing or widening
4. Diff mode answers:
   - checklist verdict change
   - blockers and warnings added/removed
   - prod gate / launch dossier / guardrail / non-prod readiness changes
   - prod and non-prod config fingerprint changes
   - build/git drift when present
5. Important history verdicts:
   - `decision_history_latest_blocked`
   - `decision_history_latest_discussion_ready`
   - `decision_history_insufficient_packets`
   - `decision_history_compare_ready`
   - `decision_history_invalid_artifact`
6. This tool analyzes previously exported packet artifacts only. It is useful
   for later review, change comparison, and incident audit trail, but it still
   does not authorize production activation.

## Activation Artifact Archive

1. Operators now also have a read-only archive index and retention-preview
   surface for exported activation artifacts, plus a bounded cleanup apply
   mode built on the same preview logic:
   - index/report mode:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --json`
   - retention-plan mode:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --retention-plan --keep-latest 10 --json`
   - retention-apply mode:
     `copybot_activation_artifact_archive --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --retention-apply --keep-latest 10 --json`
2. This command stays maintenance-safe:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not rerun heavy prod or non-prod logic
   - it only deletes archive artifact files selected by the exact same
     generation-selection logic used by retention preview
   - it never touches files outside the requested archive dir
   - it does not submit trades
3. Index/report mode answers:
   - packet/runbook artifact counts
   - latest packet and latest runbook
   - missing packet/runbook pairings
   - malformed artifacts
   - fingerprint/build/git distribution drift across the archive
4. Retention-plan mode answers:
   - which latest packet-backed generations would be kept
   - which older packet-backed generations would be removed
   - which orphaned or malformed artifacts need manual review first
5. Retention-apply mode is conservative by default:
   - cleanup is blocked if invalid or malformed artifacts are present
   - orphan runbook generations and orphan markdown are left untouched
   - only packet-backed generations outside `keep-latest` are removed
   - output lists kept generations, removed generations, and exact file paths
     removed
6. Important archive verdicts:
   - `archive_health_ok`
   - `archive_health_missing_pairings`
   - `archive_health_invalid_artifacts_present`
   - `archive_retention_plan_ready`
   - `archive_retention_plan_insufficient_artifacts`
   - `archive_cleanup_applied`
   - `archive_cleanup_blocked_by_invalid_artifacts`
   - `archive_cleanup_nothing_to_do`
   - `archive_cleanup_failed_partial`
7. This tool only manages exported activation artifacts. It still does not
   authorize production activation and does not override the Stage 3 prod gate.

## Activation Artifact Manifest

1. Operators now also have an explicit manifest/integrity surface for exported
   activation artifacts:
   - generate:
     `copybot_activation_artifact_manifest --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --generate-manifest --output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --json`
   - verify:
     `copybot_activation_artifact_manifest --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --verify-manifest /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --json`
2. Generate mode records:
   - packet/runbook generation membership
   - artifact paths relative to the archive root
   - SHA-256 hashes for packet/runbook artifacts
   - generation identity via decision-packet timestamp plus prod/non-prod
     config fingerprints
   - manifest creation time and tool/build version
3. Verify mode checks:
   - missing artifact files
   - changed artifact hashes
   - unexpected extra recognized artifact files
   - generation membership drift
   - invalid or malformed archive artifacts that block trust in the result
4. Important manifest verdicts:
   - `artifact_manifest_generated`
   - `artifact_manifest_verified`
   - `artifact_manifest_drift_detected`
   - `artifact_manifest_invalid`
   - `artifact_manifest_missing_files`
5. This surface is archive integrity only:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not rerun heavy prod or non-prod logic
   - it only writes the manifest file explicitly requested by the operator
   - it does not authorize production activation or override the Stage 3 prod
     gate

## Activation Artifact Bundle

1. Operators now also have a portable bundle layer for one selected
   packet-backed activation artifact generation:
   - export:
     `copybot_activation_artifact_bundle --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --export-bundle --generation 2026-03-26T12:00:00Z --output /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
   - verify:
     `copybot_activation_artifact_bundle --verify-bundle /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
2. Export mode stays bounded and explicit:
   - it exports exactly one packet-backed generation selected by timestamp or
     full generation id
   - it includes only that generation's decision packet json, runbook json,
     and runbook markdown when present
   - it writes a self-contained bundle manifest with generation identity,
     bundled file membership, and SHA-256 hashes
3. Verify mode checks:
   - bundle manifest structure
   - missing or tampered bundled files
   - unexpected extra bundled files
   - generation identity / membership mismatches inside the bundle
4. Important bundle verdicts:
   - `artifact_bundle_exported`
   - `artifact_bundle_verified`
   - `artifact_bundle_invalid`
   - `artifact_bundle_drift_detected`
   - `artifact_bundle_generation_not_found`
5. This surface is artifact handling only:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not rerun heavy prod or non-prod logic
   - it does not modify existing archive artifacts
   - it does not authorize production activation or override the Stage 3 prod
     gate

## Activation Artifact Provenance

1. Operators now also have one provenance-oriented report across archive,
   manifests, and bundles:
   `copybot_activation_artifact_provenance_report --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --json`
2. This report correlates lineage by generation identity:
   - decision-packet timestamp
   - prod config fingerprint
   - non-prod config fingerprint
3. It answers:
   - which packet-backed generations exist in the archive
   - which generations have manifest coverage
   - which generations have bundle coverage
   - which manifests or bundles refer to missing archive generations
   - where malformed lineage artifacts block trust
4. Important provenance verdicts:
   - `artifact_provenance_complete`
   - `artifact_provenance_incomplete`
   - `artifact_provenance_invalid_artifacts_present`
   - `artifact_provenance_inconsistent_lineage`
5. This surface is lineage/reporting only:
   - it does not enable `execution.enabled`
   - it does not mutate config
   - it does not modify archive, manifest, or bundle artifacts
   - it does not rerun heavy prod or non-prod logic
   - it does not authorize production activation or override the Stage 3 prod
     gate

## Activation Artifact Publish

1. Operators now also have a one-shot publish pipeline for one complete review
   generation:
   `copybot_activation_artifact_publish --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --bundle-output-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
2. This command stays artifact-writing only and bounded:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not delete or rewrite unrelated archive generations
   - it does not submit trades
3. In one pass it reuses the accepted packet, runbook, manifest, and bundle
   surfaces to:
   - create one decision packet
   - create one runbook json and markdown artifact
   - place them into a deterministic archive generation directory
   - optionally write a fresh archive manifest snapshot
   - optionally export a portable review bundle for that exact generation
4. Path safety is conservative:
   - it writes only under explicitly provided output paths
   - generation directory naming is deterministic from packet timestamp plus
     prod/non-prod config fingerprints
   - existing generation directories are not overwritten silently
5. Important publish verdicts:
   - `artifact_publish_succeeded`
   - `artifact_publish_blocked_by_checklist`
   - `artifact_publish_blocked_by_invalid_archive_state`
   - `artifact_publish_partial_manifest_skipped`
   - `artifact_publish_failed`
6. This command is still not activation authorization. It is the operator-safe
   glue layer that turns the accepted artifact chain into one bounded review
   generation without touching production execution state.

## Activation Artifact Channel

1. Operators now also have an explicit channel/latest-pointer manager for the
   current review generation:
   - report:
     `copybot_activation_artifact_channel --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --report --json`
   - promote:
     `copybot_activation_artifact_channel --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --promote --generation 2026-03-26T12:00:00+00:00|prod_fp|non_prod_fp --allow-overwrite --json`
   - verify:
     `copybot_activation_artifact_channel --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --verify --json`
2. The channel surface writes only explicit metadata under `--channel-dir`:
   - default channel name is `current_review`
   - channel state is stored as JSON metadata rather than hidden symlink
     conventions
   - archive generations themselves are never rewritten by this command
3. Promote mode is bounded and conservative:
   - it refuses to point at a non-existent packet-backed generation
   - it refuses to promote over invalid archive state
   - it does not silently overwrite existing channel metadata unless
     `--allow-overwrite` is passed
   - optional `--manifest-path` and `--bundle-path` references are verified
     before channel metadata is written
4. Verify mode answers:
   - whether the channel points to an existing archive generation
   - whether referenced packet/runbook paths still exist
   - whether optional manifest or bundle references still verify
   - whether channel metadata is internally consistent with archive lineage
5. Important channel verdicts:
   - `artifact_channel_ok`
   - `artifact_channel_missing_target`
   - `artifact_channel_inconsistent`
   - `artifact_channel_promoted`
   - `artifact_channel_refused_without_overwrite`
   - `artifact_channel_invalid_metadata`
6. This surface is artifact-channel management only:
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not rerun heavy prod or non-prod logic
   - it does not authorize production activation or override the Stage 3 prod
     gate

## Activation Artifact Release

1. Operators now also have one bounded release flow over publish + optional
   channel promotion:
   - publish only:
     `copybot_activation_artifact_release --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --bundle-output-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --json`
   - publish and promote current review channel:
     `copybot_activation_artifact_release --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --manifest-output /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json --bundle-output-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z --promote-channel --channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --allow-channel-overwrite --json`
2. This flow stays artifact-only and bounded:
   - it may publish one new review generation
   - it may optionally update explicit channel metadata under `--channel-dir`
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not restart services
   - it does not delete archive generations or rewrite unrelated artifacts
3. Partial-success semantics stay honest:
   - if publish does not complete cleanly, channel promotion is not attempted
   - if publish succeeds but channel promotion is blocked, the report stays
     non-green and still shows the published generation paths
   - existing generation directories and existing channel metadata are not
     overwritten silently
4. Important release verdicts:
   - `artifact_release_published`
   - `artifact_release_published_and_promoted`
   - `artifact_release_publish_failed`
   - `artifact_release_channel_promote_blocked`
   - `artifact_release_failed`
5. This is still artifact workflow only:
   - it does not authorize production activation
   - it does not override the Stage 3 prod gate
   - it does not change live trading state

## Activation Artifact Release History

1. Operators now also have a first-class history and diff surface over exported
   release artifacts:
   - history summary:
     `copybot_activation_artifact_release_history --history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --json`
   - compare two release artifacts:
     `copybot_activation_artifact_release_history --compare /var/www/solana-copy-bot/state/activation_artifacts/releases/release-older.json /var/www/solana-copy-bot/state/activation_artifacts/releases/release-newer.json --json`
2. The history surface answers:
   - when releases happened
   - whether the latest release was publish-only or published-and-promoted
   - whether channel promotion is progressing or repeatedly blocked
   - how the current review generation changed over time
3. The compare surface answers:
   - release verdict drift
   - generation drift
   - packet/runbook/manifest/bundle path drift
   - channel promotion and channel target drift
   - whether the newer release narrowed or widened blockers
4. Important history verdicts:
   - `artifact_release_history_latest_published`
   - `artifact_release_history_latest_published_and_promoted`
   - `artifact_release_history_latest_blocked`
   - `artifact_release_history_insufficient_artifacts`
   - `artifact_release_history_compare_ready`
   - `artifact_release_history_invalid_artifact`
5. This is artifact/release analysis only:
   - it does not rerun heavy prod or non-prod logic
   - it does not mutate archive or channel metadata
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact Release Archive Publisher

1. Operators now also have a first-class deterministic archive/pointer surface
   for release artifacts themselves:
   - publish one persisted release artifact:
     `copybot_activation_artifact_release_publish_report --publish --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --json`
   - publish and update latest release pointer:
     `copybot_activation_artifact_release_publish_report --publish --config /etc/solana-copy-bot/live.server.toml --non-prod-config /etc/solana-copy-bot/devnet.server.toml --archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --persist-latest-pointer --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --allow-latest-pointer-overwrite --json`
   - verify latest release pointer:
     `copybot_activation_artifact_release_publish_report --verify-latest --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
2. The release archive layout is deterministic and bounded:
   - one persisted release artifact file is written under explicit
     `--release-archive-dir`
   - filename is derived from `released_at` plus release verdict
   - collisions refuse overwrite rather than replacing an existing release
     artifact silently
3. Latest release pointer behavior is explicit:
   - pointer metadata is stored separately under explicit `--latest-pointer-dir`
   - it records target release artifact path, released-at source, verdict, and
     generation identity
   - pointer replacement requires explicit `--allow-latest-pointer-overwrite`
4. Legacy timestamp ambiguity stays visible:
   - compat-loaded release artifacts without stored `released_at` are surfaced
     explicitly
   - if a legacy target lacks both `released_at` and a deterministic generation
     timestamp, verify mode reports that the pointer is not suitable for ordered
     history confidence
   - `copybot_activation_artifact_release_history --history-dir <release-archive-dir>`
     can read the same persisted release archive directly
5. Important publisher verdicts:
   - `artifact_release_report_published`
   - `artifact_release_report_published_and_pointed_latest`
   - `artifact_release_report_pointer_blocked`
   - `artifact_release_report_failed`
   - `artifact_release_report_verify_ok`
   - `artifact_release_report_verify_missing_target`
   - `artifact_release_report_verify_ambiguous_timestamp`
   - `artifact_release_report_invalid_metadata`
6. This remains artifact/release management only:
   - it does not modify review generations
   - it does not enable `execution.enabled`
   - it does not mutate live config
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact Release Provenance Report

1. Operators now also have a provenance-oriented surface for the release side:
   - release provenance report:
     `copybot_activation_artifact_release_provenance_report --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --json`
2. The report correlates three release-side inputs without rerunning publish or
   heavy prod/non-prod logic:
   - persisted release artifacts in the deterministic release archive
   - latest-pointer metadata plus target verification
   - release history inputs from a history dir or explicit release artifact
     paths
3. Release provenance completeness means:
   - archive releases are covered by the history surface
   - latest pointer resolves to an existing release artifact and matches its
     recorded identity
   - release-side lineage is not dangling or silently inconsistent
4. Legacy timestamp ambiguity remains explicit:
   - compat-loaded release artifacts without stored `released_at` are surfaced
     honestly
   - if ordering still depends on ambiguous legacy timestamps, the provenance
     report does not return a clean green verdict
5. Latest pointer participates directly in provenance:
   - the report shows which release artifact it selects
   - whether it matches the latest archive/history release
   - whether it is behind, ahead of history, missing, or inconsistent
6. This is release artifact analysis only:
   - it does not rewrite release archive artifacts
   - it does not rewrite latest-pointer metadata
   - it does not enable execution or authorize activation

## Activation Artifact Linkage Report

1. Operators now also have an explicit linkage surface between persisted
   release artifacts and the persisted review-generation archive:
   - release archive plus latest release pointer plus optional current review
     channel:
     `copybot_activation_artifact_linkage_report --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --review-channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --json`
   - explicit release artifact set instead of a release archive:
     `copybot_activation_artifact_linkage_report --release-artifact /var/www/solana-copy-bot/state/activation_artifacts/releases/release__2026-03-26T12-00-00Z__artifact_release_published.json --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --json`
2. Linkage completeness means:
   - each examined release artifact resolves to an existing persisted review
     generation
   - referenced decision packet and runbook files still exist and belong to the
     expected generation
   - the latest release pointer, if supplied, still resolves to a release
     artifact whose linked review generation is present
   - the current review channel, if supplied, does not silently diverge from
     the release-side latest selection
3. Legacy linkage ambiguity remains explicit:
   - older release artifacts with weak linkage context do not get a clean green
     verdict unless they still resolve deterministically
   - missing generation refs, missing packet/runbook refs, or refs outside the
     review archive are surfaced directly in the report
4. Important linkage verdicts:
   - `artifact_linkage_complete`
   - `artifact_linkage_incomplete`
   - `artifact_linkage_invalid_artifacts_present`
   - `artifact_linkage_inconsistent`
   - `artifact_linkage_ambiguous_legacy_reference`
5. This remains artifact analysis only:
   - it does not rewrite release artifacts or review generations
   - it does not rewrite latest-pointer or review-channel metadata
   - it does not enable execution or authorize activation

## Activation Artifact State Report

1. Operators now also have one final end-to-end current-state surface over the
   persisted artifact chain:
   `copybot_activation_artifact_state_report --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --review-manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --review-bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --review-channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --release-history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
2. The new surface reuses accepted artifact reports instead of inventing a new
   parser:
   - current review selection from `copybot_activation_artifact_channel`
   - latest release selection from
     `copybot_activation_artifact_release_publish_report`
   - review-side provenance from `copybot_activation_artifact_provenance_report`
   - release-side provenance from
     `copybot_activation_artifact_release_provenance_report`
   - current release-to-review linkage from
     `copybot_activation_artifact_linkage_report`
3. `artifact_state_coherent` means:
   - the current review channel selects a valid persisted review generation
   - the latest release pointer selects a valid persisted release artifact
   - review-side provenance is healthy enough
   - release-side provenance is healthy enough
   - current review and latest release selections agree on generation identity
   - no ambiguous legacy timestamp/reference state is degrading confidence
4. Legacy ambiguity remains explicit:
   - compat-loaded release artifacts without deterministic timestamp confidence
     do not get a false clean-green current-state verdict
   - if current/latest confidence depends on ambiguous legacy release state, the
     report returns `artifact_state_ambiguous_legacy_state`
5. This remains artifact analysis only:
   - it does not rewrite archive contents
   - it does not rewrite review-channel or latest-pointer metadata
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Publisher

1. Operators can now persist the current end-to-end artifact state into a
   deterministic snapshot archive instead of manually saving point-in-time
   console output:
   - publish one snapshot artifact:
     `copybot_activation_artifact_state_publish_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --publish --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --review-manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --review-bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --review-channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --release-history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
   - publish and update the snapshot latest pointer:
     `copybot_activation_artifact_state_publish_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --publish --persist-latest-pointer --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --review-manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --review-bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --review-channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --release-history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
   - inspect or verify the snapshot latest pointer:
     `copybot_activation_artifact_state_publish_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --verify-latest --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --json`
2. The snapshot artifact is a persisted copy of the accepted
   `copybot_activation_artifact_state_report` result, not a new evaluation
   contract:
   - the state verdict stays explicit
   - current review generation selection stays explicit
   - current latest release selection stays explicit
   - ambiguity and inconsistency remain visible in the persisted artifact
3. Deterministic archive behavior is conservative:
   - snapshot files are written under the explicit archive dir only
   - collisions do not silently overwrite an existing snapshot
   - latest-pointer metadata is written only when explicitly requested
   - latest-pointer verification checks that the target snapshot still exists
     and still parses as a valid persisted state snapshot
4. This remains artifact-state management only:
   - it does not rewrite review-generation artifacts
   - it does not rewrite release artifacts
   - it does not enable execution
   - it does not authorize activation

## Activation Artifact State History

1. Operators now also have a first-class history/diff surface over persisted
   state snapshots:
   - summary over a snapshot archive:
     `copybot_activation_artifact_state_history --history-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --json`
   - compare two persisted snapshots:
     `copybot_activation_artifact_state_history --compare /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots/state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots/state_snapshot__2026-03-27T12-00-00Z__artifact_state_incomplete.json --json`
2. History summary answers:
   - the latest persisted state verdict
   - how many snapshots were coherent vs incomplete vs inconsistent vs
     ambiguous
   - the latest selected review generation and latest selected release
     generation
   - whether review/release alignment has remained stable or drifted
   - whether the persisted snapshot set is too sparse or stale for current
     operator confidence
3. Compare mode answers:
   - state verdict change
   - review-channel selection change
   - latest-release selection change
   - alignment change
   - review/release provenance drift
   - linkage drift
   - ambiguous legacy count drift
4. Ambiguity remains non-green in the temporal view too:
   - `artifact_state_ambiguous_legacy_state` does not collapse into a healthy
     history summary
   - broken/inconsistent state remains stronger than ambiguity-only concerns
5. This remains artifact analysis only:
   - it reads persisted state snapshots instead of rerunning heavy operator
     flows
   - it does not rewrite state snapshots or pointer metadata
   - it does not enable execution or authorize activation

## Activation Artifact State Snapshot Provenance

1. Operators now also have a provenance-oriented surface for the persisted
   state-snapshot layer itself:
   - audit snapshot archive + latest pointer + history coverage:
     `copybot_activation_artifact_state_provenance_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --history-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --json`
2. Provenance completeness means:
   - persisted state snapshots exist in the archive
   - the snapshot latest pointer resolves to a real persisted snapshot
   - the history surface covers the same snapshot lineage
   - archive/history/pointer lineage does not drift away from itself
3. Ambiguity and non-green state snapshots stay explicit:
   - ambiguous current-state snapshots do not get flattened into a healthy
     provenance verdict
   - if the latest pointer or history depends on ambiguous or otherwise
     non-green snapshots, the provenance result stays non-green
4. This remains artifact analysis only:
   - it does not rewrite state snapshots
   - it does not rewrite snapshot latest-pointer metadata
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Linkage

1. Operators now also have a direct linkage audit from persisted state
   snapshots back to the current underlying review/release artifact chain:
   - `copybot_activation_artifact_state_linkage_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --review-manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --review-bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --review-channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --release-history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
2. State-snapshot linkage means:
   - each persisted snapshot’s selected review generation still resolves in the
     current review archive
   - each persisted snapshot’s selected latest release generation still
     resolves in the current release archive/history inputs
   - the state snapshot latest pointer, if supplied, still resolves to a
     snapshot whose summarized selections remain valid now
   - persisted snapshots that summarize ambiguous or otherwise non-green state
     do not get flattened into clean linkage
3. The report keeps drift against current reality explicit:
   - it shows when persisted selections no longer match the current review
     channel
   - it shows when persisted selections no longer match the current latest
     release selection
   - it keeps missing review/release generation references visible in
     aggregate counters
4. Important linkage verdicts:
   - `artifact_state_linkage_complete`
   - `artifact_state_linkage_incomplete`
   - `artifact_state_linkage_invalid_artifacts_present`
   - `artifact_state_linkage_inconsistent`
   - `artifact_state_linkage_ambiguous_legacy_state`
5. This remains artifact analysis only:
   - it does not rewrite state snapshots
   - it does not rewrite review or release artifacts
   - it does not rewrite pointer or channel metadata
   - it does not enable execution or authorize activation

## Activation Artifact State Snapshot Archive Manager

1. Operators now also have a first-class archive-management surface for
   persisted state snapshots:
   - report/archive health:
     `copybot_activation_artifact_state_archive --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --report --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --json`
   - retention preview:
     `copybot_activation_artifact_state_archive --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --retention-plan --keep-latest 10 --json`
   - bounded retention apply:
     `copybot_activation_artifact_state_archive --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --retention-apply --keep-latest 10 --json`
2. Report mode answers:
   - how many persisted state snapshots exist
   - the latest snapshot verdict and whether coverage is sparse or stale
   - whether malformed snapshot artifacts are present
   - whether a supplied snapshot latest pointer is valid and which snapshot it
     protects
3. Retention preview/apply are deliberately conservative:
   - preview and apply use the exact same keep/remove selection logic
   - a valid snapshot latest pointer target is protected from deletion
   - malformed snapshots block cleanup by default
   - dangling or invalid latest-pointer metadata is surfaced explicitly instead
     of being silently ignored
   - cleanup never rewrites snapshot contents and never rewrites latest-pointer
     metadata in this batch
4. This remains archive management only:
   - it does not rewrite review-generation artifacts
   - it does not rewrite release artifacts
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle

1. Operators now also have a portable bundle layer for one selected persisted
   state snapshot:
   - export one bundled snapshot:
     `copybot_activation_artifact_state_bundle --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --export-bundle --snapshot state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json --output /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/current --json`
   - verify a transferred bundle:
     `copybot_activation_artifact_state_bundle --verify-bundle /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/current --json`
2. Export mode is deliberately bounded:
   - it selects exactly one persisted state snapshot by path, file name, or
     `snapshotted_at`
   - it writes one self-contained bundle directory plus a bundle manifest with
     snapshot identity and file hashes
   - it does not export unrelated snapshots
3. Bundle integrity is intentionally separate from snapshot health:
   - verify mode checks structure, hashes, and snapshot identity metadata
   - ambiguous or otherwise non-green snapshot state stays explicit in bundle
     metadata and verify output
   - a bundle can verify cleanly while still preserving a non-green
     `state_verdict`
4. This remains artifact handling only:
   - it does not rewrite the state snapshot archive
   - it does not rewrite latest-pointer metadata
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle Provenance

1. Operators now also have a provenance-oriented surface over the persisted
   state snapshot bundle layer:
   - audit archive + history + latest pointer + exported bundles together:
     `copybot_activation_artifact_state_bundle_provenance_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --history-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle --json`
2. Bundle provenance completeness means:
   - persisted state snapshots exist in the archive
   - history covers the same snapshot lineage
   - exported bundles resolve back to those persisted snapshots
   - the latest-pointer-selected snapshot, if supplied, still has honest
     bundle coverage
3. Bundle integrity is still distinct from snapshot health:
   - an ambiguous or otherwise non-green bundled snapshot stays explicit in
     provenance output
   - the latest pointer can be structurally valid and still keep provenance
     non-green if it selects an ambiguous/non-green snapshot
   - bundle coverage that exists only for stale snapshots is reported as
     incomplete provenance, not as a healthy current state
4. This remains artifact analysis only:
   - it does not rewrite state snapshots
   - it does not rewrite latest-pointer metadata
   - it does not rewrite bundle contents
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle Archive Publisher

1. Operators now also have a deterministic archive/pointer surface for
   persisted state-snapshot bundles themselves:
   - publish one selected snapshot bundle into the bundle archive:
     `copybot_activation_artifact_state_bundle_publish_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --publish --snapshot state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json --json`
   - publish and update the current/latest bundle pointer:
     `copybot_activation_artifact_state_bundle_publish_report --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --publish --snapshot state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json --persist-latest-pointer --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --json`
   - report or verify the current/latest archived bundle pointer:
     `copybot_activation_artifact_state_bundle_publish_report --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --report-latest --json`
     `copybot_activation_artifact_state_bundle_publish_report --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --verify-latest --json`
2. The archive layout is intentionally deterministic and conservative:
   - one archived bundle directory is derived from the selected persisted state
     snapshot file name
   - publish refuses to silently overwrite an existing archived bundle on
     collision
   - latest-pointer metadata is written only under the explicit
     `--bundle-latest-pointer-dir`, and overwrite still requires an explicit
     flag
3. Current/latest bundle verification is intentionally separate from snapshot
   state health:
   - verify/report latest checks pointer metadata, bundle existence, bundle
     integrity, and pointer-target identity
   - it also preserves the selected snapshot's original
     `state_verdict`/reason/ambiguity fields
   - an archived bundle can be integrity-clean while still preserving an
     ambiguous or otherwise non-green snapshot truth
4. This remains artifact handling only:
   - it does not rewrite the state snapshot archive
   - it does not rewrite existing bundle contents
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle Archive Manager

1. Operators now also have a first-class archive-management surface for
   archived state-snapshot bundles:
   - inspect archive health:
     `copybot_activation_artifact_state_bundle_archive --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --report --json`
   - preview retention with latest-pointer protection:
     `copybot_activation_artifact_state_bundle_archive --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --retention-plan --keep-latest 5 --json`
   - boundedly apply the exact same retention plan:
     `copybot_activation_artifact_state_bundle_archive --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --retention-apply --keep-latest 5 --json`
2. Archive management is intentionally conservative:
   - report mode counts clean vs invalid/drifted archived bundles and shows the
     latest archived bundle by deterministic archive ordering
   - retention preview and apply use the exact same selection logic
   - a valid latest bundle pointer target is protected from cleanup
   - dangling or invalid latest bundle pointer metadata stays explicit and
     blocks cleanup instead of being ignored
3. Bundle integrity is still distinct from artifact-state health:
   - archived bundles can verify cleanly while still preserving incomplete,
     inconsistent, or ambiguous snapshot truth
   - if the latest bundle pointer selects a non-green snapshot bundle, report
     mode stays non-green and shows that selected snapshot verdict/reason
   - malformed or drifted bundles are surfaced explicitly and block cleanup
     until reviewed
4. This remains artifact handling only:
   - it never rewrites the state snapshot archive
   - it never rewrites existing archived bundle contents
   - it never rewrites latest bundle pointer metadata in this batch
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle Archive Provenance

1. Operators now also have a provenance-oriented surface over deterministic
   archived bundles, the latest bundle pointer, and the current persisted
   state-snapshot surfaces:
   - `copybot_activation_artifact_state_bundle_archive_provenance_report --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --history-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --json`
2. Archived-bundle provenance completeness means:
   - deterministic archived bundles verify cleanly
   - the latest bundle pointer resolves to a real archived bundle
   - those archived bundles still resolve back to the current persisted state
     snapshot archive and history context
   - current snapshot truth is not missing archived-bundle coverage
3. Integrity-green archived bundles still do not upgrade snapshot truth:
   - archived bundles over ambiguous or otherwise non-green snapshots remain
     explicit and keep provenance non-green or explicitly ambiguous
   - a valid latest bundle pointer can still leave provenance non-green if it
     selects a stale, foreign, or non-green snapshot bundle
   - bundle archive coverage is checked against the current snapshot archive
     root, not just a loose snapshot identity tuple
4. This remains artifact analysis only:
   - it does not rewrite the state snapshot archive
   - it does not rewrite archived bundle contents
   - it does not rewrite latest bundle pointer metadata
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle Archive History

1. Operators now also have a first-class history/diff surface over
   deterministic archived bundles:
   - summarize archived-bundle progression:
     `copybot_activation_artifact_state_bundle_archive_history --history --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --json`
   - compare two archived bundles directly:
     `copybot_activation_artifact_state_bundle_archive_history --compare /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive/state_snapshot_bundle__state_snapshot__2026-03-26T12-00-00Z__artifact_state_incomplete /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive/state_snapshot_bundle__state_snapshot__2026-03-27T12-00-00Z__artifact_state_coherent --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --json`
2. History summary keeps archive ordering and pointer context explicit:
   - it shows the latest archived bundle by deterministic archive naming
   - it counts coherent vs incomplete vs inconsistent vs ambiguous embedded
     snapshot truth across archived bundles
   - if the latest bundle pointer is supplied, it shows whether that pointer
     still matches latest-by-archive or is intentionally/stale selecting an
     older bundle
3. Compare mode keeps integrity distinct from snapshot truth:
   - it shows snapshot verdict drift, reason drift, selected review/release id
     drift, ambiguity drift, and `coherent_for_review_operations` drift
   - it does not flatten an integrity-clean archived bundle into coherent
     activation state automatically
   - ambiguous or otherwise non-green archived bundles remain explicit in both
     summary and compare output
4. This remains artifact analysis only:
   - it does not rewrite the state snapshot archive
   - it does not rewrite archived bundle contents
   - it does not rewrite latest bundle pointer metadata
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

## Activation Artifact State Snapshot Bundle Archive Linkage

1. Operators now also have an explicit linkage surface from deterministic
   archived bundles back to the current underlying review/release artifact
   chain:
   - `copybot_activation_artifact_state_bundle_archive_linkage_report --bundle-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/archive --bundle-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshot_bundle/latest --state-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/state_snapshots --snapshot-latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/state_latest --review-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/archive --review-manifest-dir /var/www/solana-copy-bot/state/activation_artifacts/archive_manifest --review-bundle-dir /var/www/solana-copy-bot/state/activation_artifacts/bundles --review-channel-dir /var/www/solana-copy-bot/state/activation_artifacts/channel --release-archive-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --release-history-dir /var/www/solana-copy-bot/state/activation_artifacts/releases --latest-pointer-dir /var/www/solana-copy-bot/state/activation_artifacts/release_latest --json`
2. Archived-bundle linkage means more than bundle integrity:
   - each archived bundle is checked to see whether its selected review
     generation still exists in the current review archive
   - each archived bundle is checked to see whether its selected latest release
     generation still exists in the current release archive/history surface
   - the report also shows whether archived bundles still agree with the
     current review channel and current latest release pointer
3. Latest bundle pointer context stays explicit:
   - if the latest bundle pointer selects an archived bundle with stale or
     broken underlying linkage, the report stays non-green and says so
   - if the pointer selects an ambiguous or otherwise non-green archived bundle,
     that truth remains explicit
   - integrity-green archived bundles still do not automatically imply coherent
     live linkage
4. This remains artifact handling/analysis only:
   - it does not rewrite archived bundles
   - it does not rewrite bundle pointer metadata
   - it does not rewrite the current state snapshot archive
   - it does not enable execution
   - it does not authorize activation or override the Stage 3 prod gate

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
