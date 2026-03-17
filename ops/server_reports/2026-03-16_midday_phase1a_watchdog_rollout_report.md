# 2026-03-16 midday — Phase 1A watchdog rollout report

## Scope

- Deploy commit `6218368 Add post-bootstrap rotation watchdog`
- Verify post-restart selection state and runtime behavior on live server
- Confirm whether live exercised the new `post_bootstrap_rotation_blocked_cap_truncated` path or a different fail-close path

## Server

- Host: `ubuntu@52.28.0.218`
- Repo path: `/var/www/solana-copy-bot`
- Pre-deploy live commit: `f924312`
- Deployed commit: `6218368`

## Build / rollout

- Checked out `6218368` on server in detached HEAD
- Built release binaries:
  - `copybot-app`
  - `copybot-adapter`
  - `copybot-executor`
- Restarted services
- New app runtime:
  - `MainPID=315039`
  - `NRestarts=0`
  - `ActiveEnterTimestamp=2026-03-16 11:21:29 UTC`

## Backup note

- Started offline DB copy before restart:
  - `/var/www/solana-copy-bot/state/live_copybot.db.bak_before_phase1a_20260316_111838Z`
- Copy entered long I/O wait and was aborted
- Background copy processes were cleaned up

## Pre-rollout live state

- `discovery_strategy_state.trusted_selection_bootstrap_required = 0`
- `trusted_selection_reason = trusted_selection_bootstrap_satisfied`
- `followlist.active = 15`

This was the known frozen bridged-bootstrap state from the earlier recovery.

## What actually happened after restart

Startup log:

- `holding recovered active follow wallets out of runtime snapshot until discovery publishes a trusted selection state`
- `trusted_selection_bootstrap_required = false`
- `trusted_selection_reason = trusted_selection_bootstrap_satisfied`
- `trusted_selection_state = None`
- `trusted_selection_legacy_bool_fallback_used = true`
- `trusted_selection_active_snapshot_id = None`
- `trusted_selection_active_snapshot_window_start = None`
- `trusted_selection_last_bootstrap_source_kind = None`

This means startup did **not** see a fully populated typed trusted-selection record. It fell back through the legacy bool compatibility path.

Live DB state after restart:

- `trusted_selection_bootstrap_required = 1`
- `trusted_selection_reason = trusted_selection_bootstrap_unavailable`
- `trusted_selection_state = invalid`
- `followlist.active = 0`

Observed log path:

- `discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`
- first clear: `recovered_follow_wallets_cleared = 15`
- later repeats: `recovered_follow_wallets_cleared = 0`

## Important verdict

The rollout **did remove the silent frozen-bootstrap live mode**, but the observed live fail-close path was:

- `startup_selection_bootstrap_required` / `trusted bootstrap unavailable`

not a clearly observed:

- `post_bootstrap_rotation_blocked_cap_truncated`

No live log line for the new watchdog reason was observed during this restart window.

So the factual result is:

1. `6218368` is deployed and running on live.
2. Live is now fail-closed again with `followlist.active = 0`.
3. The server did **not** remain on a silent frozen top-15 set.
4. This rollout does **not** yet prove that the new Phase 1A watchdog reason was the effective trigger on this restart.
5. The immediate fail-close appears to have come from missing typed trusted-selection metadata plus bootstrap-unavailable handling.

## Runtime health after rollout

Immediate restart catch-up showed startup pressure:

- `yellowstone_output_queue_fill_ratio` briefly hit `1.0`
- `ws_notifications_replaced_oldest` increased during catch-up
- `ingestion_lag_ms_p95` briefly rose above `50s`

But data-plane stayed healthy:

- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`

Catch-up later drained:

- `yellowstone_output_queue_fill_ratio = 0.0`
- `yellowstone_output_queue_depth = 0`

App loop after fail-close:

- `app_follow_rejected_ratio = 1.0`

which is expected with `followlist.active = 0`.

## Practical conclusion

- As a safety rollout, this is acceptable:
  - live no longer runs on a frozen bootstrap top-15
  - runtime is healthy
  - selection is back in explicit `invalid + fail-close`
- As a pure proof of the new Phase 1A watchdog trigger, this rollout is incomplete:
  - live appears to have fallen through the older bootstrap-unavailable path because typed trusted-selection metadata was not present at startup

## Next check if needed

If a clean proof of the new watchdog path is needed, the next test must start from a live state where:

- typed trusted bridged snapshot metadata is present
- `trusted_selection_state = trusted_bridged`
- startup does not use `legacy_bool_fallback`

Only then can the first-next-bucket watchdog transition be validated directly on live.
