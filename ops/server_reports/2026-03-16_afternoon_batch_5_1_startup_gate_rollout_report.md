# 2026-03-16 afternoon — Batch 5.1 startup gate rollout report

## Scope

- Deploy `571289a Harden startup gate against stale trusted snapshots`
- Perform one controlled restart of `solana-copy-bot.service`
- Verify startup gate behavior only

## Deploy

- Host: `ubuntu@52.28.0.218`
- Repo: `/var/www/solana-copy-bot`
- Deployed commit: `571289a`
- Built target: `copybot-app`
- Restarted unit: `solana-copy-bot.service`

Service after restart:

- `MainPID=317246`
- `NRestarts=0`
- `ActiveEnterTimestamp=2026-03-16 12:13:07 UTC`

## Critical live DB fact

On live, the trusted snapshot metadata table is empty:

- `SELECT COUNT(*) FROM trusted_wallet_metrics_snapshots;` -> `0`

This is why startup cannot populate active trusted snapshot metadata fields from persisted snapshot lineage.

## What changed with Batch 5.1

Startup no longer falls back through the legacy bool path.

Observed startup log:

- `trusted_selection_legacy_bool_fallback_used = false`
- `trusted_selection_state = Some(Invalid)`
- `trusted_selection_bootstrap_required = true`
- `trusted_selection_reason = Some("trusted_selection_bootstrap_unavailable")`

So:

- startup is now on the typed path
- live stays in explicit `invalid + fail-close`
- silent frozen top-15 does not return

## What did not happen

The following fields were still empty at startup:

- `trusted_selection_active_snapshot_id = None`
- `trusted_selection_active_snapshot_window_start = None`
- `trusted_selection_last_bootstrap_source_kind = None`

This is not a regression in `571289a`.

It is a live-data fact:

- there is no row in `trusted_wallet_metrics_snapshots` to hydrate those fields from

## Post-restart selection state

From `discovery_strategy_state`:

- `trusted_selection_bootstrap_required = 1`
- `trusted_selection_reason = trusted_selection_bootstrap_unavailable`
- `trusted_selection_state = invalid`
- `followlist.active = 0`

Observed log:

- `discovery trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`

## Runtime health

After restart:

- `observed_swap_writer_pending_requests = 0`
- `sqlite_busy_error_total = 0`
- `sqlite_write_retry_total = 0`
- `yellowstone_output_queue_fill_ratio = 0.0`
- `app_follow_rejected_ratio = 1.0`

Runtime stayed healthy while selection remained fail-closed.

## Verdict

Batch 5.1 **partially achieved** the requested startup-gate objective:

### Confirmed

- `trusted_selection_legacy_bool_fallback_used = false`
- startup now goes through the typed path
- live honestly remains in `invalid + fail-close`
- silent frozen top-15 did not return

### Not confirmed

- `trusted_selection_state / active_snapshot_id / active_snapshot_window_start / source_kind` were **not** all populated

Reason:

- live has **no persisted trusted snapshot metadata rows** to populate them from

## Scope closure decision

Bootstrap/control-plane scope is **not cleanly closeable yet** if the closure criterion requires populated typed snapshot metadata on live startup.

What Batch 5.1 proved:

- the startup gate logic is hardened enough to avoid legacy bool fallback

What still blocks clean closure:

- live metadata lineage for trusted snapshots is absent

That is now the remaining concrete issue.
