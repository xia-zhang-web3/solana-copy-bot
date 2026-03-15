# 2026-03-13 Afternoon SQLite Restart Hardening Rollout Report

## Scope

- Deployed `origin/main = 06ade443fe94f9149cc17aef5905f1f86adf46d9`
- Functional code delta relevant to runtime:
  - `dc107bb Harden sqlite restart recovery paths`
- Docs-only commit included in the fast-forward:
  - `06ade44 Document stale close rollout evidence`

## Pre-deploy baseline

Server state before rollout:

- previous live code: `d06da0a48c7c52ed469961d8dc5193dc1358f93e`
- `solana-copy-bot.service` active since `2026-03-13 09:01:11 UTC`
- services:
  - `solana-copy-bot.service = active`
  - `copybot-adapter.service = active`
  - `copybot-executor.service = active`
- filesystems:
  - `/dev/root = 145G total, 137G free`
  - `/dev/nvme1n1 = 492G total, 355G free`

Business/runtime baseline immediately before deploy:

- `followlist.active = 111`
- `copy_signals = 503312`
- `shadow_lots_open = 51`
- `shadow_closed_trades = 732`
- `orders = 0`
- `positions = 0`
- `fills = 0`
- realized shadow PnL:
  - `-8.199671 SOL`
- heads:
  - `observed_swaps_max_ts = 2026-03-13T12:29:10.961897882+00:00`
  - `discovery_runtime.cursor_ts = 2026-03-13T12:29:10.108299953+00:00`
  - direct gap `~= 0.85s`

## Rollout procedure

1. Fast-forwarded `/var/www/solana-copy-bot` from `d06da0a` to `06ade44`.
2. Rebuilt release binaries:
   - `copybot-app`
   - `copybot-adapter`
   - `copybot-executor`
3. Backed up server config:
   - `/etc/solana-copy-bot/live.server.toml.bak.20260313T123101Z`
4. Added server-local config parity for the newly introduced cooldown knob:
   - `risk.execution_buy_cooldown_seconds = 60`
   - note: `execution.enabled` remains `false`, so this knob is inert for current live runtime.
5. Performed a controlled stop of:
   - `solana-copy-bot.service`
   - `copybot-adapter.service`
   - `copybot-executor.service`
6. Ran static DB checkpoint before restart:

```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

Result:

- `0|0|0`
- checkpoint window:
  - start `2026-03-13T12:31:01Z`
  - done `2026-03-13T12:31:01Z`

7. Started services again in order:
   - `copybot-executor.service`
   - `copybot-adapter.service`
   - `solana-copy-bot.service`

## Immediate startup evidence

Fresh app process:

- `ActiveEnterTimestamp = 2026-03-13 12:31:01 UTC`
- `NRestarts = 0`

Startup logs:

- migrations clean:
  - `sqlite migrations applied: 0`
- startup WAL hygiene executed successfully:

```text
startup sqlite wal checkpoint completed
wal_checkpoint_mode=truncate
wal_checkpoint_busy=0
wal_log_frames=0
wal_checkpointed_frames=0
```

- startup heartbeat WAL telemetry:
  - `sqlite_wal_size_bytes = 16512`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`

## Discovery restart behavior after rollout

### First post-restart protected cycle

At `2026-03-13T12:31:47Z`:

- one transient warning was recorded:
  - `failed persisting discovery runtime cursor`
- the protected warm-restore branch executed exactly as intended:
  - `scoring_source = persisted_wallet_metrics_truncated_warm_restore`
  - `follow_promoted = 0`
  - `follow_demoted = 0`
  - `metrics_written = 0`
  - `metrics_persisted = false`
  - `snapshot_recomputed = false`
  - `followlist_activations_suppressed = true`
  - `followlist_deactivations_suppressed = true`
  - `swaps_warm_loaded = 100000`
  - `swaps_delta_fetched = 1`
  - `swaps_evicted_due_cap = 1`
  - `active_follow_wallets = 126`

Interpretation:

- the restart no longer mass-demoted the active followlist on a capped warm-restored tail;
- the first cycle bridged through persisted wallet metrics without writing a fresh partial `wallet_metrics` bucket.

### Subsequent cycles

By `2026-03-13T12:36:18Z` and later, discovery had returned to normal raw-window processing:

- `scoring_source = raw_window`
- `followlist_activations_suppressed = false`
- `followlist_deactivations_suppressed = true`
- `metrics_written = 0`
- `metrics_persisted = false`
- `follow_demoted = 0`
- `follow_promoted = 4` on the first raw cycle
- `active_follow_wallets = 130`

Later cycles at `12:37:01`, `12:38:01`, `12:39:01`, `12:40:01`, `12:41:01`, `12:42:01 UTC` remained stable:

- `scoring_source = raw_window`
- `follow_demoted = 0`
- `active_follow_wallets = 130`
- cycle durations `15-27 ms` for non-recompute ticks
- no repeat of the `1521 -> 5` style collapse

Interpretation:

- `truncated_warm_restore_bootstrap` behaved as a one-shot bridge, not a sticky mode;
- the second and later cycles resumed the intended capped-raw runtime semantics.

## WAL / contention outcome

WAL and contention after rollout:

- `12:35:41 UTC`:
  - `sqlite_busy_error_total = 4`
  - `sqlite_write_retry_total = 3`
  - `sqlite_wal_size_bytes = 416152`
- `12:36:31 UTC`:
  - `sqlite_busy_error_total = 5`
  - `sqlite_write_retry_total = 4`
  - `sqlite_wal_size_bytes = 62875352`
- from `12:36:31 UTC` through `12:42:31 UTC`:
  - contention counters remained flat at `5 / 4`
  - WAL telemetry stayed flat at `62875352 bytes`

Filesystem-level check at the end of soak:

- `/var/www/solana-copy-bot/state/live_copybot.db-wal ~= 60M`

Interpretation:

- the previous `11G WAL` restart failure mode did not recur;
- startup checkpoint plus post-restart runtime behavior kept WAL in a bounded range;
- no lock storm or runaway WAL growth was observed during the post-deploy soak window.

## Residual notes

1. The one transient `failed persisting discovery runtime cursor` occurred during the first protected cycle. It did not recur after the restart settled and did not prevent the protected cycle from completing.
2. Shadow runtime remains constrained by the existing soft-cap pause, unrelated to this deploy:
   - repeated `shadow risk timed pause activated`
   - reason `exposure_soft_cap: open_notional_sol=10.196962 >= soft_cap=10.000000`
3. `execution.enabled` remains `false`; this rollout did not switch live trading on.

## Verdict

This rollout is accepted as successful, with residual cursor-persist contention to monitor.

What was confirmed in live:

1. `dc107bb` startup WAL hardening executed on startup and prevented the previous restart-time WAL blow-up from recurring in this rollout window.
2. The warm-restore truncation fix behaved correctly:
   - first cycle protected through persisted bootstrap,
   - no false followlist demotion,
   - later cycles returned to `raw_window`.
3. Services remained healthy:
   - all three services `active`
   - `NRestarts = 0`
   - no repeat of the prior full-stall / frozen-head incident.
4. Residual watch item:
   - one transient `failed persisting discovery runtime cursor` occurred during the first protected cycle,
   - bounded contention counters remained non-zero (`sqlite_busy_error_total = 5`, `sqlite_write_retry_total = 4`),
   - this should be treated as monitor-worthy residual contention, not as a closed-forever condition.
