## 2026-03-09 late evening pinned-WAL recovery report

### Scope

Document the second March disk-pressure recovery where the immediate failure mode was not raw DB size alone, but a pinned SQLite WAL caused by stale diagnostic readers.

### Server / timing

- Host: `52.28.0.218`
- Recovery window: approximately `2026-03-09 21:17 UTC` to `2026-03-09 21:39 UTC`
- Service returned to `active`: `2026-03-09 21:39:30 UTC`

### Root cause

The acute disk event was caused by a pinned WAL, not only by a large main DB file.

Observed on the server before recovery:

- `/dev/root 145G`, `142G used`, only `3.1-3.8G free`
- `live_copybot.db ~98G`
- `live_copybot.db-wal ~36G`

Stale readers holding the WAL open:

- `PID 197958` / `PID 197959`
  - command: `sudo sqlite3 -header -column /var/www/solana-copy-bot/state/live_copybot.db`
  - start time observed in `ps`: `Mon Mar 9 20:15:03 2026`
- `PID 198074` / `PID 198075`
  - command: `sudo sqlite3 -header -column /var/www/solana-copy-bot/state/live_copybot.db SELECT wallet_id, MAX(qty_in) ...`
  - start time observed in `ps`: `Mon Mar 9 20:15:43 2026`

Both `sqlite3` children were stuck in `D` state and had live handles on:

- `live_copybot.db`
- `live_copybot.db-wal`
- `live_copybot.db-shm`

Operational meaning:

- the old diagnostic readers pinned the WAL,
- online `wal_checkpoint(TRUNCATE)` could not reclaim it while they existed,
- even after killing the stale readers, the live `copybot-app` process still kept the WAL active enough that a clean static checkpoint was needed.

### Recovery actions taken

1. Killed the two stale diagnostic `sqlite3` reader process pairs.
2. Confirmed only `copybot-app` still held the DB/WAL after those kills.
3. Reverted the temporary server-only debug knob:
   - `metric_snapshot_interval_seconds = 60 -> 1800`
4. Stopped only `solana-copy-bot.service`.
5. Ran `PRAGMA wal_checkpoint(TRUNCATE);` against the static DB.
6. Waited for checkpoint completion.
7. Restarted `solana-copy-bot.service`.
8. Verified early runtime health and checked that no stale diagnostic DB readers remained.

### Checkpoint duration / cost

- Approximate static checkpoint duration on the live host: about `21-22 minutes`
- WAL size at the time of checkpoint: about `36G`
- Main DB size at the time of checkpoint: about `98-99G`

This is an important operational fact on its own:

- with `state/` still on the root volume,
- a large pinned WAL can require a long maintenance window even when the recovery procedure is correct.

### Before / after

#### Disk / storage

- Before:
  - `WAL ~36G`
  - `/dev/root free ~3.1-3.8G`
- After:
  - `WAL ~52M`
  - `/dev/root free ~41G`
  - `live_copybot.db ~99G`

#### Runtime

- `solana-copy-bot.service active`
- `NRestarts=0`
- fresh process after restart reported:
  - `sqlite_busy_error_total=0`
  - `sqlite_write_retry_total=0`

#### Business / shadow path

Immediate post-recovery checkpoint:

- `followlist.active = 301`
- `copy_signals = 2954`
- `shadow_lots = 34`

Follow-up spot check after recovery remained live:

- `followlist.active = 15`
- `copy_signals = 2985`
- `shadow_lots = 34`

Interpretation:

- the emergency profile remained alive after recovery,
- shadow signals continued to flow,
- followlist was not dead after the maintenance window,
- the exact followlist count continued to move with fresh discovery cycles and should not be treated as a disk-recovery invariant.

### Residual risk

This incident is operationally recovered, but not systemically solved.

Residual risk remains high because:

- `state/` is still on the root volume,
- `live_copybot.db` alone is already about `99G` on a `145G` root filesystem,
- one missed checkpoint or another stale reader can still push the host back toward a low-free-space state,
- checkpoint recovery on this storage takes long enough to be operationally expensive.

### Operational conclusion

1. The immediate incident was successfully removed.
2. The acute failure mode is now clearly identified:
   - stale sqlite readers can pin the WAL and simulate a disk leak.
3. The architectural risk is still unchanged:
   - hot `state/` does not belong on the root filesystem long-term.
4. The next infra track remains:
   - move `state/` to a dedicated volume,
   - keep retention / archive discipline,
   - avoid ad hoc long-lived diagnostic sqlite readers on the live DB.
