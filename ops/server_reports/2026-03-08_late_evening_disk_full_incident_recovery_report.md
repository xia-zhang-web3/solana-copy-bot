# Late-Evening Disk-Full Incident Recovery Report

Date (UTC incident read): `2026-03-08T21:00:30Z`  
Server: `52.28.0.218`  
Live commit: `8b2bc43`  
Service start: `2026-03-08 09:18:57 UTC`

## Summary

1. This was **not** a return of the earlier SQLite lock/contention rollback regression.
2. This was a new `disk full / xShmMap I/O` incident:
   - root filesystem hit `100%`
   - `systemd-journald` started emitting `No space left on device`
   - observed-swap writer began failing commits with `disk I/O error ... xShmMap method`
   - heartbeat writes also started failing
3. The service stayed `active`, but runtime was degraded until disk space was freed.
4. Recovery succeeded **without restart** after conservative cleanup outside the SQLite data path.

## Forensic snapshot

1. Root filesystem:
   - before cleanup: `/dev/root 96G used / 0 avail / 100%`
   - inodes were not the problem: `IUse% = 2%`
2. Space consumers:
   - `/var/www/solana-copy-bot/state ~90G`
   - `/var/www/solana-copy-bot/target ~922M`
   - `/home/copybot ~1.4G` (`.rustup/toolchains ~1.3G`)
   - `/home/ubuntu ~911M` (`.rustup/toolchains ~573M`, `.cargo/registry ~319M`)
3. SQLite files at incident time:
   - `live_copybot.db ~86.6G`
   - `live_copybot.db-wal ~9.18G`
   - `live_copybot.db-shm ~192K`
4. Kernel/system evidence:
   - `systemd-journald: Failed to open system journal: No space left on device`

## Runtime symptoms

1. Heartbeat failures began at `2026-03-08T20:54:27Z`.
   - observed count before cleanup check: `14`
2. Observed-swap writer failures began around `2026-03-08T21:01:22Z`.
   - error:
     - `failed to insert observed swap batch with activity days`
     - `failed to commit observed swap batch write transaction`
     - `disk I/O error: Error code 4874: I/O error within the xShmMap method`
3. At the degraded read:
   - `sqlite_busy_error_total=2`
   - `sqlite_write_retry_total=2`
   - `database is locked = 0`
   - `discovery cycle still running = 0`
   - interpretation: this was disk exhaustion, not a lock-escalation replay
4. Latest live-state read before cleanup:
   - `observed_swaps_max_ts = 2026-03-08T18:51:25.162017399+00:00`
   - `discovery_cursor_ts = 2026-03-08T18:50:55.625889766+00:00`
   - the apparent `29s` DB gap was misleading because both DB head and cursor had already stalled roughly `~2h 09m` behind wall-clock

## Cleanup actions

Only space outside the SQLite data path was touched.

1. `journalctl --vacuum-size=50M`
   - freed `~140.7M` combined from `/run/log/journal` and `/var/log/journal`
2. `apt-get clean` and archive cleanup
3. Removed build artifacts:
   - `/var/www/solana-copy-bot/target/release/deps`
   - `/var/www/solana-copy-bot/target/release/build`
   - `/var/www/solana-copy-bot/target/release/.fingerprint`
   - `/var/www/solana-copy-bot/target/release/examples`
   - `/var/www/solana-copy-bot/target/release/incremental`
4. Removed Ubuntu cargo registry caches:
   - `/home/ubuntu/.cargo/registry/cache`
   - `/home/ubuntu/.cargo/registry/index`
   - `/home/ubuntu/.cargo/registry/src`
5. Removed additional non-DB cache/storage layers after initial recovery:
   - `/home/ubuntu/.rustup/toolchains`
   - `/home/copybot/.rustup/toolchains`
   - `/var/lib/apt/lists/*`
   - disabled snap revisions and snap cache leftovers
6. Explicitly **not** done:
   - no manual deletion of `live_copybot.db-wal`
   - no manual deletion of `live_copybot.db-shm`
   - no restart
   - no config changes

## Post-cleanup state

1. Filesystem:
   - after conservative non-DB cleanup: `/dev/root 96G / 1.4G avail / 99%`
2. Immediate post-cleanup error window (`since 2026-03-08 21:05:00 UTC`):
   - `heartbeat_failures=2`
   - `swap_batch_failures=15239`
   - these failures continued briefly during the recovery tail, then stopped
3. Runtime recovered on its own:
   - `observed_swaps_max_ts = 2026-03-08T21:07:08.988683135+00:00`
   - `discovery_cursor_ts = 2026-03-08T21:06:55.813419790+00:00`
   - `discovery cycle completed` again at `2026-03-08T21:07:02Z`
   - recompute cycle:
     - `discovery_cycle_duration_ms=5565`
     - `metrics_persisted=true`
     - `metrics_written=19804`
   - ingestion queue recovered:
     - transient bad sample: `ws_to_fetch_queue_depth=934`, `lag p95=5434`
     - next sample: `ws_to_fetch_queue_depth=1`, `lag p95=3699`
4. Service state remained:
   - `active`
   - `NRestarts=0`
5. Additional space recovery after runtime had stabilized:
   - `PRAGMA wal_checkpoint(PASSIVE)` succeeded but did not shrink the WAL file
   - `PRAGMA wal_checkpoint(TRUNCATE)` then succeeded with:
     - before: `live_copybot.db-wal ~8.6G`, `/ ~3.1G avail / 97%`
     - after: `live_copybot.db-wal ~178K` immediately, then `~57M` under resumed writes
     - filesystem after truncate: `/dev/root 96G / 12G avail / 88%`
6. Post-checkpoint sanity:
   - no new `failed to insert observed swap batch with activity days` after `21:30 UTC`
   - no new `failed to record heartbeat` after `21:30 UTC`
   - heads still advancing:
     - `observed_swaps_max_ts = 2026-03-08T21:30:58.021916155+00:00`
     - `discovery_cursor_ts = 2026-03-08T21:30:55.801050268+00:00`
   - service remained `active`, `NRestarts=0`

## Operational conclusion

1. The hotfix rollout `8b2bc43` itself did not fail by reverting to the old contention mode.
2. The late-evening incident was caused by **root filesystem exhaustion**, which then surfaced as:
   - heartbeat write failures
   - observed-swap writer commit failures
   - journald write failures
3. Conservative cleanup outside the SQLite data path was enough to restore runtime without restart.
4. A follow-up `wal_checkpoint(TRUNCATE)` was safe and materially reduced the immediate recurrence risk for tonight.
5. The server is materially safer now, but the durable remediation is still storage hygiene / capacity work:
   - maintain more free root space
   - stop treating hot SQLite as an archive
   - move the live `state/` directory off root filesystem in a controlled maintenance window
