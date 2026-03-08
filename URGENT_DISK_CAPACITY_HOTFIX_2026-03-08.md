# Urgent Disk Capacity Hotfix Plan

Date: `2026-03-08`
Status: `urgent operational hotfix / before-sleep runbook`

## Situation

1. The earlier `8b2bc43` contention hotfix is still valid.
2. The current live blocker is different:
   - root filesystem exhaustion
   - SQLite `xShmMap` / disk I/O failures
   - heartbeat write failures once `/` hit `100%`
3. Recovery already worked once without restart after freeing space outside the SQLite data path.
4. Current state is still fragile because free space is too small and recurrence risk is high.

## What This Is Not

1. This is **not** another `database is locked` incident.
2. This is **not** something that will be solved by waiting for current retention to save us tonight.
3. This is **not** a good reason to manually delete:
   - `live_copybot.db`
   - `live_copybot.db-wal`
   - `live_copybot.db-shm`

## Why This Will Repeat If We Do Nothing

1. Live config keeps:
   - `discovery.scoring_window_days = 30`
   - `discovery.observed_swaps_retention_days = 45`
2. Config validation also enforces:
   - `observed_swaps_retention_days >= scoring_window_days`
3. So the live DB is still acting as both:
   - hot operational runtime state
   - long-horizon raw event archive
4. That means simple log cleanup buys time, but does not remove the underlying growth path.

## Tonight Goal

Before sleep, get the server into a state where:

1. `/` has at least `10G` free.
2. Preferred target is `15G+` free.
3. `observed_swaps_max_ts` is moving again.
4. `discovery_cursor_ts` is moving again.
5. No fresh burst of:
   - `failed to record heartbeat`
   - `failed to insert observed swap batch`
   - `xShmMap`
   - `No space left on device`

If the system is healthy but free space is still around `1-2G`, do **not** call it fixed.

## Safe Tonight Actions

Do these in order.

All commands below should be run either:

1. as `root`, or
2. as an operator with `sudo`.

If you are already in a root shell, omit the `sudo` prefix.

### 1. Re-check current health first

```bash
df -h /
du -xhd1 /var/www/solana-copy-bot /home/copybot /home/ubuntu /var/log 2>/dev/null | sort -h
systemctl show solana-copy-bot --property=ActiveState --property=NRestarts
journalctl -u solana-copy-bot --since "30 minutes ago" | tail -n 200
```

### 2. Keep cleanup outside the SQLite data path

Safe actions already proven useful:

```bash
sudo journalctl --vacuum-size=50M
sudo apt-get clean
sudo rm -rf /var/www/solana-copy-bot/target/release/deps
sudo rm -rf /var/www/solana-copy-bot/target/release/build
sudo rm -rf /var/www/solana-copy-bot/target/release/.fingerprint
sudo rm -rf /var/www/solana-copy-bot/target/release/examples
sudo rm -rf /var/www/solana-copy-bot/target/release/incremental
sudo rm -rf /home/ubuntu/.cargo/registry/cache
sudo rm -rf /home/ubuntu/.cargo/registry/index
sudo rm -rf /home/ubuntu/.cargo/registry/src
```

Important:

1. Do **not** delete the deployed executable or unit-managed runtime binary.
2. Do **not** touch `state/live_copybot.db*` manually while the service is active.

### 3. If free space is still under `10G`, use these next

Only do these if you are **not** planning to compile on the server tonight:

```bash
sudo rm -rf /home/ubuntu/.rustup/toolchains
sudo rm -rf /home/copybot/.rustup/toolchains
```

Then re-check:

```bash
df -h /
du -xhd1 /var/www/solana-copy-bot /home/copybot /home/ubuntu /var/log 2>/dev/null | sort -h
```

### 4. If free space is still under `10G`, do not keep gambling

At that point, take one of these actions before sleep:

1. Move non-runtime directories off `/` if another volume is available:
   - build artifacts
   - report bundles
   - cache directories

If neither is available, the risk of a repeat incident overnight remains high.

### 5. Verify recovery with concrete DB checks

Use explicit DB checks instead of relying on log impressions:

```bash
sqlite3 -noheader /var/www/solana-copy-bot/state/live_copybot.db <<'SQL'
SELECT 'observed_swaps_max_ts|' || COALESCE(MAX(ts), 'n/a') FROM observed_swaps;
SELECT 'discovery_cursor_ts|' || COALESCE(
  (SELECT cursor_ts
   FROM discovery_runtime_state
   WHERE id = 1),
  'n/a'
);
SQL
```

Run it twice a few minutes apart. Recovery means both timestamps advance again.

## Do Not Do Tonight

1. Do not manually delete:
   - `live_copybot.db-wal`
   - `live_copybot.db-shm`
2. Do not run `VACUUM` on the live DB during active runtime.
3. Do not assume `cursor/head gap` is healthy unless `observed_swaps_max_ts` is also moving.
4. Do not mix this incident with the earlier contention rollback story.
5. Do not move `/var/www/solana-copy-bot/state` to another volume while the live service is active.

## Before-Sleep Checklist

Only go to sleep after checking all of these:

1. `df -h /` shows `>= 10G` free.
2. `systemctl show solana-copy-bot --property=ActiveState --property=NRestarts` still shows:
   - `ActiveState=active`
   - `NRestarts=0`
3. Latest logs show no new repeating:
   - `failed to record heartbeat`
   - `failed to insert observed swap batch`
   - `xShmMap`
   - `No space left on device`
4. `observed_swaps_max_ts` advanced after cleanup.
5. `discovery_cursor_ts` advanced after cleanup.
6. The timestamp check above was run explicitly with `sqlite3`, not inferred indirectly from one log line.

## Tomorrow: Durable Ops Fix

This is the minimum durable ops plan.

1. Move live SQLite state off root filesystem.
2. Keep a headroom floor:
   - hard minimum `10G`
   - target `15-20G`
3. Add alerts for:
   - root filesystem usage
   - `live_copybot.db` size
   - `live_copybot.db-wal` size
   - heartbeat failure rate
   - observed-swap writer failure rate
4. Stop using the production server as a build/cache host.
5. Add a documented cleanup runbook so incident response is not ad hoc.
6. If state must be moved to a larger volume, treat it as a maintenance action:
   - stop service
   - verify shutdown
   - move/mount/bind-mount `state/`
   - verify permissions and paths
   - start service
   - confirm DB timestamps move again

## This Week: Real Engineering Exit

If we do not want to keep buying disk, we need to stop using live SQLite as a month-long raw archive.

Required direction:

1. Decouple discovery/followlist scoring from long raw `observed_swaps` retention.
2. Make discovery rely on persisted aggregates instead of 30-45 days of raw swaps.
3. Remove the operational dependency:
   - `observed_swaps_retention_days >= scoring_window_days`
4. Split hot vs cold storage:
   - hot SQLite for runtime windows
   - archived older raw swaps outside live DB
5. Add a compaction strategy after retention actually starts deleting old rows.

## Bottom Line

Tonight's fix is:

1. free real disk space outside the SQLite data path,
2. get to `>= 10G` free,
3. confirm writes are moving again,
4. do not restart unless recovery fails.

The long-term fix is:

1. move live state off root,
2. stop keeping month-scale raw event history in the live runtime DB,
3. redesign storage lifecycle so retention and scoring are no longer tied together.
