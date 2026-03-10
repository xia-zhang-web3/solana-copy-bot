# 2026-03-10 Morning Post-EBS State Migration Runtime Report

## Scope

- Host: `52.28.0.218`
- Instance: `i-06b3f1f76ac5855de`
- Region / AZ: `eu-central-1` / `eu-central-1b`
- New EBS state volume: `gp3`, `500 GiB`, attached as Linux device `/dev/nvme1n1`
- Migration window completed on `2026-03-10`

## Migration Result

`/var/www/solana-copy-bot/state` is now mounted from the new dedicated EBS volume.

Verification:

```text
/dev/nvme1n1 ext4 491.1G 105.9G 360.1G /var/www/solana-copy-bot/state
```

Ownership / mode:

```text
copybot:copybot 750 /var/www/solana-copy-bot/state
```

The previous on-root state directory was intentionally retained as a safety backup and was **not** deleted:

```text
/var/www/solana-copy-bot/state.pre_ebs.20260310T073735Z
```

Size:

```text
106G
```

## Disk State After Migration

Root filesystem:

```text
/dev/root 145G total, 112G used, 33G avail, 78%
```

New state volume:

```text
/dev/nvme1n1 492G total, 107G used, 361G avail, 23%
```

DB files on the new state volume:

```text
live_copybot.db      ~106G
live_copybot.db-wal  ~149M
live_copybot.db-shm  ~320K
```

## Runtime Health

Services after migration:

```text
solana-copy-bot.service   active
copybot-adapter.service   active
copybot-executor.service  active
```

`solana-copy-bot.service`:

```text
ActiveEnterTimestamp = Tue 2026-03-10 07:52:06 UTC
ExecMainPID          = 208173
NRestarts            = 0
```

Live heads were moving after restart:

```text
observed_swaps_max_ts      = 2026-03-10T07:55:17.543212246+00:00
discovery_runtime.cursor   = 2026-03-10T07:55:03.044826516+00:00
direct cursor/head gap     ~= 14.5s
```

Recent logs immediately after migration showed active shadow processing again:

- `shadow followed wallet swap reached pipeline`
- `shadow signal recorded`

## Business Checkpoint

Post-migration spot check:

```text
followlist.active       = 304
copy_signals            = 32950
shadow_lots             = 63
shadow_closed_trades    = 667
shadow realized pnl     = +2.190049326 SOL
```

Real execution remains intentionally off:

```text
orders    = 0
positions = 0
fills     = 0
```

## Notes

- The migration solved the main infra problem: `state/` is no longer sharing the root filesystem failure domain.
- Root still has only `~33G` free because the old state tree was preserved as a rollback/safety backup.
- That backup should be deleted only after:
  1. a short soak period on the new EBS volume,
  2. an AWS snapshot of the new EBS volume,
  3. one more clean runtime check.
- A post-restart shadow risk pause also appeared:
  - `shadow risk timed pause activated`
  - reason: `exposure_soft_cap: open_notional_sol=10.386682 >= soft_cap=10.000000`
  - this is a shadow risk-gate issue, not a storage migration issue.

## Verdict

The EBS state migration succeeded.

- storage isolation is now materially improved,
- services recovered cleanly,
- live runtime resumed with near-head cursor movement,
- shadow path remained alive after the cutover.

## Post-Soak Checkpoint

Additional soak validation taken around `2026-03-10 12:05 Europe/Kiev` / `10:05 UTC`:

```text
/dev/root       145G total, 112G used, 33G avail, 78%
/dev/nvme1n1    492G total, 108G used, 359G avail, 24%
```

State volume remained mounted correctly:

```text
/dev/nvme1n1 ext4 491.1G 107.4G 358.7G /var/www/solana-copy-bot/state
```

DB files remained healthy on the new volume:

```text
live_copybot.db      ~108G
live_copybot.db-wal  ~149M
live_copybot.db-shm  ~320K
```

Runtime remained clean after soak:

```text
solana-copy-bot.service   active
copybot-adapter.service   active
copybot-executor.service  active
NRestarts                 = 0
```

Live heads were still moving near-head:

```text
observed_swaps_max_ts      = 2026-03-10T10:06:21.633082418+00:00
discovery_runtime.cursor   = 2026-03-10T10:06:04.763182497+00:00
direct cursor/head gap     ~= 16.9s
```

Business activity also remained live:

```text
followlist.active       = 356
copy_signals            = 42036
shadow_lots             = 63
shadow_closed_trades    = 667
```

Recent logs continued to show normal shadow processing:

- `shadow followed wallet swap reached pipeline`
- `shadow signal recorded`

Operational conclusion after soak:

1. The migration remains stable after the initial soak window.
2. The new EBS volume is ready for an AWS snapshot.
3. The old backup directory should still be retained until that AWS snapshot completes and one more quick runtime check is clean.

## Post-Snapshot Checkpoint

AWS snapshot of the new state volume was created successfully:

```text
snapshot_id = snap-0f41f0a025e9d31f5
status      = completed
```

Post-snapshot runtime check around `2026-03-10 15:43 Europe/Kiev` / `13:43 UTC` remained clean:

```text
/dev/root       145G total, 112G used, 33G avail, 78%
/dev/nvme1n1    492G total, 110G used, 357G avail, 24%
```

Mount and DB files remained healthy:

```text
/dev/nvme1n1 ext4 491.1G 109.5G 357.0G /var/www/solana-copy-bot/state
live_copybot.db      ~110G
live_copybot.db-wal  ~149M
live_copybot.db-shm  ~320K
```

Runtime stayed healthy:

```text
solana-copy-bot.service   active
copybot-adapter.service   active
copybot-executor.service  active
NRestarts                 = 0
```

Live heads continued moving near-head:

```text
discovery_runtime.cursor   = 2026-03-10T13:43:04.770272796+00:00
observed_swaps_max_ts      = 2026-03-10T13:43:32.869169321+00:00
direct cursor/head gap     ~= 28.1s
```

Business activity remained alive:

```text
followlist.active       = 439
copy_signals            = 51227
shadow_lots             = 63
shadow_closed_trades    = 667
```

Recent logs still showed ongoing shadow processing:

- `shadow followed wallet swap reached pipeline`
- `shadow signal recorded`

Operational conclusion after snapshot:

1. The dedicated EBS migration is now fully protected by a completed AWS snapshot.
2. Runtime remained healthy after snapshot completion.
3. The retained rollback backup directory `/var/www/solana-copy-bot/state.pre_ebs.20260310T073735Z` is now safe to delete once the operator is ready to reclaim root space.

## Post-Cleanup Checkpoint

The retained rollback backup was deleted after snapshot completion:

```text
removed = /var/www/solana-copy-bot/state.pre_ebs.20260310T073735Z
reclaimed_root_space ~= 106G
```

Disk state after cleanup:

```text
/dev/root       145G total, 5.6G used, 139G avail, 4%
/dev/nvme1n1    492G total, 110G used, 357G avail, 24%
```

Runtime remained healthy after cleanup:

```text
solana-copy-bot.service   active
copybot-adapter.service   active
copybot-executor.service  active
NRestarts                 = 0
```

Live heads were still moving near-head:

```text
discovery_runtime.cursor   = 2026-03-10T13:50:04.755001069+00:00
observed_swaps_max_ts      = 2026-03-10T13:50:16.011140292+00:00
direct cursor/head gap     ~= 11.3s
```

Business activity stayed live:

```text
followlist.active       = 439
copy_signals            = 51938
shadow_lots             = 63
shadow_closed_trades    = 667
```

Recent logs still showed normal shadow processing immediately after cleanup:

- `shadow followed wallet swap reached pipeline`
- `shadow signal recorded`

## Final Operational Verdict

The dedicated EBS migration is operationally complete.

1. `state/` has been moved off root onto the dedicated EBS volume.
2. The new volume has passed cutover, soak, AWS snapshot, and post-snapshot runtime verification.
3. The old root backup has been safely removed.
4. Root filesystem headroom is now restored to a healthy level.
