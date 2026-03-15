## 2026-03-13 Late Afternoon Shadow Soft-Cap Stall Report

### Scope

This report captures the live server state at approximately `2026-03-13 16:34 Europe/Kiev` after the stale-close rollout and the subsequent SQLite restart-hardening rollout.

The purpose of this report is narrow:

- explain why trading/shadow activity is effectively stalled even though runtime is healthy;
- separate the real post-fix shadow PnL from the earlier forced stale-close accounting shock;
- give auditors a concrete, reproducible evidence bundle;
- give operators a fast command pack to re-check the same state in seconds.

### Executive Summary

Runtime is currently stable, but shadow trading is effectively throttled by a repeatedly re-armed timed pause on `shadow_soft_exposure_cap_sol`.

Key facts:

- infra/runtime is healthy:
  - all three services are `active`;
  - `NRestarts=0`;
  - discovery remains near-head;
  - WAL is bounded, not runaway.
- business/shadow path is not healthy:
  - `shadow_lots_open = 51`;
  - `open_shadow_cost_sol = 10.196961527`;
  - server-local `shadow_soft_exposure_cap_sol = 10.0`;
  - the app keeps emitting `shadow_risk_pause` with reason `exposure_soft_cap`.

This is not a hard-stop and not an infra failure. It is an operational/business-path stall caused by risk policy interacting with the current open-lot inventory.

### Current Runtime State

Service state:

- `solana-copy-bot.service`: `active`, `NRestarts=0`
- `copybot-adapter.service`: `active`, `NRestarts=0`
- `copybot-executor.service`: `active`, `NRestarts=0`
- current `solana-copy-bot.service` uptime window began at `2026-03-13 12:31:01 UTC`

Storage state:

- root FS: `145G total`, `137G free`
- state volume: `492G total`, `354G free`
- `live_copybot.db`: `113G`
- `live_copybot.db-wal`: `~209M`

SQLite/runtime health:

- latest heartbeat counters observed:
  - `sqlite_busy_error_total = 17`
  - `sqlite_write_retry_total = 16`
  - `sqlite_wal_size_bytes = 218248792`
- no fresh `database is locked` incidents in the checked window
- no fresh `failed persisting discovery runtime cursor` in the checked window

Heads:

- `observed_swaps_max_ts = 2026-03-13T14:34:28.257744449+00:00`
- `cursor_ts = 2026-03-13T14:33:59.901472593+00:00`
- discovery lag at sample time: about `28s`

### Current Shadow / Business State

Counts:

- `followlist_active = 173`
- `copy_signals = 506313`
- `shadow_lots_open = 51`
- `shadow_closed_trades = 732`
- `orders = 0`
- `positions = 0`
- `fills = 0`

PnL:

- total realized shadow PnL: `-8.199671300 SOL`
- realized shadow PnL over last 24h: `-10.389720626 SOL`

Important interpretation note:

- the total realized shadow PnL is contaminated by the earlier forced stale-close cleanup batch;
- for strategy evaluation, the more relevant short-window PnL is the post-fix/post-recovery delta, not the lifetime total.

### Clean Post-Fix PnL Windows

#### Since hard-stop recovery restart

Anchor:

- `2026-03-13 08:05:03 UTC`

Results since that point:

- `2` closed shadow trades
- realized PnL: `-0.003038473 SOL`
- wins/losses: `0 / 2`

Both trades closed at:

- `2026-03-13T08:07:59.521024215+00:00`

#### Since SQLite restart-hardening rollout

Anchor:

- `2026-03-13 12:31:01 UTC`

Results since that point:

- `0` closed shadow trades
- realized PnL: `0 SOL`

This means the current blocker is not “strategy losing badly right now”. The immediate issue is that the shadow path is barely progressing because the same open lots keep holding exposure above the soft cap.

### The Actual Blocker

The server is repeatedly creating `shadow_risk_pause` events with:

- `pause_type = exposure_soft_cap`
- `detail = open_notional_sol=10.196962 >= soft_cap=10.000000`

Recent rows from `risk_events`:

```text
2026-03-13T14:40:53.814071856+00:00 | shadow_risk_pause | warn | {"pause_type":"exposure_soft_cap","detail":"open_notional_sol=10.196962 >= soft_cap=10.000000","until":"2026-03-13T14:55:53.814071856+00:00"}
2026-03-13T14:40:48.801526170+00:00 | shadow_risk_pause | warn | {"pause_type":"exposure_soft_cap","detail":"open_notional_sol=10.196962 >= soft_cap=10.000000","until":"2026-03-13T14:55:48.801526170+00:00"}
2026-03-13T14:40:41.548311111+00:00 | shadow_risk_pause | warn | {"pause_type":"exposure_soft_cap","detail":"open_notional_sol=10.196962 >= soft_cap=10.000000","until":"2026-03-13T14:55:41.548311111+00:00"}
```

This is re-arming every few seconds.

Current relevant server-local config:

- `max_hold_hours = 6`
- `shadow_stale_close_terminal_zero_price_hours = 12`
- `shadow_soft_exposure_cap_sol = 10.0`
- `shadow_hard_exposure_cap_sol = 12.0`

### Open-Lot Inventory That Is Pinning the Pause

Current open-lot state:

- `open_lots_total = 51`
- `open_shadow_cost_sol = 10.196961527`
- `open_shadow_wallets = 3`
- `open_shadow_tokens = 2`

Age:

- `open_lots_older_than_6h = 51`
- `open_lots_older_than_12h = 0`
- `oldest_open_age_hours = 6.587`
- `newest_open_age_hours = 6.560`

Concentration:

- token `7AvcBXqw7tXNaL43r37QPdZLHkoeyKmXikg82VpYoj9k`
  - open cost: `9.996961527 SOL`
  - open lots: `50`
- token `85j6jmkHuATk31buFqstfVC6VtESX8c6bDHaTaihuoER`
  - open cost: `0.200000000 SOL`
  - open lots: `1`

Representative open lots:

```text
7AvcBXqw7tXNaL43r37QPdZLHkoeyKmXikg82VpYoj9k | BqRp9bqojQPzYiugKdmVaonFTAqCumoaW8xfEtrqShJB | 0.200000000 | 2026-03-13T08:06:17.689312579+00:00
7AvcBXqw7tXNaL43r37QPdZLHkoeyKmXikg82VpYoj9k | zwDfKaVLcBpV1fWJqpnywfR7E74j2LXYTVh6kv8Ker8 | 0.200000000 | 2026-03-13T08:06:19.227536504+00:00
85j6jmkHuATk31buFqstfVC6VtESX8c6bDHaTaihuoER | 6gA7ZUaSWcUaFVvWdRQeWTXj5yv6H654yP4WgyEWkhHZ | 0.200000000 | 2026-03-13T08:06:31.943920467+00:00
```

### Why Trading Is Effectively Stalled

The stall is the combined effect of three facts:

1. All currently open lots are older than `max_hold_hours = 6`, but not yet older than `terminal_zero_price_hours = 12`.
2. The open inventory is already above the soft cap:
   - `10.196961527 SOL > 10.0 SOL`
3. The app keeps receiving and recording many shadow signals, but the recent signal stream is mostly `sell` signals that do not unwind the currently open inventory.

Recent logs show repeated examples like:

```text
shadow signal recorded | side="sell" | closed_qty=0.0 | realized_pnl_sol=0.0
```

for tokens such as:

- `2tJepVeaDJ9AhYXPskyeRfVpckbduWznGK6NpMjiu7vM`
- `6G3DermZAXerbepbjrcWKoxkbhuFPsNsWUGoy7tNdYVg`
- `AD6QUYCzmFDM831gbYRvsuzWXBUSGaVw8LRNHUs3Mx5v`
- `5r4Am1gW3woM66bHx1FPBjTLi8a777us912QhJNaUeyE`

Those are not the same tokens that currently dominate open exposure (`7Avc...`, `85j6...`), so these sell signals are not reducing the exposure that keeps the pause active.

### What This Means

This is not “the bot is dead”.

It is:

- runtime healthy;
- discovery healthy;
- followlist healthy enough to produce signals;
- shadow pipeline still receiving signals;
- but new entries are being throttled by a timed risk pause because open exposure is stuck just above the soft cap.

This is also not the same failure mode as the earlier stale-close deadlock:

- stale-close cleanup already ran and closed the old permanently stuck backlog;
- the current open lots are new lots from the post-recovery period;
- they have crossed `6h` but have not yet crossed the `12h` terminal-zero threshold.

### Expected Near-Term Behavior Without Intervention

If nothing changes:

- timed pause will likely continue re-arming while `open_notional_sol` remains above `10.0`;
- shadow trading will remain effectively constrained on new entries;
- the next important time boundary is when these current lots cross `12h` age, because only then can the terminal-zero stale-close path apply if no reliable price is available.

Given the observed open timestamps around `2026-03-13T08:06Z`, that threshold is around:

- `2026-03-13 ~20:06 UTC`

Until then, this can stay in the current “runtime alive, shadow constrained” mode.

### Auditor Questions / Work Items

The right audit target is not infra. It is the interaction between stale-close policy, open inventory concentration, and risk-pause semantics.

Questions to answer:

1. Is the current `shadow_soft_exposure_cap_sol = 10.0` simply too tight relative to the strategy’s open-lot batch size and recent followlist scale?
2. Should timed-pause re-arm on every qualifying loop, or should it be latched less aggressively once already active?
3. Should stale-close for `6h < age < 12h` do something stronger than pure skip when the position is the sole reason the soft cap is pinned?
4. Are sell signals for the currently open tokens missing, or are they arriving but failing lot matching/accounting?
5. Is the intended business policy actually:
   - “no new entries until current exposure exits naturally”, or
   - “soft cap should slow entries, not effectively freeze them”?

### Fast Command Pack For Future Checks

These commands are enough to diagnose this class of issue in seconds.

#### 1. Runtime / heads / counts / PnL

```bash
ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  "sudo sqlite3 -cmd '.timeout 5000' /var/www/solana-copy-bot/state/live_copybot.db \
  \"SELECT 'observed_swaps_max_ts', COALESCE(MAX(ts), '') FROM observed_swaps
   UNION ALL
   SELECT 'cursor_ts', COALESCE(cursor_ts, '') FROM discovery_runtime_state WHERE id = 1
   UNION ALL
   SELECT 'followlist_active', COUNT(*) FROM followlist WHERE active = 1
   UNION ALL
   SELECT 'copy_signals', COUNT(*) FROM copy_signals
   UNION ALL
   SELECT 'shadow_lots_open', COUNT(*) FROM shadow_lots
   UNION ALL
   SELECT 'shadow_closed_trades', COUNT(*) FROM shadow_closed_trades
   UNION ALL
   SELECT 'realized_shadow_pnl_sol_total', COALESCE(printf('%.9f', SUM(pnl_sol)), '0.000000000') FROM shadow_closed_trades
   UNION ALL
   SELECT 'open_shadow_cost_sol', COALESCE(printf('%.9f', SUM(cost_sol)), '0.000000000') FROM shadow_lots;\""
```

#### 2. Open-lot age / cap pressure

```bash
ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  "sudo sqlite3 -cmd '.timeout 5000' /var/www/solana-copy-bot/state/live_copybot.db \
  \"SELECT 'open_lots_total', COUNT(*) FROM shadow_lots
   UNION ALL
   SELECT 'open_lots_older_than_6h', COUNT(*) FROM shadow_lots WHERE (julianday('now') - julianday(opened_ts))*24.0 >= 6.0
   UNION ALL
   SELECT 'open_lots_older_than_12h', COUNT(*) FROM shadow_lots WHERE (julianday('now') - julianday(opened_ts))*24.0 >= 12.0
   UNION ALL
   SELECT 'oldest_open_age_hours', COALESCE(printf('%.3f', MAX((julianday('now') - julianday(opened_ts))*24.0)), '0.000') FROM shadow_lots
   UNION ALL
   SELECT 'open_shadow_cost_sol', COALESCE(printf('%.9f', SUM(cost_sol)), '0.000000000') FROM shadow_lots;\""
```

#### 3. Current risk-pause evidence

```bash
ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  "sudo sqlite3 -cmd '.timeout 5000' /var/www/solana-copy-bot/state/live_copybot.db \
  \"SELECT ts, type, severity, details_json
     FROM risk_events
    WHERE type IN ('shadow_risk_pause','shadow_risk_pause_cleared','shadow_risk_hard_stop')
    ORDER BY ts DESC
    LIMIT 15;\""
```

#### 4. Live app log slice

```bash
ssh -i /Users/tigranambarcumyan/Documents/keys/solana-copy-bot.pem ubuntu@52.28.0.218 \
  "journalctl -u solana-copy-bot.service -n 200 --no-pager | \
   egrep 'shadow risk timed pause activated|shadow signal recorded|shadow snapshot|discovery cycle completed|sqlite contention counters|shadow_stale_close_price_unavailable|shadow_stale_close_terminal_zero_price'"
```

### Bottom Line

At `2026-03-13 16:34 Europe/Kiev`, the project is not suffering from an infra outage. It is suffering from a business/risk-path stall:

- runtime healthy;
- discovery healthy;
- followlist alive;
- signals flowing;
- but `51` open shadow lots totaling `10.196961527 SOL` keep the bot above `shadow_soft_exposure_cap_sol = 10.0`,
  so timed pause re-arms continuously and effectively blocks new entries.

That is the issue to audit and fix next.
