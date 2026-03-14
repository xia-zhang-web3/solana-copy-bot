## 2026-03-14 morning shadow policy analysis

### Scope

This note analyzes shadow trade lifecycle after the forced cleanup executed on `2026-03-13 20:48 Europe/Kiev` and recommends the next policy change for stale-close behavior.

This analysis deliberately separates:

1. normal `market` closes after cleanup,
2. automatic zero-price stale closes after cleanup,
3. the one-off manual recovery cleanup that happened at `2026-03-13 20:48 Europe/Kiev`.

The manual cleanup is **not** included in the recommended strategy PnL baseline.

### Current live policy

Tracked baseline:

- [configs/live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L165): `max_hold_hours = 6`
- [configs/live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L166): `shadow_stale_close_terminal_zero_price_hours = 12`
- [configs/live.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/live.toml#L167): `shadow_stale_close_recovery_zero_price_enabled = false`

Equivalent tracked values also exist in:

- [configs/prod.toml](/Users/tigranambarcumyan/Documents/solana-copy-bot/configs/prod.toml#L148)
- [ops/server_templates/live.server.toml.example](/Users/tigranambarcumyan/Documents/solana-copy-bot/ops/server_templates/live.server.toml.example#L165)

### Evidence window

Sample window analyzed here:

- start: `2026-03-13 20:48 Europe/Kiev`
- end: `2026-03-14 09:31 Europe/Kiev`
- duration: about `12h 43m`

### Observed results in that window

Normal market path:

- `139` `market` closes
- realized PnL: `+2.127307293 SOL`
- wins / losses: `95 / 44`
- all `market` closes completed in `< 1h`

Hold-time breakdown for `market` closes:

- `< 1h`: `139` closes, `+2.127307293 SOL`
- `1h+`: `0` closes

Automatic stale-close path:

- `52` `stale_terminal_zero_price` closes
- realized PnL: `-9.533076415 SOL`
- average hold: `12.060186h`
- min hold: `12.004409h`
- max hold: `12.078399h`

Manual recovery cleanup at the start of the window:

- `51` `recovery_terminal_zero_price` closes
- realized PnL: `-10.196961527 SOL`
- average hold: `10.695321h`

### Concentration

The negative stale tail is concentrated, not broad-based.

Largest `stale_terminal_zero_price` contributors after cleanup:

- token `6ECa...`: `25` closes, `-4.298673624 SOL`
- token `BznS...`: `10` closes, `-2.000000000 SOL`
- token `AmmT...`: `6` closes, `-1.200000000 SOL`
- token `8exv...`: `5` closes, `-1.000000000 SOL`
- token `CmFY...`: `5` closes, `-1.000000000 SOL`

This supports the interpretation that the tail problem is illiquid/rug inventory, not a uniformly bad entry signal.

### Current live state at analysis checkpoint

Morning checkpoint around `2026-03-14 09:31 Europe/Kiev`:

- `followlist.active = 560`
- `copy_signals = 559249`
- `shadow_lots_open = 63`
- `open_shadow_cost_basis = 11.998392274 SOL`
- runtime healthy, `cursor/head gap ~= 10s`

Open-lot age at the earlier post-cleanup checkpoint showed all open inventory was `< 1h`, which is consistent with the strategy operating on a short holding horizon rather than a multi-hour one.

### Conclusion

The current `6h -> 12h zero` stale policy is too slow for the observed strategy behavior.

The evidence supports all of the following:

1. The profitable part of the strategy realizes within the first hour.
2. The strategy does not show evidence of needing multi-hour holds to realize its edge.
3. The dominant loss source is not the normal `market` close path.
4. The dominant loss source is the stale tail that survives until `~12h` and then zero-closes.

In this sample window:

- `market` path produced `+2.127307293 SOL`
- automatic `stale_terminal_zero_price` produced `-9.533076415 SOL`

That means the stale tail erased roughly `4.5x` the profit produced by the normal market exits in the same window.

### Recommended policy change

Recommended first policy step:

1. change `max_hold_hours` from `6` to `2`
2. change `shadow_stale_close_terminal_zero_price_hours` from `12` to `4`
3. keep `shadow_stale_close_recovery_zero_price_enabled = false` for the first rollout, to preserve attribution clarity

Rationale:

1. `2h/4h` is materially tighter than `6h/12h` and should cut the observed stale tail much earlier.
2. It still leaves a bounded `2h..4h` recovery window instead of forcing an immediate `1h/2h` jump.
3. It is a safer first policy move than jumping directly to `1h/2h` from only one strong evidence window.

### What this recommendation is **not**

This is not a recommendation to:

1. raise `soft_cap` as the primary fix,
2. change only risk limits while leaving stale-close policy untouched,
3. enable recovery zero-price mode by default for normal operation.

The data indicates the main problem is stale inventory lifetime, not entry generation and not soft-cap mechanics.

### Acceptance criteria after a `2h/4h` rollout

Monitor over the next comparable window:

1. `market` closes count
2. `market` realized PnL
3. `stale_terminal_zero_price` close count and realized loss
4. hold-time distribution of `market` closes
5. open-lot age buckets
6. `shadow_risk_pause` frequency
7. `open_shadow_cost_basis` / `risk_open_notional`

Successful outcome looks like:

1. `market` close cadence remains healthy
2. `market` PnL stays positive or at least materially better than stale-tail losses
3. `stale_terminal_zero_price` count and loss shrink materially versus the current `6h/12h` baseline
4. open inventory no longer accumulates into a large `8h..12h` dead tail

### Rollback / re-evaluation signals

Re-evaluate or rollback if:

1. `market` closes collapse materially versus the current baseline
2. `market` PnL turns clearly negative over the first meaningful post-change sample
3. stale closes still dominate realized losses even after shortening the hold policy
4. followlist/copy-signals collapse independently of the stale policy change

### Recommendation summary

Auditor-facing recommendation:

- The observed strategy behaves like a short-horizon strategy.
- The current `6h -> 12h zero` stale policy is not aligned with that behavior.
- A first controlled policy change to `2h hold / 4h terminal zero` is justified by live evidence.
- Keep recovery mode disabled on the first rollout for cleaner attribution.
