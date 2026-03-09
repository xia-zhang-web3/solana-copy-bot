## 2026-03-09 late evening emergency-profile status

### Scope

Document the first live checkpoint after the 5-day emergency discovery profile and the `max_rug_ratio = 1.0` emergency override.

### Server checkpoint

- Host: `52.28.0.218`
- Approximate capture window: `2026-03-09 20:24 UTC` to `2026-03-09 20:29 UTC`
- Runtime: `solana-copy-bot.service`, `copybot-adapter.service`, `copybot-executor.service` all `active`
- `solana-copy-bot.service` start time: `2026-03-09 20:23:35 UTC`
- `NRestarts=0`

### Live config actually running

From `/etc/solana-copy-bot/live.server.toml`:

- `scoring_window_days = 5`
- `decay_window_days = 5`
- `min_leader_notional_sol = 0.5`
- `min_active_days = 3`
- `min_score = 0.4`
- `max_rug_ratio = 1.0`
- `metric_snapshot_interval_seconds = 60`
- `thin_market_min_volume_sol = 0.0`
- `thin_market_min_unique_traders = 0`
- `max_window_swaps_in_memory = 100000`
- `scoring_aggregates_write_enabled = false`
- `scoring_aggregates_enabled = false`

### Observed outcomes

#### Followlist / scoring

- Latest `wallet_metrics.window_start`: `2026-03-04T20:24:00+00:00`
- `positive_score_wallet_rows = 518`
- `followlist.active = 39`

Top scored wallets in the latest bucket:

- `ASA4euK5Lx7Wz3ntmsJufkCjfBkKFjgxo11pMwmBa1FW score=0.8718 trades=21 buy_total=13 tradable_ratio=1.0 rug_ratio=1.0`
- `D8VewbbDoMh6BPCTTYbtTmvWAnyPAyPdmpLWv4sCtQRe score=0.8245 trades=32 buy_total=15 tradable_ratio=1.0 rug_ratio=1.0`
- `EYZfU3xQT444SRATGFCzM3oMshvtcvAwsu7ZFejGDE6j score=0.8068 trades=28 buy_total=14 tradable_ratio=1.0 rug_ratio=1.0`

Interpretation:

- The `0 active wallets` blocker is closed.
- The emergency 5-day profile plus `max_rug_ratio = 1.0` override materially changed live outcomes.

#### Shadow pipeline

- `copy_signals = 95`
- `shadow_lots = 20`
- `orders = 0`
- `copy_signals.status` distribution: `shadow_recorded = 95`

Live logs show repeated:

- `shadow followed wallet swap reached pipeline`
- `shadow signal recorded`

Interpretation:

- Shadow trading is active and end-to-end alive.
- Signals are being created and shadow positions are being tracked.

### Why orders are still zero

This is not the same blocker as `0 active wallets`.

The immediate reason is operational config:

- `[execution].enabled = false`
- `mode = "adapter_submit_confirm"`

So live execution is intentionally disabled. The runtime is currently shadow-only.

There is also a second live-execution blocker still visible in current server config / service env:

- `execution_signer_pubkey = "11111111111111111111111111111111"`
- `COPYBOT_EXECUTOR_SIGNER_PUBKEY=11111111111111111111111111111111`
- `COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE=/etc/solana-copy-bot/secrets/executor-signer.devnet.json`
- `COPYBOT_ADAPTER_SIGNER_PUBKEY=11111111111111111111111111111111`

Interpretation:

- Even if `execution.enabled` were flipped on immediately, the signer material currently looks like placeholder/devnet state, not a production mainnet signer cutover.

### Quality / risk notes

Current shadow quality gates are not the primary explanation for `orders = 0`.

Observed from logs:

- quality metrics are being evaluated successfully
- many followed swaps are dropped at `stage="notional"` with `reason="below_notional"`
- some are dropped at `stage="quality"` with `reason="too_new"`

These are expected shadow/runtime gates, but they do not explain the zero-order state because execution is disabled before live order submission.

### Operational conclusion

Current live status is:

1. Emergency followlist activation succeeded.
2. Shadow trading is working.
3. The next blocker is no longer discovery/followlist.
4. The next blocker is live execution cutover:
   - real signer material,
   - `execution.enabled = true`,
   - restart,
   - submit/reject monitoring.

### Recommended next step

Do not change discovery again right now.

Next track should be a controlled `shadow -> live execution` cutover runbook:

1. install / verify the real execution signer,
2. enable execution,
3. restart `copybot-executor.service`, `copybot-adapter.service`, `solana-copy-bot.service`,
4. watch adapter/executor submit logs and order creation,
5. only after that revert the temporary `metric_snapshot_interval_seconds = 60` debug knob back to `1800`.
