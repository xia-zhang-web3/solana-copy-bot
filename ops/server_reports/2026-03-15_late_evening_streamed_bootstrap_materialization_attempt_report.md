## 2026-03-15 late evening — streamed trusted bootstrap materialization attempt

### Scope

- Logical rollout target: `5ec3bc7 Stream trusted bootstrap materialization`
- Live-safe server deployment base: `c36f40a`
- Narrow server commit actually deployed: `f924312`
  - reason: `5ec3bc7` sits on top of unrelated `6cba34a`, so the server received a clean cherry-pick of the bootstrap rewrite onto `c36f40a` instead of a dirty `main` fast-forward.

### Local validation

Validated on a clean detached worktree at:

- `71f644d = c36f40a + cherry-pick 5ec3bc7`

Commands:

- `cargo check -p copybot-discovery -p copybot-app -p copybot-storage --quiet`
- `cargo test -p copybot-discovery -p copybot-storage`

Results:

- `copybot-discovery`: `54 passed`
- `copybot-storage`: `102 passed`
- `backfill_discovery_scoring`: `3 passed`
- `cargo check` passed; only pre-existing `ShadowRiskGuard` dead-code warnings remained in `copybot-app`

### Pre-attempt live state

Before the streamed bootstrap attempt:

- deployed live code: `c36f40a`
- `wallet_metrics MAX(window_start) = 2026-03-10T14:00:00+00:00`
- `discovery_strategy_state.trusted_selection_bootstrap_required = 1`
- reason: `trusted_selection_bootstrap_unavailable`
- `followlist.active = 0`
- runtime healthy and intentionally fail-closed

### What was deployed

Server steps:

1. Fetched `origin/main`
2. Cherry-picked `5ec3bc7` onto detached `c36f40a`
3. Built:
   - `copybot-app`
   - `materialize_wallet_metrics_bootstrap`

Server rollout commit after cherry-pick:

- `f924312`

### Attempt sequence

Controlled one-shot bootstrap attempt:

1. Confirm pre-state.
2. Stop `solana-copy-bot.service`.
3. Run persisted-only bootstrap tool:

   `target/release/materialize_wallet_metrics_bootstrap state/live_copybot.db --config configs/live.toml`

4. Expect a fresh trusted `wallet_metrics` bucket for the current bootstrap window.
5. Restart `solana-copy-bot.service`.

### What changed relative to the previous attempt

The previous one-shot materializer (`c36f40a`) failed due to memory blow-up.

The streamed rewrite fixed that part:

- no multi-GB RSS explosion,
- memory stayed bounded for most of the run,
- server stayed healthy from a RAM perspective.

Observed during this attempt:

- early RSS stayed around `~10 MiB`
- later final-phase RSS rose only to about `~1.6 GiB`
- the host never approached the previous OOM-risk state

### What still failed

The streamed rewrite did **not** make the bootstrap tool operationally viable as a one-shot live bootstrap.

Observed facts:

- the app had to stay stopped for the duration of the materialization attempt
- the tool kept running for more than `76m`
- process telemetry during the run showed continued progress:
  - `read_bytes` grew past `23 GB`
  - `write_bytes` reached about `1.57 GB`
- but the tool still did not finish within a reasonable maintenance window

At that point the attempt was aborted manually to return the live runtime.

### Post-abort verification

After aborting the tool:

- `wallet_metrics MAX(window_start)` was still `2026-03-10T14:00:00+00:00`
- `trusted_selection_bootstrap_required` remained `1`
- reason remained `trusted_selection_bootstrap_unavailable`
- `followlist.active` remained `0`

So despite substantial I/O, the attempt did **not** leave behind a fresh trusted bootstrap bucket that discovery could use.

### Recovery

1. The materialization process was killed.
2. `solana-copy-bot.service` was restarted.
3. `copybot-adapter.service` and `copybot-executor.service` remained active.

Post-restart state:

- `solana-copy-bot.service MainPID=301752`
- `NRestarts=0`
- `ActiveEnterTimestamp=2026-03-15 20:18:29 UTC`
- startup WAL checkpoint completed cleanly:
  - `wal_checkpoint_mode = truncate`
  - `wal_checkpoint_busy = 0`
  - `wal_log_frames = 0`
  - `wal_checkpointed_frames = 0`
- runtime healthy again:
  - `observed_swap_writer_pending_requests = 0`
  - `sqlite_busy_error_total = 0`
  - `sqlite_write_retry_total = 0`
- strategy still fail-closed:
  - `trusted_selection_bootstrap_required = true`
  - `app_follow_rejected_ratio = 1.0`
  - discovery again logs `trusted bootstrap unavailable; fail-closing recovered followlist until a trusted persisted selection source exists`

### Verdict

`5ec3bc7` fixed the **memory** failure mode, but did **not** yet produce a production-viable trusted bootstrap recovery path.

What is now proven:

1. The streamed rewrite is materially better than the previous one-shot implementation:
   - it no longer blows up RAM,
   - it can process the live DB without driving the host toward OOM.
2. But it is still too slow / heavy to use as the current one-shot operational bootstrap:
   - app downtime exceeded one hour,
   - the tool still did not finish,
   - no fresh trusted bucket was materialized.

### Required next step

Do **not** keep retrying this exact one-shot tool as-is.

The next fix must target **wall-clock operational viability**, not memory:

- either a resumable / chunked bootstrap materialization path,
- or an equivalent bounded multi-run bootstrap workflow that can advance trusted `wallet_metrics` without keeping the app stopped for >1h in one shot.

Until then:

- keep live on the current fail-closed strategy contract
- do not treat this bootstrap path as production-ready
