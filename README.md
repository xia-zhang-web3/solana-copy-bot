# solana-copy-bot

Phase 0 skeleton for Solana copy bot in Rust:
- workspace layout,
- configuration loader,
- structured logging,
- SQLite bootstrap with migrations,
- app heartbeat loop.

Phase 1 scaffold included:
- ingestion service (`mock` + `helius_ws` + `yellowstone_grpc` source modes),
- Raydium/PumpSwap swap parser by program IDs,
- observed swaps persistence into SQLite (`observed_swaps` table).

Phase 1 runtime additions:
- rolling wallet metrics + scoring writes (`wallet_metrics`),
- top-N follow-list promotion/demotion (`followlist`),
- hard gates for min trades/active days/notional and tx-per-minute spam filter.

## Quick Start

```bash
cd solana-copy-bot
cargo run -p copybot-app -- --config configs/dev.toml
```

Alternative config path via env:

```bash
cd solana-copy-bot
SOLANA_COPY_BOT_CONFIG=configs/paper.toml cargo run -p copybot-app
```

Run adapter backend (contract gateway for `adapter_submit_confirm` mode):

```bash
cd solana-copy-bot
COPYBOT_ADAPTER_BIND_ADDR=127.0.0.1:8080 \
COPYBOT_ADAPTER_CONTRACT_VERSION=v1 \
COPYBOT_ADAPTER_SIGNER_PUBKEY=<YOUR_SIGNER_PUBKEY> \
COPYBOT_ADAPTER_ROUTE_ALLOWLIST=rpc,paper \
COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=https://<UPSTREAM_EXECUTOR>/submit \
COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=https://<UPSTREAM_EXECUTOR>/simulate \
cargo run -p copybot-adapter
```

Run shadow ingestion against Helius WebSocket + HTTP RPC (no execution logic, only ingest + persist):

```bash
cd solana-copy-bot
SOLANA_COPY_BOT_CONFIG=configs/paper.toml \
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws \
SOLANA_COPY_BOT_HELIUS_WS_URL="wss://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>" \
SOLANA_COPY_BOT_HELIUS_HTTP_URL="https://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>" \
cargo run -p copybot-app
```

Run shadow ingestion against QuickNode Yellowstone gRPC (primary path) with Helius HTTP fallback for quality refresh:

```bash
cd solana-copy-bot
SOLANA_COPY_BOT_CONFIG=configs/paper.toml \
SOLANA_COPY_BOT_INGESTION_SOURCE=yellowstone_grpc \
SOLANA_COPY_BOT_YELLOWSTONE_GRPC_URL="https://<YOUR_QUICKNODE_GRPC_HOST>:10000" \
SOLANA_COPY_BOT_YELLOWSTONE_X_TOKEN="<YOUR_QUICKNODE_X_TOKEN>" \
SOLANA_COPY_BOT_HELIUS_HTTP_URL="https://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>" \
cargo run -p copybot-app
```

Use the exact scheme/host provided by your gRPC vendor (`https://...` for TLS endpoints, `http://...` only for explicit plaintext/local setups).

Use dedicated HTTP RPC endpoints for quality-refresh workloads (recommended):

```bash
SOLANA_COPY_BOT_DISCOVERY_HELIUS_HTTP_URL="https://mainnet.helius-rpc.com/?api-key=<DISCOVERY_KEY>" \
SOLANA_COPY_BOT_SHADOW_HELIUS_HTTP_URL="https://mainnet.helius-rpc.com/?api-key=<SHADOW_KEY>"
```

Tune ingestion parallel pipeline from env:

```bash
SOLANA_COPY_BOT_INGESTION_FETCH_CONCURRENCY=3 \
SOLANA_COPY_BOT_INGESTION_WS_QUEUE_CAPACITY=512 \
SOLANA_COPY_BOT_INGESTION_OUTPUT_QUEUE_CAPACITY=2048 \
SOLANA_COPY_BOT_INGESTION_PREFETCH_STALE_DROP_MS=30000 \
SOLANA_COPY_BOT_INGESTION_SEEN_SIGNATURES_TTL_MS=600000 \
SOLANA_COPY_BOT_INGESTION_QUEUE_OVERFLOW_POLICY=drop_oldest \
SOLANA_COPY_BOT_INGESTION_REORDER_HOLD_MS=1500 \
SOLANA_COPY_BOT_INGESTION_REORDER_MAX_BUFFER=1024 \
SOLANA_COPY_BOT_INGESTION_GLOBAL_RPC_RPS_LIMIT=45 \
SOLANA_COPY_BOT_INGESTION_PER_ENDPOINT_RPC_RPS_LIMIT=16 \
SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_DELAY_MS=500 \
SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_MAX_MS=2000 \
SOLANA_COPY_BOT_INGESTION_TX_FETCH_RETRY_JITTER_MS=150 \
SOLANA_COPY_BOT_INGESTION_TELEMETRY_REPORT_SECONDS=30
```

Optional: load-balance ingestion `getTransaction` calls across multiple Helius keys:

```bash
SOLANA_COPY_BOT_INGESTION_HELIUS_HTTP_URLS="https://mainnet.helius-rpc.com/?api-key=<KEY1>,https://mainnet.helius-rpc.com/?api-key=<KEY2>,https://mainnet.helius-rpc.com/?api-key=<KEY3>"
```

Optional sell causal holdback controls:

```bash
SOLANA_COPY_BOT_SHADOW_CAUSAL_HOLDBACK_ENABLED=true \
SOLANA_COPY_BOT_SHADOW_CAUSAL_HOLDBACK_MS=2500
```

Optional program filters override:

```bash
SOLANA_COPY_BOT_PROGRAM_IDS="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8,pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
```

Optional Yellowstone-specific overrides:

```bash
SOLANA_COPY_BOT_YELLOWSTONE_CONNECT_TIMEOUT_MS=5000 \
SOLANA_COPY_BOT_YELLOWSTONE_SUBSCRIBE_TIMEOUT_MS=15000 \
SOLANA_COPY_BOT_YELLOWSTONE_STREAM_BUFFER_CAPACITY=512 \
SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_INITIAL_MS=500 \
SOLANA_COPY_BOT_YELLOWSTONE_RECONNECT_MAX_MS=8000 \
SOLANA_COPY_BOT_YELLOWSTONE_PROGRAM_IDS="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8,pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
```

Watchdog failover helper (writes `state/ingestion_source_override.env` and optional restart):

```bash
cd solana-copy-bot
POLICY_FILE=ops/ingestion_failover_policy.toml \
CONFIG_PATH=configs/paper.toml \
./tools/ingestion_failover_watchdog.sh
```

Systemd wiring example for watchdog and override profile:

- `ops/ingestion_failover_watchdog.md`

Quick recovery from failover:

```bash
rm -f state/ingestion_source_override.env state/ingestion_failover_cooldown.json
```

A/B gate report for replay or live canary (`control` vs `candidate`):

```bash
cd solana-copy-bot
./tools/ingestion_ab_report.sh \
  --control-config configs/paper.toml \
  --candidate-config configs/paper.toml \
  --mode replay \
  --fixture-id replay-2026-02-18 \
  --fixture-sha256 <FIXTURE_SHA256> \
  --output-json state/ab_report.json
```

Live canary variant with telemetry gates from journald:

```bash
./tools/ingestion_ab_report.sh \
  --control-config configs/paper.toml \
  --candidate-config configs/paper-canary-yellowstone.toml \
  --control-service solana-copy-bot \
  --candidate-service solana-copy-bot-canary \
  --mode live \
  --window-minutes 360 \
  --output-json state/ab_report_live.json
```

Cutover runbook:

- `ops/yellowstone_rollout_runbook.md`
- `ops/execution_manual_reconcile_runbook.md` (`execution_price_unavailable_fallback_used` handling)

## Layout

- `crates/app`: runtime entrypoint.
- `crates/config`: typed TOML config loader.
- `crates/storage`: SQLite store + migration runner.
- `crates/core-types`: shared event and domain types.
- `crates/adapter`: adapter backend HTTP service (`/simulate`, `/submit`, `/healthz`).
- `migrations`: SQL schema files.
- `configs`: dev/paper/prod presets.

## Notes

- `ingestion.source="mock"` generates synthetic swaps and stores them in SQLite.
- `ingestion.source="helius_ws"` uses `logsSubscribe` for target program IDs and a bounded parallel `getTransaction` worker pool for full swap parsing.
- `ingestion.source="yellowstone_grpc"` subscribes to transaction stream directly and removes per-event HTTP fetch from the hot path.
- ws->fetch queue supports bounded policies (`block`/`drop_oldest`) to prioritize fresh traffic under overload.
- ingestion includes bounded reorder-by-`(slot, arrival_seq, signature)` holdback to reduce out-of-order processing risk.
- ingestion supports pre-fetch stale drop, signature dedupe TTL/LRU, and optional global/per-endpoint token-bucket HTTP rate limiting.
- shadow scheduler supports per-`(wallet, token)` sell causal holdback before release into processing queue.
- ingestion emits periodic pipeline metrics (`ws_to_fetch_queue_depth`, `ws_notifications_backpressured`, `fetch_latency_ms`, `ingestion_lag_ms`, `reorder_buffer_size`, RPC `429/5xx`, gRPC reconnect/decode/parse counters).
- app emits SQLite contention counters (`sqlite_write_retry_total`, `sqlite_busy_error_total`) in heartbeat logs.
- discovery cycle runs every `discovery.refresh_seconds` and recalculates score/follow-list.
- If URLs are invalid/missing key, bot fails closed for entries and keeps retrying stream connection.
- Execution runtime is implemented (paper lifecycle + adapter submit/confirm hardening path).
- Adapter backend deploy/runbook: `ops/adapter_backend_runbook.md`.
- Keep keys out of repository and mounted via environment/secret files.
