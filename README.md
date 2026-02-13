# solana-copy-bot

Phase 0 skeleton for Solana copy bot in Rust:
- workspace layout,
- configuration loader,
- structured logging,
- SQLite bootstrap with migrations,
- app heartbeat loop.

Phase 1 scaffold included:
- ingestion service (`mock` + `helius_ws` source modes),
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

Run shadow ingestion against Helius WebSocket + HTTP RPC (no execution logic, only ingest + persist):

```bash
cd solana-copy-bot
SOLANA_COPY_BOT_CONFIG=configs/paper.toml \
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws \
SOLANA_COPY_BOT_HELIUS_WS_URL="wss://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>" \
SOLANA_COPY_BOT_HELIUS_HTTP_URL="https://mainnet.helius-rpc.com/?api-key=<YOUR_KEY>" \
cargo run -p copybot-app
```

Optional program filters override:

```bash
SOLANA_COPY_BOT_PROGRAM_IDS="675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8,pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
```

## Layout

- `crates/app`: runtime entrypoint.
- `crates/config`: typed TOML config loader.
- `crates/storage`: SQLite store + migration runner.
- `crates/core-types`: shared event and domain types.
- `migrations`: SQL schema files.
- `configs`: dev/paper/prod presets.

## Notes

- `ingestion.source="mock"` generates synthetic swaps and stores them in SQLite.
- `ingestion.source="helius_ws"` uses `logsSubscribe` for target program IDs and `getTransaction` for full swap parsing.
- discovery cycle runs every `discovery.refresh_seconds` and recalculates score/follow-list.
- If URLs are invalid/missing key, bot fails closed for entries and keeps retrying stream connection.
- Execution/copy trading logic is not implemented yet.
- Keep keys out of repository and mounted via environment/secret files.
