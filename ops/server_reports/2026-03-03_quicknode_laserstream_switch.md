# QuickNode LaserStream Switch Report

Date (UTC): 2026-03-03  
Server: `52.28.0.218`  
Scope: switch ingestion from WS+HTTP polling to QuickNode Yellowstone gRPC (shadow run, no live trading)

## Applied change

Updated [/etc/solana-copy-bot/live.server.toml](/etc/solana-copy-bot/live.server.toml):

1. `ingestion.source = "yellowstone_grpc"`
2. `ingestion.yellowstone_grpc_url = "https://<QUICKNODE_HOST>:10000"`
3. `ingestion.yellowstone_x_token = "<set on server>"`

Also removed stale override (none present) and restarted:

1. `rm -f /var/www/solana-copy-bot/state/ingestion_source_override.env`
2. `systemctl restart solana-copy-bot`

## Validation against success criteria

1. `grpc_transaction_updates_total > 0`  
   PASS
   - Snapshot 1 (`2026-03-03T17:48:56Z`): `grpc_transaction_updates_total=1`
   - Snapshot 2 (`2026-03-03T17:49:31Z`): `grpc_transaction_updates_total=16948`
   - Delta: `+16947` in 35 seconds

2. `rpc_429` stops growing at previous pace  
   PASS
   - Snapshot 1: `rpc_429=0`
   - Snapshot 2: `rpc_429=0`
   - Delta: `0`

3. No dominant `retryable getTransaction status 429` after switch  
   PASS
   - `journalctl -u solana-copy-bot --since "2026-03-03 17:48:30" | grep -c "getTransaction status 429"` => `0`

## Notes

1. Service is active after restart.
2. `SOLANA_COPY_BOT_EXECUTION_ENABLED=false` remains in effect (shadow-only run).
