#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-tmp/refactor-baseline}"
SECRETS_DIR="$OUT_DIR/secrets"
FAKE_BIN_DIR="$OUT_DIR/fake-bin"
DB_PATH="$OUT_DIR/legacy.db"
CONFIG_PATH="$OUT_DIR/devnet_rehearsal.toml"
ADAPTER_ENV_PATH="$OUT_DIR/adapter.env"

require_bin() {
  local bin="$1"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "required binary not found: $bin" >&2
    exit 1
  fi
}

require_bin sqlite3
require_bin python3

mkdir -p "$OUT_DIR" "$SECRETS_DIR" "$FAKE_BIN_DIR"
rm -f "$DB_PATH" "$CONFIG_PATH" "$ADAPTER_ENV_PATH"
rm -f "$SECRETS_DIR"/*.token "$SECRETS_DIR"/*.secret 2>/dev/null || true

cat >"$FAKE_BIN_DIR/journalctl" <<'JEOF'
#!/usr/bin/env bash
set -euo pipefail
cat <<'LOGS'
2026-02-19T12:00:00Z INFO ingestion pipeline metrics {"ingestion_lag_ms_p95":1700,"ingestion_lag_ms_p99":2600,"ws_to_fetch_queue_depth":2,"fetch_to_output_queue_depth":1,"fetch_concurrency_inflight":3,"ws_notifications_enqueued":222,"ws_notifications_replaced_oldest":1,"reconnect_count":1,"stream_gap_detected":0,"parse_rejected_total":5,"parse_rejected_by_reason":{"missing_signer":3,"other":2},"parse_fallback_by_reason":{"missing_program_ids_fallback":1,"missing_slot_fallback":2},"grpc_message_total":22345,"grpc_decode_errors":1,"rpc_429":1,"rpc_5xx":0}
2026-02-19T12:00:01Z INFO sqlite contention counters {"sqlite_write_retry_total":0,"sqlite_busy_error_total":0}
LOGS
JEOF
chmod +x "$FAKE_BIN_DIR/journalctl"

cat >"$CONFIG_PATH" <<EOF_CFG
[system]
env = "dev"

[sqlite]
path = "$DB_PATH"

[risk]
max_position_sol = 0.5
max_total_exposure_sol = 3.0
max_hold_hours = 8
shadow_soft_exposure_cap_sol = 10.0
shadow_hard_exposure_cap_sol = 12.0
shadow_killswitch_enabled = true

[execution]
enabled = true
mode = "adapter_submit_confirm"
execution_signer_pubkey = "Signer1111111111111111111111111111111111"
rpc_http_url = "https://rpc.mainnet.local"
rpc_devnet_http_url = "https://api.devnet.solana.com"
submit_adapter_http_url = "https://adapter.primary.local/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
default_route = "paper"
submit_allowed_routes = ["paper"]
submit_route_order = ["paper"]
submit_route_max_slippage_bps = { paper = 50.0 }
submit_route_tip_lamports = { paper = 0 }
submit_route_compute_unit_limit = { paper = 300000 }
submit_route_compute_unit_price_micro_lamports = { paper = 1000 }
submit_adapter_auth_token = "token-inline"
EOF_CFG

cat >"$ADAPTER_ENV_PATH" <<'EOF_ENV'
COPYBOT_ADAPTER_BEARER_TOKEN_FILE="secrets/adapter_bearer.token"
COPYBOT_ADAPTER_HMAC_KEY_ID="key-rotation"
COPYBOT_ADAPTER_HMAC_SECRET_FILE="secrets/adapter_hmac.secret"
COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE="secrets/upstream_auth.token"
COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE="secrets/upstream_fallback_auth.token"
COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE="secrets/send_rpc_auth.token"
COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE="secrets/send_rpc_fallback_auth.token"
COPYBOT_ADAPTER_ROUTE_RPC_AUTH_TOKEN_FILE="secrets/route_rpc_auth.token"
COPYBOT_ADAPTER_ROUTE_RPC_SEND_RPC_AUTH_TOKEN_FILE="secrets/route_rpc_send_rpc_auth.token"
COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=false
EOF_ENV

printf '%s\n' "adapter-bearer" >"$SECRETS_DIR/adapter_bearer.token"
printf '%s\n' "adapter-hmac-secret" >"$SECRETS_DIR/adapter_hmac.secret"
printf '%s\n' "upstream-auth" >"$SECRETS_DIR/upstream_auth.token"
printf '%s\n' "upstream-fallback-auth" >"$SECRETS_DIR/upstream_fallback_auth.token"
printf '%s\n' "send-rpc-auth" >"$SECRETS_DIR/send_rpc_auth.token"
printf '%s\n' "send-rpc-fallback-auth" >"$SECRETS_DIR/send_rpc_fallback_auth.token"
printf '%s\n' "route-rpc-auth" >"$SECRETS_DIR/route_rpc_auth.token"
printf '%s\n' "route-rpc-send-rpc-auth" >"$SECRETS_DIR/route_rpc_send_rpc_auth.token"
chmod 600 "$SECRETS_DIR"/*.token
chmod 600 "$SECRETS_DIR"/*.secret

sqlite3 "$DB_PATH" <<'SQL'
PRAGMA journal_mode = WAL;

CREATE TABLE shadow_lots (
  wallet_id TEXT,
  token TEXT,
  cost_sol REAL,
  opened_ts TEXT
);

CREATE TABLE shadow_closed_trades (
  pnl_sol REAL,
  closed_ts TEXT
);

CREATE TABLE copy_signals (
  ts TEXT,
  status TEXT,
  side TEXT
);

CREATE TABLE fills (
  order_id TEXT,
  fee REAL
);

CREATE TABLE risk_events (
  ts TEXT,
  type TEXT,
  severity TEXT,
  details_json TEXT
);

CREATE TABLE orders (
  order_id TEXT PRIMARY KEY,
  route TEXT,
  status TEXT,
  err_code TEXT,
  simulation_error TEXT,
  submit_ts TEXT,
  confirm_ts TEXT,
  applied_tip_lamports INTEGER,
  ata_create_rent_lamports INTEGER,
  network_fee_lamports_hint INTEGER,
  base_fee_lamports_hint INTEGER,
  priority_fee_lamports_hint INTEGER
);

INSERT INTO shadow_lots(wallet_id, token, cost_sol, opened_ts)
VALUES ('wallet-a', 'token-a', 0.25, datetime('now', '-1 hour'));

INSERT INTO shadow_closed_trades(pnl_sol, closed_ts)
VALUES (0.02, datetime('now', '-30 minutes'));

INSERT INTO copy_signals(ts, status, side)
VALUES
  (datetime('now', '-10 minutes'), 'shadow_recorded', 'buy'),
  (datetime('now', '-8 minutes'), 'execution_confirmed', 'sell');

INSERT INTO risk_events(ts, type, severity, details_json)
VALUES
  (datetime('now', '-5 minutes'), 'execution_submit_failed', 'error', '{"order_id":"order-strict","route":"paper","error_code":"submit_adapter_policy_echo_missing"}'),
  (datetime('now', '-4 minutes'), 'execution_network_fee_unavailable_submit_hint_used', 'warn', '{"route":"paper"}'),
  (datetime('now', '-3 minutes'), 'execution_network_fee_unavailable_fallback_used', 'warn', '{"route":"paper"}'),
  (datetime('now', '-2 minutes'), 'execution_network_fee_hint_mismatch', 'warn', '{"route":"paper"}');

INSERT INTO orders(
  order_id, route, status, err_code, simulation_error, submit_ts, confirm_ts,
  applied_tip_lamports, ata_create_rent_lamports, network_fee_lamports_hint,
  base_fee_lamports_hint, priority_fee_lamports_hint
)
VALUES
  ('order-confirmed-modern', 'paper', 'execution_confirmed', NULL, NULL, datetime('now', '-20 minutes'), datetime('now', '-15 minutes'), 3000, 2039280, 7000, 5000, 2000),
  ('order-strict', 'paper', 'execution_failed', 'submit_terminal_rejected', 'submit_adapter_policy_echo_missing', datetime('now', '-10 minutes'), NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO fills(order_id, fee)
VALUES ('order-confirmed-modern', 0.00204928);
SQL

echo "prepared_refactor_baseline_dir: $OUT_DIR"
echo "config_path: $CONFIG_PATH"
echo "adapter_env_path: $ADAPTER_ENV_PATH"
echo "db_path: $DB_PATH"
echo "fake_journalctl_dir: $FAKE_BIN_DIR"
