#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$ROOT_DIR/tools/lib/common.sh"

OUT_DIR="${1:-tmp/refactor-baseline}"
SECRETS_DIR="$OUT_DIR/secrets"
FAKE_BIN_DIR="$OUT_DIR/fake-bin"
DB_PATH="$OUT_DIR/legacy.db"
CONFIG_PATH="$OUT_DIR/devnet_rehearsal.toml"
ADAPTER_ENV_PATH="$OUT_DIR/adapter.env"
EXECUTOR_ENV_PATH="$OUT_DIR/executor.env"
refactor_baseline_exact_money_ready_raw="${REFACTOR_BASELINE_EXACT_MONEY_READY:-false}"
if ! refactor_baseline_exact_money_ready="$(parse_bool_token_strict "$refactor_baseline_exact_money_ready_raw")"; then
  echo "REFACTOR_BASELINE_EXACT_MONEY_READY must be a boolean token (got: ${refactor_baseline_exact_money_ready_raw:-<empty>})" >&2
  exit 1
fi

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
rm -f "$DB_PATH" "$CONFIG_PATH" "$ADAPTER_ENV_PATH" "$EXECUTOR_ENV_PATH"
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

[ingestion]
source = "yellowstone_grpc"

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

cat >"$EXECUTOR_ENV_PATH" <<'EOF_EXECUTOR_ENV'
COPYBOT_EXECUTOR_BACKEND_MODE=upstream
COPYBOT_EXECUTOR_ROUTE_ALLOWLIST=paper
COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=http://127.0.0.1:18080/submit
COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=http://127.0.0.1:18080/simulate
EOF_EXECUTOR_ENV

if [[ -n "${REFACTOR_BASELINE_EXECUTOR_SIGNER_PUBKEY:-}" ]]; then
  printf 'COPYBOT_EXECUTOR_SIGNER_PUBKEY=%s\n' "$REFACTOR_BASELINE_EXECUTOR_SIGNER_PUBKEY" >>"$EXECUTOR_ENV_PATH"
fi
if [[ -n "${REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_STRICT:-}" ]]; then
  printf 'COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT=%s\n' "$REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_STRICT" >>"$EXECUTOR_ENV_PATH"
fi
if [[ -n "${REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_RPC_URL:-}" ]]; then
  printf 'COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL=%s\n' "$REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_RPC_URL" >>"$EXECUTOR_ENV_PATH"
fi
if [[ -n "${REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL:-}" ]]; then
  printf 'COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL=%s\n' "$REFACTOR_BASELINE_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL" >>"$EXECUTOR_ENV_PATH"
fi

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
VALUES ('wallet-a', 'token-a', 0.25, '2026-02-19 11:00:00');

INSERT INTO shadow_closed_trades(pnl_sol, closed_ts)
VALUES (0.02, '2026-02-19 11:30:00');

INSERT INTO copy_signals(ts, status, side)
VALUES
  ('2026-02-19 11:50:00', 'shadow_recorded', 'buy'),
  ('2026-02-19 11:52:00', 'execution_confirmed', 'sell');

INSERT INTO risk_events(ts, type, severity, details_json)
VALUES
  ('2026-02-19 11:55:00', 'execution_submit_failed', 'error', '{"order_id":"order-strict","route":"paper","error_code":"submit_adapter_policy_echo_missing"}'),
  ('2026-02-19 11:56:00', 'execution_network_fee_unavailable_submit_hint_used', 'warn', '{"route":"paper"}'),
  ('2026-02-19 11:57:00', 'execution_network_fee_unavailable_fallback_used', 'warn', '{"route":"paper"}'),
  ('2026-02-19 11:58:00', 'execution_network_fee_hint_mismatch', 'warn', '{"route":"paper"}');

INSERT INTO orders(
  order_id, route, status, err_code, simulation_error, submit_ts, confirm_ts,
  applied_tip_lamports, ata_create_rent_lamports, network_fee_lamports_hint,
  base_fee_lamports_hint, priority_fee_lamports_hint
)
VALUES
  ('order-confirmed-modern', 'paper', 'execution_confirmed', NULL, NULL, '2026-02-19 11:40:00', '2026-02-19 11:45:00', 3000, 2039280, 7000, 5000, 2000),
  ('order-strict', 'paper', 'execution_failed', 'submit_terminal_rejected', 'submit_adapter_policy_echo_missing', '2026-02-19 11:50:00', NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO fills(order_id, fee)
VALUES ('order-confirmed-modern', 0.00204928);
SQL

if [[ "$refactor_baseline_exact_money_ready" == "true" ]]; then
  sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE copy_signals ADD COLUMN notional_lamports INTEGER;
ALTER TABLE copy_signals ADD COLUMN notional_origin TEXT;
ALTER TABLE fills ADD COLUMN qty_raw TEXT;
ALTER TABLE fills ADD COLUMN qty_decimals INTEGER;
ALTER TABLE fills ADD COLUMN notional_lamports INTEGER;
ALTER TABLE fills ADD COLUMN fee_lamports INTEGER;
ALTER TABLE shadow_lots ADD COLUMN accounting_bucket TEXT;
ALTER TABLE shadow_lots ADD COLUMN qty_raw TEXT;
ALTER TABLE shadow_lots ADD COLUMN qty_decimals INTEGER;
ALTER TABLE shadow_lots ADD COLUMN cost_lamports INTEGER;
ALTER TABLE shadow_closed_trades ADD COLUMN accounting_bucket TEXT;
ALTER TABLE shadow_closed_trades ADD COLUMN qty_raw TEXT;
ALTER TABLE shadow_closed_trades ADD COLUMN qty_decimals INTEGER;
ALTER TABLE shadow_closed_trades ADD COLUMN entry_cost_lamports INTEGER;
ALTER TABLE shadow_closed_trades ADD COLUMN exit_value_lamports INTEGER;
ALTER TABLE shadow_closed_trades ADD COLUMN pnl_lamports INTEGER;

CREATE TABLE observed_swaps(
  signature TEXT PRIMARY KEY,
  ts TEXT,
  qty_in_raw TEXT,
  qty_in_decimals INTEGER,
  qty_out_raw TEXT,
  qty_out_decimals INTEGER
);

CREATE TABLE positions(
  id INTEGER PRIMARY KEY,
  opened_ts TEXT,
  closed_ts TEXT,
  accounting_bucket TEXT NOT NULL,
  qty_raw TEXT,
  qty_decimals INTEGER,
  cost_lamports INTEGER,
  pnl_lamports INTEGER
);

CREATE TABLE exact_money_cutover_state(
  id INTEGER PRIMARY KEY,
  cutover_ts TEXT NOT NULL,
  recorded_ts TEXT NOT NULL,
  note TEXT
);

INSERT INTO exact_money_cutover_state(id, cutover_ts, recorded_ts, note)
VALUES (1, datetime('now', '-30 minutes'), datetime('now', '-29 minutes'), 'refactor baseline exact ready');

UPDATE copy_signals
SET notional_lamports = 1000000,
    notional_origin = 'leader_exact_lamports';

UPDATE fills
SET qty_raw = '1000000',
    qty_decimals = 6,
    notional_lamports = 2000000,
    fee_lamports = 12000;

UPDATE shadow_lots
SET accounting_bucket = 'exact_post_cutover',
    qty_raw = '250000000',
    qty_decimals = 9,
    cost_lamports = 250000000;

UPDATE shadow_closed_trades
SET accounting_bucket = 'exact_post_cutover',
    qty_raw = '50000000',
    qty_decimals = 9,
    entry_cost_lamports = 100000000,
    exit_value_lamports = 120000000,
    pnl_lamports = 20000000;

INSERT OR REPLACE INTO observed_swaps(signature, ts, qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals)
VALUES ('sig-exact-ready', datetime('now', '-5 minutes'), '100', 6, '200', 6);

INSERT INTO positions(id, opened_ts, closed_ts, accounting_bucket, qty_raw, qty_decimals, cost_lamports, pnl_lamports)
VALUES (1, datetime('now', '-4 minutes'), datetime('now', '-3 minutes'), 'exact_post_cutover', '750000', 6, 1500000, 250000);
SQL
fi

echo "prepared_refactor_baseline_dir: $OUT_DIR"
echo "config_path: $CONFIG_PATH"
echo "adapter_env_path: $ADAPTER_ENV_PATH"
echo "executor_env_path: $EXECUTOR_ENV_PATH"
echo "db_path: $DB_PATH"
echo "fake_journalctl_dir: $FAKE_BIN_DIR"
echo "exact_money_ready: $refactor_baseline_exact_money_ready"
