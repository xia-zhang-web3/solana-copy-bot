#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/state/6_1_local_dry_run"
LOG_DIR="$RUN_DIR/logs"
SECRETS_DIR="$RUN_DIR/secrets"
FAKE_BIN_DIR="$RUN_DIR/fake-bin"

PUBKEY="11111111111111111111111111111111"
TOKEN_VALUE="local-6-1-bearer-token"
RUN_REAL_EXECUTOR="${RUN_REAL_EXECUTOR:-false}"

mkdir -p "$RUN_DIR" "$LOG_DIR" "$SECRETS_DIR" "$FAKE_BIN_DIR"

EXECUTOR_ENV_PATH="$RUN_DIR/executor.env"
ADAPTER_ENV_PATH="$RUN_DIR/adapter.env"
APP_ENV_PATH="$RUN_DIR/app.env"
CONFIG_PATH_LOCAL="$RUN_DIR/live.server.toml"

EXECUTOR_LOG="$LOG_DIR/executor.log"
HEALTHZ_JSON="$RUN_DIR/healthz.json"
ADAPTER_PREFLIGHT_OUT="$RUN_DIR/execution_adapter_preflight.out"
EXECUTOR_PREFLIGHT_OUT="$RUN_DIR/executor_preflight.out"
SUMMARY_OUT="$RUN_DIR/summary.txt"

BEARER_TOKEN_FILE="$SECRETS_DIR/executor_bearer.token"
ADAPTER_BEARER_TOKEN_FILE="$SECRETS_DIR/adapter_bearer.token"
SIGNER_KEYPAIR_FILE="$SECRETS_DIR/executor_signer_keypair.json"

cleanup() {
  if [[ -n "${EXECUTOR_PID-}" ]]; then
    if kill -0 "$EXECUTOR_PID" 2>/dev/null; then
      kill "$EXECUTOR_PID" 2>/dev/null || true
      wait "$EXECUTOR_PID" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT

write_fake_curl_executor_preflight() {
  local fake_bin_dir="$1"
  local token="$2"
  local script_path="$fake_bin_dir/curl"
  cat >"$script_path" <<'CURL_EOF'
#!/usr/bin/env bash
set -euo pipefail

expected_token='__EXPECTED_TOKEN__'
simulate_without_auth_status="${FAKE_EXECUTOR_SIMULATE_WITHOUT_AUTH_STATUS:-200}"
simulate_with_auth_status="${FAKE_EXECUTOR_SIMULATE_WITH_AUTH_STATUS:-200}"
simulate_invalid_auth_status="${FAKE_EXECUTOR_SIMULATE_INVALID_AUTH_STATUS:-200}"
health_enabled_routes_csv="${FAKE_EXECUTOR_HEALTH_ENABLED_ROUTES_CSV:-jito,paper,rpc}"
health_routes_alias_csv="${FAKE_EXECUTOR_HEALTH_ROUTES_CSV:-$health_enabled_routes_csv}"
health_send_rpc_enabled_routes_csv="${FAKE_EXECUTOR_HEALTH_SEND_RPC_ENABLED_ROUTES_CSV:-jito,rpc}"
health_send_rpc_fallback_routes_csv="${FAKE_EXECUTOR_HEALTH_SEND_RPC_FALLBACK_ROUTES_CSV:-jito}"
health_send_rpc_alias_routes_csv="${FAKE_EXECUTOR_HEALTH_SEND_RPC_ROUTES_CSV:-$health_send_rpc_enabled_routes_csv}"
health_signer_source="${FAKE_EXECUTOR_HEALTH_SIGNER_SOURCE:-file}"
health_signer_pubkey="${FAKE_EXECUTOR_HEALTH_SIGNER_PUBKEY:-11111111111111111111111111111111}"
health_submit_fastlane_enabled="${FAKE_EXECUTOR_HEALTH_SUBMIT_FASTLANE_ENABLED:-false}"
output_file=""
auth_header=""
url=""
status_code="200"
body='{"status":"not_found"}'

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

csv_to_json_array() {
  local csv="$1"
  local out=""
  local token normalized
  local -a values=()
  IFS=',' read -r -a values <<< "$csv"
  for token in "${values[@]-}"; do
    normalized="$(printf '%s' "$(trim_string "$token")" | tr '[:upper:]' '[:lower:]')"
    [[ -z "$normalized" ]] && continue
    out+="${out:+,}\"${normalized}\""
  done
  printf '[%s]' "$out"
}

while (($#)); do
  case "$1" in
    -o)
      output_file="$2"
      shift 2
      ;;
    -w|-d|--data|-X|-m|--connect-timeout)
      shift 2
      ;;
    -H)
      header="$2"
      header_lower="$(printf '%s' "$header" | tr '[:upper:]' '[:lower:]')"
      if [[ "$header_lower" == authorization:* ]]; then
        auth_header="${header#*:}"
        auth_header="${auth_header#"${auth_header%%[![:space:]]*}"}"
        auth_header="${auth_header%"${auth_header##*[![:space:]]}"}"
      fi
      shift 2
      ;;
    -s|-S)
      shift
      ;;
    http://*|https://*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if [[ "$url" == *"/healthz" ]]; then
  health_enabled_routes_json="$(csv_to_json_array "$health_enabled_routes_csv")"
  health_routes_alias_json="$(csv_to_json_array "$health_routes_alias_csv")"
  health_send_rpc_enabled_routes_json="$(csv_to_json_array "$health_send_rpc_enabled_routes_csv")"
  health_send_rpc_fallback_routes_json="$(csv_to_json_array "$health_send_rpc_fallback_routes_csv")"
  health_send_rpc_alias_routes_json="$(csv_to_json_array "$health_send_rpc_alias_routes_csv")"
  body="{\"status\":\"ok\",\"contract_version\":\"v1\",\"enabled_routes\":${health_enabled_routes_json},\"routes\":${health_routes_alias_json},\"signer_source\":\"${health_signer_source}\",\"submit_fastlane_enabled\":${health_submit_fastlane_enabled},\"signer_pubkey\":\"${health_signer_pubkey}\",\"idempotency_store_status\":\"ok\",\"send_rpc_enabled_routes\":${health_send_rpc_enabled_routes_json},\"send_rpc_fallback_routes\":${health_send_rpc_fallback_routes_json},\"send_rpc_routes\":${health_send_rpc_alias_routes_json}}"
  status_code="200"
elif [[ "$url" == *"/simulate" ]]; then
  if [[ -z "$auth_header" ]]; then
    code="auth_missing"
    status_code="$simulate_without_auth_status"
  elif [[ "$auth_header" == "Bearer ${expected_token}" ]]; then
    code="invalid_request"
    status_code="$simulate_with_auth_status"
  else
    code="auth_invalid"
    status_code="$simulate_invalid_auth_status"
  fi
  body="{\"status\":\"reject\",\"retryable\":false,\"code\":\"${code}\",\"detail\":\"local 6.1 dry-run probe\"}"
fi

if [[ -n "$output_file" ]]; then
  printf '%s' "$body" >"$output_file"
fi
printf '%s' "$status_code"
CURL_EOF

  python3 - "$script_path" "$token" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
token = sys.argv[2]
path.write_text(path.read_text().replace("__EXPECTED_TOKEN__", token))
PY

  chmod +x "$script_path"
}

printf '%s' "$TOKEN_VALUE" >"$BEARER_TOKEN_FILE"
printf '%s' "$TOKEN_VALUE" >"$ADAPTER_BEARER_TOKEN_FILE"
chmod 600 "$BEARER_TOKEN_FILE" "$ADAPTER_BEARER_TOKEN_FILE"

cat >"$SIGNER_KEYPAIR_FILE" <<'JSON'
[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
JSON
chmod 600 "$SIGNER_KEYPAIR_FILE"

cp "$ROOT_DIR/configs/live.toml" "$CONFIG_PATH_LOCAL"
perl -0pi -e 's/enabled = false/enabled = true/' "$CONFIG_PATH_LOCAL"
perl -0pi -e 's|submit_adapter_http_url = "https://REPLACE_ME"|submit_adapter_http_url = "http://127.0.0.1:8080/submit"|' "$CONFIG_PATH_LOCAL"
perl -0pi -e 's/execution_signer_pubkey = ""/execution_signer_pubkey = "11111111111111111111111111111111"/' "$CONFIG_PATH_LOCAL"

cat >"$EXECUTOR_ENV_PATH" <<EOF_ENV
COPYBOT_EXECUTOR_BIND_ADDR=127.0.0.1:8090
COPYBOT_EXECUTOR_CONTRACT_VERSION=v1
COPYBOT_EXECUTOR_ROUTE_ALLOWLIST=jito,rpc
COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=false
COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=false
COPYBOT_EXECUTOR_SIGNER_SOURCE=file
COPYBOT_EXECUTOR_SIGNER_PUBKEY=$PUBKEY
COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE=$SIGNER_KEYPAIR_FILE
COPYBOT_EXECUTOR_BEARER_TOKEN_FILE=$BEARER_TOKEN_FILE
COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL=https://example.com/submit
COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL=https://example.com/simulate
EOF_ENV

cat >"$ADAPTER_ENV_PATH" <<EOF_ENV
COPYBOT_ADAPTER_BIND_ADDR=127.0.0.1:8080
COPYBOT_ADAPTER_CONTRACT_VERSION=v1
COPYBOT_ADAPTER_SIGNER_PUBKEY=$PUBKEY
COPYBOT_ADAPTER_ROUTE_ALLOWLIST=jito,rpc
COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=false
COPYBOT_ADAPTER_BEARER_TOKEN_FILE=$ADAPTER_BEARER_TOKEN_FILE
COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL=http://127.0.0.1:8090/submit
COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL=http://127.0.0.1:8090/simulate
COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE=$BEARER_TOKEN_FILE
EOF_ENV

cat >"$APP_ENV_PATH" <<EOF_ENV
SOLANA_COPY_BOT_CONFIG=$CONFIG_PATH_LOCAL
SOLANA_COPY_BOT_EXECUTION_ENABLED=true
EOF_ENV

executor_mode="fake_curl"
if [[ "$RUN_REAL_EXECUTOR" == "true" ]]; then
  executor_mode="real_executor"
  (
    cd "$ROOT_DIR"
    set -a
    source "$EXECUTOR_ENV_PATH"
    set +a
    cargo run -p copybot-executor -q >"$EXECUTOR_LOG" 2>&1
  ) &
  EXECUTOR_PID=$!

  health_ready="false"
  for _i in $(seq 1 120); do
    if curl -sS "http://127.0.0.1:8090/healthz" >"$HEALTHZ_JSON"; then
      health_ready="true"
      break
    fi
    sleep 1
  done
  if [[ "$health_ready" != "true" ]]; then
    echo "executor healthz did not become ready; see $EXECUTOR_LOG" >&2
    exit 1
  fi
else
  write_fake_curl_executor_preflight "$FAKE_BIN_DIR" "$TOKEN_VALUE"
  PATH="$FAKE_BIN_DIR:$PATH" \
  FAKE_EXECUTOR_HEALTH_ENABLED_ROUTES_CSV="jito,rpc" \
  FAKE_EXECUTOR_HEALTH_ROUTES_CSV="jito,rpc" \
  FAKE_EXECUTOR_HEALTH_SEND_RPC_ENABLED_ROUTES_CSV=" " \
  FAKE_EXECUTOR_HEALTH_SEND_RPC_FALLBACK_ROUTES_CSV=" " \
  FAKE_EXECUTOR_HEALTH_SEND_RPC_ROUTES_CSV=" " \
  FAKE_EXECUTOR_HEALTH_SIGNER_SOURCE="file" \
  FAKE_EXECUTOR_HEALTH_SIGNER_PUBKEY="$PUBKEY" \
  FAKE_EXECUTOR_HEALTH_SUBMIT_FASTLANE_ENABLED="false" \
  curl -sS -o "$HEALTHZ_JSON" -w "%{http_code}" "http://127.0.0.1:8090/healthz" >/dev/null
fi

(
  cd "$ROOT_DIR"
  CONFIG_PATH="$CONFIG_PATH_LOCAL" \
  SOLANA_COPY_BOT_EXECUTION_ENABLED=true \
  bash tools/execution_adapter_preflight.sh
) >"$ADAPTER_PREFLIGHT_OUT" 2>&1

(
  cd "$ROOT_DIR"
  CONFIG_PATH="$CONFIG_PATH_LOCAL" \
  EXECUTOR_ENV_PATH="$EXECUTOR_ENV_PATH" \
  ADAPTER_ENV_PATH="$ADAPTER_ENV_PATH" \
  SOLANA_COPY_BOT_EXECUTION_ENABLED=true \
  PATH="$FAKE_BIN_DIR:$PATH" \
  FAKE_EXECUTOR_HEALTH_ENABLED_ROUTES_CSV="jito,rpc" \
  FAKE_EXECUTOR_HEALTH_ROUTES_CSV="jito,rpc" \
  FAKE_EXECUTOR_HEALTH_SEND_RPC_ENABLED_ROUTES_CSV=" " \
  FAKE_EXECUTOR_HEALTH_SEND_RPC_FALLBACK_ROUTES_CSV=" " \
  FAKE_EXECUTOR_HEALTH_SEND_RPC_ROUTES_CSV=" " \
  FAKE_EXECUTOR_HEALTH_SIGNER_SOURCE="file" \
  FAKE_EXECUTOR_HEALTH_SIGNER_PUBKEY="$PUBKEY" \
  FAKE_EXECUTOR_HEALTH_SUBMIT_FASTLANE_ENABLED="false" \
  bash tools/executor_preflight.sh
) >"$EXECUTOR_PREFLIGHT_OUT" 2>&1

adapter_verdict="$(awk -F': ' '/^preflight_verdict:/ {print $2}' "$ADAPTER_PREFLIGHT_OUT" | tail -1)"
executor_verdict="$(awk -F': ' '/^preflight_verdict:/ {print $2}' "$EXECUTOR_PREFLIGHT_OUT" | tail -1)"
health_status="$(jq -r '.status // "missing"' "$HEALTHZ_JSON")"

cat >"$SUMMARY_OUT" <<EOF_SUM
6.1 local dry-run summary
run_dir: $RUN_DIR
executor_mode: $executor_mode
config_path: $CONFIG_PATH_LOCAL
executor_env: $EXECUTOR_ENV_PATH
adapter_env: $ADAPTER_ENV_PATH

execution_adapter_preflight_verdict: $adapter_verdict
executor_preflight_verdict: $executor_verdict
executor_healthz_status: $health_status

artifacts:
- $ADAPTER_PREFLIGHT_OUT
- $EXECUTOR_PREFLIGHT_OUT
- $HEALTHZ_JSON
- $EXECUTOR_LOG
EOF_SUM

cat "$SUMMARY_OUT"

if [[ "$adapter_verdict" != "PASS" || "$executor_verdict" != "PASS" || "$health_status" != "ok" ]]; then
  echo "6.1 local dry-run failed (see summary/artifacts in $RUN_DIR)" >&2
  exit 1
fi

echo "6.1 local dry-run PASS"
