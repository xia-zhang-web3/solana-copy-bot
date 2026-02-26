#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"
EXECUTOR_ENV_PATH="${EXECUTOR_ENV_PATH:-/etc/solana-copy-bot/executor.env}"
ADAPTER_ENV_PATH="${ADAPTER_ENV_PATH:-/etc/solana-copy-bot/adapter.env}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
HTTP_TIMEOUT_SEC="${HTTP_TIMEOUT_SEC:-4}"
EXECUTOR_HEALTH_URL="${EXECUTOR_HEALTH_URL:-}"
EXECUTOR_EXPECT_SUBMIT_URL="${EXECUTOR_EXPECT_SUBMIT_URL:-}"
EXECUTOR_EXPECT_SIMULATE_URL="${EXECUTOR_EXPECT_SIMULATE_URL:-}"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
fi

declare -a errors=()
PYTHON3_BIN="$(command -v python3 || true)"
if [[ -z "$PYTHON3_BIN" ]]; then
  errors+=("python3 is required for executor_preflight URL/JSON helpers")
fi

cfg_value() {
  local section="$1"
  local key="$2"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      line = $0
      sub(/#.*/, "", line)
      left = line
      sub(/=.*/, "", left)
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = line
        sub(/^[^=]*=/, "", value)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "$CONFIG_PATH"
}

parse_bool_token() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on)
      printf 'true'
      ;;
    0|false|no|off)
      printf 'false'
      ;;
    *)
      printf ''
      ;;
  esac
}

cfg_or_env_bool_into() {
  local section="$1"
  local key="$2"
  local env_key="$3"
  local default_value="$4"
  local output_var="$5"
  local env_raw cfg_raw parsed
  env_raw="$(trim_string "${!env_key-}")"
  if [[ -n "$env_raw" ]]; then
    parsed="$(parse_bool_token "$env_raw")"
    if [[ -z "$parsed" ]]; then
      errors+=("$env_key must be boolean token (true/false/1/0/yes/no/on/off), got: $env_raw")
      printf -v "$output_var" '%s' "$default_value"
      return
    fi
    printf -v "$output_var" '%s' "$parsed"
    return
  fi
  cfg_raw="$(trim_string "$(cfg_value "$section" "$key")")"
  if [[ -z "$cfg_raw" ]]; then
    printf -v "$output_var" '%s' "$default_value"
    return
  fi
  parsed="$(parse_bool_token "$cfg_raw")"
  if [[ -z "$parsed" ]]; then
    errors+=("config [$section].$key must be boolean token (true/false/1/0/yes/no/on/off), got: $cfg_raw")
    printf -v "$output_var" '%s' "$default_value"
    return
  fi
  printf -v "$output_var" '%s' "$parsed"
}

cfg_or_env_string() {
  local section="$1"
  local key="$2"
  local env_key="$3"
  local default_value="${4:-}"
  local env_raw cfg_raw
  env_raw="$(trim_string "${!env_key-}")"
  if [[ -n "$env_raw" ]]; then
    printf '%s' "$env_raw"
    return
  fi
  cfg_raw="$(trim_string "$(cfg_value "$section" "$key")")"
  if [[ -n "$cfg_raw" ]]; then
    printf '%s' "$cfg_raw"
    return
  fi
  printf '%s' "$default_value"
}

env_file_value() {
  local env_file="$1"
  local key="$2"
  if [[ ! -f "$env_file" ]]; then
    printf ''
    return
  fi
  awk -v wanted_key="$key" '
    function trim(s) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", s)
      return s
    }
    function strip_comment(s,    out, i, ch, in_single, in_double, sq) {
      out = ""
      in_single = 0
      in_double = 0
      sq = sprintf("%c", 39)
      for (i = 1; i <= length(s); i++) {
        ch = substr(s, i, 1)
        if (ch == "\"" && !in_single) {
          in_double = !in_double
          out = out ch
          continue
        }
        if (ch == sq && !in_double) {
          in_single = !in_single
          out = out ch
          continue
        }
        if (ch == "#" && !in_single && !in_double) {
          break
        }
        out = out ch
      }
      return out
    }
    function unquote(s, first, last) {
      first = substr(s, 1, 1)
      last = substr(s, length(s), 1)
      if (length(s) >= 2 && ((first == "\"" && last == "\"") || (first == "\x27" && last == "\x27"))) {
        return substr(s, 2, length(s) - 2)
      }
      return s
    }
    {
      line = strip_comment($0)
      line = trim(line)
      if (line == "") {
        next
      }
      if (line ~ /^export[[:space:]]+/) {
        sub(/^export[[:space:]]+/, "", line)
        line = trim(line)
      }
      eq = index(line, "=")
      if (eq == 0) {
        next
      }
      k = trim(substr(line, 1, eq - 1))
      if (k != wanted_key) {
        next
      }
      value = trim(substr(line, eq + 1))
      value = unquote(value)
      found = 1
      last = value
    }
    END {
      if (found) {
        print last
      }
    }
  ' "$env_file"
}

env_or_file_value() {
  local env_file="$1"
  local key="$2"
  local value
  value="$(trim_string "${!key-}")"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return
  fi
  printf '%s' "$(trim_string "$(env_file_value "$env_file" "$key")")"
}

resolve_path_from_env_file() {
  local env_file="$1"
  local raw_path="$2"
  if [[ "$raw_path" = /* ]]; then
    printf '%s' "$raw_path"
    return
  fi
  local base_dir
  base_dir="$(cd "$(dirname "$env_file")" && pwd)"
  printf '%s/%s' "$base_dir" "$raw_path"
}

read_secret_from_source() {
  local env_file="$1"
  local inline_key="$2"
  local file_key="$3"
  local label="$4"
  local inline_value file_raw file_resolved file_value
  inline_value="$(env_or_file_value "$env_file" "$inline_key")"
  file_raw="$(env_or_file_value "$env_file" "$file_key")"
  if [[ -n "$inline_value" && -n "$file_raw" ]]; then
    errors+=("$label: $inline_key and $file_key cannot both be set")
    printf '%s' "$inline_value"
    return
  fi
  if [[ -n "$file_raw" ]]; then
    file_resolved="$(resolve_path_from_env_file "$env_file" "$file_raw")"
    if [[ ! -f "$file_resolved" ]]; then
      errors+=("$label: $file_key file not found: $file_resolved")
      printf ''
      return
    fi
    file_value="$(cat "$file_resolved" 2>/dev/null)" || {
      errors+=("$label: $file_key file unreadable: $file_resolved")
      printf ''
      return
    }
    file_value="$(trim_string "$file_value")"
    if [[ -z "$file_value" ]]; then
      errors+=("$label: $file_key file is empty after trim: $file_resolved")
      printf ''
      return
    fi
    printf '%s' "$file_value"
    return
  fi
  printf '%s' "$inline_value"
}

normalize_route_token() {
  local raw
  raw="$(trim_string "$1")"
  printf '%s' "$raw" | tr '[:upper:]' '[:lower:]'
}

normalized_routes_lines() {
  local csv="$1"
  if [[ -z "${csv//[[:space:]]/}" ]]; then
    return
  fi
  local seen=""
  local raw normalized
  local -a values=()
  IFS=',' read -r -a values <<< "$csv"
  for raw in "${values[@]}"; do
    normalized="$(normalize_route_token "$raw")"
    [[ -z "$normalized" ]] && continue
    if printf '%s\n' "$seen" | grep -Fqx -- "$normalized"; then
      continue
    fi
    seen+="${normalized}"$'\n'
    printf '%s\n' "$normalized"
  done
}

csv_contains_route() {
  local csv="$1"
  local needle="$2"
  local route
  while IFS= read -r route; do
    if [[ "$route" == "$needle" ]]; then
      return 0
    fi
  done < <(normalized_routes_lines "$csv")
  return 1
}

endpoint_identity() {
  local endpoint="$1"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf ''
    return 0
  fi
  "$PYTHON3_BIN" - "$endpoint" <<'PY'
import sys
from urllib.parse import urlsplit

endpoint = sys.argv[1].strip()
parsed = urlsplit(endpoint)
scheme = parsed.scheme.lower()
host = (parsed.hostname or "").lower()
if parsed.port is not None:
    port = parsed.port
elif scheme == "http":
    port = 80
elif scheme == "https":
    port = 443
else:
    port = ""
path = parsed.path if parsed.path else "/"
print(f"{scheme}://{host}:{port}{path}")
PY
}

url_from_bind_addr() {
  local bind_addr="$1"
  local path="$2"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf ''
    return 0
  fi
  "$PYTHON3_BIN" - "$bind_addr" "$path" <<'PY'
import sys

bind = sys.argv[1].strip()
path = sys.argv[2]
if not bind:
    raise SystemExit("bind address is empty")

host = ""
port = ""
if bind.startswith("["):
    if "]:" not in bind:
        raise SystemExit("bind address must be [host]:port for ipv6")
    host, port = bind[1:].split("]:", 1)
else:
    if ":" not in bind:
        raise SystemExit("bind address must be host:port")
    host, port = bind.rsplit(":", 1)

host = host.strip()
port = port.strip()
if not host or not port:
    raise SystemExit("bind address host/port cannot be empty")
if host in {"0.0.0.0", "::", "::0", "[::]"}:
    host = "127.0.0.1"
if ":" in host and not host.startswith("["):
    host = f"[{host}]"
print(f"http://{host}:{port}/{path}")
PY
}

json_string_field() {
  local body="$1"
  local key="$2"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf ''
    return 0
  fi
  "$PYTHON3_BIN" - "$key" "$body" <<'PY'
import json
import sys

key = sys.argv[1]
raw = (sys.argv[2] or "").strip()
if not raw:
    print("")
    raise SystemExit(0)
try:
    data = json.loads(raw)
except Exception:
    print("")
    raise SystemExit(0)
value = data.get(key)
if isinstance(value, str):
    print(value)
elif value is None:
    print("")
else:
    print(str(value))
PY
}

json_routes_csv_field() {
  local body="$1"
  local key="$2"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf ''
    return 0
  fi
  "$PYTHON3_BIN" - "$key" "$body" <<'PY'
import json
import sys

key = sys.argv[1]
raw = (sys.argv[2] or "").strip()
if not raw:
    print("")
    raise SystemExit(0)
try:
    data = json.loads(raw)
except Exception:
    print("")
    raise SystemExit(0)
value = data.get(key)
if not isinstance(value, list):
    print("")
    raise SystemExit(0)
out = []
seen = set()
for item in value:
    token = str(item).strip().lower()
    if not token or token in seen:
        continue
    seen.add(token)
    out.append(token)
print(",".join(out))
PY
}

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

cfg_or_env_bool_into execution enabled SOLANA_COPY_BOT_EXECUTION_ENABLED false execution_enabled
execution_mode="$(normalize_route_token "$(cfg_or_env_string execution mode SOLANA_COPY_BOT_EXECUTION_MODE paper)")"

if (( ${#errors[@]} > 0 )); then
  cat <<EOF
=== Executor Preflight ===
config: $CONFIG_PATH
timestamp_utc: $timestamp_utc
execution_enabled: $execution_enabled
execution_mode: ${execution_mode:-paper}
preflight_verdict: FAIL
preflight_reason_code: config_error
preflight_reason: bool parsing failed before preflight gate evaluation
error_count: ${#errors[@]}
artifacts_written: false
EOF
  for error in "${errors[@]}"; do
    echo "error: $error"
  done
  exit 1
fi

if [[ "$execution_enabled" != "true" ]]; then
  cat <<EOF
=== Executor Preflight ===
config: $CONFIG_PATH
timestamp_utc: $timestamp_utc
execution_enabled: $execution_enabled
execution_mode: ${execution_mode:-paper}
preflight_verdict: SKIP
preflight_reason_code: execution_disabled
preflight_reason: execution.enabled is not true
artifacts_written: false
EOF
  exit 0
fi

if [[ "$execution_mode" != "adapter_submit_confirm" ]]; then
  cat <<EOF
=== Executor Preflight ===
config: $CONFIG_PATH
timestamp_utc: $timestamp_utc
execution_enabled: $execution_enabled
execution_mode: ${execution_mode:-paper}
preflight_verdict: SKIP
preflight_reason_code: requires_adapter_mode
preflight_reason: execution.mode is not adapter_submit_confirm
artifacts_written: false
EOF
  exit 0
fi

if ! [[ "$HTTP_TIMEOUT_SEC" =~ ^[0-9]+$ ]] || (( HTTP_TIMEOUT_SEC < 1 || HTTP_TIMEOUT_SEC > 60 )); then
  errors+=("HTTP_TIMEOUT_SEC must be integer in 1..=60")
fi

if [[ ! -f "$EXECUTOR_ENV_PATH" ]]; then
  errors+=("executor env file not found: $EXECUTOR_ENV_PATH")
fi
if [[ ! -f "$ADAPTER_ENV_PATH" ]]; then
  errors+=("adapter env file not found: $ADAPTER_ENV_PATH")
fi

executor_bind_addr="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_BIND_ADDR)" "127.0.0.1:8090")"
executor_contract_version_expected="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_CONTRACT_VERSION)" "v1")"
executor_route_allowlist_csv="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_ROUTE_ALLOWLIST)" "paper,rpc,jito")"
executor_submit_fastlane_enabled_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED)" "false")"
executor_allow_unauthenticated_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED)" "false")"
executor_upstream_submit_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL)"
executor_upstream_simulate_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL)"
executor_signer_pubkey="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SIGNER_PUBKEY)"

executor_submit_fastlane_enabled="$(parse_bool_token "$executor_submit_fastlane_enabled_raw")"
if [[ -z "$executor_submit_fastlane_enabled" ]]; then
  errors+=("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED must be boolean token")
  executor_submit_fastlane_enabled="false"
fi
executor_allow_unauthenticated="$(parse_bool_token "$executor_allow_unauthenticated_raw")"
if [[ -z "$executor_allow_unauthenticated" ]]; then
  errors+=("COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED must be boolean token")
  executor_allow_unauthenticated="false"
fi

if [[ -z "$executor_signer_pubkey" ]]; then
  errors+=("COPYBOT_EXECUTOR_SIGNER_PUBKEY must be non-empty")
fi

if [[ -z "${executor_route_allowlist_csv//[[:space:]]/}" ]]; then
  errors+=("COPYBOT_EXECUTOR_ROUTE_ALLOWLIST must not be empty")
fi

while IFS= read -r route; do
  [[ -z "$route" ]] && continue
  route_upper="$(printf '%s' "$route" | tr '[:lower:]' '[:upper:]')"
  route_submit="$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SUBMIT_URL")"
  route_simulate="$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SIMULATE_URL")"
  if [[ -z "$route_submit" && -z "$executor_upstream_submit_default" ]]; then
    errors+=("missing submit backend URL for executor route=$route (set COPYBOT_EXECUTOR_ROUTE_${route_upper}_SUBMIT_URL or COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL)")
  fi
  if [[ -z "$route_simulate" && -z "$executor_upstream_simulate_default" ]]; then
    errors+=("missing simulate backend URL for executor route=$route (set COPYBOT_EXECUTOR_ROUTE_${route_upper}_SIMULATE_URL or COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL)")
  fi
done < <(normalized_routes_lines "$executor_route_allowlist_csv")

executor_bearer_token="$(read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_BEARER_TOKEN COPYBOT_EXECUTOR_BEARER_TOKEN_FILE "executor ingress auth")"
executor_hmac_key_id="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_HMAC_KEY_ID)"
executor_hmac_secret="$(read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_HMAC_SECRET COPYBOT_EXECUTOR_HMAC_SECRET_FILE "executor hmac")"
executor_hmac_ttl_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_HMAC_TTL_SEC)" "30")"

if [[ -n "$executor_hmac_key_id" && -z "$executor_hmac_secret" ]]; then
  errors+=("COPYBOT_EXECUTOR_HMAC_KEY_ID requires COPYBOT_EXECUTOR_HMAC_SECRET")
fi
if [[ -z "$executor_hmac_key_id" && -n "$executor_hmac_secret" ]]; then
  errors+=("COPYBOT_EXECUTOR_HMAC_SECRET requires COPYBOT_EXECUTOR_HMAC_KEY_ID")
fi
if [[ -n "$executor_hmac_key_id" ]]; then
  if ! [[ "$executor_hmac_ttl_raw" =~ ^[0-9]+$ ]]; then
    errors+=("COPYBOT_EXECUTOR_HMAC_TTL_SEC must be integer when HMAC is configured")
  elif (( executor_hmac_ttl_raw < 5 || executor_hmac_ttl_raw > 300 )); then
    errors+=("COPYBOT_EXECUTOR_HMAC_TTL_SEC must be in 5..=300 when HMAC is configured")
  fi
fi

executor_bearer_required="false"
if [[ "$executor_allow_unauthenticated" != "true" ]]; then
  executor_bearer_required="true"
fi
if [[ "$executor_bearer_required" == "true" && -z "$executor_bearer_token" ]]; then
  errors+=("COPYBOT_EXECUTOR_BEARER_TOKEN or COPYBOT_EXECUTOR_BEARER_TOKEN_FILE must be set when COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=false")
fi

if [[ -z "$EXECUTOR_EXPECT_SUBMIT_URL" ]]; then
  if ! EXECUTOR_EXPECT_SUBMIT_URL="$(url_from_bind_addr "$executor_bind_addr" "submit" 2>/dev/null)"; then
    errors+=("failed to derive expected submit URL from COPYBOT_EXECUTOR_BIND_ADDR=$executor_bind_addr")
    EXECUTOR_EXPECT_SUBMIT_URL=""
  fi
fi
if [[ -z "$EXECUTOR_EXPECT_SIMULATE_URL" ]]; then
  if ! EXECUTOR_EXPECT_SIMULATE_URL="$(url_from_bind_addr "$executor_bind_addr" "simulate" 2>/dev/null)"; then
    errors+=("failed to derive expected simulate URL from COPYBOT_EXECUTOR_BIND_ADDR=$executor_bind_addr")
    EXECUTOR_EXPECT_SIMULATE_URL=""
  fi
fi

if [[ -z "$EXECUTOR_HEALTH_URL" ]]; then
  if ! EXECUTOR_HEALTH_URL="$(url_from_bind_addr "$executor_bind_addr" "healthz" 2>/dev/null)"; then
    errors+=("failed to derive health URL from COPYBOT_EXECUTOR_BIND_ADDR=$executor_bind_addr")
    EXECUTOR_HEALTH_URL=""
  fi
fi

adapter_route_allowlist_csv="$(first_non_empty "$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_ROUTE_ALLOWLIST)" "paper,rpc,fastlane,jito")"
adapter_submit_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL)"
adapter_simulate_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL)"
adapter_auth_default="$(read_secret_from_source "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE "adapter upstream auth")"

expected_submit_identity=""
expected_simulate_identity=""
if [[ -n "$EXECUTOR_EXPECT_SUBMIT_URL" ]]; then
  if ! expected_submit_identity="$(endpoint_identity "$EXECUTOR_EXPECT_SUBMIT_URL" 2>/dev/null)"; then
    errors+=("invalid expected submit URL: $EXECUTOR_EXPECT_SUBMIT_URL")
    expected_submit_identity=""
  fi
fi
if [[ -n "$EXECUTOR_EXPECT_SIMULATE_URL" ]]; then
  if ! expected_simulate_identity="$(endpoint_identity "$EXECUTOR_EXPECT_SIMULATE_URL" 2>/dev/null)"; then
    errors+=("invalid expected simulate URL: $EXECUTOR_EXPECT_SIMULATE_URL")
    expected_simulate_identity=""
  fi
fi

while IFS= read -r route; do
  [[ -z "$route" ]] && continue
  route_upper="$(printf '%s' "$route" | tr '[:lower:]' '[:upper:]')"

  if ! csv_contains_route "$executor_route_allowlist_csv" "$route"; then
    errors+=("adapter route allowlist includes route=$route that is not present in executor allowlist")
  fi

  adapter_route_submit="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SUBMIT_URL")" \
    "$adapter_submit_default")"
  adapter_route_simulate="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SIMULATE_URL")" \
    "$adapter_simulate_default")"

  if [[ -z "$adapter_route_submit" ]]; then
    errors+=("missing adapter submit upstream URL for route=$route (set COPYBOT_ADAPTER_ROUTE_${route_upper}_SUBMIT_URL or COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL)")
  fi
  if [[ -z "$adapter_route_simulate" ]]; then
    errors+=("missing adapter simulate upstream URL for route=$route (set COPYBOT_ADAPTER_ROUTE_${route_upper}_SIMULATE_URL or COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL)")
  fi

  if [[ -n "$adapter_route_submit" && -n "$expected_submit_identity" ]]; then
    if ! submit_identity="$(endpoint_identity "$adapter_route_submit" 2>/dev/null)"; then
      errors+=("invalid adapter submit URL for route=$route: $adapter_route_submit")
    elif [[ "$submit_identity" != "$expected_submit_identity" ]]; then
      errors+=("adapter submit URL for route=$route must target executor submit endpoint ($EXECUTOR_EXPECT_SUBMIT_URL)")
    fi
  fi
  if [[ -n "$adapter_route_simulate" && -n "$expected_simulate_identity" ]]; then
    if ! simulate_identity="$(endpoint_identity "$adapter_route_simulate" 2>/dev/null)"; then
      errors+=("invalid adapter simulate URL for route=$route: $adapter_route_simulate")
    elif [[ "$simulate_identity" != "$expected_simulate_identity" ]]; then
      errors+=("adapter simulate URL for route=$route must target executor simulate endpoint ($EXECUTOR_EXPECT_SIMULATE_URL)")
    fi
  fi

  if [[ "$executor_bearer_required" == "true" ]]; then
    route_auth_token="$(first_non_empty \
      "$(read_secret_from_source "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_AUTH_TOKEN" "COPYBOT_ADAPTER_ROUTE_${route_upper}_AUTH_TOKEN_FILE" "adapter route auth ($route)")" \
      "$adapter_auth_default")"
    if [[ -z "$route_auth_token" ]]; then
      errors+=("adapter auth token missing for route=$route while executor bearer auth is required")
    elif [[ -n "$executor_bearer_token" && "$route_auth_token" != "$executor_bearer_token" ]]; then
      errors+=("adapter auth token mismatch for route=$route vs executor bearer token")
    fi
  fi
done < <(normalized_routes_lines "$adapter_route_allowlist_csv")

health_http_status="n/a"
health_status_field="n/a"
health_contract_version="n/a"
health_routes_csv="n/a"
idempotency_store_status="n/a"
auth_probe_without_auth_code="n/a"
auth_probe_with_auth_code="n/a"
auth_probe_with_auth_http_status="n/a"

if command -v curl >/dev/null 2>&1; then
  if [[ -n "$EXECUTOR_HEALTH_URL" ]]; then
    health_body_file="$(mktemp)"
    if health_http_status="$(curl -sS -m "$HTTP_TIMEOUT_SEC" -o "$health_body_file" -w "%{http_code}" "$EXECUTOR_HEALTH_URL" 2>/dev/null)"; then
      health_body="$(cat "$health_body_file")"
      health_status_field="$(json_string_field "$health_body" "status")"
      health_contract_version="$(json_string_field "$health_body" "contract_version")"
      idempotency_store_status="$(json_string_field "$health_body" "idempotency_store_status")"
      health_routes_csv="$(json_routes_csv_field "$health_body" "enabled_routes")"
      if [[ -z "$health_routes_csv" ]]; then
        health_routes_csv="$(json_routes_csv_field "$health_body" "routes")"
      fi
      if [[ "$health_http_status" != "200" ]]; then
        errors+=("executor health endpoint returned HTTP $health_http_status")
      fi
      if [[ "$health_status_field" != "ok" ]]; then
        errors+=("executor health status must be ok, got: ${health_status_field:-<empty>}")
      fi
      if [[ "$idempotency_store_status" != "ok" ]]; then
        errors+=("executor idempotency_store_status must be ok, got: ${idempotency_store_status:-<empty>}")
      fi
      if [[ "$health_contract_version" != "$executor_contract_version_expected" ]]; then
        errors+=("executor contract_version mismatch: health=$health_contract_version expected=$executor_contract_version_expected")
      fi
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$health_routes_csv" "$route"; then
          errors+=("health enabled_routes missing executor route=$route")
        fi
      done < <(normalized_routes_lines "$executor_route_allowlist_csv")
    else
      errors+=("failed to query executor health endpoint: $EXECUTOR_HEALTH_URL")
    fi
    rm -f "$health_body_file"

    simulate_probe_payload='{}'
    probe_body_file="$(mktemp)"
    probe_url="${EXECUTOR_EXPECT_SIMULATE_URL:-}"
    if [[ -n "$probe_url" ]]; then
      if probe_http_status="$(curl -sS -m "$HTTP_TIMEOUT_SEC" -H "content-type: application/json" --data "$simulate_probe_payload" -o "$probe_body_file" -w "%{http_code}" "$probe_url" 2>/dev/null)"; then
        probe_body="$(cat "$probe_body_file")"
        auth_probe_without_auth_code="$(json_string_field "$probe_body" "code")"
        if [[ "$probe_http_status" != "200" ]]; then
          errors+=("auth probe without token must return HTTP 200, got $probe_http_status")
        fi
        if [[ "$executor_bearer_required" == "true" ]]; then
          if [[ "$auth_probe_without_auth_code" != "auth_missing" && "$auth_probe_without_auth_code" != "auth_invalid" ]]; then
            errors+=("auth probe without token must fail with auth_missing/auth_invalid when bearer is required")
          fi
        else
          if [[ "$auth_probe_without_auth_code" == "auth_missing" || "$auth_probe_without_auth_code" == "auth_invalid" ]]; then
            errors+=("executor is configured with COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true but simulate endpoint still requires auth")
          fi
        fi
      else
        errors+=("failed auth probe without token against simulate endpoint: $probe_url")
      fi

      if [[ -n "$executor_bearer_token" ]]; then
        if probe_with_auth_status="$(curl -sS -m "$HTTP_TIMEOUT_SEC" -H "content-type: application/json" -H "authorization: Bearer $executor_bearer_token" --data "$simulate_probe_payload" -o "$probe_body_file" -w "%{http_code}" "$probe_url" 2>/dev/null)"; then
          probe_with_auth_body="$(cat "$probe_body_file")"
          auth_probe_with_auth_http_status="$probe_with_auth_status"
          auth_probe_with_auth_code="$(json_string_field "$probe_with_auth_body" "code")"
          if [[ "$probe_with_auth_status" != "200" ]]; then
            errors+=("auth probe with configured bearer token must return HTTP 200, got $probe_with_auth_status")
          fi
          if [[ "$auth_probe_with_auth_code" == "auth_missing" || "$auth_probe_with_auth_code" == "auth_invalid" ]]; then
            errors+=("auth probe with configured bearer token still failed auth check")
          fi
        else
          errors+=("failed auth probe with bearer token against simulate endpoint: $probe_url")
        fi
      fi
    fi
    rm -f "$probe_body_file"
  fi
else
  errors+=("curl is required for executor_preflight endpoint/auth probes")
fi

preflight_verdict="PASS"
preflight_reason_code="checks_passed"
preflight_reason="executor contract, endpoint, and auth checks passed"
exit_code=0
if ((${#errors[@]} > 0)); then
  preflight_verdict="FAIL"
  preflight_reason_code="contract_checks_failed"
  preflight_reason="executor preflight checks failed"
  exit_code=1
fi

artifacts_written="false"
if [[ -n "$OUTPUT_DIR" ]]; then
  artifacts_written="true"
fi

summary="$({
  echo "=== Executor Preflight ==="
  echo "config: $CONFIG_PATH"
  echo "executor_env_path: $EXECUTOR_ENV_PATH"
  echo "adapter_env_path: $ADAPTER_ENV_PATH"
  echo "timestamp_utc: $timestamp_utc"
  echo "execution_enabled: $execution_enabled"
  echo "execution_mode: $execution_mode"
  echo "executor_bind_addr: $executor_bind_addr"
  echo "executor_health_url: $EXECUTOR_HEALTH_URL"
  echo "executor_expected_submit_url: $EXECUTOR_EXPECT_SUBMIT_URL"
  echo "executor_expected_simulate_url: $EXECUTOR_EXPECT_SIMULATE_URL"
  echo "executor_contract_version_expected: $executor_contract_version_expected"
  echo "executor_route_allowlist_csv: $executor_route_allowlist_csv"
  echo "executor_submit_fastlane_enabled: $executor_submit_fastlane_enabled"
  echo "executor_bearer_required: $executor_bearer_required"
  echo "health_http_status: $health_http_status"
  echo "health_status: $health_status_field"
  echo "health_contract_version: $health_contract_version"
  echo "health_routes_csv: $health_routes_csv"
  echo "idempotency_store_status: $idempotency_store_status"
  echo "auth_probe_without_auth_code: $auth_probe_without_auth_code"
  echo "auth_probe_with_auth_http_status: $auth_probe_with_auth_http_status"
  echo "auth_probe_with_auth_code: $auth_probe_with_auth_code"
  echo "preflight_verdict: $preflight_verdict"
  echo "preflight_reason_code: $preflight_reason_code"
  echo "preflight_reason: $preflight_reason"
  echo "artifacts_written: $artifacts_written"
  if ((${#errors[@]} > 0)); then
    echo "error_count: ${#errors[@]}"
    for error in "${errors[@]}"; do
      echo "error: $error"
    done
  fi
})"

echo "$summary"

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  summary_path="$OUTPUT_DIR/executor_preflight_summary_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/executor_preflight_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary" >"$summary_path"
  summary_sha256="$(sha256_file_value "$summary_path")"
  cat >"$manifest_path" <<EOF
summary_sha256: $summary_sha256
EOF
  echo "artifact_summary: $summary_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
fi

exit "$exit_code"
