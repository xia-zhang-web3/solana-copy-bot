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

default_executor_mock_backend_url() {
  local route="$1"
  local action="$2"
  printf 'https://executor.mock.local/%s/%s' "$route" "$action"
}

default_executor_internal_paper_backend_url() {
  local route="$1"
  local action="$2"
  printf 'https://executor.paper.local/%s/%s' "$route" "$action"
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
  local output_var="${5-}"
  local inline_value file_raw file_resolved file_value secret_value file_read_error
  secret_value=""
  inline_value="$(env_or_file_value "$env_file" "$inline_key")"
  file_raw="$(env_or_file_value "$env_file" "$file_key")"
  if [[ -n "$inline_value" && -n "$file_raw" ]]; then
    errors+=("$label: $inline_key and $file_key cannot both be set")
    secret_value="$inline_value"
  elif [[ -n "$file_raw" ]]; then
    file_resolved="$(resolve_path_from_env_file "$env_file" "$file_raw")"
    if [[ ! -f "$file_resolved" ]]; then
      errors+=("$label: $file_key file not found: $file_resolved")
    else
      file_read_error="false"
      file_value="$(cat "$file_resolved" 2>/dev/null)" || {
        errors+=("$label: $file_key file unreadable: $file_resolved")
        file_read_error="true"
        file_value=""
      }
      file_value="$(trim_string "$file_value")"
      if [[ "$file_read_error" != "true" && -z "$file_value" ]]; then
        errors+=("$label: $file_key file is empty after trim: $file_resolved")
      else
        secret_value="$file_value"
      fi
    fi
  else
    secret_value="$inline_value"
  fi

  if [[ -n "$output_var" ]]; then
    printf -v "$output_var" '%s' "$secret_value"
    return 0
  fi
  printf '%s' "$secret_value"
}

normalize_route_token() {
  local raw
  raw="$(trim_string "$1")"
  printf '%s' "$raw" | tr '[:upper:]' '[:lower:]'
}

is_known_route_token() {
  local route="$1"
  case "$route" in
    paper|rpc|jito|fastlane)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

parse_route_allowlist_csv_strict_into() {
  local csv="$1"
  local env_key="$2"
  local output_var="$3"
  local remaining token_raw token_trim token_norm
  local normalized_csv=""
  local seen=""
  local continue_loop="true"

  if [[ -z "${csv//[[:space:]]/}" ]]; then
    errors+=("$env_key must contain at least one route")
    printf -v "$output_var" ''
    return
  fi

  remaining="$csv"
  while [[ "$continue_loop" == "true" ]]; do
    if [[ "$remaining" == *,* ]]; then
      token_raw="${remaining%%,*}"
      remaining="${remaining#*,}"
    else
      token_raw="$remaining"
      continue_loop="false"
    fi

    token_trim="$(trim_string "$token_raw")"
    if [[ -z "$token_trim" ]]; then
      errors+=("$env_key contains empty route entry")
      continue
    fi

    token_norm="$(normalize_route_token "$token_trim")"
    if ! is_known_route_token "$token_norm"; then
      errors+=("$env_key contains unsupported route=$token_norm (supported: paper,rpc,jito,fastlane)")
      continue
    fi
    if printf '%s\n' "$seen" | grep -Fqx -- "$token_norm"; then
      errors+=("$env_key contains duplicate route=$token_norm")
      continue
    fi
    seen+="${token_norm}"$'\n'
    normalized_csv+="${normalized_csv:+,}${token_norm}"
  done

  if [[ -z "$normalized_csv" ]]; then
    errors+=("$env_key must contain at least one route")
  fi
  printf -v "$output_var" '%s' "$normalized_csv"
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
  for raw in "${values[@]-}"; do
    normalized="$(normalize_route_token "$raw")"
    [[ -z "$normalized" ]] && continue
    if printf '%s\n' "$seen" | grep -Fqx -- "$normalized"; then
      continue
    fi
    seen+="${normalized}"$'\n'
    printf '%s\n' "$normalized"
  done
}

sorted_routes_csv() {
  local csv="$1"
  local routes=""
  while IFS= read -r route; do
    [[ -z "$route" ]] && continue
    routes+="${route}"$'\n'
  done < <(normalized_routes_lines "$csv")
  if [[ -z "$routes" ]]; then
    printf ''
    return
  fi
  printf '%s' "$routes" | sort | paste -sd, -
}

routes_array_to_csv() {
  local out=""
  local route
  for route in "$@"; do
    [[ -z "$route" ]] && continue
    out+="${out:+,}${route}"
  done
  printf '%s' "$out"
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

validate_pubkey_like_token() {
  local raw="$1"
  if [[ -z "$PYTHON3_BIN" ]]; then
    return 0
  fi
  "$PYTHON3_BIN" - "$raw" <<'PY'
import sys

raw = (sys.argv[1] or "").strip()
if not raw:
    print("pubkey must be non-empty")
    raise SystemExit(1)

alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
alphabet_index = {ch: idx for idx, ch in enumerate(alphabet)}

value = 0
for ch in raw:
    if ch not in alphabet_index:
        print(f"invalid base58 character: {ch}")
        raise SystemExit(1)
    value = value * 58 + alphabet_index[ch]

decoded = b"" if value == 0 else value.to_bytes((value.bit_length() + 7) // 8, "big")
leading_ones = len(raw) - len(raw.lstrip("1"))
decoded = (b"\x00" * leading_ones) + decoded
if len(decoded) != 32:
    print(f"decoded pubkey length must be 32 bytes, got: {len(decoded)}")
    raise SystemExit(1)
PY
}

hmac_sha256_hex() {
  local secret="$1"
  local payload="$2"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf ''
    return 1
  fi
  printf '%s' "$payload" | "$PYTHON3_BIN" -c '
import hashlib
import hmac
import sys

secret = (sys.argv[1] or "").encode("utf-8")
payload = sys.stdin.buffer.read()
print(hmac.new(secret, payload, hashlib.sha256).hexdigest())
' "$secret"
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
if not endpoint:
    raise SystemExit("endpoint is empty")
parsed = urlsplit(endpoint)
scheme = parsed.scheme.lower()
if scheme not in {"http", "https"}:
    raise SystemExit(f"unsupported scheme {parsed.scheme}")
host = (parsed.hostname or "").lower()
if not host:
    raise SystemExit("host missing")
if parsed.username or parsed.password:
    raise SystemExit("URL credentials are not allowed")
if parsed.query or parsed.fragment:
    raise SystemExit("query/fragment are not allowed")
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

validate_endpoint_identity_into() {
  local endpoint="$1"
  local error_prefix="$2"
  local output_var="$3"
  local identity=""
  if ! identity="$(endpoint_identity "$endpoint" 2>/dev/null)"; then
    errors+=("$error_prefix: $endpoint")
    printf -v "$output_var" ''
    return 1
  fi
  printf -v "$output_var" '%s' "$identity"
  return 0
}

endpoint_placeholder_host() {
  local endpoint="$1"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf ''
    return 0
  fi
  "$PYTHON3_BIN" - "$endpoint" <<'PY'
import sys
from urllib.parse import urlsplit

endpoint = (sys.argv[1] or "").strip()
if not endpoint:
    print("")
    raise SystemExit(0)
try:
    parsed = urlsplit(endpoint)
except Exception:
    print("")
    raise SystemExit(0)
host = (parsed.hostname or "").strip().lower()
if not host:
    print("")
    raise SystemExit(0)
if host == "example.com" or host.endswith(".example.com"):
    print("example.com")
elif host == "executor.mock.local" or host.endswith(".executor.mock.local"):
    print("executor.mock.local")
else:
    print("")
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

json_field_kind() {
  local body="$1"
  local key="$2"
  if [[ -z "$PYTHON3_BIN" ]]; then
    printf 'missing'
    return 0
  fi
  "$PYTHON3_BIN" - "$key" "$body" <<'PY'
import json
import sys

key = sys.argv[1]
raw = (sys.argv[2] or "").strip()
if not raw:
    print("missing")
    raise SystemExit(0)
try:
    data = json.loads(raw)
except Exception:
    print("missing")
    raise SystemExit(0)
if not isinstance(data, dict) or key not in data:
    print("missing")
    raise SystemExit(0)
value = data.get(key)
if value is None:
    print("null")
elif isinstance(value, bool):
    print("bool")
elif isinstance(value, str):
    print("string")
elif isinstance(value, list):
    print("array")
elif isinstance(value, (int, float)):
    print("number")
elif isinstance(value, dict):
    print("object")
else:
    print("unknown")
PY
}

json_routes_contract_violations() {
  local body="$1"
  local key="$2"
  if [[ -z "$PYTHON3_BIN" ]]; then
    return 0
  fi
  "$PYTHON3_BIN" - "$key" "$body" <<'PY'
import json
import sys

key = sys.argv[1]
raw = (sys.argv[2] or "").strip()
if not raw:
    raise SystemExit(0)
try:
    data = json.loads(raw)
except Exception:
    raise SystemExit(0)
if not isinstance(data, dict) or key not in data:
    raise SystemExit(0)
value = data.get(key)
if not isinstance(value, list):
    raise SystemExit(0)

seen = set()
seen_order = []
for index, item in enumerate(value):
    if not isinstance(item, str):
        if item is None:
            item_type = "null"
        elif isinstance(item, bool):
            item_type = "bool"
        elif isinstance(item, (int, float)):
            item_type = "number"
        elif isinstance(item, dict):
            item_type = "object"
        else:
            item_type = type(item).__name__
        print(f"non_string:{index}:{item_type}")
        continue
    token = item.strip()
    if not token:
        print(f"empty:{index}")
        continue
    normalized = token.lower()
    if token != normalized:
        print(f"not_lowercase:{index}:{item}")
    if normalized in seen:
        print(f"duplicate:{index}:{normalized}")
    seen.add(normalized)
    if normalized not in seen_order:
        seen_order.append(normalized)

sorted_order = sorted(seen_order)
if seen_order != sorted_order:
    print(f"not_sorted:{','.join(seen_order)}:{','.join(sorted_order)}")
PY
}

append_routes_contract_errors() {
  local body="$1"
  local key="$2"
  local field_label="$3"
  local violation kind index value
  while IFS= read -r violation; do
    [[ -z "$violation" ]] && continue
    IFS=':' read -r kind index value <<<"$violation"
    case "$kind" in
      non_string)
        errors+=("executor health $field_label must contain only string route tokens, got: $value at index=$index")
        ;;
      empty)
        errors+=("executor health $field_label must not contain empty route token at index=$index")
        ;;
      not_lowercase)
        errors+=("executor health $field_label route token must be lowercase at index=$index, got: $value")
        ;;
      duplicate)
        errors+=("executor health $field_label contains duplicate route token after normalization: $value")
        ;;
      not_sorted)
        errors+=("executor health $field_label route tokens must be sorted lexicographically, got: $index expected: $value")
        ;;
    esac
  done < <(json_routes_contract_violations "$body" "$key")
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
  for error in "${errors[@]-}"; do
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
executor_backend_mode_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_BACKEND_MODE)" "upstream")"
executor_backend_mode="$(printf '%s' "$executor_backend_mode_raw" | tr '[:upper:]' '[:lower:]')"
executor_route_allowlist_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_ROUTE_ALLOWLIST)" "paper,rpc,jito")"
executor_route_allowlist_csv=""
executor_submit_fastlane_enabled_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED)" "false")"
executor_allow_unauthenticated_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED)" "false")"
executor_upstream_submit_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL)"
executor_upstream_submit_fallback_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_FALLBACK_URL)"
executor_upstream_simulate_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL)"
executor_upstream_simulate_fallback_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_FALLBACK_URL)"
executor_send_rpc_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SEND_RPC_URL)"
executor_send_rpc_fallback_default="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_URL)"
executor_submit_verify_rpc_url="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL)"
executor_submit_verify_rpc_fallback_url="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL)"
executor_submit_verify_strict_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT)" "false")"
read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_AUTH_TOKEN COPYBOT_EXECUTOR_UPSTREAM_AUTH_TOKEN_FILE "executor upstream auth" executor_upstream_auth_default
read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE "executor upstream fallback auth" executor_upstream_fallback_auth_default
read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN_FILE "executor send-rpc auth" executor_send_rpc_auth_default
read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE "executor send-rpc fallback auth" executor_send_rpc_fallback_auth_default
executor_signer_source_expected_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SIGNER_SOURCE)" "file")"
executor_signer_source_expected="$(printf '%s' "$executor_signer_source_expected_raw" | tr '[:upper:]' '[:lower:]')"
executor_signer_pubkey="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_SIGNER_PUBKEY)"
declare -a expected_send_rpc_enabled_routes=()
declare -a expected_send_rpc_fallback_routes=()
expected_send_rpc_enabled_routes_csv=""
expected_send_rpc_fallback_routes_csv=""
executor_signer_source_expected_valid="true"

parse_route_allowlist_csv_strict_into "$executor_route_allowlist_raw" "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST" executor_route_allowlist_csv

case "$executor_backend_mode" in
  upstream|mock)
    ;;
  *)
    errors+=("COPYBOT_EXECUTOR_BACKEND_MODE must be one of: upstream,mock")
    executor_backend_mode="upstream"
    ;;
esac

executor_submit_fastlane_enabled="$(parse_bool_token "$executor_submit_fastlane_enabled_raw")"
if [[ -z "$executor_submit_fastlane_enabled" ]]; then
  errors+=("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED must be boolean token")
  executor_submit_fastlane_enabled="false"
fi
executor_submit_verify_strict="$(parse_bool_token "$executor_submit_verify_strict_raw")"
if [[ -z "$executor_submit_verify_strict" ]]; then
  errors+=("COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT must be boolean token")
  executor_submit_verify_strict="false"
fi
executor_allow_unauthenticated="$(parse_bool_token "$executor_allow_unauthenticated_raw")"
if [[ -z "$executor_allow_unauthenticated" ]]; then
  errors+=("COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED must be boolean token")
  executor_allow_unauthenticated="false"
fi

if [[ -z "$executor_signer_pubkey" ]]; then
  errors+=("COPYBOT_EXECUTOR_SIGNER_PUBKEY must be non-empty")
elif ! signer_pubkey_validation_error="$(validate_pubkey_like_token "$executor_signer_pubkey" 2>/dev/null)"; then
  if [[ -z "$signer_pubkey_validation_error" ]]; then
    signer_pubkey_validation_error="unknown validation error"
  fi
  errors+=("COPYBOT_EXECUTOR_SIGNER_PUBKEY must be valid base58 pubkey-like value: $signer_pubkey_validation_error")
fi
case "$executor_signer_source_expected" in
  file|kms)
    ;;
  *)
    errors+=("COPYBOT_EXECUTOR_SIGNER_SOURCE must be one of: file,kms")
    executor_signer_source_expected_valid="false"
    ;;
esac

if csv_contains_route "$executor_route_allowlist_csv" "fastlane" && [[ "$executor_submit_fastlane_enabled" != "true" ]]; then
  errors+=("COPYBOT_EXECUTOR_ROUTE_ALLOWLIST includes fastlane but COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED is false (allowlist=$(sorted_routes_csv "$executor_route_allowlist_csv"))")
fi

any_upstream_fallback_endpoint="false"
any_send_rpc_primary_endpoint="false"
any_send_rpc_fallback_endpoint="false"

while IFS= read -r route; do
  [[ -z "$route" ]] && continue
  route_upper="$(printf '%s' "$route" | tr '[:lower:]' '[:upper:]')"
  route_submit="$(first_non_empty \
    "$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SUBMIT_URL")" \
    "$executor_upstream_submit_default")"
  route_submit_fallback="$(first_non_empty \
    "$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SUBMIT_FALLBACK_URL")" \
    "$executor_upstream_submit_fallback_default")"
  route_simulate="$(first_non_empty \
    "$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SIMULATE_URL")" \
    "$executor_upstream_simulate_default")"
  route_simulate_fallback="$(first_non_empty \
    "$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SIMULATE_FALLBACK_URL")" \
    "$executor_upstream_simulate_fallback_default")"
  route_send_rpc="$(first_non_empty \
    "$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SEND_RPC_URL")" \
    "$executor_send_rpc_default")"
  route_send_rpc_fallback="$(first_non_empty \
    "$(env_or_file_value "$EXECUTOR_ENV_PATH" "COPYBOT_EXECUTOR_ROUTE_${route_upper}_SEND_RPC_FALLBACK_URL")" \
    "$executor_send_rpc_fallback_default")"
  if [[ -z "$route_submit" && "$executor_backend_mode" == "mock" ]]; then
    route_submit="$(default_executor_mock_backend_url "$route" "submit")"
  fi
  if [[ -z "$route_simulate" && "$executor_backend_mode" == "mock" ]]; then
    route_simulate="$(default_executor_mock_backend_url "$route" "simulate")"
  fi
  if [[ -z "$route_submit" && "$executor_backend_mode" == "upstream" && "$route" == "paper" ]]; then
    route_submit="$(default_executor_internal_paper_backend_url "$route" "submit")"
  fi
  if [[ -z "$route_simulate" && "$executor_backend_mode" == "upstream" && "$route" == "paper" ]]; then
    route_simulate="$(default_executor_internal_paper_backend_url "$route" "simulate")"
  fi
  if [[ -z "$route_submit" ]]; then
    errors+=("missing submit backend URL for executor route=$route (set COPYBOT_EXECUTOR_ROUTE_${route_upper}_SUBMIT_URL or COPYBOT_EXECUTOR_UPSTREAM_SUBMIT_URL)")
  fi
  if [[ -z "$route_simulate" ]]; then
    errors+=("missing simulate backend URL for executor route=$route (set COPYBOT_EXECUTOR_ROUTE_${route_upper}_SIMULATE_URL or COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_URL)")
  fi
  if [[ -n "$route_send_rpc_fallback" && -z "$route_send_rpc" ]]; then
    errors+=("missing primary send-rpc URL for executor route=$route while send-rpc fallback is configured")
  fi
  if [[ "$executor_backend_mode" == "upstream" ]]; then
    route_endpoint_placeholder_host=""
    route_endpoint_placeholder_host="$(endpoint_placeholder_host "$route_submit")"
    if [[ -n "$route_endpoint_placeholder_host" ]]; then
      errors+=("submit URL for executor route=$route uses placeholder host=$route_endpoint_placeholder_host in upstream mode: $route_submit")
    fi
    route_endpoint_placeholder_host="$(endpoint_placeholder_host "$route_submit_fallback")"
    if [[ -n "$route_endpoint_placeholder_host" ]]; then
      errors+=("submit fallback URL for executor route=$route uses placeholder host=$route_endpoint_placeholder_host in upstream mode: $route_submit_fallback")
    fi
    route_endpoint_placeholder_host="$(endpoint_placeholder_host "$route_simulate")"
    if [[ -n "$route_endpoint_placeholder_host" ]]; then
      errors+=("simulate URL for executor route=$route uses placeholder host=$route_endpoint_placeholder_host in upstream mode: $route_simulate")
    fi
    route_endpoint_placeholder_host="$(endpoint_placeholder_host "$route_simulate_fallback")"
    if [[ -n "$route_endpoint_placeholder_host" ]]; then
      errors+=("simulate fallback URL for executor route=$route uses placeholder host=$route_endpoint_placeholder_host in upstream mode: $route_simulate_fallback")
    fi
    route_endpoint_placeholder_host="$(endpoint_placeholder_host "$route_send_rpc")"
    if [[ -n "$route_endpoint_placeholder_host" ]]; then
      errors+=("send-rpc URL for executor route=$route uses placeholder host=$route_endpoint_placeholder_host in upstream mode: $route_send_rpc")
    fi
    route_endpoint_placeholder_host="$(endpoint_placeholder_host "$route_send_rpc_fallback")"
    if [[ -n "$route_endpoint_placeholder_host" ]]; then
      errors+=("send-rpc fallback URL for executor route=$route uses placeholder host=$route_endpoint_placeholder_host in upstream mode: $route_send_rpc_fallback")
    fi
  fi

  has_upstream_fallback_endpoint="false"
  has_send_rpc_primary_endpoint="false"
  has_send_rpc_fallback_endpoint="false"
  if [[ -n "$route_submit_fallback" || -n "$route_simulate_fallback" ]]; then
    has_upstream_fallback_endpoint="true"
    any_upstream_fallback_endpoint="true"
  fi
  if [[ -n "$route_send_rpc" ]]; then
    has_send_rpc_primary_endpoint="true"
    any_send_rpc_primary_endpoint="true"
  fi
  if [[ -n "$route_send_rpc_fallback" ]]; then
    has_send_rpc_fallback_endpoint="true"
    any_send_rpc_fallback_endpoint="true"
  fi

  route_fallback_auth_key="COPYBOT_EXECUTOR_ROUTE_${route_upper}_FALLBACK_AUTH_TOKEN"
  route_fallback_auth_file_key="COPYBOT_EXECUTOR_ROUTE_${route_upper}_FALLBACK_AUTH_TOKEN_FILE"
  route_send_rpc_auth_key="COPYBOT_EXECUTOR_ROUTE_${route_upper}_SEND_RPC_AUTH_TOKEN"
  route_send_rpc_auth_file_key="COPYBOT_EXECUTOR_ROUTE_${route_upper}_SEND_RPC_AUTH_TOKEN_FILE"
  route_send_rpc_fallback_auth_key="COPYBOT_EXECUTOR_ROUTE_${route_upper}_SEND_RPC_FALLBACK_AUTH_TOKEN"
  route_send_rpc_fallback_auth_file_key="COPYBOT_EXECUTOR_ROUTE_${route_upper}_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE"

  read_secret_from_source "$EXECUTOR_ENV_PATH" "$route_fallback_auth_key" "$route_fallback_auth_file_key" "executor route upstream fallback auth ($route)" route_specific_fallback_auth_token
  if [[ -n "$route_specific_fallback_auth_token" && "$has_upstream_fallback_endpoint" != "true" ]]; then
    errors+=("${route_fallback_auth_key} or ${route_fallback_auth_file_key} requires route=${route} submit/simulate fallback endpoint")
  fi

  read_secret_from_source "$EXECUTOR_ENV_PATH" "$route_send_rpc_auth_key" "$route_send_rpc_auth_file_key" "executor route send-rpc auth ($route)" route_specific_send_rpc_primary_auth_token
  if [[ -n "$route_specific_send_rpc_primary_auth_token" && "$has_send_rpc_primary_endpoint" != "true" ]]; then
    errors+=("${route_send_rpc_auth_key} or ${route_send_rpc_auth_file_key} requires route=${route} send-rpc endpoint")
  fi

  read_secret_from_source "$EXECUTOR_ENV_PATH" "$route_send_rpc_fallback_auth_key" "$route_send_rpc_fallback_auth_file_key" "executor route send-rpc fallback auth ($route)" route_specific_send_rpc_fallback_auth_token
  if [[ -n "$route_specific_send_rpc_fallback_auth_token" && "$has_send_rpc_fallback_endpoint" != "true" ]]; then
    errors+=("${route_send_rpc_fallback_auth_key} or ${route_send_rpc_fallback_auth_file_key} requires route=${route} send-rpc fallback endpoint")
  fi

  route_submit_identity=""
  route_submit_fallback_identity=""
  route_simulate_identity=""
  route_simulate_fallback_identity=""
  route_send_rpc_identity=""
  route_send_rpc_fallback_identity=""

  if [[ -n "$route_submit" ]]; then
    validate_endpoint_identity_into "$route_submit" "invalid submit URL for executor route=$route" route_submit_identity || true
  fi
  if [[ -n "$route_submit_fallback" ]]; then
    validate_endpoint_identity_into "$route_submit_fallback" "invalid submit fallback URL for executor route=$route" route_submit_fallback_identity || true
    if [[ -n "$route_submit_identity" && -n "$route_submit_fallback_identity" && "$route_submit_identity" == "$route_submit_fallback_identity" ]]; then
      errors+=("submit fallback URL for executor route=$route must resolve to distinct endpoint")
    fi
  fi

  if [[ -n "$route_simulate" ]]; then
    validate_endpoint_identity_into "$route_simulate" "invalid simulate URL for executor route=$route" route_simulate_identity || true
  fi
  if [[ -n "$route_simulate_fallback" ]]; then
    validate_endpoint_identity_into "$route_simulate_fallback" "invalid simulate fallback URL for executor route=$route" route_simulate_fallback_identity || true
    if [[ -n "$route_simulate_identity" && -n "$route_simulate_fallback_identity" && "$route_simulate_identity" == "$route_simulate_fallback_identity" ]]; then
      errors+=("simulate fallback URL for executor route=$route must resolve to distinct endpoint")
    fi
  fi

  if [[ -n "$route_send_rpc" ]]; then
    validate_endpoint_identity_into "$route_send_rpc" "invalid send-rpc URL for executor route=$route" route_send_rpc_identity || true
  fi
  if [[ -n "$route_send_rpc_fallback" ]]; then
    validate_endpoint_identity_into "$route_send_rpc_fallback" "invalid send-rpc fallback URL for executor route=$route" route_send_rpc_fallback_identity || true
    if [[ -n "$route_send_rpc_identity" && -n "$route_send_rpc_fallback_identity" && "$route_send_rpc_identity" == "$route_send_rpc_fallback_identity" ]]; then
      errors+=("send-rpc fallback URL for executor route=$route must resolve to distinct endpoint")
    fi
  fi

  if [[ -n "$route_send_rpc" ]]; then
    expected_send_rpc_enabled_routes+=("$route")
  fi
  if [[ -n "$route_send_rpc_fallback" ]]; then
    expected_send_rpc_fallback_routes+=("$route")
  fi
done < <(normalized_routes_lines "$executor_route_allowlist_csv")
expected_send_rpc_enabled_routes_csv="$(routes_array_to_csv "${expected_send_rpc_enabled_routes[@]-}")"
expected_send_rpc_fallback_routes_csv="$(routes_array_to_csv "${expected_send_rpc_fallback_routes[@]-}")"

if [[ -n "$executor_upstream_fallback_auth_default" && "$any_upstream_fallback_endpoint" != "true" ]]; then
  errors+=("COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN or COPYBOT_EXECUTOR_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE requires at least one submit/simulate fallback endpoint")
fi
if [[ -n "$executor_send_rpc_auth_default" && "$any_send_rpc_primary_endpoint" != "true" ]]; then
  errors+=("COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN or COPYBOT_EXECUTOR_SEND_RPC_AUTH_TOKEN_FILE requires at least one send-rpc endpoint")
fi
if [[ -n "$executor_send_rpc_fallback_auth_default" && "$any_send_rpc_fallback_endpoint" != "true" ]]; then
  errors+=("COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN or COPYBOT_EXECUTOR_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE requires at least one send-rpc fallback endpoint")
fi

executor_submit_verify_primary_identity=""
executor_submit_verify_fallback_identity=""
if [[ "$executor_submit_verify_strict" == "true" && -z "$executor_submit_verify_rpc_url" ]]; then
  errors+=("COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT requires COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL")
fi
if [[ -z "$executor_submit_verify_rpc_url" && -n "$executor_submit_verify_rpc_fallback_url" ]]; then
  errors+=("COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL requires COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL")
fi
if [[ -n "$executor_submit_verify_rpc_url" ]]; then
  validate_endpoint_identity_into "$executor_submit_verify_rpc_url" "invalid COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL" executor_submit_verify_primary_identity || true
fi
if [[ -n "$executor_submit_verify_rpc_fallback_url" ]]; then
  validate_endpoint_identity_into "$executor_submit_verify_rpc_fallback_url" "invalid COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL" executor_submit_verify_fallback_identity || true
  if [[ -n "$executor_submit_verify_primary_identity" && -n "$executor_submit_verify_fallback_identity" && "$executor_submit_verify_primary_identity" == "$executor_submit_verify_fallback_identity" ]]; then
    errors+=("COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL must resolve to distinct endpoint")
  fi
fi
if [[ "$executor_backend_mode" == "upstream" ]]; then
  submit_verify_placeholder_host=""
  submit_verify_placeholder_host="$(endpoint_placeholder_host "$executor_submit_verify_rpc_url")"
  if [[ -n "$submit_verify_placeholder_host" ]]; then
    errors+=("COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL uses placeholder host=$submit_verify_placeholder_host in upstream mode: $executor_submit_verify_rpc_url")
  fi
  submit_verify_placeholder_host="$(endpoint_placeholder_host "$executor_submit_verify_rpc_fallback_url")"
  if [[ -n "$submit_verify_placeholder_host" ]]; then
    errors+=("COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_FALLBACK_URL uses placeholder host=$submit_verify_placeholder_host in upstream mode: $executor_submit_verify_rpc_fallback_url")
  fi
fi

read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_BEARER_TOKEN COPYBOT_EXECUTOR_BEARER_TOKEN_FILE "executor ingress auth" executor_bearer_token
executor_hmac_key_id="$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_HMAC_KEY_ID)"
read_secret_from_source "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_HMAC_SECRET COPYBOT_EXECUTOR_HMAC_SECRET_FILE "executor hmac" executor_hmac_secret
executor_hmac_ttl_raw="$(first_non_empty "$(env_or_file_value "$EXECUTOR_ENV_PATH" COPYBOT_EXECUTOR_HMAC_TTL_SEC)" "30")"
executor_hmac_ttl_sec=""

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
  else
    executor_hmac_ttl_sec="$executor_hmac_ttl_raw"
  fi
fi

executor_bearer_required="false"
if [[ -n "$executor_bearer_token" ]]; then
  executor_bearer_required="true"
fi
executor_hmac_required="false"
if [[ -n "$executor_hmac_key_id" ]]; then
  executor_hmac_required="true"
fi
executor_auth_required="false"
if [[ "$executor_allow_unauthenticated" != "true" || "$executor_bearer_required" == "true" || "$executor_hmac_required" == "true" ]]; then
  executor_auth_required="true"
fi
if [[ "$executor_allow_unauthenticated" != "true" && "$executor_bearer_required" != "true" && "$executor_hmac_required" != "true" ]]; then
  errors+=("COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=false requires COPYBOT_EXECUTOR_BEARER_TOKEN (or *_FILE) or HMAC pair (COPYBOT_EXECUTOR_HMAC_KEY_ID/COPYBOT_EXECUTOR_HMAC_SECRET)")
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

adapter_route_allowlist_raw="$(first_non_empty "$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_ROUTE_ALLOWLIST)" "paper,rpc,fastlane,jito")"
adapter_route_allowlist_csv=""
adapter_submit_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL)"
adapter_submit_fallback_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_SUBMIT_FALLBACK_URL)"
adapter_simulate_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL)"
adapter_simulate_fallback_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_SIMULATE_FALLBACK_URL)"
adapter_send_rpc_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_SEND_RPC_URL)"
adapter_send_rpc_fallback_default="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_SEND_RPC_FALLBACK_URL)"
read_secret_from_source "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE "adapter upstream auth" adapter_auth_default
read_secret_from_source "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN_FILE "adapter upstream fallback auth" adapter_fallback_auth_default
read_secret_from_source "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE "adapter send-rpc auth" adapter_send_rpc_auth_default
read_secret_from_source "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE "adapter send-rpc fallback auth" adapter_send_rpc_fallback_auth_default
adapter_upstream_hmac_key_id="$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID)"
read_secret_from_source "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET_FILE "adapter upstream hmac" adapter_upstream_hmac_secret
adapter_upstream_hmac_ttl_raw="$(first_non_empty "$(env_or_file_value "$ADAPTER_ENV_PATH" COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC)" "30")"
adapter_upstream_hmac_ttl_sec=""
adapter_upstream_hmac_configured="false"

if [[ -n "$adapter_upstream_hmac_key_id" && -z "$adapter_upstream_hmac_secret" ]]; then
  errors+=("COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID requires COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET")
fi
if [[ -z "$adapter_upstream_hmac_key_id" && -n "$adapter_upstream_hmac_secret" ]]; then
  errors+=("COPYBOT_ADAPTER_UPSTREAM_HMAC_SECRET requires COPYBOT_ADAPTER_UPSTREAM_HMAC_KEY_ID")
fi
if [[ -n "$adapter_upstream_hmac_key_id" ]]; then
  adapter_upstream_hmac_configured="true"
  if ! [[ "$adapter_upstream_hmac_ttl_raw" =~ ^[0-9]+$ ]]; then
    errors+=("COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC must be integer when upstream HMAC auth is configured")
  elif (( adapter_upstream_hmac_ttl_raw < 5 || adapter_upstream_hmac_ttl_raw > 300 )); then
    errors+=("COPYBOT_ADAPTER_UPSTREAM_HMAC_TTL_SEC must be in 5..=300 when upstream HMAC auth is configured")
  else
    adapter_upstream_hmac_ttl_sec="$adapter_upstream_hmac_ttl_raw"
  fi
fi

parse_route_allowlist_csv_strict_into "$adapter_route_allowlist_raw" "COPYBOT_ADAPTER_ROUTE_ALLOWLIST" adapter_route_allowlist_csv

if [[ "$executor_hmac_required" == "true" ]]; then
  if [[ "$adapter_upstream_hmac_configured" != "true" ]]; then
    errors+=("adapter upstream hmac config missing while executor HMAC auth is required")
  else
    if [[ "$adapter_upstream_hmac_key_id" != "$executor_hmac_key_id" ]]; then
      errors+=("adapter upstream HMAC key id mismatch: adapter=$adapter_upstream_hmac_key_id executor=$executor_hmac_key_id")
    fi
    if [[ -n "$executor_hmac_secret" && "$adapter_upstream_hmac_secret" != "$executor_hmac_secret" ]]; then
      errors+=("adapter upstream HMAC secret mismatch vs executor HMAC secret")
    fi
    if [[ -n "$executor_hmac_ttl_sec" && -n "$adapter_upstream_hmac_ttl_sec" && "$adapter_upstream_hmac_ttl_sec" != "$executor_hmac_ttl_sec" ]]; then
      errors+=("adapter upstream HMAC ttl mismatch: adapter=$adapter_upstream_hmac_ttl_sec executor=$executor_hmac_ttl_sec")
    fi
  fi
fi

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
  adapter_route_submit_fallback="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SUBMIT_FALLBACK_URL")" \
    "$adapter_submit_fallback_default")"
  adapter_route_simulate="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SIMULATE_URL")" \
    "$adapter_simulate_default")"
  adapter_route_simulate_fallback="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SIMULATE_FALLBACK_URL")" \
    "$adapter_simulate_fallback_default")"
  adapter_route_send_rpc="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SEND_RPC_URL")" \
    "$adapter_send_rpc_default")"
  adapter_route_send_rpc_fallback="$(first_non_empty \
    "$(env_or_file_value "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SEND_RPC_FALLBACK_URL")" \
    "$adapter_send_rpc_fallback_default")"
  read_secret_from_source "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SEND_RPC_AUTH_TOKEN" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SEND_RPC_AUTH_TOKEN_FILE" "adapter route send-rpc auth ($route)" route_send_rpc_auth_source
  read_secret_from_source "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SEND_RPC_FALLBACK_AUTH_TOKEN" "COPYBOT_ADAPTER_ROUTE_${route_upper}_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE" "adapter route send-rpc fallback auth ($route)" route_send_rpc_fallback_auth_source

  if [[ -z "$adapter_route_submit" ]]; then
    errors+=("missing adapter submit upstream URL for route=$route (set COPYBOT_ADAPTER_ROUTE_${route_upper}_SUBMIT_URL or COPYBOT_ADAPTER_UPSTREAM_SUBMIT_URL)")
  fi
  if [[ -z "$adapter_route_simulate" ]]; then
    errors+=("missing adapter simulate upstream URL for route=$route (set COPYBOT_ADAPTER_ROUTE_${route_upper}_SIMULATE_URL or COPYBOT_ADAPTER_UPSTREAM_SIMULATE_URL)")
  fi

  submit_identity=""
  if [[ -n "$adapter_route_submit" && -n "$expected_submit_identity" ]]; then
    if validate_endpoint_identity_into "$adapter_route_submit" "invalid adapter submit URL for route=$route" submit_identity && [[ "$submit_identity" != "$expected_submit_identity" ]]; then
      errors+=("adapter submit URL for route=$route must target executor submit endpoint ($EXECUTOR_EXPECT_SUBMIT_URL)")
    fi
  elif [[ -n "$adapter_route_submit" ]]; then
    validate_endpoint_identity_into "$adapter_route_submit" "invalid adapter submit URL for route=$route" submit_identity || true
  fi

  if [[ -n "$adapter_route_submit_fallback" ]]; then
    submit_fallback_identity=""
    validate_endpoint_identity_into "$adapter_route_submit_fallback" "invalid adapter submit fallback URL for route=$route" submit_fallback_identity || true
    if [[ -n "$submit_identity" && -n "$submit_fallback_identity" && "$submit_identity" == "$submit_fallback_identity" ]]; then
      errors+=("adapter submit fallback URL for route=$route must resolve to distinct endpoint")
    fi
  fi

  simulate_identity=""
  if [[ -n "$adapter_route_simulate" && -n "$expected_simulate_identity" ]]; then
    if validate_endpoint_identity_into "$adapter_route_simulate" "invalid adapter simulate URL for route=$route" simulate_identity && [[ "$simulate_identity" != "$expected_simulate_identity" ]]; then
      errors+=("adapter simulate URL for route=$route must target executor simulate endpoint ($EXECUTOR_EXPECT_SIMULATE_URL)")
    fi
  elif [[ -n "$adapter_route_simulate" ]]; then
    validate_endpoint_identity_into "$adapter_route_simulate" "invalid adapter simulate URL for route=$route" simulate_identity || true
  fi

  if [[ -n "$adapter_route_simulate_fallback" ]]; then
    simulate_fallback_identity=""
    validate_endpoint_identity_into "$adapter_route_simulate_fallback" "invalid adapter simulate fallback URL for route=$route" simulate_fallback_identity || true
    if [[ -n "$simulate_identity" && -n "$simulate_fallback_identity" && "$simulate_identity" == "$simulate_fallback_identity" ]]; then
      errors+=("adapter simulate fallback URL for route=$route must resolve to distinct endpoint")
    fi
  fi

  if [[ -n "$adapter_route_send_rpc_fallback" && -z "$adapter_route_send_rpc" ]]; then
    errors+=("missing adapter send-rpc upstream URL for route=$route while send-rpc fallback is configured")
  fi
  if [[ -n "$adapter_route_send_rpc" ]]; then
    send_rpc_identity=""
    validate_endpoint_identity_into "$adapter_route_send_rpc" "invalid adapter send-rpc URL for route=$route" send_rpc_identity || true
  else
    send_rpc_identity=""
  fi
  if [[ -n "$adapter_route_send_rpc_fallback" ]]; then
    send_rpc_fallback_identity=""
    validate_endpoint_identity_into "$adapter_route_send_rpc_fallback" "invalid adapter send-rpc fallback URL for route=$route" send_rpc_fallback_identity || true
    if [[ -n "$send_rpc_identity" && -n "$send_rpc_fallback_identity" && "$send_rpc_identity" == "$send_rpc_fallback_identity" ]]; then
      errors+=("adapter send-rpc fallback URL for route=$route must resolve to distinct endpoint")
    fi
  fi

  if [[ "$executor_bearer_required" == "true" ]]; then
    read_secret_from_source "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_AUTH_TOKEN" "COPYBOT_ADAPTER_ROUTE_${route_upper}_AUTH_TOKEN_FILE" "adapter route auth ($route)" route_auth_source
    route_auth_token="$(first_non_empty \
      "$route_auth_source" \
      "$adapter_auth_default")"
    if [[ -z "$route_auth_token" ]]; then
      errors+=("adapter auth token missing for route=$route while executor bearer auth is required")
    elif [[ -n "$executor_bearer_token" && "$route_auth_token" != "$executor_bearer_token" ]]; then
      errors+=("adapter auth token mismatch for route=$route vs executor bearer token")
    fi

    if [[ -n "$adapter_route_submit_fallback" || -n "$adapter_route_simulate_fallback" ]]; then
      read_secret_from_source "$ADAPTER_ENV_PATH" "COPYBOT_ADAPTER_ROUTE_${route_upper}_FALLBACK_AUTH_TOKEN" "COPYBOT_ADAPTER_ROUTE_${route_upper}_FALLBACK_AUTH_TOKEN_FILE" "adapter route fallback auth ($route)" route_fallback_auth_source
      route_fallback_auth_token="$(first_non_empty \
        "$route_fallback_auth_source" \
        "$(first_non_empty "$adapter_fallback_auth_default" "$route_auth_token")")"
      if [[ -z "$route_fallback_auth_token" ]]; then
        errors+=("adapter fallback auth token missing for route=$route while executor bearer auth is required and adapter fallback endpoint is configured")
      elif [[ -n "$executor_bearer_token" && "$route_fallback_auth_token" != "$executor_bearer_token" ]]; then
        errors+=("adapter fallback auth token mismatch for route=$route vs executor bearer token")
      fi
    fi

    route_send_rpc_auth_token=""
    if [[ -n "$adapter_route_send_rpc" ]]; then
      route_send_rpc_auth_token="$(first_non_empty \
        "$route_send_rpc_auth_source" \
        "$adapter_send_rpc_auth_default")"
      if [[ -z "$route_send_rpc_auth_token" ]]; then
        errors+=("adapter send-rpc auth token missing for route=$route while executor bearer auth is required and adapter send-rpc endpoint is configured")
      elif [[ -n "$executor_bearer_token" && "$route_send_rpc_auth_token" != "$executor_bearer_token" ]]; then
        errors+=("adapter send-rpc auth token mismatch for route=$route vs executor bearer token")
      fi
    fi

    if [[ -n "$adapter_route_send_rpc_fallback" ]]; then
      route_send_rpc_fallback_auth_token="$(first_non_empty \
        "$route_send_rpc_fallback_auth_source" \
        "$(first_non_empty "$adapter_send_rpc_fallback_auth_default" "$route_send_rpc_auth_token")")"
      if [[ -z "$route_send_rpc_fallback_auth_token" ]]; then
        errors+=("adapter send-rpc fallback auth token missing for route=$route while executor bearer auth is required and adapter send-rpc fallback endpoint is configured")
      elif [[ -n "$executor_bearer_token" && "$route_send_rpc_fallback_auth_token" != "$executor_bearer_token" ]]; then
        errors+=("adapter send-rpc fallback auth token mismatch for route=$route vs executor bearer token")
      fi
    fi
  fi
done < <(normalized_routes_lines "$adapter_route_allowlist_csv")

health_http_status="n/a"
health_status_field="n/a"
health_contract_version="n/a"
health_backend_mode="n/a"
health_status_field_kind="n/a"
health_contract_version_field_kind="n/a"
health_backend_mode_field_kind="n/a"
health_routes_csv="n/a"
health_routes_alias_csv="n/a"
health_routes_field_kind="n/a"
health_routes_alias_field_kind="n/a"
health_send_rpc_enabled_routes_csv="n/a"
health_send_rpc_fallback_routes_csv="n/a"
health_send_rpc_alias_routes_csv="n/a"
health_send_rpc_enabled_field_kind="n/a"
health_send_rpc_fallback_field_kind="n/a"
health_send_rpc_alias_field_kind="n/a"
health_signer_source="n/a"
health_signer_pubkey="n/a"
health_submit_fastlane_enabled="n/a"
health_signer_source_field_kind="n/a"
health_signer_pubkey_field_kind="n/a"
health_submit_fastlane_enabled_field_kind="n/a"
idempotency_store_status="n/a"
idempotency_store_status_field_kind="n/a"
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
      health_backend_mode="$(printf '%s' "$(json_string_field "$health_body" "backend_mode")" | tr '[:upper:]' '[:lower:]')"
      health_signer_source="$(printf '%s' "$(json_string_field "$health_body" "signer_source")" | tr '[:upper:]' '[:lower:]')"
      health_signer_pubkey="$(json_string_field "$health_body" "signer_pubkey")"
      health_submit_fastlane_enabled_raw="$(json_string_field "$health_body" "submit_fastlane_enabled")"
      health_submit_fastlane_enabled="$(parse_bool_token "$health_submit_fastlane_enabled_raw")"
      idempotency_store_status="$(json_string_field "$health_body" "idempotency_store_status")"
      health_status_field_kind="$(json_field_kind "$health_body" "status")"
      health_contract_version_field_kind="$(json_field_kind "$health_body" "contract_version")"
      health_backend_mode_field_kind="$(json_field_kind "$health_body" "backend_mode")"
      health_signer_source_field_kind="$(json_field_kind "$health_body" "signer_source")"
      health_signer_pubkey_field_kind="$(json_field_kind "$health_body" "signer_pubkey")"
      health_submit_fastlane_enabled_field_kind="$(json_field_kind "$health_body" "submit_fastlane_enabled")"
      idempotency_store_status_field_kind="$(json_field_kind "$health_body" "idempotency_store_status")"
      health_routes_field_kind="$(json_field_kind "$health_body" "enabled_routes")"
      health_routes_alias_field_kind="$(json_field_kind "$health_body" "routes")"
      health_send_rpc_enabled_field_kind="$(json_field_kind "$health_body" "send_rpc_enabled_routes")"
      health_send_rpc_fallback_field_kind="$(json_field_kind "$health_body" "send_rpc_fallback_routes")"
      health_send_rpc_alias_field_kind="$(json_field_kind "$health_body" "send_rpc_routes")"
      health_routes_alias_csv="$(json_routes_csv_field "$health_body" "routes")"
      health_routes_csv="$(json_routes_csv_field "$health_body" "enabled_routes")"
      if [[ -z "$health_routes_csv" ]]; then
        health_routes_csv="$health_routes_alias_csv"
      fi
      health_send_rpc_alias_routes_csv="$(json_routes_csv_field "$health_body" "send_rpc_routes")"
      health_send_rpc_enabled_routes_csv="$(json_routes_csv_field "$health_body" "send_rpc_enabled_routes")"
      if [[ -z "$health_send_rpc_enabled_routes_csv" ]]; then
        health_send_rpc_enabled_routes_csv="$health_send_rpc_alias_routes_csv"
      fi
      health_send_rpc_fallback_routes_csv="$(json_routes_csv_field "$health_body" "send_rpc_fallback_routes")"
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
      if [[ -n "$health_backend_mode" && "$health_backend_mode" != "$executor_backend_mode" ]]; then
        errors+=("executor backend_mode mismatch: health=$health_backend_mode expected=$executor_backend_mode")
      fi
      if [[ "$health_status_field_kind" != "missing" && "$health_status_field_kind" != "string" ]]; then
        errors+=("executor health status must be string when present, got: $health_status_field_kind")
      fi
      if [[ "$health_contract_version_field_kind" != "missing" && "$health_contract_version_field_kind" != "string" ]]; then
        errors+=("executor health contract_version must be string when present, got: $health_contract_version_field_kind")
      fi
      if [[ "$health_backend_mode_field_kind" != "missing" && "$health_backend_mode_field_kind" != "string" ]]; then
        errors+=("executor health backend_mode must be string when present, got: $health_backend_mode_field_kind")
      fi
      if [[ "$health_signer_source_field_kind" != "missing" && "$health_signer_source_field_kind" != "string" ]]; then
        errors+=("executor health signer_source must be string when present, got: $health_signer_source_field_kind")
      fi
      if [[ "$health_signer_pubkey_field_kind" != "missing" && "$health_signer_pubkey_field_kind" != "string" ]]; then
        errors+=("executor health signer_pubkey must be string when present, got: $health_signer_pubkey_field_kind")
      fi
      if [[ "$health_submit_fastlane_enabled_field_kind" != "missing" && "$health_submit_fastlane_enabled_field_kind" != "bool" ]]; then
        errors+=("executor health submit_fastlane_enabled must be bool when present, got: $health_submit_fastlane_enabled_field_kind")
      fi
      if [[ "$idempotency_store_status_field_kind" != "missing" && "$idempotency_store_status_field_kind" != "string" ]]; then
        errors+=("executor health idempotency_store_status must be string when present, got: $idempotency_store_status_field_kind")
      fi
      if [[ "$health_routes_field_kind" != "missing" && "$health_routes_field_kind" != "array" ]]; then
        errors+=("executor health enabled_routes must be array when present, got: $health_routes_field_kind")
      fi
      if [[ "$health_routes_alias_field_kind" != "missing" && "$health_routes_alias_field_kind" != "array" ]]; then
        errors+=("executor health routes alias must be array when present, got: $health_routes_alias_field_kind")
      fi
      if [[ "$health_send_rpc_enabled_field_kind" != "missing" && "$health_send_rpc_enabled_field_kind" != "array" ]]; then
        errors+=("executor health send_rpc_enabled_routes must be array when present, got: $health_send_rpc_enabled_field_kind")
      fi
      if [[ "$health_send_rpc_fallback_field_kind" != "missing" && "$health_send_rpc_fallback_field_kind" != "array" ]]; then
        errors+=("executor health send_rpc_fallback_routes must be array when present, got: $health_send_rpc_fallback_field_kind")
      fi
      if [[ "$health_send_rpc_alias_field_kind" != "missing" && "$health_send_rpc_alias_field_kind" != "array" ]]; then
        errors+=("executor health send_rpc_routes alias must be array when present, got: $health_send_rpc_alias_field_kind")
      fi
      append_routes_contract_errors "$health_body" "enabled_routes" "enabled_routes"
      append_routes_contract_errors "$health_body" "routes" "routes alias"
      append_routes_contract_errors "$health_body" "send_rpc_enabled_routes" "send_rpc_enabled_routes"
      append_routes_contract_errors "$health_body" "send_rpc_fallback_routes" "send_rpc_fallback_routes"
      append_routes_contract_errors "$health_body" "send_rpc_routes" "send_rpc_routes alias"
      if [[ -z "$health_signer_source" ]]; then
        errors+=("executor health signer_source must be non-empty")
      elif [[ "$executor_signer_source_expected_valid" == "true" && "$health_signer_source" != "$executor_signer_source_expected" ]]; then
        errors+=("executor signer_source mismatch: health=$health_signer_source expected=$executor_signer_source_expected")
      fi
      if [[ -z "$health_signer_pubkey" ]]; then
        errors+=("executor health signer_pubkey must be non-empty")
      elif [[ "$health_signer_pubkey" != "$executor_signer_pubkey" ]]; then
        errors+=("executor signer_pubkey mismatch: health=$health_signer_pubkey expected=$executor_signer_pubkey")
      fi
      if [[ -z "$health_submit_fastlane_enabled" ]]; then
        errors+=("executor health submit_fastlane_enabled must be boolean token, got: ${health_submit_fastlane_enabled_raw:-<empty>}")
      elif [[ "$health_submit_fastlane_enabled" != "$executor_submit_fastlane_enabled" ]]; then
        errors+=("executor submit_fastlane_enabled mismatch: health=$health_submit_fastlane_enabled expected=$executor_submit_fastlane_enabled")
      fi
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$health_routes_csv" "$route"; then
          errors+=("health enabled_routes missing executor route=$route")
        fi
      done < <(normalized_routes_lines "$executor_route_allowlist_csv")
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$executor_route_allowlist_csv" "$route"; then
          errors+=("health enabled_routes include unexpected route=$route")
        fi
      done < <(normalized_routes_lines "$health_routes_csv")
      if [[ -n "$health_routes_alias_csv" && -n "$health_routes_csv" ]]; then
        while IFS= read -r route; do
          [[ -z "$route" ]] && continue
          if ! csv_contains_route "$health_routes_csv" "$route"; then
            errors+=("health routes alias include unexpected route=$route")
          fi
        done < <(normalized_routes_lines "$health_routes_alias_csv")
        while IFS= read -r route; do
          [[ -z "$route" ]] && continue
          if ! csv_contains_route "$health_routes_alias_csv" "$route"; then
            errors+=("health routes alias missing enabled route=$route")
          fi
        done < <(normalized_routes_lines "$health_routes_csv")
      fi
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$health_send_rpc_enabled_routes_csv" "$route"; then
          errors+=("health send-rpc enabled routes missing executor route=$route")
        fi
      done < <(normalized_routes_lines "$expected_send_rpc_enabled_routes_csv")
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$expected_send_rpc_enabled_routes_csv" "$route"; then
          errors+=("health send-rpc enabled routes include unexpected route=$route")
        fi
      done < <(normalized_routes_lines "$health_send_rpc_enabled_routes_csv")
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$health_send_rpc_fallback_routes_csv" "$route"; then
          errors+=("health send-rpc fallback routes missing executor route=$route")
        fi
      done < <(normalized_routes_lines "$expected_send_rpc_fallback_routes_csv")
      if [[ -n "$health_send_rpc_alias_routes_csv" && -n "$health_send_rpc_enabled_routes_csv" ]]; then
        while IFS= read -r route; do
          [[ -z "$route" ]] && continue
          if ! csv_contains_route "$health_send_rpc_enabled_routes_csv" "$route"; then
            errors+=("health send-rpc alias routes include unexpected route=$route")
          fi
        done < <(normalized_routes_lines "$health_send_rpc_alias_routes_csv")
        while IFS= read -r route; do
          [[ -z "$route" ]] && continue
          if ! csv_contains_route "$health_send_rpc_alias_routes_csv" "$route"; then
            errors+=("health send-rpc alias routes missing enabled route=$route")
          fi
        done < <(normalized_routes_lines "$health_send_rpc_enabled_routes_csv")
      fi
      while IFS= read -r route; do
        [[ -z "$route" ]] && continue
        if ! csv_contains_route "$expected_send_rpc_fallback_routes_csv" "$route"; then
          errors+=("health send-rpc fallback routes include unexpected route=$route")
        fi
        if ! csv_contains_route "$health_send_rpc_enabled_routes_csv" "$route"; then
          errors+=("health send-rpc fallback routes include route=$route without send-rpc primary route")
        fi
      done < <(normalized_routes_lines "$health_send_rpc_fallback_routes_csv")
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
        if [[ "$executor_auth_required" == "true" ]]; then
          if [[ "$auth_probe_without_auth_code" != "auth_missing" && "$auth_probe_without_auth_code" != "auth_invalid" && "$auth_probe_without_auth_code" != "hmac_missing" && "$auth_probe_without_auth_code" != "hmac_invalid" ]]; then
            errors+=("auth probe without token must fail with auth_missing/auth_invalid/hmac_missing/hmac_invalid when executor auth is required")
          fi
        else
          if [[ "$auth_probe_without_auth_code" == "auth_missing" || "$auth_probe_without_auth_code" == "auth_invalid" || "$auth_probe_without_auth_code" == "hmac_missing" || "$auth_probe_without_auth_code" == "hmac_invalid" ]]; then
            errors+=("executor is configured with COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=true but simulate endpoint still requires auth")
          fi
        fi
      else
        errors+=("failed auth probe without token against simulate endpoint: $probe_url")
      fi

      if [[ "$executor_bearer_required" == "true" || "$executor_hmac_required" == "true" ]]; then
        probe_auth_headers=(-H "content-type: application/json")
        if [[ "$executor_bearer_required" == "true" ]]; then
          probe_auth_headers+=(-H "authorization: Bearer $executor_bearer_token")
        fi
        if [[ "$executor_hmac_required" == "true" ]]; then
          if [[ -z "$executor_hmac_ttl_sec" ]]; then
            errors+=("cannot build HMAC auth probe headers: executor HMAC TTL is invalid")
          else
            auth_probe_timestamp="$(date -u +%s)"
            auth_probe_nonce="executor-preflight-${auth_probe_timestamp}-$$"
            auth_probe_hmac_payload="$(printf '%s\n%s\n%s\n%s' "$auth_probe_timestamp" "$executor_hmac_ttl_sec" "$auth_probe_nonce" "$simulate_probe_payload")"
            if auth_probe_hmac_signature="$(hmac_sha256_hex "$executor_hmac_secret" "$auth_probe_hmac_payload")"; then
              probe_auth_headers+=(-H "x-copybot-key-id: $executor_hmac_key_id")
              probe_auth_headers+=(-H "x-copybot-signature-alg: hmac-sha256-v1")
              probe_auth_headers+=(-H "x-copybot-timestamp: $auth_probe_timestamp")
              probe_auth_headers+=(-H "x-copybot-auth-ttl-sec: $executor_hmac_ttl_sec")
              probe_auth_headers+=(-H "x-copybot-nonce: $auth_probe_nonce")
              probe_auth_headers+=(-H "x-copybot-signature: $auth_probe_hmac_signature")
            else
              errors+=("failed building HMAC auth probe signature")
            fi
          fi
        fi
        if probe_with_auth_status="$(curl -sS -m "$HTTP_TIMEOUT_SEC" "${probe_auth_headers[@]}" --data "$simulate_probe_payload" -o "$probe_body_file" -w "%{http_code}" "$probe_url" 2>/dev/null)"; then
          probe_with_auth_body="$(cat "$probe_body_file")"
          auth_probe_with_auth_http_status="$probe_with_auth_status"
          auth_probe_with_auth_code="$(json_string_field "$probe_with_auth_body" "code")"
          if [[ "$probe_with_auth_status" != "200" ]]; then
            errors+=("auth probe with configured executor auth headers must return HTTP 200, got $probe_with_auth_status")
          fi
          if [[ "$auth_probe_with_auth_code" == "auth_missing" || "$auth_probe_with_auth_code" == "auth_invalid" || "$auth_probe_with_auth_code" == "hmac_missing" || "$auth_probe_with_auth_code" == "hmac_invalid" ]]; then
            errors+=("auth probe with configured executor auth headers still failed auth check")
          fi
        else
          errors+=("failed auth probe with configured executor auth headers against simulate endpoint: $probe_url")
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

executor_upstream_auth_configured="false"
executor_upstream_fallback_auth_configured="false"
executor_send_rpc_auth_configured="false"
executor_send_rpc_fallback_auth_configured="false"
executor_submit_verify_configured="false"
executor_submit_verify_fallback_configured="false"
if [[ -n "$executor_upstream_auth_default" ]]; then
  executor_upstream_auth_configured="true"
fi
if [[ -n "$executor_upstream_fallback_auth_default" ]]; then
  executor_upstream_fallback_auth_configured="true"
fi
if [[ -n "$executor_send_rpc_auth_default" ]]; then
  executor_send_rpc_auth_configured="true"
fi
if [[ -n "$executor_send_rpc_fallback_auth_default" ]]; then
  executor_send_rpc_fallback_auth_configured="true"
fi
if [[ -n "$executor_submit_verify_rpc_url" ]]; then
  executor_submit_verify_configured="true"
fi
if [[ -n "$executor_submit_verify_rpc_fallback_url" ]]; then
  executor_submit_verify_fallback_configured="true"
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
  echo "executor_backend_mode: $executor_backend_mode"
  echo "executor_contract_version_expected: $executor_contract_version_expected"
  echo "executor_route_allowlist_raw: $executor_route_allowlist_raw"
  echo "executor_route_allowlist_csv: $executor_route_allowlist_csv"
  echo "adapter_route_allowlist_raw: $adapter_route_allowlist_raw"
  echo "adapter_route_allowlist_csv: $adapter_route_allowlist_csv"
  echo "expected_send_rpc_enabled_routes_csv: $expected_send_rpc_enabled_routes_csv"
  echo "expected_send_rpc_fallback_routes_csv: $expected_send_rpc_fallback_routes_csv"
  echo "executor_signer_source_expected: $executor_signer_source_expected"
  echo "executor_signer_pubkey_expected: $executor_signer_pubkey"
  echo "executor_submit_fastlane_enabled: $executor_submit_fastlane_enabled"
  echo "executor_submit_verify_strict: $executor_submit_verify_strict"
  echo "executor_submit_verify_configured: $executor_submit_verify_configured"
  echo "executor_submit_verify_fallback_configured: $executor_submit_verify_fallback_configured"
  echo "executor_upstream_auth_configured: $executor_upstream_auth_configured"
  echo "executor_upstream_fallback_auth_configured: $executor_upstream_fallback_auth_configured"
  echo "executor_send_rpc_auth_configured: $executor_send_rpc_auth_configured"
  echo "executor_send_rpc_fallback_auth_configured: $executor_send_rpc_fallback_auth_configured"
  echo "executor_any_upstream_fallback_endpoint: $any_upstream_fallback_endpoint"
  echo "executor_any_send_rpc_primary_endpoint: $any_send_rpc_primary_endpoint"
  echo "executor_any_send_rpc_fallback_endpoint: $any_send_rpc_fallback_endpoint"
  echo "executor_auth_required: $executor_auth_required"
  echo "executor_bearer_required: $executor_bearer_required"
  echo "executor_hmac_required: $executor_hmac_required"
  echo "adapter_upstream_hmac_configured: $adapter_upstream_hmac_configured"
  echo "adapter_upstream_hmac_ttl_sec: $adapter_upstream_hmac_ttl_sec"
  echo "health_http_status: $health_http_status"
  echo "health_status: $health_status_field"
  echo "health_status_field_kind: $health_status_field_kind"
  echo "health_contract_version: $health_contract_version"
  echo "health_contract_version_field_kind: $health_contract_version_field_kind"
  echo "health_backend_mode: $health_backend_mode"
  echo "health_backend_mode_field_kind: $health_backend_mode_field_kind"
  echo "health_signer_source: $health_signer_source"
  echo "health_signer_source_field_kind: $health_signer_source_field_kind"
  echo "health_signer_pubkey: $health_signer_pubkey"
  echo "health_signer_pubkey_field_kind: $health_signer_pubkey_field_kind"
  echo "health_submit_fastlane_enabled: $health_submit_fastlane_enabled"
  echo "health_submit_fastlane_enabled_field_kind: $health_submit_fastlane_enabled_field_kind"
  echo "health_routes_csv: $health_routes_csv"
  echo "health_routes_alias_csv: $health_routes_alias_csv"
  echo "health_routes_field_kind: $health_routes_field_kind"
  echo "health_routes_alias_field_kind: $health_routes_alias_field_kind"
  echo "health_send_rpc_enabled_routes_csv: $health_send_rpc_enabled_routes_csv"
  echo "health_send_rpc_fallback_routes_csv: $health_send_rpc_fallback_routes_csv"
  echo "health_send_rpc_alias_routes_csv: $health_send_rpc_alias_routes_csv"
  echo "health_send_rpc_enabled_field_kind: $health_send_rpc_enabled_field_kind"
  echo "health_send_rpc_fallback_field_kind: $health_send_rpc_fallback_field_kind"
  echo "health_send_rpc_alias_field_kind: $health_send_rpc_alias_field_kind"
  echo "idempotency_store_status: $idempotency_store_status"
  echo "idempotency_store_status_field_kind: $idempotency_store_status_field_kind"
  echo "auth_probe_without_auth_code: $auth_probe_without_auth_code"
  echo "auth_probe_with_auth_http_status: $auth_probe_with_auth_http_status"
  echo "auth_probe_with_auth_code: $auth_probe_with_auth_code"
  echo "preflight_verdict: $preflight_verdict"
  echo "preflight_reason_code: $preflight_reason_code"
  echo "preflight_reason: $preflight_reason"
  echo "artifacts_written: $artifacts_written"
  if ((${#errors[@]} > 0)); then
    echo "error_count: ${#errors[@]}"
    for error in "${errors[@]-}"; do
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
  manifest_sha256="$(sha256_file_value "$manifest_path")"
  echo "artifact_summary: $summary_path"
  echo "artifact_manifest: $manifest_path"
  echo "summary_sha256: $summary_sha256"
  echo "manifest_sha256: $manifest_sha256"
fi

exit "$exit_code"
