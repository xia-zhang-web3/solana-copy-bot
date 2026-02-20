#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
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

cfg_list_csv() {
  local section="$1"
  local key="$2"
  awk -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
      if ($0 != section) {
        collecting = 0
        value = ""
      }
    }
    in_section {
      line = $0
      sub(/#.*/, "", line)
      if (!collecting) {
        left = line
        sub(/=.*/, "", left)
        gsub(/[[:space:]]/, "", left)
        if (left != key) {
          next
        }
        collecting = 1
        value = line
      } else {
        value = value " " line
      }
      if (index(value, "]") > 0) {
        start = index(value, "[")
        if (start == 0) {
          print ""
          exit
        }
        body = substr(value, start + 1)
        end = index(body, "]")
        if (end == 0) {
          next
        }
        body = substr(body, 1, end - 1)
        gsub(/"/, "", body)
        gsub(/'\''/, "", body)
        gsub(/[[:space:]]/, "", body)
        gsub(/,+/, ",", body)
        sub(/^,/, "", body)
        sub(/,$/, "", body)
        print body
        exit
      }
    }
  ' "$CONFIG_PATH"
}

cfg_map_keys_csv() {
  local section="$1"
  local key="$2"
  awk -v section="[$section]" -v key="$key" '
    function trim(s) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", s)
      return s
    }
    /^\s*\[/ {
      in_section = ($0 == section)
      if ($0 != section) {
        collecting = 0
        value = ""
      }
    }
    in_section {
      line = $0
      sub(/#.*/, "", line)
      if (!collecting) {
        left = line
        sub(/=.*/, "", left)
        gsub(/[[:space:]]/, "", left)
        if (left != key) {
          next
        }
        collecting = 1
        value = line
      } else {
        value = value " " line
      }
      if (index(value, "}") > 0) {
        start = index(value, "{")
        if (start == 0) {
          print ""
          exit
        }
        body = substr(value, start + 1)
        end = index(body, "}")
        if (end == 0) {
          next
        }
        body = substr(body, 1, end - 1)
        n = split(body, pairs, ",")
        out = ""
        for (i = 1; i <= n; i++) {
          pair = trim(pairs[i])
          if (pair == "") {
            continue
          }
          eq = index(pair, "=")
          if (eq == 0) {
            continue
          }
          map_key = trim(substr(pair, 1, eq - 1))
          gsub(/^"|"$/, "", map_key)
          gsub(/^'\''|'\''$/, "", map_key)
          gsub(/[[:space:]]/, "", map_key)
          if (map_key == "") {
            continue
          }
          if (out != "") {
            out = out ","
          }
          out = out map_key
        }
        print out
        exit
      }
    }
  ' "$CONFIG_PATH"
}

normalize_bool_token() {
  local raw="$1"
  raw="${raw#"${raw%%[![:space:]]*}"}"
  raw="${raw%"${raw##*[![:space:]]}"}"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on)
      printf 'true'
      ;;
    *)
      printf 'false'
      ;;
  esac
}

normalize_route_token() {
  local raw="$1"
  raw="${raw#"${raw%%[![:space:]]*}"}"
  raw="${raw%"${raw##*[![:space:]]}"}"
  printf '%s' "$raw" | tr '[:upper:]' '[:lower:]'
}

contains_placeholder_value() {
  local value="$1"
  [[ "$(printf '%s' "$value" | tr '[:lower:]' '[:upper:]')" == *"REPLACE_ME"* ]]
}

is_production_env_profile() {
  local env_norm
  env_norm="$(normalize_route_token "$1")"
  [[ "$env_norm" == "prod" || "$env_norm" == "production" || "$env_norm" == prod-* || "$env_norm" == prod_* || "$env_norm" == production-* || "$env_norm" == production_* ]]
}

resolve_secret_path() {
  local raw_path="$1"
  if [[ -z "$raw_path" ]]; then
    printf ""
    return
  fi
  if [[ "$raw_path" = /* ]]; then
    printf "%s" "$raw_path"
    return
  fi
  local config_dir
  config_dir="$(cd "$(dirname "$CONFIG_PATH")" && pwd)"
  printf "%s/%s" "$config_dir" "$raw_path"
}

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf "%s" "$value"
}

csv_contains_route() {
  local csv="$1"
  local needle="$2"
  local -a values=()
  local raw normalized
  if [[ -z "$csv" || -z "$needle" ]]; then
    return 1
  fi
  IFS=',' read -r -a values <<< "$csv"
  for raw in "${values[@]}"; do
    normalized="$(normalize_route_token "$raw")"
    if [[ "$normalized" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

csv_first_duplicate_normalized() {
  local csv="$1"
  local -a values=()
  local seen_routes=""
  local raw normalized
  if [[ -z "$csv" ]]; then
    return 1
  fi
  IFS=',' read -r -a values <<< "$csv"
  for raw in "${values[@]}"; do
    normalized="$(normalize_route_token "$raw")"
    if [[ -z "$normalized" ]]; then
      continue
    fi
    if printf '%s\n' "$seen_routes" | grep -Fqx -- "$normalized"; then
      printf "%s" "$normalized"
      return 0
    fi
    seen_routes+="${normalized}"$'\n'
  done
  return 1
}

validate_adapter_endpoint_url() {
  local endpoint="$1"
  local field_name="$2"
  local strict_transport_policy="$3"
  python3 - "$endpoint" "$field_name" "$strict_transport_policy" <<'PY'
import ipaddress
import sys
from urllib.parse import urlsplit

endpoint = sys.argv[1]
field_name = sys.argv[2]
strict_transport_policy = sys.argv[3] == "true"

if any(ch.isspace() for ch in endpoint):
    raise SystemExit(f"{field_name} must not contain whitespace")
if "REPLACE_ME" in endpoint.upper():
    raise SystemExit(f"{field_name} must not contain placeholder value REPLACE_ME")
try:
    parsed = urlsplit(endpoint)
except ValueError as exc:
    raise SystemExit(f"{field_name} must be a valid http(s) URL: {exc}")
if parsed.scheme not in ("http", "https"):
    raise SystemExit(f"{field_name} must use http:// or https:// scheme (got: {parsed.scheme or 'none'})")
if not parsed.hostname:
    raise SystemExit(f"{field_name} must include a host")
if parsed.username is not None or parsed.password is not None:
    raise SystemExit(f"{field_name} must not embed credentials in URL")
if parsed.query:
    raise SystemExit(f"{field_name} must not include query parameters")
if parsed.fragment:
    raise SystemExit(f"{field_name} must not include URL fragment")
if strict_transport_policy and parsed.scheme == "http":
    host = parsed.hostname
    is_loopback = False
    if host == "localhost":
        is_loopback = True
    else:
        try:
            is_loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            is_loopback = False
    if not is_loopback:
        raise SystemExit(f"{field_name} must use https:// in production-like envs (http:// allowed only for loopback hosts)")
PY
}

adapter_endpoint_identity() {
  local endpoint="$1"
  python3 - "$endpoint" <<'PY'
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

read_non_empty_secret() {
  local resolved_path="$1"
  if [[ ! -f "$resolved_path" ]]; then
    echo "secret file not found: $resolved_path"
    return 1
  fi
  local raw trimmed
  raw="$(cat "$resolved_path" 2>/dev/null)" || {
    echo "secret file unreadable: $resolved_path"
    return 1
  }
  trimmed="$(trim_string "$raw")"
  if [[ -z "$trimmed" ]]; then
    echo "secret file is empty after trim: $resolved_path"
    return 1
  fi
  printf "%s" "$trimmed"
}

declare -a errors=()

system_env="$(cfg_value system env)"
if [[ -z "$system_env" ]]; then
  system_env="paper"
fi
prod_like="false"
if is_production_env_profile "$system_env"; then
  prod_like="true"
fi

execution_enabled="$(normalize_bool_token "$(cfg_value execution enabled)")"
execution_mode="$(normalize_route_token "$(cfg_value execution mode)")"

if [[ "$execution_enabled" != "true" ]]; then
  cat <<EOF
=== Execution Adapter Preflight ===
config: $CONFIG_PATH
system_env: $system_env
execution_enabled: $execution_enabled
execution_mode: ${execution_mode:-paper}
preflight_verdict: SKIP
preflight_reason: execution.enabled is not true
EOF
  exit 0
fi

if [[ "$execution_mode" != "adapter_submit_confirm" ]]; then
  cat <<EOF
=== Execution Adapter Preflight ===
config: $CONFIG_PATH
system_env: $system_env
execution_enabled: $execution_enabled
execution_mode: ${execution_mode:-paper}
preflight_verdict: SKIP
preflight_reason: execution.mode is not adapter_submit_confirm
EOF
  exit 0
fi

signer_pubkey="$(trim_string "$(cfg_value execution execution_signer_pubkey)")"
submit_primary="$(trim_string "$(cfg_value execution submit_adapter_http_url)")"
submit_fallback="$(trim_string "$(cfg_value execution submit_adapter_fallback_http_url)")"
contract_version="$(trim_string "$(cfg_value execution submit_adapter_contract_version)")"
strict_policy_echo="$(normalize_bool_token "$(cfg_value execution submit_adapter_require_policy_echo)")"
submit_allowed_routes_csv="$(cfg_list_csv execution submit_allowed_routes)"
submit_route_order_csv="$(cfg_list_csv execution submit_route_order)"
default_route="$(normalize_route_token "$(cfg_value execution default_route)")"
if [[ -z "$default_route" ]]; then
  default_route="paper"
fi

auth_token_inline="$(trim_string "$(cfg_value execution submit_adapter_auth_token)")"
auth_token_file_raw="$(trim_string "$(cfg_value execution submit_adapter_auth_token_file)")"
hmac_key_id="$(trim_string "$(cfg_value execution submit_adapter_hmac_key_id)")"
hmac_secret_inline="$(trim_string "$(cfg_value execution submit_adapter_hmac_secret)")"
hmac_secret_file_raw="$(trim_string "$(cfg_value execution submit_adapter_hmac_secret_file)")"
hmac_ttl_sec_raw="$(trim_string "$(cfg_value execution submit_adapter_hmac_ttl_sec)")"
submit_route_max_slippage_bps_keys_csv="$(cfg_map_keys_csv execution submit_route_max_slippage_bps)"
submit_route_tip_lamports_keys_csv="$(cfg_map_keys_csv execution submit_route_tip_lamports)"
submit_route_compute_unit_limit_keys_csv="$(cfg_map_keys_csv execution submit_route_compute_unit_limit)"
submit_route_compute_unit_price_keys_csv="$(cfg_map_keys_csv execution submit_route_compute_unit_price_micro_lamports)"

if [[ -z "$signer_pubkey" ]]; then
  errors+=("execution.execution_signer_pubkey must be non-empty in adapter_submit_confirm mode")
fi
if [[ -z "$submit_primary" && -z "$submit_fallback" ]]; then
  errors+=("execution.submit_adapter_http_url or execution.submit_adapter_fallback_http_url must be set")
fi
if [[ -n "$submit_primary" ]]; then
  if ! endpoint_error="$(validate_adapter_endpoint_url "$submit_primary" "execution.submit_adapter_http_url" "$prod_like" 2>&1)"; then
    errors+=("$endpoint_error")
  fi
fi
if [[ -n "$submit_fallback" ]]; then
  if ! endpoint_error="$(validate_adapter_endpoint_url "$submit_fallback" "execution.submit_adapter_fallback_http_url" "$prod_like" 2>&1)"; then
    errors+=("$endpoint_error")
  fi
fi
if [[ -n "$submit_primary" && -n "$submit_fallback" ]]; then
  primary_identity="$(adapter_endpoint_identity "$submit_primary")"
  fallback_identity="$(adapter_endpoint_identity "$submit_fallback")"
  if [[ "$primary_identity" == "$fallback_identity" ]]; then
    errors+=("execution.submit_adapter_http_url and execution.submit_adapter_fallback_http_url must resolve to distinct endpoints")
  fi
fi

if [[ -z "$contract_version" ]]; then
  errors+=("execution.submit_adapter_contract_version must be non-empty")
elif [[ ${#contract_version} -gt 64 ]]; then
  errors+=("execution.submit_adapter_contract_version must be <= 64 chars")
elif [[ ! "$contract_version" =~ ^[A-Za-z0-9._-]+$ ]]; then
  errors+=("execution.submit_adapter_contract_version must contain only [A-Za-z0-9._-]")
fi

if [[ "$prod_like" == "true" && "$strict_policy_echo" != "true" ]]; then
  errors+=("execution.submit_adapter_require_policy_echo must be true in production-like env profiles")
fi

if [[ -z "${submit_allowed_routes_csv//[[:space:]]/}" ]]; then
  errors+=("execution.submit_allowed_routes must not be empty in adapter_submit_confirm mode")
elif ! csv_contains_route "$submit_allowed_routes_csv" "$default_route"; then
  errors+=("execution.default_route=$default_route must be present in execution.submit_allowed_routes")
fi

if duplicate_route="$(csv_first_duplicate_normalized "$submit_allowed_routes_csv")"; then
  errors+=("execution.submit_allowed_routes contains duplicate route after normalization: $duplicate_route")
fi

declare -a allowed_routes_normalized=()
allowed_routes_seen=""
IFS=',' read -r -a submit_allowed_routes_values <<< "$submit_allowed_routes_csv"
for raw_route in "${submit_allowed_routes_values[@]}"; do
  normalized_route="$(normalize_route_token "$raw_route")"
  if [[ -z "$normalized_route" ]]; then
    continue
  fi
  if printf '%s\n' "$allowed_routes_seen" | grep -Fqx -- "$normalized_route"; then
    continue
  fi
  allowed_routes_seen+="${normalized_route}"$'\n'
  allowed_routes_normalized+=("$normalized_route")
done

if [[ -n "${submit_route_order_csv//[[:space:]]/}" ]]; then
  if duplicate_route="$(csv_first_duplicate_normalized "$submit_route_order_csv")"; then
    errors+=("execution.submit_route_order contains duplicate route after normalization: $duplicate_route")
  fi
  IFS=',' read -r -a submit_route_order_values <<< "$submit_route_order_csv"
  for raw_route in "${submit_route_order_values[@]}"; do
    normalized_route="$(normalize_route_token "$raw_route")"
    if [[ -z "$normalized_route" ]]; then
      errors+=("execution.submit_route_order contains an empty route value")
      continue
    fi
    if ! csv_contains_route "$submit_allowed_routes_csv" "$normalized_route"; then
      errors+=("execution.submit_route_order route=$normalized_route must be present in execution.submit_allowed_routes")
    fi
  done
  if ! csv_contains_route "$submit_route_order_csv" "$default_route"; then
    errors+=("execution.submit_route_order must include execution.default_route=$default_route")
  fi
fi

validate_route_policy_map_coverage() {
  local field_name="$1"
  local map_keys_csv="$2"
  local duplicate_map_route normalized_route
  if [[ -z "${map_keys_csv//[[:space:]]/}" ]]; then
    errors+=("${field_name} must not be empty in adapter_submit_confirm mode")
    return
  fi
  if duplicate_map_route="$(csv_first_duplicate_normalized "$map_keys_csv")"; then
    errors+=("${field_name} contains duplicate route key after normalization: $duplicate_map_route")
  fi
  for normalized_route in "${allowed_routes_normalized[@]}"; do
    if ! csv_contains_route "$map_keys_csv" "$normalized_route"; then
      errors+=("${field_name} is missing entry for allowed route=$normalized_route")
    fi
  done
  if ! csv_contains_route "$map_keys_csv" "$default_route"; then
    errors+=("${field_name} is missing entry for default route=$default_route")
  fi
}

validate_route_policy_map_coverage "execution.submit_route_max_slippage_bps" "$submit_route_max_slippage_bps_keys_csv"
validate_route_policy_map_coverage "execution.submit_route_tip_lamports" "$submit_route_tip_lamports_keys_csv"
validate_route_policy_map_coverage "execution.submit_route_compute_unit_limit" "$submit_route_compute_unit_limit_keys_csv"
validate_route_policy_map_coverage "execution.submit_route_compute_unit_price_micro_lamports" "$submit_route_compute_unit_price_keys_csv"

if [[ -n "$auth_token_inline" && -n "$auth_token_file_raw" ]]; then
  errors+=("execution.submit_adapter_auth_token and execution.submit_adapter_auth_token_file cannot both be set")
fi
if [[ -n "$auth_token_file_raw" ]]; then
  auth_token_file_resolved="$(resolve_secret_path "$auth_token_file_raw")"
  if ! secret_error="$(read_non_empty_secret "$auth_token_file_resolved" 2>&1)"; then
    errors+=("execution.submit_adapter_auth_token_file invalid: $secret_error")
  fi
fi

if [[ -n "$hmac_secret_inline" && -n "$hmac_secret_file_raw" ]]; then
  errors+=("execution.submit_adapter_hmac_secret and execution.submit_adapter_hmac_secret_file cannot both be set")
fi
hmac_secret_present="false"
if [[ -n "$hmac_secret_inline" || -n "$hmac_secret_file_raw" ]]; then
  hmac_secret_present="true"
fi
if [[ -n "$hmac_secret_file_raw" ]]; then
  hmac_secret_file_resolved="$(resolve_secret_path "$hmac_secret_file_raw")"
  if ! secret_error="$(read_non_empty_secret "$hmac_secret_file_resolved" 2>&1)"; then
    errors+=("execution.submit_adapter_hmac_secret_file invalid: $secret_error")
  fi
fi

if [[ -n "$hmac_key_id" && "$hmac_secret_present" != "true" ]]; then
  errors+=("execution.submit_adapter_hmac_key_id requires non-empty HMAC secret (inline or file)")
fi
if [[ -z "$hmac_key_id" && "$hmac_secret_present" == "true" ]]; then
  errors+=("execution.submit_adapter_hmac_secret requires non-empty execution.submit_adapter_hmac_key_id")
fi
if [[ -n "$hmac_key_id" ]]; then
  if ! [[ "$hmac_ttl_sec_raw" =~ ^[0-9]+$ ]]; then
    errors+=("execution.submit_adapter_hmac_ttl_sec must be an integer when HMAC auth is enabled")
  elif (( hmac_ttl_sec_raw < 5 || hmac_ttl_sec_raw > 300 )); then
    errors+=("execution.submit_adapter_hmac_ttl_sec must be in 5..=300 when HMAC auth is enabled")
  fi
fi

echo "=== Execution Adapter Preflight ==="
echo "config: $CONFIG_PATH"
echo "system_env: $system_env"
echo "production_profile: $prod_like"
echo "execution_enabled: $execution_enabled"
echo "execution_mode: $execution_mode"
echo "default_route: $default_route"
echo "submit_allowed_routes_csv: ${submit_allowed_routes_csv:-<empty>}"
echo "submit_route_order_csv: ${submit_route_order_csv:-<empty>}"
echo "adapter_primary_url_set: $([[ -n "$submit_primary" ]] && echo true || echo false)"
echo "adapter_fallback_url_set: $([[ -n "$submit_fallback" ]] && echo true || echo false)"
echo "strict_policy_echo: $strict_policy_echo"
echo "auth_token_inline_set: $([[ -n "$auth_token_inline" ]] && echo true || echo false)"
echo "auth_token_file_set: $([[ -n "$auth_token_file_raw" ]] && echo true || echo false)"
echo "hmac_key_id_set: $([[ -n "$hmac_key_id" ]] && echo true || echo false)"
echo "hmac_secret_inline_set: $([[ -n "$hmac_secret_inline" ]] && echo true || echo false)"
echo "hmac_secret_file_set: $([[ -n "$hmac_secret_file_raw" ]] && echo true || echo false)"

if (( ${#errors[@]} > 0 )); then
  echo "preflight_verdict: FAIL"
  echo "error_count: ${#errors[@]}"
  for error in "${errors[@]}"; do
    echo "error: $error"
  done
  exit 1
fi

echo "preflight_verdict: PASS"
echo "preflight_reason: adapter runtime contract checks passed"
