#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-configs/paper.toml}}"
EXECUTION_ROUTE_TIP_LAMPORTS_MAX=100000000

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
fi

declare -a errors=()

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

cfg_map_entries_csv() {
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
          map_value = trim(substr(pair, eq + 1))
          gsub(/^"|"$/, "", map_key)
          gsub(/^'\''|'\''$/, "", map_key)
          gsub(/[[:space:]]/, "", map_key)
          gsub(/^"|"$/, "", map_value)
          gsub(/^'\''|'\''$/, "", map_value)
          if (map_key == "" || map_value == "") {
            continue
          }
          if (out != "") {
            out = out ","
          }
          out = out map_key ":" map_value
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

parse_env_bool_token() {
  local raw="$1"
  raw="$(trim_string "$raw")"
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

cfg_or_env_string() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  if [[ -n "${!env_name+x}" ]]; then
    printf "%s" "${!env_name}"
    return
  fi
  cfg_value "$section" "$key"
}

cfg_or_env_trimmed_nonempty_string() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  if [[ -n "${!env_name+x}" ]]; then
    local trimmed_env
    trimmed_env="$(trim_string "${!env_name}")"
    if [[ -n "$trimmed_env" ]]; then
      printf "%s" "$trimmed_env"
      return
    fi
  fi
  cfg_value "$section" "$key"
}

cfg_or_env_bool() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local file_value parsed_env
  file_value="$(normalize_bool_token "$(cfg_value "$section" "$key")")"
  if [[ -n "${!env_name+x}" ]]; then
    parsed_env="$(parse_env_bool_token "${!env_name}")"
    if [[ -n "$parsed_env" ]]; then
      printf "%s" "$parsed_env"
      return
    fi
  fi
  printf "%s" "$file_value"
}

cfg_or_env_u64_string() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  if [[ -n "${!env_name+x}" ]]; then
    local raw_env
    raw_env="$(trim_string "${!env_name}")"
    if [[ "$raw_env" =~ ^[0-9]+$ ]]; then
      printf "%s" "$raw_env"
      return
    fi
  fi
  cfg_value "$section" "$key"
}

parse_execution_route_list_env_csv() {
  local csv="$1"
  local env_name="$2"
  python3 - "$csv" "$env_name" <<'PY'
import sys

csv = sys.argv[1]
env_name = sys.argv[2]
seen = set()
values = []

for token in csv.split(","):
    route = token.strip()
    if not route:
        continue
    normalized = route.lower()
    if normalized in seen:
        print(
            f"{env_name} contains duplicate route after normalization: {normalized}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    seen.add(normalized)
    values.append(route)

print(",".join(values))
PY
}

parse_execution_route_map_env_keys_csv() {
  local csv="$1"
  local env_name="$2"
  local value_type="$3"
  python3 - "$csv" "$env_name" "$value_type" <<'PY'
import sys

csv = sys.argv[1]
env_name = sys.argv[2]
value_type = sys.argv[3]
seen = set()
keys = []

for token in csv.split(","):
    token = token.strip()
    if not token:
        continue
    if ":" not in token:
        continue
    route, raw_value = token.split(":", 1)
    route = route.strip().lower()
    if not route:
        continue
    raw_value = raw_value.strip()
    try:
        if value_type == "f64":
            float(raw_value)
        elif value_type == "u64":
            parsed = int(raw_value, 10)
            if parsed < 0 or parsed > (2**64 - 1):
                raise ValueError("out of range")
        elif value_type == "u32":
            parsed = int(raw_value, 10)
            if parsed < 0 or parsed > (2**32 - 1):
                raise ValueError("out of range")
        else:
            raise ValueError("unsupported value type")
    except Exception:
        continue
    if route in seen:
        print(
            f"{env_name} contains duplicate route after normalization: {route}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    seen.add(route)
    keys.append(route)

print(",".join(keys))
PY
}

parse_execution_route_map_env_pairs_csv() {
  local csv="$1"
  local env_name="$2"
  local value_type="$3"
  python3 - "$csv" "$env_name" "$value_type" <<'PY'
import sys

csv = sys.argv[1]
env_name = sys.argv[2]
value_type = sys.argv[3]
seen = set()
pairs = []

for token in csv.split(","):
    token = token.strip()
    if not token:
        continue
    if ":" not in token:
        continue
    route, raw_value = token.split(":", 1)
    route = route.strip().lower()
    if not route:
        continue
    raw_value = raw_value.strip()
    try:
        if value_type == "f64":
            parsed_value = float(raw_value)
            value_repr = repr(parsed_value)
        elif value_type == "u64":
            parsed_value = int(raw_value, 10)
            if parsed_value < 0 or parsed_value > (2**64 - 1):
                raise ValueError("out of range")
            value_repr = str(parsed_value)
        elif value_type == "u32":
            parsed_value = int(raw_value, 10)
            if parsed_value < 0 or parsed_value > (2**32 - 1):
                raise ValueError("out of range")
            value_repr = str(parsed_value)
        else:
            raise ValueError("unsupported value type")
    except Exception:
        continue
    if route in seen:
        print(
            f"{env_name} contains duplicate route after normalization: {route}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    seen.add(route)
    pairs.append(f"{route}:{value_repr}")

print(",".join(pairs))
PY
}

cfg_or_env_route_list_csv() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local file_csv env_csv parsed_csv
  file_csv="$(cfg_list_csv "$section" "$key")"
  if [[ -z "${!env_name+x}" ]]; then
    printf "%s" "$file_csv"
    return
  fi
  env_csv="${!env_name}"
  if ! parsed_csv="$(parse_execution_route_list_env_csv "$env_csv" "$env_name" 2>&1)"; then
    errors+=("$parsed_csv")
    printf "%s" "$file_csv"
    return
  fi
  if [[ -n "${parsed_csv//[[:space:]]/}" ]]; then
    printf "%s" "$parsed_csv"
  else
    printf "%s" "$file_csv"
  fi
}

cfg_or_env_route_map_keys_csv() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local value_type="$4"
  local file_csv env_csv parsed_csv
  file_csv="$(cfg_map_keys_csv "$section" "$key")"
  if [[ -z "${!env_name+x}" ]]; then
    printf "%s" "$file_csv"
    return
  fi
  env_csv="${!env_name}"
  if ! parsed_csv="$(parse_execution_route_map_env_keys_csv "$env_csv" "$env_name" "$value_type" 2>&1)"; then
    errors+=("$parsed_csv")
    printf "%s" "$file_csv"
    return
  fi
  if [[ -n "${parsed_csv//[[:space:]]/}" ]]; then
    printf "%s" "$parsed_csv"
  else
    printf "%s" "$file_csv"
  fi
}

cfg_or_env_route_map_pairs_csv() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local value_type="$4"
  local default_pairs_csv="$5"
  local file_raw_csv file_pairs_csv env_csv parsed_env_pairs
  file_raw_csv="$(cfg_map_entries_csv "$section" "$key")"
  if [[ -z "${file_raw_csv//[[:space:]]/}" ]]; then
    file_raw_csv="$default_pairs_csv"
  fi
  if ! file_pairs_csv="$(parse_execution_route_map_env_pairs_csv "$file_raw_csv" "execution.$key" "$value_type" 2>&1)"; then
    errors+=("$file_pairs_csv")
    file_pairs_csv=""
  fi
  if [[ -z "${!env_name+x}" ]]; then
    printf "%s" "$file_pairs_csv"
    return
  fi
  env_csv="${!env_name}"
  if ! parsed_env_pairs="$(parse_execution_route_map_env_pairs_csv "$env_csv" "$env_name" "$value_type" 2>&1)"; then
    errors+=("$parsed_env_pairs")
    printf "%s" "$file_pairs_csv"
    return
  fi
  if [[ -n "${parsed_env_pairs//[[:space:]]/}" ]]; then
    printf "%s" "$parsed_env_pairs"
  else
    printf "%s" "$file_pairs_csv"
  fi
}

route_pairs_to_keys_csv() {
  local pairs_csv="$1"
  local -a pairs=()
  local route out=""
  if [[ -z "${pairs_csv//[[:space:]]/}" ]]; then
    printf ""
    return
  fi
  IFS=',' read -r -a pairs <<< "$pairs_csv"
  for pair in "${pairs[@]}"; do
    route="${pair%%:*}"
    if [[ -z "$route" ]]; then
      continue
    fi
    if [[ -n "$out" ]]; then
      out+=","
    fi
    out+="$route"
  done
  printf "%s" "$out"
}

cfg_or_env_f64_string() {
  local section="$1"
  local key="$2"
  local env_name="$3"
  local default_value="$4"
  local raw parsed
  raw="$(cfg_value "$section" "$key")"
  parsed="$(python3 - "$raw" <<'PY'
import sys
s = sys.argv[1].strip()
if not s:
    raise SystemExit(0)
try:
    print(repr(float(s)))
except Exception:
    pass
PY
)"
  if [[ -z "$parsed" ]]; then
    parsed="$default_value"
  fi
  if [[ -n "${!env_name+x}" ]]; then
    local env_parsed
    env_parsed="$(python3 - "${!env_name}" <<'PY'
import sys
s = sys.argv[1].strip()
if not s:
    raise SystemExit(0)
try:
    print(repr(float(s)))
except Exception:
    pass
PY
)"
    if [[ -n "$env_parsed" ]]; then
      printf "%s" "$env_parsed"
      return
    fi
  fi
  printf "%s" "$parsed"
}

append_multiline_errors() {
  local block="$1"
  local line
  while IFS= read -r line; do
    if [[ -n "${line//[[:space:]]/}" ]]; then
      errors+=("$line")
    fi
  done <<< "$block"
}

validate_route_policy_numeric_parity() {
  local default_route="$1"
  local allowed_routes_csv="$2"
  local slippage_bps_raw="$3"
  local pretrade_max_priority_fee_raw="$4"
  local cap_pairs_csv="$5"
  local tip_pairs_csv="$6"
  local limit_pairs_csv="$7"
  local price_pairs_csv="$8"
  local tip_max="$9"
  python3 - "$default_route" "$allowed_routes_csv" "$slippage_bps_raw" "$pretrade_max_priority_fee_raw" "$cap_pairs_csv" "$tip_pairs_csv" "$limit_pairs_csv" "$price_pairs_csv" "$tip_max" <<'PY'
import math
import sys

default_route = sys.argv[1].strip().lower()
allowed_routes_csv = sys.argv[2]
slippage_bps_raw = sys.argv[3]
pretrade_max_priority_fee_raw = sys.argv[4]
cap_pairs_csv = sys.argv[5]
tip_pairs_csv = sys.argv[6]
limit_pairs_csv = sys.argv[7]
price_pairs_csv = sys.argv[8]
tip_max = int(sys.argv[9])
errors = []

def parse_pairs(csv, type_name):
    values = {}
    if not csv.strip():
        return values
    for token in csv.split(","):
        token = token.strip()
        if not token:
            continue
        if ":" not in token:
            continue
        route, raw_value = token.split(":", 1)
        route = route.strip().lower()
        if not route:
            continue
        raw_value = raw_value.strip()
        try:
            if type_name == "f64":
                value = float(raw_value)
            else:
                value = int(raw_value, 10)
        except Exception:
            continue
        values[route] = value
    return values

route_caps = parse_pairs(cap_pairs_csv, "f64")
route_tips = parse_pairs(tip_pairs_csv, "u64")
route_limits = parse_pairs(limit_pairs_csv, "u32")
route_prices = parse_pairs(price_pairs_csv, "u64")

for route, cap in route_caps.items():
    if not math.isfinite(cap) or cap <= 0.0:
        errors.append(
            f"execution.submit_route_max_slippage_bps route={route} must be finite and > 0, got {cap}"
        )

for route, tip in route_tips.items():
    if tip < 0 or tip > tip_max:
        errors.append(
            f"execution.submit_route_tip_lamports route={route} must be in 0..={tip_max}, got {tip}"
        )

for route, limit in route_limits.items():
    if limit <= 0 or limit > 1_400_000:
        errors.append(
            f"execution.submit_route_compute_unit_limit route={route} must be in 1..=1400000, got {limit}"
        )

for route, price in route_prices.items():
    if price <= 0 or price > 10_000_000:
        errors.append(
            f"execution.submit_route_compute_unit_price_micro_lamports route={route} must be in 1..=10000000, got {price}"
        )

try:
    slippage_bps = float(slippage_bps_raw)
except Exception:
    slippage_bps = 50.0
if not math.isfinite(slippage_bps):
    slippage_bps = 50.0

try:
    pretrade_max_priority_fee = int(pretrade_max_priority_fee_raw, 10)
except Exception:
    pretrade_max_priority_fee = 0
if pretrade_max_priority_fee < 0:
    pretrade_max_priority_fee = 0

default_route_cap = route_caps.get(default_route)
if default_route_cap is not None and slippage_bps > default_route_cap:
    errors.append(
        f"execution.slippage_bps ({slippage_bps}) cannot exceed cap ({default_route_cap}) for default route {default_route}"
    )

default_route_limit = route_limits.get(default_route)
if default_route_limit is not None and default_route_limit < 100_000:
    errors.append(
        f"execution.submit_route_compute_unit_limit default route {default_route} limit ({default_route_limit}) is too low for reliable swaps; expected >= 100000"
    )

if pretrade_max_priority_fee > 0:
    allowed_routes = []
    seen_routes = set()
    for token in allowed_routes_csv.split(","):
        route = token.strip().lower()
        if not route or route in seen_routes:
            continue
        seen_routes.add(route)
        allowed_routes.append(route)
    for route in allowed_routes:
        route_price = route_prices.get(route)
        if route_price is None:
            continue
        if route_price > pretrade_max_priority_fee:
            errors.append(
                f"execution.submit_route_compute_unit_price_micro_lamports route {route} price ({route_price}) cannot exceed execution.pretrade_max_priority_fee_lamports ({pretrade_max_priority_fee}) (unit: micro-lamports per CU for both fields)"
            )

if errors:
    for err in errors:
        print(err)
    raise SystemExit(1)
PY
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

system_env="$(cfg_value system env)"
if [[ -z "$system_env" ]]; then
  system_env="paper"
fi
prod_like="false"
if is_production_env_profile "$system_env"; then
  prod_like="true"
fi

execution_enabled="$(cfg_or_env_bool execution enabled SOLANA_COPY_BOT_EXECUTION_ENABLED)"
execution_mode="$(normalize_route_token "$(cfg_or_env_trimmed_nonempty_string execution mode SOLANA_COPY_BOT_EXECUTION_MODE)")"

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

signer_pubkey="$(trim_string "$(cfg_or_env_string execution execution_signer_pubkey SOLANA_COPY_BOT_EXECUTION_SIGNER_PUBKEY)")"
submit_primary="$(trim_string "$(cfg_or_env_string execution submit_adapter_http_url SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HTTP_URL)")"
submit_fallback="$(trim_string "$(cfg_or_env_string execution submit_adapter_fallback_http_url SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_FALLBACK_HTTP_URL)")"
contract_version="$(trim_string "$(cfg_or_env_trimmed_nonempty_string execution submit_adapter_contract_version SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_CONTRACT_VERSION)")"
strict_policy_echo="$(cfg_or_env_bool execution submit_adapter_require_policy_echo SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO)"
submit_allowed_routes_csv="$(cfg_or_env_route_list_csv execution submit_allowed_routes SOLANA_COPY_BOT_EXECUTION_SUBMIT_ALLOWED_ROUTES)"
submit_route_order_csv="$(cfg_or_env_route_list_csv execution submit_route_order SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER)"
default_route="$(normalize_route_token "$(cfg_or_env_trimmed_nonempty_string execution default_route SOLANA_COPY_BOT_EXECUTION_DEFAULT_ROUTE)")"
if [[ -z "$default_route" ]]; then
  default_route="paper"
fi

auth_token_inline="$(trim_string "$(cfg_or_env_string execution submit_adapter_auth_token SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_AUTH_TOKEN)")"
auth_token_file_raw="$(trim_string "$(cfg_or_env_string execution submit_adapter_auth_token_file SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_AUTH_TOKEN_FILE)")"
hmac_key_id="$(trim_string "$(cfg_or_env_string execution submit_adapter_hmac_key_id SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_KEY_ID)")"
hmac_secret_inline="$(trim_string "$(cfg_or_env_string execution submit_adapter_hmac_secret SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_SECRET)")"
hmac_secret_file_raw="$(trim_string "$(cfg_or_env_string execution submit_adapter_hmac_secret_file SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_SECRET_FILE)")"
hmac_ttl_sec_raw="$(trim_string "$(cfg_or_env_u64_string execution submit_adapter_hmac_ttl_sec SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_HMAC_TTL_SEC)")"
slippage_bps_raw="$(cfg_or_env_f64_string execution slippage_bps SOLANA_COPY_BOT_EXECUTION_SLIPPAGE_BPS 50.0)"
pretrade_max_priority_fee_lamports_raw="$(trim_string "$(cfg_or_env_u64_string execution pretrade_max_priority_fee_lamports SOLANA_COPY_BOT_EXECUTION_PRETRADE_MAX_PRIORITY_FEE_LAMPORTS)")"
if [[ -z "$pretrade_max_priority_fee_lamports_raw" ]]; then
  pretrade_max_priority_fee_lamports_raw="0"
fi
submit_route_max_slippage_bps_pairs_csv="$(cfg_or_env_route_map_pairs_csv execution submit_route_max_slippage_bps SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS f64 "paper:50.0")"
submit_route_tip_lamports_pairs_csv="$(cfg_or_env_route_map_pairs_csv execution submit_route_tip_lamports SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS u64 "paper:0")"
submit_route_compute_unit_limit_pairs_csv="$(cfg_or_env_route_map_pairs_csv execution submit_route_compute_unit_limit SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT u32 "paper:300000")"
submit_route_compute_unit_price_pairs_csv="$(cfg_or_env_route_map_pairs_csv execution submit_route_compute_unit_price_micro_lamports SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS u64 "paper:1000")"
submit_route_max_slippage_bps_keys_csv="$(route_pairs_to_keys_csv "$submit_route_max_slippage_bps_pairs_csv")"
submit_route_tip_lamports_keys_csv="$(route_pairs_to_keys_csv "$submit_route_tip_lamports_pairs_csv")"
submit_route_compute_unit_limit_keys_csv="$(route_pairs_to_keys_csv "$submit_route_compute_unit_limit_pairs_csv")"
submit_route_compute_unit_price_keys_csv="$(route_pairs_to_keys_csv "$submit_route_compute_unit_price_pairs_csv")"

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

if ! numeric_parity_errors="$(validate_route_policy_numeric_parity \
  "$default_route" \
  "$submit_allowed_routes_csv" \
  "$slippage_bps_raw" \
  "$pretrade_max_priority_fee_lamports_raw" \
  "$submit_route_max_slippage_bps_pairs_csv" \
  "$submit_route_tip_lamports_pairs_csv" \
  "$submit_route_compute_unit_limit_pairs_csv" \
  "$submit_route_compute_unit_price_pairs_csv" \
  "$EXECUTION_ROUTE_TIP_LAMPORTS_MAX" 2>&1)"; then
  append_multiline_errors "$numeric_parity_errors"
fi

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
