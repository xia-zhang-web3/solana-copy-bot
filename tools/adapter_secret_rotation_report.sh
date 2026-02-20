#!/usr/bin/env bash
set -euo pipefail

ADAPTER_ENV_PATH="${ADAPTER_ENV_PATH:-/etc/solana-copy-bot/adapter.env}"
OUTPUT_DIR="${OUTPUT_DIR:-}"

if [[ ! -f "$ADAPTER_ENV_PATH" ]]; then
  echo "adapter env file not found: $ADAPTER_ENV_PATH" >&2
  exit 1
fi

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

normalize_bool_token() {
  local value
  local lower
  value="$(trim_string "$1")"
  lower="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
  case "$lower" in
  1 | true | yes | on)
    echo "true"
    ;;
  *)
    echo "false"
    ;;
  esac
}

resolve_path() {
  local raw="$1"
  if [[ "$raw" = /* ]]; then
    printf '%s' "$raw"
    return
  fi
  local base_dir
  base_dir="$(cd "$(dirname "$ADAPTER_ENV_PATH")" && pwd)"
  printf '%s/%s' "$base_dir" "$raw"
}

env_value() {
  local key="$1"
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
      key = trim(substr(line, 1, eq - 1))
      if (key != wanted_key) {
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
  ' "$ADAPTER_ENV_PATH"
}

list_secret_file_keys() {
  awk '
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
      key = trim(substr(line, 1, eq - 1))
      if (key ~ /^COPYBOT_ADAPTER_.*_FILE$/) {
        print key
      }
    }
  ' "$ADAPTER_ENV_PATH" | sort -u
}

list_route_auth_ids() {
  awk '
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
      key = trim(substr(line, 1, eq - 1))
      if (key ~ /^COPYBOT_ADAPTER_ROUTE_[A-Za-z0-9_]+_AUTH_TOKEN(_FILE)?$/) {
        route = key
        sub(/^COPYBOT_ADAPTER_ROUTE_/, "", route)
        sub(/_AUTH_TOKEN(_FILE)?$/, "", route)
        if (route != "") {
          print route
        }
      }
    }
  ' "$ADAPTER_ENV_PATH" | sort -u
}

is_secret_file_permission_restrictive() {
  local path="$1"
  python3 - "$path" <<'PY'
import os
import sys

path = sys.argv[1]
mode = os.stat(path).st_mode & 0o777
if mode & 0o077:
    print(f"{mode:o}")
    raise SystemExit(1)
raise SystemExit(0)
PY
}

validate_secret_file_non_empty() {
  local path="$1"
  python3 - "$path" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
raw = path.read_text()
if not raw.strip():
    raise SystemExit(1)
raise SystemExit(0)
PY
}

declare -a report_lines=()
declare -a secret_keys=()
declare -a errors=()
declare -a warnings=()

while IFS= read -r key; do
  [[ -z "$key" ]] && continue
  secret_keys+=("$key")
done < <(list_secret_file_keys)

for key in "${secret_keys[@]}"; do
  value="$(trim_string "$(env_value "$key")")"
  [[ -z "$value" ]] && continue
  resolved_path="$(resolve_path "$value")"
  if [[ ! -e "$resolved_path" ]]; then
    errors+=("$key missing file: $resolved_path")
    continue
  fi
  if [[ ! -r "$resolved_path" ]]; then
    errors+=("$key unreadable file: $resolved_path")
    continue
  fi
  if ! validate_secret_file_non_empty "$resolved_path"; then
    errors+=("$key empty after trim: $resolved_path")
    continue
  fi

  if ! mode="$(is_secret_file_permission_restrictive "$resolved_path" 2>/dev/null)"; then
    warnings+=("$key broad permissions (mode ${mode:-unknown}): $resolved_path")
    report_lines+=("secret_file[$key]: WARN broad_permissions path=$resolved_path")
  else
    report_lines+=("secret_file[$key]: OK path=$resolved_path")
  fi
done

bearer_inline="$(trim_string "$(env_value COPYBOT_ADAPTER_BEARER_TOKEN)")"
bearer_file="$(trim_string "$(env_value COPYBOT_ADAPTER_BEARER_TOKEN_FILE)")"
hmac_key_id="$(trim_string "$(env_value COPYBOT_ADAPTER_HMAC_KEY_ID)")"
hmac_secret_inline="$(trim_string "$(env_value COPYBOT_ADAPTER_HMAC_SECRET)")"
hmac_secret_file="$(trim_string "$(env_value COPYBOT_ADAPTER_HMAC_SECRET_FILE)")"
upstream_auth_inline="$(trim_string "$(env_value COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN)")"
upstream_auth_file="$(trim_string "$(env_value COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE)")"
allow_unauthenticated="$(normalize_bool_token "$(env_value COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED)")"

if [[ -n "$bearer_inline" && -n "$bearer_file" ]]; then
  errors+=("COPYBOT_ADAPTER_BEARER_TOKEN and COPYBOT_ADAPTER_BEARER_TOKEN_FILE cannot both be set")
fi
if [[ -n "$hmac_secret_inline" && -n "$hmac_secret_file" ]]; then
  errors+=("COPYBOT_ADAPTER_HMAC_SECRET and COPYBOT_ADAPTER_HMAC_SECRET_FILE cannot both be set")
fi
if [[ -n "$upstream_auth_inline" && -n "$upstream_auth_file" ]]; then
  errors+=("COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN and COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE cannot both be set")
fi
while IFS= read -r route_id; do
  [[ -z "$route_id" ]] && continue
  route_inline_key="COPYBOT_ADAPTER_ROUTE_${route_id}_AUTH_TOKEN"
  route_file_key="COPYBOT_ADAPTER_ROUTE_${route_id}_AUTH_TOKEN_FILE"
  route_inline="$(trim_string "$(env_value "$route_inline_key")")"
  route_file="$(trim_string "$(env_value "$route_file_key")")"
  if [[ -n "$route_inline" && -n "$route_file" ]]; then
    errors+=("${route_inline_key} and ${route_file_key} cannot both be set")
  fi
done < <(list_route_auth_ids)

if [[ "$allow_unauthenticated" != "true" ]]; then
  has_bearer="false"
  has_hmac_pair="false"
  if [[ -n "$bearer_inline" || -n "$bearer_file" ]]; then
    has_bearer="true"
  fi
  if [[ -n "$hmac_key_id" && ( -n "$hmac_secret_inline" || -n "$hmac_secret_file" ) ]]; then
    has_hmac_pair="true"
  fi
  if [[ "$has_bearer" != "true" && "$has_hmac_pair" != "true" ]]; then
    errors+=("inbound auth missing: set bearer token (inline/file) or HMAC key+secret (inline/file)")
  fi
fi

if [[ -n "$hmac_key_id" && -z "$hmac_secret_inline" && -z "$hmac_secret_file" ]]; then
  errors+=("HMAC key id is set but HMAC secret (inline/file) is missing")
fi
if [[ -z "$hmac_key_id" && ( -n "$hmac_secret_inline" || -n "$hmac_secret_file" ) ]]; then
  errors+=("HMAC secret is set but HMAC key id is missing")
fi

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"
verdict="PASS"
reason="secret files are readable, non-empty, and auth contract is satisfied"
exit_code=0
if ((${#errors[@]} > 0)); then
  verdict="FAIL"
  reason="secret rotation baseline checks failed"
  exit_code=1
elif ((${#warnings[@]} > 0)); then
  verdict="WARN"
  reason="secret rotation baseline checks passed with warnings (permissions hardening recommended)"
  exit_code=2
fi

summary="$({
  echo "=== Adapter Secret Rotation Report ==="
  echo "adapter_env_path: $ADAPTER_ENV_PATH"
  echo "timestamp_utc: $timestamp_utc"
  echo "secret_file_entries_total: ${#secret_keys[@]}"
  echo "secret_file_checks_warnings: ${#warnings[@]}"
  echo "secret_file_checks_errors: ${#errors[@]}"
  echo "allow_unauthenticated: $allow_unauthenticated"
  echo "rotation_readiness_verdict: $verdict"
  echo "rotation_readiness_reason: $reason"
  if ((${#report_lines[@]} > 0)); then
    echo "--- secret file checks ---"
    printf '%s\n' "${report_lines[@]}"
  fi
  if ((${#warnings[@]} > 0)); then
    echo "--- warnings ---"
    printf '%s\n' "${warnings[@]}"
  fi
  if ((${#errors[@]} > 0)); then
    echo "--- errors ---"
    printf '%s\n' "${errors[@]}"
  fi
})"

echo "$summary"

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR"
  artifact_path="$OUTPUT_DIR/adapter_secret_rotation_report_${timestamp_compact}.txt"
  printf '%s\n' "$summary" >"$artifact_path"
  echo "artifact_report: $artifact_path"
fi

exit "$exit_code"
