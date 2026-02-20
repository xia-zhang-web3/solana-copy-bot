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

strip_wrapping_quotes() {
  local value="$1"
  local first_char="${value:0:1}"
  local last_char="${value: -1}"
  if [[ ${#value} -ge 2 && "$first_char" == "$last_char" && ( "$first_char" == "'" || "$first_char" == '"' ) ]]; then
    printf '%s' "${value:1:${#value}-2}"
  else
    printf '%s' "$value"
  fi
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
    function unquote(s, first, last) {
      first = substr(s, 1, 1)
      last = substr(s, length(s), 1)
      if (length(s) >= 2 && ((first == "\"" && last == "\"") || (first == "\x27" && last == "\x27"))) {
        return substr(s, 2, length(s) - 2)
      }
      return s
    }
    {
      line = $0
      sub(/#.*/, "", line)
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
      print value
      exit
    }
  ' "$ADAPTER_ENV_PATH"
}

list_secret_file_keys() {
  awk '
    function trim(s) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", s)
      return s
    }
    {
      line = $0
      sub(/#.*/, "", line)
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
allow_unauthenticated="$(normalize_bool_token "$(env_value COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED)")"

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
