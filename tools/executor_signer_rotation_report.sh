#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

EXECUTOR_ENV_PATH="${EXECUTOR_ENV_PATH:-/etc/solana-copy-bot/executor.env}"
OUTPUT_DIR="${OUTPUT_DIR:-}"

if [[ ! -f "$EXECUTOR_ENV_PATH" ]]; then
  echo "executor env file not found: $EXECUTOR_ENV_PATH" >&2
  exit 1
fi

resolve_path() {
  local raw="$1"
  if [[ "$raw" = /* ]]; then
    printf '%s' "$raw"
    return
  fi
  local base_dir
  base_dir="$(cd "$(dirname "$EXECUTOR_ENV_PATH")" && pwd)"
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
  ' "$EXECUTOR_ENV_PATH"
}

is_owner_only_permissions() {
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

is_non_empty_trimmed_file() {
  local path="$1"
  python3 - "$path" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
if not path.read_text().strip():
    raise SystemExit(1)
raise SystemExit(0)
PY
}

declare -a warnings=()
declare -a errors=()
declare -a report_lines=()

signer_source_raw="$(trim_string "$(env_value COPYBOT_EXECUTOR_SIGNER_SOURCE)")"
if [[ -z "$signer_source_raw" ]]; then
  signer_source="file"
else
  signer_source="$(printf '%s' "$signer_source_raw" | tr '[:upper:]' '[:lower:]')"
fi
signer_pubkey="$(trim_string "$(env_value COPYBOT_EXECUTOR_SIGNER_PUBKEY)")"
signer_keypair_file="$(trim_string "$(env_value COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE)")"
signer_kms_key_id="$(trim_string "$(env_value COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID)")"

if [[ -z "$signer_pubkey" ]]; then
  errors+=("COPYBOT_EXECUTOR_SIGNER_PUBKEY must be set")
fi

signer_file_permissions_owner_only="n/a"
signer_file_non_empty="n/a"
signer_file_exists="n/a"
signer_file_readable="n/a"
resolved_signer_keypair_path=""

case "$signer_source" in
  file)
    if [[ -z "$signer_keypair_file" ]]; then
      errors+=("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be set when signer source is file")
    else
      resolved_signer_keypair_path="$(resolve_path "$signer_keypair_file")"
      if [[ ! -e "$resolved_signer_keypair_path" ]]; then
        errors+=("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE missing file: $resolved_signer_keypair_path")
        signer_file_exists="false"
      else
        signer_file_exists="true"
        if [[ ! -r "$resolved_signer_keypair_path" ]]; then
          errors+=("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE unreadable file: $resolved_signer_keypair_path")
          signer_file_readable="false"
        else
          signer_file_readable="true"
          if ! is_non_empty_trimmed_file "$resolved_signer_keypair_path"; then
            errors+=("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE is empty after trim: $resolved_signer_keypair_path")
            signer_file_non_empty="false"
          else
            signer_file_non_empty="true"
          fi
          if ! mode="$(is_owner_only_permissions "$resolved_signer_keypair_path" 2>/dev/null)"; then
            errors+=("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must use owner-only permissions (mode ${mode:-unknown}): $resolved_signer_keypair_path")
            signer_file_permissions_owner_only="false"
          else
            signer_file_permissions_owner_only="true"
          fi
        fi
      fi
    fi
    if [[ -n "$signer_kms_key_id" ]]; then
      errors+=("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be empty when signer source is file")
    fi
    ;;
  kms)
    if [[ -z "$signer_kms_key_id" ]]; then
      errors+=("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be set when signer source is kms")
    elif [[ ${#signer_kms_key_id} -gt 256 ]]; then
      errors+=("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be <= 256 chars")
    fi
    if [[ -n "$signer_keypair_file" ]]; then
      errors+=("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be empty when signer source is kms")
    fi
    ;;
  *)
    errors+=("COPYBOT_EXECUTOR_SIGNER_SOURCE must be one of: file,kms")
    ;;
esac

if [[ -n "$resolved_signer_keypair_path" ]]; then
  report_lines+=("signer_keypair_file_path: $resolved_signer_keypair_path")
fi

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"
verdict="PASS"
reason="signer source and key material checks passed"
exit_code=0
if ((${#errors[@]} > 0)); then
  verdict="FAIL"
  reason="signer rotation readiness checks failed"
  exit_code=1
elif ((${#warnings[@]} > 0)); then
  verdict="WARN"
  reason="signer rotation readiness checks passed with warnings"
  exit_code=2
fi
artifacts_written="false"
if [[ -n "$OUTPUT_DIR" ]]; then
  artifacts_written="true"
fi

summary="$({
  echo "=== Executor Signer Rotation Report ==="
  echo "executor_env_path: $EXECUTOR_ENV_PATH"
  echo "timestamp_utc: $timestamp_utc"
  echo "signer_source: $signer_source"
  echo "signer_pubkey_present: $(normalize_bool_token "$([[ -n "$signer_pubkey" ]] && echo true || echo false)")"
  echo "signer_keypair_file_configured: $(normalize_bool_token "$([[ -n "$signer_keypair_file" ]] && echo true || echo false)")"
  echo "signer_kms_key_id_configured: $(normalize_bool_token "$([[ -n "$signer_kms_key_id" ]] && echo true || echo false)")"
  echo "signer_file_exists: $signer_file_exists"
  echo "signer_file_readable: $signer_file_readable"
  echo "signer_file_non_empty: $signer_file_non_empty"
  echo "signer_file_permissions_owner_only: $signer_file_permissions_owner_only"
  echo "rotation_readiness_verdict: $verdict"
  echo "rotation_readiness_reason: $reason"
  echo "artifacts_written: $artifacts_written"
  if ((${#report_lines[@]} > 0)); then
    echo "--- details ---"
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
  artifact_path="$OUTPUT_DIR/executor_signer_rotation_report_${timestamp_compact}.txt"
  manifest_path="$OUTPUT_DIR/executor_signer_rotation_manifest_${timestamp_compact}.txt"
  printf '%s\n' "$summary" >"$artifact_path"
  report_sha256="$(sha256_file_value "$artifact_path")"
  cat >"$manifest_path" <<EOF
report_sha256: $report_sha256
EOF
  echo "artifact_report: $artifact_path"
  echo "artifact_manifest: $manifest_path"
  echo "report_sha256: $report_sha256"
fi

exit "$exit_code"
