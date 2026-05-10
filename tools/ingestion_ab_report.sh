#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  tools/ingestion_ab_report.sh
    --control-config <path>
    --candidate-config <path>
    [--control-db <path>]
    [--candidate-db <path>]
    [--control-service <name>]
    [--candidate-service <name>]
    [--window-minutes <int>]
    [--mode replay|live]
    [--fixture-id <id>]
    [--fixture-sha256 <sha256>]
    [--output-json <path>]
    [--buy-target-pct <float>]
    [--sell-max-degrade-pct <float>]

Defaults:
  --window-minutes 360
  --mode live
  --buy-target-pct 15
  --sell-max-degrade-pct 3

Notes:
  - DB metrics are read from observed_swaps/copy_signals/risk_events.
  - Telemetry gates are evaluated from journald ingestion/app logs when service names are provided.
  - Exit code is 0 on pass, 2 on gate failure.
USAGE
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
mkdir -p "${REPO_ROOT}/state"

CONTROL_CONFIG=""
CANDIDATE_CONFIG=""
CONTROL_DB=""
CANDIDATE_DB=""
CONTROL_SERVICE=""
CANDIDATE_SERVICE=""
WINDOW_MINUTES=360
MODE="live"
FIXTURE_ID=""
FIXTURE_SHA256=""
OUTPUT_JSON=""
BUY_TARGET_PCT="15"
SELL_MAX_DEGRADE_PCT="3"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --control-config)
      CONTROL_CONFIG="$2"
      shift 2
      ;;
    --candidate-config)
      CANDIDATE_CONFIG="$2"
      shift 2
      ;;
    --control-db)
      CONTROL_DB="$2"
      shift 2
      ;;
    --candidate-db)
      CANDIDATE_DB="$2"
      shift 2
      ;;
    --control-service)
      CONTROL_SERVICE="$2"
      shift 2
      ;;
    --candidate-service)
      CANDIDATE_SERVICE="$2"
      shift 2
      ;;
    --window-minutes)
      WINDOW_MINUTES="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --fixture-id)
      FIXTURE_ID="$2"
      shift 2
      ;;
    --fixture-sha256)
      FIXTURE_SHA256="$2"
      shift 2
      ;;
    --output-json)
      OUTPUT_JSON="$2"
      shift 2
      ;;
    --buy-target-pct)
      BUY_TARGET_PCT="$2"
      shift 2
      ;;
    --sell-max-degrade-pct)
      SELL_MAX_DEGRADE_PCT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

cfg_value() {
  local config_path="$1"
  local section="$2"
  local key="$3"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ { in_section = ($0 == section) }
    in_section {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = $2
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "$config_path"
}

resolve_db_path() {
  local config_path="$1"
  local explicit_db="$2"
  if [[ -n "${explicit_db}" ]]; then
    printf '%s\n' "${explicit_db}"
    return 0
  fi
  cfg_value "${config_path}" sqlite path
}

sha256_file() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print $1}'
  else
    shasum -a 256 "$file_path" | awk '{print $1}'
  fi
}

collect_journal() {
  local service_name="$1"
  local out_file="$2"
  if [[ -z "${service_name}" ]]; then
    : >"${out_file}"
    return 0
  fi
  if journalctl -u "${service_name}" -n 5 --no-pager >/dev/null 2>&1; then
    journalctl -u "${service_name}" --since "${WINDOW_MINUTES} min ago" --no-pager -o cat >"${out_file}" || true
    return 0
  fi
  if sudo -n journalctl -u "${service_name}" -n 5 --no-pager >/dev/null 2>&1; then
    sudo -n journalctl -u "${service_name}" --since "${WINDOW_MINUTES} min ago" --no-pager -o cat >"${out_file}" || true
    return 0
  fi
  : >"${out_file}"
}

if [[ -z "${CONTROL_CONFIG}" || -z "${CANDIDATE_CONFIG}" ]]; then
  echo "--control-config and --candidate-config are required" >&2
  usage >&2
  exit 1
fi

if ! [[ "${WINDOW_MINUTES}" =~ ^[0-9]+$ ]] || ((WINDOW_MINUTES < 1)); then
  echo "--window-minutes must be integer >= 1" >&2
  exit 1
fi

case "${MODE}" in
  replay|live) ;;
  *)
    echo "--mode must be replay or live (got: ${MODE})" >&2
    exit 1
    ;;
esac

for cfg in "${CONTROL_CONFIG}" "${CANDIDATE_CONFIG}"; do
  if [[ ! -f "${cfg}" ]]; then
    echo "config file not found: ${cfg}" >&2
    exit 1
  fi
done

CONTROL_DB="$(resolve_db_path "${CONTROL_CONFIG}" "${CONTROL_DB}")"
CANDIDATE_DB="$(resolve_db_path "${CANDIDATE_CONFIG}" "${CANDIDATE_DB}")"

for db in "${CONTROL_DB}" "${CANDIDATE_DB}"; do
  if [[ ! -f "${db}" ]]; then
    echo "sqlite db not found: ${db}" >&2
    exit 1
  fi
done

CONTROL_LOG_FILE="$(mktemp "${REPO_ROOT}/state/control_ingestion_log.XXXXXX")"
CANDIDATE_LOG_FILE="$(mktemp "${REPO_ROOT}/state/candidate_ingestion_log.XXXXXX")"
trap 'rm -f "${CONTROL_LOG_FILE}" "${CANDIDATE_LOG_FILE}"' EXIT

collect_journal "${CONTROL_SERVICE}" "${CONTROL_LOG_FILE}"
collect_journal "${CANDIDATE_SERVICE}" "${CANDIDATE_LOG_FILE}"

python3 "${SCRIPT_DIR}/lib/ingestion_ab_report.py" \
  "${CONTROL_DB}" \
  "${CANDIDATE_DB}" \
  "${WINDOW_MINUTES}" \
  "${MODE}" \
  "${FIXTURE_ID}" \
  "${FIXTURE_SHA256}" \
  "${CONTROL_LOG_FILE}" \
  "${CANDIDATE_LOG_FILE}" \
  "${CONTROL_SERVICE}" \
  "${CANDIDATE_SERVICE}" \
  "${OUTPUT_JSON}" \
  "${BUY_TARGET_PCT}" \
  "${SELL_MAX_DEGRADE_PCT}" \
  "${CONTROL_CONFIG}" \
  "${CANDIDATE_CONFIG}" \
  "$(sha256_file "${CONTROL_CONFIG}")" \
  "$(sha256_file "${CANDIDATE_CONFIG}")" \
  "$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
