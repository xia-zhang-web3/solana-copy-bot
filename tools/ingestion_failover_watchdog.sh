#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

POLICY_FILE="${POLICY_FILE:-${REPO_ROOT}/ops/ingestion_failover_policy.toml}"
STATE_FILE="${STATE_FILE:-${REPO_ROOT}/state/ingestion_failover_state.json}"
COOLDOWN_FILE="${COOLDOWN_FILE:-${REPO_ROOT}/state/ingestion_failover_cooldown.json}"
OVERRIDE_FILE="${OVERRIDE_FILE:-${REPO_ROOT}/state/ingestion_source_override.env}"
CONFIG_PATH="${CONFIG_PATH:-${SOLANA_COPY_BOT_CONFIG:-${REPO_ROOT}/configs/paper.toml}}"

if [[ ! -f "${POLICY_FILE}" ]]; then
  echo "policy file not found: ${POLICY_FILE}" >&2
  exit 1
fi

policy_value() {
  local key="$1"
  local default="${2:-}"
  local value
  value="$(awk -F'=' -v key="$key" '
    {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        right = $2
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", right)
        gsub(/^"|"$/, "", right)
        print right
        exit
      }
    }
  ' "${POLICY_FILE}")"
  if [[ -n "${value}" ]]; then
    printf '%s\n' "${value}"
  else
    printf '%s\n' "${default}"
  fi
}

parse_uint() {
  local raw="$1"
  if [[ "${raw}" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "${raw}"
  else
    printf '0\n'
  fi
}

parse_float() {
  local raw="$1"
  if [[ "${raw}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    printf '%s\n' "${raw}"
  else
    printf '0\n'
  fi
}

epoch_to_iso_utc() {
  local epoch="$1"
  python3 - "${epoch}" <<'PY'
import sys
from datetime import datetime, timezone

epoch = int(sys.argv[1])
print(datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
}

atomic_rename_with_fsync() {
  local tmp_path="$1"
  local target_path="$2"
  python3 - "$tmp_path" "$target_path" <<'PY'
import os
import sys

tmp_path, target_path = sys.argv[1], sys.argv[2]
with open(tmp_path, "rb") as fh:
    os.fsync(fh.fileno())
os.replace(tmp_path, target_path)
dir_path = os.path.dirname(os.path.abspath(target_path)) or "."
dir_fd = os.open(dir_path, os.O_RDONLY)
try:
    os.fsync(dir_fd)
finally:
    os.close(dir_fd)
PY
}

window_delta() {
  local key="$1"
  local window_seconds="$2"
  jq -r \
    --arg key "${key}" \
    --argjson now_epoch "${now_epoch}" \
    --argjson window_seconds "${window_seconds}" \
    '
      def points: [(.history // [])[] | select(.epoch >= ($now_epoch - $window_seconds))];
      (points) as $p
      | if ($p|length) >= 2 then (($p[-1][$key] // 0) - ($p[0][$key] // 0)) else 0 end
    ' "${STATE_FILE}"
}

WINDOW_HOURS="$(policy_value runtime_snapshot_window_hours 1)"
RISK_MINUTES="$(policy_value runtime_snapshot_risk_minutes 15)"
COOLDOWN_MINUTES="$(policy_value cooldown_minutes 15)"
LAG_THRESHOLD_MS="$(policy_value lag_p95_threshold_ms 10000)"
LAG_BREACH_CONSECUTIVE="$(policy_value lag_breach_consecutive 5)"
REPLACED_THRESHOLD="$(policy_value replaced_ratio_threshold 0.93)"
REPLACED_BREACH_CONSECUTIVE="$(policy_value replaced_ratio_breach_consecutive 5)"
RECONNECT_STORM_WINDOW_SECONDS="$(policy_value reconnect_storm_window_seconds 300)"
RECONNECT_STORM_THRESHOLD="$(policy_value reconnect_storm_threshold 6)"
REJECT_RATE_WINDOW_SECONDS="$(policy_value reject_rate_window_seconds 300)"
REJECT_RATE_THRESHOLD="$(policy_value reject_rate_threshold 0.20)"
REJECT_RATE_MIN_DENOMINATOR="$(policy_value reject_rate_min_denominator 500)"
NO_PROCESSED_WINDOW_SECONDS="$(policy_value no_processed_window_seconds 120)"
NO_PROCESSED_MIN_INBOUND="$(policy_value no_processed_min_inbound 200)"
SERVICE_NAME="$(policy_value service_name solana-copy-bot)"
ALLOW_RESTART="$(policy_value allow_restart false)"
HISTORY_RETENTION_SECONDS=1800

mkdir -p "${REPO_ROOT}/state"

now_epoch="$(date -u +%s)"
now_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if [[ -f "${COOLDOWN_FILE}" ]]; then
  cooldown_until_epoch="$(jq -r '.cooldown_until_epoch // 0' "${COOLDOWN_FILE}" 2>/dev/null || echo 0)"
  if [[ "${cooldown_until_epoch}" =~ ^[0-9]+$ ]] && (( now_epoch < cooldown_until_epoch )); then
    echo "watchdog: cooldown active until epoch=${cooldown_until_epoch}, skipping checks"
    exit 0
  fi
fi

snapshot="$("${SCRIPT_DIR}/runtime_snapshot.sh" "${WINDOW_HOURS}" "${RISK_MINUTES}" 2>/dev/null || true)"
if [[ -z "${snapshot}" ]]; then
  echo "watchdog: runtime snapshot unavailable, skipping"
  exit 0
fi

extract_metric() {
  local key="$1"
  printf '%s\n' "${snapshot}" | awk -F': ' -v key="${key}" '$1 == key { print $2; exit }'
}

lag_p95="$(parse_uint "$(extract_metric ingestion_lag_ms_p95)")"
replaced_ratio="$(parse_float "$(extract_metric replaced_ratio_last_interval)")"
reconnect_count_raw="$(extract_metric reconnect_count)"
reconnect_count="$(parse_uint "${reconnect_count_raw}")"
parse_rejected_total="$(parse_uint "$(extract_metric parse_rejected_total)")"
grpc_decode_errors="$(parse_uint "$(extract_metric grpc_decode_errors)")"
grpc_message_total_raw="$(extract_metric grpc_message_total)"
grpc_message_total="$(parse_uint "${grpc_message_total_raw}")"
processed_total="$(parse_uint "$(extract_metric ws_notifications_enqueued)")"

grpc_metrics_available=0
if [[ "${grpc_message_total_raw}" =~ ^[0-9]+$ ]]; then
  grpc_metrics_available=1
fi

prev_lag_streak=0
prev_replaced_streak=0
if [[ -f "${STATE_FILE}" ]]; then
  prev_lag_streak="$(jq -r '.lag_streak // 0' "${STATE_FILE}" 2>/dev/null || echo 0)"
  prev_replaced_streak="$(jq -r '.replaced_streak // 0' "${STATE_FILE}" 2>/dev/null || echo 0)"
fi

lag_streak=0
if (( lag_p95 > LAG_THRESHOLD_MS )); then
  lag_streak=$((prev_lag_streak + 1))
fi

replaced_streak=0
replaced_cmp="$(awk -v lhs="${replaced_ratio}" -v rhs="${REPLACED_THRESHOLD}" 'BEGIN { if (lhs > rhs) print 1; else print 0 }')"
if [[ "${replaced_cmp}" == "1" ]]; then
  replaced_streak=$((prev_replaced_streak + 1))
fi

tmp_state="$(mktemp "${STATE_FILE}.tmp.XXXXXX")"
if [[ -f "${STATE_FILE}" ]]; then
  jq \
    --arg now_iso "${now_iso}" \
    --argjson now_epoch "${now_epoch}" \
    --argjson lag_p95 "${lag_p95}" \
    --argjson replaced_ratio "${replaced_ratio}" \
    --argjson reconnect_count "${reconnect_count}" \
    --argjson parse_rejected_total "${parse_rejected_total}" \
    --argjson grpc_decode_errors "${grpc_decode_errors}" \
    --argjson grpc_message_total "${grpc_message_total}" \
    --argjson processed_total "${processed_total}" \
    --argjson lag_streak "${lag_streak}" \
    --argjson replaced_streak "${replaced_streak}" \
    --argjson history_retention_seconds "${HISTORY_RETENTION_SECONDS}" \
    '
      .last_run_ts = $now_iso
      | .last_run_epoch = $now_epoch
      | .last_lag_p95_ms = $lag_p95
      | .last_replaced_ratio = $replaced_ratio
      | .lag_streak = $lag_streak
      | .replaced_streak = $replaced_streak
      | .history = (
          ((.history // []) + [{
            epoch: $now_epoch,
            reconnect_count: $reconnect_count,
            parse_rejected_total: $parse_rejected_total,
            grpc_decode_errors: $grpc_decode_errors,
            grpc_message_total: $grpc_message_total,
            processed_total: $processed_total
          }])
          | map(select(.epoch >= ($now_epoch - $history_retention_seconds)))
        )
    ' "${STATE_FILE}" > "${tmp_state}"
else
  jq -n \
    --arg now_iso "${now_iso}" \
    --argjson now_epoch "${now_epoch}" \
    --argjson lag_p95 "${lag_p95}" \
    --argjson replaced_ratio "${replaced_ratio}" \
    --argjson reconnect_count "${reconnect_count}" \
    --argjson parse_rejected_total "${parse_rejected_total}" \
    --argjson grpc_decode_errors "${grpc_decode_errors}" \
    --argjson grpc_message_total "${grpc_message_total}" \
    --argjson processed_total "${processed_total}" \
    --argjson lag_streak "${lag_streak}" \
    --argjson replaced_streak "${replaced_streak}" \
    '{
      last_run_ts: $now_iso,
      last_run_epoch: $now_epoch,
      last_lag_p95_ms: $lag_p95,
      last_replaced_ratio: $replaced_ratio,
      lag_streak: $lag_streak,
      replaced_streak: $replaced_streak,
      history: [{
        epoch: $now_epoch,
        reconnect_count: $reconnect_count,
        parse_rejected_total: $parse_rejected_total,
        grpc_decode_errors: $grpc_decode_errors,
        grpc_message_total: $grpc_message_total,
        processed_total: $processed_total
      }]
    }' > "${tmp_state}"
fi
atomic_rename_with_fsync "${tmp_state}" "${STATE_FILE}"

reconnect_delta_window=0
inbound_delta_5m=0
parse_reject_delta_5m=0
decode_error_delta_5m=0
reject_total_delta_5m=0
processed_delta_120=0
inbound_delta_120=0
reject_rate=0

if (( grpc_metrics_available == 1 )); then
  reconnect_delta_window="$(window_delta reconnect_count "${RECONNECT_STORM_WINDOW_SECONDS}")"
  inbound_delta_5m="$(window_delta grpc_message_total "${REJECT_RATE_WINDOW_SECONDS}")"
  parse_reject_delta_5m="$(window_delta parse_rejected_total "${REJECT_RATE_WINDOW_SECONDS}")"
  decode_error_delta_5m="$(window_delta grpc_decode_errors "${REJECT_RATE_WINDOW_SECONDS}")"
  reject_total_delta_5m=$((parse_reject_delta_5m + decode_error_delta_5m))
  processed_delta_120="$(window_delta processed_total "${NO_PROCESSED_WINDOW_SECONDS}")"
  inbound_delta_120="$(window_delta grpc_message_total "${NO_PROCESSED_WINDOW_SECONDS}")"
  reject_rate="$(awk -v reject="${reject_total_delta_5m}" -v inbound="${inbound_delta_5m}" 'BEGIN { if (inbound > 0) printf "%.6f", reject / inbound; else printf "0" }')"
fi

reason=""
if (( lag_streak >= LAG_BREACH_CONSECUTIVE )); then
  reason="lag_p95_above_threshold"
elif (( replaced_streak >= REPLACED_BREACH_CONSECUTIVE )); then
  reason="replaced_ratio_above_threshold"
elif (( grpc_metrics_available == 1 )) && (( reconnect_delta_window >= RECONNECT_STORM_THRESHOLD )); then
  reason="reconnect_storm"
else
  reject_rate_cmp=0
  if (( grpc_metrics_available == 1 )) && (( inbound_delta_5m >= REJECT_RATE_MIN_DENOMINATOR )); then
    reject_rate_cmp="$(awk -v lhs="${reject_rate}" -v rhs="${REJECT_RATE_THRESHOLD}" 'BEGIN { if (lhs > rhs) print 1; else print 0 }')"
  fi
  if (( reject_rate_cmp == 1 )); then
    reason="decode_or_parse_reject_rate"
  elif (( grpc_metrics_available == 1 )) && (( inbound_delta_120 >= NO_PROCESSED_MIN_INBOUND )) && (( processed_delta_120 == 0 )); then
    reason="no_processed_swaps_with_inbound"
  fi
fi

if [[ -z "${reason}" ]]; then
  echo "watchdog: healthy (lag_p95=${lag_p95}, replaced_ratio=${replaced_ratio}, reconnect_delta=${reconnect_delta_window}, reject_rate=${reject_rate}, inbound_120=${inbound_delta_120}, processed_120=${processed_delta_120}, lag_streak=${lag_streak}, replaced_streak=${replaced_streak})"
  exit 0
fi

cooldown_until_epoch=$((now_epoch + COOLDOWN_MINUTES * 60))
cooldown_until_ts="$(epoch_to_iso_utc "${cooldown_until_epoch}")"

tmp_override="$(mktemp "${OVERRIDE_FILE}.tmp.XXXXXX")"
cat > "${tmp_override}" <<EOF
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws
SOLANA_COPY_BOT_INGESTION_SOURCE_REASON=${reason}
SOLANA_COPY_BOT_INGESTION_SOURCE_TS=${now_iso}
EOF
atomic_rename_with_fsync "${tmp_override}" "${OVERRIDE_FILE}"

tmp_cooldown="$(mktemp "${COOLDOWN_FILE}.tmp.XXXXXX")"
jq -n \
  --arg reason "${reason}" \
  --arg now_iso "${now_iso}" \
  --arg cooldown_until_ts "${cooldown_until_ts}" \
  --argjson now_epoch "${now_epoch}" \
  --argjson cooldown_until_epoch "${cooldown_until_epoch}" \
  --argjson reconnect_delta_window "${reconnect_delta_window}" \
  --argjson reject_total_delta_5m "${reject_total_delta_5m}" \
  --argjson inbound_delta_5m "${inbound_delta_5m}" \
  --argjson inbound_delta_120 "${inbound_delta_120}" \
  --argjson processed_delta_120 "${processed_delta_120}" \
  '{
    reason: $reason,
    last_failover_ts: $now_iso,
    last_failover_epoch: $now_epoch,
    cooldown_until_ts: $cooldown_until_ts,
    cooldown_until_epoch: $cooldown_until_epoch,
    trigger_context: {
      reconnect_delta_window: $reconnect_delta_window,
      reject_total_delta_5m: $reject_total_delta_5m,
      inbound_delta_5m: $inbound_delta_5m,
      inbound_delta_120: $inbound_delta_120,
      processed_delta_120: $processed_delta_120
    }
  }' > "${tmp_cooldown}"
atomic_rename_with_fsync "${tmp_cooldown}" "${COOLDOWN_FILE}"

echo "watchdog: failover armed reason=${reason}, override=${OVERRIDE_FILE}, cooldown_until=${cooldown_until_ts}"

if [[ "${ALLOW_RESTART}" == "true" ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    systemctl restart "${SERVICE_NAME}"
    echo "watchdog: restarted service ${SERVICE_NAME}"
  else
    echo "watchdog: systemctl not found; restart skipped" >&2
  fi
else
  echo "watchdog: allow_restart=false; restart skipped"
fi
