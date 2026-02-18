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

WINDOW_HOURS="$(policy_value runtime_snapshot_window_hours 1)"
RISK_MINUTES="$(policy_value runtime_snapshot_risk_minutes 15)"
COOLDOWN_MINUTES="$(policy_value cooldown_minutes 15)"
LAG_THRESHOLD_MS="$(policy_value lag_p95_threshold_ms 10000)"
LAG_BREACH_CONSECUTIVE="$(policy_value lag_breach_consecutive 5)"
REPLACED_THRESHOLD="$(policy_value replaced_ratio_threshold 0.93)"
REPLACED_BREACH_CONSECUTIVE="$(policy_value replaced_ratio_breach_consecutive 5)"
SERVICE_NAME="$(policy_value service_name solana-copy-bot)"
ALLOW_RESTART="$(policy_value allow_restart false)"

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

lag_p95_raw="$(extract_metric ingestion_lag_ms_p95)"
replaced_ratio_raw="$(extract_metric replaced_ratio_last_interval)"

lag_p95=0
if [[ "${lag_p95_raw}" =~ ^[0-9]+$ ]]; then
  lag_p95="${lag_p95_raw}"
fi

replaced_ratio=0
if [[ "${replaced_ratio_raw}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  replaced_ratio="${replaced_ratio_raw}"
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
jq -n \
  --arg now_iso "${now_iso}" \
  --argjson now_epoch "${now_epoch}" \
  --argjson lag_p95 "${lag_p95}" \
  --argjson replaced_ratio "${replaced_ratio}" \
  --argjson lag_streak "${lag_streak}" \
  --argjson replaced_streak "${replaced_streak}" \
  '{
    last_run_ts: $now_iso,
    last_run_epoch: $now_epoch,
    last_lag_p95_ms: $lag_p95,
    last_replaced_ratio: $replaced_ratio,
    lag_streak: $lag_streak,
    replaced_streak: $replaced_streak
  }' > "${tmp_state}"
mv "${tmp_state}" "${STATE_FILE}"

reason=""
if (( lag_streak >= LAG_BREACH_CONSECUTIVE )); then
  reason="lag_p95_above_threshold"
elif (( replaced_streak >= REPLACED_BREACH_CONSECUTIVE )); then
  reason="replaced_ratio_above_threshold"
fi

if [[ -z "${reason}" ]]; then
  echo "watchdog: healthy (lag_p95=${lag_p95}, replaced_ratio=${replaced_ratio}, lag_streak=${lag_streak}, replaced_streak=${replaced_streak})"
  exit 0
fi

cooldown_until_epoch=$((now_epoch + COOLDOWN_MINUTES * 60))
cooldown_until_ts="$(date -u -r "${cooldown_until_epoch}" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v+${COOLDOWN_MINUTES}M +"%Y-%m-%dT%H:%M:%SZ")"

tmp_override="$(mktemp "${OVERRIDE_FILE}.tmp.XXXXXX")"
cat > "${tmp_override}" <<EOF
SOLANA_COPY_BOT_INGESTION_SOURCE=helius_ws
SOLANA_COPY_BOT_INGESTION_SOURCE_REASON=${reason}
SOLANA_COPY_BOT_INGESTION_SOURCE_TS=${now_iso}
EOF
mv "${tmp_override}" "${OVERRIDE_FILE}"

tmp_cooldown="$(mktemp "${COOLDOWN_FILE}.tmp.XXXXXX")"
jq -n \
  --arg reason "${reason}" \
  --arg now_iso "${now_iso}" \
  --arg cooldown_until_ts "${cooldown_until_ts}" \
  --argjson now_epoch "${now_epoch}" \
  --argjson cooldown_until_epoch "${cooldown_until_epoch}" \
  '{
    reason: $reason,
    last_failover_ts: $now_iso,
    last_failover_epoch: $now_epoch,
    cooldown_until_ts: $cooldown_until_ts,
    cooldown_until_epoch: $cooldown_until_epoch
  }' > "${tmp_cooldown}"
mv "${tmp_cooldown}" "${COOLDOWN_FILE}"

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
