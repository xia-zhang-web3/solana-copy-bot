#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

usage() {
  echo "usage: exact_money_cutover_readiness_report.sh <sqlite.db>" >&2
}

if [[ "$#" -ne 1 ]]; then
  usage
  exit 1
fi

DB_PATH="$(trim_string "$1")"
if [[ -z "$DB_PATH" ]]; then
  usage
  exit 1
fi
if [[ ! -f "$DB_PATH" ]]; then
  echo "sqlite db not found: $DB_PATH" >&2
  exit 1
fi

tmp_output="$(mktemp -t exact-money-readiness-output.XXXXXX)"
tmp_error="$(mktemp -t exact-money-readiness-error.XXXXXX)"
cleanup() {
  rm -f "$tmp_output" "$tmp_error"
}
trap cleanup EXIT

report_verdict="UNKNOWN"
report_reason_code="coverage_report_failed"
coverage_output=""
coverage_error=""

if python3 "$ROOT_DIR/tools/exact_money_coverage_report.py" "$DB_PATH" >"$tmp_output" 2>"$tmp_error"; then
  coverage_output="$(cat "$tmp_output")"
else
  coverage_error="$(tr '\n' ' ' <"$tmp_error" | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//')"
fi

declare -a input_errors=()

extract_u64_field_strict() {
  local key="$1"
  local text="$2"
  local raw=""
  raw="$(trim_string "$(extract_field "$key" "$text")")"
  if [[ -z "$raw" ]]; then
    return 1
  fi
  parse_u64_token_strict "$raw"
}

print_default_report() {
  echo "exact_money_cutover_present: n/a"
  echo "exact_money_cutover_ts: n/a"
  echo "exact_money_cutover_recorded_ts: n/a"
  echo "exact_money_cutover_note: n/a"
  echo "observed_swaps_post_cutover_rows: n/a"
  echo "observed_swaps_post_cutover_exact_rows: n/a"
  echo "observed_swaps_legacy_approximate_rows: n/a"
  echo "copy_signals_post_cutover_rows: n/a"
  echo "copy_signals_post_cutover_exact_rows: n/a"
  echo "copy_signals_legacy_approximate_rows: n/a"
  echo "fills_post_cutover_rows: n/a"
  echo "fills_post_cutover_exact_rows: n/a"
  echo "fills_legacy_approximate_rows: n/a"
  echo "fills_qty_post_cutover_rows: n/a"
  echo "fills_qty_post_cutover_exact_rows: n/a"
  echo "positions_post_cutover_rows: n/a"
  echo "positions_post_cutover_exact_rows: n/a"
  echo "positions_legacy_approximate_rows: n/a"
  echo "positions_qty_post_cutover_rows: n/a"
  echo "positions_qty_post_cutover_exact_rows: n/a"
  echo "positions_pnl_post_cutover_rows: n/a"
  echo "positions_pnl_post_cutover_exact_rows: n/a"
  echo "positions_pnl_legacy_approximate_rows: n/a"
  echo "shadow_lots_post_cutover_rows: n/a"
  echo "shadow_lots_post_cutover_exact_rows: n/a"
  echo "shadow_lots_legacy_approximate_rows: n/a"
  echo "shadow_lots_qty_post_cutover_rows: n/a"
  echo "shadow_lots_qty_post_cutover_exact_rows: n/a"
  echo "shadow_closed_trades_post_cutover_rows: n/a"
  echo "shadow_closed_trades_post_cutover_exact_rows: n/a"
  echo "shadow_closed_trades_legacy_approximate_rows: n/a"
  echo "shadow_closed_trades_qty_post_cutover_rows: n/a"
  echo "shadow_closed_trades_qty_post_cutover_exact_rows: n/a"
  echo "positions_bucket_forbidden_merge_rows: n/a"
  echo "shadow_lots_bucket_forbidden_merge_rows: n/a"
  echo "shadow_closed_trades_bucket_forbidden_merge_rows: n/a"
  echo "observed_swaps_invalid_exact_rows: n/a"
  echo "fills_invalid_exact_rows: n/a"
  echo "positions_invalid_exact_rows: n/a"
  echo "shadow_lots_invalid_exact_rows: n/a"
  echo "shadow_closed_trades_invalid_exact_rows: n/a"
  echo "exact_money_post_cutover_surface_failures: n/a"
  echo "exact_money_invalid_exact_rows_total: n/a"
  echo "exact_money_forbidden_merge_rows_total: n/a"
  echo "exact_money_cutover_guard_verdict: $report_verdict"
  echo "exact_money_cutover_guard_reason_code: $report_reason_code"
  if [[ -n "$coverage_error" ]]; then
    echo "exact_money_cutover_guard_detail: $coverage_error"
  fi
}

if [[ -z "$coverage_output" ]]; then
  print_default_report
  exit 0
fi

exact_money_cutover_present="$(trim_string "$(extract_field "exact_money_cutover_present" "$coverage_output")")"
exact_money_cutover_ts="$(trim_string "$(extract_field "exact_money_cutover_ts" "$coverage_output")")"
exact_money_cutover_recorded_ts="$(trim_string "$(extract_field "exact_money_cutover_recorded_ts" "$coverage_output")")"
exact_money_cutover_note="$(trim_string "$(extract_field "exact_money_cutover_note" "$coverage_output")")"

if [[ "$exact_money_cutover_present" != "yes" && "$exact_money_cutover_present" != "no" ]]; then
  input_errors+=("exact_money_cutover_present must be yes/no (got: ${exact_money_cutover_present:-<empty>})")
  exact_money_cutover_present="n/a"
fi

if [[ "$exact_money_cutover_present" == "no" ]]; then
  report_verdict="SKIP"
  report_reason_code="cutover_not_marked"
  echo "exact_money_cutover_present: no"
  echo "exact_money_cutover_ts: ${exact_money_cutover_ts:-n/a}"
  echo "exact_money_cutover_recorded_ts: ${exact_money_cutover_recorded_ts:-n/a}"
  echo "exact_money_cutover_note: ${exact_money_cutover_note:-n/a}"
  for key in \
    observed_swaps_post_cutover_rows \
    observed_swaps_post_cutover_exact_rows \
    observed_swaps_legacy_approximate_rows \
    copy_signals_post_cutover_rows \
    copy_signals_post_cutover_exact_rows \
    copy_signals_legacy_approximate_rows \
    fills_post_cutover_rows \
    fills_post_cutover_exact_rows \
    fills_legacy_approximate_rows \
    fills_qty_post_cutover_rows \
    fills_qty_post_cutover_exact_rows \
    positions_post_cutover_rows \
    positions_post_cutover_exact_rows \
    positions_legacy_approximate_rows \
    positions_qty_post_cutover_rows \
    positions_qty_post_cutover_exact_rows \
    positions_pnl_post_cutover_rows \
    positions_pnl_post_cutover_exact_rows \
    positions_pnl_legacy_approximate_rows \
    shadow_lots_post_cutover_rows \
    shadow_lots_post_cutover_exact_rows \
    shadow_lots_legacy_approximate_rows \
    shadow_lots_qty_post_cutover_rows \
    shadow_lots_qty_post_cutover_exact_rows \
    shadow_closed_trades_post_cutover_rows \
    shadow_closed_trades_post_cutover_exact_rows \
    shadow_closed_trades_legacy_approximate_rows \
    shadow_closed_trades_qty_post_cutover_rows \
    shadow_closed_trades_qty_post_cutover_exact_rows \
    positions_bucket_forbidden_merge_rows \
    shadow_lots_bucket_forbidden_merge_rows \
    shadow_closed_trades_bucket_forbidden_merge_rows \
    observed_swaps_invalid_exact_rows \
    fills_invalid_exact_rows \
    positions_invalid_exact_rows \
    shadow_lots_invalid_exact_rows \
    shadow_closed_trades_invalid_exact_rows; do
    echo "$key: n/a"
  done
  echo "exact_money_post_cutover_surface_failures: n/a"
  echo "exact_money_invalid_exact_rows_total: n/a"
  echo "exact_money_forbidden_merge_rows_total: n/a"
  echo "exact_money_cutover_guard_verdict: $report_verdict"
  echo "exact_money_cutover_guard_reason_code: $report_reason_code"
  exit 0
fi

for key in \
  observed_swaps_post_cutover_rows \
  observed_swaps_post_cutover_exact_rows \
  observed_swaps_legacy_approximate_rows \
  copy_signals_post_cutover_rows \
  copy_signals_post_cutover_exact_rows \
  copy_signals_legacy_approximate_rows \
  fills_post_cutover_rows \
  fills_post_cutover_exact_rows \
  fills_legacy_approximate_rows \
  fills_qty_post_cutover_rows \
  fills_qty_post_cutover_exact_rows \
  positions_post_cutover_rows \
  positions_post_cutover_exact_rows \
  positions_legacy_approximate_rows \
  positions_qty_post_cutover_rows \
  positions_qty_post_cutover_exact_rows \
  positions_pnl_post_cutover_rows \
  positions_pnl_post_cutover_exact_rows \
  positions_pnl_legacy_approximate_rows \
  shadow_lots_post_cutover_rows \
  shadow_lots_post_cutover_exact_rows \
  shadow_lots_legacy_approximate_rows \
  shadow_lots_qty_post_cutover_rows \
  shadow_lots_qty_post_cutover_exact_rows \
  shadow_closed_trades_post_cutover_rows \
  shadow_closed_trades_post_cutover_exact_rows \
  shadow_closed_trades_legacy_approximate_rows \
  shadow_closed_trades_qty_post_cutover_rows \
  shadow_closed_trades_qty_post_cutover_exact_rows \
  positions_bucket_forbidden_merge_rows \
  shadow_lots_bucket_forbidden_merge_rows \
  shadow_closed_trades_bucket_forbidden_merge_rows \
  observed_swaps_invalid_exact_rows \
  fills_invalid_exact_rows \
  positions_invalid_exact_rows \
  shadow_lots_invalid_exact_rows \
  shadow_closed_trades_invalid_exact_rows; do
  raw_value="$(trim_string "$(extract_field "$key" "$coverage_output")")"
  field_var="field_${key}"
  if [[ -z "$raw_value" || "$raw_value" == "n/a" ]]; then
    input_errors+=("$key missing from exact money coverage report")
    printf -v "$field_var" '%s' "n/a"
    continue
  fi
  if ! parsed_value="$(parse_u64_token_strict "$raw_value")"; then
    input_errors+=("$key must be a non-negative integer (got: $raw_value)")
    printf -v "$field_var" '%s' "n/a"
    continue
  fi
  printf -v "$field_var" '%s' "$parsed_value"
done

surface_failures="0"
invalid_exact_total="0"
forbidden_merge_total="0"
if [[ "${#input_errors[@]}" -eq 0 ]]; then
  for pair in \
    "observed_swaps_post_cutover_rows observed_swaps_post_cutover_exact_rows" \
    "copy_signals_post_cutover_rows copy_signals_post_cutover_exact_rows" \
    "fills_post_cutover_rows fills_post_cutover_exact_rows" \
    "fills_qty_post_cutover_rows fills_qty_post_cutover_exact_rows" \
    "positions_post_cutover_rows positions_post_cutover_exact_rows" \
    "positions_qty_post_cutover_rows positions_qty_post_cutover_exact_rows" \
    "positions_pnl_post_cutover_rows positions_pnl_post_cutover_exact_rows" \
    "shadow_lots_post_cutover_rows shadow_lots_post_cutover_exact_rows" \
    "shadow_lots_qty_post_cutover_rows shadow_lots_qty_post_cutover_exact_rows" \
    "shadow_closed_trades_post_cutover_rows shadow_closed_trades_post_cutover_exact_rows" \
    "shadow_closed_trades_qty_post_cutover_rows shadow_closed_trades_qty_post_cutover_exact_rows"; do
    set -- $pair
    total_var="field_$1"
    exact_var="field_$2"
    total_rows="${!total_var}"
    exact_rows="${!exact_var}"
    if [[ "$total_rows" == "n/a" || "$exact_rows" == "n/a" ]]; then
      input_errors+=("post-cutover surface pair unavailable: $1/$2")
      continue
    fi
    if (( exact_rows < total_rows )); then
      surface_failures="$((surface_failures + 1))"
    fi
  done

  for key in \
    observed_swaps_invalid_exact_rows \
    fills_invalid_exact_rows \
    positions_invalid_exact_rows \
    shadow_lots_invalid_exact_rows \
    shadow_closed_trades_invalid_exact_rows; do
    value_var="field_${key}"
    value="${!value_var}"
    if [[ "$value" == "n/a" ]]; then
      input_errors+=("$key unavailable")
      continue
    fi
    invalid_exact_total="$((invalid_exact_total + value))"
  done

  for key in \
    positions_bucket_forbidden_merge_rows \
    shadow_lots_bucket_forbidden_merge_rows \
    shadow_closed_trades_bucket_forbidden_merge_rows; do
    value_var="field_${key}"
    value="${!value_var}"
    if [[ "$value" == "n/a" ]]; then
      input_errors+=("$key unavailable")
      continue
    fi
    forbidden_merge_total="$((forbidden_merge_total + value))"
  done
fi

if [[ "${#input_errors[@]}" -gt 0 ]]; then
  report_verdict="UNKNOWN"
  report_reason_code="input_error"
elif [[ "$exact_money_cutover_present" == "no" ]]; then
  report_verdict="SKIP"
  report_reason_code="cutover_not_marked"
elif (( surface_failures > 0 || invalid_exact_total > 0 || forbidden_merge_total > 0 )); then
  report_verdict="WARN"
  report_reason_code="post_cutover_exact_violations_detected"
else
  report_verdict="PASS"
  report_reason_code="post_cutover_exact_ready"
fi

echo "exact_money_cutover_present: $exact_money_cutover_present"
echo "exact_money_cutover_ts: ${exact_money_cutover_ts:-n/a}"
echo "exact_money_cutover_recorded_ts: ${exact_money_cutover_recorded_ts:-n/a}"
echo "exact_money_cutover_note: ${exact_money_cutover_note:-n/a}"
for key in \
  observed_swaps_post_cutover_rows \
  observed_swaps_post_cutover_exact_rows \
  observed_swaps_legacy_approximate_rows \
  copy_signals_post_cutover_rows \
  copy_signals_post_cutover_exact_rows \
  copy_signals_legacy_approximate_rows \
  fills_post_cutover_rows \
  fills_post_cutover_exact_rows \
  fills_legacy_approximate_rows \
  fills_qty_post_cutover_rows \
  fills_qty_post_cutover_exact_rows \
  positions_post_cutover_rows \
  positions_post_cutover_exact_rows \
  positions_legacy_approximate_rows \
  positions_qty_post_cutover_rows \
  positions_qty_post_cutover_exact_rows \
  positions_pnl_post_cutover_rows \
  positions_pnl_post_cutover_exact_rows \
  positions_pnl_legacy_approximate_rows \
  shadow_lots_post_cutover_rows \
  shadow_lots_post_cutover_exact_rows \
  shadow_lots_legacy_approximate_rows \
  shadow_lots_qty_post_cutover_rows \
  shadow_lots_qty_post_cutover_exact_rows \
  shadow_closed_trades_post_cutover_rows \
  shadow_closed_trades_post_cutover_exact_rows \
  shadow_closed_trades_legacy_approximate_rows \
  shadow_closed_trades_qty_post_cutover_rows \
  shadow_closed_trades_qty_post_cutover_exact_rows \
  positions_bucket_forbidden_merge_rows \
  shadow_lots_bucket_forbidden_merge_rows \
  shadow_closed_trades_bucket_forbidden_merge_rows \
  observed_swaps_invalid_exact_rows \
  fills_invalid_exact_rows \
  positions_invalid_exact_rows \
  shadow_lots_invalid_exact_rows \
  shadow_closed_trades_invalid_exact_rows; do
  value_var="field_${key}"
  echo "$key: ${!value_var}"
done
if [[ "${#input_errors[@]}" -gt 0 ]]; then
  echo "exact_money_post_cutover_surface_failures: n/a"
  echo "exact_money_invalid_exact_rows_total: n/a"
  echo "exact_money_forbidden_merge_rows_total: n/a"
  echo "exact_money_cutover_guard_detail: $(printf '%s; ' "${input_errors[@]}" | sed 's/; $//')"
else
  echo "exact_money_post_cutover_surface_failures: $surface_failures"
  echo "exact_money_invalid_exact_rows_total: $invalid_exact_total"
  echo "exact_money_forbidden_merge_rows_total: $forbidden_merge_total"
fi
echo "exact_money_cutover_guard_verdict: $report_verdict"
echo "exact_money_cutover_guard_reason_code: $report_reason_code"
