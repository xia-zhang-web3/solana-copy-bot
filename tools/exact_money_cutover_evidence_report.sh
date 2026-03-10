#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

usage() {
  echo "usage: exact_money_cutover_evidence_report.sh <sqlite.db>" >&2
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

OUTPUT_DIR="${OUTPUT_DIR:-state/exact-money-cutover-evidence-$(date -u +"%Y%m%dT%H%M%SZ")}"
mkdir -p "$OUTPUT_DIR"
timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"

declare -a input_errors=()

readiness_output=""
readiness_exit_code=0
readiness_verdict="UNKNOWN"
readiness_reason_code="not_executed"
readiness_cutover_present="n/a"
readiness_cutover_ts="n/a"
readiness_surface_failures="n/a"
readiness_invalid_exact_rows_total="n/a"
readiness_forbidden_merge_rows_total="n/a"

if readiness_output="$(
  bash "$ROOT_DIR/tools/exact_money_cutover_readiness_report.sh" "$DB_PATH" 2>&1
)"; then
  readiness_exit_code=0
else
  readiness_exit_code=$?
fi

readiness_verdict_raw="$(trim_string "$(extract_field "exact_money_cutover_guard_verdict" "$readiness_output")")"
readiness_verdict="$(normalize_strict_guard_verdict "$readiness_verdict_raw")"
if [[ -z "$readiness_verdict_raw" ]]; then
  input_errors+=("nested readiness exact_money_cutover_guard_verdict must be non-empty")
  readiness_verdict="UNKNOWN"
elif [[ "$readiness_verdict_raw" != "PASS" && "$readiness_verdict_raw" != "WARN" && "$readiness_verdict_raw" != "UNKNOWN" && "$readiness_verdict_raw" != "SKIP" ]]; then
  input_errors+=("nested readiness exact_money_cutover_guard_verdict must be one of PASS,WARN,UNKNOWN,SKIP (got: ${readiness_verdict_raw})")
  readiness_verdict="UNKNOWN"
fi

readiness_reason_code="$(trim_string "$(extract_field "exact_money_cutover_guard_reason_code" "$readiness_output")")"
if [[ -z "$readiness_reason_code" ]]; then
  input_errors+=("nested readiness exact_money_cutover_guard_reason_code must be non-empty")
  readiness_reason_code="n/a"
fi

readiness_cutover_present="$(trim_string "$(extract_field "exact_money_cutover_present" "$readiness_output")")"
if [[ "$readiness_cutover_present" != "yes" && "$readiness_cutover_present" != "no" && "$readiness_cutover_present" != "n/a" ]]; then
  input_errors+=("nested readiness exact_money_cutover_present must be yes/no/n/a (got: ${readiness_cutover_present:-<empty>})")
  readiness_cutover_present="n/a"
fi
readiness_cutover_ts="$(trim_string "$(extract_field "exact_money_cutover_ts" "$readiness_output")")"

if [[ "$readiness_verdict" == "PASS" || "$readiness_verdict" == "WARN" ]]; then
  readiness_surface_failures_raw="$(trim_string "$(extract_field "exact_money_post_cutover_surface_failures" "$readiness_output")")"
  if ! readiness_surface_failures="$(parse_u64_token_strict "$readiness_surface_failures_raw")"; then
    input_errors+=("nested readiness exact_money_post_cutover_surface_failures must be a non-negative integer (got: ${readiness_surface_failures_raw:-<empty>})")
    readiness_surface_failures="n/a"
  fi
  readiness_invalid_exact_rows_total_raw="$(trim_string "$(extract_field "exact_money_invalid_exact_rows_total" "$readiness_output")")"
  if ! readiness_invalid_exact_rows_total="$(parse_u64_token_strict "$readiness_invalid_exact_rows_total_raw")"; then
    input_errors+=("nested readiness exact_money_invalid_exact_rows_total must be a non-negative integer (got: ${readiness_invalid_exact_rows_total_raw:-<empty>})")
    readiness_invalid_exact_rows_total="n/a"
  fi
  readiness_forbidden_merge_rows_total_raw="$(trim_string "$(extract_field "exact_money_forbidden_merge_rows_total" "$readiness_output")")"
  if ! readiness_forbidden_merge_rows_total="$(parse_u64_token_strict "$readiness_forbidden_merge_rows_total_raw")"; then
    input_errors+=("nested readiness exact_money_forbidden_merge_rows_total must be a non-negative integer (got: ${readiness_forbidden_merge_rows_total_raw:-<empty>})")
    readiness_forbidden_merge_rows_total="n/a"
  fi
fi

readiness_capture="$OUTPUT_DIR/exact_money_cutover_readiness_captured_${timestamp_compact}.txt"
printf '%s\n' "$readiness_output" >"$readiness_capture"
readiness_capture_sha256="$(shasum -a 256 "$readiness_capture" | awk '{print $1}')"

legacy_export_output=""
legacy_export_exit_code=0
legacy_export_verdict="SKIP"
legacy_export_reason_code="not_run"
legacy_approximate_rows_total="n/a"
post_cutover_approximate_rows_total="n/a"
legacy_export_summary="n/a"
legacy_export_manifest="n/a"
legacy_export_summary_sha256="n/a"
legacy_export_manifest_sha256="n/a"

if [[ "${#input_errors[@]}" -eq 0 && "$readiness_exit_code" -eq 0 && "$readiness_verdict" != "UNKNOWN" && "$readiness_verdict" != "SKIP" ]]; then
  export_output_dir="$OUTPUT_DIR/legacy_export"
  mkdir -p "$export_output_dir"
  if legacy_export_output="$(
    python3 "$ROOT_DIR/tools/export_exact_money_legacy_evidence.py" "$DB_PATH" "$export_output_dir" 2>&1
  )"; then
    legacy_export_exit_code=0
  else
    legacy_export_exit_code=$?
  fi

  legacy_export_verdict_raw="$(trim_string "$(extract_field "exact_money_legacy_export_verdict" "$legacy_export_output")")"
  legacy_export_verdict="$(printf '%s' "$legacy_export_verdict_raw" | tr '[:lower:]' '[:upper:]')"
  if [[ -z "$legacy_export_verdict_raw" ]]; then
    input_errors+=("nested legacy export exact_money_legacy_export_verdict must be non-empty")
    legacy_export_verdict="UNKNOWN"
  elif [[ "$legacy_export_verdict" != "PASS" && "$legacy_export_verdict" != "WARN" ]]; then
    input_errors+=("nested legacy export exact_money_legacy_export_verdict must be PASS or WARN (got: ${legacy_export_verdict_raw})")
    legacy_export_verdict="UNKNOWN"
  fi

  legacy_export_reason_code="$(trim_string "$(extract_field "exact_money_legacy_export_reason_code" "$legacy_export_output")")"
  if [[ -z "$legacy_export_reason_code" ]]; then
    input_errors+=("nested legacy export exact_money_legacy_export_reason_code must be non-empty")
    legacy_export_reason_code="n/a"
  fi

  legacy_approximate_rows_total_raw="$(trim_string "$(extract_field "legacy_approximate_rows_total" "$legacy_export_output")")"
  if ! legacy_approximate_rows_total="$(parse_u64_token_strict "$legacy_approximate_rows_total_raw")"; then
    input_errors+=("nested legacy export legacy_approximate_rows_total must be a non-negative integer (got: ${legacy_approximate_rows_total_raw:-<empty>})")
    legacy_approximate_rows_total="n/a"
  fi

  post_cutover_approximate_rows_total_raw="$(trim_string "$(extract_field "post_cutover_approximate_rows_total" "$legacy_export_output")")"
  if ! post_cutover_approximate_rows_total="$(parse_u64_token_strict "$post_cutover_approximate_rows_total_raw")"; then
    input_errors+=("nested legacy export post_cutover_approximate_rows_total must be a non-negative integer (got: ${post_cutover_approximate_rows_total_raw:-<empty>})")
    post_cutover_approximate_rows_total="n/a"
  fi

  legacy_export_summary="$(trim_string "$(extract_field "artifact_summary" "$legacy_export_output")")"
  if [[ -z "$legacy_export_summary" || "$legacy_export_summary" == "n/a" ]]; then
    input_errors+=("nested legacy export artifact_summary must be non-empty")
    legacy_export_summary="n/a"
  fi
  legacy_export_manifest="$(trim_string "$(extract_field "artifact_manifest" "$legacy_export_output")")"
  if [[ -z "$legacy_export_manifest" || "$legacy_export_manifest" == "n/a" ]]; then
    input_errors+=("nested legacy export artifact_manifest must be non-empty")
    legacy_export_manifest="n/a"
  fi
  legacy_export_summary_sha256="$(trim_string "$(extract_field "summary_sha256" "$legacy_export_output")")"
  if [[ -z "$legacy_export_summary_sha256" || "$legacy_export_summary_sha256" == "n/a" ]]; then
    input_errors+=("nested legacy export summary_sha256 must be non-empty")
    legacy_export_summary_sha256="n/a"
  fi
  legacy_export_manifest_sha256="$(trim_string "$(extract_field "manifest_sha256" "$legacy_export_output")")"
  if [[ -z "$legacy_export_manifest_sha256" || "$legacy_export_manifest_sha256" == "n/a" ]]; then
    input_errors+=("nested legacy export manifest_sha256 must be non-empty")
    legacy_export_manifest_sha256="n/a"
  fi
elif [[ "$readiness_verdict" == "SKIP" ]]; then
  legacy_export_verdict="SKIP"
  legacy_export_reason_code="cutover_not_marked"
elif [[ "$readiness_exit_code" -ne 0 ]]; then
  legacy_export_verdict="SKIP"
  legacy_export_reason_code="readiness_failed"
fi

legacy_export_capture="$OUTPUT_DIR/exact_money_legacy_export_captured_${timestamp_compact}.txt"
printf '%s\n' "$legacy_export_output" >"$legacy_export_capture"
legacy_export_capture_sha256="$(shasum -a 256 "$legacy_export_capture" | awk '{print $1}')"

report_verdict="UNKNOWN"
report_reason_code="input_error"
if [[ "${#input_errors[@]}" -gt 0 ]]; then
  report_verdict="UNKNOWN"
  report_reason_code="input_error"
elif [[ "$readiness_exit_code" -ne 0 ]]; then
  report_verdict="UNKNOWN"
  report_reason_code="readiness_failed"
elif [[ "$readiness_verdict" == "SKIP" ]]; then
  report_verdict="SKIP"
  report_reason_code="cutover_not_marked"
elif [[ "$readiness_verdict" == "UNKNOWN" ]]; then
  report_verdict="UNKNOWN"
  report_reason_code="readiness_unknown"
elif [[ "$legacy_export_verdict" == "UNKNOWN" || "$legacy_export_exit_code" -ne 0 ]]; then
  report_verdict="UNKNOWN"
  report_reason_code="legacy_export_failed"
elif [[ "$readiness_verdict" == "WARN" ]]; then
  report_verdict="WARN"
  report_reason_code="$readiness_reason_code"
elif [[ "$legacy_export_verdict" == "WARN" ]]; then
  report_verdict="WARN"
  report_reason_code="$legacy_export_reason_code"
else
  report_verdict="PASS"
  report_reason_code="cutover_evidence_ready"
fi

summary_path="$OUTPUT_DIR/exact_money_cutover_evidence_summary_${timestamp_compact}.txt"
summary_output="=== Exact Money Cutover Evidence Report ===
utc_now: $timestamp_utc
db: $DB_PATH
output_dir: $OUTPUT_DIR
exact_money_cutover_evidence_verdict: $report_verdict
exact_money_cutover_evidence_reason_code: $report_reason_code
exact_money_cutover_present: ${readiness_cutover_present:-n/a}
exact_money_cutover_ts: ${readiness_cutover_ts:-n/a}
readiness_exit_code: ${readiness_exit_code:-n/a}
readiness_guard_verdict: ${readiness_verdict:-n/a}
readiness_guard_reason_code: ${readiness_reason_code:-n/a}
readiness_post_cutover_surface_failures: ${readiness_surface_failures:-n/a}
readiness_invalid_exact_rows_total: ${readiness_invalid_exact_rows_total:-n/a}
readiness_forbidden_merge_rows_total: ${readiness_forbidden_merge_rows_total:-n/a}
legacy_export_exit_code: ${legacy_export_exit_code:-n/a}
legacy_export_verdict: ${legacy_export_verdict:-n/a}
legacy_export_reason_code: ${legacy_export_reason_code:-n/a}
legacy_approximate_rows_total: ${legacy_approximate_rows_total:-n/a}
post_cutover_approximate_rows_total: ${post_cutover_approximate_rows_total:-n/a}
artifact_readiness_capture: $readiness_capture
readiness_capture_sha256: $readiness_capture_sha256
artifact_legacy_export_capture: $legacy_export_capture
legacy_export_capture_sha256: $legacy_export_capture_sha256
artifact_legacy_export_summary: ${legacy_export_summary:-n/a}
legacy_export_summary_sha256: ${legacy_export_summary_sha256:-n/a}
artifact_legacy_export_manifest: ${legacy_export_manifest:-n/a}
legacy_export_manifest_sha256: ${legacy_export_manifest_sha256:-n/a}"

if [[ "${#input_errors[@]}" -gt 0 ]]; then
  error_index=1
  for error in "${input_errors[@]}"; do
    summary_output+=$'\n'"input_error_${error_index}: $error"
    error_index=$((error_index + 1))
  done
fi

printf '%s\n' "$summary_output" >"$summary_path"
summary_sha256="$(shasum -a 256 "$summary_path" | awk '{print $1}')"

manifest_path="$OUTPUT_DIR/exact_money_cutover_evidence_manifest_${timestamp_compact}.txt"
cat >"$manifest_path" <<EOF
summary_path=$summary_path
summary_sha256=$summary_sha256
readiness_capture_path=$readiness_capture
readiness_capture_sha256=$readiness_capture_sha256
legacy_export_capture_path=$legacy_export_capture
legacy_export_capture_sha256=$legacy_export_capture_sha256
legacy_export_summary_path=${legacy_export_summary:-n/a}
legacy_export_summary_sha256=${legacy_export_summary_sha256:-n/a}
legacy_export_manifest_path=${legacy_export_manifest:-n/a}
legacy_export_manifest_sha256=${legacy_export_manifest_sha256:-n/a}
EOF
manifest_sha256="$(shasum -a 256 "$manifest_path" | awk '{print $1}')"

printf '%s\n' "$summary_output"
echo "artifact_summary: $summary_path"
echo "summary_sha256: $summary_sha256"
echo "artifact_manifest: $manifest_path"
echo "manifest_sha256: $manifest_sha256"

exit 0
