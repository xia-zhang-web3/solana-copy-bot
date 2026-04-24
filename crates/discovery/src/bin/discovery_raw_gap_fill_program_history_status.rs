use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history_status --progress-path <path> --window-start-utc <rfc3339> --window-end-utc <rfc3339> [--json]";
const CONTROL_FIELD_MISSING_REASON: &str =
    "program_history_gap_fill_status_unproven_progress_control_field_missing";

fn main() {
    let Some(config) = (match parse_args() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }) else {
        println!("{USAGE}");
        return;
    };

    let report = match build_status_report(&config) {
        Ok(report) => report,
        Err(error) => StatusReport::operator_error(&config, error.reason, error.detail),
    };
    if config.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("status report should serialize to json")
        );
    } else {
        println!("{}", render_human(&report));
    }

    if report.operator_reason.is_some() {
        std::process::exit(1);
    }
}

#[derive(Debug, Clone)]
struct Config {
    progress_path: PathBuf,
    window_start_utc: DateTime<Utc>,
    window_end_utc: DateTime<Utc>,
    json: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct StatusReport {
    event: &'static str,
    progress_path: String,
    window_start_utc: String,
    window_end_utc: String,
    attempt_number: Option<usize>,
    verdict: Option<String>,
    reason: Option<String>,
    current_phase: Option<String>,
    replayable_output: Option<bool>,
    staged_rows: Option<usize>,
    next_batch_start_slot: Option<u64>,
    attempt_frontier_advanced_slots: Option<usize>,
    zero_progress_retry_count: Option<usize>,
    zero_progress_escape_applied: Option<bool>,
    missing_segments_count: Option<usize>,
    covered_through_ts_utc: Option<String>,
    progress_percent: Option<f64>,
    remaining_gap_hours: Option<f64>,
    status: String,
    operator_reason: Option<String>,
    operator_detail: Option<String>,
}

#[derive(Debug, Clone)]
struct StatusFailure {
    reason: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct ProgressSnapshot {
    attempt_number: Option<usize>,
    verdict: String,
    reason: Option<String>,
    current_phase: String,
    replayable_output: bool,
    staged_rows: Option<usize>,
    next_batch_start_slot: Option<u64>,
    attempt_frontier_advanced_slots: Option<usize>,
    zero_progress_retry_count: Option<usize>,
    zero_progress_escape_applied: Option<bool>,
    missing_segments_count: Option<usize>,
    covered_through: Option<DateTime<Utc>>,
}

impl StatusReport {
    fn operator_error(config: &Config, reason: String, detail: String) -> Self {
        Self {
            event: "discovery_raw_gap_fill_program_history_status",
            progress_path: config.progress_path.display().to_string(),
            window_start_utc: format_ts(config.window_start_utc),
            window_end_utc: format_ts(config.window_end_utc),
            attempt_number: None,
            verdict: None,
            reason: None,
            current_phase: None,
            replayable_output: None,
            staged_rows: None,
            next_batch_start_slot: None,
            attempt_frontier_advanced_slots: None,
            zero_progress_retry_count: None,
            zero_progress_escape_applied: None,
            missing_segments_count: None,
            covered_through_ts_utc: None,
            progress_percent: None,
            remaining_gap_hours: None,
            status: "unproven_operator_error".to_string(),
            operator_reason: Some(reason),
            operator_detail: Some(detail),
        }
    }
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut progress_path: Option<PathBuf> = None;
    let mut window_start_utc: Option<DateTime<Utc>> = None;
    let mut window_end_utc: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--progress-path" => {
                progress_path = Some(PathBuf::from(parse_string_arg(
                    "--progress-path",
                    args.next(),
                )?))
            }
            "--window-start-utc" => {
                window_start_utc = Some(parse_ts_arg("--window-start-utc", args.next())?)
            }
            "--window-end-utc" => {
                window_end_utc = Some(parse_ts_arg("--window-end-utc", args.next())?)
            }
            "--json" => json = true,
            other => bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    let progress_path = progress_path.ok_or_else(|| anyhow!("missing --progress-path\n{USAGE}"))?;
    let window_start_utc =
        window_start_utc.ok_or_else(|| anyhow!("missing --window-start-utc\n{USAGE}"))?;
    let window_end_utc =
        window_end_utc.ok_or_else(|| anyhow!("missing --window-end-utc\n{USAGE}"))?;
    if window_end_utc <= window_start_utc {
        bail!("--window-end-utc must be after --window-start-utc");
    }

    Ok(Some(Config {
        progress_path,
        window_start_utc,
        window_end_utc,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    parse_ts(&raw).with_context(|| format!("invalid timestamp for {flag}"))
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn build_status_report(config: &Config) -> Result<StatusReport, StatusFailure> {
    let progress = load_progress_snapshot(&config.progress_path)?;
    let covered_through_ts_utc = progress.covered_through.map(format_ts);
    let progress_percent = progress.covered_through.map(|covered_through| {
        progress_percent(
            config.window_start_utc,
            config.window_end_utc,
            covered_through,
        )
    });
    let remaining_gap_hours = progress.covered_through.map(|covered_through| {
        remaining_gap_hours(
            config.window_start_utc,
            config.window_end_utc,
            covered_through,
        )
    });
    let status = if progress.covered_through.is_none() {
        "unproven_incomplete_missing_covered_through"
    } else if progress.replayable_output {
        "replayable_output_true"
    } else {
        "incomplete"
    }
    .to_string();

    Ok(StatusReport {
        event: "discovery_raw_gap_fill_program_history_status",
        progress_path: config.progress_path.display().to_string(),
        window_start_utc: format_ts(config.window_start_utc),
        window_end_utc: format_ts(config.window_end_utc),
        attempt_number: progress.attempt_number,
        verdict: Some(progress.verdict),
        reason: progress.reason,
        current_phase: Some(progress.current_phase),
        replayable_output: Some(progress.replayable_output),
        staged_rows: progress.staged_rows,
        next_batch_start_slot: progress.next_batch_start_slot,
        attempt_frontier_advanced_slots: progress.attempt_frontier_advanced_slots,
        zero_progress_retry_count: progress.zero_progress_retry_count,
        zero_progress_escape_applied: progress.zero_progress_escape_applied,
        missing_segments_count: progress.missing_segments_count,
        covered_through_ts_utc,
        progress_percent,
        remaining_gap_hours,
        status,
        operator_reason: None,
        operator_detail: None,
    })
}

fn load_progress_snapshot(path: &Path) -> Result<ProgressSnapshot, StatusFailure> {
    let raw = fs::read_to_string(path).map_err(|error| StatusFailure {
        reason: "program_history_gap_fill_status_unproven_progress_file_missing".to_string(),
        detail: format!("failed reading progress file {}: {error}", path.display()),
    })?;
    let value = serde_json::from_str::<Value>(&raw).map_err(|error| StatusFailure {
        reason: "program_history_gap_fill_status_unproven_progress_json_parse_failed".to_string(),
        detail: format!("failed parsing progress file {}: {error}", path.display()),
    })?;
    progress_snapshot_from_value(&value)
}

fn progress_snapshot_from_value(value: &Value) -> Result<ProgressSnapshot, StatusFailure> {
    let verdict = required_string(value, "verdict")?;
    let current_phase = required_string(value, "current_phase")?;
    let replayable_output = required_bool(value, "replayable_output")?;
    let covered_through = value
        .get("gap_fill_covered_through_cursor")
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| control_field_error("gap_fill_covered_through_cursor.ts_utc", error))?;

    Ok(ProgressSnapshot {
        attempt_number: optional_usize(value, "attempt_number")?,
        verdict,
        reason: optional_string(value, "reason")?,
        current_phase,
        replayable_output,
        staged_rows: optional_usize(value, "staged_rows")?,
        next_batch_start_slot: optional_u64(value, "next_batch_start_slot")?,
        attempt_frontier_advanced_slots: optional_usize(value, "attempt_frontier_advanced_slots")?,
        zero_progress_retry_count: optional_usize(value, "zero_progress_retry_count")?.or(Some(0)),
        zero_progress_escape_applied: optional_bool(value, "zero_progress_escape_applied")?
            .or(Some(false)),
        missing_segments_count: value
            .get("missing_segments")
            .and_then(Value::as_array)
            .map(Vec::len)
            .or(Some(0)),
        covered_through,
    })
}

fn required_string(value: &Value, field: &'static str) -> Result<String, StatusFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| control_field_error(field, "missing or non-string value"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, StatusFailure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| control_field_error(field, "missing or non-bool value"))
}

fn optional_string(value: &Value, field: &'static str) -> Result<Option<String>, StatusFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| control_field_error(field, "non-string value")),
    }
}

fn optional_bool(value: &Value, field: &'static str) -> Result<Option<bool>, StatusFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| control_field_error(field, "non-bool value")),
    }
}

fn optional_u64(value: &Value, field: &'static str) -> Result<Option<u64>, StatusFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| control_field_error(field, "non-u64 value")),
    }
}

fn optional_usize(value: &Value, field: &'static str) -> Result<Option<usize>, StatusFailure> {
    optional_u64(value, field)?
        .map(|value| {
            usize::try_from(value)
                .map_err(|error| control_field_error(field, format!("usize overflow: {error}")))
        })
        .transpose()
}

fn control_field_error(field: &'static str, detail: impl std::fmt::Display) -> StatusFailure {
    StatusFailure {
        reason: CONTROL_FIELD_MISSING_REASON.to_string(),
        detail: format!("{field}: {detail}"),
    }
}

fn progress_percent(
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    covered_through: DateTime<Utc>,
) -> f64 {
    let total_ms = window_end
        .signed_duration_since(window_start)
        .num_milliseconds()
        .max(1) as f64;
    let covered_ms = covered_through
        .signed_duration_since(window_start)
        .num_milliseconds()
        .max(0) as f64;
    (covered_ms / total_ms * 100.0).clamp(0.0, 100.0)
}

fn remaining_gap_hours(
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    covered_through: DateTime<Utc>,
) -> f64 {
    let effective_covered_through = covered_through.clamp(window_start, window_end);
    window_end
        .signed_duration_since(effective_covered_through)
        .num_seconds()
        .max(0) as f64
        / 3600.0
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn render_human(report: &StatusReport) -> String {
    [
        format!("event={}", report.event),
        format!("progress_path={}", report.progress_path),
        format!("window_start_utc={}", report.window_start_utc),
        format!("window_end_utc={}", report.window_end_utc),
        format!("attempt_number={}", format_option(report.attempt_number)),
        format!("verdict={}", report.verdict.as_deref().unwrap_or("null")),
        format!("reason={}", report.reason.as_deref().unwrap_or("null")),
        format!(
            "current_phase={}",
            report.current_phase.as_deref().unwrap_or("null")
        ),
        format!(
            "replayable_output={}",
            report
                .replayable_output
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("staged_rows={}", format_option(report.staged_rows)),
        format!(
            "next_batch_start_slot={}",
            format_option(report.next_batch_start_slot)
        ),
        format!(
            "attempt_frontier_advanced_slots={}",
            format_option(report.attempt_frontier_advanced_slots)
        ),
        format!(
            "zero_progress_retry_count={}",
            format_option(report.zero_progress_retry_count)
        ),
        format!(
            "zero_progress_escape_applied={}",
            report
                .zero_progress_escape_applied
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "missing_segments_count={}",
            format_option(report.missing_segments_count)
        ),
        format!(
            "covered_through_ts_utc={}",
            report.covered_through_ts_utc.as_deref().unwrap_or("null")
        ),
        format!(
            "progress_percent={}",
            format_float_option(report.progress_percent)
        ),
        format!(
            "remaining_gap_hours={}",
            format_float_option(report.remaining_gap_hours)
        ),
        format!("status={}", report.status),
        format!(
            "operator_reason={}",
            report.operator_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "operator_detail={}",
            report.operator_detail.as_deref().unwrap_or("null")
        ),
    ]
    .join("\n")
}

fn format_option<T: ToString>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_float_option(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.6}"))
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config(progress_path: PathBuf) -> Config {
        Config {
            progress_path,
            window_start_utc: parse_ts("2026-04-18T16:56:04Z").unwrap(),
            window_end_utc: parse_ts("2026-04-20T16:56:04Z").unwrap(),
            json: true,
        }
    }

    fn write_progress(path: &Path, body: &str) {
        fs::write(path, body).expect("write progress file");
    }

    fn valid_progress(replayable_output: bool, covered_through: &str) -> String {
        format!(
            r#"{{
  "attempt_number": 7,
  "verdict": "not_proven_due_to_retryable_provider_failure",
  "reason": "program_history_gap_fill_retryable_block_fetch_http_408",
  "current_phase": "awaiting_next_attempt",
  "replayable_output": {replayable_output},
  "staged_rows": 1234,
  "next_batch_start_slot": 4567,
  "attempt_frontier_advanced_slots": 89,
  "zero_progress_retry_count": 2,
  "zero_progress_escape_applied": false,
  "missing_segments": [
    {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T17:56:04Z", "reason": "still_missing" }}
  ],
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }}
}}"#
        )
    }

    #[test]
    fn valid_incomplete_retryable_progress_computes_percent_and_remaining_hours_stage1(
    ) -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(
            &progress_path,
            &valid_progress(false, "2026-04-19T16:56:04Z"),
        );

        let report = build_status_report(&config(progress_path))
            .expect("valid incomplete progress should report status");

        assert_eq!(
            report.verdict.as_deref(),
            Some("not_proven_due_to_retryable_provider_failure")
        );
        assert_eq!(
            report.current_phase.as_deref(),
            Some("awaiting_next_attempt")
        );
        assert_eq!(report.replayable_output, Some(false));
        assert_eq!(report.staged_rows, Some(1234));
        assert_eq!(report.next_batch_start_slot, Some(4567));
        assert_eq!(report.attempt_frontier_advanced_slots, Some(89));
        assert_eq!(report.zero_progress_retry_count, Some(2));
        assert_eq!(report.zero_progress_escape_applied, Some(false));
        assert_eq!(report.missing_segments_count, Some(1));
        assert_eq!(
            report.covered_through_ts_utc.as_deref(),
            Some("2026-04-19T16:56:04Z")
        );
        assert_eq!(report.progress_percent, Some(50.0));
        assert_eq!(report.remaining_gap_hours, Some(24.0));
        assert_eq!(report.status, "incomplete");
        assert_eq!(report.operator_reason, None);
        Ok(())
    }

    #[test]
    fn missing_replayable_output_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(
            &progress_path,
            r#"{
  "attempt_number": 1,
  "verdict": "not_proven_due_to_retryable_provider_failure",
  "current_phase": "awaiting_next_attempt"
}"#,
        );

        let error = build_status_report(&config(progress_path))
            .expect_err("missing replayable_output must fail closed");

        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("replayable_output"));
        Ok(())
    }

    #[test]
    fn missing_verdict_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(
            &progress_path,
            r#"{
  "attempt_number": 1,
  "current_phase": "awaiting_next_attempt",
  "replayable_output": false
}"#,
        );

        let error =
            build_status_report(&config(progress_path)).expect_err("missing verdict must fail");

        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("verdict"));
        Ok(())
    }

    #[test]
    fn missing_current_phase_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(
            &progress_path,
            r#"{
  "attempt_number": 1,
  "verdict": "not_proven_due_to_retryable_provider_failure",
  "replayable_output": false
}"#,
        );

        let error = build_status_report(&config(progress_path))
            .expect_err("missing current_phase must fail");

        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("current_phase"));
        Ok(())
    }

    #[test]
    fn replayable_output_true_is_reported_but_not_synthesized_stage1() -> Result<()> {
        let temp = tempdir()?;
        let true_progress_path = temp.path().join("replayable.progress.json");
        write_progress(
            &true_progress_path,
            &valid_progress(true, "2026-04-20T16:56:04Z"),
        );

        let true_report = build_status_report(&config(true_progress_path))
            .expect("explicit replayable output should report");

        assert_eq!(true_report.replayable_output, Some(true));
        assert_eq!(true_report.progress_percent, Some(100.0));
        assert_eq!(true_report.status, "replayable_output_true");

        let false_progress_path = temp.path().join("nonreplayable.progress.json");
        write_progress(
            &false_progress_path,
            &valid_progress(false, "2026-04-20T16:56:04Z"),
        );

        let false_report = build_status_report(&config(false_progress_path))
            .expect("explicit non-replayable output should report");

        assert_eq!(false_report.replayable_output, Some(false));
        assert_eq!(false_report.progress_percent, Some(100.0));
        assert_eq!(false_report.status, "incomplete");
        Ok(())
    }

    #[test]
    fn missing_progress_file_fails_with_explicit_operator_error_stage1() {
        let temp = tempdir().expect("tempdir");
        let progress_path = temp.path().join("missing.progress.json");

        let error = build_status_report(&config(progress_path))
            .expect_err("missing progress file must fail");

        assert_eq!(
            error.reason,
            "program_history_gap_fill_status_unproven_progress_file_missing"
        );
        assert!(error.detail.contains("missing.progress.json"));
    }
}
