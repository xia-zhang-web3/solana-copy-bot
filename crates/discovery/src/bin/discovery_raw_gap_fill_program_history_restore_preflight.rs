use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history_restore_preflight --progress-path <path> --window-start-utc <rfc3339> --window-end-utc <rfc3339> [--json]";
const CONTROL_FIELD_MISSING_REASON: &str =
    "program_history_gap_fill_restore_preflight_unproven_progress_control_field_missing";

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

    let report = match build_preflight_report(&config) {
        Ok(report) => report,
        Err(error) => RestorePreflightReport::operator_error(&config, error.reason, error.detail),
    };
    if config.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .expect("restore preflight report should serialize to json")
        );
    } else {
        println!("{}", render_human(&report));
    }

    if report.operator_reason.is_some() || !report.restore_ready {
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
struct RestorePreflightReport {
    event: &'static str,
    progress_path: String,
    window_start_utc: String,
    window_end_utc: String,
    attempt_number: Option<usize>,
    verdict: Option<String>,
    reason: Option<String>,
    current_phase: Option<String>,
    replayable_output: Option<bool>,
    covered_through_ts_utc: Option<String>,
    missing_segments_count: Option<usize>,
    restore_ready: bool,
    restore_ready_reason: String,
    operator_reason: Option<String>,
    operator_detail: Option<String>,
}

#[derive(Debug, Clone)]
struct PreflightFailure {
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
    covered_through: Option<DateTime<Utc>>,
    missing_segments_count: usize,
}

impl RestorePreflightReport {
    fn operator_error(config: &Config, reason: String, detail: String) -> Self {
        Self {
            event: "discovery_raw_gap_fill_program_history_restore_preflight",
            progress_path: config.progress_path.display().to_string(),
            window_start_utc: format_ts(config.window_start_utc),
            window_end_utc: format_ts(config.window_end_utc),
            attempt_number: None,
            verdict: None,
            reason: None,
            current_phase: None,
            replayable_output: None,
            covered_through_ts_utc: None,
            missing_segments_count: None,
            restore_ready: false,
            restore_ready_reason: reason.clone(),
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

fn build_preflight_report(config: &Config) -> Result<RestorePreflightReport, PreflightFailure> {
    let progress = load_progress_snapshot(&config.progress_path)?;
    let (restore_ready, restore_ready_reason) = restore_readiness(&progress, config.window_end_utc);

    Ok(RestorePreflightReport {
        event: "discovery_raw_gap_fill_program_history_restore_preflight",
        progress_path: config.progress_path.display().to_string(),
        window_start_utc: format_ts(config.window_start_utc),
        window_end_utc: format_ts(config.window_end_utc),
        attempt_number: progress.attempt_number,
        verdict: Some(progress.verdict),
        reason: progress.reason,
        current_phase: Some(progress.current_phase),
        replayable_output: Some(progress.replayable_output),
        covered_through_ts_utc: progress.covered_through.map(format_ts),
        missing_segments_count: Some(progress.missing_segments_count),
        restore_ready,
        restore_ready_reason,
        operator_reason: None,
        operator_detail: None,
    })
}

fn restore_readiness(progress: &ProgressSnapshot, window_end_utc: DateTime<Utc>) -> (bool, String) {
    if !progress.replayable_output {
        return (
            false,
            "program_history_gap_fill_restore_preflight_not_ready_replayable_output_false"
                .to_string(),
        );
    }
    let Some(covered_through) = progress.covered_through else {
        return (
            false,
            "program_history_gap_fill_restore_preflight_not_ready_missing_covered_through"
                .to_string(),
        );
    };
    if covered_through < window_end_utc {
        return (
            false,
            "program_history_gap_fill_restore_preflight_not_ready_covered_through_before_window_end"
                .to_string(),
        );
    }
    if progress.missing_segments_count > 0 {
        return (
            false,
            "program_history_gap_fill_restore_preflight_not_ready_missing_segments_present"
                .to_string(),
        );
    }
    (
        true,
        "program_history_gap_fill_restore_preflight_ready_explicit_replayable_complete_no_missing_segments"
            .to_string(),
    )
}

fn load_progress_snapshot(path: &Path) -> Result<ProgressSnapshot, PreflightFailure> {
    let raw = fs::read_to_string(path).map_err(|error| PreflightFailure {
        reason: "program_history_gap_fill_restore_preflight_unproven_progress_file_missing"
            .to_string(),
        detail: format!("failed reading progress file {}: {error}", path.display()),
    })?;
    let value = serde_json::from_str::<Value>(&raw).map_err(|error| PreflightFailure {
        reason: "program_history_gap_fill_restore_preflight_unproven_progress_json_parse_failed"
            .to_string(),
        detail: format!("failed parsing progress file {}: {error}", path.display()),
    })?;
    progress_snapshot_from_value(&value)
}

fn progress_snapshot_from_value(value: &Value) -> Result<ProgressSnapshot, PreflightFailure> {
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
    let missing_segments_count = value
        .get("missing_segments")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);

    Ok(ProgressSnapshot {
        attempt_number: optional_usize(value, "attempt_number")?,
        verdict,
        reason: optional_string(value, "reason")?,
        current_phase,
        replayable_output,
        covered_through,
        missing_segments_count,
    })
}

fn required_string(value: &Value, field: &'static str) -> Result<String, PreflightFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| control_field_error(field, "missing or non-string value"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, PreflightFailure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| control_field_error(field, "missing or non-bool value"))
}

fn optional_string(value: &Value, field: &'static str) -> Result<Option<String>, PreflightFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| control_field_error(field, "non-string value")),
    }
}

fn optional_u64(value: &Value, field: &'static str) -> Result<Option<u64>, PreflightFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| control_field_error(field, "non-u64 value")),
    }
}

fn optional_usize(value: &Value, field: &'static str) -> Result<Option<usize>, PreflightFailure> {
    optional_u64(value, field)?
        .map(|value| {
            usize::try_from(value)
                .map_err(|error| control_field_error(field, format!("usize overflow: {error}")))
        })
        .transpose()
}

fn control_field_error(field: &'static str, detail: impl std::fmt::Display) -> PreflightFailure {
    PreflightFailure {
        reason: CONTROL_FIELD_MISSING_REASON.to_string(),
        detail: format!("{field}: {detail}"),
    }
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn render_human(report: &RestorePreflightReport) -> String {
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
        format!(
            "covered_through_ts_utc={}",
            report.covered_through_ts_utc.as_deref().unwrap_or("null")
        ),
        format!(
            "missing_segments_count={}",
            format_option(report.missing_segments_count)
        ),
        format!("restore_ready={}", report.restore_ready),
        format!("restore_ready_reason={}", report.restore_ready_reason),
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

    fn progress_json(
        replayable_output: bool,
        covered_through: &str,
        missing_segments: &str,
    ) -> String {
        format!(
            r#"{{
  "attempt_number": 9,
  "verdict": "complete_but_insufficient_for_healthy_restore",
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "replayable_output": {replayable_output},
  "missing_segments": {missing_segments},
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }}
}}"#
        )
    }

    #[test]
    fn valid_replayable_complete_no_missing_segments_is_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("complete.progress.json");
        write_progress(
            &progress_path,
            &progress_json(true, "2026-04-20T16:56:04Z", "[]"),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("complete replayable progress should be reportable");

        assert!(report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_ready_explicit_replayable_complete_no_missing_segments"
        );
        assert_eq!(report.replayable_output, Some(true));
        assert_eq!(report.missing_segments_count, Some(0));
        Ok(())
    }

    #[test]
    fn replayable_output_false_at_one_hundred_percent_is_not_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("false-100.progress.json");
        write_progress(
            &progress_path,
            &progress_json(false, "2026-04-20T16:56:04Z", "[]"),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("explicit non-replayable progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_replayable_output_false"
        );
        assert_eq!(report.replayable_output, Some(false));
        Ok(())
    }

    #[test]
    fn missing_segments_present_is_not_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("missing-segments.progress.json");
        write_progress(
            &progress_path,
            &progress_json(
                true,
                "2026-04-20T16:56:04Z",
                r#"[{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T17:56:04Z", "reason": "gap" }]"#,
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("missing segment progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_missing_segments_present"
        );
        assert_eq!(report.missing_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn covered_through_before_window_end_is_not_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("short.progress.json");
        write_progress(
            &progress_path,
            &progress_json(true, "2026-04-20T15:56:04Z", "[]"),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("short coverage progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_covered_through_before_window_end"
        );
        Ok(())
    }

    #[test]
    fn missing_control_fields_fail_closed_stage1() -> Result<()> {
        let temp = tempdir()?;

        let missing_replayable_output = temp.path().join("missing-replayable.progress.json");
        write_progress(
            &missing_replayable_output,
            r#"{
  "verdict": "complete_but_insufficient_for_healthy_restore",
  "current_phase": "complete"
}"#,
        );
        let error = build_preflight_report(&config(missing_replayable_output))
            .expect_err("missing replayable_output must fail");
        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("replayable_output"));

        let missing_verdict = temp.path().join("missing-verdict.progress.json");
        write_progress(
            &missing_verdict,
            r#"{
  "current_phase": "complete",
  "replayable_output": true
}"#,
        );
        let error = build_preflight_report(&config(missing_verdict))
            .expect_err("missing verdict must fail");
        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("verdict"));

        let missing_current_phase = temp.path().join("missing-phase.progress.json");
        write_progress(
            &missing_current_phase,
            r#"{
  "verdict": "complete_but_insufficient_for_healthy_restore",
  "replayable_output": true
}"#,
        );
        let error = build_preflight_report(&config(missing_current_phase))
            .expect_err("missing current_phase must fail");
        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("current_phase"));
        Ok(())
    }

    #[test]
    fn missing_progress_file_fails_with_explicit_operator_error_stage1() {
        let temp = tempdir().expect("tempdir");
        let progress_path = temp.path().join("missing.progress.json");

        let error = build_preflight_report(&config(progress_path))
            .expect_err("missing progress file must fail");

        assert_eq!(
            error.reason,
            "program_history_gap_fill_restore_preflight_unproven_progress_file_missing"
        );
        assert!(error.detail.contains("missing.progress.json"));
    }
}
