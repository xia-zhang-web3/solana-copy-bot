use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history_artifact_validate --progress-path <path> --window-start-utc <rfc3339> --window-end-utc <rfc3339> [--json]";
const REQUIRED_FIELD_REASON: &str =
    "program_history_gap_fill_artifact_validate_unproven_required_field_missing_or_malformed";

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

    let report = match build_artifact_validation_report(&config) {
        Ok(report) => report,
        Err(error) => ArtifactValidationReport::operator_error(&config, error.reason, error.detail),
    };
    if config.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .expect("artifact validation report should serialize to json")
        );
    } else {
        println!("{}", render_human(&report));
    }

    if report.operator_reason.is_some() || !report.artifact_valid_for_restore_review {
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
struct ArtifactValidationReport {
    event: &'static str,
    progress_path: String,
    expected_window_start_utc: String,
    expected_window_end_utc: String,
    requested_window_start: Option<String>,
    requested_window_end: Option<String>,
    verdict: Option<String>,
    reason: Option<String>,
    current_phase: Option<String>,
    replayable_output: Option<bool>,
    covered_through_ts_utc: Option<String>,
    missing_segments_count: Option<usize>,
    inserted_rows: Option<usize>,
    rows_withheld_due_to_incomplete_outcome: Option<usize>,
    artifact_valid_for_restore_review: bool,
    artifact_validation_reason: String,
    operator_reason: Option<String>,
    operator_detail: Option<String>,
}

#[derive(Debug, Clone)]
struct ArtifactValidationFailure {
    reason: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct ArtifactSnapshot {
    verdict: String,
    reason: String,
    current_phase: String,
    replayable_output: bool,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    covered_through: Option<DateTime<Utc>>,
    missing_segments_count: usize,
    inserted_rows: usize,
    rows_withheld_due_to_incomplete_outcome: usize,
}

impl ArtifactValidationReport {
    fn operator_error(config: &Config, reason: String, detail: String) -> Self {
        Self {
            event: "discovery_raw_gap_fill_program_history_artifact_validate",
            progress_path: config.progress_path.display().to_string(),
            expected_window_start_utc: format_ts(config.window_start_utc),
            expected_window_end_utc: format_ts(config.window_end_utc),
            requested_window_start: None,
            requested_window_end: None,
            verdict: None,
            reason: None,
            current_phase: None,
            replayable_output: None,
            covered_through_ts_utc: None,
            missing_segments_count: None,
            inserted_rows: None,
            rows_withheld_due_to_incomplete_outcome: None,
            artifact_valid_for_restore_review: false,
            artifact_validation_reason: reason.clone(),
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

fn build_artifact_validation_report(
    config: &Config,
) -> Result<ArtifactValidationReport, ArtifactValidationFailure> {
    let artifact = load_artifact_snapshot(&config.progress_path)?;
    let (artifact_valid_for_restore_review, artifact_validation_reason) =
        artifact_review_validity(&artifact, config.window_start_utc, config.window_end_utc);

    Ok(ArtifactValidationReport {
        event: "discovery_raw_gap_fill_program_history_artifact_validate",
        progress_path: config.progress_path.display().to_string(),
        expected_window_start_utc: format_ts(config.window_start_utc),
        expected_window_end_utc: format_ts(config.window_end_utc),
        requested_window_start: Some(format_ts(artifact.requested_window_start)),
        requested_window_end: Some(format_ts(artifact.requested_window_end)),
        verdict: Some(artifact.verdict),
        reason: Some(artifact.reason),
        current_phase: Some(artifact.current_phase),
        replayable_output: Some(artifact.replayable_output),
        covered_through_ts_utc: artifact.covered_through.map(format_ts),
        missing_segments_count: Some(artifact.missing_segments_count),
        inserted_rows: Some(artifact.inserted_rows),
        rows_withheld_due_to_incomplete_outcome: Some(
            artifact.rows_withheld_due_to_incomplete_outcome,
        ),
        artifact_valid_for_restore_review,
        artifact_validation_reason,
        operator_reason: None,
        operator_detail: None,
    })
}

fn artifact_review_validity(
    artifact: &ArtifactSnapshot,
    window_start_utc: DateTime<Utc>,
    window_end_utc: DateTime<Utc>,
) -> (bool, String) {
    if !artifact.replayable_output {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_replayable_output_false"
                .to_string(),
        );
    }
    if artifact.requested_window_start != window_start_utc {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_requested_window_start_mismatch"
                .to_string(),
        );
    }
    if artifact.requested_window_end != window_end_utc {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_requested_window_end_mismatch"
                .to_string(),
        );
    }
    let Some(covered_through) = artifact.covered_through else {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_missing_covered_through"
                .to_string(),
        );
    };
    if covered_through < window_end_utc {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_covered_through_before_window_end"
                .to_string(),
        );
    }
    if artifact.missing_segments_count > 0 {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_missing_segments_present"
                .to_string(),
        );
    }
    if artifact.inserted_rows == 0 {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_inserted_rows_not_positive"
                .to_string(),
        );
    }
    if artifact.rows_withheld_due_to_incomplete_outcome > 0 {
        return (
            false,
            "program_history_gap_fill_artifact_validate_not_ready_rows_withheld_due_to_incomplete_outcome"
                .to_string(),
        );
    }
    (
        true,
        "program_history_gap_fill_artifact_validate_ready_explicit_replayable_exact_window_complete_no_missing_or_withheld_rows"
            .to_string(),
    )
}

fn load_artifact_snapshot(path: &Path) -> Result<ArtifactSnapshot, ArtifactValidationFailure> {
    let raw = fs::read_to_string(path).map_err(|error| ArtifactValidationFailure {
        reason: "program_history_gap_fill_artifact_validate_unproven_progress_file_missing"
            .to_string(),
        detail: format!("failed reading progress file {}: {error}", path.display()),
    })?;
    let value = serde_json::from_str::<Value>(&raw).map_err(|error| ArtifactValidationFailure {
        reason: "program_history_gap_fill_artifact_validate_unproven_progress_json_parse_failed"
            .to_string(),
        detail: format!("failed parsing progress file {}: {error}", path.display()),
    })?;
    artifact_snapshot_from_value(&value)
}

fn artifact_snapshot_from_value(
    value: &Value,
) -> Result<ArtifactSnapshot, ArtifactValidationFailure> {
    let verdict = required_string(value, "verdict")?;
    let reason = required_string(value, "reason")?;
    let current_phase = required_string(value, "current_phase")?;
    let replayable_output = required_bool(value, "replayable_output")?;
    let requested_window_start = required_ts(value, "requested_window_start")?;
    let requested_window_end = required_ts(value, "requested_window_end")?;
    let missing_segments_count = required_array_len(value, "missing_segments")?;
    let inserted_rows = required_usize(value, "inserted_rows")?;
    let rows_withheld_due_to_incomplete_outcome =
        required_usize(value, "rows_withheld_due_to_incomplete_outcome")?;
    let covered_through = optional_cursor_ts(value, "gap_fill_covered_through_cursor")?;
    if replayable_output && covered_through.is_none() {
        return Err(required_field_error(
            "gap_fill_covered_through_cursor.ts_utc",
            "missing for replayable_output=true",
        ));
    }

    Ok(ArtifactSnapshot {
        verdict,
        reason,
        current_phase,
        replayable_output,
        requested_window_start,
        requested_window_end,
        covered_through,
        missing_segments_count,
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome,
    })
}

fn required_string(
    value: &Value,
    field: &'static str,
) -> Result<String, ArtifactValidationFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| required_field_error(field, "missing or non-string value"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, ArtifactValidationFailure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| required_field_error(field, "missing or non-bool value"))
}

fn required_u64(value: &Value, field: &'static str) -> Result<u64, ArtifactValidationFailure> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| required_field_error(field, "missing or non-u64 value"))
}

fn required_usize(value: &Value, field: &'static str) -> Result<usize, ArtifactValidationFailure> {
    let raw = required_u64(value, field)?;
    usize::try_from(raw)
        .map_err(|error| required_field_error(field, format!("usize overflow: {error}")))
}

fn required_ts(
    value: &Value,
    field: &'static str,
) -> Result<DateTime<Utc>, ArtifactValidationFailure> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).map_err(|error| required_field_error(field, error))
}

fn required_array_len(
    value: &Value,
    field: &'static str,
) -> Result<usize, ArtifactValidationFailure> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or_else(|| required_field_error(field, "missing or non-array value"))
}

fn optional_cursor_ts(
    value: &Value,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, ArtifactValidationFailure> {
    value
        .get(field)
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| required_field_error("gap_fill_covered_through_cursor.ts_utc", error))
}

fn required_field_error(
    field: &'static str,
    detail: impl std::fmt::Display,
) -> ArtifactValidationFailure {
    ArtifactValidationFailure {
        reason: REQUIRED_FIELD_REASON.to_string(),
        detail: format!("{field}: {detail}"),
    }
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn render_human(report: &ArtifactValidationReport) -> String {
    [
        format!("event={}", report.event),
        format!("progress_path={}", report.progress_path),
        format!(
            "expected_window_start_utc={}",
            report.expected_window_start_utc
        ),
        format!("expected_window_end_utc={}", report.expected_window_end_utc),
        format!(
            "requested_window_start={}",
            report.requested_window_start.as_deref().unwrap_or("null")
        ),
        format!(
            "requested_window_end={}",
            report.requested_window_end.as_deref().unwrap_or("null")
        ),
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
        format!("inserted_rows={}", format_option(report.inserted_rows)),
        format!(
            "rows_withheld_due_to_incomplete_outcome={}",
            format_option(report.rows_withheld_due_to_incomplete_outcome)
        ),
        format!(
            "artifact_valid_for_restore_review={}",
            report.artifact_valid_for_restore_review
        ),
        format!(
            "artifact_validation_reason={}",
            report.artifact_validation_reason
        ),
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

    fn artifact_json(
        replayable_output: bool,
        requested_window_start: &str,
        requested_window_end: &str,
        covered_through: &str,
        missing_segments: &str,
        inserted_rows: usize,
        rows_withheld: usize,
    ) -> String {
        format!(
            r#"{{
  "verdict": "complete_but_insufficient_for_healthy_restore",
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "replayable_output": {replayable_output},
  "requested_window_start": "{requested_window_start}",
  "requested_window_end": "{requested_window_end}",
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }},
  "missing_segments": {missing_segments},
  "inserted_rows": {inserted_rows},
  "rows_withheld_due_to_incomplete_outcome": {rows_withheld}
}}"#
        )
    }

    fn write_artifact(
        progress_path: &Path,
        replayable_output: bool,
        requested_window_start: &str,
        requested_window_end: &str,
        covered_through: &str,
        missing_segments: &str,
        inserted_rows: usize,
        rows_withheld: usize,
    ) {
        write_progress(
            progress_path,
            &artifact_json(
                replayable_output,
                requested_window_start,
                requested_window_end,
                covered_through,
                missing_segments,
                inserted_rows,
                rows_withheld,
            ),
        );
    }

    #[test]
    fn valid_replayable_exact_window_artifact_is_valid_for_restore_review_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            true,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "[]",
            10,
            0,
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("valid artifact should report");

        assert!(report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_ready_explicit_replayable_exact_window_complete_no_missing_or_withheld_rows"
        );
        assert_eq!(report.inserted_rows, Some(10));
        Ok(())
    }

    #[test]
    fn replayable_output_false_is_not_valid_for_restore_review_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            false,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "[]",
            10,
            0,
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("non-replayable artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_replayable_output_false"
        );
        Ok(())
    }

    #[test]
    fn requested_window_mismatch_is_not_valid_for_restore_review_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            true,
            "2026-04-18T17:56:04Z",
            "2026-04-20T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "[]",
            10,
            0,
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("window mismatch artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_requested_window_start_mismatch"
        );
        Ok(())
    }

    #[test]
    fn covered_through_before_window_end_is_not_valid_for_restore_review_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            true,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "2026-04-20T15:56:04Z",
            "[]",
            10,
            0,
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("short coverage artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_covered_through_before_window_end"
        );
        Ok(())
    }

    #[test]
    fn missing_segments_present_is_not_valid_for_restore_review_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            true,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "2026-04-20T16:56:04Z",
            r#"[{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T17:56:04Z", "reason": "gap" }]"#,
            10,
            0,
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("missing segment artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_missing_segments_present"
        );
        assert_eq!(report.missing_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn withheld_rows_are_not_valid_for_restore_review_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            true,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "[]",
            10,
            3,
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("withheld rows artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_rows_withheld_due_to_incomplete_outcome"
        );
        Ok(())
    }

    #[test]
    fn missing_required_control_field_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            r#"{
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "replayable_output": true,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "gap_fill_covered_through_cursor": { "ts_utc": "2026-04-20T16:56:04Z" },
  "missing_segments": [],
  "inserted_rows": 10,
  "rows_withheld_due_to_incomplete_outcome": 0
}"#,
        );

        let error = build_artifact_validation_report(&config(progress_path))
            .expect_err("missing verdict must fail closed");

        assert_eq!(error.reason, REQUIRED_FIELD_REASON);
        assert!(error.detail.contains("verdict"));
        Ok(())
    }

    #[test]
    fn malformed_timestamp_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_artifact(
            &progress_path,
            true,
            "not-a-timestamp",
            "2026-04-20T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "[]",
            10,
            0,
        );

        let error = build_artifact_validation_report(&config(progress_path))
            .expect_err("malformed requested_window_start must fail closed");

        assert_eq!(error.reason, REQUIRED_FIELD_REASON);
        assert!(error.detail.contains("requested_window_start"));
        Ok(())
    }

    #[test]
    fn missing_progress_file_fails_with_explicit_operator_error_stage1() {
        let temp = tempdir().expect("tempdir");
        let progress_path = temp.path().join("missing.progress.json");

        let error = build_artifact_validation_report(&config(progress_path))
            .expect_err("missing progress file must fail");

        assert_eq!(
            error.reason,
            "program_history_gap_fill_artifact_validate_unproven_progress_file_missing"
        );
        assert!(error.detail.contains("missing.progress.json"));
    }
}
