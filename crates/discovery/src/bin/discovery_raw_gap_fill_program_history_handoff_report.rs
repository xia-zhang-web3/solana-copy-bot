use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history_handoff_report --progress-path <path> --window-start-utc <rfc3339> --window-end-utc <rfc3339> [--json]";
const REQUIRED_FIELD_REASON: &str =
    "program_history_gap_fill_handoff_report_unproven_required_field_missing_or_malformed";

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

    let report = match build_handoff_report(&config) {
        Ok(report) => report,
        Err(error) => HandoffReport::operator_error(&config, error.reason, error.detail),
    };
    if config.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("handoff report should serialize to json")
        );
    } else {
        println!("{}", render_human(&report));
    }

    if report.operator_reason.is_some() || !report.handoff_ready_for_human_restore_review {
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
struct HandoffReport {
    event: &'static str,
    progress_path: String,
    expected_window_start_utc: String,
    expected_window_end_utc: String,
    requested_window_start: Option<String>,
    requested_window_end: Option<String>,
    verdict: Option<String>,
    reason: Option<String>,
    current_phase: Option<String>,
    handoff_ready_for_human_restore_review: bool,
    handoff_reason: String,
    artifact_valid_for_restore_review: bool,
    replayable_output: Option<bool>,
    covered_through_ts_utc: Option<String>,
    missing_segments_count: Option<usize>,
    inserted_rows: Option<usize>,
    rows_withheld_due_to_incomplete_outcome: Option<usize>,
    recommended_status_command: String,
    recommended_restore_preflight_command: String,
    recommended_artifact_validate_command: String,
    operator_reason: Option<String>,
    operator_detail: Option<String>,
}

#[derive(Debug, Clone)]
struct HandoffFailure {
    reason: String,
    detail: String,
}

impl std::fmt::Display for HandoffFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.reason, self.detail)
    }
}

impl std::error::Error for HandoffFailure {}

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

impl HandoffReport {
    fn operator_error(config: &Config, reason: String, detail: String) -> Self {
        let commands = RecommendedCommands::for_config(config);
        Self {
            event: "discovery_raw_gap_fill_program_history_handoff_report",
            progress_path: config.progress_path.display().to_string(),
            expected_window_start_utc: format_ts(config.window_start_utc),
            expected_window_end_utc: format_ts(config.window_end_utc),
            requested_window_start: None,
            requested_window_end: None,
            verdict: None,
            reason: None,
            current_phase: None,
            handoff_ready_for_human_restore_review: false,
            handoff_reason: reason.clone(),
            artifact_valid_for_restore_review: false,
            replayable_output: None,
            covered_through_ts_utc: None,
            missing_segments_count: None,
            inserted_rows: None,
            rows_withheld_due_to_incomplete_outcome: None,
            recommended_status_command: commands.status,
            recommended_restore_preflight_command: commands.restore_preflight,
            recommended_artifact_validate_command: commands.artifact_validate,
            operator_reason: Some(reason),
            operator_detail: Some(detail),
        }
    }
}

#[derive(Debug, Clone)]
struct RecommendedCommands {
    status: String,
    restore_preflight: String,
    artifact_validate: String,
}

impl RecommendedCommands {
    fn for_config(config: &Config) -> Self {
        let progress_path = shell_quote(&config.progress_path.display().to_string());
        let window_start = shell_quote(&format_ts(config.window_start_utc));
        let window_end = shell_quote(&format_ts(config.window_end_utc));
        let common_args = format!(
            "--progress-path {progress_path} --window-start-utc {window_start} --window-end-utc {window_end} --json"
        );
        Self {
            status: format!("discovery_raw_gap_fill_program_history_status {common_args}"),
            restore_preflight: format!(
                "discovery_raw_gap_fill_program_history_restore_preflight {common_args}"
            ),
            artifact_validate: format!(
                "discovery_raw_gap_fill_program_history_artifact_validate {common_args}"
            ),
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

fn build_handoff_report(config: &Config) -> Result<HandoffReport, HandoffFailure> {
    let artifact = load_artifact_snapshot(&config.progress_path)?;
    let (artifact_valid_for_restore_review, handoff_reason) =
        handoff_validity(&artifact, config.window_start_utc, config.window_end_utc);
    let commands = RecommendedCommands::for_config(config);

    Ok(HandoffReport {
        event: "discovery_raw_gap_fill_program_history_handoff_report",
        progress_path: config.progress_path.display().to_string(),
        expected_window_start_utc: format_ts(config.window_start_utc),
        expected_window_end_utc: format_ts(config.window_end_utc),
        requested_window_start: Some(format_ts(artifact.requested_window_start)),
        requested_window_end: Some(format_ts(artifact.requested_window_end)),
        verdict: Some(artifact.verdict),
        reason: Some(artifact.reason),
        current_phase: Some(artifact.current_phase),
        handoff_ready_for_human_restore_review: artifact_valid_for_restore_review,
        handoff_reason,
        artifact_valid_for_restore_review,
        replayable_output: Some(artifact.replayable_output),
        covered_through_ts_utc: artifact.covered_through.map(format_ts),
        missing_segments_count: Some(artifact.missing_segments_count),
        inserted_rows: Some(artifact.inserted_rows),
        rows_withheld_due_to_incomplete_outcome: Some(
            artifact.rows_withheld_due_to_incomplete_outcome,
        ),
        recommended_status_command: commands.status,
        recommended_restore_preflight_command: commands.restore_preflight,
        recommended_artifact_validate_command: commands.artifact_validate,
        operator_reason: None,
        operator_detail: None,
    })
}

fn handoff_validity(
    artifact: &ArtifactSnapshot,
    window_start_utc: DateTime<Utc>,
    window_end_utc: DateTime<Utc>,
) -> (bool, String) {
    if !artifact.replayable_output {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_replayable_output_false".to_string(),
        );
    }
    if artifact.requested_window_start != window_start_utc {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_requested_window_start_mismatch"
                .to_string(),
        );
    }
    if artifact.requested_window_end != window_end_utc {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_requested_window_end_mismatch".to_string(),
        );
    }
    let Some(covered_through) = artifact.covered_through else {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_missing_covered_through".to_string(),
        );
    };
    if covered_through < window_end_utc {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_covered_through_before_window_end"
                .to_string(),
        );
    }
    if artifact.missing_segments_count > 0 {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_missing_segments_present".to_string(),
        );
    }
    if artifact.inserted_rows == 0 {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_inserted_rows_not_positive".to_string(),
        );
    }
    if artifact.rows_withheld_due_to_incomplete_outcome > 0 {
        return (
            false,
            "program_history_gap_fill_handoff_not_ready_rows_withheld_due_to_incomplete_outcome"
                .to_string(),
        );
    }
    (
        true,
        "program_history_gap_fill_handoff_ready_for_human_restore_review_explicit_replayable_artifact_valid"
            .to_string(),
    )
}

fn load_artifact_snapshot(path: &Path) -> Result<ArtifactSnapshot, HandoffFailure> {
    let raw = fs::read_to_string(path).map_err(|error| HandoffFailure {
        reason: "program_history_gap_fill_handoff_unproven_progress_file_missing".to_string(),
        detail: format!("failed reading progress file {}: {error}", path.display()),
    })?;
    let value = serde_json::from_str::<Value>(&raw).map_err(|error| HandoffFailure {
        reason: "program_history_gap_fill_handoff_unproven_progress_json_parse_failed".to_string(),
        detail: format!("failed parsing progress file {}: {error}", path.display()),
    })?;
    artifact_snapshot_from_value(&value)
}

fn artifact_snapshot_from_value(value: &Value) -> Result<ArtifactSnapshot, HandoffFailure> {
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

fn required_string(value: &Value, field: &'static str) -> Result<String, HandoffFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| required_field_error(field, "missing or non-string value"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, HandoffFailure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| required_field_error(field, "missing or non-bool value"))
}

fn required_u64(value: &Value, field: &'static str) -> Result<u64, HandoffFailure> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| required_field_error(field, "missing or non-u64 value"))
}

fn required_usize(value: &Value, field: &'static str) -> Result<usize, HandoffFailure> {
    let raw = required_u64(value, field)?;
    usize::try_from(raw)
        .map_err(|error| required_field_error(field, format!("usize overflow: {error}")))
}

fn required_ts(value: &Value, field: &'static str) -> Result<DateTime<Utc>, HandoffFailure> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).map_err(|error| required_field_error(field, error))
}

fn required_array_len(value: &Value, field: &'static str) -> Result<usize, HandoffFailure> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or_else(|| required_field_error(field, "missing or non-array value"))
}

fn optional_cursor_ts(
    value: &Value,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, HandoffFailure> {
    value
        .get(field)
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| required_field_error("gap_fill_covered_through_cursor.ts_utc", error))
}

fn required_field_error(field: &'static str, detail: impl std::fmt::Display) -> HandoffFailure {
    HandoffFailure {
        reason: REQUIRED_FIELD_REASON.to_string(),
        detail: format!("{field}: {detail}"),
    }
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn render_human(report: &HandoffReport) -> String {
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
            "handoff_ready_for_human_restore_review={}",
            report.handoff_ready_for_human_restore_review
        ),
        format!("handoff_reason={}", report.handoff_reason),
        format!(
            "artifact_valid_for_restore_review={}",
            report.artifact_valid_for_restore_review
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
            "recommended_status_command={}",
            report.recommended_status_command
        ),
        format!(
            "recommended_restore_preflight_command={}",
            report.recommended_restore_preflight_command
        ),
        format!(
            "recommended_artifact_validate_command={}",
            report.recommended_artifact_validate_command
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

    fn recommended_commands(report: &HandoffReport) -> [&str; 3] {
        [
            report.recommended_status_command.as_str(),
            report.recommended_restore_preflight_command.as_str(),
            report.recommended_artifact_validate_command.as_str(),
        ]
    }

    fn assert_only_read_only_recommended_commands(report: &HandoffReport) {
        let commands = recommended_commands(report);
        assert!(commands[0].contains("discovery_raw_gap_fill_program_history_status"));
        assert!(commands[1].contains("discovery_raw_gap_fill_program_history_restore_preflight"));
        assert!(commands[2].contains("discovery_raw_gap_fill_program_history_artifact_validate"));
        for command in commands {
            assert!(command.contains("--progress-path"));
            assert!(command.contains("--window-start-utc"));
            assert!(command.contains("--window-end-utc"));
            assert!(command.contains("--json"));
            assert!(!command.contains("restore apply"));
            assert!(!command.contains("restore_apply"));
            assert!(!command.contains("runtime restore"));
            assert!(!command.contains("runtime_restore"));
            assert!(!command.contains("discovery_runtime_restore"));
            assert!(!command.contains("discovery_raw_gap_fill_program_history_loop"));
            assert!(!command.contains("discovery_raw_gap_fill_program_history --"));
        }
    }

    #[test]
    fn valid_replayable_exact_window_artifact_emits_handoff_ready_stage1() -> Result<()> {
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

        let report = build_handoff_report(&config(progress_path))?;

        assert!(report.handoff_ready_for_human_restore_review);
        assert!(report.artifact_valid_for_restore_review);
        assert_eq!(
            report.handoff_reason,
            "program_history_gap_fill_handoff_ready_for_human_restore_review_explicit_replayable_artifact_valid"
        );
        assert_only_read_only_recommended_commands(&report);
        Ok(())
    }

    #[test]
    fn replayable_output_false_emits_false_and_read_only_commands_stage1() -> Result<()> {
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

        let report = build_handoff_report(&config(progress_path))?;

        assert!(!report.handoff_ready_for_human_restore_review);
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.handoff_reason,
            "program_history_gap_fill_handoff_not_ready_replayable_output_false"
        );
        assert_only_read_only_recommended_commands(&report);
        Ok(())
    }

    #[test]
    fn window_mismatch_emits_false_stage1() -> Result<()> {
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

        let report = build_handoff_report(&config(progress_path))?;

        assert!(!report.handoff_ready_for_human_restore_review);
        assert_eq!(
            report.handoff_reason,
            "program_history_gap_fill_handoff_not_ready_requested_window_start_mismatch"
        );
        Ok(())
    }

    #[test]
    fn missing_segments_present_emits_false_stage1() -> Result<()> {
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

        let report = build_handoff_report(&config(progress_path))?;

        assert!(!report.handoff_ready_for_human_restore_review);
        assert_eq!(
            report.handoff_reason,
            "program_history_gap_fill_handoff_not_ready_missing_segments_present"
        );
        assert_eq!(report.missing_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn rows_withheld_emits_false_stage1() -> Result<()> {
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
            2,
        );

        let report = build_handoff_report(&config(progress_path))?;

        assert!(!report.handoff_ready_for_human_restore_review);
        assert_eq!(
            report.handoff_reason,
            "program_history_gap_fill_handoff_not_ready_rows_withheld_due_to_incomplete_outcome"
        );
        Ok(())
    }

    #[test]
    fn missing_required_field_fails_closed_with_operator_reason_stage1() -> Result<()> {
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

        let error =
            build_handoff_report(&config(progress_path)).expect_err("missing verdict must fail");

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

        let error = build_handoff_report(&config(progress_path))
            .expect_err("malformed requested_window_start must fail");

        assert_eq!(error.reason, REQUIRED_FIELD_REASON);
        assert!(error.detail.contains("requested_window_start"));
        Ok(())
    }

    #[test]
    fn missing_progress_file_emits_explicit_operator_error_stage1() {
        let temp = tempdir().expect("tempdir");
        let progress_path = temp.path().join("missing.progress.json");

        let error =
            build_handoff_report(&config(progress_path)).expect_err("missing file must fail");

        assert_eq!(
            error.reason,
            "program_history_gap_fill_handoff_unproven_progress_file_missing"
        );
        assert!(error.detail.contains("missing.progress.json"));
    }

    #[test]
    fn operator_error_report_still_recommends_only_read_only_commands_stage1() {
        let temp = tempdir().expect("tempdir");
        let progress_path = temp.path().join("missing.progress.json");
        let config = config(progress_path);

        let report = HandoffReport::operator_error(
            &config,
            "program_history_gap_fill_handoff_unproven_progress_file_missing".to_string(),
            "missing".to_string(),
        );

        assert!(!report.handoff_ready_for_human_restore_review);
        assert_only_read_only_recommended_commands(&report);
    }
}
