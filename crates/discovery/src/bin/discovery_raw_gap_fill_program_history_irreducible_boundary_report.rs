use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history_irreducible_boundary_report --progress-path <path> --window-start-utc <rfc3339> --window-end-utc <rfc3339> --json";
const REQUIRED_FIELD_REASON: &str =
    "program_history_irreducible_boundary_report_unproven_required_field_missing_or_malformed";
const IRREDUCIBLE_VERDICT: &str = "not_proven_due_to_irreducible_boundary_evidence";
const IRREDUCIBLE_REASON: &str =
    "program_history_gap_fill_repair_explicit_missing_segments_irreducible_boundary_evidence_remains";
const ZERO_PROGRESS_ROOT_REASON: &str =
    "program_history_gap_fill_skipped_persistently_provider_blocked_slot_after_bounded_retries";
const BOUNDARY_PREFIX_REASON: &str =
    "requested_window_prefix_uncovered_after_start_slot_adjustment";
const BOUNDARY_SUFFIX_REASON: &str = "requested_window_suffix_uncovered_after_end_slot_adjustment";

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

    let report = match build_report(&config) {
        Ok(report) => report,
        Err(error) => {
            IrreducibleBoundaryReport::operator_error(&config, error.reason, error.detail)
        }
    };
    if config.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .expect("irreducible boundary report should serialize to json")
        );
    } else {
        println!("{}", render_human(&report));
    }

    if report.operator_reason.is_some() || !report.requires_human_contract_decision {
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
struct MissingSegmentReport {
    start: String,
    end: String,
    reason: String,
    duration_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct IrreducibleBoundaryReport {
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
    restore_ready: bool,
    production_green: bool,
    requires_human_contract_decision: bool,
    decision_reason: String,
    total_missing_segments: Option<usize>,
    target_boundary_segments_count: Option<usize>,
    non_target_segments_count: Option<usize>,
    unknown_non_target_segments_count: Option<usize>,
    zero_progress_root_segments_count: Option<usize>,
    irreducible_boundary_segments_count: Option<usize>,
    total_irreducible_boundary_missing_ms: Option<i64>,
    max_irreducible_boundary_segment_ms: Option<i64>,
    irreducible_boundary_segments: Vec<MissingSegmentReport>,
    non_target_evidence_segments: Vec<MissingSegmentReport>,
    window_duration_ms: Option<i64>,
    irreducible_boundary_fraction_of_window: Option<f64>,
    operator_reason: Option<String>,
    operator_detail: Option<String>,
}

#[derive(Debug, Clone)]
struct ReportFailure {
    reason: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct ProgressSnapshot {
    verdict: String,
    reason: String,
    current_phase: String,
    replayable_output: bool,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    missing_segments: Vec<MissingSegment>,
}

#[derive(Debug, Clone, PartialEq)]
struct MissingSegment {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    reason: String,
}

impl IrreducibleBoundaryReport {
    fn operator_error(config: &Config, reason: String, detail: String) -> Self {
        Self {
            event: "discovery_raw_gap_fill_program_history_irreducible_boundary_report",
            progress_path: config.progress_path.display().to_string(),
            expected_window_start_utc: format_ts(config.window_start_utc),
            expected_window_end_utc: format_ts(config.window_end_utc),
            requested_window_start: None,
            requested_window_end: None,
            verdict: None,
            reason: None,
            current_phase: None,
            replayable_output: None,
            restore_ready: false,
            production_green: false,
            requires_human_contract_decision: false,
            decision_reason: reason.clone(),
            total_missing_segments: None,
            target_boundary_segments_count: None,
            non_target_segments_count: None,
            unknown_non_target_segments_count: None,
            zero_progress_root_segments_count: None,
            irreducible_boundary_segments_count: None,
            total_irreducible_boundary_missing_ms: None,
            max_irreducible_boundary_segment_ms: None,
            irreducible_boundary_segments: Vec::new(),
            non_target_evidence_segments: Vec::new(),
            window_duration_ms: None,
            irreducible_boundary_fraction_of_window: None,
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
    if !json {
        bail!("missing --json\n{USAGE}");
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

fn build_report(config: &Config) -> Result<IrreducibleBoundaryReport, ReportFailure> {
    let progress = load_progress_snapshot(&config.progress_path)?;
    if progress.requested_window_start != config.window_start_utc
        || progress.requested_window_end != config.window_end_utc
    {
        return Err(ReportFailure {
            reason: "program_history_irreducible_boundary_report_window_mismatch".to_string(),
            detail: format!(
                "progress requested window {}..{} does not match expected {}..{}",
                format_ts(progress.requested_window_start),
                format_ts(progress.requested_window_end),
                format_ts(config.window_start_utc),
                format_ts(config.window_end_utc)
            ),
        });
    }

    let boundary_segments = progress
        .missing_segments
        .iter()
        .filter(|segment| is_boundary_reason(&segment.reason))
        .cloned()
        .collect::<Vec<_>>();
    let zero_progress_root_segments_count = progress
        .missing_segments
        .iter()
        .filter(|segment| segment.reason == ZERO_PROGRESS_ROOT_REASON)
        .count();
    let non_target_segments = progress
        .missing_segments
        .iter()
        .filter(|segment| {
            !is_boundary_reason(&segment.reason) && segment.reason != ZERO_PROGRESS_ROOT_REASON
        })
        .cloned()
        .collect::<Vec<_>>();
    let unknown_non_target_segments_count = non_target_segments
        .iter()
        .filter(|segment| !is_known_synthetic_non_target_reason(&segment.reason))
        .count();
    let boundary_reports = boundary_segments
        .iter()
        .map(missing_segment_report)
        .collect::<Vec<_>>();
    let non_target_reports = non_target_segments
        .iter()
        .map(missing_segment_report)
        .collect::<Vec<_>>();
    let total_boundary_ms = boundary_reports
        .iter()
        .map(|segment| segment.duration_ms)
        .sum::<i64>();
    let max_boundary_ms = boundary_reports
        .iter()
        .map(|segment| segment.duration_ms)
        .max()
        .unwrap_or(0);
    let window_duration_ms = config
        .window_end_utc
        .signed_duration_since(config.window_start_utc)
        .num_milliseconds();
    if window_duration_ms <= 0 {
        return Err(ReportFailure {
            reason: REQUIRED_FIELD_REASON.to_string(),
            detail: "window duration must be positive".to_string(),
        });
    }

    let (requires_human_contract_decision, decision_reason) = decision_surface(
        &progress,
        boundary_segments.len(),
        unknown_non_target_segments_count,
        zero_progress_root_segments_count,
    );

    Ok(IrreducibleBoundaryReport {
        event: "discovery_raw_gap_fill_program_history_irreducible_boundary_report",
        progress_path: config.progress_path.display().to_string(),
        expected_window_start_utc: format_ts(config.window_start_utc),
        expected_window_end_utc: format_ts(config.window_end_utc),
        requested_window_start: Some(format_ts(progress.requested_window_start)),
        requested_window_end: Some(format_ts(progress.requested_window_end)),
        verdict: Some(progress.verdict),
        reason: Some(progress.reason),
        current_phase: Some(progress.current_phase),
        replayable_output: Some(progress.replayable_output),
        restore_ready: false,
        production_green: false,
        requires_human_contract_decision,
        decision_reason,
        total_missing_segments: Some(progress.missing_segments.len()),
        target_boundary_segments_count: Some(boundary_segments.len()),
        non_target_segments_count: Some(non_target_segments.len()),
        unknown_non_target_segments_count: Some(unknown_non_target_segments_count),
        zero_progress_root_segments_count: Some(zero_progress_root_segments_count),
        irreducible_boundary_segments_count: Some(boundary_segments.len()),
        total_irreducible_boundary_missing_ms: Some(total_boundary_ms),
        max_irreducible_boundary_segment_ms: Some(max_boundary_ms),
        irreducible_boundary_segments: boundary_reports,
        non_target_evidence_segments: non_target_reports,
        window_duration_ms: Some(window_duration_ms),
        irreducible_boundary_fraction_of_window: Some(
            total_boundary_ms as f64 / window_duration_ms as f64,
        ),
        operator_reason: None,
        operator_detail: None,
    })
}

fn decision_surface(
    progress: &ProgressSnapshot,
    target_boundary_segments_count: usize,
    unknown_non_target_segments_count: usize,
    zero_progress_root_segments_count: usize,
) -> (bool, String) {
    if progress.replayable_output {
        return (
            false,
            "program_history_irreducible_boundary_report_artifact_already_replayable_use_restore_preflight"
                .to_string(),
        );
    }
    if progress.verdict != IRREDUCIBLE_VERDICT {
        return (
            false,
            "program_history_irreducible_boundary_report_verdict_not_irreducible_boundary_evidence"
                .to_string(),
        );
    }
    if progress.reason != IRREDUCIBLE_REASON {
        return (
            false,
            "program_history_irreducible_boundary_report_reason_not_irreducible_boundary_evidence"
                .to_string(),
        );
    }
    if progress.current_phase != "completed_with_explicit_missing_segments" {
        return (
            false,
            "program_history_irreducible_boundary_report_current_phase_not_completed_with_explicit_missing_segments"
                .to_string(),
        );
    }
    if zero_progress_root_segments_count > 0 {
        return (
            false,
            "program_history_irreducible_boundary_report_root_segments_still_remain".to_string(),
        );
    }
    if unknown_non_target_segments_count > 0 {
        return (
            false,
            "program_history_irreducible_boundary_report_unexpected_non_target_evidence_remains"
                .to_string(),
        );
    }
    if target_boundary_segments_count == 0 {
        return (
            false,
            "program_history_irreducible_boundary_report_no_target_boundary_segments".to_string(),
        );
    }
    (
        true,
        "program_history_irreducible_boundary_report_requires_human_contract_decision".to_string(),
    )
}

fn load_progress_snapshot(path: &Path) -> Result<ProgressSnapshot, ReportFailure> {
    let raw = fs::read_to_string(path).map_err(|error| ReportFailure {
        reason: "program_history_irreducible_boundary_report_progress_file_missing".to_string(),
        detail: format!("failed reading progress file {}: {error}", path.display()),
    })?;
    let value = serde_json::from_str::<Value>(&raw).map_err(|error| ReportFailure {
        reason: "program_history_irreducible_boundary_report_progress_json_parse_failed"
            .to_string(),
        detail: format!("failed parsing progress file {}: {error}", path.display()),
    })?;
    progress_snapshot_from_value(&value)
}

fn progress_snapshot_from_value(value: &Value) -> Result<ProgressSnapshot, ReportFailure> {
    let verdict = required_string(value, "verdict")?;
    let reason = required_string(value, "reason")?;
    let current_phase = required_string(value, "current_phase")?;
    let replayable_output = required_bool(value, "replayable_output")?;
    let requested_window_start = required_ts(value, "requested_window_start")?;
    let requested_window_end = required_ts(value, "requested_window_end")?;
    let missing_segments = required_missing_segments(value)?;

    Ok(ProgressSnapshot {
        verdict,
        reason,
        current_phase,
        replayable_output,
        requested_window_start,
        requested_window_end,
        missing_segments,
    })
}

fn required_missing_segments(value: &Value) -> Result<Vec<MissingSegment>, ReportFailure> {
    let segments = value
        .get("missing_segments")
        .and_then(Value::as_array)
        .ok_or_else(|| required_field_error("missing_segments", "missing or non-array value"))?;
    segments
        .iter()
        .enumerate()
        .map(|(index, segment)| missing_segment_from_value(index, segment))
        .collect()
}

fn missing_segment_from_value(
    index: usize,
    value: &Value,
) -> Result<MissingSegment, ReportFailure> {
    let start = required_segment_ts(value, index, "start")?;
    let end = required_segment_ts(value, index, "end")?;
    if end < start {
        return Err(required_field_error(
            "missing_segments.duration",
            format!("segment {index} end is before start"),
        ));
    }
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            required_field_error(
                "missing_segments.reason",
                format!("segment {index} missing or non-string reason"),
            )
        })?;
    Ok(MissingSegment { start, end, reason })
}

fn required_segment_ts(
    value: &Value,
    index: usize,
    field: &'static str,
) -> Result<DateTime<Utc>, ReportFailure> {
    let raw = value.get(field).and_then(Value::as_str).ok_or_else(|| {
        required_field_error(
            "missing_segments.timestamp",
            format!("segment {index} missing or non-string {field}"),
        )
    })?;
    parse_ts(raw).map_err(|error| {
        required_field_error(
            "missing_segments.timestamp",
            format!("segment {index} invalid {field}: {error}"),
        )
    })
}

fn required_string(value: &Value, field: &'static str) -> Result<String, ReportFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| required_field_error(field, "missing or non-string value"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, ReportFailure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| required_field_error(field, "missing or non-bool value"))
}

fn required_ts(value: &Value, field: &'static str) -> Result<DateTime<Utc>, ReportFailure> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).map_err(|error| required_field_error(field, error))
}

fn required_field_error(field: &'static str, detail: impl std::fmt::Display) -> ReportFailure {
    ReportFailure {
        reason: REQUIRED_FIELD_REASON.to_string(),
        detail: format!("{field}: {detail}"),
    }
}

fn is_boundary_reason(reason: &str) -> bool {
    reason == BOUNDARY_PREFIX_REASON || reason == BOUNDARY_SUFFIX_REASON
}

fn is_known_synthetic_non_target_reason(reason: &str) -> bool {
    reason == IRREDUCIBLE_REASON
}

fn missing_segment_report(segment: &MissingSegment) -> MissingSegmentReport {
    MissingSegmentReport {
        start: format_ts(segment.start),
        end: format_ts(segment.end),
        reason: segment.reason.clone(),
        duration_ms: segment_duration_ms(segment),
    }
}

fn segment_duration_ms(segment: &MissingSegment) -> i64 {
    segment
        .end
        .signed_duration_since(segment.start)
        .num_milliseconds()
        .max(0)
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn render_human(report: &IrreducibleBoundaryReport) -> String {
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
        format!("restore_ready={}", report.restore_ready),
        format!("production_green={}", report.production_green),
        format!(
            "requires_human_contract_decision={}",
            report.requires_human_contract_decision
        ),
        format!("decision_reason={}", report.decision_reason),
        format!(
            "total_missing_segments={}",
            format_option(report.total_missing_segments)
        ),
        format!(
            "target_boundary_segments_count={}",
            format_option(report.target_boundary_segments_count)
        ),
        format!(
            "non_target_segments_count={}",
            format_option(report.non_target_segments_count)
        ),
        format!(
            "unknown_non_target_segments_count={}",
            format_option(report.unknown_non_target_segments_count)
        ),
        format!(
            "zero_progress_root_segments_count={}",
            format_option(report.zero_progress_root_segments_count)
        ),
        format!(
            "irreducible_boundary_segments_count={}",
            format_option(report.irreducible_boundary_segments_count)
        ),
        format!(
            "total_irreducible_boundary_missing_ms={}",
            format_option(report.total_irreducible_boundary_missing_ms)
        ),
        format!(
            "max_irreducible_boundary_segment_ms={}",
            format_option(report.max_irreducible_boundary_segment_ms)
        ),
        format!(
            "window_duration_ms={}",
            format_option(report.window_duration_ms)
        ),
        format!(
            "irreducible_boundary_fraction_of_window={}",
            format_option(report.irreducible_boundary_fraction_of_window)
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

    fn progress_json(
        verdict: &str,
        reason: &str,
        current_phase: &str,
        replayable_output: bool,
        requested_window_start: &str,
        requested_window_end: &str,
        missing_segments: &str,
    ) -> String {
        format!(
            r#"{{
  "verdict": "{verdict}",
  "reason": "{reason}",
  "current_phase": "{current_phase}",
  "replayable_output": {replayable_output},
  "requested_window_start": "{requested_window_start}",
  "requested_window_end": "{requested_window_end}",
  "missing_segments": {missing_segments}
}}"#
        )
    }

    fn boundary_segment(start: &str, end: &str, reason: &str) -> String {
        format!(r#"{{"start":"{start}","end":"{end}","reason":"{reason}"}}"#)
    }

    fn synthetic_segment() -> String {
        boundary_segment(
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            IRREDUCIBLE_REASON,
        )
    }

    fn valid_irreducible_progress(missing_segments: &str) -> String {
        progress_json(
            IRREDUCIBLE_VERDICT,
            IRREDUCIBLE_REASON,
            "completed_with_explicit_missing_segments",
            false,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            missing_segments,
        )
    }

    #[test]
    fn valid_irreducible_boundary_report_with_synthetic_non_target_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("irreducible.progress.json");
        let prefix = boundary_segment(
            "2026-04-19T20:06:31Z",
            "2026-04-19T20:06:32Z",
            BOUNDARY_PREFIX_REASON,
        );
        let suffix = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:03Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &valid_irreducible_progress(&format!("[{prefix},{suffix},{}]", synthetic_segment())),
        );

        let report = build_report(&config(progress_path)).expect("valid report should build");

        assert!(report.requires_human_contract_decision);
        assert!(!report.restore_ready);
        assert!(!report.production_green);
        assert_eq!(report.total_missing_segments, Some(3));
        assert_eq!(report.target_boundary_segments_count, Some(2));
        assert_eq!(report.non_target_segments_count, Some(1));
        assert_eq!(report.unknown_non_target_segments_count, Some(0));
        assert_eq!(report.zero_progress_root_segments_count, Some(0));
        assert_eq!(report.irreducible_boundary_segments_count, Some(2));
        assert_eq!(report.total_irreducible_boundary_missing_ms, Some(3_000));
        assert_eq!(report.max_irreducible_boundary_segment_ms, Some(2_000));
        assert_eq!(report.irreducible_boundary_segments.len(), 2);
        assert_eq!(report.non_target_evidence_segments.len(), 1);
        assert!(report
            .irreducible_boundary_fraction_of_window
            .is_some_and(|value| value > 0.0 && value < 0.001));
        Ok(())
    }

    #[test]
    fn unknown_non_target_reason_blocks_decision_surface_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("unknown-non-target.progress.json");
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        let unknown = boundary_segment(
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            "program_history_gap_fill_repair_explicit_missing_segments_non_target_segments_remain",
        );
        write_progress(
            &progress_path,
            &valid_irreducible_progress(&format!("[{boundary},{unknown}]")),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert!(!report.requires_human_contract_decision);
        assert_eq!(
            report.decision_reason,
            "program_history_irreducible_boundary_report_unexpected_non_target_evidence_remains"
        );
        assert_eq!(report.non_target_segments_count, Some(1));
        assert_eq!(report.unknown_non_target_segments_count, Some(1));
        assert_eq!(report.non_target_evidence_segments.len(), 1);
        Ok(())
    }

    #[test]
    fn known_synthetic_irreducible_full_window_reason_does_not_block_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("known-synthetic.progress.json");
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &valid_irreducible_progress(&format!("[{boundary},{}]", synthetic_segment())),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert!(report.requires_human_contract_decision);
        assert_eq!(report.non_target_segments_count, Some(1));
        assert_eq!(report.unknown_non_target_segments_count, Some(0));
        Ok(())
    }

    #[test]
    fn root_zero_progress_segment_blocks_decision_surface_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("root.progress.json");
        let root = boundary_segment(
            "2026-04-19T20:06:31Z",
            "2026-04-19T20:06:32Z",
            ZERO_PROGRESS_ROOT_REASON,
        );
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &valid_irreducible_progress(&format!("[{root},{boundary}]")),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert!(!report.requires_human_contract_decision);
        assert_eq!(
            report.decision_reason,
            "program_history_irreducible_boundary_report_root_segments_still_remain"
        );
        assert_eq!(report.zero_progress_root_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn wrong_verdict_blocks_decision_surface_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("wrong-verdict.progress.json");
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &progress_json(
                "not_proven_due_to_provider_throttling",
                IRREDUCIBLE_REASON,
                "completed_with_explicit_missing_segments",
                false,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &format!("[{boundary}]"),
            ),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert!(!report.requires_human_contract_decision);
        assert_eq!(
            report.decision_reason,
            "program_history_irreducible_boundary_report_verdict_not_irreducible_boundary_evidence"
        );
        Ok(())
    }

    #[test]
    fn wrong_current_phase_blocks_decision_surface_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("wrong-phase.progress.json");
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &progress_json(
                IRREDUCIBLE_VERDICT,
                IRREDUCIBLE_REASON,
                "awaiting_next_attempt",
                false,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &format!("[{boundary}]"),
            ),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert!(!report.requires_human_contract_decision);
        assert_eq!(
            report.decision_reason,
            "program_history_irreducible_boundary_report_current_phase_not_completed_with_explicit_missing_segments"
        );
        Ok(())
    }

    #[test]
    fn replayable_output_true_blocks_decision_surface_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("replayable.progress.json");
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &progress_json(
                IRREDUCIBLE_VERDICT,
                IRREDUCIBLE_REASON,
                "completed_with_explicit_missing_segments",
                true,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &format!("[{boundary}]"),
            ),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert!(!report.requires_human_contract_decision);
        assert_eq!(
            report.decision_reason,
            "program_history_irreducible_boundary_report_artifact_already_replayable_use_restore_preflight"
        );
        assert!(!report.restore_ready);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn window_mismatch_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("window-mismatch.progress.json");
        write_progress(
            &progress_path,
            &valid_irreducible_progress(&format!(
                "[{}]",
                boundary_segment(
                    "2026-04-19T20:09:01Z",
                    "2026-04-19T20:09:02Z",
                    BOUNDARY_SUFFIX_REASON
                )
            ))
            .replace("2026-04-20T16:56:04Z", "2026-04-20T16:56:05Z"),
        );

        let error = build_report(&config(progress_path)).expect_err("window mismatch must fail");

        assert_eq!(
            error.reason,
            "program_history_irreducible_boundary_report_window_mismatch"
        );
        Ok(())
    }

    #[test]
    fn missing_required_control_fields_fail_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("missing-field.progress.json");
        write_progress(
            &progress_path,
            r#"{
  "reason": "program_history_gap_fill_repair_explicit_missing_segments_irreducible_boundary_evidence_remains",
  "current_phase": "completed_with_explicit_missing_segments",
  "replayable_output": false,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "missing_segments": []
}"#,
        );

        let error = build_report(&config(progress_path)).expect_err("missing verdict must fail");

        assert_eq!(error.reason, REQUIRED_FIELD_REASON);
        assert!(error.detail.contains("verdict"));
        Ok(())
    }

    #[test]
    fn malformed_timestamp_or_duration_fails_closed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let bad_ts_path = temp.path().join("bad-ts.progress.json");
        write_progress(
            &bad_ts_path,
            &valid_irreducible_progress(
                r#"[{"start":"not-a-ts","end":"2026-04-19T20:09:02Z","reason":"requested_window_suffix_uncovered_after_end_slot_adjustment"}]"#,
            ),
        );
        let bad_ts = build_report(&config(bad_ts_path)).expect_err("bad timestamp must fail");
        assert_eq!(bad_ts.reason, REQUIRED_FIELD_REASON);

        let bad_duration_path = temp.path().join("bad-duration.progress.json");
        write_progress(
            &bad_duration_path,
            &valid_irreducible_progress(
                r#"[{"start":"2026-04-19T20:09:03Z","end":"2026-04-19T20:09:02Z","reason":"requested_window_suffix_uncovered_after_end_slot_adjustment"}]"#,
            ),
        );
        let bad_duration =
            build_report(&config(bad_duration_path)).expect_err("bad duration must fail");
        assert_eq!(bad_duration.reason, REQUIRED_FIELD_REASON);
        assert!(bad_duration.detail.contains("duration"));
        Ok(())
    }

    #[test]
    fn duration_calculation_excludes_synthetic_full_window_reason_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("duration.progress.json");
        let boundary = boundary_segment(
            "2026-04-19T20:09:01Z",
            "2026-04-19T20:09:02Z",
            BOUNDARY_SUFFIX_REASON,
        );
        write_progress(
            &progress_path,
            &valid_irreducible_progress(&format!("[{boundary},{}]", synthetic_segment())),
        );

        let report = build_report(&config(progress_path)).expect("report should build");

        assert_eq!(report.total_irreducible_boundary_missing_ms, Some(1_000));
        assert_eq!(report.max_irreducible_boundary_segment_ms, Some(1_000));
        assert_eq!(report.non_target_segments_count, Some(1));
        assert_eq!(report.unknown_non_target_segments_count, Some(0));
        assert_eq!(report.non_target_evidence_segments.len(), 1);
        Ok(())
    }
}
