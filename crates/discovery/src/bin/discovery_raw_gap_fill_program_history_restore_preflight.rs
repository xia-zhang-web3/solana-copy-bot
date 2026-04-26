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
const ACCEPTED_RESIDUE_POLICY: &str = "accepted_irreducible_boundary_residue";
const IRREDUCIBLE_VERDICT: &str = "not_proven_due_to_irreducible_boundary_evidence";
const IRREDUCIBLE_REASON: &str =
    "program_history_gap_fill_repair_explicit_missing_segments_irreducible_boundary_evidence_remains";
const ZERO_PROGRESS_ROOT_REASON: &str =
    "program_history_gap_fill_skipped_persistently_provider_blocked_slot_after_bounded_retries";
const BOUNDARY_PREFIX_REASON: &str =
    "requested_window_prefix_uncovered_after_start_slot_adjustment";
const BOUNDARY_SUFFIX_REASON: &str = "requested_window_suffix_uncovered_after_end_slot_adjustment";
const MAX_ACCEPTED_RESIDUE_TOTAL_MS: i64 = 10_000;
const MAX_ACCEPTED_RESIDUE_SEGMENT_MS: i64 = 1_000;

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
    requested_window_start: Option<String>,
    requested_window_end: Option<String>,
    gap_fill_covered_since: Option<String>,
    covered_through_ts_utc: Option<String>,
    missing_segments_count: Option<usize>,
    fetched_rows: Option<usize>,
    staged_rows: Option<usize>,
    inserted_rows: Option<usize>,
    rows_withheld_due_to_incomplete_outcome: Option<usize>,
    accepted_residue_policy: Option<String>,
    accepted_irreducible_boundary_residue: bool,
    total_irreducible_boundary_missing_ms: Option<i64>,
    start_coverage_deficit_ms: Option<i64>,
    end_coverage_deficit_ms: Option<i64>,
    total_accepted_residue_ms: Option<i64>,
    max_accepted_residue_segment_or_deficit_ms: Option<i64>,
    max_irreducible_boundary_segment_ms: Option<i64>,
    target_boundary_segments_count: Option<usize>,
    zero_progress_root_segments_count: Option<usize>,
    unknown_non_target_segments_count: Option<usize>,
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
    reason: String,
    current_phase: String,
    replayable_output: bool,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    covered_since: Option<DateTime<Utc>>,
    covered_through: Option<DateTime<Utc>>,
    missing_segments: Vec<MissingSegment>,
    fetched_rows: Option<usize>,
    staged_rows: Option<usize>,
    inserted_rows: usize,
    rows_withheld_due_to_incomplete_outcome: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct MissingSegment {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    reason: String,
}

#[derive(Debug, Clone, Default)]
struct ResidueStats {
    target_boundary_segments_count: usize,
    zero_progress_root_segments_count: usize,
    unknown_non_target_segments_count: usize,
    total_boundary_missing_ms: i64,
    start_coverage_deficit_ms: Option<i64>,
    end_coverage_deficit_ms: Option<i64>,
    total_accepted_residue_ms: Option<i64>,
    max_accepted_residue_segment_or_deficit_ms: Option<i64>,
    max_boundary_segment_ms: i64,
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
            requested_window_start: None,
            requested_window_end: None,
            gap_fill_covered_since: None,
            covered_through_ts_utc: None,
            missing_segments_count: None,
            fetched_rows: None,
            staged_rows: None,
            inserted_rows: None,
            rows_withheld_due_to_incomplete_outcome: None,
            accepted_residue_policy: None,
            accepted_irreducible_boundary_residue: false,
            total_irreducible_boundary_missing_ms: None,
            start_coverage_deficit_ms: None,
            end_coverage_deficit_ms: None,
            total_accepted_residue_ms: None,
            max_accepted_residue_segment_or_deficit_ms: None,
            max_irreducible_boundary_segment_ms: None,
            target_boundary_segments_count: None,
            zero_progress_root_segments_count: None,
            unknown_non_target_segments_count: None,
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
    let residue_stats = residue_stats(
        &progress.missing_segments,
        progress.covered_since,
        progress.covered_through,
        progress.requested_window_start,
        progress.requested_window_end,
    );
    let readiness = restore_readiness(&progress, config.window_start_utc, config.window_end_utc);

    Ok(RestorePreflightReport {
        event: "discovery_raw_gap_fill_program_history_restore_preflight",
        progress_path: config.progress_path.display().to_string(),
        window_start_utc: format_ts(config.window_start_utc),
        window_end_utc: format_ts(config.window_end_utc),
        attempt_number: progress.attempt_number,
        verdict: Some(progress.verdict),
        reason: Some(progress.reason),
        current_phase: Some(progress.current_phase),
        replayable_output: Some(progress.replayable_output),
        requested_window_start: Some(format_ts(progress.requested_window_start)),
        requested_window_end: Some(format_ts(progress.requested_window_end)),
        gap_fill_covered_since: progress.covered_since.map(format_ts),
        covered_through_ts_utc: progress.covered_through.map(format_ts),
        missing_segments_count: Some(progress.missing_segments.len()),
        fetched_rows: progress.fetched_rows,
        staged_rows: progress.staged_rows,
        inserted_rows: Some(progress.inserted_rows),
        rows_withheld_due_to_incomplete_outcome: Some(
            progress.rows_withheld_due_to_incomplete_outcome,
        ),
        accepted_residue_policy: readiness.accepted_residue_policy.map(ToString::to_string),
        accepted_irreducible_boundary_residue: readiness.accepted_irreducible_boundary_residue,
        total_irreducible_boundary_missing_ms: Some(residue_stats.total_boundary_missing_ms),
        start_coverage_deficit_ms: residue_stats.start_coverage_deficit_ms,
        end_coverage_deficit_ms: residue_stats.end_coverage_deficit_ms,
        total_accepted_residue_ms: residue_stats.total_accepted_residue_ms,
        max_accepted_residue_segment_or_deficit_ms: residue_stats
            .max_accepted_residue_segment_or_deficit_ms,
        max_irreducible_boundary_segment_ms: Some(residue_stats.max_boundary_segment_ms),
        target_boundary_segments_count: Some(residue_stats.target_boundary_segments_count),
        zero_progress_root_segments_count: Some(residue_stats.zero_progress_root_segments_count),
        unknown_non_target_segments_count: Some(residue_stats.unknown_non_target_segments_count),
        restore_ready: readiness.restore_ready,
        restore_ready_reason: readiness.reason,
        operator_reason: None,
        operator_detail: None,
    })
}

#[derive(Debug, Clone)]
struct RestoreReadinessDecision {
    restore_ready: bool,
    reason: String,
    accepted_residue_policy: Option<&'static str>,
    accepted_irreducible_boundary_residue: bool,
}

fn restore_readiness(
    progress: &ProgressSnapshot,
    window_start_utc: DateTime<Utc>,
    window_end_utc: DateTime<Utc>,
) -> RestoreReadinessDecision {
    if progress.requested_window_start != window_start_utc {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_requested_window_start_mismatch",
        );
    }
    if progress.requested_window_end != window_end_utc {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_requested_window_end_mismatch",
        );
    }
    if progress.replayable_output {
        let Some(covered_through) = progress.covered_through else {
            return preflight_not_ready(
                "program_history_gap_fill_restore_preflight_not_ready_missing_covered_through",
            );
        };
        if covered_through < window_end_utc {
            return preflight_not_ready(
                "program_history_gap_fill_restore_preflight_not_ready_covered_through_before_window_end",
            );
        }
        if progress.inserted_rows == 0 {
            return preflight_not_ready(
                "program_history_gap_fill_restore_preflight_not_ready_inserted_rows_not_positive",
            );
        }
        if !progress.missing_segments.is_empty() {
            return preflight_not_ready(
                "program_history_gap_fill_restore_preflight_not_ready_missing_segments_present",
            );
        }
        if progress.rows_withheld_due_to_incomplete_outcome > 0 {
            return preflight_not_ready(
                "program_history_gap_fill_restore_preflight_not_ready_rows_withheld_due_to_incomplete_outcome",
            );
        }
        return RestoreReadinessDecision {
            restore_ready: true,
            reason: "program_history_gap_fill_restore_preflight_ready_explicit_replayable_complete_no_missing_segments"
                .to_string(),
            accepted_residue_policy: None,
            accepted_irreducible_boundary_residue: false,
        };
    }

    accepted_residue_readiness(progress)
}

fn preflight_not_ready(reason: &str) -> RestoreReadinessDecision {
    RestoreReadinessDecision {
        restore_ready: false,
        reason: reason.to_string(),
        accepted_residue_policy: None,
        accepted_irreducible_boundary_residue: false,
    }
}

fn accepted_residue_readiness(progress: &ProgressSnapshot) -> RestoreReadinessDecision {
    if progress.verdict != IRREDUCIBLE_VERDICT {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_replayable_output_false",
        );
    }
    if progress.reason != IRREDUCIBLE_REASON {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_reason_mismatch",
        );
    }
    if progress.current_phase != "completed_with_explicit_missing_segments" {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_current_phase_mismatch",
        );
    }
    if progress.inserted_rows > 0 {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_inserted_rows_present",
        );
    }
    let Some(staged_rows) = progress.staged_rows else {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_staged_rows_missing",
        );
    };
    let Some(fetched_rows) = progress.fetched_rows else {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_fetched_rows_missing",
        );
    };
    if staged_rows == 0 {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_staged_rows_not_positive",
        );
    }
    if fetched_rows != staged_rows {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_fetched_rows_mismatch",
        );
    }
    if progress.rows_withheld_due_to_incomplete_outcome != staged_rows {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_rows_withheld_mismatch",
        );
    }
    if progress.covered_since.is_none() {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_missing_covered_since",
        );
    }
    if progress.covered_through.is_none() {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_missing_covered_through",
        );
    }
    let stats = residue_stats(
        &progress.missing_segments,
        progress.covered_since,
        progress.covered_through,
        progress.requested_window_start,
        progress.requested_window_end,
    );
    if stats.zero_progress_root_segments_count > 0 {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_root_segments_present",
        );
    }
    if stats.unknown_non_target_segments_count > 0 {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_unknown_non_target_segments_present",
        );
    }
    if stats.target_boundary_segments_count == 0 {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_no_target_segments",
        );
    }
    if stats.start_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_start_deficit_too_large",
        );
    }
    if stats.end_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_end_deficit_too_large",
        );
    }
    if stats.total_accepted_residue_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_TOTAL_MS {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_total_duration_too_large",
        );
    }
    if stats
        .max_accepted_residue_segment_or_deficit_ms
        .unwrap_or(i64::MAX)
        > MAX_ACCEPTED_RESIDUE_SEGMENT_MS
    {
        return preflight_not_ready(
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_segment_duration_too_large",
        );
    }
    RestoreReadinessDecision {
        restore_ready: true,
        reason:
            "program_history_gap_fill_restore_preflight_ready_accepted_irreducible_boundary_residue"
                .to_string(),
        accepted_residue_policy: Some(ACCEPTED_RESIDUE_POLICY),
        accepted_irreducible_boundary_residue: true,
    }
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
    let reason = required_string(value, "reason")?;
    let current_phase = required_string(value, "current_phase")?;
    let replayable_output = required_bool(value, "replayable_output")?;
    let requested_window_start = required_ts(value, "requested_window_start")?;
    let requested_window_end = required_ts(value, "requested_window_end")?;
    let covered_through = value
        .get("gap_fill_covered_through_cursor")
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| control_field_error("gap_fill_covered_through_cursor.ts_utc", error))?;
    let missing_segments = required_missing_segments(value)?;
    let fetched_rows = optional_usize(value, "fetched_rows")?;
    let staged_rows = optional_usize(value, "staged_rows")?;
    let inserted_rows = required_usize(value, "inserted_rows")?;
    let rows_withheld_due_to_incomplete_outcome =
        required_usize(value, "rows_withheld_due_to_incomplete_outcome")?;
    let covered_since = optional_ts(value, "gap_fill_covered_since")?;

    Ok(ProgressSnapshot {
        attempt_number: optional_usize(value, "attempt_number")?,
        verdict,
        reason,
        current_phase,
        replayable_output,
        requested_window_start,
        requested_window_end,
        covered_since,
        covered_through,
        missing_segments,
        fetched_rows,
        staged_rows,
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome,
    })
}

fn required_missing_segments(value: &Value) -> Result<Vec<MissingSegment>, PreflightFailure> {
    let segments = value
        .get("missing_segments")
        .and_then(Value::as_array)
        .ok_or_else(|| control_field_error("missing_segments", "missing or non-array value"))?;
    segments
        .iter()
        .enumerate()
        .map(|(index, segment)| missing_segment_from_value(index, segment))
        .collect()
}

fn missing_segment_from_value(
    index: usize,
    value: &Value,
) -> Result<MissingSegment, PreflightFailure> {
    let start = required_segment_ts(value, index, "start")?;
    let end = required_segment_ts(value, index, "end")?;
    if end < start {
        return Err(control_field_error(
            "missing_segments.duration",
            format!("segment {index} end is before start"),
        ));
    }
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            control_field_error(
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
) -> Result<DateTime<Utc>, PreflightFailure> {
    let raw = value.get(field).and_then(Value::as_str).ok_or_else(|| {
        control_field_error(
            "missing_segments.timestamp",
            format!("segment {index} missing or non-string {field}"),
        )
    })?;
    parse_ts(raw).map_err(|error| {
        control_field_error(
            "missing_segments.timestamp",
            format!("segment {index} invalid {field}: {error}"),
        )
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

fn required_u64(value: &Value, field: &'static str) -> Result<u64, PreflightFailure> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| control_field_error(field, "missing or non-u64 value"))
}

fn required_usize(value: &Value, field: &'static str) -> Result<usize, PreflightFailure> {
    let raw = required_u64(value, field)?;
    usize::try_from(raw)
        .map_err(|error| control_field_error(field, format!("usize overflow: {error}")))
}

fn required_ts(value: &Value, field: &'static str) -> Result<DateTime<Utc>, PreflightFailure> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).map_err(|error| control_field_error(field, error))
}

fn optional_ts(
    value: &Value,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, PreflightFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| control_field_error(field, error))
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

fn residue_stats(
    missing_segments: &[MissingSegment],
    covered_since: Option<DateTime<Utc>>,
    covered_through: Option<DateTime<Utc>>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
) -> ResidueStats {
    let mut stats = ResidueStats::default();
    for segment in missing_segments {
        if is_boundary_reason(&segment.reason) {
            let duration_ms = segment_duration_ms(segment);
            stats.target_boundary_segments_count =
                stats.target_boundary_segments_count.saturating_add(1);
            stats.total_boundary_missing_ms =
                stats.total_boundary_missing_ms.saturating_add(duration_ms);
            stats.max_boundary_segment_ms = stats.max_boundary_segment_ms.max(duration_ms);
        } else if segment.reason == ZERO_PROGRESS_ROOT_REASON {
            stats.zero_progress_root_segments_count =
                stats.zero_progress_root_segments_count.saturating_add(1);
        } else if segment.reason != IRREDUCIBLE_REASON {
            stats.unknown_non_target_segments_count =
                stats.unknown_non_target_segments_count.saturating_add(1);
        }
    }
    let start_deficit = covered_since.map(|covered_since| {
        covered_since
            .signed_duration_since(requested_window_start)
            .num_milliseconds()
            .max(0)
    });
    let end_deficit = covered_through.map(|covered_through| {
        requested_window_end
            .signed_duration_since(covered_through)
            .num_milliseconds()
            .max(0)
    });
    stats.start_coverage_deficit_ms = start_deficit;
    stats.end_coverage_deficit_ms = end_deficit;
    if let (Some(start_deficit), Some(end_deficit)) = (start_deficit, end_deficit) {
        stats.total_accepted_residue_ms = Some(
            stats
                .total_boundary_missing_ms
                .saturating_add(start_deficit)
                .saturating_add(end_deficit),
        );
        stats.max_accepted_residue_segment_or_deficit_ms = Some(
            stats
                .max_boundary_segment_ms
                .max(start_deficit)
                .max(end_deficit),
        );
    }
    stats
}

fn is_boundary_reason(reason: &str) -> bool {
    reason == BOUNDARY_PREFIX_REASON || reason == BOUNDARY_SUFFIX_REASON
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
            "requested_window_start={}",
            report.requested_window_start.as_deref().unwrap_or("null")
        ),
        format!(
            "requested_window_end={}",
            report.requested_window_end.as_deref().unwrap_or("null")
        ),
        format!(
            "covered_through_ts_utc={}",
            report.covered_through_ts_utc.as_deref().unwrap_or("null")
        ),
        format!(
            "gap_fill_covered_since={}",
            report.gap_fill_covered_since.as_deref().unwrap_or("null")
        ),
        format!(
            "missing_segments_count={}",
            format_option(report.missing_segments_count)
        ),
        format!("fetched_rows={}", format_option(report.fetched_rows)),
        format!("staged_rows={}", format_option(report.staged_rows)),
        format!("inserted_rows={}", format_option(report.inserted_rows)),
        format!(
            "rows_withheld_due_to_incomplete_outcome={}",
            format_option(report.rows_withheld_due_to_incomplete_outcome)
        ),
        format!(
            "accepted_residue_policy={}",
            report.accepted_residue_policy.as_deref().unwrap_or("null")
        ),
        format!(
            "accepted_irreducible_boundary_residue={}",
            report.accepted_irreducible_boundary_residue
        ),
        format!(
            "total_irreducible_boundary_missing_ms={}",
            format_option(report.total_irreducible_boundary_missing_ms)
        ),
        format!(
            "start_coverage_deficit_ms={}",
            format_option(report.start_coverage_deficit_ms)
        ),
        format!(
            "end_coverage_deficit_ms={}",
            format_option(report.end_coverage_deficit_ms)
        ),
        format!(
            "total_accepted_residue_ms={}",
            format_option(report.total_accepted_residue_ms)
        ),
        format!(
            "max_accepted_residue_segment_or_deficit_ms={}",
            format_option(report.max_accepted_residue_segment_or_deficit_ms)
        ),
        format!(
            "max_irreducible_boundary_segment_ms={}",
            format_option(report.max_irreducible_boundary_segment_ms)
        ),
        format!(
            "target_boundary_segments_count={}",
            format_option(report.target_boundary_segments_count)
        ),
        format!(
            "zero_progress_root_segments_count={}",
            format_option(report.zero_progress_root_segments_count)
        ),
        format!(
            "unknown_non_target_segments_count={}",
            format_option(report.unknown_non_target_segments_count)
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
        progress_json_with_control(
            "complete_but_insufficient_for_healthy_restore",
            "program_history_gap_fill_completed_but_still_missing_required_raw_window",
            "complete",
            replayable_output,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:04Z",
            covered_through,
            missing_segments,
            10,
            0,
        )
    }

    fn progress_json_with_control(
        verdict: &str,
        reason: &str,
        current_phase: &str,
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
  "attempt_number": 9,
  "verdict": "{verdict}",
  "reason": "{reason}",
  "current_phase": "{current_phase}",
  "replayable_output": {replayable_output},
  "requested_window_start": "{requested_window_start}",
  "requested_window_end": "{requested_window_end}",
  "missing_segments": {missing_segments},
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }},
  "gap_fill_covered_since": "{requested_window_start}",
  "fetched_rows": {inserted_rows},
  "staged_rows": {inserted_rows},
  "inserted_rows": {inserted_rows},
  "rows_withheld_due_to_incomplete_outcome": {rows_withheld}
}}"#
        )
    }

    fn accepted_residue_progress_json(missing_segments: &str, rows_withheld: usize) -> String {
        accepted_residue_progress_json_with_fields(
            missing_segments,
            rows_withheld,
            rows_withheld,
            0,
            rows_withheld,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:03.143Z",
        )
    }

    fn accepted_residue_progress_json_with_fields(
        missing_segments: &str,
        staged_rows: usize,
        fetched_rows: usize,
        inserted_rows: usize,
        rows_withheld: usize,
        covered_since: &str,
        covered_through: &str,
    ) -> String {
        format!(
            r#"{{
  "attempt_number": 9,
  "verdict": "{IRREDUCIBLE_VERDICT}",
  "reason": "{IRREDUCIBLE_REASON}",
  "current_phase": "completed_with_explicit_missing_segments",
  "replayable_output": false,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "missing_segments": {missing_segments},
  "gap_fill_covered_since": "{covered_since}",
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }},
  "fetched_rows": {fetched_rows},
  "staged_rows": {staged_rows},
  "inserted_rows": {inserted_rows},
  "rows_withheld_due_to_incomplete_outcome": {rows_withheld}
}}"#
        )
    }

    fn live_shaped_boundary_segments() -> String {
        format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:04.600Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-20T16:56:03Z", "end": "2026-04-20T16:56:04Z", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-20T16:56:04Z", "reason": "{IRREDUCIBLE_REASON}" }}
]"#
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
        assert!(!report.accepted_irreducible_boundary_residue);
        assert_eq!(report.accepted_residue_policy, None);
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
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "missing_segments": [],
  "inserted_rows": 10,
  "rows_withheld_due_to_incomplete_outcome": 0
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
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "replayable_output": true,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "missing_segments": [],
  "inserted_rows": 10,
  "rows_withheld_due_to_incomplete_outcome": 0
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
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "replayable_output": true,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "missing_segments": [],
  "inserted_rows": 10,
  "rows_withheld_due_to_incomplete_outcome": 0
}"#,
        );
        let error = build_preflight_report(&config(missing_current_phase))
            .expect_err("missing current_phase must fail");
        assert_eq!(error.reason, CONTROL_FIELD_MISSING_REASON);
        assert!(error.detail.contains("current_phase"));
        Ok(())
    }

    #[test]
    fn accepted_irreducible_boundary_residue_is_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("accepted-residue.progress.json");
        write_progress(
            &progress_path,
            &accepted_residue_progress_json(&live_shaped_boundary_segments(), 5),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("accepted residue progress should be reportable");

        assert!(report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_ready_accepted_irreducible_boundary_residue"
        );
        assert_eq!(
            report.accepted_residue_policy.as_deref(),
            Some(ACCEPTED_RESIDUE_POLICY)
        );
        assert!(report.accepted_irreducible_boundary_residue);
        assert_eq!(report.target_boundary_segments_count, Some(2));
        assert_eq!(report.total_irreducible_boundary_missing_ms, Some(1_600));
        assert_eq!(report.start_coverage_deficit_ms, Some(0));
        assert_eq!(report.end_coverage_deficit_ms, Some(857));
        assert_eq!(report.total_accepted_residue_ms, Some(2_457));
        assert_eq!(
            report.max_accepted_residue_segment_or_deficit_ms,
            Some(1_000)
        );
        assert_eq!(report.max_irreducible_boundary_segment_ms, Some(1_000));
        assert_eq!(report.zero_progress_root_segments_count, Some(0));
        assert_eq!(report.unknown_non_target_segments_count, Some(0));
        assert_eq!(report.fetched_rows, Some(5));
        assert_eq!(report.staged_rows, Some(5));
        assert_eq!(report.inserted_rows, Some(0));
        assert_eq!(report.rows_withheld_due_to_incomplete_outcome, Some(5));
        Ok(())
    }

    #[test]
    fn accepted_residue_root_segment_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("root-residue.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:04.600Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "{ZERO_PROGRESS_ROOT_REASON}" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_progress_json(&segments, 7),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("root residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_root_segments_present"
        );
        assert_eq!(report.zero_progress_root_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn accepted_residue_unknown_non_target_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("unknown-residue.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:04.600Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "unexpected_boundary_gap" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_progress_json(&segments, 7),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("unknown residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_unknown_non_target_segments_present"
        );
        assert_eq!(report.unknown_non_target_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn accepted_residue_total_duration_over_policy_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("large-total-residue.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:05Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:05Z", "end": "2026-04-18T16:56:06Z", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:06Z", "end": "2026-04-18T16:56:07Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:07Z", "end": "2026-04-18T16:56:08Z", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:08Z", "end": "2026-04-18T16:56:09Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:09Z", "end": "2026-04-18T16:56:10Z", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:10Z", "end": "2026-04-18T16:56:11Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:11Z", "end": "2026-04-18T16:56:12Z", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:12Z", "end": "2026-04-18T16:56:13Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:13Z", "end": "2026-04-18T16:56:14Z", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "2026-04-18T16:56:14Z", "end": "2026-04-18T16:56:15Z", "reason": "{BOUNDARY_PREFIX_REASON}" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_progress_json(&segments, 7),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("large total residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_total_duration_too_large"
        );
        assert_eq!(report.total_irreducible_boundary_missing_ms, Some(11_000));
        assert_eq!(report.total_accepted_residue_ms, Some(11_857));
        Ok(())
    }

    #[test]
    fn accepted_residue_max_segment_over_policy_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("large-segment-residue.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:05.001Z", "reason": "{BOUNDARY_PREFIX_REASON}" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_progress_json(&segments, 7),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("large segment residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_segment_duration_too_large"
        );
        assert_eq!(report.max_irreducible_boundary_segment_ms, Some(1_001));
        Ok(())
    }

    #[test]
    fn accepted_residue_staged_rows_zero_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("zero-staged-residue.progress.json");
        write_progress(
            &progress_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                0,
                0,
                0,
                0,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("zero staged residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_staged_rows_not_positive"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_missing_staged_or_fetched_rows_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let missing_staged_path = temp.path().join("missing-staged-residue.progress.json");
        write_progress(
            &missing_staged_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            )
            .replace(
                r#"  "staged_rows": 7,
"#,
                "",
            ),
        );
        let report = build_preflight_report(&config(missing_staged_path))
            .expect("missing staged residue progress should be reportable");
        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_staged_rows_missing"
        );

        let missing_fetched_path = temp.path().join("missing-fetched-residue.progress.json");
        write_progress(
            &missing_fetched_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            )
            .replace(
                r#"  "fetched_rows": 7,
"#,
                "",
            ),
        );
        let report = build_preflight_report(&config(missing_fetched_path))
            .expect("missing fetched residue progress should be reportable");
        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_fetched_rows_missing"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_row_accounting_mismatches_block_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let fetched_mismatch_path = temp.path().join("fetched-mismatch-residue.progress.json");
        write_progress(
            &fetched_mismatch_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                6,
                0,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );
        let report = build_preflight_report(&config(fetched_mismatch_path))
            .expect("fetched mismatch residue progress should be reportable");
        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_fetched_rows_mismatch"
        );

        let withheld_mismatch_path = temp.path().join("withheld-mismatch-residue.progress.json");
        write_progress(
            &withheld_mismatch_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                6,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );
        let report = build_preflight_report(&config(withheld_mismatch_path))
            .expect("withheld mismatch residue progress should be reportable");
        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_rows_withheld_mismatch"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_inserted_rows_present_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("inserted-residue.progress.json");
        write_progress(
            &progress_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                1,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("inserted residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_inserted_rows_present"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_coverage_deficits_over_policy_block_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let start_deficit_path = temp.path().join("start-deficit-residue.progress.json");
        write_progress(
            &start_deficit_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                7,
                "2026-04-18T16:56:05.001Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );
        let report = build_preflight_report(&config(start_deficit_path))
            .expect("start deficit residue progress should be reportable");
        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_start_deficit_too_large"
        );
        assert_eq!(report.start_coverage_deficit_ms, Some(1_001));

        let end_deficit_path = temp.path().join("end-deficit-residue.progress.json");
        write_progress(
            &end_deficit_path,
            &accepted_residue_progress_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:02.999Z",
            ),
        );
        let report = build_preflight_report(&config(end_deficit_path))
            .expect("end deficit residue progress should be reportable");
        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_end_deficit_too_large"
        );
        assert_eq!(report.end_coverage_deficit_ms, Some(1_001));
        Ok(())
    }

    #[test]
    fn accepted_residue_wrong_control_fields_block_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("wrong-control-residue.progress.json");
        write_progress(
            &progress_path,
            &progress_json_with_control(
                IRREDUCIBLE_VERDICT,
                IRREDUCIBLE_REASON,
                "awaiting_next_attempt",
                false,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &live_shaped_boundary_segments(),
                10,
                0,
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("wrong control residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_current_phase_mismatch"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_wrong_reason_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("wrong-reason-residue.progress.json");
        write_progress(
            &progress_path,
            &progress_json_with_control(
                IRREDUCIBLE_VERDICT,
                "program_history_gap_fill_incomplete_due_to_persistently_blocked_slot_gap",
                "completed_with_explicit_missing_segments",
                false,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &live_shaped_boundary_segments(),
                10,
                0,
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("wrong reason residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_irreducible_boundary_reason_mismatch"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_window_mismatch_blocks_restore_ready_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("window-mismatch-residue.progress.json");
        write_progress(
            &progress_path,
            &progress_json_with_control(
                IRREDUCIBLE_VERDICT,
                IRREDUCIBLE_REASON,
                "completed_with_explicit_missing_segments",
                false,
                "2026-04-18T16:56:05Z",
                "2026-04-20T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &live_shaped_boundary_segments(),
                10,
                0,
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("window mismatch residue progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_requested_window_start_mismatch"
        );
        Ok(())
    }

    #[test]
    fn ordinary_non_replayable_provider_throttling_still_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("ordinary-non-replayable.progress.json");
        write_progress(
            &progress_path,
            &progress_json_with_control(
                "not_proven_due_to_provider_throttling",
                "program_history_gap_fill_incomplete_due_to_persistently_blocked_slot_gap",
                "completed_with_explicit_missing_segments",
                false,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:04Z",
                "2026-04-20T16:56:04Z",
                &live_shaped_boundary_segments(),
                10,
                0,
            ),
        );

        let report = build_preflight_report(&config(progress_path))
            .expect("ordinary non-replayable progress should be reportable");

        assert!(!report.restore_ready);
        assert_eq!(
            report.restore_ready_reason,
            "program_history_gap_fill_restore_preflight_not_ready_replayable_output_false"
        );
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
