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
    gap_fill_covered_since: Option<String>,
    covered_through_ts_utc: Option<String>,
    missing_segments_count: Option<usize>,
    fetched_rows: Option<usize>,
    staged_rows: Option<usize>,
    inserted_rows: Option<usize>,
    rows_withheld_due_to_incomplete_outcome: Option<usize>,
    artifact_valid_for_restore_review: bool,
    artifact_validation_reason: String,
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
            gap_fill_covered_since: None,
            covered_through_ts_utc: None,
            missing_segments_count: None,
            fetched_rows: None,
            staged_rows: None,
            inserted_rows: None,
            rows_withheld_due_to_incomplete_outcome: None,
            artifact_valid_for_restore_review: false,
            artifact_validation_reason: reason.clone(),
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
    let residue_stats = residue_stats(
        &artifact.missing_segments,
        artifact.covered_since,
        artifact.covered_through,
        artifact.requested_window_start,
        artifact.requested_window_end,
    );
    let validation =
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
        gap_fill_covered_since: artifact.covered_since.map(format_ts),
        covered_through_ts_utc: artifact.covered_through.map(format_ts),
        missing_segments_count: Some(artifact.missing_segments.len()),
        fetched_rows: artifact.fetched_rows,
        staged_rows: artifact.staged_rows,
        inserted_rows: Some(artifact.inserted_rows),
        rows_withheld_due_to_incomplete_outcome: Some(
            artifact.rows_withheld_due_to_incomplete_outcome,
        ),
        artifact_valid_for_restore_review: validation.valid_for_restore_review,
        artifact_validation_reason: validation.reason,
        accepted_residue_policy: validation.accepted_residue_policy.map(ToString::to_string),
        accepted_irreducible_boundary_residue: validation.accepted_irreducible_boundary_residue,
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
        operator_reason: None,
        operator_detail: None,
    })
}

#[derive(Debug, Clone)]
struct ArtifactValidationDecision {
    valid_for_restore_review: bool,
    reason: String,
    accepted_residue_policy: Option<&'static str>,
    accepted_irreducible_boundary_residue: bool,
}

fn artifact_review_validity(
    artifact: &ArtifactSnapshot,
    window_start_utc: DateTime<Utc>,
    window_end_utc: DateTime<Utc>,
) -> ArtifactValidationDecision {
    if artifact.requested_window_start != window_start_utc {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_requested_window_start_mismatch",
        );
    }
    if artifact.requested_window_end != window_end_utc {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_requested_window_end_mismatch",
        );
    }
    if artifact.replayable_output {
        let Some(covered_through) = artifact.covered_through else {
            return artifact_not_ready(
                "program_history_gap_fill_artifact_validate_not_ready_missing_covered_through",
            );
        };
        if covered_through < window_end_utc {
            return artifact_not_ready(
                "program_history_gap_fill_artifact_validate_not_ready_covered_through_before_window_end",
            );
        }
        if artifact.inserted_rows == 0 {
            return artifact_not_ready(
                "program_history_gap_fill_artifact_validate_not_ready_inserted_rows_not_positive",
            );
        }
        if !artifact.missing_segments.is_empty() {
            return artifact_not_ready(
                "program_history_gap_fill_artifact_validate_not_ready_missing_segments_present",
            );
        }
        if artifact.rows_withheld_due_to_incomplete_outcome > 0 {
            return artifact_not_ready(
                "program_history_gap_fill_artifact_validate_not_ready_rows_withheld_due_to_incomplete_outcome",
            );
        }
        return ArtifactValidationDecision {
            valid_for_restore_review: true,
            reason: "program_history_gap_fill_artifact_validate_ready_explicit_replayable_exact_window_complete_no_missing_or_withheld_rows"
                .to_string(),
            accepted_residue_policy: None,
            accepted_irreducible_boundary_residue: false,
        };
    }

    accepted_residue_validity(artifact)
}

fn artifact_not_ready(reason: &str) -> ArtifactValidationDecision {
    ArtifactValidationDecision {
        valid_for_restore_review: false,
        reason: reason.to_string(),
        accepted_residue_policy: None,
        accepted_irreducible_boundary_residue: false,
    }
}

fn accepted_residue_validity(artifact: &ArtifactSnapshot) -> ArtifactValidationDecision {
    if artifact.verdict != IRREDUCIBLE_VERDICT {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_replayable_output_false",
        );
    }
    if artifact.reason != IRREDUCIBLE_REASON {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_reason_mismatch",
        );
    }
    if artifact.current_phase != "completed_with_explicit_missing_segments" {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_current_phase_mismatch",
        );
    }
    if artifact.inserted_rows > 0 {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_inserted_rows_present",
        );
    }
    let Some(staged_rows) = artifact.staged_rows else {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_staged_rows_missing",
        );
    };
    let Some(fetched_rows) = artifact.fetched_rows else {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_fetched_rows_missing",
        );
    };
    if staged_rows == 0 {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_staged_rows_not_positive",
        );
    }
    if fetched_rows != staged_rows {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_fetched_rows_mismatch",
        );
    }
    if artifact.rows_withheld_due_to_incomplete_outcome != staged_rows {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_rows_withheld_mismatch",
        );
    }
    if artifact.covered_since.is_none() {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_missing_covered_since",
        );
    }
    if artifact.covered_through.is_none() {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_missing_covered_through",
        );
    }
    let stats = residue_stats(
        &artifact.missing_segments,
        artifact.covered_since,
        artifact.covered_through,
        artifact.requested_window_start,
        artifact.requested_window_end,
    );
    if stats.zero_progress_root_segments_count > 0 {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_root_segments_present",
        );
    }
    if stats.unknown_non_target_segments_count > 0 {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_unknown_non_target_segments_present",
        );
    }
    if stats.target_boundary_segments_count == 0 {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_no_target_segments",
        );
    }
    if stats.start_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_start_deficit_too_large",
        );
    }
    if stats.end_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_end_deficit_too_large",
        );
    }
    if stats.total_accepted_residue_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_TOTAL_MS {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_total_duration_too_large",
        );
    }
    if stats
        .max_accepted_residue_segment_or_deficit_ms
        .unwrap_or(i64::MAX)
        > MAX_ACCEPTED_RESIDUE_SEGMENT_MS
    {
        return artifact_not_ready(
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_segment_duration_too_large",
        );
    }
    ArtifactValidationDecision {
        valid_for_restore_review: true,
        reason:
            "program_history_gap_fill_artifact_validate_ready_accepted_irreducible_boundary_residue"
                .to_string(),
        accepted_residue_policy: Some(ACCEPTED_RESIDUE_POLICY),
        accepted_irreducible_boundary_residue: true,
    }
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
    let missing_segments = required_missing_segments(value)?;
    let fetched_rows = optional_usize(value, "fetched_rows")?;
    let staged_rows = optional_usize(value, "staged_rows")?;
    let inserted_rows = required_usize(value, "inserted_rows")?;
    let rows_withheld_due_to_incomplete_outcome =
        required_usize(value, "rows_withheld_due_to_incomplete_outcome")?;
    let covered_since = optional_ts(value, "gap_fill_covered_since")?;
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
        covered_since,
        covered_through,
        missing_segments,
        fetched_rows,
        staged_rows,
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome,
    })
}

fn required_missing_segments(
    value: &Value,
) -> Result<Vec<MissingSegment>, ArtifactValidationFailure> {
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
) -> Result<MissingSegment, ArtifactValidationFailure> {
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
) -> Result<DateTime<Utc>, ArtifactValidationFailure> {
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

fn optional_usize(
    value: &Value,
    field: &'static str,
) -> Result<Option<usize>, ArtifactValidationFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let raw = value
                .as_u64()
                .ok_or_else(|| required_field_error(field, "non-u64 value"))?;
            usize::try_from(raw)
                .map(Some)
                .map_err(|error| required_field_error(field, format!("usize overflow: {error}")))
        }
    }
}

fn required_ts(
    value: &Value,
    field: &'static str,
) -> Result<DateTime<Utc>, ArtifactValidationFailure> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).map_err(|error| required_field_error(field, error))
}

fn optional_ts(
    value: &Value,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, ArtifactValidationFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| required_field_error(field, error))
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
            "artifact_valid_for_restore_review={}",
            report.artifact_valid_for_restore_review
        ),
        format!(
            "artifact_validation_reason={}",
            report.artifact_validation_reason
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
        artifact_json_with_control(
            "complete_but_insufficient_for_healthy_restore",
            "program_history_gap_fill_completed_but_still_missing_required_raw_window",
            "complete",
            replayable_output,
            requested_window_start,
            requested_window_end,
            covered_through,
            missing_segments,
            inserted_rows,
            rows_withheld,
        )
    }

    fn artifact_json_with_control(
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
  "verdict": "{verdict}",
  "reason": "{reason}",
  "current_phase": "{current_phase}",
  "replayable_output": {replayable_output},
  "requested_window_start": "{requested_window_start}",
  "requested_window_end": "{requested_window_end}",
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }},
  "gap_fill_covered_since": "{requested_window_start}",
  "missing_segments": {missing_segments},
  "fetched_rows": {inserted_rows},
  "staged_rows": {inserted_rows},
  "inserted_rows": {inserted_rows},
  "rows_withheld_due_to_incomplete_outcome": {rows_withheld}
}}"#
        )
    }

    fn accepted_residue_artifact_json(missing_segments: &str, rows_withheld: usize) -> String {
        accepted_residue_artifact_json_with_fields(
            missing_segments,
            rows_withheld,
            rows_withheld,
            0,
            rows_withheld,
            "2026-04-18T16:56:04Z",
            "2026-04-20T16:56:03.143Z",
        )
    }

    fn accepted_residue_artifact_json_with_fields(
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
  "verdict": "{IRREDUCIBLE_VERDICT}",
  "reason": "{IRREDUCIBLE_REASON}",
  "current_phase": "completed_with_explicit_missing_segments",
  "replayable_output": false,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-20T16:56:04Z",
  "gap_fill_covered_since": "{covered_since}",
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{covered_through}",
    "slot": 123,
    "signature": "sig"
  }},
  "missing_segments": {missing_segments},
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
        assert!(!report.accepted_irreducible_boundary_residue);
        assert_eq!(report.accepted_residue_policy, None);
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
    fn live_shaped_accepted_irreducible_boundary_residue_is_valid_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            &accepted_residue_artifact_json(&live_shaped_boundary_segments(), 7),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("accepted residue artifact should report");

        assert!(report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_ready_accepted_irreducible_boundary_residue"
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
        assert_eq!(report.fetched_rows, Some(7));
        assert_eq!(report.staged_rows, Some(7));
        assert_eq!(report.inserted_rows, Some(0));
        assert_eq!(report.rows_withheld_due_to_incomplete_outcome, Some(7));
        Ok(())
    }

    #[test]
    fn root_segment_blocks_accepted_irreducible_boundary_residue_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:04.600Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "{ZERO_PROGRESS_ROOT_REASON}" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_artifact_json(&segments, 7),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("root segment artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_root_segments_present"
        );
        assert_eq!(report.zero_progress_root_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn unknown_non_target_segment_blocks_accepted_irreducible_boundary_residue_stage1() -> Result<()>
    {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:04.600Z", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "unexpected_boundary_gap" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_artifact_json(&segments, 7),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("unknown segment artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_unknown_non_target_segments_present"
        );
        assert_eq!(report.unknown_non_target_segments_count, Some(1));
        Ok(())
    }

    #[test]
    fn total_boundary_duration_over_policy_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
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
            &accepted_residue_artifact_json(&segments, 7),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("oversized total residue artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_total_duration_too_large"
        );
        assert_eq!(report.total_irreducible_boundary_missing_ms, Some(11_000));
        assert_eq!(report.total_accepted_residue_ms, Some(11_857));
        Ok(())
    }

    #[test]
    fn max_boundary_segment_duration_over_policy_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        let segments = format!(
            r#"[
  {{ "start": "2026-04-18T16:56:04Z", "end": "2026-04-18T16:56:05.001Z", "reason": "{BOUNDARY_PREFIX_REASON}" }}
]"#
        );
        write_progress(
            &progress_path,
            &accepted_residue_artifact_json(&segments, 7),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("oversized segment residue artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_segment_duration_too_large"
        );
        assert_eq!(report.max_irreducible_boundary_segment_ms, Some(1_001));
        Ok(())
    }

    #[test]
    fn accepted_residue_staged_rows_zero_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            &accepted_residue_artifact_json_with_fields(
                &live_shaped_boundary_segments(),
                0,
                0,
                0,
                0,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("zero staged residue artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_staged_rows_not_positive"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_missing_staged_or_fetched_rows_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let missing_staged_path = temp.path().join("missing-staged.progress.json");
        write_progress(
            &missing_staged_path,
            &accepted_residue_artifact_json_with_fields(
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
        let report = build_artifact_validation_report(&config(missing_staged_path))
            .expect("missing staged residue artifact should report");
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_staged_rows_missing"
        );

        let missing_fetched_path = temp.path().join("missing-fetched.progress.json");
        write_progress(
            &missing_fetched_path,
            &accepted_residue_artifact_json_with_fields(
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
        let report = build_artifact_validation_report(&config(missing_fetched_path))
            .expect("missing fetched residue artifact should report");
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_fetched_rows_missing"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_row_accounting_mismatches_block_stage1() -> Result<()> {
        let temp = tempdir()?;
        let fetched_mismatch_path = temp.path().join("fetched-mismatch.progress.json");
        write_progress(
            &fetched_mismatch_path,
            &accepted_residue_artifact_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                6,
                0,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );
        let report = build_artifact_validation_report(&config(fetched_mismatch_path))
            .expect("fetched mismatch residue artifact should report");
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_fetched_rows_mismatch"
        );

        let withheld_mismatch_path = temp.path().join("withheld-mismatch.progress.json");
        write_progress(
            &withheld_mismatch_path,
            &accepted_residue_artifact_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                6,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );
        let report = build_artifact_validation_report(&config(withheld_mismatch_path))
            .expect("withheld mismatch residue artifact should report");
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_rows_withheld_mismatch"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_inserted_rows_present_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            &accepted_residue_artifact_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                1,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("inserted residue artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_inserted_rows_present"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_coverage_deficits_over_policy_block_stage1() -> Result<()> {
        let temp = tempdir()?;
        let start_deficit_path = temp.path().join("start-deficit.progress.json");
        write_progress(
            &start_deficit_path,
            &accepted_residue_artifact_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                7,
                "2026-04-18T16:56:05.001Z",
                "2026-04-20T16:56:03.143Z",
            ),
        );
        let report = build_artifact_validation_report(&config(start_deficit_path))
            .expect("start deficit residue artifact should report");
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_start_deficit_too_large"
        );
        assert_eq!(report.start_coverage_deficit_ms, Some(1_001));

        let end_deficit_path = temp.path().join("end-deficit.progress.json");
        write_progress(
            &end_deficit_path,
            &accepted_residue_artifact_json_with_fields(
                &live_shaped_boundary_segments(),
                7,
                7,
                0,
                7,
                "2026-04-18T16:56:04Z",
                "2026-04-20T16:56:02.999Z",
            ),
        );
        let report = build_artifact_validation_report(&config(end_deficit_path))
            .expect("end deficit residue artifact should report");
        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_end_deficit_too_large"
        );
        assert_eq!(report.end_coverage_deficit_ms, Some(1_001));
        Ok(())
    }

    #[test]
    fn wrong_irreducible_control_fields_block_accepted_residue_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            &artifact_json_with_control(
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

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("wrong phase residue artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_current_phase_mismatch"
        );
        Ok(())
    }

    #[test]
    fn irreducible_verdict_with_wrong_reason_blocks_accepted_residue_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            &artifact_json_with_control(
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

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("wrong reason residue artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_irreducible_boundary_reason_mismatch"
        );
        Ok(())
    }

    #[test]
    fn ordinary_non_replayable_provider_throttling_still_blocks_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("artifact.progress.json");
        write_progress(
            &progress_path,
            &artifact_json_with_control(
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

        let report = build_artifact_validation_report(&config(progress_path))
            .expect("ordinary non-replayable artifact should report");

        assert!(!report.artifact_valid_for_restore_review);
        assert_eq!(
            report.artifact_validation_reason,
            "program_history_gap_fill_artifact_validate_not_ready_replayable_output_false"
        );
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
