use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
#[cfg(test)]
use copybot_core_types::SwapEvent;
use copybot_discovery::operator_status::DiscoveryOperatorStatus;
use copybot_discovery::restore_verdict::{
    DiscoveryRuntimeArtifactFreshnessAssessment, DiscoveryRuntimeRestoreVerdict,
};
use copybot_discovery::DiscoveryService;
#[cfg(test)]
use copybot_storage::{DiscoveryPublicationStateUpdate, WalletMetricRow, WalletUpsertRow};
use copybot_storage::{
    DiscoveryRecentRawRestoreStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    RecentRawJournalReplaySummary, SqliteStore,
};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::path::{Path, PathBuf};
#[cfg(test)]
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_runtime_restore --config <path> --artifact <path> [--db-path <path>] [--journal-db-path <path>] [--gap-fill-db-path <path> --gap-fill-progress-path <path> --gap-fill-window-start-utc <rfc3339> --gap-fill-window-end-utc <rfc3339>] [--bootstrap-degraded] [--json] [--now <rfc3339>]";
const GAP_FILL_ACCEPTANCE_FULL_REPLAYABLE: &str = "full_replayable_coverage";
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

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("{}", run(config)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    artifact_path: PathBuf,
    db_path: Option<PathBuf>,
    journal_db_path: Option<PathBuf>,
    gap_fill_db_path: Option<PathBuf>,
    gap_fill_progress_path: Option<PathBuf>,
    gap_fill_window_start_utc: Option<DateTime<Utc>>,
    gap_fill_window_end_utc: Option<DateTime<Utc>>,
    bootstrap_degraded: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct RestoreOutput {
    db_path: String,
    journal_db_path: String,
    gap_fill_db_path: Option<String>,
    gap_fill_acceptance_policy: Option<String>,
    gap_fill_accepted_irreducible_boundary_residue: bool,
    gap_fill_acceptance_fetched_rows: Option<u64>,
    gap_fill_acceptance_staged_rows: Option<u64>,
    gap_fill_acceptance_rows_withheld_due_to_incomplete_outcome: Option<u64>,
    gap_fill_acceptance_start_coverage_deficit_ms: Option<i64>,
    gap_fill_acceptance_end_coverage_deficit_ms: Option<i64>,
    gap_fill_acceptance_total_accepted_residue_ms: Option<i64>,
    gap_fill_acceptance_max_segment_or_deficit_ms: Option<i64>,
    freshness: DiscoveryRuntimeArtifactFreshnessAssessment,
    replay: RestoreReplaySummary,
    verdict: DiscoveryRuntimeRestoreVerdict,
    operator_status: DiscoveryOperatorStatus,
}

#[derive(Debug, Clone, Serialize)]
struct RestoreReplaySummary {
    required_window_start: DateTime<Utc>,
    artifact_runtime_cursor: DiscoveryRuntimeCursor,
    journal_available: bool,
    journal_covered_since: Option<DateTime<Utc>>,
    journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    journal_covers_artifact_cursor: bool,
    gap_fill_replayed: bool,
    gap_fill_covered_since: Option<DateTime<Utc>>,
    gap_fill_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    effective_covered_since: Option<DateTime<Utc>>,
    effective_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    gap_fill_replayed_rows: usize,
    replayed_rows: usize,
    raw_coverage_satisfied: bool,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut artifact_path: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut journal_db_path: Option<PathBuf> = None;
    let mut gap_fill_db_path: Option<PathBuf> = None;
    let mut gap_fill_progress_path: Option<PathBuf> = None;
    let mut gap_fill_window_start_utc: Option<DateTime<Utc>> = None;
    let mut gap_fill_window_end_utc: Option<DateTime<Utc>> = None;
    let mut bootstrap_degraded = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--artifact" => {
                artifact_path = Some(PathBuf::from(parse_string_arg("--artifact", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--journal-db-path" => {
                journal_db_path = Some(PathBuf::from(parse_string_arg(
                    "--journal-db-path",
                    args.next(),
                )?))
            }
            "--gap-fill-db-path" => {
                gap_fill_db_path = Some(PathBuf::from(parse_string_arg(
                    "--gap-fill-db-path",
                    args.next(),
                )?))
            }
            "--gap-fill-progress-path" => {
                gap_fill_progress_path = Some(PathBuf::from(parse_string_arg(
                    "--gap-fill-progress-path",
                    args.next(),
                )?))
            }
            "--gap-fill-window-start-utc" => {
                gap_fill_window_start_utc =
                    Some(parse_ts_arg("--gap-fill-window-start-utc", args.next())?)
            }
            "--gap-fill-window-end-utc" => {
                gap_fill_window_end_utc =
                    Some(parse_ts_arg("--gap-fill-window-end-utc", args.next())?)
            }
            "--bootstrap-degraded" => bootstrap_degraded = true,
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if gap_fill_db_path.is_some() {
        if gap_fill_progress_path.is_none() {
            bail!(
                "program_history_gap_fill_restore_gate_missing_progress_path: --gap-fill-progress-path is required when --gap-fill-db-path is supplied"
            );
        }
        if gap_fill_window_start_utc.is_none() || gap_fill_window_end_utc.is_none() {
            bail!(
                "program_history_gap_fill_restore_gate_missing_window_args: --gap-fill-window-start-utc and --gap-fill-window-end-utc are required when --gap-fill-db-path is supplied"
            );
        }
        if gap_fill_window_end_utc <= gap_fill_window_start_utc {
            bail!(
                "program_history_gap_fill_restore_gate_invalid_window: --gap-fill-window-end-utc must be after --gap-fill-window-start-utc"
            );
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        artifact_path: artifact_path.ok_or_else(|| anyhow!("missing required --artifact"))?,
        db_path,
        journal_db_path,
        gap_fill_db_path,
        gap_fill_progress_path,
        gap_fill_window_start_utc,
        gap_fill_window_end_utc,
        bootstrap_degraded,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    parse_ts(&raw).with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    let configured_db_path = PathBuf::from(configured_db_path);
    if configured_db_path.is_absolute() {
        return configured_db_path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_db_path)
}

fn resolve_migrations_dir(config_path: &Path, configured_migrations_dir: &str) -> PathBuf {
    let configured = PathBuf::from(configured_migrations_dir);
    if configured.is_absolute() || configured.exists() {
        return configured;
    }
    if let Some(config_parent) = config_path.parent() {
        let sibling_candidate = config_parent.join(&configured);
        if sibling_candidate.exists() {
            return sibling_candidate;
        }
        if let Some(project_root) = config_parent.parent() {
            let root_candidate = project_root.join(&configured);
            if root_candidate.exists() {
                return root_candidate;
            }
        }
    }
    let repo_fallback = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    if repo_fallback.exists() {
        return repo_fallback;
    }
    configured
}

fn validate_program_history_gap_fill_restore_gate(
    gap_fill_db_path: Option<&Path>,
    gap_fill_progress_path: Option<&Path>,
    gap_fill_window_start_utc: Option<&DateTime<Utc>>,
    gap_fill_window_end_utc: Option<&DateTime<Utc>>,
) -> Result<Option<GapFillRestoreGateAcceptance>> {
    let Some(gap_fill_db_path) = gap_fill_db_path else {
        return Ok(None);
    };
    let gap_fill_progress_path = gap_fill_progress_path.ok_or_else(|| {
        anyhow!(
            "program_history_gap_fill_restore_gate_missing_progress_path: --gap-fill-progress-path is required when --gap-fill-db-path is supplied"
        )
    })?;
    let (Some(gap_fill_window_start_utc), Some(gap_fill_window_end_utc)) = (
        gap_fill_window_start_utc.cloned(),
        gap_fill_window_end_utc.cloned(),
    ) else {
        bail!(
            "program_history_gap_fill_restore_gate_missing_window_args: --gap-fill-window-start-utc and --gap-fill-window-end-utc are required when --gap-fill-db-path is supplied"
        );
    };
    if gap_fill_window_end_utc <= gap_fill_window_start_utc {
        bail!(
            "program_history_gap_fill_restore_gate_invalid_window: gap-fill window end must be after start"
        );
    }
    if !gap_fill_db_path.exists() {
        bail!(
            "program_history_gap_fill_restore_gate_db_path_missing: gap-fill DB does not exist: {}",
            gap_fill_db_path.display()
        );
    }

    let raw = std::fs::read_to_string(gap_fill_progress_path).with_context(|| {
        format!(
            "program_history_gap_fill_restore_gate_progress_unreadable: failed reading {}",
            gap_fill_progress_path.display()
        )
    })?;
    let progress: Value = serde_json::from_str(&raw).with_context(|| {
        format!(
            "program_history_gap_fill_restore_gate_progress_unreadable: failed parsing {}",
            gap_fill_progress_path.display()
        )
    })?;

    validate_program_history_gap_fill_progress(
        &progress,
        gap_fill_window_start_utc,
        gap_fill_window_end_utc,
    )
    .map(Some)
}

#[derive(Debug, Clone, PartialEq)]
struct GapFillRestoreGateAcceptance {
    policy: &'static str,
    accepted_irreducible_boundary_residue: bool,
    fetched_rows: Option<u64>,
    staged_rows: Option<u64>,
    rows_withheld_due_to_incomplete_outcome: Option<u64>,
    start_coverage_deficit_ms: Option<i64>,
    end_coverage_deficit_ms: Option<i64>,
    total_accepted_residue_ms: Option<i64>,
    max_segment_or_deficit_ms: Option<i64>,
}

fn validate_program_history_gap_fill_progress(
    progress: &Value,
    gap_fill_window_start_utc: DateTime<Utc>,
    gap_fill_window_end_utc: DateTime<Utc>,
) -> Result<GapFillRestoreGateAcceptance> {
    let verdict = required_progress_string(progress, "verdict")?;
    let reason = required_progress_string(progress, "reason")?;
    let current_phase = required_progress_string(progress, "current_phase")?;
    let replayable_output = required_progress_bool(progress, "replayable_output")?;
    let requested_window_start = required_progress_ts(progress, "requested_window_start")?;
    let requested_window_end = required_progress_ts(progress, "requested_window_end")?;
    let covered_through = required_progress_cursor_ts(progress, "gap_fill_covered_through_cursor")?;
    let missing_segments = required_progress_array(progress, "missing_segments")?;
    let covered_since = optional_progress_ts(progress, "gap_fill_covered_since")?;
    let fetched_rows = optional_progress_u64(progress, "fetched_rows")?;
    let staged_rows = optional_progress_u64(progress, "staged_rows")?;
    let inserted_rows = required_progress_u64(progress, "inserted_rows")?;
    let rows_withheld_due_to_incomplete_outcome =
        required_progress_u64(progress, "rows_withheld_due_to_incomplete_outcome")?;

    if requested_window_start != gap_fill_window_start_utc
        || requested_window_end != gap_fill_window_end_utc
    {
        bail!(
            "program_history_gap_fill_restore_gate_window_mismatch: requested_window_start={} requested_window_end={} expected_window_start={} expected_window_end={}",
            requested_window_start.to_rfc3339(),
            requested_window_end.to_rfc3339(),
            gap_fill_window_start_utc.to_rfc3339(),
            gap_fill_window_end_utc.to_rfc3339()
        );
    }
    if replayable_output {
        if covered_through < gap_fill_window_end_utc {
            bail!(
                "program_history_gap_fill_restore_gate_covered_through_before_window_end: covered_through={} expected_window_end={}",
                covered_through.to_rfc3339(),
                gap_fill_window_end_utc.to_rfc3339()
            );
        }
        if inserted_rows == 0 {
            bail!(
                "program_history_gap_fill_restore_gate_inserted_rows_not_positive: inserted_rows=0"
            );
        }
        if !missing_segments.is_empty() {
            bail!(
                "program_history_gap_fill_restore_gate_missing_segments_present: missing_segments_count={}",
                missing_segments.len()
            );
        }
        if rows_withheld_due_to_incomplete_outcome > 0 {
            bail!(
                "program_history_gap_fill_restore_gate_rows_withheld_present: rows_withheld_due_to_incomplete_outcome={rows_withheld_due_to_incomplete_outcome}"
            );
        }
        return Ok(GapFillRestoreGateAcceptance {
            policy: GAP_FILL_ACCEPTANCE_FULL_REPLAYABLE,
            accepted_irreducible_boundary_residue: false,
            fetched_rows,
            staged_rows,
            rows_withheld_due_to_incomplete_outcome: Some(rows_withheld_due_to_incomplete_outcome),
            start_coverage_deficit_ms: None,
            end_coverage_deficit_ms: None,
            total_accepted_residue_ms: None,
            max_segment_or_deficit_ms: None,
        });
    }

    validate_accepted_irreducible_boundary_residue(
        &verdict,
        &reason,
        &current_phase,
        covered_since,
        covered_through,
        requested_window_start,
        requested_window_end,
        fetched_rows,
        staged_rows,
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome,
        missing_segments,
    )
}

#[derive(Debug, Clone, PartialEq)]
struct GapFillMissingSegment {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    reason: String,
}

#[derive(Debug, Clone, Default)]
struct GapFillResidueStats {
    target_boundary_segments_count: usize,
    zero_progress_root_segments_count: usize,
    unknown_non_target_segments_count: usize,
    total_boundary_missing_ms: i64,
    start_coverage_deficit_ms: Option<i64>,
    end_coverage_deficit_ms: Option<i64>,
    total_accepted_residue_ms: Option<i64>,
    max_segment_or_deficit_ms: Option<i64>,
    max_boundary_segment_ms: i64,
}

fn validate_accepted_irreducible_boundary_residue(
    verdict: &str,
    reason: &str,
    current_phase: &str,
    covered_since: Option<DateTime<Utc>>,
    covered_through: DateTime<Utc>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    fetched_rows: Option<u64>,
    staged_rows: Option<u64>,
    inserted_rows: u64,
    rows_withheld_due_to_incomplete_outcome: u64,
    missing_segments: &[Value],
) -> Result<GapFillRestoreGateAcceptance> {
    if verdict != IRREDUCIBLE_VERDICT {
        bail!("program_history_gap_fill_restore_gate_replayable_output_false: progress replayable_output=false");
    }
    if reason != IRREDUCIBLE_REASON {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_reason_mismatch: reason={reason}"
        );
    }
    if current_phase != "completed_with_explicit_missing_segments" {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_current_phase_mismatch: current_phase={current_phase}"
        );
    }
    if inserted_rows > 0 {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_inserted_rows_present: inserted_rows={inserted_rows}"
        );
    }
    let staged_rows = staged_rows.ok_or_else(|| {
        progress_required_field_error("staged_rows", "missing for accepted residue")
    })?;
    let fetched_rows = fetched_rows.ok_or_else(|| {
        progress_required_field_error("fetched_rows", "missing for accepted residue")
    })?;
    if staged_rows == 0 {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_staged_rows_not_positive: staged_rows=0"
        );
    }
    if fetched_rows != staged_rows {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_fetched_rows_mismatch: fetched_rows={fetched_rows} staged_rows={staged_rows}"
        );
    }
    if rows_withheld_due_to_incomplete_outcome != staged_rows {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_rows_withheld_mismatch: rows_withheld_due_to_incomplete_outcome={rows_withheld_due_to_incomplete_outcome} staged_rows={staged_rows}"
        );
    }
    let covered_since = covered_since.ok_or_else(|| {
        progress_required_field_error("gap_fill_covered_since", "missing for accepted residue")
    })?;
    let parsed_segments = missing_segments
        .iter()
        .enumerate()
        .map(|(index, segment)| gap_fill_missing_segment_from_value(index, segment))
        .collect::<Result<Vec<_>>>()?;
    let stats = gap_fill_residue_stats(
        &parsed_segments,
        Some(covered_since),
        Some(covered_through),
        requested_window_start,
        requested_window_end,
    );
    if stats.zero_progress_root_segments_count > 0 {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_root_segments_present: zero_progress_root_segments_count={}",
            stats.zero_progress_root_segments_count
        );
    }
    if stats.unknown_non_target_segments_count > 0 {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_unknown_non_target_segments_present: unknown_non_target_segments_count={}",
            stats.unknown_non_target_segments_count
        );
    }
    if stats.target_boundary_segments_count == 0 {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_no_target_segments: target_boundary_segments_count=0"
        );
    }
    if stats.start_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_start_deficit_too_large: start_coverage_deficit_ms={}",
            stats.start_coverage_deficit_ms.unwrap_or(i64::MAX)
        );
    }
    if stats.end_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_end_deficit_too_large: end_coverage_deficit_ms={}",
            stats.end_coverage_deficit_ms.unwrap_or(i64::MAX)
        );
    }
    if stats.total_accepted_residue_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_TOTAL_MS {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_total_duration_too_large: total_accepted_residue_ms={}",
            stats.total_accepted_residue_ms.unwrap_or(i64::MAX)
        );
    }
    if stats.max_segment_or_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        bail!(
            "program_history_gap_fill_restore_gate_irreducible_boundary_segment_duration_too_large: max_segment_or_deficit_ms={}",
            stats.max_segment_or_deficit_ms.unwrap_or(i64::MAX)
        );
    }
    Ok(GapFillRestoreGateAcceptance {
        policy: ACCEPTED_RESIDUE_POLICY,
        accepted_irreducible_boundary_residue: true,
        fetched_rows: Some(fetched_rows),
        staged_rows: Some(staged_rows),
        rows_withheld_due_to_incomplete_outcome: Some(rows_withheld_due_to_incomplete_outcome),
        start_coverage_deficit_ms: stats.start_coverage_deficit_ms,
        end_coverage_deficit_ms: stats.end_coverage_deficit_ms,
        total_accepted_residue_ms: stats.total_accepted_residue_ms,
        max_segment_or_deficit_ms: stats.max_segment_or_deficit_ms,
    })
}

fn required_progress_string(progress: &Value, field: &'static str) -> Result<String> {
    progress
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| progress_required_field_error(field, "missing or non-string value"))
}

fn required_progress_bool(progress: &Value, field: &'static str) -> Result<bool> {
    progress
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| progress_required_field_error(field, "missing or non-bool value"))
}

fn required_progress_u64(progress: &Value, field: &'static str) -> Result<u64> {
    progress
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| progress_required_field_error(field, "missing or non-u64 value"))
}

fn optional_progress_u64(progress: &Value, field: &'static str) -> Result<Option<u64>> {
    match progress.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| progress_required_field_error(field, "non-u64 value")),
    }
}

fn required_progress_ts(progress: &Value, field: &'static str) -> Result<DateTime<Utc>> {
    let raw = required_progress_string(progress, field)?;
    parse_ts(&raw).map_err(|error| progress_required_field_error(field, error))
}

fn optional_progress_ts(progress: &Value, field: &'static str) -> Result<Option<DateTime<Utc>>> {
    progress
        .get(field)
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| progress_required_field_error(field, error))
}

fn required_progress_cursor_ts(progress: &Value, field: &'static str) -> Result<DateTime<Utc>> {
    let raw = progress
        .get(field)
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            progress_required_field_error(
                "gap_fill_covered_through_cursor.ts_utc",
                "missing or non-string value",
            )
        })?;
    parse_ts(raw).map_err(|error| {
        progress_required_field_error("gap_fill_covered_through_cursor.ts_utc", error)
    })
}

fn required_progress_array<'a>(progress: &'a Value, field: &'static str) -> Result<&'a Vec<Value>> {
    progress
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| progress_required_field_error(field, "missing or non-array value"))
}

fn gap_fill_missing_segment_from_value(
    index: usize,
    value: &Value,
) -> Result<GapFillMissingSegment> {
    let start = required_progress_segment_ts(value, index, "start")?;
    let end = required_progress_segment_ts(value, index, "end")?;
    if end < start {
        return Err(progress_required_field_error(
            "missing_segments.duration",
            format!("segment {index} end is before start"),
        ));
    }
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            progress_required_field_error(
                "missing_segments.reason",
                format!("segment {index} missing or non-string reason"),
            )
        })?;
    Ok(GapFillMissingSegment { start, end, reason })
}

fn required_progress_segment_ts(
    value: &Value,
    index: usize,
    field: &'static str,
) -> Result<DateTime<Utc>> {
    let raw = value.get(field).and_then(Value::as_str).ok_or_else(|| {
        progress_required_field_error(
            "missing_segments.timestamp",
            format!("segment {index} missing or non-string {field}"),
        )
    })?;
    parse_ts(raw).map_err(|error| {
        progress_required_field_error(
            "missing_segments.timestamp",
            format!("segment {index} invalid {field}: {error}"),
        )
    })
}

fn gap_fill_residue_stats(
    missing_segments: &[GapFillMissingSegment],
    covered_since: Option<DateTime<Utc>>,
    covered_through: Option<DateTime<Utc>>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
) -> GapFillResidueStats {
    let mut stats = GapFillResidueStats::default();
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
        stats.max_segment_or_deficit_ms = Some(
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

fn segment_duration_ms(segment: &GapFillMissingSegment) -> i64 {
    segment
        .end
        .signed_duration_since(segment.start)
        .num_milliseconds()
        .max(0)
}

fn progress_required_field_error(
    field: &'static str,
    detail: impl std::fmt::Display,
) -> anyhow::Error {
    anyhow!(
        "program_history_gap_fill_restore_gate_required_field_missing_or_malformed: {field}: {detail}"
    )
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn run(config: Config) -> Result<String> {
    let gap_fill_acceptance = validate_program_history_gap_fill_restore_gate(
        config.gap_fill_db_path.as_deref(),
        config.gap_fill_progress_path.as_deref(),
        config.gap_fill_window_start_utc.as_ref(),
        config.gap_fill_window_end_utc.as_ref(),
    )?;

    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let journal_db_path = resolve_db_path(
        &config.config_path,
        config.journal_db_path.as_deref(),
        &loaded_config.recent_raw_journal.path,
    );
    let migrations_dir =
        resolve_migrations_dir(&config.config_path, &loaded_config.system.migrations_dir);
    let artifact: DiscoveryRuntimeArtifact = serde_json::from_slice(
        &std::fs::read(&config.artifact_path)
            .with_context(|| format!("failed reading {}", config.artifact_path.display()))?,
    )
    .with_context(|| format!("failed parsing {}", config.artifact_path.display()))?;

    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
    if !freshness.fresh_for_normal_restore() && !config.bootstrap_degraded {
        bail!(
            "runtime artifact is stale for normal restore (fresh_under_export_gate={}, fresh_under_current_gate={}); rerun with --bootstrap-degraded if incident recovery is intentional",
            freshness.fresh_under_export_gate,
            freshness.fresh_under_current_gate
        );
    }
    if config.bootstrap_degraded && loaded_config.execution.enabled {
        bail!("--bootstrap-degraded requires execution.enabled=false");
    }

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    let mut store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    store.run_migrations(&migrations_dir).with_context(|| {
        format!(
            "failed applying migrations from {}",
            migrations_dir.display()
        )
    })?;
    store.restore_discovery_runtime_artifact(&artifact, config.now, config.bootstrap_degraded)?;
    let replay = replay_recent_raw_journal(
        &store,
        &journal_db_path,
        config.gap_fill_db_path.as_deref(),
        config.now,
        i64::from(loaded_config.discovery.scoring_window_days.max(1)),
        loaded_config.recent_raw_journal.replay_batch_size.max(1),
        &artifact.runtime_cursor,
    )?;

    let operator_status = discovery.operator_status(&store, config.now)?;
    let verdict = discovery.runtime_restore_verdict(&store, config.now)?;
    let output = RestoreOutput {
        db_path: db_path.display().to_string(),
        journal_db_path: journal_db_path.display().to_string(),
        gap_fill_db_path: config
            .gap_fill_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        gap_fill_acceptance_policy: gap_fill_acceptance
            .as_ref()
            .map(|acceptance| acceptance.policy.to_string()),
        gap_fill_accepted_irreducible_boundary_residue: gap_fill_acceptance
            .as_ref()
            .is_some_and(|acceptance| acceptance.accepted_irreducible_boundary_residue),
        gap_fill_acceptance_fetched_rows: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.fetched_rows),
        gap_fill_acceptance_staged_rows: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.staged_rows),
        gap_fill_acceptance_rows_withheld_due_to_incomplete_outcome: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.rows_withheld_due_to_incomplete_outcome),
        gap_fill_acceptance_start_coverage_deficit_ms: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.start_coverage_deficit_ms),
        gap_fill_acceptance_end_coverage_deficit_ms: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.end_coverage_deficit_ms),
        gap_fill_acceptance_total_accepted_residue_ms: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.total_accepted_residue_ms),
        gap_fill_acceptance_max_segment_or_deficit_ms: gap_fill_acceptance
            .as_ref()
            .and_then(|acceptance| acceptance.max_segment_or_deficit_ms),
        freshness,
        replay,
        verdict,
        operator_status,
    };
    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing restore output json")
    } else {
        Ok(render_human(
            &config.config_path,
            &config.artifact_path,
            &journal_db_path,
            &output,
        ))
    }
}

fn render_human(
    config_path: &Path,
    artifact_path: &Path,
    journal_db_path: &Path,
    output: &RestoreOutput,
) -> String {
    let gap_fill_db_path = output.gap_fill_db_path.as_deref().unwrap_or("null");
    let gap_fill_acceptance_policy = output
        .gap_fill_acceptance_policy
        .as_deref()
        .unwrap_or("null");
    [
        "event=discovery_runtime_restore".to_string(),
        format!("config_path={}", config_path.display()),
        format!("artifact_path={}", artifact_path.display()),
        format!("db_path={}", output.db_path),
        format!("journal_db_path={}", journal_db_path.display()),
        format!("gap_fill_db_path={gap_fill_db_path}"),
        format!("gap_fill_acceptance_policy={gap_fill_acceptance_policy}"),
        format!(
            "gap_fill_accepted_irreducible_boundary_residue={}",
            output.gap_fill_accepted_irreducible_boundary_residue
        ),
        format!(
            "gap_fill_acceptance_fetched_rows={}",
            format_option(output.gap_fill_acceptance_fetched_rows)
        ),
        format!(
            "gap_fill_acceptance_staged_rows={}",
            format_option(output.gap_fill_acceptance_staged_rows)
        ),
        format!(
            "gap_fill_acceptance_rows_withheld_due_to_incomplete_outcome={}",
            format_option(output.gap_fill_acceptance_rows_withheld_due_to_incomplete_outcome)
        ),
        format!(
            "gap_fill_acceptance_start_coverage_deficit_ms={}",
            format_option(output.gap_fill_acceptance_start_coverage_deficit_ms)
        ),
        format!(
            "gap_fill_acceptance_end_coverage_deficit_ms={}",
            format_option(output.gap_fill_acceptance_end_coverage_deficit_ms)
        ),
        format!(
            "gap_fill_acceptance_total_accepted_residue_ms={}",
            format_option(output.gap_fill_acceptance_total_accepted_residue_ms)
        ),
        format!(
            "gap_fill_acceptance_max_segment_or_deficit_ms={}",
            format_option(output.gap_fill_acceptance_max_segment_or_deficit_ms)
        ),
        format!("verdict={}", output.verdict.verdict),
        format!(
            "fresh_under_export_gate={}",
            output.freshness.fresh_under_export_gate
        ),
        format!(
            "fresh_under_current_gate={}",
            output.freshness.fresh_under_current_gate
        ),
        format!("runtime_state={}", output.verdict.runtime_state),
        format!("runtime_mode={}", output.verdict.runtime_mode),
        format!("scoring_source={}", output.verdict.scoring_source),
        format!(
            "runtime_cursor_restored={}",
            output.verdict.runtime_cursor_restored
        ),
        format!("journal_available={}", output.verdict.journal_available),
        format!("journal_replayed={}", output.verdict.journal_replayed),
        format!(
            "journal_covers_artifact_cursor={}",
            output.verdict.journal_covers_artifact_cursor
        ),
        format!(
            "raw_coverage_satisfied={}",
            output.verdict.raw_coverage_satisfied
        ),
        format!(
            "journal_replayed_rows={}",
            output.verdict.journal_replayed_rows
        ),
        format!("gap_fill_replayed={}", output.replay.gap_fill_replayed),
        format!(
            "gap_fill_replayed_rows={}",
            output.replay.gap_fill_replayed_rows
        ),
        format!(
            "bootstrap_degraded_active={}",
            output.verdict.bootstrap_degraded_active
        ),
    ]
    .join("\n")
}

fn format_option<T: ToString>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn replay_recent_raw_journal(
    runtime_store: &SqliteStore,
    journal_db_path: &Path,
    gap_fill_db_path: Option<&Path>,
    now: DateTime<Utc>,
    scoring_window_days: i64,
    replay_batch_size: usize,
    artifact_runtime_cursor: &DiscoveryRuntimeCursor,
) -> Result<RestoreReplaySummary> {
    let required_window_start = now - Duration::days(scoring_window_days.max(1));
    runtime_store.set_discovery_recent_raw_restore_state(
        &DiscoveryRecentRawRestoreStateUpdate {
            journal_available: false,
            journal_replayed: false,
            required_window_start: Some(required_window_start),
            journal_covered_since: None,
            journal_covered_through_cursor: None,
            gap_fill_replayed: false,
            gap_fill_covered_since: None,
            gap_fill_covered_through_cursor: None,
            effective_covered_since: None,
            effective_covered_through_cursor: None,
            artifact_runtime_cursor: Some(artifact_runtime_cursor.clone()),
            journal_covers_artifact_cursor: false,
            raw_coverage_satisfied: false,
            gap_fill_replayed_rows: 0,
            replayed_rows: 0,
            reason: Some("journal_replay_started".to_string()),
            replay_started_at: Some(now),
            replay_completed_at: None,
        },
    )?;

    let replay = if journal_db_path.exists() {
        let journal_store = SqliteStore::open_read_only(journal_db_path).with_context(|| {
            format!(
                "failed opening recent raw journal db {}",
                journal_db_path.display()
            )
        })?;
        journal_store.replay_recent_raw_journal_into_runtime_store(
            runtime_store,
            required_window_start,
            artifact_runtime_cursor,
            replay_batch_size.max(1),
        )?
    } else {
        RecentRawJournalReplaySummary {
            required_window_start,
            artifact_runtime_cursor: artifact_runtime_cursor.clone(),
            journal_available: false,
            journal_covered_since: None,
            journal_covered_through_cursor: None,
            journal_covers_artifact_cursor: false,
            replayed_rows: 0,
            raw_coverage_satisfied: false,
        }
    };

    let gap_fill_replay = match gap_fill_db_path {
        Some(path) if path.exists() => {
            let gap_fill_store = SqliteStore::open_read_only(path)
                .with_context(|| format!("failed opening raw gap fill db {}", path.display()))?;
            Some(gap_fill_store.replay_recent_raw_journal_into_runtime_store(
                runtime_store,
                required_window_start,
                artifact_runtime_cursor,
                replay_batch_size.max(1),
            )?)
        }
        Some(_) | None => None,
    };

    let effective_covered_since = min_ts_opt(
        replay.journal_covered_since,
        gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_since),
    );
    let effective_covered_through_cursor = max_cursor_opt(
        replay.journal_covered_through_cursor.as_ref(),
        gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_through_cursor.as_ref()),
    );
    let runtime_window_has_rows = !runtime_store
        .load_recent_observed_swaps_since(required_window_start, 1)?
        .0
        .is_empty();
    let raw_coverage_satisfied = replay.journal_available
        && replay.journal_covers_artifact_cursor
        && effective_covered_since
            .is_some_and(|covered_since| covered_since <= required_window_start)
        && runtime_window_has_rows;
    let gap_fill_replayed = gap_fill_replay.is_some();
    let gap_fill_replayed_rows = gap_fill_replay
        .as_ref()
        .map(|summary| summary.replayed_rows)
        .unwrap_or(0);
    let replayed_rows = replay.replayed_rows.saturating_add(gap_fill_replayed_rows);

    let replay_summary = RestoreReplaySummary {
        required_window_start,
        artifact_runtime_cursor: artifact_runtime_cursor.clone(),
        journal_available: replay.journal_available,
        journal_covered_since: replay.journal_covered_since,
        journal_covered_through_cursor: replay.journal_covered_through_cursor.clone(),
        journal_covers_artifact_cursor: replay.journal_covers_artifact_cursor,
        gap_fill_replayed,
        gap_fill_covered_since: gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_since),
        gap_fill_covered_through_cursor: gap_fill_replay
            .as_ref()
            .and_then(|summary| summary.journal_covered_through_cursor.clone()),
        effective_covered_since,
        effective_covered_through_cursor,
        gap_fill_replayed_rows,
        replayed_rows,
        raw_coverage_satisfied,
    };

    runtime_store.set_discovery_recent_raw_restore_state(
        &DiscoveryRecentRawRestoreStateUpdate {
            journal_available: replay.journal_available,
            journal_replayed: replay.replayed_rows > 0 || replay.journal_available,
            required_window_start: Some(replay_summary.required_window_start),
            journal_covered_since: replay.journal_covered_since,
            journal_covered_through_cursor: replay.journal_covered_through_cursor.clone(),
            gap_fill_replayed,
            gap_fill_covered_since: replay_summary.gap_fill_covered_since,
            gap_fill_covered_through_cursor: replay_summary.gap_fill_covered_through_cursor.clone(),
            effective_covered_since: replay_summary.effective_covered_since,
            effective_covered_through_cursor: replay_summary
                .effective_covered_through_cursor
                .clone(),
            artifact_runtime_cursor: Some(replay.artifact_runtime_cursor.clone()),
            journal_covers_artifact_cursor: replay.journal_covers_artifact_cursor,
            raw_coverage_satisfied,
            gap_fill_replayed_rows,
            replayed_rows,
            reason: Some(recent_raw_journal_restore_reason(
                &replay,
                gap_fill_replay.as_ref(),
                journal_db_path,
                gap_fill_db_path,
                raw_coverage_satisfied,
            )),
            replay_started_at: Some(now),
            replay_completed_at: Some(now),
        },
    )?;
    Ok(replay_summary)
}

fn recent_raw_journal_restore_reason(
    replay: &RecentRawJournalReplaySummary,
    gap_fill_replay: Option<&RecentRawJournalReplaySummary>,
    journal_db_path: &Path,
    gap_fill_db_path: Option<&Path>,
    raw_coverage_satisfied: bool,
) -> String {
    if !replay.journal_available {
        return format!(
            "recent_raw_journal_unavailable:{}",
            journal_db_path.display()
        );
    }
    if !replay.journal_covers_artifact_cursor {
        return "recent_raw_journal_missing_artifact_cursor_lineage".to_string();
    }
    if !raw_coverage_satisfied {
        if let Some(path) = gap_fill_db_path {
            if gap_fill_replay.is_some() {
                return format!(
                    "recent_raw_gap_fill_raw_coverage_unsatisfied:{}",
                    path.display()
                );
            }
        }
        return "recent_raw_journal_raw_coverage_unsatisfied".to_string();
    }
    if gap_fill_replay.is_some() {
        return "recent_raw_journal_gap_fill_replay_completed".to_string();
    }
    "recent_raw_journal_replay_completed".to_string()
}

fn min_ts_opt(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_cursor_opt(
    left: Option<&DiscoveryRuntimeCursor>,
    right: Option<&DiscoveryRuntimeCursor>,
) -> Option<DiscoveryRuntimeCursor> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if cmp_cursor(left, right) != std::cmp::Ordering::Less {
                Some(left.clone())
            } else {
                Some(right.clone())
            }
        }
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn cmp_cursor(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

#[cfg(test)]
mod tests {
    use super::{
        export_artifact, make_fixture, make_swap, parse_ts, seed_recent_raw_journal,
        seed_runtime_artifact_source, SqliteStore, Value,
    };
    use super::{
        parse_args_from, run, validate_program_history_gap_fill_restore_gate, Config,
        ACCEPTED_RESIDUE_POLICY, BOUNDARY_PREFIX_REASON, BOUNDARY_SUFFIX_REASON,
        GAP_FILL_ACCEPTANCE_FULL_REPLAYABLE, IRREDUCIBLE_REASON, IRREDUCIBLE_VERDICT,
        ZERO_PROGRESS_ROOT_REASON,
    };
    use anyhow::Result;
    use chrono::{DateTime, Duration, Utc};
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn write_gap_fill_progress(
        path: &Path,
        replayable_output: bool,
        requested_window_start: DateTime<Utc>,
        requested_window_end: DateTime<Utc>,
        covered_through: DateTime<Utc>,
        missing_segments: &str,
        inserted_rows: u64,
        rows_withheld: u64,
    ) -> Result<()> {
        write_gap_fill_progress_with_control(
            path,
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

    fn write_gap_fill_progress_with_control(
        path: &Path,
        verdict: &str,
        reason: &str,
        current_phase: &str,
        replayable_output: bool,
        requested_window_start: DateTime<Utc>,
        requested_window_end: DateTime<Utc>,
        covered_through: DateTime<Utc>,
        missing_segments: &str,
        inserted_rows: u64,
        rows_withheld: u64,
    ) -> Result<()> {
        std::fs::write(
            path,
            format!(
                r#"{{
  "verdict": "{verdict}",
  "reason": "{reason}",
  "current_phase": "{current_phase}",
  "replayable_output": {replayable_output},
  "requested_window_start": "{}",
  "requested_window_end": "{}",
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{}",
    "slot": 123,
    "signature": "gap-fill-cursor"
  }},
  "gap_fill_covered_since": "{}",
  "missing_segments": {missing_segments},
  "fetched_rows": {inserted_rows},
  "staged_rows": {inserted_rows},
  "inserted_rows": {inserted_rows},
  "rows_withheld_due_to_incomplete_outcome": {rows_withheld}
}}"#,
                requested_window_start.to_rfc3339(),
                requested_window_end.to_rfc3339(),
                covered_through.to_rfc3339(),
                requested_window_start.to_rfc3339(),
            ),
        )?;
        Ok(())
    }

    fn write_accepted_residue_gap_fill_progress(
        path: &Path,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        missing_segments: &str,
        rows_withheld: u64,
    ) -> Result<()> {
        write_accepted_residue_gap_fill_progress_with_fields(
            path,
            window_start,
            window_end,
            missing_segments,
            rows_withheld,
            rows_withheld,
            0,
            rows_withheld,
            window_start,
            window_end - Duration::milliseconds(857),
        )
    }

    fn write_accepted_residue_gap_fill_progress_with_fields(
        path: &Path,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        missing_segments: &str,
        staged_rows: u64,
        fetched_rows: u64,
        inserted_rows: u64,
        rows_withheld: u64,
        covered_since: DateTime<Utc>,
        covered_through: DateTime<Utc>,
    ) -> Result<()> {
        std::fs::write(
            path,
            format!(
                r#"{{
  "verdict": "{IRREDUCIBLE_VERDICT}",
  "reason": "{IRREDUCIBLE_REASON}",
  "current_phase": "completed_with_explicit_missing_segments",
  "replayable_output": false,
  "requested_window_start": "{}",
  "requested_window_end": "{}",
  "gap_fill_covered_since": "{}",
  "gap_fill_covered_through_cursor": {{
    "ts_utc": "{}",
    "slot": 123,
    "signature": "gap-fill-cursor"
  }},
  "missing_segments": {missing_segments},
  "fetched_rows": {fetched_rows},
  "staged_rows": {staged_rows},
  "inserted_rows": {inserted_rows},
  "rows_withheld_due_to_incomplete_outcome": {rows_withheld}
}}"#,
                window_start.to_rfc3339(),
                window_end.to_rfc3339(),
                covered_since.to_rfc3339(),
                covered_through.to_rfc3339(),
            ),
        )?;
        Ok(())
    }

    fn write_valid_gap_fill_progress(
        path: &Path,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<()> {
        write_gap_fill_progress(path, true, window_start, window_end, window_end, "[]", 1, 0)
    }

    fn live_shaped_boundary_segments(
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> String {
        format!(
            r#"[
  {{ "start": "{}", "end": "{}", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "{}", "end": "{}", "reason": "{BOUNDARY_SUFFIX_REASON}" }},
  {{ "start": "{}", "end": "{}", "reason": "{IRREDUCIBLE_REASON}" }}
]"#,
            window_start.to_rfc3339(),
            (window_start + Duration::milliseconds(600)).to_rfc3339(),
            (window_end - Duration::seconds(1)).to_rfc3339(),
            window_end.to_rfc3339(),
            window_start.to_rfc3339(),
            window_end.to_rfc3339(),
        )
    }

    fn assert_reason(error: anyhow::Error, reason: &str) {
        let message = error.to_string();
        assert!(
            message.starts_with(reason),
            "expected error to start with {reason}, got {message}"
        );
    }

    fn restore_test_now() -> DateTime<Utc> {
        Utc::now() + Duration::minutes(5)
    }

    #[test]
    fn parse_args_from_accepts_bootstrap_json_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--artifact".to_string(),
            "artifacts/runtime.json".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--bootstrap-degraded".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");
        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(
            parsed.artifact_path,
            PathBuf::from("artifacts/runtime.json")
        );
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert_eq!(parsed.journal_db_path, None);
        assert_eq!(parsed.gap_fill_db_path, None);
        assert_eq!(parsed.gap_fill_progress_path, None);
        assert_eq!(parsed.gap_fill_window_start_utc, None);
        assert_eq!(parsed.gap_fill_window_end_utc, None);
        assert!(parsed.bootstrap_degraded);
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn parse_args_requires_gap_fill_progress_path_with_gap_fill_db_path() {
        let error = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--artifact".to_string(),
            "artifacts/runtime.json".to_string(),
            "--gap-fill-db-path".to_string(),
            "state/gap-fill.db".to_string(),
            "--gap-fill-window-start-utc".to_string(),
            "2026-04-18T16:56:04Z".to_string(),
            "--gap-fill-window-end-utc".to_string(),
            "2026-04-23T15:59:39Z".to_string(),
        ])
        .expect_err("missing gap-fill progress path must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_missing_progress_path",
        );
    }

    #[test]
    fn parse_args_requires_gap_fill_window_args_with_gap_fill_db_path() {
        let error = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--artifact".to_string(),
            "artifacts/runtime.json".to_string(),
            "--gap-fill-db-path".to_string(),
            "state/gap-fill.db".to_string(),
            "--gap-fill-progress-path".to_string(),
            "state/gap-fill.progress.json".to_string(),
        ])
        .expect_err("missing gap-fill window args must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_missing_window_args",
        );
    }

    #[test]
    fn parse_args_rejects_gap_fill_window_end_equal_start() {
        let error = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--artifact".to_string(),
            "artifacts/runtime.json".to_string(),
            "--gap-fill-db-path".to_string(),
            "state/gap-fill.db".to_string(),
            "--gap-fill-progress-path".to_string(),
            "state/gap-fill.progress.json".to_string(),
            "--gap-fill-window-start-utc".to_string(),
            "2026-04-18T16:56:04Z".to_string(),
            "--gap-fill-window-end-utc".to_string(),
            "2026-04-18T16:56:04Z".to_string(),
        ])
        .expect_err("equal gap-fill window bounds must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_invalid_window",
        );
    }

    #[test]
    fn valid_progress_gate_allows_existing_gap_fill_db_path() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_valid_gap_fill_progress(&progress_path, window_start, window_end)?;

        let acceptance = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )?
        .expect("gap-fill gate should return acceptance");
        assert_eq!(acceptance.policy, GAP_FILL_ACCEPTANCE_FULL_REPLAYABLE);
        assert!(!acceptance.accepted_irreducible_boundary_residue);
        Ok(())
    }

    #[test]
    fn accepted_residue_progress_gate_allows_existing_gap_fill_db_path() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_accepted_residue_gap_fill_progress(
            &progress_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            5,
        )?;

        let acceptance = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )?
        .expect("gap-fill gate should return accepted residue");
        assert_eq!(acceptance.policy, ACCEPTED_RESIDUE_POLICY);
        assert!(acceptance.accepted_irreducible_boundary_residue);
        assert_eq!(acceptance.fetched_rows, Some(5));
        assert_eq!(acceptance.staged_rows, Some(5));
        assert_eq!(acceptance.rows_withheld_due_to_incomplete_outcome, Some(5));
        assert_eq!(acceptance.start_coverage_deficit_ms, Some(0));
        assert_eq!(acceptance.end_coverage_deficit_ms, Some(857));
        assert_eq!(acceptance.total_accepted_residue_ms, Some(2_457));
        assert_eq!(acceptance.max_segment_or_deficit_ms, Some(1_000));
        Ok(())
    }

    #[test]
    fn missing_gap_fill_db_path_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("missing-gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        write_valid_gap_fill_progress(&progress_path, window_start, window_end)?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("missing gap-fill DB must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_db_path_missing",
        );
        Ok(())
    }

    #[test]
    fn validation_rejects_gap_fill_window_end_before_start() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-23T15:59:39Z")?;
        let window_end = parse_ts("2026-04-18T16:56:04Z")?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("reversed gap-fill window must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_invalid_window",
        );
        Ok(())
    }

    #[test]
    fn replayable_output_false_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &progress_path,
            false,
            window_start,
            window_end,
            window_end,
            "[]",
            1,
            0,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("replayable_output=false must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_replayable_output_false",
        );
        Ok(())
    }

    #[test]
    fn requested_window_mismatch_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &progress_path,
            true,
            window_start + Duration::seconds(1),
            window_end,
            window_end,
            "[]",
            1,
            0,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("requested window mismatch must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_window_mismatch",
        );
        Ok(())
    }

    #[test]
    fn covered_through_before_window_end_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &progress_path,
            true,
            window_start,
            window_end,
            window_end - Duration::seconds(1),
            "[]",
            1,
            0,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("covered_through before window end must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_covered_through_before_window_end",
        );
        Ok(())
    }

    #[test]
    fn missing_segments_non_empty_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &progress_path,
            true,
            window_start,
            window_end,
            window_end,
            r#"[{"start":"2026-04-18T16:56:04Z","end":"2026-04-18T17:56:04Z"}]"#,
            1,
            0,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("missing segments must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_missing_segments_present",
        );
        Ok(())
    }

    #[test]
    fn rows_withheld_present_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &progress_path,
            true,
            window_start,
            window_end,
            window_end,
            "[]",
            1,
            1,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("withheld rows must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_rows_withheld_present",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_root_segment_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        let segments = format!(
            r#"[
  {{ "start": "{}", "end": "{}", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "{ZERO_PROGRESS_ROOT_REASON}" }}
]"#,
            window_start.to_rfc3339(),
            (window_start + Duration::milliseconds(600)).to_rfc3339(),
        );
        write_accepted_residue_gap_fill_progress(
            &progress_path,
            window_start,
            window_end,
            &segments,
            7,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("root residue must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_root_segments_present",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_unknown_non_target_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        let segments = format!(
            r#"[
  {{ "start": "{}", "end": "{}", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "unexpected_boundary_gap" }}
]"#,
            window_start.to_rfc3339(),
            (window_start + Duration::milliseconds(600)).to_rfc3339(),
        );
        write_accepted_residue_gap_fill_progress(
            &progress_path,
            window_start,
            window_end,
            &segments,
            7,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("unknown residue must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_unknown_non_target_segments_present",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_wrong_reason_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress_with_control(
            &progress_path,
            IRREDUCIBLE_VERDICT,
            "program_history_gap_fill_incomplete_due_to_persistently_blocked_slot_gap",
            "completed_with_explicit_missing_segments",
            false,
            window_start,
            window_end,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            1,
            0,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("wrong residue reason must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_reason_mismatch",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_total_duration_over_policy_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        let mut segment_lines = Vec::new();
        for offset in 0..11 {
            let reason = if offset % 2 == 0 {
                BOUNDARY_PREFIX_REASON
            } else {
                BOUNDARY_SUFFIX_REASON
            };
            let start = window_start + Duration::seconds(offset);
            let end = start + Duration::seconds(1);
            segment_lines.push(format!(
                r#"{{ "start": "{}", "end": "{}", "reason": "{reason}" }}"#,
                start.to_rfc3339(),
                end.to_rfc3339()
            ));
        }
        let segments = format!("[{}]", segment_lines.join(","));
        write_accepted_residue_gap_fill_progress(
            &progress_path,
            window_start,
            window_end,
            &segments,
            7,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("oversized total residue must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_total_duration_too_large",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_max_segment_over_policy_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        let segments = format!(
            r#"[
  {{ "start": "{}", "end": "{}", "reason": "{BOUNDARY_PREFIX_REASON}" }}
]"#,
            window_start.to_rfc3339(),
            (window_start + Duration::milliseconds(1_001)).to_rfc3339(),
        );
        write_accepted_residue_gap_fill_progress(
            &progress_path,
            window_start,
            window_end,
            &segments,
            7,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("oversized segment residue must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_segment_duration_too_large",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_staged_rows_zero_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_accepted_residue_gap_fill_progress_with_fields(
            &progress_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            0,
            0,
            0,
            0,
            window_start,
            window_end - Duration::milliseconds(857),
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("zero staged accepted residue must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_staged_rows_not_positive",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_missing_staged_or_fetched_rows_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;

        let missing_staged_path = temp.path().join("missing-staged.progress.json");
        std::fs::write(
            &missing_staged_path,
            std::fs::read_to_string({
                let path = temp.path().join("template-staged.progress.json");
                write_accepted_residue_gap_fill_progress_with_fields(
                    &path,
                    window_start,
                    window_end,
                    &live_shaped_boundary_segments(window_start, window_end),
                    7,
                    7,
                    0,
                    7,
                    window_start,
                    window_end - Duration::milliseconds(857),
                )?;
                path
            })?
            .replace(
                r#"  "staged_rows": 7,
"#,
                "",
            ),
        )?;
        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&missing_staged_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("missing staged accepted residue must fail");
        assert!(error.to_string().contains("staged_rows"));
        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_required_field_missing_or_malformed",
        );

        let missing_fetched_path = temp.path().join("missing-fetched.progress.json");
        std::fs::write(
            &missing_fetched_path,
            std::fs::read_to_string({
                let path = temp.path().join("template-fetched.progress.json");
                write_accepted_residue_gap_fill_progress_with_fields(
                    &path,
                    window_start,
                    window_end,
                    &live_shaped_boundary_segments(window_start, window_end),
                    7,
                    7,
                    0,
                    7,
                    window_start,
                    window_end - Duration::milliseconds(857),
                )?;
                path
            })?
            .replace(
                r#"  "fetched_rows": 7,
"#,
                "",
            ),
        )?;
        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&missing_fetched_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("missing fetched accepted residue must fail");
        assert!(error.to_string().contains("fetched_rows"));
        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_required_field_missing_or_malformed",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_row_accounting_mismatches_fail_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;

        let fetched_mismatch_path = temp.path().join("fetched-mismatch.progress.json");
        write_accepted_residue_gap_fill_progress_with_fields(
            &fetched_mismatch_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            7,
            6,
            0,
            7,
            window_start,
            window_end - Duration::milliseconds(857),
        )?;
        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&fetched_mismatch_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("fetched mismatch accepted residue must fail");
        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_fetched_rows_mismatch",
        );

        let withheld_mismatch_path = temp.path().join("withheld-mismatch.progress.json");
        write_accepted_residue_gap_fill_progress_with_fields(
            &withheld_mismatch_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            7,
            7,
            0,
            6,
            window_start,
            window_end - Duration::milliseconds(857),
        )?;
        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&withheld_mismatch_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("withheld mismatch accepted residue must fail");
        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_rows_withheld_mismatch",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_inserted_rows_present_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_accepted_residue_gap_fill_progress_with_fields(
            &progress_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            7,
            7,
            1,
            7,
            window_start,
            window_end - Duration::milliseconds(857),
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("inserted rows accepted residue must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_inserted_rows_present",
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_coverage_deficits_over_policy_fail_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;

        let start_deficit_path = temp.path().join("start-deficit.progress.json");
        write_accepted_residue_gap_fill_progress_with_fields(
            &start_deficit_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            7,
            7,
            0,
            7,
            window_start + Duration::milliseconds(1_001),
            window_end - Duration::milliseconds(857),
        )?;
        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&start_deficit_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("start deficit accepted residue must fail");
        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_start_deficit_too_large",
        );

        let end_deficit_path = temp.path().join("end-deficit.progress.json");
        write_accepted_residue_gap_fill_progress_with_fields(
            &end_deficit_path,
            window_start,
            window_end,
            &live_shaped_boundary_segments(window_start, window_end),
            7,
            7,
            0,
            7,
            window_start,
            window_end - Duration::milliseconds(1_001),
        )?;
        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&end_deficit_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("end deficit accepted residue must fail");
        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_end_deficit_too_large",
        );
        Ok(())
    }

    #[test]
    fn inserted_rows_not_positive_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &progress_path,
            true,
            window_start,
            window_end,
            window_end,
            "[]",
            0,
            0,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("zero inserted rows must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_inserted_rows_not_positive",
        );
        Ok(())
    }

    #[test]
    fn missing_required_field_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        std::fs::write(
            &progress_path,
            r#"{
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "replayable_output": true,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-23T15:59:39Z",
  "gap_fill_covered_through_cursor": { "ts_utc": "2026-04-23T15:59:39Z" },
  "missing_segments": [],
  "inserted_rows": 1,
  "rows_withheld_due_to_incomplete_outcome": 0
}"#,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("missing verdict must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_required_field_missing_or_malformed",
        );
        Ok(())
    }

    #[test]
    fn malformed_required_field_fails_closed() -> Result<()> {
        let temp = tempdir()?;
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let progress_path = temp.path().join("gap-fill.progress.json");
        let window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        std::fs::write(
            &progress_path,
            r#"{
  "verdict": "complete_but_insufficient_for_healthy_restore",
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "replayable_output": true,
  "requested_window_start": "2026-04-18T16:56:04Z",
  "requested_window_end": "2026-04-23T15:59:39Z",
  "gap_fill_covered_through_cursor": { "ts_utc": false },
  "missing_segments": [],
  "inserted_rows": 1,
  "rows_withheld_due_to_incomplete_outcome": 0
}"#,
        )?;

        let error = validate_program_history_gap_fill_restore_gate(
            Some(&gap_fill_db_path),
            Some(&progress_path),
            Some(&window_start),
            Some(&window_end),
        )
        .expect_err("malformed covered_through timestamp must fail");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_required_field_missing_or_malformed",
        );
        Ok(())
    }

    #[test]
    fn invalid_gap_fill_gate_does_not_create_target_db() -> Result<()> {
        let fixture = make_fixture("runtime-restore-invalid-gap-fill-gate")?;
        let now = restore_test_now();
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-invalid-gate.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture
            .temp
            .path()
            .join("nested-target")
            .join("restored-invalid-gate.db");
        let gap_fill_db_path = fixture.temp.path().join("gap-fill-invalid-gate.db");
        let gap_fill_progress_path = fixture.temp.path().join("gap-fill-invalid-gate.json");
        let gap_fill_window_start = now - Duration::days(7);
        let gap_fill_window_end = now;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        write_gap_fill_progress(
            &gap_fill_progress_path,
            false,
            gap_fill_window_start,
            gap_fill_window_end,
            gap_fill_window_end,
            "[]",
            1,
            0,
        )?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: None,
            gap_fill_db_path: Some(gap_fill_db_path),
            gap_fill_progress_path: Some(gap_fill_progress_path),
            gap_fill_window_start_utc: Some(gap_fill_window_start),
            gap_fill_window_end_utc: Some(gap_fill_window_end),
            bootstrap_degraded: false,
            json: true,
            now,
        })
        .expect_err("invalid gate must stop before target DB mutation");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_replayable_output_false",
        );
        assert!(!restored_db_path.exists());
        assert!(!restored_db_path.parent().expect("target parent").exists());
        Ok(())
    }

    #[test]
    fn invalid_gap_fill_window_does_not_create_target_db() -> Result<()> {
        let temp = tempdir()?;
        let restored_db_path = temp
            .path()
            .join("invalid-window-target")
            .join("restored.db");
        let gap_fill_window_start = parse_ts("2026-04-23T15:59:39Z")?;
        let gap_fill_window_end = parse_ts("2026-04-18T16:56:04Z")?;

        let error = run(Config {
            config_path: temp.path().join("missing-config.toml"),
            artifact_path: temp.path().join("missing-artifact.json"),
            db_path: Some(restored_db_path.clone()),
            journal_db_path: None,
            gap_fill_db_path: Some(temp.path().join("gap-fill.db")),
            gap_fill_progress_path: Some(temp.path().join("gap-fill.progress.json")),
            gap_fill_window_start_utc: Some(gap_fill_window_start),
            gap_fill_window_end_utc: Some(gap_fill_window_end),
            bootstrap_degraded: false,
            json: true,
            now: restore_test_now(),
        })
        .expect_err("invalid window must stop before target DB mutation");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_invalid_window",
        );
        assert!(!restored_db_path.exists());
        assert!(!restored_db_path.parent().expect("target parent").exists());
        Ok(())
    }

    #[test]
    fn run_roundtrips_runtime_artifact_into_fresh_db() -> Result<()> {
        let fixture = make_fixture("runtime-restore-roundtrip")?;
        let now = restore_test_now();
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-roundtrip.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-roundtrip.db");
        let _ = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(restored_db_path.clone()),
            journal_db_path: None,
            gap_fill_db_path: None,
            gap_fill_progress_path: None,
            gap_fill_window_start_utc: None,
            gap_fill_window_end_utc: None,
            bootstrap_degraded: false,
            json: false,
            now,
        })?;

        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let publication_state = restored
            .discovery_publication_state()?
            .expect("publication state must be restored");
        assert_eq!(
            publication_state.last_published_at,
            artifact.publication_state.last_published_at
        );
        assert_eq!(
            publication_state.last_published_window_start,
            artifact.publication_state.last_published_window_start
        );
        assert_eq!(
            restored.load_discovery_runtime_cursor()?,
            Some(artifact.runtime_cursor.clone())
        );
        assert_eq!(
            restored
                .load_wallet_metric_snapshots_for_window(
                    artifact
                        .publication_state
                        .last_published_window_start
                        .expect("artifact window")
                )?
                .len(),
            artifact.published_wallet_metrics_snapshot.len()
        );
        Ok(())
    }

    #[test]
    fn run_rejects_stale_artifact_without_bootstrap_degraded() -> Result<()> {
        let fixture = make_fixture("runtime-restore-stale-reject")?;
        let artifact_now = restore_test_now();
        let now = artifact_now + Duration::hours(3);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact_path = fixture.temp.path().join("artifact-stale.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(fixture.temp.path().join("restored-stale.db")),
            journal_db_path: None,
            gap_fill_db_path: None,
            gap_fill_progress_path: None,
            gap_fill_window_start_utc: None,
            gap_fill_window_end_utc: None,
            bootstrap_degraded: false,
            json: false,
            now,
        })
        .expect_err("stale artifact must be rejected for normal restore");

        assert!(error
            .to_string()
            .contains("runtime artifact is stale for normal restore"));
        Ok(())
    }

    #[test]
    fn run_restores_stale_artifact_in_bootstrap_degraded_mode() -> Result<()> {
        let fixture = make_fixture("runtime-restore-bootstrap")?;
        let artifact_now = restore_test_now();
        let now = artifact_now + Duration::hours(3);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact_path = fixture.temp.path().join("artifact-bootstrap.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-bootstrap.db");

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(restored_db_path.clone()),
            journal_db_path: None,
            gap_fill_db_path: None,
            gap_fill_progress_path: None,
            gap_fill_window_start_utc: None,
            gap_fill_window_end_utc: None,
            bootstrap_degraded: true,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("bootstrap_degraded")
        );
        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let bootstrap_state = restored.discovery_bootstrap_degraded_state()?;
        assert!(bootstrap_state.active);
        let publication_state = restored
            .discovery_publication_state()?
            .expect("publication state must be restored");
        assert_eq!(
            publication_state.last_published_at,
            artifact.publication_state.last_published_at
        );
        assert_eq!(
            publication_state.last_published_window_start,
            artifact.publication_state.last_published_window_start
        );
        Ok(())
    }

    #[test]
    fn run_restore_verdict_stays_fail_closed_without_required_raw_coverage() -> Result<()> {
        let fixture = make_fixture("runtime-restore-short-journal")?;
        let now = restore_test_now();
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-short-journal.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-short-journal.db");
        let journal_db_path = fixture.temp.path().join("recent-short-journal.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap(
                "journal-short-only",
                artifact_now + Duration::minutes(10),
                50,
            )],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: None,
            gap_fill_progress_path: None,
            gap_fill_window_start_utc: None,
            gap_fill_window_end_utc: None,
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("fail_closed")
        );
        assert_eq!(
            parsed
                .pointer("/verdict/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            parsed
                .pointer("/operator_status/recent_raw_restore/journal_available")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/operator_status/recent_raw_restore/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn run_replays_recent_raw_journal_into_fresh_runtime_db_and_becomes_trading_ready() -> Result<()>
    {
        let fixture = make_fixture("runtime-restore-trading-ready")?;
        let now = restore_test_now();
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-trading-ready.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-trading-ready.db");
        let journal_db_path = fixture.temp.path().join("recent-trading-ready.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[
                make_swap("journal-too-old", now - Duration::days(9), 10),
                make_swap("journal-window-start", now - Duration::days(7), 11),
                make_swap("journal-recent", now - Duration::minutes(5), 12),
            ],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path: artifact_path.clone(),
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: None,
            gap_fill_progress_path: None,
            gap_fill_window_start_utc: None,
            gap_fill_window_end_utc: None,
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("trading_ready")
        );
        assert_eq!(
            parsed
                .pointer("/verdict/journal_available")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/verdict/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/replay/replayed_rows")
                .and_then(Value::as_u64),
            Some(2)
        );
        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let restored_signatures = restored
            .load_observed_swaps_since(now - Duration::days(30))?
            .into_iter()
            .map(|swap| swap.signature)
            .collect::<Vec<_>>();
        assert_eq!(
            restored_signatures,
            vec![
                "journal-window-start".to_string(),
                "journal-recent".to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn run_keeps_bootstrap_degraded_verdict_even_when_journal_replay_restores_raw_window(
    ) -> Result<()> {
        let fixture = make_fixture("runtime-restore-bootstrap-journal")?;
        let artifact_now = restore_test_now();
        let now = artifact_now + Duration::hours(3);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact_path = fixture.temp.path().join("artifact-bootstrap-journal.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;
        let restored_db_path = fixture.temp.path().join("restored-bootstrap-journal.db");
        let journal_db_path = fixture.temp.path().join("recent-bootstrap-journal.db");
        seed_recent_raw_journal(
            &journal_db_path,
            &[
                make_swap("journal-bootstrap-window", now - Duration::days(7), 21),
                make_swap("journal-bootstrap-recent", now - Duration::minutes(5), 22),
            ],
            now,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: None,
            gap_fill_progress_path: None,
            gap_fill_window_start_utc: None,
            gap_fill_window_end_utc: None,
            bootstrap_degraded: true,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("bootstrap_degraded")
        );
        assert_eq!(
            parsed
                .pointer("/operator_status/recent_raw_restore/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );
        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        assert!(restored.discovery_bootstrap_degraded_state()?.active);
        Ok(())
    }

    #[test]
    fn run_gap_fill_plus_recent_journal_restores_trading_ready_verdict() -> Result<()> {
        let fixture = make_fixture("runtime-restore-gap-fill-healthy")?;
        let now = restore_test_now();
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-gap-fill-healthy.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-gap-fill-healthy.db");
        let journal_db_path = fixture.temp.path().join("recent-gap-fill-healthy.db");
        let gap_fill_db_path = fixture.temp.path().join("gap-fill-healthy.db");
        let gap_fill_progress_path = fixture.temp.path().join("gap-fill-healthy.progress.json");
        let gap_fill_window_start = now - Duration::days(7);
        let gap_fill_window_end = now;
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap("journal-recent", now - Duration::minutes(5), 12)],
            now,
        )?;
        seed_recent_raw_journal(
            &gap_fill_db_path,
            &[make_swap(
                "gap-fill-window-start",
                now - Duration::days(7),
                11,
            )],
            now,
        )?;
        write_valid_gap_fill_progress(
            &gap_fill_progress_path,
            gap_fill_window_start,
            gap_fill_window_end,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: Some(gap_fill_db_path),
            gap_fill_progress_path: Some(gap_fill_progress_path),
            gap_fill_window_start_utc: Some(gap_fill_window_start),
            gap_fill_window_end_utc: Some(gap_fill_window_end),
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("trading_ready")
        );
        assert_eq!(
            parsed
                .pointer("/replay/gap_fill_replayed")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/replay/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_policy")
                .and_then(Value::as_str),
            Some(GAP_FILL_ACCEPTANCE_FULL_REPLAYABLE)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_accepted_irreducible_boundary_residue")
                .and_then(Value::as_bool),
            Some(false)
        );

        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let restore_state = restored.discovery_recent_raw_restore_state()?;
        assert!(restore_state.raw_coverage_satisfied);
        assert!(restore_state.gap_fill_replayed);
        assert_eq!(restore_state.gap_fill_replayed_rows, 1);
        Ok(())
    }

    #[test]
    fn run_accepted_residue_gap_fill_records_policy_and_can_restore_raw_window() -> Result<()> {
        let fixture = make_fixture("runtime-restore-gap-fill-accepted-residue")?;
        let now = restore_test_now();
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-gap-fill-residue.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-gap-fill-residue.db");
        let journal_db_path = fixture.temp.path().join("recent-gap-fill-residue.db");
        let gap_fill_db_path = fixture.temp.path().join("gap-fill-residue.db");
        let gap_fill_progress_path = fixture.temp.path().join("gap-fill-residue.progress.json");
        let gap_fill_window_start = now - Duration::days(7);
        let gap_fill_window_end = now;
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap("journal-recent", now - Duration::minutes(5), 12)],
            now,
        )?;
        seed_recent_raw_journal(
            &gap_fill_db_path,
            &[make_swap(
                "gap-fill-window-start-residue",
                now - Duration::days(7),
                11,
            )],
            now,
        )?;
        write_accepted_residue_gap_fill_progress(
            &gap_fill_progress_path,
            gap_fill_window_start,
            gap_fill_window_end,
            &live_shaped_boundary_segments(gap_fill_window_start, gap_fill_window_end),
            5,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: Some(gap_fill_db_path),
            gap_fill_progress_path: Some(gap_fill_progress_path),
            gap_fill_window_start_utc: Some(gap_fill_window_start),
            gap_fill_window_end_utc: Some(gap_fill_window_end),
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("trading_ready")
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_policy")
                .and_then(Value::as_str),
            Some(ACCEPTED_RESIDUE_POLICY)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_accepted_irreducible_boundary_residue")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_fetched_rows")
                .and_then(Value::as_u64),
            Some(5)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_staged_rows")
                .and_then(Value::as_u64),
            Some(5)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_rows_withheld_due_to_incomplete_outcome")
                .and_then(Value::as_u64),
            Some(5)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_start_coverage_deficit_ms")
                .and_then(Value::as_i64),
            Some(0)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_end_coverage_deficit_ms")
                .and_then(Value::as_i64),
            Some(857)
        );
        assert_eq!(
            parsed
                .pointer("/gap_fill_acceptance_total_accepted_residue_ms")
                .and_then(Value::as_i64),
            Some(2_457)
        );
        assert_eq!(
            parsed
                .pointer("/replay/gap_fill_replayed")
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn invalid_accepted_residue_gate_does_not_create_target_db() -> Result<()> {
        let temp = tempdir()?;
        let restored_db_path = temp.path().join("target").join("restored.db");
        let gap_fill_db_path = temp.path().join("gap-fill.db");
        let gap_fill_progress_path = temp.path().join("gap-fill.progress.json");
        let gap_fill_window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let gap_fill_window_end = parse_ts("2026-04-23T15:59:39Z")?;
        std::fs::write(&gap_fill_db_path, b"existing gap-fill db placeholder")?;
        let segments = format!(
            r#"[
  {{ "start": "{}", "end": "{}", "reason": "{BOUNDARY_PREFIX_REASON}" }},
  {{ "start": "2026-04-19T20:06:00Z", "end": "2026-04-19T20:06:01Z", "reason": "{ZERO_PROGRESS_ROOT_REASON}" }}
]"#,
            gap_fill_window_start.to_rfc3339(),
            (gap_fill_window_start + Duration::milliseconds(600)).to_rfc3339(),
        );
        write_accepted_residue_gap_fill_progress(
            &gap_fill_progress_path,
            gap_fill_window_start,
            gap_fill_window_end,
            &segments,
            7,
        )?;

        let error = run(Config {
            config_path: temp.path().join("missing-config.toml"),
            artifact_path: temp.path().join("missing-artifact.json"),
            db_path: Some(restored_db_path.clone()),
            journal_db_path: None,
            gap_fill_db_path: Some(gap_fill_db_path),
            gap_fill_progress_path: Some(gap_fill_progress_path),
            gap_fill_window_start_utc: Some(gap_fill_window_start),
            gap_fill_window_end_utc: Some(gap_fill_window_end),
            bootstrap_degraded: false,
            json: true,
            now: restore_test_now(),
        })
        .expect_err("invalid accepted residue must stop before target DB mutation");

        assert_reason(
            error,
            "program_history_gap_fill_restore_gate_irreducible_boundary_root_segments_present",
        );
        assert!(!restored_db_path.exists());
        assert!(!restored_db_path.parent().expect("target parent").exists());
        Ok(())
    }

    #[test]
    fn run_partial_gap_fill_keeps_restore_fail_closed() -> Result<()> {
        let fixture = make_fixture("runtime-restore-gap-fill-partial")?;
        let now = restore_test_now();
        let artifact_now = now - Duration::minutes(30);
        seed_runtime_artifact_source(&fixture.source_store, &fixture.config_path, artifact_now)?;
        let artifact = export_artifact(&fixture.source_store, &fixture.config_path, now)?;
        let artifact_path = fixture.temp.path().join("artifact-gap-fill-partial.json");
        std::fs::write(&artifact_path, serde_json::to_vec_pretty(&artifact)?)?;

        let restored_db_path = fixture.temp.path().join("restored-gap-fill-partial.db");
        let journal_db_path = fixture.temp.path().join("recent-gap-fill-partial.db");
        let gap_fill_db_path = fixture.temp.path().join("gap-fill-partial.db");
        let gap_fill_progress_path = fixture.temp.path().join("gap-fill-partial.progress.json");
        let gap_fill_window_start = now - Duration::days(7);
        let gap_fill_window_end = now;
        seed_recent_raw_journal(
            &journal_db_path,
            &[make_swap("journal-recent", now - Duration::minutes(5), 12)],
            now,
        )?;
        seed_recent_raw_journal(
            &gap_fill_db_path,
            &[make_swap(
                "gap-fill-too-new",
                now - Duration::days(6) + Duration::hours(1),
                11,
            )],
            now,
        )?;
        write_valid_gap_fill_progress(
            &gap_fill_progress_path,
            gap_fill_window_start,
            gap_fill_window_end,
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            artifact_path,
            db_path: Some(restored_db_path.clone()),
            journal_db_path: Some(journal_db_path),
            gap_fill_db_path: Some(gap_fill_db_path),
            gap_fill_progress_path: Some(gap_fill_progress_path),
            gap_fill_window_start_utc: Some(gap_fill_window_start),
            gap_fill_window_end_utc: Some(gap_fill_window_end),
            bootstrap_degraded: false,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.pointer("/verdict/verdict").and_then(Value::as_str),
            Some("fail_closed")
        );
        assert_eq!(
            parsed
                .pointer("/replay/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(false)
        );

        let restored = SqliteStore::open(Path::new(&restored_db_path))?;
        let restore_state = restored.discovery_recent_raw_restore_state()?;
        assert!(!restore_state.raw_coverage_satisfied);
        assert!(restore_state.gap_fill_replayed);
        Ok(())
    }
}

#[cfg(test)]
struct Fixture {
    source_store: SqliteStore,
    config_path: PathBuf,
    temp: tempfile::TempDir,
}

#[cfg(test)]
fn make_fixture(name: &str) -> Result<Fixture> {
    let temp = tempdir().context("failed creating tempdir")?;
    let db_path = temp.path().join(format!("{name}.db"));
    let config_path = temp.path().join(format!("{name}.toml"));
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut source_store = SqliteStore::open(&db_path)?;
    source_store.run_migrations(&migration_dir)?;
    std::fs::write(
        &config_path,
        format!(
            "[system]\nmigrations_dir = \"{}\"\n\n[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[execution]\nenabled = false\n",
            migration_dir.display(),
            db_path.display()
        ),
    )?;
    Ok(Fixture {
        source_store,
        config_path,
        temp,
    })
}

#[cfg(test)]
fn export_artifact(
    store: &SqliteStore,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    let loaded_config = copybot_config::load_from_path(config_path)?;
    let discovery = DiscoveryService::new(loaded_config.discovery.clone(), loaded_config.shadow);
    store.export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())
}

#[cfg(test)]
fn seed_runtime_artifact_source(
    store: &SqliteStore,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<()> {
    let published_window_start = now - Duration::days(7);
    let policy_fingerprint = test_publication_selection_policy_fingerprint(config_path)?;
    store.persist_discovery_cycle(
        &[WalletUpsertRow {
            wallet_id: "wallet-restore".to_string(),
            first_seen: published_window_start - Duration::days(1),
            last_seen: published_window_start + Duration::minutes(5),
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-restore".to_string(),
            window_start: published_window_start,
            pnl: 3.2,
            win_rate: 0.8,
            trades: 10,
            closed_trades: 10,
            hold_median_seconds: 120,
            score: 0.9,
            buy_total: 10,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &["wallet-restore".to_string()],
        true,
        true,
        now,
        "seed_runtime_artifact_source",
    )?;
    store.set_discovery_publication_state_with_options(
        &DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
            reason: "raw_window".to_string(),
            last_published_at: Some(now),
            last_published_window_start: Some(published_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-restore".to_string()]),
        },
        false,
        Some(&policy_fingerprint),
    )?;
    store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now,
        slot: 42,
        signature: "sig-restore".to_string(),
    })?;
    Ok(())
}

#[cfg(test)]
fn test_publication_selection_policy_fingerprint(config_path: &Path) -> Result<String> {
    let loaded_config = copybot_config::load_from_path(config_path)?;
    let config = loaded_config.discovery;
    Ok(format!(
        concat!(
            "follow_top_n={};",
            "scoring_window_days={};",
            "decay_window_days={};",
            "min_leader_notional_sol={:.6};",
            "min_trades={};",
            "min_active_days={};",
            "min_score={:.6};",
            "max_tx_per_minute={};",
            "min_buy_count={};",
            "min_tradable_ratio={:.6};",
            "require_open_positions_for_publication={};",
            "max_rug_ratio={:.6};",
            "rug_lookahead_seconds={};",
            "thin_market_min_volume_sol={:.6};",
            "thin_market_min_unique_traders={}"
        ),
        config.follow_top_n,
        config.scoring_window_days,
        config.decay_window_days,
        config.min_leader_notional_sol,
        config.min_trades,
        config.min_active_days,
        config.min_score,
        config.max_tx_per_minute,
        config.min_buy_count,
        config.min_tradable_ratio,
        config.require_open_positions_for_publication,
        config.max_rug_ratio,
        config.rug_lookahead_seconds,
        config.thin_market_min_volume_sol,
        config.thin_market_min_unique_traders,
    ))
}

#[cfg(test)]
fn make_swap(signature: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
    SwapEvent {
        wallet: "wallet-restore".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: format!("token-{signature}"),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

#[cfg(test)]
fn seed_recent_raw_journal(
    journal_db_path: &Path,
    swaps: &[SwapEvent],
    completed_at: DateTime<Utc>,
) -> Result<()> {
    let journal_store = SqliteStore::open(journal_db_path)?;
    journal_store.insert_recent_raw_journal_batch(swaps, completed_at)?;
    Ok(())
}
