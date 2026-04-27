use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    runtime_restore_ops::{artifact_latest_path, resolve_db_path, resolve_relative_to_config},
    DiscoveryService,
};
use copybot_storage::{
    DiscoveryPublicationStateRow, DiscoveryRecentRawRestoreStateRow, DiscoveryRuntimeCursor,
    SqliteStore,
};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_runtime_export_gap_fill_blocker_report --config <path> [--db-path <path>] --gap-fill-progress-path <path> --gap-fill-window-start-utc <rfc3339> --gap-fill-window-end-utc <rfc3339> [--gap-fill-db-path <path>] [--now <rfc3339>] --json";

const OPERATOR_RESTORE_STATE_MISSING: &str =
    "program_history_fresh_runtime_export_blocker_unproven_restore_state_missing";
const OPERATOR_PROGRESS_UNREADABLE: &str =
    "program_history_fresh_runtime_export_blocker_unproven_progress_unreadable";
const OPERATOR_PROGRESS_MALFORMED: &str =
    "program_history_fresh_runtime_export_blocker_unproven_progress_malformed";

const ACCEPTED_RESIDUE_POLICY: &str = "accepted_irreducible_boundary_residue";
const FULL_REPLAYABLE_POLICY: &str = "full_replayable_exact_window";
const IRREDUCIBLE_VERDICT: &str = "not_proven_due_to_irreducible_boundary_evidence";
const IRREDUCIBLE_REASON: &str =
    "program_history_gap_fill_repair_explicit_missing_segments_irreducible_boundary_evidence_remains";
const IRREDUCIBLE_PHASE: &str = "completed_with_explicit_missing_segments";
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

    let report = build_report(&config);
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .expect("fresh runtime export blocker report should serialize")
    );
    if report.operator_reason.is_some() {
        std::process::exit(1);
    }
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    gap_fill_db_path: Option<PathBuf>,
    gap_fill_progress_path: PathBuf,
    gap_fill_window_start_utc: DateTime<Utc>,
    gap_fill_window_end_utc: DateTime<Utc>,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct FreshRuntimeExportBlockerReport {
    event: &'static str,
    config_path: String,
    db_path: Option<String>,
    gap_fill_db_path: Option<String>,
    gap_fill_progress_path: String,
    gap_fill_window_start_utc: String,
    gap_fill_window_end_utc: String,
    now: String,
    publication_runtime_mode: Option<String>,
    publication_reason: Option<String>,
    publication_truth_complete: Option<bool>,
    fresh_under_export_gate: Option<bool>,
    fresh_under_current_gate: Option<bool>,
    last_published_at: Option<String>,
    last_published_window_start: Option<String>,
    publication_updated_at: Option<String>,
    recent_raw_restore_state_present: bool,
    required_window_start: Option<String>,
    effective_covered_since: Option<String>,
    effective_covered_through_cursor: Option<ReportCursor>,
    raw_coverage_satisfied: Option<bool>,
    journal_available: Option<bool>,
    journal_covers_artifact_cursor: Option<bool>,
    gap_fill_replayed: Option<bool>,
    gap_fill_replayed_rows: Option<usize>,
    recent_raw_restore_reason: Option<String>,
    recent_raw_restore_updated_at: Option<String>,
    gap_fill_artifact_valid_for_restore_review: bool,
    gap_fill_artifact_validation_reason: String,
    gap_fill_acceptance_policy: Option<String>,
    accepted_irreducible_boundary_residue: bool,
    gap_fill_requested_window_start: Option<String>,
    gap_fill_requested_window_end: Option<String>,
    gap_fill_replayable_output: Option<bool>,
    gap_fill_covered_since: Option<String>,
    gap_fill_covered_through_ts_utc: Option<String>,
    gap_fill_missing_segments_count: Option<usize>,
    gap_fill_inserted_rows: Option<usize>,
    gap_fill_staged_rows: Option<usize>,
    gap_fill_fetched_rows: Option<usize>,
    gap_fill_rows_withheld_due_to_incomplete_outcome: Option<usize>,
    runtime_db_incorporated_accepted_program_history_gap_fill_artifact: Option<bool>,
    latest_runtime_artifact_path: Option<String>,
    latest_runtime_artifact_exists: Option<bool>,
    fresh_runtime_export_ready: bool,
    accepted_residue_readiness_alone_is_fresh_export_readiness: bool,
    blocker_reason: String,
    next_safe_operator_action: String,
    production_green: bool,
    operator_reason: Option<String>,
    operator_detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReportCursor {
    ts_utc: String,
    slot: u64,
    signature: String,
}

#[derive(Debug, Clone)]
struct OperatorFailure {
    reason: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct GapFillReview {
    valid_for_restore_review: bool,
    validation_reason: String,
    acceptance_policy: Option<&'static str>,
    accepted_irreducible_boundary_residue: bool,
    snapshot: GapFillSnapshot,
}

#[derive(Debug, Clone)]
struct GapFillSnapshot {
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

#[derive(Debug, Clone)]
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
    max_boundary_segment_ms: i64,
    start_coverage_deficit_ms: Option<i64>,
    end_coverage_deficit_ms: Option<i64>,
    total_accepted_residue_ms: Option<i64>,
    max_accepted_residue_segment_or_deficit_ms: Option<i64>,
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
    let mut db_path: Option<PathBuf> = None;
    let mut gap_fill_db_path: Option<PathBuf> = None;
    let mut gap_fill_progress_path: Option<PathBuf> = None;
    let mut gap_fill_window_start_utc: Option<DateTime<Utc>> = None;
    let mut gap_fill_window_end_utc: Option<DateTime<Utc>> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
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
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    if !json {
        bail!("missing required --json\n{USAGE}");
    }
    let gap_fill_window_start_utc = gap_fill_window_start_utc
        .ok_or_else(|| anyhow!("missing --gap-fill-window-start-utc\n{USAGE}"))?;
    let gap_fill_window_end_utc = gap_fill_window_end_utc
        .ok_or_else(|| anyhow!("missing --gap-fill-window-end-utc\n{USAGE}"))?;
    if gap_fill_window_end_utc <= gap_fill_window_start_utc {
        bail!("--gap-fill-window-end-utc must be after --gap-fill-window-start-utc");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config\n{USAGE}"))?,
        db_path,
        gap_fill_db_path,
        gap_fill_progress_path: gap_fill_progress_path
            .ok_or_else(|| anyhow!("missing --gap-fill-progress-path\n{USAGE}"))?,
        gap_fill_window_start_utc,
        gap_fill_window_end_utc,
        now: now.unwrap_or_else(Utc::now),
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    parse_ts(&raw).with_context(|| format!("invalid timestamp for {flag}: {raw}"))
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn build_report(config: &Config) -> FreshRuntimeExportBlockerReport {
    match build_report_inner(config) {
        Ok(report) => report,
        Err(error) => {
            FreshRuntimeExportBlockerReport::operator_error(config, error.reason, error.detail)
        }
    }
}

fn build_report_inner(config: &Config) -> Result<FreshRuntimeExportBlockerReport, OperatorFailure> {
    let loaded_config = load_from_path(&config.config_path).map_err(|error| OperatorFailure {
        reason: "program_history_fresh_runtime_export_blocker_unproven_config_unreadable"
            .to_string(),
        detail: format!(
            "failed loading config {}: {error:#}",
            config.config_path.display()
        ),
    })?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let artifact_dir = resolve_relative_to_config(
        &config.config_path,
        Path::new(&loaded_config.runtime_restore_ops.artifact_dir),
    );
    let latest_runtime_artifact_path = artifact_latest_path(&artifact_dir);
    let store = SqliteStore::open_read_only(&db_path).map_err(|error| OperatorFailure {
        reason: "program_history_fresh_runtime_export_blocker_unproven_runtime_db_unreadable"
            .to_string(),
        detail: format!(
            "failed opening runtime db {} read-only: {error:#}",
            db_path.display()
        ),
    })?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let publication_gate = discovery.publication_freshness_gate();
    let publication_state = store
        .discovery_publication_state_read_only()
        .map_err(|error| OperatorFailure {
            reason:
                "program_history_fresh_runtime_export_blocker_unproven_publication_state_unreadable"
                    .to_string(),
            detail: format!("failed reading discovery publication state: {error:#}"),
        })?;
    let restore_state = store
        .discovery_recent_raw_restore_state_read_only()
        .map_err(|error| OperatorFailure {
            reason:
                "program_history_fresh_runtime_export_blocker_unproven_restore_state_unreadable"
                    .to_string(),
            detail: format!("failed reading recent raw restore state: {error:#}"),
        })?;
    if restore_state.updated_at.is_none() {
        return Err(OperatorFailure {
            reason: OPERATOR_RESTORE_STATE_MISSING.to_string(),
            detail:
                "runtime DB has no persisted discovery_recent_raw_restore_state row; rerun restore drill/read-only status before using this export blocker proof"
                    .to_string(),
        });
    }
    let gap_fill_review = load_gap_fill_review(config)?;

    let publication_truth_complete = publication_state
        .as_ref()
        .map(DiscoveryPublicationStateRow::has_complete_publication_truth)
        .unwrap_or(false);
    let fresh_under_export_gate = publication_state
        .as_ref()
        .is_some_and(|state| state.is_fresh_under_gate(publication_gate, config.now));
    let fresh_under_current_gate = fresh_under_export_gate;
    let runtime_db_incorporated =
        runtime_db_incorporated_gap_fill(&restore_state, &gap_fill_review);
    let fresh_runtime_export_ready = gap_fill_review.valid_for_restore_review
        && runtime_db_incorporated
        && restore_state.raw_coverage_satisfied
        && publication_truth_complete
        && fresh_under_export_gate;
    let (blocker_reason, next_safe_operator_action) = classify_blocker(
        config,
        &gap_fill_review,
        &restore_state,
        publication_state.as_ref(),
        publication_truth_complete,
        fresh_under_export_gate,
        runtime_db_incorporated,
        latest_runtime_artifact_path.exists(),
    );

    Ok(FreshRuntimeExportBlockerReport {
        event: "discovery_runtime_export_gap_fill_blocker_report",
        config_path: config.config_path.display().to_string(),
        db_path: Some(db_path.display().to_string()),
        gap_fill_db_path: config
            .gap_fill_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        gap_fill_progress_path: config.gap_fill_progress_path.display().to_string(),
        gap_fill_window_start_utc: format_ts(config.gap_fill_window_start_utc),
        gap_fill_window_end_utc: format_ts(config.gap_fill_window_end_utc),
        now: format_ts(config.now),
        publication_runtime_mode: publication_state
            .as_ref()
            .map(|state| state.runtime_mode.as_str().to_string()),
        publication_reason: publication_state.as_ref().map(|state| state.reason.clone()),
        publication_truth_complete: Some(publication_truth_complete),
        fresh_under_export_gate: Some(fresh_under_export_gate),
        fresh_under_current_gate: Some(fresh_under_current_gate),
        last_published_at: publication_state
            .as_ref()
            .and_then(|state| state.last_published_at)
            .map(format_ts),
        last_published_window_start: publication_state
            .as_ref()
            .and_then(|state| state.last_published_window_start)
            .map(format_ts),
        publication_updated_at: publication_state
            .as_ref()
            .map(|state| format_ts(state.updated_at)),
        recent_raw_restore_state_present: true,
        required_window_start: restore_state.required_window_start.map(format_ts),
        effective_covered_since: restore_state.effective_covered_since.map(format_ts),
        effective_covered_through_cursor: restore_state
            .effective_covered_through_cursor
            .as_ref()
            .map(report_cursor),
        raw_coverage_satisfied: Some(restore_state.raw_coverage_satisfied),
        journal_available: Some(restore_state.journal_available),
        journal_covers_artifact_cursor: Some(restore_state.journal_covers_artifact_cursor),
        gap_fill_replayed: Some(restore_state.gap_fill_replayed),
        gap_fill_replayed_rows: Some(restore_state.gap_fill_replayed_rows),
        recent_raw_restore_reason: restore_state.reason.clone(),
        recent_raw_restore_updated_at: restore_state.updated_at.map(format_ts),
        gap_fill_artifact_valid_for_restore_review: gap_fill_review.valid_for_restore_review,
        gap_fill_artifact_validation_reason: gap_fill_review.validation_reason.clone(),
        gap_fill_acceptance_policy: gap_fill_review.acceptance_policy.map(ToString::to_string),
        accepted_irreducible_boundary_residue: gap_fill_review
            .accepted_irreducible_boundary_residue,
        gap_fill_requested_window_start: Some(format_ts(
            gap_fill_review.snapshot.requested_window_start,
        )),
        gap_fill_requested_window_end: Some(format_ts(
            gap_fill_review.snapshot.requested_window_end,
        )),
        gap_fill_replayable_output: Some(gap_fill_review.snapshot.replayable_output),
        gap_fill_covered_since: gap_fill_review.snapshot.covered_since.map(format_ts),
        gap_fill_covered_through_ts_utc: gap_fill_review.snapshot.covered_through.map(format_ts),
        gap_fill_missing_segments_count: Some(gap_fill_review.snapshot.missing_segments.len()),
        gap_fill_inserted_rows: Some(gap_fill_review.snapshot.inserted_rows),
        gap_fill_staged_rows: gap_fill_review.snapshot.staged_rows,
        gap_fill_fetched_rows: gap_fill_review.snapshot.fetched_rows,
        gap_fill_rows_withheld_due_to_incomplete_outcome: Some(
            gap_fill_review
                .snapshot
                .rows_withheld_due_to_incomplete_outcome,
        ),
        runtime_db_incorporated_accepted_program_history_gap_fill_artifact: Some(
            runtime_db_incorporated,
        ),
        latest_runtime_artifact_path: Some(latest_runtime_artifact_path.display().to_string()),
        latest_runtime_artifact_exists: Some(latest_runtime_artifact_path.exists()),
        fresh_runtime_export_ready,
        accepted_residue_readiness_alone_is_fresh_export_readiness: false,
        blocker_reason,
        next_safe_operator_action,
        production_green: false,
        operator_reason: None,
        operator_detail: None,
    })
}

impl FreshRuntimeExportBlockerReport {
    fn operator_error(config: &Config, reason: String, detail: String) -> Self {
        Self {
            event: "discovery_runtime_export_gap_fill_blocker_report",
            config_path: config.config_path.display().to_string(),
            db_path: config.db_path.as_ref().map(|path| path.display().to_string()),
            gap_fill_db_path: config
                .gap_fill_db_path
                .as_ref()
                .map(|path| path.display().to_string()),
            gap_fill_progress_path: config.gap_fill_progress_path.display().to_string(),
            gap_fill_window_start_utc: format_ts(config.gap_fill_window_start_utc),
            gap_fill_window_end_utc: format_ts(config.gap_fill_window_end_utc),
            now: format_ts(config.now),
            publication_runtime_mode: None,
            publication_reason: None,
            publication_truth_complete: None,
            fresh_under_export_gate: None,
            fresh_under_current_gate: None,
            last_published_at: None,
            last_published_window_start: None,
            publication_updated_at: None,
            recent_raw_restore_state_present: false,
            required_window_start: None,
            effective_covered_since: None,
            effective_covered_through_cursor: None,
            raw_coverage_satisfied: None,
            journal_available: None,
            journal_covers_artifact_cursor: None,
            gap_fill_replayed: None,
            gap_fill_replayed_rows: None,
            recent_raw_restore_reason: None,
            recent_raw_restore_updated_at: None,
            gap_fill_artifact_valid_for_restore_review: false,
            gap_fill_artifact_validation_reason: "unproven_operator_error".to_string(),
            gap_fill_acceptance_policy: None,
            accepted_irreducible_boundary_residue: false,
            gap_fill_requested_window_start: None,
            gap_fill_requested_window_end: None,
            gap_fill_replayable_output: None,
            gap_fill_covered_since: None,
            gap_fill_covered_through_ts_utc: None,
            gap_fill_missing_segments_count: None,
            gap_fill_inserted_rows: None,
            gap_fill_staged_rows: None,
            gap_fill_fetched_rows: None,
            gap_fill_rows_withheld_due_to_incomplete_outcome: None,
            runtime_db_incorporated_accepted_program_history_gap_fill_artifact: None,
            latest_runtime_artifact_path: None,
            latest_runtime_artifact_exists: None,
            fresh_runtime_export_ready: false,
            accepted_residue_readiness_alone_is_fresh_export_readiness: false,
            blocker_reason: "unproven_operator_error".to_string(),
            next_safe_operator_action:
                "Fix the explicit operator error and rerun this read-only report; do not run restore/export from this incomplete proof."
                    .to_string(),
            production_green: false,
            operator_reason: Some(reason),
            operator_detail: Some(detail),
        }
    }
}

fn load_gap_fill_review(config: &Config) -> Result<GapFillReview, OperatorFailure> {
    let raw =
        fs::read_to_string(&config.gap_fill_progress_path).map_err(|error| OperatorFailure {
            reason: OPERATOR_PROGRESS_UNREADABLE.to_string(),
            detail: format!(
                "failed reading gap-fill progress {}: {error}",
                config.gap_fill_progress_path.display()
            ),
        })?;
    let value = serde_json::from_str::<Value>(&raw).map_err(|error| OperatorFailure {
        reason: OPERATOR_PROGRESS_MALFORMED.to_string(),
        detail: format!(
            "failed parsing gap-fill progress {}: {error}",
            config.gap_fill_progress_path.display()
        ),
    })?;
    let snapshot = gap_fill_snapshot_from_value(&value)?;
    let validation = gap_fill_review_validity(
        &snapshot,
        config.gap_fill_window_start_utc,
        config.gap_fill_window_end_utc,
    );
    Ok(GapFillReview {
        valid_for_restore_review: validation.0,
        validation_reason: validation.1,
        acceptance_policy: validation.2,
        accepted_irreducible_boundary_residue: validation.3,
        snapshot,
    })
}

fn gap_fill_snapshot_from_value(value: &Value) -> Result<GapFillSnapshot, OperatorFailure> {
    let verdict = required_string(value, "verdict")?;
    let reason = required_string(value, "reason")?;
    let current_phase = required_string(value, "current_phase")?;
    let replayable_output = required_bool(value, "replayable_output")?;
    let requested_window_start = required_ts(value, "requested_window_start")?;
    let requested_window_end = required_ts(value, "requested_window_end")?;
    let missing_segments = required_missing_segments(value)?;
    let inserted_rows = required_usize(value, "inserted_rows")?;
    let rows_withheld_due_to_incomplete_outcome =
        required_usize(value, "rows_withheld_due_to_incomplete_outcome")?;
    Ok(GapFillSnapshot {
        verdict,
        reason,
        current_phase,
        replayable_output,
        requested_window_start,
        requested_window_end,
        covered_since: optional_ts(value, "gap_fill_covered_since")?,
        covered_through: optional_cursor_ts(value, "gap_fill_covered_through_cursor")?,
        missing_segments,
        fetched_rows: optional_usize(value, "fetched_rows")?,
        staged_rows: optional_usize(value, "staged_rows")?,
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome,
    })
}

fn gap_fill_review_validity(
    snapshot: &GapFillSnapshot,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> (bool, String, Option<&'static str>, bool) {
    if snapshot.requested_window_start != window_start {
        return (
            false,
            "program_history_fresh_runtime_export_blocker_gap_fill_window_start_mismatch"
                .to_string(),
            None,
            false,
        );
    }
    if snapshot.requested_window_end != window_end {
        return (
            false,
            "program_history_fresh_runtime_export_blocker_gap_fill_window_end_mismatch".to_string(),
            None,
            false,
        );
    }
    if snapshot.replayable_output {
        return full_replayable_validity(snapshot, window_end);
    }
    accepted_residue_validity(snapshot)
}

fn full_replayable_validity(
    snapshot: &GapFillSnapshot,
    window_end: DateTime<Utc>,
) -> (bool, String, Option<&'static str>, bool) {
    if snapshot
        .covered_through
        .is_none_or(|covered| covered < window_end)
    {
        return (
            false,
            "program_history_fresh_runtime_export_blocker_gap_fill_full_replayable_covered_through_before_window_end"
                .to_string(),
            None,
            false,
        );
    }
    if !snapshot.missing_segments.is_empty() {
        return (
            false,
            "program_history_fresh_runtime_export_blocker_gap_fill_full_replayable_missing_segments_present"
                .to_string(),
            None,
            false,
        );
    }
    if snapshot.inserted_rows == 0 {
        return (
            false,
            "program_history_fresh_runtime_export_blocker_gap_fill_full_replayable_inserted_rows_not_positive"
                .to_string(),
            None,
            false,
        );
    }
    if snapshot.rows_withheld_due_to_incomplete_outcome != 0 {
        return (
            false,
            "program_history_fresh_runtime_export_blocker_gap_fill_full_replayable_rows_withheld_present"
                .to_string(),
            None,
            false,
        );
    }
    (
        true,
        "program_history_fresh_runtime_export_blocker_gap_fill_full_replayable_restore_review_valid"
            .to_string(),
        Some(FULL_REPLAYABLE_POLICY),
        false,
    )
}

fn accepted_residue_validity(
    snapshot: &GapFillSnapshot,
) -> (bool, String, Option<&'static str>, bool) {
    if snapshot.verdict != IRREDUCIBLE_VERDICT {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_wrong_verdict",
        );
    }
    if snapshot.reason != IRREDUCIBLE_REASON {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_wrong_reason",
        );
    }
    if snapshot.current_phase != IRREDUCIBLE_PHASE {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_wrong_current_phase",
        );
    }
    if snapshot.inserted_rows != 0 {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_inserted_rows_present",
        );
    }
    let Some(staged_rows) = snapshot.staged_rows else {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_staged_rows_missing",
        );
    };
    let Some(fetched_rows) = snapshot.fetched_rows else {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_fetched_rows_missing",
        );
    };
    if staged_rows == 0 {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_staged_rows_not_positive",
        );
    }
    if fetched_rows != staged_rows {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_fetched_rows_mismatch",
        );
    }
    if snapshot.rows_withheld_due_to_incomplete_outcome != staged_rows {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_rows_withheld_mismatch",
        );
    }
    if snapshot.covered_since.is_none() {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_missing_covered_since",
        );
    }
    if snapshot.covered_through.is_none() {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_missing_covered_through",
        );
    }
    let stats = residue_stats(
        &snapshot.missing_segments,
        snapshot.covered_since,
        snapshot.covered_through,
        snapshot.requested_window_start,
        snapshot.requested_window_end,
    );
    if stats.zero_progress_root_segments_count > 0 {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_zero_root_segments_present",
        );
    }
    if stats.unknown_non_target_segments_count > 0 {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_unknown_non_target_segments_present",
        );
    }
    if stats.target_boundary_segments_count == 0 {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_no_boundary_segments",
        );
    }
    if stats.start_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_start_deficit_too_large",
        );
    }
    if stats.end_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_end_deficit_too_large",
        );
    }
    if stats.total_accepted_residue_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_TOTAL_MS {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_total_residue_too_large",
        );
    }
    if stats
        .max_accepted_residue_segment_or_deficit_ms
        .unwrap_or(i64::MAX)
        > MAX_ACCEPTED_RESIDUE_SEGMENT_MS
    {
        return accepted_not_ready(
            "program_history_fresh_runtime_export_blocker_gap_fill_not_accepted_segment_or_deficit_too_large",
        );
    }
    (
        true,
        "program_history_fresh_runtime_export_blocker_gap_fill_accepted_irreducible_boundary_residue_restore_review_valid"
            .to_string(),
        Some(ACCEPTED_RESIDUE_POLICY),
        true,
    )
}

fn accepted_not_ready(reason: &str) -> (bool, String, Option<&'static str>, bool) {
    (false, reason.to_string(), None, false)
}

fn residue_stats(
    segments: &[MissingSegment],
    covered_since: Option<DateTime<Utc>>,
    covered_through: Option<DateTime<Utc>>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
) -> ResidueStats {
    let mut stats = ResidueStats::default();
    for segment in segments {
        if segment.reason == ZERO_PROGRESS_ROOT_REASON {
            stats.zero_progress_root_segments_count += 1;
        } else if is_boundary_reason(&segment.reason) {
            stats.target_boundary_segments_count += 1;
            let duration_ms = segment
                .end
                .signed_duration_since(segment.start)
                .num_milliseconds()
                .max(0);
            stats.total_boundary_missing_ms += duration_ms;
            stats.max_boundary_segment_ms = stats.max_boundary_segment_ms.max(duration_ms);
        } else if segment.reason != IRREDUCIBLE_REASON {
            stats.unknown_non_target_segments_count += 1;
        }
    }

    let start_deficit = covered_since.map(|covered| {
        covered
            .signed_duration_since(requested_window_start)
            .num_milliseconds()
            .max(0)
    });
    let end_deficit = covered_through.map(|covered| {
        requested_window_end
            .signed_duration_since(covered)
            .num_milliseconds()
            .max(0)
    });
    stats.start_coverage_deficit_ms = start_deficit;
    stats.end_coverage_deficit_ms = end_deficit;
    stats.total_accepted_residue_ms = start_deficit.zip(end_deficit).map(|(start, end)| {
        stats
            .total_boundary_missing_ms
            .saturating_add(start)
            .saturating_add(end)
    });
    stats.max_accepted_residue_segment_or_deficit_ms = start_deficit
        .zip(end_deficit)
        .map(|(start, end)| stats.max_boundary_segment_ms.max(start).max(end));
    stats
}

fn is_boundary_reason(reason: &str) -> bool {
    reason == BOUNDARY_PREFIX_REASON || reason == BOUNDARY_SUFFIX_REASON
}

fn runtime_db_incorporated_gap_fill(
    restore_state: &DiscoveryRecentRawRestoreStateRow,
    review: &GapFillReview,
) -> bool {
    let Some(restored_since) = restore_state.gap_fill_covered_since else {
        return false;
    };
    let Some(restored_through) = restore_state.gap_fill_covered_through_cursor.as_ref() else {
        return false;
    };
    let target_since = review
        .snapshot
        .covered_since
        .unwrap_or(review.snapshot.requested_window_start);
    let target_through = review
        .snapshot
        .covered_through
        .unwrap_or(review.snapshot.requested_window_end);
    review.valid_for_restore_review
        && restore_state.gap_fill_replayed
        && restore_state.gap_fill_replayed_rows > 0
        && restored_since <= target_since
        && restored_through.ts_utc >= target_through
}

fn classify_blocker(
    config: &Config,
    review: &GapFillReview,
    restore_state: &DiscoveryRecentRawRestoreStateRow,
    publication_state: Option<&DiscoveryPublicationStateRow>,
    publication_truth_complete: bool,
    fresh_under_export_gate: bool,
    runtime_db_incorporated: bool,
    latest_runtime_artifact_exists: bool,
) -> (String, String) {
    if !review.valid_for_restore_review {
        return (
            "accepted_program_history_gap_fill_artifact_not_restore_review_valid".to_string(),
            format!(
                "Run discovery_raw_gap_fill_program_history_artifact_validate --progress-path {} --window-start-utc {} --window-end-utc {} --json and do not run restore/export until it reports restore-review valid.",
                config.gap_fill_progress_path.display(),
                format_ts(config.gap_fill_window_start_utc),
                format_ts(config.gap_fill_window_end_utc)
            ),
        );
    }
    if !runtime_db_incorporated {
        return (
            "runtime_db_not_restored_with_gap_fill".to_string(),
            format!(
                "Run the human-approved restore drill/restore path with --gap-fill-db-path {} --gap-fill-progress-path {} --gap-fill-window-start-utc {} --gap-fill-window-end-utc {}; this report does not replay or mutate anything.",
                config
                    .gap_fill_db_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<gap-fill-db-path>".to_string()),
                config.gap_fill_progress_path.display(),
                format_ts(config.gap_fill_window_start_utc),
                format_ts(config.gap_fill_window_end_utc)
            ),
        );
    }
    if !restore_state.raw_coverage_satisfied {
        return (
            "raw_coverage_satisfied_false".to_string(),
            "Inspect discovery_recent_raw_restore_state and rerun the read-only restore preflight/drill; do not export until raw_coverage_satisfied=true is persisted."
                .to_string(),
        );
    }
    if !publication_truth_complete {
        return (
            "export_gate_requires_publication_refresh".to_string(),
            "Run a normal discovery publication refresh cycle, then rerun this read-only report before scheduled export."
                .to_string(),
        );
    }
    if !fresh_under_export_gate {
        let blocker = if publication_state
            .and_then(|state| state.last_published_at)
            .is_some()
        {
            "publication_truth_stale_after_restore"
        } else {
            "export_gate_requires_publication_refresh"
        };
        return (
            blocker.to_string(),
            "Run a normal discovery publication refresh cycle so publication truth is fresh under the export gate, then rerun discovery_runtime_export --scheduled."
                .to_string(),
        );
    }
    if !latest_runtime_artifact_exists {
        return (
            "fresh_runtime_artifact_missing".to_string(),
            "Run discovery_runtime_export --scheduled after the read-only report confirms the publication truth is fresh; this report does not create artifacts."
                .to_string(),
        );
    }
    (
        "no_export_blocker_proven_from_read_only_state".to_string(),
        "Rerun discovery_runtime_export --scheduled; this read-only report sees no persisted state blocker."
            .to_string(),
    )
}

fn required_missing_segments(value: &Value) -> Result<Vec<MissingSegment>, OperatorFailure> {
    let segments = value
        .get("missing_segments")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("missing_segments", "missing or non-array value"))?;
    segments
        .iter()
        .enumerate()
        .map(|(index, segment)| missing_segment_from_value(index, segment))
        .collect()
}

fn missing_segment_from_value(
    index: usize,
    value: &Value,
) -> Result<MissingSegment, OperatorFailure> {
    let start = required_segment_ts(value, index, "start")?;
    let end = required_segment_ts(value, index, "end")?;
    if end < start {
        return Err(malformed(
            "missing_segments.duration",
            format!("segment {index} end is before start"),
        ));
    }
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            malformed(
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
) -> Result<DateTime<Utc>, OperatorFailure> {
    let raw = value.get(field).and_then(Value::as_str).ok_or_else(|| {
        malformed(
            "missing_segments.timestamp",
            format!("segment {index} missing or non-string {field}"),
        )
    })?;
    parse_ts(raw).map_err(|error| {
        malformed(
            "missing_segments.timestamp",
            format!("segment {index} invalid {field}: {error}"),
        )
    })
}

fn required_string(value: &Value, field: &'static str) -> Result<String, OperatorFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| malformed(field, "missing or non-string value"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool, OperatorFailure> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| malformed(field, "missing or non-bool value"))
}

fn required_usize(value: &Value, field: &'static str) -> Result<usize, OperatorFailure> {
    let raw = value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| malformed(field, "missing or non-u64 value"))?;
    usize::try_from(raw).map_err(|error| malformed(field, format!("usize overflow: {error}")))
}

fn optional_usize(value: &Value, field: &'static str) -> Result<Option<usize>, OperatorFailure> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let raw = value
                .as_u64()
                .ok_or_else(|| malformed(field, "non-u64 value"))?;
            usize::try_from(raw)
                .map(Some)
                .map_err(|error| malformed(field, format!("usize overflow: {error}")))
        }
    }
}

fn required_ts(value: &Value, field: &'static str) -> Result<DateTime<Utc>, OperatorFailure> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).map_err(|error| malformed(field, error))
}

fn optional_ts(
    value: &Value,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, OperatorFailure> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| malformed(field, error))
}

fn optional_cursor_ts(
    value: &Value,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, OperatorFailure> {
    value
        .get(field)
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .map_err(|error| malformed("gap_fill_covered_through_cursor.ts_utc", error))
}

fn malformed(field: &str, detail: impl std::fmt::Display) -> OperatorFailure {
    OperatorFailure {
        reason: OPERATOR_PROGRESS_MALFORMED.to_string(),
        detail: format!("{field}: {detail}"),
    }
}

fn report_cursor(cursor: &DiscoveryRuntimeCursor) -> ReportCursor {
    ReportCursor {
        ts_utc: format_ts(cursor.ts_utc),
        slot: cursor.slot,
        signature: cursor.signature.clone(),
    }
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRecentRawRestoreStateUpdate, DiscoveryRuntimeMode,
    };
    use tempfile::tempdir;

    struct Fixture {
        config: Config,
        store: SqliteStore,
        _temp: tempfile::TempDir,
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir()?;
        let db_path = temp.path().join(format!("{name}.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let progress_path = temp.path().join(format!("{name}.progress.json"));
        let gap_fill_db_path = temp.path().join(format!("{name}.gap-fill.db"));
        let mut store = SqliteStore::open(&db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[runtime_restore_ops]\nartifact_dir = \"{}\"\nartifact_retention = 2\nartifact_cadence_minutes = 10\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display(),
                temp.path().join("artifacts").display()
            ),
        )?;
        write_accepted_residue_progress(
            &progress_path,
            parse_ts("2026-04-18T16:56:04Z")?,
            parse_ts("2026-04-23T15:59:39.857Z")?,
            7,
        )?;
        let config = Config {
            config_path,
            db_path: Some(db_path.clone()),
            gap_fill_db_path: Some(gap_fill_db_path),
            gap_fill_progress_path: progress_path.clone(),
            gap_fill_window_start_utc: parse_ts("2026-04-18T16:56:04Z")?,
            gap_fill_window_end_utc: parse_ts("2026-04-23T15:59:39.857Z")?,
            now: parse_ts("2026-04-23T16:05:00Z")?,
        };
        seed_publication(
            &store,
            parse_ts("2026-04-23T16:00:00Z")?,
            parse_ts("2026-04-16T16:00:00Z")?,
        )?;
        seed_runtime_cursor(&store, parse_ts("2026-04-23T15:59:39Z")?)?;
        seed_restore_state(
            &store,
            RestoreSeed {
                gap_fill_replayed: false,
                gap_fill_replayed_rows: 0,
                raw_coverage_satisfied: false,
                reason: "recent_raw_restore_not_yet_replayed",
            },
        )?;
        Ok(Fixture {
            config,
            store,
            _temp: temp,
        })
    }

    struct RestoreSeed {
        gap_fill_replayed: bool,
        gap_fill_replayed_rows: usize,
        raw_coverage_satisfied: bool,
        reason: &'static str,
    }

    fn seed_publication(
        store: &SqliteStore,
        last_published_at: DateTime<Utc>,
        window_start: DateTime<Utc>,
    ) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window".to_string(),
            last_published_at: Some(last_published_at),
            last_published_window_start: Some(window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
        })
    }

    fn seed_runtime_cursor(store: &SqliteStore, ts: DateTime<Utc>) -> Result<()> {
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: ts,
            slot: ts.timestamp().max(0) as u64,
            signature: "runtime-cursor".to_string(),
        })
    }

    fn seed_restore_state(store: &SqliteStore, seed: RestoreSeed) -> Result<()> {
        let required_window_start = parse_ts("2026-04-18T16:56:04Z")?;
        let covered_through = DiscoveryRuntimeCursor {
            ts_utc: parse_ts("2026-04-23T15:59:39Z")?,
            slot: 42,
            signature: "covered-through".to_string(),
        };
        store.set_discovery_recent_raw_restore_state(&DiscoveryRecentRawRestoreStateUpdate {
            journal_available: true,
            journal_replayed: true,
            required_window_start: Some(required_window_start),
            journal_covered_since: Some(required_window_start - Duration::seconds(30)),
            journal_covered_through_cursor: Some(covered_through.clone()),
            gap_fill_replayed: seed.gap_fill_replayed,
            gap_fill_covered_since: seed.gap_fill_replayed.then_some(required_window_start),
            gap_fill_covered_through_cursor: seed
                .gap_fill_replayed
                .then_some(covered_through.clone()),
            effective_covered_since: Some(required_window_start),
            effective_covered_through_cursor: Some(covered_through),
            artifact_runtime_cursor: Some(DiscoveryRuntimeCursor {
                ts_utc: parse_ts("2026-04-23T15:59:39Z")?,
                slot: 43,
                signature: "artifact-cursor".to_string(),
            }),
            journal_covers_artifact_cursor: true,
            raw_coverage_satisfied: seed.raw_coverage_satisfied,
            gap_fill_replayed_rows: seed.gap_fill_replayed_rows,
            replayed_rows: seed.gap_fill_replayed_rows,
            reason: Some(seed.reason.to_string()),
            replay_started_at: Some(parse_ts("2026-04-23T16:01:00Z")?),
            replay_completed_at: Some(parse_ts("2026-04-23T16:02:00Z")?),
        })
    }

    fn write_accepted_residue_progress(
        path: &Path,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        staged_rows: usize,
    ) -> Result<()> {
        let covered_through = window_end - Duration::milliseconds(857);
        let value = serde_json::json!({
            "verdict": IRREDUCIBLE_VERDICT,
            "reason": IRREDUCIBLE_REASON,
            "current_phase": IRREDUCIBLE_PHASE,
            "replayable_output": false,
            "requested_window_start": format_ts(window_start),
            "requested_window_end": format_ts(window_end),
            "gap_fill_covered_since": format_ts(window_start),
            "gap_fill_covered_through_cursor": {
                "ts_utc": format_ts(covered_through),
                "slot": 414,
                "signature": "covered-through"
            },
            "missing_segments": [
                {
                    "start": "2026-04-19T20:06:00Z",
                    "end": "2026-04-19T20:06:01Z",
                    "reason": BOUNDARY_PREFIX_REASON
                },
                {
                    "start": "2026-04-19T20:08:00Z",
                    "end": "2026-04-19T20:08:00.600Z",
                    "reason": BOUNDARY_SUFFIX_REASON
                },
                {
                    "start": format_ts(window_start),
                    "end": format_ts(window_end),
                    "reason": IRREDUCIBLE_REASON
                }
            ],
            "inserted_rows": 0,
            "staged_rows": staged_rows,
            "fetched_rows": staged_rows,
            "rows_withheld_due_to_incomplete_outcome": staged_rows
        });
        fs::write(path, serde_json::to_vec_pretty(&value)?)?;
        Ok(())
    }

    #[test]
    fn accepted_gap_fill_exists_but_runtime_db_restore_state_has_not_replayed_it() -> Result<()> {
        let fixture = make_fixture("gap-fill-not-replayed")?;
        let report = build_report(&fixture.config);
        assert!(report.gap_fill_artifact_valid_for_restore_review);
        assert!(report.accepted_irreducible_boundary_residue);
        assert_eq!(report.gap_fill_replayed, Some(false));
        assert_eq!(
            report.runtime_db_incorporated_accepted_program_history_gap_fill_artifact,
            Some(false)
        );
        assert_eq!(
            report.blocker_reason,
            "runtime_db_not_restored_with_gap_fill"
        );
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn raw_coverage_satisfied_false_blocks_export_after_gap_fill_replay() -> Result<()> {
        let fixture = make_fixture("raw-coverage-false")?;
        seed_restore_state(
            &fixture.store,
            RestoreSeed {
                gap_fill_replayed: true,
                gap_fill_replayed_rows: 7,
                raw_coverage_satisfied: false,
                reason: "recent_raw_restore_gap_fill_replayed_raw_coverage_unsatisfied",
            },
        )?;

        let report = build_report(&fixture.config);
        assert_eq!(
            report.runtime_db_incorporated_accepted_program_history_gap_fill_artifact,
            Some(true)
        );
        assert_eq!(report.raw_coverage_satisfied, Some(false));
        assert_eq!(report.blocker_reason, "raw_coverage_satisfied_false");
        assert!(!report.fresh_runtime_export_ready);
        Ok(())
    }

    #[test]
    fn stale_last_published_at_reports_stale_publication_truth_after_restore() -> Result<()> {
        let fixture = make_fixture("stale-publication-after-restore")?;
        seed_publication(
            &fixture.store,
            parse_ts("2026-04-06T17:55:23Z")?,
            parse_ts("2026-04-16T16:00:00Z")?,
        )?;
        seed_restore_state(
            &fixture.store,
            RestoreSeed {
                gap_fill_replayed: true,
                gap_fill_replayed_rows: 7,
                raw_coverage_satisfied: true,
                reason: "recent_raw_restore_gap_fill_replayed_coverage_satisfied",
            },
        )?;

        let report = build_report(&fixture.config);
        assert_eq!(report.publication_truth_complete, Some(true));
        assert_eq!(report.fresh_under_export_gate, Some(false));
        assert_eq!(
            report.blocker_reason,
            "publication_truth_stale_after_restore"
        );
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn operator_never_reports_production_green() -> Result<()> {
        let fixture = make_fixture("never-production-green")?;
        seed_restore_state(
            &fixture.store,
            RestoreSeed {
                gap_fill_replayed: true,
                gap_fill_replayed_rows: 7,
                raw_coverage_satisfied: true,
                reason: "recent_raw_restore_gap_fill_replayed_coverage_satisfied",
            },
        )?;

        let report = build_report(&fixture.config);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn missing_restore_state_fails_closed_with_explicit_reason() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("missing-restore-state.db");
        let config_path = temp.path().join("missing-restore-state.toml");
        let progress_path = temp.path().join("missing-restore-state.progress.json");
        let mut store = SqliteStore::open(&db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )?;
        write_accepted_residue_progress(
            &progress_path,
            parse_ts("2026-04-18T16:56:04Z")?,
            parse_ts("2026-04-23T15:59:39.857Z")?,
            7,
        )?;
        seed_publication(
            &store,
            parse_ts("2026-04-23T16:00:00Z")?,
            parse_ts("2026-04-16T16:00:00Z")?,
        )?;

        let report = build_report(&Config {
            config_path,
            db_path: Some(db_path),
            gap_fill_db_path: None,
            gap_fill_progress_path: progress_path,
            gap_fill_window_start_utc: parse_ts("2026-04-18T16:56:04Z")?,
            gap_fill_window_end_utc: parse_ts("2026-04-23T15:59:39.857Z")?,
            now: parse_ts("2026-04-23T16:05:00Z")?,
        });

        assert_eq!(
            report.operator_reason.as_deref(),
            Some(OPERATOR_RESTORE_STATE_MISSING)
        );
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn accepted_residue_readiness_alone_is_not_fresh_export_readiness() -> Result<()> {
        let fixture = make_fixture("accepted-alone-not-export-ready")?;
        let report = build_report(&fixture.config);
        assert!(report.gap_fill_artifact_valid_for_restore_review);
        assert!(report.accepted_irreducible_boundary_residue);
        assert!(!report.accepted_residue_readiness_alone_is_fresh_export_readiness);
        assert!(!report.fresh_runtime_export_ready);
        assert_eq!(
            report.blocker_reason,
            "runtime_db_not_restored_with_gap_fill"
        );
        Ok(())
    }
}
