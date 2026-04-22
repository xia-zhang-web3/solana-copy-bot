use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::DiscoveryService;
use copybot_storage::{DiscoveryPersistedRebuildPhase, SqliteStore};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_replay_sol_leg_update_path_audit --db <path> --config <path> [--json]";

const CONCLUSION_PARTIAL_CHECKPOINT_UPDATES_BEFORE_COMPLETION: &str =
    "replay_progress_can_stop_after_partial_checkpoint_updates_before_completion";
const CONCLUSION_REPLAY_PHASE_PERSISTS_ACROSS_RUNS: &str =
    "replay_progress_can_persist_in_replay_phase_across_runs_without_publishable_transition";
const CONCLUSION_ROOT_CAUSE_REQUIRES_LIVE_RUNTIME_EVIDENCE: &str =
    "replay_progress_stop_root_cause_still_requires_live_runtime_evidence";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const REPLAY_LOOP_SURFACE: &str =
    "copybot_discovery::DiscoveryService::advance_persisted_stream_rebuild_with_phase_page_limits";
const REPLAY_CHUNK_ITERATION_SURFACE: &str =
    "copybot_discovery::DiscoveryService::advance_persisted_stream_replay_with_phase_page_limits";
const REPLAY_PROGRESS_PERSIST_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persist_persisted_stream_rebuild_state";
const REPLAY_COMPLETION_TRANSITION_SURFACE: &str =
    "copybot_discovery::DiscoveryService::advance_persisted_stream_rebuild_with_phase_page_limits";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!("{}", render_output(&report, config.json)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct ReplaySolLegUpdatePathAuditReport {
    replay_state_present: bool,
    replay_phase: Option<String>,
    replay_started_at_utc: Option<DateTime<Utc>>,
    replay_updated_at_utc: Option<DateTime<Utc>>,
    replay_rows_processed: Option<u64>,
    replay_pages_processed: Option<u64>,
    replay_chunks_completed: Option<u64>,
    replay_cursor_ts_utc: Option<DateTime<Utc>>,
    replay_cursor_slot: Option<u64>,
    replay_cursor_signature: Option<String>,
    replay_publishable: bool,
    replay_completed: bool,
    replay_updated_before_db_mtime: Option<bool>,
    replay_updated_lag_seconds_vs_db_mtime: Option<i64>,
    replay_updated_lag_days_vs_db_mtime: Option<i64>,
    replay_has_partial_progress: bool,
    replay_stopped_mid_replay_phase: bool,
    replay_cursor_present: bool,
    replay_loop_surface: String,
    replay_chunk_iteration_surface: String,
    replay_progress_persist_surface: String,
    replay_completion_transition_surface: String,
    replay_error_or_early_return_surface_present: bool,
    replay_progress_persist_not_guaranteed_on_all_paths: bool,
    replay_can_remain_in_replay_phase_after_partial_progress: bool,
    replay_publishable_depends_on_completion_transition: bool,
    code_supports_partial_progress_without_completion: bool,
    code_supports_replay_phase_persisting_across_runs: bool,
    code_supports_early_stop_before_completion_without_clearing_checkpoint: bool,
    code_supports_completion_only_after_cursor_reaches_target_frontier: bool,
    replay_sol_leg_update_path_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct ReplayStateFacts {
    state_present: bool,
    phase: Option<String>,
    started_at_utc: Option<DateTime<Utc>>,
    updated_at_utc: Option<DateTime<Utc>>,
    rows_processed: Option<u64>,
    pages_processed: Option<u64>,
    chunks_completed: Option<u64>,
    cursor_ts_utc: Option<DateTime<Utc>>,
    cursor_slot: Option<u64>,
    cursor_signature: Option<String>,
    publishable: bool,
    completed: bool,
    cursor_present: bool,
}

#[derive(Debug, Clone, Default)]
struct StopShapeFacts {
    updated_before_db_mtime: Option<bool>,
    updated_lag_seconds_vs_db_mtime: Option<i64>,
    updated_lag_days_vs_db_mtime: Option<i64>,
    has_partial_progress: bool,
    stopped_mid_replay_phase: bool,
    cursor_present: bool,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    replay_error_or_early_return_surface_present: bool,
    replay_progress_persist_not_guaranteed_on_all_paths: bool,
    replay_can_remain_in_replay_phase_after_partial_progress: bool,
    replay_publishable_depends_on_completion_transition: bool,
    code_supports_partial_progress_without_completion: bool,
    code_supports_replay_phase_persisting_across_runs: bool,
    code_supports_early_stop_before_completion_without_clearing_checkpoint: bool,
    code_supports_completion_only_after_cursor_reaches_target_frontier: bool,
}

#[derive(Debug, Clone)]
struct DbMetadataFacts {
    file_mtime_utc: DateTime<Utc>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut db_path: Option<PathBuf> = None;
    let mut config_path: Option<PathBuf> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db_path = Some(PathBuf::from(parse_string_arg("--db", args.next())?)),
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db"))?,
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        json,
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

fn run(config: &Config) -> Result<ReplaySolLegUpdatePathAuditReport> {
    let _loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let db_metadata = load_db_metadata(&config.db_path)?;
    let replay_state = load_replay_state_facts(&store)?;
    let stop_shape = compute_stop_shape_facts(&replay_state, db_metadata.file_mtime_utc);
    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(&replay_state, &stop_shape, &code_paths);

    Ok(ReplaySolLegUpdatePathAuditReport {
        replay_state_present: replay_state.state_present,
        replay_phase: replay_state.phase,
        replay_started_at_utc: replay_state.started_at_utc,
        replay_updated_at_utc: replay_state.updated_at_utc,
        replay_rows_processed: replay_state.rows_processed,
        replay_pages_processed: replay_state.pages_processed,
        replay_chunks_completed: replay_state.chunks_completed,
        replay_cursor_ts_utc: replay_state.cursor_ts_utc,
        replay_cursor_slot: replay_state.cursor_slot,
        replay_cursor_signature: replay_state.cursor_signature,
        replay_publishable: replay_state.publishable,
        replay_completed: replay_state.completed,
        replay_updated_before_db_mtime: stop_shape.updated_before_db_mtime,
        replay_updated_lag_seconds_vs_db_mtime: stop_shape.updated_lag_seconds_vs_db_mtime,
        replay_updated_lag_days_vs_db_mtime: stop_shape.updated_lag_days_vs_db_mtime,
        replay_has_partial_progress: stop_shape.has_partial_progress,
        replay_stopped_mid_replay_phase: stop_shape.stopped_mid_replay_phase,
        replay_cursor_present: stop_shape.cursor_present,
        replay_loop_surface: REPLAY_LOOP_SURFACE.to_string(),
        replay_chunk_iteration_surface: REPLAY_CHUNK_ITERATION_SURFACE.to_string(),
        replay_progress_persist_surface: REPLAY_PROGRESS_PERSIST_SURFACE.to_string(),
        replay_completion_transition_surface: REPLAY_COMPLETION_TRANSITION_SURFACE.to_string(),
        replay_error_or_early_return_surface_present: code_paths
            .replay_error_or_early_return_surface_present,
        replay_progress_persist_not_guaranteed_on_all_paths: code_paths
            .replay_progress_persist_not_guaranteed_on_all_paths,
        replay_can_remain_in_replay_phase_after_partial_progress: code_paths
            .replay_can_remain_in_replay_phase_after_partial_progress,
        replay_publishable_depends_on_completion_transition: code_paths
            .replay_publishable_depends_on_completion_transition,
        code_supports_partial_progress_without_completion: code_paths
            .code_supports_partial_progress_without_completion,
        code_supports_replay_phase_persisting_across_runs: code_paths
            .code_supports_replay_phase_persisting_across_runs,
        code_supports_early_stop_before_completion_without_clearing_checkpoint: code_paths
            .code_supports_early_stop_before_completion_without_clearing_checkpoint,
        code_supports_completion_only_after_cursor_reaches_target_frontier: code_paths
            .code_supports_completion_only_after_cursor_reaches_target_frontier,
        replay_sol_leg_update_path_conclusion: conclusion.to_string(),
    })
}

fn load_db_metadata(path: &Path) -> Result<DbMetadataFacts> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed reading db metadata for {}", path.display()))?;
    let file_mtime_utc: DateTime<Utc> = metadata
        .modified()
        .map(DateTime::<Utc>::from)
        .with_context(|| format!("failed reading db mtime for {}", path.display()))?;
    Ok(DbMetadataFacts { file_mtime_utc })
}

fn load_replay_state_facts(store: &SqliteStore) -> Result<ReplayStateFacts> {
    let inspection = DiscoveryService::inspect_persisted_rebuild_state_read_only(store)?;
    let raw_row = store.load_discovery_persisted_rebuild_state_read_only()?;
    if !inspection.persisted_rebuild_checkpoint_exists {
        return Ok(ReplayStateFacts::default());
    }

    let started_at_utc = raw_row.as_ref().map(|row| row.started_at);
    let updated_at_utc = raw_row
        .as_ref()
        .map(|row| row.updated_at)
        .or(inspection.updated_at);
    let cursor = raw_row
        .as_ref()
        .and_then(|row| row.phase_cursor.clone())
        .or_else(|| inspection.phase_cursor.clone());
    let rows_processed = raw_row
        .as_ref()
        .map(|row| row.replay_rows_processed as u64)
        .or_else(|| inspection.replay_rows_processed.map(|value| value as u64));
    let pages_processed = raw_row
        .as_ref()
        .map(|row| row.replay_pages_processed as u64)
        .or_else(|| inspection.replay_pages_processed.map(|value| value as u64));
    let chunks_completed = raw_row.as_ref().map(|row| row.chunks_completed as u64);
    let publishable = inspection.publishable_checkpoint_blocker.is_none();
    let completed = !inspection.replay_incomplete;

    Ok(ReplayStateFacts {
        state_present: true,
        phase: inspection.rebuild_phase,
        started_at_utc,
        updated_at_utc,
        rows_processed,
        pages_processed,
        chunks_completed,
        cursor_ts_utc: cursor.as_ref().map(|cursor| cursor.ts_utc),
        cursor_slot: cursor.as_ref().map(|cursor| cursor.slot),
        cursor_signature: cursor.as_ref().map(|cursor| cursor.signature.clone()),
        publishable,
        completed,
        cursor_present: cursor.is_some(),
    })
}

fn compute_stop_shape_facts(
    replay_state: &ReplayStateFacts,
    db_file_mtime_utc: DateTime<Utc>,
) -> StopShapeFacts {
    let updated_before_db_mtime = replay_state
        .updated_at_utc
        .map(|updated_at| updated_at < db_file_mtime_utc);
    let updated_lag_seconds_vs_db_mtime = replay_state
        .updated_at_utc
        .map(|updated_at| (db_file_mtime_utc - updated_at).num_seconds());
    let updated_lag_days_vs_db_mtime =
        updated_lag_seconds_vs_db_mtime.map(|seconds| seconds.div_euclid(86_400));
    let has_partial_progress = replay_state.state_present
        && !replay_state.completed
        && (replay_state.rows_processed.unwrap_or(0) > 0
            || replay_state.pages_processed.unwrap_or(0) > 0
            || replay_state.chunks_completed.unwrap_or(0) > 0
            || replay_state.cursor_present);
    let stopped_mid_replay_phase = replay_state.state_present
        && replay_state.phase.as_deref() == Some(DiscoveryPersistedRebuildPhase::Replay.as_str())
        && has_partial_progress;

    StopShapeFacts {
        updated_before_db_mtime,
        updated_lag_seconds_vs_db_mtime,
        updated_lag_days_vs_db_mtime,
        has_partial_progress,
        stopped_mid_replay_phase,
        cursor_present: replay_state.cursor_present,
    }
}

fn inspect_code_path_facts() -> CodePathFacts {
    let replay_error_or_early_return_surface_present = DISCOVERY_SOURCE.contains("return Ok((")
        && DISCOVERY_SOURCE.contains("PersistedStreamRebuildAdvanceOutcome::Completed");
    let replay_progress_persist_not_guaranteed_on_all_paths = DISCOVERY_SOURCE.contains(
        "if repaired_for_resume || exact_target_surface_repair.completed {",
    ) && DISCOVERY_SOURCE.contains(
        "self.persist_persisted_stream_rebuild_state(store, &mut state, now)?;",
    ) && DISCOVERY_SOURCE.contains("PersistedStreamRebuildRestoreOutcome::ResumedExisting")
        && DISCOVERY_SOURCE.contains("return Ok((");
    let replay_can_remain_in_replay_phase_after_partial_progress =
        DISCOVERY_SOURCE.contains("PersistedStreamRebuildRestoreOutcome::ResumedExisting")
            && DISCOVERY_SOURCE.contains(
                "rebuild_replay_subphase = Self::replay_subphase(",
            )
            && DISCOVERY_SOURCE.contains(
                "state.phase == DiscoveryPersistedRebuildPhase::Replay",
            );
    let replay_publishable_depends_on_completion_transition = DISCOVERY_SOURCE
        .contains("fn persisted_stream_publishable_checkpoint_blocker_from_state(")
        && DISCOVERY_SOURCE.contains("Some(\"sol_leg\") => \"replay_sol_leg_incomplete\"")
        && DISCOVERY_SOURCE.contains(
            "state.phase = DiscoveryPersistedRebuildPhase::PublishPending;",
        );
    let code_supports_partial_progress_without_completion = ordered_contains(
        DISCOVERY_SOURCE,
        "state.chunks_completed = state.chunks_completed.saturating_add(1);",
        "self.persist_persisted_stream_rebuild_state(store, &mut state, now)?;",
    ) && DISCOVERY_SOURCE.contains("partial: true,")
        && DISCOVERY_SOURCE.contains("completed: false,")
        && DISCOVERY_SOURCE.contains(
            "Ok(PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry })",
        );
    let code_supports_replay_phase_persisting_across_runs = DISCOVERY_SOURCE.contains(
        "fn load_or_start_persisted_stream_rebuild_state_with_options(",
    ) && DISCOVERY_SOURCE.contains("PersistedStreamRebuildRestoreOutcome::ResumedExisting")
        && DISCOVERY_SOURCE.contains("rebuild_phase = Some(state.phase.as_str())");
    let code_supports_early_stop_before_completion_without_clearing_checkpoint = slice_between(
        DISCOVERY_SOURCE,
        "state.chunks_completed = state.chunks_completed.saturating_add(1);",
        "Ok(PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry })",
    )
    .is_some_and(|between| {
        between.contains("self.persist_persisted_stream_rebuild_state(store, &mut state, now)?;")
            && !between.contains("clear_discovery_persisted_rebuild_state")
    }) && DISCOVERY_SOURCE.contains(
        "yielding bounded discovery persisted observed_swaps rebuild back to scheduler",
    );
    let code_supports_completion_only_after_cursor_reaches_target_frontier =
        DISCOVERY_SOURCE.contains("Some(\"sol_leg_swap_source_exhaustion\")")
            && DISCOVERY_SOURCE.contains("source_rows_exist_beyond_stored_replay_checkpoint")
            && DISCOVERY_SOURCE.contains(
                "persisted replay checkpoint already has an exact target surface, but the source still has {count} exact-target SOL-leg rows beyond the stored replay cursor within the frozen replay horizon",
            );

    CodePathFacts {
        replay_error_or_early_return_surface_present,
        replay_progress_persist_not_guaranteed_on_all_paths,
        replay_can_remain_in_replay_phase_after_partial_progress,
        replay_publishable_depends_on_completion_transition,
        code_supports_partial_progress_without_completion,
        code_supports_replay_phase_persisting_across_runs,
        code_supports_early_stop_before_completion_without_clearing_checkpoint,
        code_supports_completion_only_after_cursor_reaches_target_frontier,
    }
}

fn ordered_contains(haystack: &str, first: &str, second: &str) -> bool {
    let Some(first_index) = haystack.find(first) else {
        return false;
    };
    haystack[first_index + first.len()..].contains(second)
}

fn slice_between<'a>(haystack: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let start_index = haystack.find(start)?;
    let remainder = &haystack[start_index + start.len()..];
    let end_index = remainder.find(end)?;
    Some(&remainder[..end_index])
}

fn choose_conclusion(
    replay_state: &ReplayStateFacts,
    stop_shape: &StopShapeFacts,
    code_paths: &CodePathFacts,
) -> &'static str {
    if stop_shape.has_partial_progress
        && stop_shape.stopped_mid_replay_phase
        && stop_shape.updated_before_db_mtime == Some(true)
        && code_paths.code_supports_replay_phase_persisting_across_runs
        && code_paths.replay_can_remain_in_replay_phase_after_partial_progress
        && code_paths.replay_publishable_depends_on_completion_transition
    {
        return CONCLUSION_REPLAY_PHASE_PERSISTS_ACROSS_RUNS;
    }

    if stop_shape.has_partial_progress
        && code_paths.code_supports_partial_progress_without_completion
        && code_paths.code_supports_early_stop_before_completion_without_clearing_checkpoint
    {
        return CONCLUSION_PARTIAL_CHECKPOINT_UPDATES_BEFORE_COMPLETION;
    }

    if replay_state.state_present
        && (stop_shape.has_partial_progress || stop_shape.stopped_mid_replay_phase)
    {
        return CONCLUSION_ROOT_CAUSE_REQUIRES_LIVE_RUNTIME_EVIDENCE;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &ReplaySolLegUpdatePathAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed to serialize replay sol leg update path audit report");
    }

    Ok(format!(
        concat!(
            "replay_state_present={replay_state_present}\n",
            "replay_phase={replay_phase}\n",
            "replay_started_at_utc={replay_started_at_utc}\n",
            "replay_updated_at_utc={replay_updated_at_utc}\n",
            "replay_rows_processed={replay_rows_processed}\n",
            "replay_pages_processed={replay_pages_processed}\n",
            "replay_chunks_completed={replay_chunks_completed}\n",
            "replay_cursor_ts_utc={replay_cursor_ts_utc}\n",
            "replay_cursor_slot={replay_cursor_slot}\n",
            "replay_cursor_signature={replay_cursor_signature}\n",
            "replay_publishable={replay_publishable}\n",
            "replay_completed={replay_completed}\n",
            "replay_updated_before_db_mtime={replay_updated_before_db_mtime}\n",
            "replay_updated_lag_seconds_vs_db_mtime={replay_updated_lag_seconds_vs_db_mtime}\n",
            "replay_updated_lag_days_vs_db_mtime={replay_updated_lag_days_vs_db_mtime}\n",
            "replay_has_partial_progress={replay_has_partial_progress}\n",
            "replay_stopped_mid_replay_phase={replay_stopped_mid_replay_phase}\n",
            "replay_cursor_present={replay_cursor_present}\n",
            "replay_loop_surface={replay_loop_surface}\n",
            "replay_chunk_iteration_surface={replay_chunk_iteration_surface}\n",
            "replay_progress_persist_surface={replay_progress_persist_surface}\n",
            "replay_completion_transition_surface={replay_completion_transition_surface}\n",
            "replay_error_or_early_return_surface_present={replay_error_or_early_return_surface_present}\n",
            "replay_progress_persist_not_guaranteed_on_all_paths={replay_progress_persist_not_guaranteed_on_all_paths}\n",
            "replay_can_remain_in_replay_phase_after_partial_progress={replay_can_remain_in_replay_phase_after_partial_progress}\n",
            "replay_publishable_depends_on_completion_transition={replay_publishable_depends_on_completion_transition}\n",
            "code_supports_partial_progress_without_completion={code_supports_partial_progress_without_completion}\n",
            "code_supports_replay_phase_persisting_across_runs={code_supports_replay_phase_persisting_across_runs}\n",
            "code_supports_early_stop_before_completion_without_clearing_checkpoint={code_supports_early_stop_before_completion_without_clearing_checkpoint}\n",
            "code_supports_completion_only_after_cursor_reaches_target_frontier={code_supports_completion_only_after_cursor_reaches_target_frontier}\n",
            "replay_sol_leg_update_path_conclusion={replay_sol_leg_update_path_conclusion}\n",
        ),
        replay_state_present = report.replay_state_present,
        replay_phase = format_optional_str(report.replay_phase.as_deref()),
        replay_started_at_utc = format_optional_ts(report.replay_started_at_utc),
        replay_updated_at_utc = format_optional_ts(report.replay_updated_at_utc),
        replay_rows_processed = format_optional_u64(report.replay_rows_processed),
        replay_pages_processed = format_optional_u64(report.replay_pages_processed),
        replay_chunks_completed = format_optional_u64(report.replay_chunks_completed),
        replay_cursor_ts_utc = format_optional_ts(report.replay_cursor_ts_utc),
        replay_cursor_slot = format_optional_u64(report.replay_cursor_slot),
        replay_cursor_signature = format_optional_str(report.replay_cursor_signature.as_deref()),
        replay_publishable = report.replay_publishable,
        replay_completed = report.replay_completed,
        replay_updated_before_db_mtime =
            format_optional_bool(report.replay_updated_before_db_mtime),
        replay_updated_lag_seconds_vs_db_mtime =
            format_optional_i64(report.replay_updated_lag_seconds_vs_db_mtime),
        replay_updated_lag_days_vs_db_mtime =
            format_optional_i64(report.replay_updated_lag_days_vs_db_mtime),
        replay_has_partial_progress = report.replay_has_partial_progress,
        replay_stopped_mid_replay_phase = report.replay_stopped_mid_replay_phase,
        replay_cursor_present = report.replay_cursor_present,
        replay_loop_surface = report.replay_loop_surface,
        replay_chunk_iteration_surface = report.replay_chunk_iteration_surface,
        replay_progress_persist_surface = report.replay_progress_persist_surface,
        replay_completion_transition_surface = report.replay_completion_transition_surface,
        replay_error_or_early_return_surface_present =
            report.replay_error_or_early_return_surface_present,
        replay_progress_persist_not_guaranteed_on_all_paths =
            report.replay_progress_persist_not_guaranteed_on_all_paths,
        replay_can_remain_in_replay_phase_after_partial_progress =
            report.replay_can_remain_in_replay_phase_after_partial_progress,
        replay_publishable_depends_on_completion_transition =
            report.replay_publishable_depends_on_completion_transition,
        code_supports_partial_progress_without_completion =
            report.code_supports_partial_progress_without_completion,
        code_supports_replay_phase_persisting_across_runs =
            report.code_supports_replay_phase_persisting_across_runs,
        code_supports_early_stop_before_completion_without_clearing_checkpoint =
            report.code_supports_early_stop_before_completion_without_clearing_checkpoint,
        code_supports_completion_only_after_cursor_reaches_target_frontier =
            report.code_supports_completion_only_after_cursor_reaches_target_frontier,
        replay_sol_leg_update_path_conclusion = report.replay_sol_leg_update_path_conclusion,
    ))
}

fn format_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_str(value: Option<&str>) -> String {
    value.unwrap_or("null").to_string()
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|count| count.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_i64(value: Option<i64>) -> String {
    value
        .map(|count| count.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_bool(value: Option<bool>) -> String {
    value
        .map(|flag| flag.to_string())
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_storage::{DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor};
    use serde_json::json;
    use tempfile::TempDir;

    fn repo_config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }

    fn create_fixture() -> Result<(TempDir, PathBuf, SqliteStore)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let db_path = temp.path().join("replay-sol-leg-update-path-audit.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        Ok((temp, db_path, store))
    }

    fn fixture_config(db_path: &Path) -> Config {
        Config {
            db_path: db_path.to_path_buf(),
            config_path: repo_config_path(),
            json: true,
        }
    }

    fn minimal_rebuild_state_json(
        replay_wallet_stats_complete: bool,
        replay_candidate_activity_backfill_pending: bool,
    ) -> String {
        json!({
            "unique_buy_mints": [],
            "discovery_critical_target_buy_mints": [],
            "replay_wallet_stats_complete": replay_wallet_stats_complete,
            "replay_candidate_activity_backfill_required": false,
            "replay_candidate_activity_backfill_pending": replay_candidate_activity_backfill_pending,
            "replay_sol_leg_reentry_pending": false,
            "token_quality_cache": {},
            "token_quality_progress": {
                "next_mint_index": 0,
                "rpc_attempted": 0,
                "rpc_spent_ms": 0
            },
            "by_wallet": {},
            "token_states": {},
            "token_recent_sol_trades": {},
            "pending_rug_checks": [],
            "token_pending_buy_starts": {},
            "completed_snapshots": [],
            "publish_pending_requested_wallet_ids": null,
            "publish_pending_quality_retry_mints": null
        })
        .to_string()
    }

    struct ReplaySeed {
        phase: DiscoveryPersistedRebuildPhase,
        cursor: Option<DiscoveryRuntimeCursor>,
        rows_processed: usize,
        pages_processed: usize,
        chunks_completed: usize,
        started_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        replay_wallet_stats_complete: bool,
        replay_candidate_activity_backfill_pending: bool,
    }

    fn seed_rebuild_state(store: &SqliteStore, seed: ReplaySeed) -> Result<()> {
        let window_start = DateTime::parse_from_rfc3339("2026-04-16T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let horizon_end = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.upsert_discovery_persisted_rebuild_state(&DiscoveryPersistedRebuildStateRow {
            phase: seed.phase,
            window_start,
            horizon_end,
            metrics_window_start: window_start,
            phase_cursor: seed.cursor,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: seed.rows_processed,
            replay_pages_processed: seed.pages_processed,
            chunks_completed: seed.chunks_completed,
            state_json: minimal_rebuild_state_json(
                seed.replay_wallet_stats_complete,
                seed.replay_candidate_activity_backfill_pending,
            ),
            started_at: seed.started_at,
            updated_at: seed.updated_at,
        })
    }

    #[test]
    fn parse_args_reads_required_replay_sol_leg_update_path_audit_flags() -> Result<()> {
        let parsed = parse_args_from([
            "--db".to_string(),
            "/tmp/example.db".to_string(),
            "--config".to_string(),
            "/tmp/example.toml".to_string(),
            "--json".to_string(),
        ])?
        .expect("config should parse");

        assert_eq!(parsed.db_path, PathBuf::from("/tmp/example.db"));
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/example.toml"));
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let (_temp, db_path, _store) = create_fixture()?;

        let report = run(&fixture_config(&db_path))?;
        let json = serde_json::to_value(&report)?;
        for key in [
            "replay_state_present",
            "replay_phase",
            "replay_started_at_utc",
            "replay_updated_at_utc",
            "replay_rows_processed",
            "replay_pages_processed",
            "replay_chunks_completed",
            "replay_cursor_ts_utc",
            "replay_cursor_slot",
            "replay_cursor_signature",
            "replay_publishable",
            "replay_completed",
            "replay_updated_before_db_mtime",
            "replay_updated_lag_seconds_vs_db_mtime",
            "replay_updated_lag_days_vs_db_mtime",
            "replay_has_partial_progress",
            "replay_stopped_mid_replay_phase",
            "replay_cursor_present",
            "replay_loop_surface",
            "replay_chunk_iteration_surface",
            "replay_progress_persist_surface",
            "replay_completion_transition_surface",
            "replay_error_or_early_return_surface_present",
            "replay_progress_persist_not_guaranteed_on_all_paths",
            "replay_can_remain_in_replay_phase_after_partial_progress",
            "replay_publishable_depends_on_completion_transition",
            "code_supports_partial_progress_without_completion",
            "code_supports_replay_phase_persisting_across_runs",
            "code_supports_early_stop_before_completion_without_clearing_checkpoint",
            "code_supports_completion_only_after_cursor_reaches_target_frontier",
            "replay_sol_leg_update_path_conclusion",
        ] {
            assert!(json.get(key).is_some(), "missing JSON key {key}");
        }
        Ok(())
    }

    #[test]
    fn fixture_with_partial_replay_state_yields_allowed_partial_progress_conclusion() -> Result<()>
    {
        let (_temp, db_path, store) = create_fixture()?;
        let started_at = DateTime::parse_from_rfc3339("2026-04-06T18:17:23Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let updated_at = DateTime::parse_from_rfc3339("2026-04-13T09:07:43Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        seed_rebuild_state(
            &store,
            ReplaySeed {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: DateTime::parse_from_rfc3339("2026-04-09T14:00:11Z")
                        .expect("valid timestamp")
                        .with_timezone(&Utc),
                    slot: 42,
                    signature: "sig-cursor".to_string(),
                }),
                rows_processed: 6_497_344,
                pages_processed: 330,
                chunks_completed: 555,
                started_at,
                updated_at,
                replay_wallet_stats_complete: true,
                replay_candidate_activity_backfill_pending: false,
            },
        )?;

        let report = run(&fixture_config(&db_path))?;
        assert!(matches!(
            report.replay_sol_leg_update_path_conclusion.as_str(),
            CONCLUSION_PARTIAL_CHECKPOINT_UPDATES_BEFORE_COMPLETION
                | CONCLUSION_REPLAY_PHASE_PERSISTS_ACROSS_RUNS
        ));
        assert!(report.replay_has_partial_progress);
        assert!(report.replay_stopped_mid_replay_phase);
        Ok(())
    }

    #[test]
    fn ambiguous_fixture_never_overclaims_beyond_allowed_conclusions() -> Result<()> {
        let (_temp, db_path, _store) = create_fixture()?;

        let report = run(&fixture_config(&db_path))?;
        assert!(matches!(
            report.replay_sol_leg_update_path_conclusion.as_str(),
            CONCLUSION_ROOT_CAUSE_REQUIRES_LIVE_RUNTIME_EVIDENCE
                | CONCLUSION_INSUFFICIENT_EVIDENCE
        ));
        Ok(())
    }

    #[test]
    fn repeated_runs_with_same_inputs_are_deterministic() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let started_at = DateTime::parse_from_rfc3339("2026-04-06T18:17:23Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let updated_at = DateTime::parse_from_rfc3339("2026-04-13T09:07:43Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        seed_rebuild_state(
            &store,
            ReplaySeed {
                phase: DiscoveryPersistedRebuildPhase::Replay,
                cursor: Some(DiscoveryRuntimeCursor {
                    ts_utc: DateTime::parse_from_rfc3339("2026-04-09T14:00:11Z")
                        .expect("valid timestamp")
                        .with_timezone(&Utc),
                    slot: 42,
                    signature: "sig-cursor".to_string(),
                }),
                rows_processed: 6_497_344,
                pages_processed: 330,
                chunks_completed: 555,
                started_at,
                updated_at,
                replay_wallet_stats_complete: true,
                replay_candidate_activity_backfill_pending: false,
            },
        )?;

        let report_one = run(&fixture_config(&db_path))?;
        let report_two = run(&fixture_config(&db_path))?;
        assert_eq!(report_one, report_two);
        assert_eq!(
            serde_json::to_value(&report_one)?,
            serde_json::to_value(&report_two)?
        );
        Ok(())
    }

    #[test]
    fn source_grounding_surfaces_exist_in_current_repo_source() {
        assert!(DISCOVERY_SOURCE.contains(
            "fn advance_persisted_stream_rebuild_with_phase_page_limits("
        ));
        assert!(DISCOVERY_SOURCE.contains(
            "fn advance_persisted_stream_replay_with_phase_page_limits("
        ));
        assert!(DISCOVERY_SOURCE.contains("fn persist_persisted_stream_rebuild_state("));
        assert!(DISCOVERY_SOURCE.contains(
            "state.phase = DiscoveryPersistedRebuildPhase::PublishPending;"
        ));
    }
}
