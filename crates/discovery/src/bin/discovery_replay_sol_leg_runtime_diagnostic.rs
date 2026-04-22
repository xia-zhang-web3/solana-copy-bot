use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::DiscoveryService;
use copybot_storage::SqliteStore;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const USAGE: &str =
    "usage: discovery_replay_sol_leg_runtime_diagnostic --db <path> --config <path> [--json]";

const CONCLUSION_EXISTING_RUNTIME_SURFACES_SHOULD_SHOW_PROGRESS: &str =
    "existing_runtime_surfaces_should_show_resume_or_progress_if_replay_is_advancing";
const CONCLUSION_CHECKPOINT_CAN_PERSIST_ACROSS_CYCLES: &str =
    "checkpoint_can_legitimately_persist_in_replay_without_completion_across_cycles";
const CONCLUSION_LIVE_ROOT_CAUSE_REQUIRES_LOG_CORRELATION: &str =
    "live_root_cause_now_requires_operator_log_correlation";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const REPLAY_RESUME_SURFACE: &str =
    "copybot_discovery::DiscoveryService::load_or_start_persisted_stream_rebuild_state";
const REPLAY_ADVANCE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::advance_persisted_stream_rebuild_with_phase_page_limits";
const REPLAY_STATE_LOAD_OR_START_SURFACE: &str =
    "copybot_discovery::DiscoveryService::load_or_start_persisted_stream_rebuild_state_with_options";
const REPLAY_STATE_PERSIST_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persist_persisted_stream_rebuild_state";

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
struct ReplaySolLegRuntimeDiagnosticReport {
    replay_phase: Option<String>,
    replay_updated_at_utc: Option<DateTime<Utc>>,
    replay_rows_processed: Option<u64>,
    replay_pages_processed: Option<u64>,
    replay_chunks_completed: Option<u64>,
    replay_cursor_ts_utc: Option<DateTime<Utc>>,
    replay_publishable: bool,
    replay_completed: bool,
    replay_resume_surface: String,
    replay_advance_surface: String,
    replay_state_load_or_start_surface: String,
    replay_state_persist_surface: String,
    replay_in_progress_return_surface_present: bool,
    replay_resumed_existing_surface_present: bool,
    replay_scheduler_yield_surface_present: bool,
    replay_can_exit_without_completion_and_keep_checkpoint: bool,
    replay_can_resume_existing_checkpoint_on_next_cycle: bool,
    replay_progress_log_surface_present: bool,
    replay_resume_log_surface_present: bool,
    replay_incomplete_blocker_log_surface_present: bool,
    existing_runtime_surface_is_sufficient_for_live_verdict: bool,
    replay_runtime_diagnostic_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct ReplayStateFacts {
    phase: Option<String>,
    updated_at_utc: Option<DateTime<Utc>>,
    rows_processed: Option<u64>,
    pages_processed: Option<u64>,
    chunks_completed: Option<u64>,
    cursor_ts_utc: Option<DateTime<Utc>>,
    publishable: bool,
    completed: bool,
    state_present: bool,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    replay_in_progress_return_surface_present: bool,
    replay_resumed_existing_surface_present: bool,
    replay_scheduler_yield_surface_present: bool,
    replay_can_exit_without_completion_and_keep_checkpoint: bool,
    replay_can_resume_existing_checkpoint_on_next_cycle: bool,
    replay_progress_log_surface_present: bool,
    replay_resume_log_surface_present: bool,
    replay_incomplete_blocker_log_surface_present: bool,
    existing_runtime_surface_is_sufficient_for_live_verdict: bool,
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

fn run(config: &Config) -> Result<ReplaySolLegRuntimeDiagnosticReport> {
    let _loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let replay_state = load_replay_state_facts(&store)?;
    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(&replay_state, &code_paths);

    Ok(ReplaySolLegRuntimeDiagnosticReport {
        replay_phase: replay_state.phase,
        replay_updated_at_utc: replay_state.updated_at_utc,
        replay_rows_processed: replay_state.rows_processed,
        replay_pages_processed: replay_state.pages_processed,
        replay_chunks_completed: replay_state.chunks_completed,
        replay_cursor_ts_utc: replay_state.cursor_ts_utc,
        replay_publishable: replay_state.publishable,
        replay_completed: replay_state.completed,
        replay_resume_surface: REPLAY_RESUME_SURFACE.to_string(),
        replay_advance_surface: REPLAY_ADVANCE_SURFACE.to_string(),
        replay_state_load_or_start_surface: REPLAY_STATE_LOAD_OR_START_SURFACE.to_string(),
        replay_state_persist_surface: REPLAY_STATE_PERSIST_SURFACE.to_string(),
        replay_in_progress_return_surface_present: code_paths
            .replay_in_progress_return_surface_present,
        replay_resumed_existing_surface_present: code_paths
            .replay_resumed_existing_surface_present,
        replay_scheduler_yield_surface_present: code_paths.replay_scheduler_yield_surface_present,
        replay_can_exit_without_completion_and_keep_checkpoint: code_paths
            .replay_can_exit_without_completion_and_keep_checkpoint,
        replay_can_resume_existing_checkpoint_on_next_cycle: code_paths
            .replay_can_resume_existing_checkpoint_on_next_cycle,
        replay_progress_log_surface_present: code_paths.replay_progress_log_surface_present,
        replay_resume_log_surface_present: code_paths.replay_resume_log_surface_present,
        replay_incomplete_blocker_log_surface_present: code_paths
            .replay_incomplete_blocker_log_surface_present,
        existing_runtime_surface_is_sufficient_for_live_verdict: code_paths
            .existing_runtime_surface_is_sufficient_for_live_verdict,
        replay_runtime_diagnostic_conclusion: conclusion.to_string(),
    })
}

fn load_replay_state_facts(store: &SqliteStore) -> Result<ReplayStateFacts> {
    let inspection = DiscoveryService::inspect_persisted_rebuild_state_read_only(store)?;
    let raw_row = store.load_discovery_persisted_rebuild_state_read_only()?;
    if !inspection.persisted_rebuild_checkpoint_exists {
        return Ok(ReplayStateFacts::default());
    }

    Ok(ReplayStateFacts {
        phase: inspection.rebuild_phase,
        updated_at_utc: raw_row.as_ref().map(|row| row.updated_at).or(inspection.updated_at),
        rows_processed: raw_row
            .as_ref()
            .map(|row| row.replay_rows_processed as u64)
            .or_else(|| inspection.replay_rows_processed.map(|value| value as u64)),
        pages_processed: raw_row
            .as_ref()
            .map(|row| row.replay_pages_processed as u64)
            .or_else(|| inspection.replay_pages_processed.map(|value| value as u64)),
        chunks_completed: raw_row.as_ref().map(|row| row.chunks_completed as u64),
        cursor_ts_utc: raw_row
            .as_ref()
            .and_then(|row| row.phase_cursor.as_ref())
            .map(|cursor| cursor.ts_utc)
            .or_else(|| inspection.phase_cursor.as_ref().map(|cursor| cursor.ts_utc)),
        publishable: inspection.publishable_checkpoint_blocker.is_none(),
        completed: !inspection.replay_incomplete,
        state_present: true,
    })
}

fn inspect_code_path_facts() -> CodePathFacts {
    let replay_in_progress_return_surface_present = DISCOVERY_SOURCE
        .contains("Ok(PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry })");
    let replay_resumed_existing_surface_present =
        DISCOVERY_SOURCE.contains("PersistedStreamRebuildRestoreOutcome::ResumedExisting")
            && DISCOVERY_SOURCE
                .contains("\"resuming bounded discovery persisted observed_swaps rebuild\"");
    let replay_scheduler_yield_surface_present = DISCOVERY_SOURCE
        .contains("\"yielding bounded discovery persisted observed_swaps rebuild back to scheduler\"");
    let replay_can_exit_without_completion_and_keep_checkpoint = slice_between(
        DISCOVERY_SOURCE,
        "state.chunks_completed = state.chunks_completed.saturating_add(1);",
        "Ok(PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry })",
    )
    .is_some_and(|between| {
        between.contains("self.persist_persisted_stream_rebuild_state(store, &mut state, now)?;")
            && !between.contains("clear_discovery_persisted_rebuild_state")
    });
    let replay_can_resume_existing_checkpoint_on_next_cycle = DISCOVERY_SOURCE
        .contains("fn load_or_start_persisted_stream_rebuild_state(")
        && DISCOVERY_SOURCE.contains("fn load_or_start_persisted_stream_rebuild_state_with_options(")
        && DISCOVERY_SOURCE.contains("PersistedStreamRebuildRestoreOutcome::ResumedExisting")
        && DISCOVERY_SOURCE.contains("rebuild_phase = Some(state.phase.as_str())");
    let replay_progress_log_surface_present = DISCOVERY_SOURCE.contains("fn log_persisted_stream_progress(")
        && DISCOVERY_SOURCE.contains("rebuild_publishable_checkpoint_blocker =")
        && DISCOVERY_SOURCE.contains("\"yielding bounded discovery persisted observed_swaps rebuild back to scheduler\"");
    let replay_resume_log_surface_present = DISCOVERY_SOURCE
        .contains("\"resuming bounded discovery persisted observed_swaps rebuild\"")
        || DISCOVERY_SOURCE.contains(
            "\"resuming bounded discovery persisted observed_swaps rebuild from publish-pending checkpoint\"",
        );
    let replay_incomplete_blocker_log_surface_present =
        DISCOVERY_SOURCE.contains("rebuild_publishable_checkpoint_blocker =")
            && DISCOVERY_SOURCE
                .contains("reason: format!(\"publication_truth_withheld_while_{checkpoint_blocker}\")");
    let existing_runtime_surface_is_sufficient_for_live_verdict = replay_in_progress_return_surface_present
        && replay_resumed_existing_surface_present
        && replay_scheduler_yield_surface_present
        && replay_progress_log_surface_present
        && replay_resume_log_surface_present
        && replay_incomplete_blocker_log_surface_present;

    CodePathFacts {
        replay_in_progress_return_surface_present,
        replay_resumed_existing_surface_present,
        replay_scheduler_yield_surface_present,
        replay_can_exit_without_completion_and_keep_checkpoint,
        replay_can_resume_existing_checkpoint_on_next_cycle,
        replay_progress_log_surface_present,
        replay_resume_log_surface_present,
        replay_incomplete_blocker_log_surface_present,
        existing_runtime_surface_is_sufficient_for_live_verdict,
    }
}

fn slice_between<'a>(haystack: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let start_index = haystack.find(start)?;
    let remainder = &haystack[start_index + start.len()..];
    let end_index = remainder.find(end)?;
    Some(&remainder[..end_index])
}

fn choose_conclusion(
    replay_state: &ReplayStateFacts,
    code_paths: &CodePathFacts,
) -> &'static str {
    if replay_state.state_present
        && !replay_state.completed
        && !replay_state.publishable
        && code_paths.existing_runtime_surface_is_sufficient_for_live_verdict
        && code_paths.replay_can_resume_existing_checkpoint_on_next_cycle
        && code_paths.replay_can_exit_without_completion_and_keep_checkpoint
    {
        return CONCLUSION_LIVE_ROOT_CAUSE_REQUIRES_LOG_CORRELATION;
    }

    if code_paths.existing_runtime_surface_is_sufficient_for_live_verdict
        && code_paths.replay_can_resume_existing_checkpoint_on_next_cycle
    {
        return CONCLUSION_EXISTING_RUNTIME_SURFACES_SHOULD_SHOW_PROGRESS;
    }

    if code_paths.replay_can_exit_without_completion_and_keep_checkpoint
        && code_paths.replay_can_resume_existing_checkpoint_on_next_cycle
    {
        return CONCLUSION_CHECKPOINT_CAN_PERSIST_ACROSS_CYCLES;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &ReplaySolLegRuntimeDiagnosticReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed to serialize replay sol leg runtime diagnostic report");
    }

    Ok(format!(
        concat!(
            "replay_phase={replay_phase}\n",
            "replay_updated_at_utc={replay_updated_at_utc}\n",
            "replay_rows_processed={replay_rows_processed}\n",
            "replay_pages_processed={replay_pages_processed}\n",
            "replay_chunks_completed={replay_chunks_completed}\n",
            "replay_cursor_ts_utc={replay_cursor_ts_utc}\n",
            "replay_publishable={replay_publishable}\n",
            "replay_completed={replay_completed}\n",
            "replay_resume_surface={replay_resume_surface}\n",
            "replay_advance_surface={replay_advance_surface}\n",
            "replay_state_load_or_start_surface={replay_state_load_or_start_surface}\n",
            "replay_state_persist_surface={replay_state_persist_surface}\n",
            "replay_in_progress_return_surface_present={replay_in_progress_return_surface_present}\n",
            "replay_resumed_existing_surface_present={replay_resumed_existing_surface_present}\n",
            "replay_scheduler_yield_surface_present={replay_scheduler_yield_surface_present}\n",
            "replay_can_exit_without_completion_and_keep_checkpoint={replay_can_exit_without_completion_and_keep_checkpoint}\n",
            "replay_can_resume_existing_checkpoint_on_next_cycle={replay_can_resume_existing_checkpoint_on_next_cycle}\n",
            "replay_progress_log_surface_present={replay_progress_log_surface_present}\n",
            "replay_resume_log_surface_present={replay_resume_log_surface_present}\n",
            "replay_incomplete_blocker_log_surface_present={replay_incomplete_blocker_log_surface_present}\n",
            "existing_runtime_surface_is_sufficient_for_live_verdict={existing_runtime_surface_is_sufficient_for_live_verdict}\n",
            "replay_runtime_diagnostic_conclusion={replay_runtime_diagnostic_conclusion}\n",
        ),
        replay_phase = format_optional_str(report.replay_phase.as_deref()),
        replay_updated_at_utc = format_optional_ts(report.replay_updated_at_utc),
        replay_rows_processed = format_optional_u64(report.replay_rows_processed),
        replay_pages_processed = format_optional_u64(report.replay_pages_processed),
        replay_chunks_completed = format_optional_u64(report.replay_chunks_completed),
        replay_cursor_ts_utc = format_optional_ts(report.replay_cursor_ts_utc),
        replay_publishable = report.replay_publishable,
        replay_completed = report.replay_completed,
        replay_resume_surface = report.replay_resume_surface,
        replay_advance_surface = report.replay_advance_surface,
        replay_state_load_or_start_surface = report.replay_state_load_or_start_surface,
        replay_state_persist_surface = report.replay_state_persist_surface,
        replay_in_progress_return_surface_present =
            report.replay_in_progress_return_surface_present,
        replay_resumed_existing_surface_present =
            report.replay_resumed_existing_surface_present,
        replay_scheduler_yield_surface_present =
            report.replay_scheduler_yield_surface_present,
        replay_can_exit_without_completion_and_keep_checkpoint =
            report.replay_can_exit_without_completion_and_keep_checkpoint,
        replay_can_resume_existing_checkpoint_on_next_cycle =
            report.replay_can_resume_existing_checkpoint_on_next_cycle,
        replay_progress_log_surface_present = report.replay_progress_log_surface_present,
        replay_resume_log_surface_present = report.replay_resume_log_surface_present,
        replay_incomplete_blocker_log_surface_present =
            report.replay_incomplete_blocker_log_surface_present,
        existing_runtime_surface_is_sufficient_for_live_verdict =
            report.existing_runtime_surface_is_sufficient_for_live_verdict,
        replay_runtime_diagnostic_conclusion = report.replay_runtime_diagnostic_conclusion,
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

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor,
        SqliteStore,
    };
    use serde_json::json;
    use std::path::Path;
    use tempfile::TempDir;

    fn repo_config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }

    fn create_fixture() -> Result<(TempDir, PathBuf, SqliteStore)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let db_path = temp.path().join("replay-sol-leg-runtime-diagnostic.db");
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

    fn minimal_rebuild_state_json() -> String {
        json!({
            "unique_buy_mints": [],
            "discovery_critical_target_buy_mints": [],
            "replay_wallet_stats_complete": true,
            "replay_candidate_activity_backfill_required": false,
            "replay_candidate_activity_backfill_pending": false,
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

    fn seed_rebuild_state(store: &SqliteStore) -> Result<()> {
        let window_start = DateTime::parse_from_rfc3339("2026-04-16T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let horizon_end = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.upsert_discovery_persisted_rebuild_state(&DiscoveryPersistedRebuildStateRow {
            phase: DiscoveryPersistedRebuildPhase::Replay,
            window_start,
            horizon_end,
            metrics_window_start: window_start,
            phase_cursor: Some(DiscoveryRuntimeCursor {
                ts_utc: DateTime::parse_from_rfc3339("2026-04-09T14:00:11Z")
                    .expect("valid timestamp")
                    .with_timezone(&Utc),
                slot: 412083685,
                signature:
                    "5MqDZNzdmPRLWdtVsYVp8GE7DrruzfgW43kHoki236aPABhzuxFNdwxyBJTiBJ6pM2mvoVEVEu4FbzECXShG11DS"
                        .to_string(),
            }),
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 6_497_344,
            replay_pages_processed: 330,
            chunks_completed: 555,
            state_json: minimal_rebuild_state_json(),
            started_at: DateTime::parse_from_rfc3339("2026-04-06T18:17:23Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339("2026-04-13T09:07:43Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
        })
    }

    #[test]
    fn parse_args_reads_required_runtime_diagnostic_flags() -> Result<()> {
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
        let (_temp, db_path, store) = create_fixture()?;
        seed_rebuild_state(&store)?;

        let report = run(&fixture_config(&db_path))?;
        let json = serde_json::to_value(&report)?;
        for key in [
            "replay_phase",
            "replay_updated_at_utc",
            "replay_rows_processed",
            "replay_pages_processed",
            "replay_chunks_completed",
            "replay_cursor_ts_utc",
            "replay_publishable",
            "replay_completed",
            "replay_resume_surface",
            "replay_advance_surface",
            "replay_state_load_or_start_surface",
            "replay_state_persist_surface",
            "replay_in_progress_return_surface_present",
            "replay_resumed_existing_surface_present",
            "replay_scheduler_yield_surface_present",
            "replay_can_exit_without_completion_and_keep_checkpoint",
            "replay_can_resume_existing_checkpoint_on_next_cycle",
            "replay_progress_log_surface_present",
            "replay_resume_log_surface_present",
            "replay_incomplete_blocker_log_surface_present",
            "existing_runtime_surface_is_sufficient_for_live_verdict",
            "replay_runtime_diagnostic_conclusion",
        ] {
            assert!(json.get(key).is_some(), "missing JSON key {key}");
        }
        Ok(())
    }

    #[test]
    fn partial_checkpoint_with_current_source_grounding_requires_live_log_correlation() -> Result<()>
    {
        let (_temp, db_path, store) = create_fixture()?;
        seed_rebuild_state(&store)?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_runtime_diagnostic_conclusion,
            CONCLUSION_LIVE_ROOT_CAUSE_REQUIRES_LOG_CORRELATION
        );
        assert!(report.existing_runtime_surface_is_sufficient_for_live_verdict);
        Ok(())
    }

    #[test]
    fn choose_conclusion_with_only_partial_checkpoint_shape_never_overclaims() {
        let replay_state = ReplayStateFacts {
            state_present: true,
            publishable: false,
            completed: false,
            ..ReplayStateFacts::default()
        };
        let code_paths = CodePathFacts {
            replay_can_exit_without_completion_and_keep_checkpoint: true,
            replay_can_resume_existing_checkpoint_on_next_cycle: true,
            ..CodePathFacts::default()
        };

        assert_eq!(
            choose_conclusion(&replay_state, &code_paths),
            CONCLUSION_CHECKPOINT_CAN_PERSIST_ACROSS_CYCLES
        );
    }

    #[test]
    fn ambiguous_facts_return_insufficient_evidence() {
        let replay_state = ReplayStateFacts::default();
        let code_paths = CodePathFacts::default();

        assert_eq!(
            choose_conclusion(&replay_state, &code_paths),
            CONCLUSION_INSUFFICIENT_EVIDENCE
        );
    }

    #[test]
    fn repeated_runs_with_same_inputs_are_deterministic() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_rebuild_state(&store)?;

        let report_one = run(&fixture_config(&db_path))?;
        let report_two = run(&fixture_config(&db_path))?;
        assert_eq!(report_one, report_two);
        assert_eq!(
            serde_json::to_value(&report_one)?,
            serde_json::to_value(&report_two)?
        );
        Ok(())
    }
}
