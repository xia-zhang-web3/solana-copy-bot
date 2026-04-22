use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, DiscoveryConfig};
use copybot_discovery::DiscoveryService;
use copybot_storage::SqliteStore;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_replay_sol_leg_lag_cause_audit --db <path> --config <path> [--json]";

const DB_MTIME_RECENT_PROGRESS_MAX_LAG_SECONDS: i64 = 300;

const CONCLUSION_PROGRESS_STOPPED_BEFORE_SNAPSHOT: &str =
    "replay_sol_leg_lag_because_progress_stopped_before_snapshot";
const CONCLUSION_SOURCE_FRONTIER_OUTPACED_RECENT_PROGRESS: &str =
    "replay_sol_leg_lag_because_source_frontier_outpaced_recent_progress";
const CONCLUSION_PUBLISHABILITY_GATE_UNSATISFIED: &str =
    "replay_sol_leg_lag_because_publishability_gate_remains_unsatisfied_after_partial_progress";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const REPLAY_STATE_PERSIST_SURFACE: &str =
    "copybot_storage::SqliteStore::upsert_discovery_persisted_rebuild_state";
const REPLAY_CHUNK_PROGRESS_UPDATE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persist_persisted_stream_rebuild_state";
const REPLAY_PUBLISHABLE_CHECKPOINT_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state";
const REPLAY_SOL_LEG_BLOCKER_SURFACE: &str =
    "copybot_discovery::DiscoveryService::explain_replay_sol_leg_incomplete_read_only";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");
const APP_MAIN_SOURCE: &str = include_str!("../../../app/src/main.rs");
const STORAGE_MARKET_DATA_SOURCE: &str = include_str!("../../../storage/src/market_data.rs");

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
struct ReplaySolLegLagCauseAuditReport {
    db_path: String,
    db_file_size_bytes: u64,
    db_file_mtime_utc: DateTime<Utc>,
    replay_state_present: bool,
    replay_phase: Option<String>,
    replay_rows_processed: Option<u64>,
    replay_pages_processed: Option<u64>,
    replay_chunks_completed: Option<u64>,
    replay_started_at_utc: Option<DateTime<Utc>>,
    replay_updated_at_utc: Option<DateTime<Utc>>,
    replay_cursor_ts_utc: Option<DateTime<Utc>>,
    replay_cursor_slot: Option<u64>,
    replay_cursor_signature: Option<String>,
    replay_publishable: bool,
    replay_completed: bool,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    publication_window_start_utc: DateTime<Utc>,
    replay_cursor_before_publication_window_start: Option<bool>,
    replay_cursor_before_observed_swaps_frontier: Option<bool>,
    replay_cursor_lag_seconds_vs_observed_swaps_frontier: Option<i64>,
    replay_cursor_lag_days_vs_observed_swaps_frontier: Option<i64>,
    replay_updated_before_db_mtime: Option<bool>,
    replay_updated_lag_seconds_vs_db_mtime: Option<i64>,
    replay_updated_lag_days_vs_db_mtime: Option<i64>,
    replay_updated_after_started: bool,
    replay_has_nonzero_rows_processed: bool,
    replay_has_nonzero_chunks_completed: bool,
    replay_cursor_present: bool,
    replay_state_persist_surface: String,
    replay_chunk_progress_update_surface: String,
    replay_publishable_checkpoint_surface: String,
    replay_sol_leg_blocker_surface: String,
    publishable_depends_on_replay_cursor_reaching_required_frontier: bool,
    fail_closed_persists_while_replay_blocker_present: bool,
    replay_sol_leg_lag_cause_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct PublicationStateFacts {
    updated_at_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct ReplayStateFacts {
    state_present: bool,
    phase: Option<String>,
    rows_processed: Option<u64>,
    pages_processed: Option<u64>,
    chunks_completed: Option<u64>,
    started_at_utc: Option<DateTime<Utc>>,
    updated_at_utc: Option<DateTime<Utc>>,
    cursor_ts_utc: Option<DateTime<Utc>>,
    cursor_slot: Option<u64>,
    cursor_signature: Option<String>,
    publishable: bool,
    completed: bool,
    updated_after_started: bool,
    has_nonzero_rows_processed: bool,
    has_nonzero_chunks_completed: bool,
    cursor_present: bool,
}

#[derive(Debug, Clone, Default)]
struct FrontierFacts {
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct FrontierComparisonFacts {
    cursor_before_publication_window_start: Option<bool>,
    cursor_before_observed_swaps_frontier: Option<bool>,
    cursor_lag_seconds_vs_observed_swaps_frontier: Option<i64>,
    cursor_lag_days_vs_observed_swaps_frontier: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct SnapshotRelativeProgressFacts {
    updated_before_db_mtime: Option<bool>,
    updated_lag_seconds_vs_db_mtime: Option<i64>,
    updated_lag_days_vs_db_mtime: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    publishable_depends_on_replay_cursor_reaching_required_frontier: bool,
    fail_closed_persists_while_replay_blocker_present: bool,
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

fn run(config: &Config) -> Result<ReplaySolLegLagCauseAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let conn = open_read_only_connection(&config.db_path)?;
    let db_metadata = load_db_metadata(&config.db_path)?;

    let publication_state = load_publication_state_facts(&store)?;
    let frontier = load_frontier_facts(&conn)?;
    let publication_window_anchor = publication_window_anchor_utc(&publication_state, &frontier)?;
    let publication_window_start_utc =
        metrics_window_start(&loaded_config.discovery, publication_window_anchor);
    let replay_state = load_replay_state_facts(&store)?;
    let frontier_comparison =
        compare_frontiers(&replay_state, &frontier, publication_window_start_utc);
    let snapshot_relative = compare_snapshot_relative_progress(&replay_state, db_metadata.file_mtime_utc);
    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(
        &replay_state,
        &frontier_comparison,
        &snapshot_relative,
        &code_paths,
    );

    Ok(ReplaySolLegLagCauseAuditReport {
        db_path: config.db_path.display().to_string(),
        db_file_size_bytes: db_metadata.file_size_bytes,
        db_file_mtime_utc: db_metadata.file_mtime_utc,
        replay_state_present: replay_state.state_present,
        replay_phase: replay_state.phase,
        replay_rows_processed: replay_state.rows_processed,
        replay_pages_processed: replay_state.pages_processed,
        replay_chunks_completed: replay_state.chunks_completed,
        replay_started_at_utc: replay_state.started_at_utc,
        replay_updated_at_utc: replay_state.updated_at_utc,
        replay_cursor_ts_utc: replay_state.cursor_ts_utc,
        replay_cursor_slot: replay_state.cursor_slot,
        replay_cursor_signature: replay_state.cursor_signature,
        replay_publishable: replay_state.publishable,
        replay_completed: replay_state.completed,
        observed_swaps_max_ts_utc: frontier.observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc: frontier.wallet_activity_days_max_day_utc,
        publication_window_start_utc,
        replay_cursor_before_publication_window_start: frontier_comparison
            .cursor_before_publication_window_start,
        replay_cursor_before_observed_swaps_frontier: frontier_comparison
            .cursor_before_observed_swaps_frontier,
        replay_cursor_lag_seconds_vs_observed_swaps_frontier: frontier_comparison
            .cursor_lag_seconds_vs_observed_swaps_frontier,
        replay_cursor_lag_days_vs_observed_swaps_frontier: frontier_comparison
            .cursor_lag_days_vs_observed_swaps_frontier,
        replay_updated_before_db_mtime: snapshot_relative.updated_before_db_mtime,
        replay_updated_lag_seconds_vs_db_mtime: snapshot_relative.updated_lag_seconds_vs_db_mtime,
        replay_updated_lag_days_vs_db_mtime: snapshot_relative.updated_lag_days_vs_db_mtime,
        replay_updated_after_started: replay_state.updated_after_started,
        replay_has_nonzero_rows_processed: replay_state.has_nonzero_rows_processed,
        replay_has_nonzero_chunks_completed: replay_state.has_nonzero_chunks_completed,
        replay_cursor_present: replay_state.cursor_present,
        replay_state_persist_surface: REPLAY_STATE_PERSIST_SURFACE.to_string(),
        replay_chunk_progress_update_surface: REPLAY_CHUNK_PROGRESS_UPDATE_SURFACE.to_string(),
        replay_publishable_checkpoint_surface: REPLAY_PUBLISHABLE_CHECKPOINT_SURFACE.to_string(),
        replay_sol_leg_blocker_surface: REPLAY_SOL_LEG_BLOCKER_SURFACE.to_string(),
        publishable_depends_on_replay_cursor_reaching_required_frontier: code_paths
            .publishable_depends_on_replay_cursor_reaching_required_frontier,
        fail_closed_persists_while_replay_blocker_present: code_paths
            .fail_closed_persists_while_replay_blocker_present,
        replay_sol_leg_lag_cause_conclusion: conclusion.to_string(),
    })
}

#[derive(Debug, Clone)]
struct DbMetadataFacts {
    file_size_bytes: u64,
    file_mtime_utc: DateTime<Utc>,
}

fn load_db_metadata(path: &Path) -> Result<DbMetadataFacts> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed reading db metadata for {}", path.display()))?;
    let file_mtime_utc: DateTime<Utc> = metadata
        .modified()
        .map(DateTime::<Utc>::from)
        .with_context(|| format!("failed reading db mtime for {}", path.display()))?;
    Ok(DbMetadataFacts {
        file_size_bytes: metadata.len(),
        file_mtime_utc,
    })
}

fn open_read_only_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed to open sqlite db read-only: {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .context("failed to set sqlite busy_timeout")?;
    conn.pragma_update(None, "query_only", "ON")
        .context("failed to enable sqlite query_only")?;
    Ok(conn)
}

fn sqlite_table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1 LIMIT 1",
            [table_name],
            |row| row.get(0),
        )
        .optional()
        .context("failed checking sqlite table existence")?;
    Ok(exists.is_some())
}

fn load_publication_state_facts(store: &SqliteStore) -> Result<PublicationStateFacts> {
    let Some(state) = store.discovery_publication_state_read_only()? else {
        return Ok(PublicationStateFacts::default());
    };
    Ok(PublicationStateFacts {
        updated_at_utc: Some(state.updated_at),
    })
}

fn load_frontier_facts(conn: &Connection) -> Result<FrontierFacts> {
    Ok(FrontierFacts {
        observed_swaps_max_ts_utc: query_observed_swaps_max_ts_utc(conn)?,
        wallet_activity_days_max_day_utc: query_wallet_activity_days_max_day_utc(conn)?,
    })
}

fn query_observed_swaps_max_ts_utc(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    if !sqlite_table_exists(conn, "observed_swaps")? {
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT ts
             FROM observed_swaps
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed loading observed_swaps max ts")?;
    raw.as_deref().map(parse_db_ts).transpose()
}

fn query_wallet_activity_days_max_day_utc(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    if !sqlite_table_exists(conn, "wallet_activity_days")? {
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT activity_day
             FROM wallet_activity_days
             ORDER BY activity_day DESC, last_seen DESC, wallet_id DESC
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed loading wallet_activity_days max day")?;
    raw.as_deref().map(parse_day_or_ts).transpose()
}

fn parse_db_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid db timestamp value: {raw}"))
}

fn parse_day_or_ts(raw: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(raw) {
        return Ok(ts.with_timezone(&Utc));
    }
    let day = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("invalid db day/timestamp value: {raw}"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        day.and_hms_opt(0, 0, 0).expect("midnight should exist"),
        Utc,
    ))
}

fn publication_window_anchor_utc(
    publication_state: &PublicationStateFacts,
    frontier: &FrontierFacts,
) -> Result<DateTime<Utc>> {
    if let Some(updated_at) = publication_state.updated_at_utc {
        return Ok(updated_at);
    }
    if let Some(max_ts) = frontier.observed_swaps_max_ts_utc {
        return Ok(max_ts);
    }
    if let Some(max_day) = frontier.wallet_activity_days_max_day_utc {
        return Ok(max_day);
    }
    DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
        .map(|ts| ts.with_timezone(&Utc))
        .context("failed constructing epoch fallback")
}

fn metrics_window_start(config: &DiscoveryConfig, now: DateTime<Utc>) -> DateTime<Utc> {
    let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
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
    let updated_after_started = started_at_utc
        .zip(updated_at_utc)
        .is_some_and(|(started, updated)| updated > started);
    let has_nonzero_rows_processed = rows_processed.unwrap_or(0) > 0;
    let has_nonzero_chunks_completed = chunks_completed.unwrap_or(0) > 0;
    let cursor_present = cursor.is_some();
    let publishable = inspection.publishable_checkpoint_blocker.is_none();
    let completed = !inspection.replay_incomplete;

    Ok(ReplayStateFacts {
        state_present: true,
        phase: inspection.rebuild_phase,
        rows_processed,
        pages_processed,
        chunks_completed,
        started_at_utc,
        updated_at_utc,
        cursor_ts_utc: cursor.as_ref().map(|cursor| cursor.ts_utc),
        cursor_slot: cursor.as_ref().map(|cursor| cursor.slot),
        cursor_signature: cursor.as_ref().map(|cursor| cursor.signature.clone()),
        publishable,
        completed,
        updated_after_started,
        has_nonzero_rows_processed,
        has_nonzero_chunks_completed,
        cursor_present,
    })
}

fn compare_frontiers(
    replay_state: &ReplayStateFacts,
    frontier: &FrontierFacts,
    publication_window_start_utc: DateTime<Utc>,
) -> FrontierComparisonFacts {
    let cursor_before_publication_window_start = replay_state
        .cursor_ts_utc
        .map(|cursor_ts| cursor_ts < publication_window_start_utc);
    let cursor_before_observed_swaps_frontier = replay_state
        .cursor_ts_utc
        .zip(frontier.observed_swaps_max_ts_utc)
        .map(|(cursor_ts, frontier_ts)| cursor_ts < frontier_ts);
    let cursor_lag_seconds_vs_observed_swaps_frontier = replay_state
        .cursor_ts_utc
        .zip(frontier.observed_swaps_max_ts_utc)
        .map(|(cursor_ts, frontier_ts)| (frontier_ts - cursor_ts).num_seconds());
    let cursor_lag_days_vs_observed_swaps_frontier = cursor_lag_seconds_vs_observed_swaps_frontier
        .map(|seconds| seconds.div_euclid(86_400));

    FrontierComparisonFacts {
        cursor_before_publication_window_start,
        cursor_before_observed_swaps_frontier,
        cursor_lag_seconds_vs_observed_swaps_frontier,
        cursor_lag_days_vs_observed_swaps_frontier,
    }
}

fn compare_snapshot_relative_progress(
    replay_state: &ReplayStateFacts,
    db_file_mtime_utc: DateTime<Utc>,
) -> SnapshotRelativeProgressFacts {
    let updated_before_db_mtime = replay_state
        .updated_at_utc
        .map(|updated_at| updated_at < db_file_mtime_utc);
    let updated_lag_seconds_vs_db_mtime = replay_state
        .updated_at_utc
        .map(|updated_at| (db_file_mtime_utc - updated_at).num_seconds());
    let updated_lag_days_vs_db_mtime = updated_lag_seconds_vs_db_mtime
        .map(|seconds| seconds.div_euclid(86_400));

    SnapshotRelativeProgressFacts {
        updated_before_db_mtime,
        updated_lag_seconds_vs_db_mtime,
        updated_lag_days_vs_db_mtime,
    }
}

fn inspect_code_path_facts() -> CodePathFacts {
    let publishable_depends_on_replay_cursor_reaching_required_frontier = DISCOVERY_SOURCE
        .contains("Some(\"sol_leg\") => \"replay_sol_leg_incomplete\"")
        && DISCOVERY_SOURCE.contains("Some(\"sol_leg_swap_source_exhaustion\")")
        && DISCOVERY_SOURCE.contains("source_rows_exist_beyond_stored_replay_checkpoint")
        && DISCOVERY_SOURCE.contains(
            "persisted replay checkpoint already has an exact target surface, but the source still has {count} exact-target SOL-leg rows beyond the stored replay cursor within the frozen replay horizon",
        )
        && DISCOVERY_SOURCE.contains("fn persist_persisted_stream_rebuild_state(")
        && STORAGE_MARKET_DATA_SOURCE.contains("pub fn upsert_discovery_persisted_rebuild_state(");
    let fail_closed_persists_while_replay_blocker_present = DISCOVERY_SOURCE
        .contains("reason: format!(\"publication_truth_withheld_while_{checkpoint_blocker}\")")
        && APP_MAIN_SOURCE.contains("fn apply_fail_closed_runtime_follow_surface_if_needed(")
        && APP_MAIN_SOURCE.contains(
            "DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded",
        );

    CodePathFacts {
        publishable_depends_on_replay_cursor_reaching_required_frontier,
        fail_closed_persists_while_replay_blocker_present,
    }
}

fn choose_conclusion(
    replay_state: &ReplayStateFacts,
    frontier_comparison: &FrontierComparisonFacts,
    snapshot_relative: &SnapshotRelativeProgressFacts,
    code_paths: &CodePathFacts,
) -> &'static str {
    let has_real_progress = replay_state.state_present
        && replay_state.cursor_present
        && (replay_state.has_nonzero_rows_processed || replay_state.has_nonzero_chunks_completed);
    let cursor_lags_frontier = frontier_comparison.cursor_before_observed_swaps_frontier == Some(true)
        && frontier_comparison
            .cursor_lag_seconds_vs_observed_swaps_frontier
            .is_some_and(|lag| lag > 0);

    if has_real_progress
        && cursor_lags_frontier
        && snapshot_relative.updated_before_db_mtime == Some(true)
        && snapshot_relative
            .updated_lag_seconds_vs_db_mtime
            .is_some_and(|lag| lag > DB_MTIME_RECENT_PROGRESS_MAX_LAG_SECONDS)
    {
        return CONCLUSION_PROGRESS_STOPPED_BEFORE_SNAPSHOT;
    }

    if has_real_progress
        && cursor_lags_frontier
        && snapshot_relative
            .updated_lag_seconds_vs_db_mtime
            .is_some_and(|lag| (0..=DB_MTIME_RECENT_PROGRESS_MAX_LAG_SECONDS).contains(&lag))
    {
        return CONCLUSION_SOURCE_FRONTIER_OUTPACED_RECENT_PROGRESS;
    }

    if has_real_progress
        && (!replay_state.publishable || !replay_state.completed)
        && code_paths.publishable_depends_on_replay_cursor_reaching_required_frontier
        && code_paths.fail_closed_persists_while_replay_blocker_present
    {
        return CONCLUSION_PUBLISHABILITY_GATE_UNSATISFIED;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &ReplaySolLegLagCauseAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed to serialize replay sol leg lag cause audit report");
    }

    Ok(format!(
        concat!(
            "db_path={db_path}\n",
            "db_file_size_bytes={db_file_size_bytes}\n",
            "db_file_mtime_utc={db_file_mtime_utc}\n",
            "replay_state_present={replay_state_present}\n",
            "replay_phase={replay_phase}\n",
            "replay_rows_processed={replay_rows_processed}\n",
            "replay_pages_processed={replay_pages_processed}\n",
            "replay_chunks_completed={replay_chunks_completed}\n",
            "replay_started_at_utc={replay_started_at_utc}\n",
            "replay_updated_at_utc={replay_updated_at_utc}\n",
            "replay_cursor_ts_utc={replay_cursor_ts_utc}\n",
            "replay_cursor_slot={replay_cursor_slot}\n",
            "replay_cursor_signature={replay_cursor_signature}\n",
            "replay_publishable={replay_publishable}\n",
            "replay_completed={replay_completed}\n",
            "observed_swaps_max_ts_utc={observed_swaps_max_ts_utc}\n",
            "wallet_activity_days_max_day_utc={wallet_activity_days_max_day_utc}\n",
            "publication_window_start_utc={publication_window_start_utc}\n",
            "replay_cursor_before_publication_window_start={replay_cursor_before_publication_window_start}\n",
            "replay_cursor_before_observed_swaps_frontier={replay_cursor_before_observed_swaps_frontier}\n",
            "replay_cursor_lag_seconds_vs_observed_swaps_frontier={replay_cursor_lag_seconds_vs_observed_swaps_frontier}\n",
            "replay_cursor_lag_days_vs_observed_swaps_frontier={replay_cursor_lag_days_vs_observed_swaps_frontier}\n",
            "replay_updated_before_db_mtime={replay_updated_before_db_mtime}\n",
            "replay_updated_lag_seconds_vs_db_mtime={replay_updated_lag_seconds_vs_db_mtime}\n",
            "replay_updated_lag_days_vs_db_mtime={replay_updated_lag_days_vs_db_mtime}\n",
            "replay_updated_after_started={replay_updated_after_started}\n",
            "replay_has_nonzero_rows_processed={replay_has_nonzero_rows_processed}\n",
            "replay_has_nonzero_chunks_completed={replay_has_nonzero_chunks_completed}\n",
            "replay_cursor_present={replay_cursor_present}\n",
            "replay_state_persist_surface={replay_state_persist_surface}\n",
            "replay_chunk_progress_update_surface={replay_chunk_progress_update_surface}\n",
            "replay_publishable_checkpoint_surface={replay_publishable_checkpoint_surface}\n",
            "replay_sol_leg_blocker_surface={replay_sol_leg_blocker_surface}\n",
            "publishable_depends_on_replay_cursor_reaching_required_frontier={publishable_depends_on_replay_cursor_reaching_required_frontier}\n",
            "fail_closed_persists_while_replay_blocker_present={fail_closed_persists_while_replay_blocker_present}\n",
            "replay_sol_leg_lag_cause_conclusion={replay_sol_leg_lag_cause_conclusion}\n",
        ),
        db_path = report.db_path,
        db_file_size_bytes = report.db_file_size_bytes,
        db_file_mtime_utc = report.db_file_mtime_utc.to_rfc3339(),
        replay_state_present = report.replay_state_present,
        replay_phase = format_optional_str(report.replay_phase.as_deref()),
        replay_rows_processed = format_optional_u64(report.replay_rows_processed),
        replay_pages_processed = format_optional_u64(report.replay_pages_processed),
        replay_chunks_completed = format_optional_u64(report.replay_chunks_completed),
        replay_started_at_utc = format_optional_ts(report.replay_started_at_utc),
        replay_updated_at_utc = format_optional_ts(report.replay_updated_at_utc),
        replay_cursor_ts_utc = format_optional_ts(report.replay_cursor_ts_utc),
        replay_cursor_slot = format_optional_u64(report.replay_cursor_slot),
        replay_cursor_signature = format_optional_str(report.replay_cursor_signature.as_deref()),
        replay_publishable = report.replay_publishable,
        replay_completed = report.replay_completed,
        observed_swaps_max_ts_utc = format_optional_ts(report.observed_swaps_max_ts_utc),
        wallet_activity_days_max_day_utc =
            format_optional_ts(report.wallet_activity_days_max_day_utc),
        publication_window_start_utc = report.publication_window_start_utc.to_rfc3339(),
        replay_cursor_before_publication_window_start =
            format_optional_bool(report.replay_cursor_before_publication_window_start),
        replay_cursor_before_observed_swaps_frontier =
            format_optional_bool(report.replay_cursor_before_observed_swaps_frontier),
        replay_cursor_lag_seconds_vs_observed_swaps_frontier =
            format_optional_i64(report.replay_cursor_lag_seconds_vs_observed_swaps_frontier),
        replay_cursor_lag_days_vs_observed_swaps_frontier =
            format_optional_i64(report.replay_cursor_lag_days_vs_observed_swaps_frontier),
        replay_updated_before_db_mtime =
            format_optional_bool(report.replay_updated_before_db_mtime),
        replay_updated_lag_seconds_vs_db_mtime =
            format_optional_i64(report.replay_updated_lag_seconds_vs_db_mtime),
        replay_updated_lag_days_vs_db_mtime =
            format_optional_i64(report.replay_updated_lag_days_vs_db_mtime),
        replay_updated_after_started = report.replay_updated_after_started,
        replay_has_nonzero_rows_processed = report.replay_has_nonzero_rows_processed,
        replay_has_nonzero_chunks_completed = report.replay_has_nonzero_chunks_completed,
        replay_cursor_present = report.replay_cursor_present,
        replay_state_persist_surface = report.replay_state_persist_surface,
        replay_chunk_progress_update_surface = report.replay_chunk_progress_update_surface,
        replay_publishable_checkpoint_surface = report.replay_publishable_checkpoint_surface,
        replay_sol_leg_blocker_surface = report.replay_sol_leg_blocker_surface,
        publishable_depends_on_replay_cursor_reaching_required_frontier = report
            .publishable_depends_on_replay_cursor_reaching_required_frontier,
        fail_closed_persists_while_replay_blocker_present =
            report.fail_closed_persists_while_replay_blocker_present,
        replay_sol_leg_lag_cause_conclusion = report.replay_sol_leg_lag_cause_conclusion,
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
    use copybot_core_types::SwapEvent;
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode,
        WalletActivityDayRow,
    };
    use serde_json::json;
    use tempfile::TempDir;

    fn repo_config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }

    fn create_fixture() -> Result<(TempDir, PathBuf, SqliteStore)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let db_path = temp.path().join("replay-sol-leg-lag-cause-audit.db");
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

    fn swap(signature: &str, wallet: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenAudit111111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    fn seed_publication_state(store: &SqliteStore) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-top".to_string()]),
        })
    }

    fn seed_frontier_context(store: &SqliteStore) -> Result<()> {
        let ts = DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap("sig-stale", "wallet-stale", ts, 1))?;
        store.upsert_wallet_activity_days(&[WalletActivityDayRow {
            wallet_id: "wallet-stale".to_string(),
            activity_day: ts.date_naive(),
            last_seen: ts,
        }])?;
        Ok(())
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
    fn parse_args_reads_required_replay_sol_leg_lag_cause_audit_flags() -> Result<()> {
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
        seed_publication_state(&store)?;
        seed_frontier_context(&store)?;

        let report = run(&fixture_config(&db_path))?;
        let json = serde_json::to_value(&report)?;
        for key in [
            "db_path",
            "db_file_size_bytes",
            "db_file_mtime_utc",
            "replay_state_present",
            "replay_phase",
            "replay_rows_processed",
            "replay_pages_processed",
            "replay_chunks_completed",
            "replay_started_at_utc",
            "replay_updated_at_utc",
            "replay_cursor_ts_utc",
            "replay_cursor_slot",
            "replay_cursor_signature",
            "replay_publishable",
            "replay_completed",
            "observed_swaps_max_ts_utc",
            "wallet_activity_days_max_day_utc",
            "publication_window_start_utc",
            "replay_cursor_before_publication_window_start",
            "replay_cursor_before_observed_swaps_frontier",
            "replay_cursor_lag_seconds_vs_observed_swaps_frontier",
            "replay_cursor_lag_days_vs_observed_swaps_frontier",
            "replay_updated_before_db_mtime",
            "replay_updated_lag_seconds_vs_db_mtime",
            "replay_updated_lag_days_vs_db_mtime",
            "replay_updated_after_started",
            "replay_has_nonzero_rows_processed",
            "replay_has_nonzero_chunks_completed",
            "replay_cursor_present",
            "replay_state_persist_surface",
            "replay_chunk_progress_update_surface",
            "replay_publishable_checkpoint_surface",
            "replay_sol_leg_blocker_surface",
            "publishable_depends_on_replay_cursor_reaching_required_frontier",
            "fail_closed_persists_while_replay_blocker_present",
            "replay_sol_leg_lag_cause_conclusion",
        ] {
            assert!(json.get(key).is_some(), "missing JSON key {key}");
        }
        Ok(())
    }

    #[test]
    fn fixture_with_old_replay_update_and_lagging_cursor_yields_stopped_before_snapshot_conclusion(
    ) -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_publication_state(&store)?;
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
                rows_processed: 100,
                pages_processed: 5,
                chunks_completed: 2,
                started_at,
                updated_at,
                replay_wallet_stats_complete: true,
                replay_candidate_activity_backfill_pending: false,
            },
        )?;
        seed_frontier_context(&store)?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_sol_leg_lag_cause_conclusion,
            CONCLUSION_PROGRESS_STOPPED_BEFORE_SNAPSHOT
        );
        assert_eq!(report.replay_cursor_before_observed_swaps_frontier, Some(true));
        assert_eq!(report.replay_updated_before_db_mtime, Some(true));
        assert!(
            report.replay_updated_lag_seconds_vs_db_mtime.unwrap_or(0)
                > DB_MTIME_RECENT_PROGRESS_MAX_LAG_SECONDS
        );
        Ok(())
    }

    #[test]
    fn fixture_with_recent_replay_update_and_lagging_cursor_yields_source_outpaced_conclusion(
    ) -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_publication_state(&store)?;
        seed_frontier_context(&store)?;
        let now = Utc::now();
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
                rows_processed: 100,
                pages_processed: 5,
                chunks_completed: 2,
                started_at: now - Duration::minutes(5),
                updated_at: now,
                replay_wallet_stats_complete: true,
                replay_candidate_activity_backfill_pending: false,
            },
        )?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_sol_leg_lag_cause_conclusion,
            CONCLUSION_SOURCE_FRONTIER_OUTPACED_RECENT_PROGRESS
        );
        assert_eq!(report.replay_cursor_before_observed_swaps_frontier, Some(true));
        assert!(
            report.replay_updated_lag_seconds_vs_db_mtime.unwrap_or(i64::MAX)
                <= DB_MTIME_RECENT_PROGRESS_MAX_LAG_SECONDS
        );
        Ok(())
    }

    #[test]
    fn ambiguous_persisted_facts_never_overclaim_beyond_allowed_conclusions() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_publication_state(&store)?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_sol_leg_lag_cause_conclusion,
            CONCLUSION_INSUFFICIENT_EVIDENCE
        );
        Ok(())
    }

    #[test]
    fn repeated_runs_with_same_inputs_are_deterministic() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_publication_state(&store)?;
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
                rows_processed: 100,
                pages_processed: 5,
                chunks_completed: 2,
                started_at,
                updated_at,
                replay_wallet_stats_complete: true,
                replay_candidate_activity_backfill_pending: false,
            },
        )?;
        seed_frontier_context(&store)?;

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
