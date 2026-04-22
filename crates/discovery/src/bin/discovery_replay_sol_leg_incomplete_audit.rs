use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, DiscoveryConfig};
use copybot_discovery::{
    wallet_freshness_audit::wallet_freshness_capture_from_row, DiscoveryService,
};
use copybot_storage::{DiscoveryWalletFreshnessCaptureRow, SqliteStore};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_replay_sol_leg_incomplete_audit --db <path> --config <path> [--json]";

const CONCLUSION_CHECKPOINT_STATE_MISSING: &str =
    "replay_sol_leg_incomplete_because_checkpoint_state_missing";
const CONCLUSION_CHECKPOINT_STATE_PRESENT_BUT_NOT_COMPLETED: &str =
    "replay_sol_leg_incomplete_because_checkpoint_state_present_but_not_completed";
const CONCLUSION_CHECKPOINT_COMPLETED_BUT_FRONTIER_STALE: &str =
    "replay_sol_leg_incomplete_because_checkpoint_completed_but_frontier_stale";
const CONCLUSION_PUBLICATION_TRUTH_READS_FAIL_CLOSED_INCOMPLETE_REPLAY: &str =
    "replay_sol_leg_incomplete_because_publication_truth_reads_fail_closed_incomplete_replay_state";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const PUBLICATION_TRUTH_WITHHELD_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persist_publication_state";
const REPLAY_SOL_LEG_INCOMPLETE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persisted_stream_publishable_checkpoint_blocker_from_state";
const REPLAY_CHECKPOINT_READ_SURFACE: &str =
    "copybot_storage::SqliteStore::load_discovery_persisted_rebuild_state_read_only";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");
const STORAGE_DISCOVERY_SOURCE: &str = include_str!("../../../storage/src/discovery.rs");
const APP_MAIN_SOURCE: &str = include_str!("../../../app/src/main.rs");

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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReplaySolLegIncompleteAuditReport {
    publication_runtime_mode: Option<String>,
    publication_reason: Option<String>,
    publication_last_published_at_utc: Option<DateTime<Utc>>,
    publication_last_published_window_start_utc: Option<DateTime<Utc>>,
    publication_published_wallet_count: Option<u64>,
    publication_truth_withheld: bool,
    latest_freshness_captured_at_utc: Option<DateTime<Utc>>,
    latest_freshness_runtime_mode: Option<String>,
    latest_freshness_reason: Option<String>,
    latest_freshness_raw_window_complete: Option<bool>,
    latest_freshness_scoring_source: Option<String>,
    latest_freshness_published_wallet_count: Option<u64>,
    latest_freshness_active_follow_wallet_count: Option<u64>,
    replay_sol_leg_state_present: bool,
    replay_sol_leg_status: Option<String>,
    replay_sol_leg_reason: Option<String>,
    replay_sol_leg_phase: Option<String>,
    replay_sol_leg_last_started_at_utc: Option<DateTime<Utc>>,
    replay_sol_leg_last_completed_at_utc: Option<DateTime<Utc>>,
    replay_sol_leg_rows_processed: Option<u64>,
    replay_sol_leg_materialized_cursor_ts_utc: Option<DateTime<Utc>>,
    replay_sol_leg_materialized_frontier_before_publication_window_start: Option<bool>,
    replay_sol_leg_completed: bool,
    replay_sol_leg_publishable: bool,
    replay_sol_leg_state_source: Option<String>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    publication_window_start_utc: DateTime<Utc>,
    publication_truth_withheld_surface: String,
    replay_sol_leg_incomplete_surface: String,
    replay_checkpoint_read_surface: String,
    publication_truth_depends_on_replay_sol_leg_completion: bool,
    fail_closed_runtime_depends_on_withheld_publication_truth: bool,
    replay_sol_leg_incomplete_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct PublicationStateFacts {
    runtime_mode: Option<String>,
    reason: Option<String>,
    last_published_at_utc: Option<DateTime<Utc>>,
    last_published_window_start_utc: Option<DateTime<Utc>>,
    published_wallet_count: Option<u64>,
    truth_withheld: bool,
    updated_at_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct LatestFreshnessFacts {
    captured_at_utc: Option<DateTime<Utc>>,
    runtime_mode: Option<String>,
    reason: Option<String>,
    raw_window_complete: Option<bool>,
    scoring_source: Option<String>,
    published_wallet_count: Option<u64>,
    active_follow_wallet_count: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct ReplaySolLegFacts {
    state_present: bool,
    status: Option<String>,
    reason: Option<String>,
    phase: Option<String>,
    last_started_at_utc: Option<DateTime<Utc>>,
    last_completed_at_utc: Option<DateTime<Utc>>,
    rows_processed: Option<u64>,
    materialized_cursor_ts_utc: Option<DateTime<Utc>>,
    materialized_frontier_before_publication_window_start: Option<bool>,
    completed: bool,
    publishable: bool,
    state_source: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ContextFrontierFacts {
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    publication_truth_depends_on_replay_sol_leg_completion: bool,
    fail_closed_runtime_depends_on_withheld_publication_truth: bool,
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

fn run(config: &Config) -> Result<ReplaySolLegIncompleteAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let conn = open_read_only_connection(&config.db_path)?;

    let publication_state = load_publication_state_facts(&store)?;
    let latest_freshness = load_latest_freshness_facts(&conn)?;
    let publication_window_anchor = publication_window_anchor_utc(
        &publication_state,
        &latest_freshness,
        &conn,
    )?;
    let publication_window_start_utc =
        metrics_window_start(&loaded_config.discovery, publication_window_anchor);
    let replay_sol_leg = load_replay_sol_leg_facts(&store, publication_window_start_utc)?;
    let frontier = load_context_frontier_facts(&conn)?;
    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(
        &publication_state,
        &replay_sol_leg,
        &code_paths,
    );

    Ok(ReplaySolLegIncompleteAuditReport {
        publication_runtime_mode: publication_state.runtime_mode,
        publication_reason: publication_state.reason,
        publication_last_published_at_utc: publication_state.last_published_at_utc,
        publication_last_published_window_start_utc: publication_state.last_published_window_start_utc,
        publication_published_wallet_count: publication_state.published_wallet_count,
        publication_truth_withheld: publication_state.truth_withheld,
        latest_freshness_captured_at_utc: latest_freshness.captured_at_utc,
        latest_freshness_runtime_mode: latest_freshness.runtime_mode,
        latest_freshness_reason: latest_freshness.reason,
        latest_freshness_raw_window_complete: latest_freshness.raw_window_complete,
        latest_freshness_scoring_source: latest_freshness.scoring_source,
        latest_freshness_published_wallet_count: latest_freshness.published_wallet_count,
        latest_freshness_active_follow_wallet_count: latest_freshness.active_follow_wallet_count,
        replay_sol_leg_state_present: replay_sol_leg.state_present,
        replay_sol_leg_status: replay_sol_leg.status,
        replay_sol_leg_reason: replay_sol_leg.reason,
        replay_sol_leg_phase: replay_sol_leg.phase,
        replay_sol_leg_last_started_at_utc: replay_sol_leg.last_started_at_utc,
        replay_sol_leg_last_completed_at_utc: replay_sol_leg.last_completed_at_utc,
        replay_sol_leg_rows_processed: replay_sol_leg.rows_processed,
        replay_sol_leg_materialized_cursor_ts_utc: replay_sol_leg.materialized_cursor_ts_utc,
        replay_sol_leg_materialized_frontier_before_publication_window_start: replay_sol_leg
            .materialized_frontier_before_publication_window_start,
        replay_sol_leg_completed: replay_sol_leg.completed,
        replay_sol_leg_publishable: replay_sol_leg.publishable,
        replay_sol_leg_state_source: replay_sol_leg.state_source,
        observed_swaps_max_ts_utc: frontier.observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc: frontier.wallet_activity_days_max_day_utc,
        publication_window_start_utc,
        publication_truth_withheld_surface: PUBLICATION_TRUTH_WITHHELD_SURFACE.to_string(),
        replay_sol_leg_incomplete_surface: REPLAY_SOL_LEG_INCOMPLETE_SURFACE.to_string(),
        replay_checkpoint_read_surface: REPLAY_CHECKPOINT_READ_SURFACE.to_string(),
        publication_truth_depends_on_replay_sol_leg_completion: code_paths
            .publication_truth_depends_on_replay_sol_leg_completion,
        fail_closed_runtime_depends_on_withheld_publication_truth: code_paths
            .fail_closed_runtime_depends_on_withheld_publication_truth,
        replay_sol_leg_incomplete_conclusion: conclusion.to_string(),
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
    let published_wallet_count = state
        .published_wallet_ids
        .as_ref()
        .map(|wallet_ids| wallet_ids.len() as u64)
        .or(Some(0));
    let truth_withheld = state.runtime_mode.as_str() == "fail_closed"
        && state.reason.starts_with("publication_truth_withheld_");

    Ok(PublicationStateFacts {
        runtime_mode: Some(state.runtime_mode.as_str().to_string()),
        reason: Some(state.reason),
        last_published_at_utc: state.last_published_at,
        last_published_window_start_utc: state.last_published_window_start,
        published_wallet_count,
        truth_withheld,
        updated_at_utc: Some(state.updated_at),
    })
}

fn load_latest_freshness_facts(conn: &Connection) -> Result<LatestFreshnessFacts> {
    if !sqlite_table_exists(conn, "discovery_wallet_freshness_history")? {
        return Ok(LatestFreshnessFacts::default());
    }

    type RawFreshnessCaptureRow = (
        i64,
        String,
        i64,
        String,
        String,
        Option<i64>,
        i64,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    );

    let raw: Option<RawFreshnessCaptureRow> = conn
        .query_row(
            "SELECT
                capture_id,
                captured_at,
                recent_cycles,
                verdict,
                reason,
                publication_age_seconds,
                raw_truth_sufficient,
                raw_truth_reason,
                shadow_signal_verdict,
                shadow_signal_reason,
                published_wallet_ids_json,
                active_follow_wallet_ids_json,
                current_raw_top_wallet_ids_json,
                audit_json,
                shadow_signal_json
             FROM discovery_wallet_freshness_history
             ORDER BY captured_at DESC, capture_id DESC
             LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, String>(14)?,
                ))
            },
        )
        .optional()
        .context("failed loading latest discovery wallet freshness history row")?;
    let Some(raw) = raw else {
        return Ok(LatestFreshnessFacts::default());
    };

    let row = DiscoveryWalletFreshnessCaptureRow {
        capture_id: raw.0,
        captured_at: parse_db_ts(&raw.1)?,
        recent_cycles: raw.2.max(1) as usize,
        verdict: raw.3,
        reason: raw.4,
        publication_age_seconds: raw.5.map(|value| value.max(0) as u64),
        raw_truth_sufficient: raw.6 != 0,
        raw_truth_reason: raw.7,
        shadow_signal_verdict: raw.8,
        shadow_signal_reason: raw.9,
        published_wallet_ids: parse_wallet_ids_json(&raw.10)?,
        active_follow_wallet_ids: parse_wallet_ids_json(&raw.11)?,
        current_raw_top_wallet_ids: parse_wallet_ids_json(&raw.12)?,
        audit_json: raw.13,
        shadow_signal_json: raw.14,
    };
    let snapshot = wallet_freshness_capture_from_row(row.clone())?;

    Ok(LatestFreshnessFacts {
        captured_at_utc: Some(row.captured_at),
        runtime_mode: snapshot.audit.publication_runtime_mode,
        reason: Some(row.reason),
        raw_window_complete: Some(snapshot.audit.raw_truth.sufficient),
        scoring_source: snapshot.audit.published_scoring_source,
        published_wallet_count: Some(snapshot.audit.published_wallet_ids.len() as u64),
        active_follow_wallet_count: Some(snapshot.audit.active_follow_wallet_ids.len() as u64),
    })
}

fn parse_wallet_ids_json(raw: &str) -> Result<Vec<String>> {
    serde_json::from_str(raw).with_context(|| format!("invalid wallet ids JSON payload: {raw}"))
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
    latest_freshness: &LatestFreshnessFacts,
    conn: &Connection,
) -> Result<DateTime<Utc>> {
    if let Some(updated_at) = publication_state.updated_at_utc {
        return Ok(updated_at);
    }
    if let Some(captured_at) = latest_freshness.captured_at_utc {
        return Ok(captured_at);
    }
    if let Some(last_published_at) = publication_state.last_published_at_utc {
        return Ok(last_published_at);
    }
    if let Some(max_ts) = query_observed_swaps_max_ts_utc(conn)? {
        return Ok(max_ts);
    }
    if let Some(max_day) = query_wallet_activity_days_max_day_utc(conn)? {
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

fn load_replay_sol_leg_facts(
    store: &SqliteStore,
    publication_window_start_utc: DateTime<Utc>,
) -> Result<ReplaySolLegFacts> {
    let inspection = DiscoveryService::inspect_persisted_rebuild_state_read_only(store)?;
    let raw_row = store.load_discovery_persisted_rebuild_state_read_only()?;
    if !inspection.persisted_rebuild_checkpoint_exists {
        return Ok(ReplaySolLegFacts::default());
    }

    let completed = !inspection.replay_incomplete;
    let publishable = inspection.publishable_checkpoint_blocker.is_none();
    let status = Some(if publishable {
        "publishable".to_string()
    } else if completed {
        "completed".to_string()
    } else {
        "incomplete".to_string()
    });
    let materialized_cursor_ts_utc = raw_row
        .as_ref()
        .and_then(|row| row.phase_cursor.as_ref().map(|cursor| cursor.ts_utc))
        .or_else(|| inspection.phase_cursor.as_ref().map(|cursor| cursor.ts_utc));
    let materialized_frontier_before_publication_window_start = materialized_cursor_ts_utc
        .map(|cursor_ts| cursor_ts < publication_window_start_utc);

    Ok(ReplaySolLegFacts {
        state_present: true,
        status,
        reason: inspection.publishable_checkpoint_blocker,
        phase: inspection.rebuild_phase,
        last_started_at_utc: raw_row.as_ref().map(|row| row.started_at),
        last_completed_at_utc: None,
        rows_processed: raw_row
            .as_ref()
            .map(|row| row.replay_rows_processed as u64)
            .or_else(|| inspection.replay_rows_processed.map(|rows| rows as u64)),
        materialized_cursor_ts_utc,
        materialized_frontier_before_publication_window_start,
        completed,
        publishable,
        state_source: Some("discovery_persisted_rebuild_state".to_string()),
    })
}

fn load_context_frontier_facts(conn: &Connection) -> Result<ContextFrontierFacts> {
    Ok(ContextFrontierFacts {
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

fn inspect_code_path_facts() -> CodePathFacts {
    let publication_truth_depends_on_replay_sol_leg_completion = DISCOVERY_SOURCE
        .contains("effective_reason = format!(\"publication_truth_withheld_while_{checkpoint_blocker}\")")
        && DISCOVERY_SOURCE.contains("Some(\"sol_leg\") => \"replay_sol_leg_incomplete\"")
        && STORAGE_DISCOVERY_SOURCE.contains("pub fn discovery_publication_state_read_only(")
        && DISCOVERY_SOURCE.contains("fn persisted_stream_publishable_checkpoint_blocker_from_state(");
    let fail_closed_runtime_depends_on_withheld_publication_truth = APP_MAIN_SOURCE
        .contains("fn startup_runtime_publication_truth(")
        && APP_MAIN_SOURCE.contains("fn apply_fail_closed_runtime_follow_surface_if_needed(")
        && APP_MAIN_SOURCE
            .contains("DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded");

    CodePathFacts {
        publication_truth_depends_on_replay_sol_leg_completion,
        fail_closed_runtime_depends_on_withheld_publication_truth,
    }
}

fn choose_conclusion(
    publication_state: &PublicationStateFacts,
    replay_sol_leg: &ReplaySolLegFacts,
    code_paths: &CodePathFacts,
) -> &'static str {
    if !replay_sol_leg.state_present
        && code_paths.publication_truth_depends_on_replay_sol_leg_completion
    {
        return CONCLUSION_CHECKPOINT_STATE_MISSING;
    }

    if replay_sol_leg.completed
        && replay_sol_leg.materialized_frontier_before_publication_window_start == Some(true)
    {
        return CONCLUSION_CHECKPOINT_COMPLETED_BUT_FRONTIER_STALE;
    }

    if replay_sol_leg.state_present
        && (!replay_sol_leg.completed
            || replay_sol_leg
                .reason
                .as_deref()
                .is_some_and(|reason| reason == "replay_sol_leg_incomplete"))
    {
        return CONCLUSION_CHECKPOINT_STATE_PRESENT_BUT_NOT_COMPLETED;
    }

    if publication_state.runtime_mode.as_deref() == Some("fail_closed")
        && publication_state.truth_withheld
        && publication_state
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("replay_sol_leg_incomplete"))
        && code_paths.publication_truth_depends_on_replay_sol_leg_completion
        && code_paths.fail_closed_runtime_depends_on_withheld_publication_truth
    {
        return CONCLUSION_PUBLICATION_TRUTH_READS_FAIL_CLOSED_INCOMPLETE_REPLAY;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &ReplaySolLegIncompleteAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed to serialize replay sol leg incomplete audit report");
    }

    Ok(format!(
        concat!(
            "publication_runtime_mode={publication_runtime_mode}\n",
            "publication_reason={publication_reason}\n",
            "publication_last_published_at_utc={publication_last_published_at_utc}\n",
            "publication_last_published_window_start_utc={publication_last_published_window_start_utc}\n",
            "publication_published_wallet_count={publication_published_wallet_count}\n",
            "publication_truth_withheld={publication_truth_withheld}\n",
            "latest_freshness_captured_at_utc={latest_freshness_captured_at_utc}\n",
            "latest_freshness_runtime_mode={latest_freshness_runtime_mode}\n",
            "latest_freshness_reason={latest_freshness_reason}\n",
            "latest_freshness_raw_window_complete={latest_freshness_raw_window_complete}\n",
            "latest_freshness_scoring_source={latest_freshness_scoring_source}\n",
            "latest_freshness_published_wallet_count={latest_freshness_published_wallet_count}\n",
            "latest_freshness_active_follow_wallet_count={latest_freshness_active_follow_wallet_count}\n",
            "replay_sol_leg_state_present={replay_sol_leg_state_present}\n",
            "replay_sol_leg_status={replay_sol_leg_status}\n",
            "replay_sol_leg_reason={replay_sol_leg_reason}\n",
            "replay_sol_leg_phase={replay_sol_leg_phase}\n",
            "replay_sol_leg_last_started_at_utc={replay_sol_leg_last_started_at_utc}\n",
            "replay_sol_leg_last_completed_at_utc={replay_sol_leg_last_completed_at_utc}\n",
            "replay_sol_leg_rows_processed={replay_sol_leg_rows_processed}\n",
            "replay_sol_leg_materialized_cursor_ts_utc={replay_sol_leg_materialized_cursor_ts_utc}\n",
            "replay_sol_leg_materialized_frontier_before_publication_window_start={replay_sol_leg_materialized_frontier_before_publication_window_start}\n",
            "replay_sol_leg_completed={replay_sol_leg_completed}\n",
            "replay_sol_leg_publishable={replay_sol_leg_publishable}\n",
            "replay_sol_leg_state_source={replay_sol_leg_state_source}\n",
            "observed_swaps_max_ts_utc={observed_swaps_max_ts_utc}\n",
            "wallet_activity_days_max_day_utc={wallet_activity_days_max_day_utc}\n",
            "publication_window_start_utc={publication_window_start_utc}\n",
            "publication_truth_withheld_surface={publication_truth_withheld_surface}\n",
            "replay_sol_leg_incomplete_surface={replay_sol_leg_incomplete_surface}\n",
            "replay_checkpoint_read_surface={replay_checkpoint_read_surface}\n",
            "publication_truth_depends_on_replay_sol_leg_completion={publication_truth_depends_on_replay_sol_leg_completion}\n",
            "fail_closed_runtime_depends_on_withheld_publication_truth={fail_closed_runtime_depends_on_withheld_publication_truth}\n",
            "replay_sol_leg_incomplete_conclusion={replay_sol_leg_incomplete_conclusion}\n",
        ),
        publication_runtime_mode = format_optional_str(report.publication_runtime_mode.as_deref()),
        publication_reason = format_optional_str(report.publication_reason.as_deref()),
        publication_last_published_at_utc =
            format_optional_ts(report.publication_last_published_at_utc),
        publication_last_published_window_start_utc =
            format_optional_ts(report.publication_last_published_window_start_utc),
        publication_published_wallet_count =
            format_optional_u64(report.publication_published_wallet_count),
        publication_truth_withheld = report.publication_truth_withheld,
        latest_freshness_captured_at_utc =
            format_optional_ts(report.latest_freshness_captured_at_utc),
        latest_freshness_runtime_mode =
            format_optional_str(report.latest_freshness_runtime_mode.as_deref()),
        latest_freshness_reason = format_optional_str(report.latest_freshness_reason.as_deref()),
        latest_freshness_raw_window_complete =
            format_optional_bool(report.latest_freshness_raw_window_complete),
        latest_freshness_scoring_source =
            format_optional_str(report.latest_freshness_scoring_source.as_deref()),
        latest_freshness_published_wallet_count =
            format_optional_u64(report.latest_freshness_published_wallet_count),
        latest_freshness_active_follow_wallet_count =
            format_optional_u64(report.latest_freshness_active_follow_wallet_count),
        replay_sol_leg_state_present = report.replay_sol_leg_state_present,
        replay_sol_leg_status = format_optional_str(report.replay_sol_leg_status.as_deref()),
        replay_sol_leg_reason = format_optional_str(report.replay_sol_leg_reason.as_deref()),
        replay_sol_leg_phase = format_optional_str(report.replay_sol_leg_phase.as_deref()),
        replay_sol_leg_last_started_at_utc =
            format_optional_ts(report.replay_sol_leg_last_started_at_utc),
        replay_sol_leg_last_completed_at_utc =
            format_optional_ts(report.replay_sol_leg_last_completed_at_utc),
        replay_sol_leg_rows_processed = format_optional_u64(report.replay_sol_leg_rows_processed),
        replay_sol_leg_materialized_cursor_ts_utc =
            format_optional_ts(report.replay_sol_leg_materialized_cursor_ts_utc),
        replay_sol_leg_materialized_frontier_before_publication_window_start =
            format_optional_bool(report.replay_sol_leg_materialized_frontier_before_publication_window_start),
        replay_sol_leg_completed = report.replay_sol_leg_completed,
        replay_sol_leg_publishable = report.replay_sol_leg_publishable,
        replay_sol_leg_state_source =
            format_optional_str(report.replay_sol_leg_state_source.as_deref()),
        observed_swaps_max_ts_utc = format_optional_ts(report.observed_swaps_max_ts_utc),
        wallet_activity_days_max_day_utc =
            format_optional_ts(report.wallet_activity_days_max_day_utc),
        publication_window_start_utc = report.publication_window_start_utc.to_rfc3339(),
        publication_truth_withheld_surface = report.publication_truth_withheld_surface,
        replay_sol_leg_incomplete_surface = report.replay_sol_leg_incomplete_surface,
        replay_checkpoint_read_surface = report.replay_checkpoint_read_surface,
        publication_truth_depends_on_replay_sol_leg_completion =
            report.publication_truth_depends_on_replay_sol_leg_completion,
        fail_closed_runtime_depends_on_withheld_publication_truth =
            report.fail_closed_runtime_depends_on_withheld_publication_truth,
        replay_sol_leg_incomplete_conclusion = report.replay_sol_leg_incomplete_conclusion,
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
        DiscoveryPublicationStateUpdate, WalletActivityDayRow,
    };
    use serde_json::json;
    use tempfile::TempDir;

    fn repo_config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }

    fn create_fixture() -> Result<(TempDir, PathBuf, SqliteStore)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let db_path = temp.path().join("replay-sol-leg-incomplete-audit.db");
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

    fn seed_rebuild_state(
        store: &SqliteStore,
        phase: DiscoveryPersistedRebuildPhase,
        phase_cursor_ts: Option<DateTime<Utc>>,
        replay_wallet_stats_complete: bool,
        replay_candidate_activity_backfill_pending: bool,
    ) -> Result<()> {
        let started_at = DateTime::parse_from_rfc3339("2026-04-21T15:50:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let updated_at = DateTime::parse_from_rfc3339("2026-04-21T16:05:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let window_start = DateTime::parse_from_rfc3339("2026-04-16T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let horizon_end = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let phase_cursor = phase_cursor_ts.map(|ts| copybot_storage::DiscoveryRuntimeCursor {
            ts_utc: ts,
            slot: 42,
            signature: "sig-cursor".to_string(),
        });
        store.upsert_discovery_persisted_rebuild_state(&DiscoveryPersistedRebuildStateRow {
            phase,
            window_start,
            horizon_end,
            metrics_window_start: window_start,
            phase_cursor,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 12,
            replay_pages_processed: 3,
            chunks_completed: 1,
            state_json: minimal_rebuild_state_json(
                replay_wallet_stats_complete,
                replay_candidate_activity_backfill_pending,
            ),
            started_at,
            updated_at,
        })
    }

    #[test]
    fn parse_args_reads_required_replay_sol_leg_incomplete_audit_flags() -> Result<()> {
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
        seed_frontier_context(&store)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-top".to_string()]),
        })?;

        let report = run(&fixture_config(&db_path))?;
        let json = serde_json::to_value(&report)?;
        for key in [
            "publication_runtime_mode",
            "publication_reason",
            "publication_last_published_at_utc",
            "publication_last_published_window_start_utc",
            "publication_published_wallet_count",
            "publication_truth_withheld",
            "latest_freshness_captured_at_utc",
            "latest_freshness_runtime_mode",
            "latest_freshness_reason",
            "latest_freshness_raw_window_complete",
            "latest_freshness_scoring_source",
            "latest_freshness_published_wallet_count",
            "latest_freshness_active_follow_wallet_count",
            "replay_sol_leg_state_present",
            "replay_sol_leg_status",
            "replay_sol_leg_reason",
            "replay_sol_leg_phase",
            "replay_sol_leg_last_started_at_utc",
            "replay_sol_leg_last_completed_at_utc",
            "replay_sol_leg_rows_processed",
            "replay_sol_leg_materialized_cursor_ts_utc",
            "replay_sol_leg_materialized_frontier_before_publication_window_start",
            "replay_sol_leg_completed",
            "replay_sol_leg_publishable",
            "replay_sol_leg_state_source",
            "observed_swaps_max_ts_utc",
            "wallet_activity_days_max_day_utc",
            "publication_window_start_utc",
            "publication_truth_withheld_surface",
            "replay_sol_leg_incomplete_surface",
            "replay_checkpoint_read_surface",
            "publication_truth_depends_on_replay_sol_leg_completion",
            "fail_closed_runtime_depends_on_withheld_publication_truth",
            "replay_sol_leg_incomplete_conclusion",
        ] {
            assert!(json.get(key).is_some(), "missing JSON key {key}");
        }
        Ok(())
    }

    #[test]
    fn missing_replay_sol_leg_state_yields_missing_checkpoint_conclusion() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_frontier_context(&store)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-top".to_string()]),
        })?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_sol_leg_incomplete_conclusion,
            CONCLUSION_CHECKPOINT_STATE_MISSING
        );
        assert!(!report.replay_sol_leg_state_present);
        Ok(())
    }

    #[test]
    fn replay_sol_leg_state_present_but_incomplete_yields_incomplete_checkpoint_conclusion(
    ) -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_frontier_context(&store)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-top".to_string()]),
        })?;
        seed_rebuild_state(
            &store,
            DiscoveryPersistedRebuildPhase::Replay,
            Some(
                DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                    .expect("valid timestamp")
                    .with_timezone(&Utc),
            ),
            true,
            false,
        )?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_sol_leg_incomplete_conclusion,
            CONCLUSION_CHECKPOINT_STATE_PRESENT_BUT_NOT_COMPLETED
        );
        assert!(report.replay_sol_leg_state_present);
        assert_eq!(report.replay_sol_leg_phase.as_deref(), Some("replay"));
        assert_eq!(
            report.replay_sol_leg_reason.as_deref(),
            Some("replay_sol_leg_incomplete")
        );
        assert!(!report.replay_sol_leg_completed);
        Ok(())
    }

    #[test]
    fn ambiguous_persisted_state_never_overclaims_stronger_than_allowed_conclusions() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_frontier_context(&store)?;
        seed_rebuild_state(
            &store,
            DiscoveryPersistedRebuildPhase::PublishPending,
            Some(
                DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                    .expect("valid timestamp")
                    .with_timezone(&Utc),
            ),
            true,
            false,
        )?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.replay_sol_leg_incomplete_conclusion,
            CONCLUSION_INSUFFICIENT_EVIDENCE
        );
        Ok(())
    }

    #[test]
    fn repeated_runs_with_same_inputs_are_deterministic() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        seed_frontier_context(&store)?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-top".to_string()]),
        })?;
        seed_rebuild_state(
            &store,
            DiscoveryPersistedRebuildPhase::Replay,
            Some(
                DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                    .expect("valid timestamp")
                    .with_timezone(&Utc),
            ),
            true,
            false,
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
}
