use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, DiscoveryConfig};
use copybot_discovery::wallet_freshness_audit::wallet_freshness_capture_from_row;
use copybot_storage::{DiscoveryRuntimeMode, DiscoveryWalletFreshnessCaptureRow, SqliteStore};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const USAGE: &str =
    "usage: discovery_publication_truth_withheld_audit --db <path> --config <path> [--json]";

const CONCLUSION_RAW_WINDOW_INCOMPLETE: &str =
    "publication_truth_withheld_because_raw_window_is_incomplete";
const CONCLUSION_NO_RECENT_PUBLISHABLE_UNIVERSE: &str =
    "publication_truth_withheld_because_no_recent_publishable_universe_exists";
const CONCLUSION_FAIL_CLOSED_PERSISTS_AFTER_INCOMPLETE_REPLAY: &str =
    "publication_truth_withheld_because_fail_closed_runtime_persists_after_incomplete_replay";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const PUBLICATION_TRUTH_GATE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persist_publication_state";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");
const APP_MAIN_SOURCE: &str = include_str!("../../../app/src/main.rs");
const STORAGE_DISCOVERY_SOURCE: &str = include_str!("../../../storage/src/discovery.rs");

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
struct PublicationTruthWithheldAuditReport {
    publication_runtime_mode: Option<String>,
    publication_reason: Option<String>,
    publication_last_published_at_utc: Option<DateTime<Utc>>,
    publication_last_published_window_start_utc: Option<DateTime<Utc>>,
    publication_published_wallet_count: Option<u64>,
    publication_has_wallet_ids: bool,
    publication_truth_withheld: bool,
    latest_freshness_captured_at_utc: Option<DateTime<Utc>>,
    latest_freshness_runtime_mode: Option<String>,
    latest_freshness_reason: Option<String>,
    latest_freshness_scoring_source: Option<String>,
    latest_freshness_recent_under_gate: Option<bool>,
    latest_freshness_raw_window_complete: Option<bool>,
    latest_freshness_published_wallet_count: Option<u64>,
    latest_freshness_active_follow_wallet_count: Option<u64>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    observed_swaps_total_rows: Option<u64>,
    wallet_activity_days_total_rows: Option<u64>,
    proof_window_start_utc: DateTime<Utc>,
    observed_swaps_rows_since_proof_window_start: Option<u64>,
    wallet_activity_days_rows_since_proof_window_start: Option<u64>,
    observed_swaps_frontier_before_proof_window_start: bool,
    wallet_activity_days_frontier_before_proof_window_start: bool,
    publication_truth_gate_surface: String,
    replay_sol_leg_incomplete_surface_present: bool,
    raw_window_incomplete_no_recent_published_universe_surface_present: bool,
    fail_closed_runtime_surface_present: bool,
    publication_truth_can_be_withheld_when_replay_sol_leg_incomplete: bool,
    published_follow_universe_depends_on_recent_publishable_truth: bool,
    publication_truth_withheld_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct PublicationStateFacts {
    runtime_mode: Option<String>,
    reason: Option<String>,
    last_published_at_utc: Option<DateTime<Utc>>,
    last_published_window_start_utc: Option<DateTime<Utc>>,
    published_wallet_count: Option<u64>,
    has_wallet_ids: bool,
    truth_withheld: bool,
    updated_at_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct LatestFreshnessFacts {
    captured_at_utc: Option<DateTime<Utc>>,
    runtime_mode: Option<String>,
    reason: Option<String>,
    scoring_source: Option<String>,
    recent_under_gate: Option<bool>,
    raw_window_complete: Option<bool>,
    published_wallet_count: Option<u64>,
    active_follow_wallet_count: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct FrontierFacts {
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    observed_swaps_total_rows: Option<u64>,
    wallet_activity_days_total_rows: Option<u64>,
    observed_swaps_rows_since_proof_window_start: Option<u64>,
    wallet_activity_days_rows_since_proof_window_start: Option<u64>,
    observed_swaps_frontier_before_proof_window_start: bool,
    wallet_activity_days_frontier_before_proof_window_start: bool,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    replay_sol_leg_incomplete_surface_present: bool,
    raw_window_incomplete_no_recent_published_universe_surface_present: bool,
    fail_closed_runtime_surface_present: bool,
    publication_truth_can_be_withheld_when_replay_sol_leg_incomplete: bool,
    published_follow_universe_depends_on_recent_publishable_truth: bool,
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

fn run(config: &Config) -> Result<PublicationTruthWithheldAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let conn = open_read_only_connection(&config.db_path)?;

    let publication_state = load_publication_state_facts(&store)?;
    let latest_freshness = load_latest_freshness_facts(&conn)?;
    let proof_window_anchor = proof_window_anchor_utc(&publication_state, &latest_freshness, &conn)?;
    let proof_window_start_utc = metrics_window_start(&loaded_config.discovery, proof_window_anchor);
    let frontier = load_frontier_facts(&conn, proof_window_start_utc)?;
    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(
        &publication_state,
        &latest_freshness,
        &frontier,
        &code_paths,
    );

    Ok(PublicationTruthWithheldAuditReport {
        publication_runtime_mode: publication_state.runtime_mode,
        publication_reason: publication_state.reason,
        publication_last_published_at_utc: publication_state.last_published_at_utc,
        publication_last_published_window_start_utc: publication_state.last_published_window_start_utc,
        publication_published_wallet_count: publication_state.published_wallet_count,
        publication_has_wallet_ids: publication_state.has_wallet_ids,
        publication_truth_withheld: publication_state.truth_withheld,
        latest_freshness_captured_at_utc: latest_freshness.captured_at_utc,
        latest_freshness_runtime_mode: latest_freshness.runtime_mode,
        latest_freshness_reason: latest_freshness.reason,
        latest_freshness_scoring_source: latest_freshness.scoring_source,
        latest_freshness_recent_under_gate: latest_freshness.recent_under_gate,
        latest_freshness_raw_window_complete: latest_freshness.raw_window_complete,
        latest_freshness_published_wallet_count: latest_freshness.published_wallet_count,
        latest_freshness_active_follow_wallet_count: latest_freshness.active_follow_wallet_count,
        observed_swaps_max_ts_utc: frontier.observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc: frontier.wallet_activity_days_max_day_utc,
        observed_swaps_total_rows: frontier.observed_swaps_total_rows,
        wallet_activity_days_total_rows: frontier.wallet_activity_days_total_rows,
        proof_window_start_utc,
        observed_swaps_rows_since_proof_window_start: frontier.observed_swaps_rows_since_proof_window_start,
        wallet_activity_days_rows_since_proof_window_start: frontier
            .wallet_activity_days_rows_since_proof_window_start,
        observed_swaps_frontier_before_proof_window_start: frontier
            .observed_swaps_frontier_before_proof_window_start,
        wallet_activity_days_frontier_before_proof_window_start: frontier
            .wallet_activity_days_frontier_before_proof_window_start,
        publication_truth_gate_surface: PUBLICATION_TRUTH_GATE_SURFACE.to_string(),
        replay_sol_leg_incomplete_surface_present: code_paths.replay_sol_leg_incomplete_surface_present,
        raw_window_incomplete_no_recent_published_universe_surface_present: code_paths
            .raw_window_incomplete_no_recent_published_universe_surface_present,
        fail_closed_runtime_surface_present: code_paths.fail_closed_runtime_surface_present,
        publication_truth_can_be_withheld_when_replay_sol_leg_incomplete: code_paths
            .publication_truth_can_be_withheld_when_replay_sol_leg_incomplete,
        published_follow_universe_depends_on_recent_publishable_truth: code_paths
            .published_follow_universe_depends_on_recent_publishable_truth,
        publication_truth_withheld_conclusion: conclusion.to_string(),
    })
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
    let has_wallet_ids = state
        .published_wallet_ids
        .as_ref()
        .is_some_and(|wallet_ids| !wallet_ids.is_empty());
    let truth_withheld = state.runtime_mode == DiscoveryRuntimeMode::FailClosed
        && state.reason.starts_with("publication_truth_withheld_");

    Ok(PublicationStateFacts {
        runtime_mode: Some(state.runtime_mode.as_str().to_string()),
        reason: Some(state.reason),
        last_published_at_utc: state.last_published_at,
        last_published_window_start_utc: state.last_published_window_start,
        published_wallet_count,
        has_wallet_ids,
        truth_withheld,
        updated_at_utc: Some(state.updated_at),
    })
}

fn open_read_only_connection(path: &std::path::Path) -> Result<Connection> {
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
        scoring_source: snapshot.audit.published_scoring_source,
        recent_under_gate: Some(snapshot.audit.publication_recent_under_gate),
        raw_window_complete: Some(snapshot.audit.raw_truth.sufficient),
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

fn proof_window_anchor_utc(
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

fn load_frontier_facts(conn: &Connection, proof_window_start_utc: DateTime<Utc>) -> Result<FrontierFacts> {
    let observed_swaps_max_ts_utc = query_observed_swaps_max_ts_utc(conn)?;
    let wallet_activity_days_max_day_utc = query_wallet_activity_days_max_day_utc(conn)?;
    let observed_swaps_total_rows = if observed_swaps_max_ts_utc.is_none() {
        Some(0)
    } else {
        None
    };
    let wallet_activity_days_total_rows = if wallet_activity_days_max_day_utc.is_none() {
        Some(0)
    } else {
        None
    };
    let observed_swaps_frontier_before_proof_window_start = observed_swaps_max_ts_utc
        .is_some_and(|latest| latest < proof_window_start_utc);
    let wallet_activity_days_frontier_before_proof_window_start = wallet_activity_days_max_day_utc
        .is_some_and(|latest| latest < proof_window_start_utc);
    let observed_swaps_rows_since_proof_window_start = if observed_swaps_total_rows == Some(0)
        || observed_swaps_frontier_before_proof_window_start
    {
        Some(0)
    } else {
        None
    };
    let wallet_activity_days_rows_since_proof_window_start = if wallet_activity_days_total_rows == Some(0)
        || wallet_activity_days_frontier_before_proof_window_start
    {
        Some(0)
    } else {
        None
    };

    Ok(FrontierFacts {
        observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc,
        observed_swaps_total_rows,
        wallet_activity_days_total_rows,
        observed_swaps_rows_since_proof_window_start,
        wallet_activity_days_rows_since_proof_window_start,
        observed_swaps_frontier_before_proof_window_start,
        wallet_activity_days_frontier_before_proof_window_start,
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
    let replay_sol_leg_incomplete_surface_present =
        DISCOVERY_SOURCE.contains("Some(\"sol_leg\") => \"replay_sol_leg_incomplete\"");
    let raw_window_incomplete_no_recent_published_universe_surface_present = DISCOVERY_SOURCE
        .contains("\"raw_window_incomplete_no_recent_published_universe\"")
        || APP_MAIN_SOURCE.contains("\"raw_window_incomplete_no_recent_published_universe\"");
    let fail_closed_runtime_surface_present = APP_MAIN_SOURCE
        .contains("fn apply_fail_closed_runtime_follow_surface_if_needed(")
        && APP_MAIN_SOURCE
            .contains("DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded");
    let publication_truth_can_be_withheld_when_replay_sol_leg_incomplete = DISCOVERY_SOURCE
        .contains("effective_reason = format!(\"publication_truth_withheld_while_{checkpoint_blocker}\")")
        && DISCOVERY_SOURCE.contains("Some(\"sol_leg\") => \"replay_sol_leg_incomplete\"");
    let published_follow_universe_depends_on_recent_publishable_truth = APP_MAIN_SOURCE
        .contains("fn startup_runtime_publication_truth(")
        && APP_MAIN_SOURCE.contains("fn startup_follow_snapshot_from_publication_truth(")
        && APP_MAIN_SOURCE.contains(
            "if let Some(RuntimePublicationTruthResolution::Recent(truth)) = runtime_publication_truth",
        )
        && APP_MAIN_SOURCE.contains("FollowSnapshot::from_active_wallets(truth.active_wallets())")
        && STORAGE_DISCOVERY_SOURCE.contains("pub fn discovery_publication_state_read_only(");

    CodePathFacts {
        replay_sol_leg_incomplete_surface_present,
        raw_window_incomplete_no_recent_published_universe_surface_present,
        fail_closed_runtime_surface_present,
        publication_truth_can_be_withheld_when_replay_sol_leg_incomplete,
        published_follow_universe_depends_on_recent_publishable_truth,
    }
}

fn choose_conclusion(
    publication_state: &PublicationStateFacts,
    latest_freshness: &LatestFreshnessFacts,
    frontier: &FrontierFacts,
    code_paths: &CodePathFacts,
) -> &'static str {
    let publication_fail_closed = publication_state.runtime_mode.as_deref() == Some("fail_closed");
    let publication_reason = publication_state.reason.as_deref().unwrap_or_default();
    let latest_reason = latest_freshness.reason.as_deref().unwrap_or_default();
    let reason_mentions_replay_or_raw_incomplete = publication_reason.contains("replay_sol_leg_incomplete")
        || publication_reason.contains("raw_window_incomplete");
    let reason_mentions_no_recent_publishable_universe =
        publication_reason.contains("no_recent_published_universe")
            || latest_reason.contains("no_recent_published_universe");

    if publication_fail_closed
        && publication_state.truth_withheld
        && reason_mentions_replay_or_raw_incomplete
        && frontier.observed_swaps_frontier_before_proof_window_start
        && code_paths.publication_truth_can_be_withheld_when_replay_sol_leg_incomplete
    {
        return CONCLUSION_RAW_WINDOW_INCOMPLETE;
    }

    if reason_mentions_no_recent_publishable_universe
        && code_paths.raw_window_incomplete_no_recent_published_universe_surface_present
        && code_paths.published_follow_universe_depends_on_recent_publishable_truth
    {
        return CONCLUSION_NO_RECENT_PUBLISHABLE_UNIVERSE;
    }

    if publication_fail_closed
        && publication_reason.contains("replay_sol_leg_incomplete")
        && code_paths.publication_truth_can_be_withheld_when_replay_sol_leg_incomplete
        && code_paths.fail_closed_runtime_surface_present
        && code_paths.published_follow_universe_depends_on_recent_publishable_truth
    {
        return CONCLUSION_FAIL_CLOSED_PERSISTS_AFTER_INCOMPLETE_REPLAY;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &PublicationTruthWithheldAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed to serialize publication truth withheld audit report");
    }
    Ok(format!(
        concat!(
            "publication_runtime_mode={publication_runtime_mode}\n",
            "publication_reason={publication_reason}\n",
            "publication_last_published_at_utc={publication_last_published_at_utc}\n",
            "publication_last_published_window_start_utc={publication_last_published_window_start_utc}\n",
            "publication_published_wallet_count={publication_published_wallet_count}\n",
            "publication_has_wallet_ids={publication_has_wallet_ids}\n",
            "publication_truth_withheld={publication_truth_withheld}\n",
            "latest_freshness_captured_at_utc={latest_freshness_captured_at_utc}\n",
            "latest_freshness_runtime_mode={latest_freshness_runtime_mode}\n",
            "latest_freshness_reason={latest_freshness_reason}\n",
            "latest_freshness_scoring_source={latest_freshness_scoring_source}\n",
            "latest_freshness_recent_under_gate={latest_freshness_recent_under_gate}\n",
            "latest_freshness_raw_window_complete={latest_freshness_raw_window_complete}\n",
            "latest_freshness_published_wallet_count={latest_freshness_published_wallet_count}\n",
            "latest_freshness_active_follow_wallet_count={latest_freshness_active_follow_wallet_count}\n",
            "observed_swaps_max_ts_utc={observed_swaps_max_ts_utc}\n",
            "wallet_activity_days_max_day_utc={wallet_activity_days_max_day_utc}\n",
            "observed_swaps_total_rows={observed_swaps_total_rows}\n",
            "wallet_activity_days_total_rows={wallet_activity_days_total_rows}\n",
            "proof_window_start_utc={proof_window_start_utc}\n",
            "observed_swaps_rows_since_proof_window_start={observed_swaps_rows_since_proof_window_start}\n",
            "wallet_activity_days_rows_since_proof_window_start={wallet_activity_days_rows_since_proof_window_start}\n",
            "observed_swaps_frontier_before_proof_window_start={observed_swaps_frontier_before_proof_window_start}\n",
            "wallet_activity_days_frontier_before_proof_window_start={wallet_activity_days_frontier_before_proof_window_start}\n",
            "publication_truth_gate_surface={publication_truth_gate_surface}\n",
            "replay_sol_leg_incomplete_surface_present={replay_sol_leg_incomplete_surface_present}\n",
            "raw_window_incomplete_no_recent_published_universe_surface_present={raw_window_incomplete_no_recent_published_universe_surface_present}\n",
            "fail_closed_runtime_surface_present={fail_closed_runtime_surface_present}\n",
            "publication_truth_can_be_withheld_when_replay_sol_leg_incomplete={publication_truth_can_be_withheld_when_replay_sol_leg_incomplete}\n",
            "published_follow_universe_depends_on_recent_publishable_truth={published_follow_universe_depends_on_recent_publishable_truth}\n",
            "publication_truth_withheld_conclusion={publication_truth_withheld_conclusion}\n",
        ),
        publication_runtime_mode = format_optional_str(report.publication_runtime_mode.as_deref()),
        publication_reason = format_optional_str(report.publication_reason.as_deref()),
        publication_last_published_at_utc =
            format_optional_ts(report.publication_last_published_at_utc),
        publication_last_published_window_start_utc =
            format_optional_ts(report.publication_last_published_window_start_utc),
        publication_published_wallet_count =
            format_optional_u64(report.publication_published_wallet_count),
        publication_has_wallet_ids = report.publication_has_wallet_ids,
        publication_truth_withheld = report.publication_truth_withheld,
        latest_freshness_captured_at_utc =
            format_optional_ts(report.latest_freshness_captured_at_utc),
        latest_freshness_runtime_mode =
            format_optional_str(report.latest_freshness_runtime_mode.as_deref()),
        latest_freshness_reason = format_optional_str(report.latest_freshness_reason.as_deref()),
        latest_freshness_scoring_source =
            format_optional_str(report.latest_freshness_scoring_source.as_deref()),
        latest_freshness_recent_under_gate =
            format_optional_bool(report.latest_freshness_recent_under_gate),
        latest_freshness_raw_window_complete =
            format_optional_bool(report.latest_freshness_raw_window_complete),
        latest_freshness_published_wallet_count =
            format_optional_u64(report.latest_freshness_published_wallet_count),
        latest_freshness_active_follow_wallet_count =
            format_optional_u64(report.latest_freshness_active_follow_wallet_count),
        observed_swaps_max_ts_utc = format_optional_ts(report.observed_swaps_max_ts_utc),
        wallet_activity_days_max_day_utc =
            format_optional_ts(report.wallet_activity_days_max_day_utc),
        observed_swaps_total_rows = format_optional_u64(report.observed_swaps_total_rows),
        wallet_activity_days_total_rows = format_optional_u64(report.wallet_activity_days_total_rows),
        proof_window_start_utc = report.proof_window_start_utc.to_rfc3339(),
        observed_swaps_rows_since_proof_window_start =
            format_optional_u64(report.observed_swaps_rows_since_proof_window_start),
        wallet_activity_days_rows_since_proof_window_start =
            format_optional_u64(report.wallet_activity_days_rows_since_proof_window_start),
        observed_swaps_frontier_before_proof_window_start =
            report.observed_swaps_frontier_before_proof_window_start,
        wallet_activity_days_frontier_before_proof_window_start =
            report.wallet_activity_days_frontier_before_proof_window_start,
        publication_truth_gate_surface = report.publication_truth_gate_surface,
        replay_sol_leg_incomplete_surface_present =
            report.replay_sol_leg_incomplete_surface_present,
        raw_window_incomplete_no_recent_published_universe_surface_present = report
            .raw_window_incomplete_no_recent_published_universe_surface_present,
        fail_closed_runtime_surface_present = report.fail_closed_runtime_surface_present,
        publication_truth_can_be_withheld_when_replay_sol_leg_incomplete = report
            .publication_truth_can_be_withheld_when_replay_sol_leg_incomplete,
        published_follow_universe_depends_on_recent_publishable_truth = report
            .published_follow_universe_depends_on_recent_publishable_truth,
        publication_truth_withheld_conclusion = report.publication_truth_withheld_conclusion,
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
    use copybot_discovery::wallet_freshness_audit::{
        WalletFreshnessAuditReport, WalletFreshnessCaptureSnapshot, WalletFreshnessRawCycleSample,
        WalletFreshnessRawTruthStatus, WalletFreshnessRotationSignal,
        WalletFreshnessShadowSignalEvidence, WalletFreshnessVerdict, WalletShadowSignalVerdict,
        WalletUniverseComparison,
    };
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryWalletFreshnessCaptureWrite, WalletActivityDayRow,
    };
    use serde_json::Value;
    use std::path::Path;
    use tempfile::TempDir;

    fn repo_config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }

    fn create_fixture() -> Result<(TempDir, PathBuf, SqliteStore)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let db_path = temp.path().join("publication-truth-withheld-audit.db");
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

    fn seed_wallet_freshness_capture(
        store: &SqliteStore,
        captured_at: DateTime<Utc>,
        runtime_mode: Option<&str>,
        scoring_source: Option<&str>,
        recent_under_gate: bool,
        raw_window_complete: bool,
        active_follow_wallet_ids: Vec<String>,
        current_raw_top_wallet_ids: Vec<String>,
        published_wallet_ids: Vec<String>,
        reason: &str,
    ) -> Result<()> {
        let audit = WalletFreshnessAuditReport {
            now: captured_at,
            window_start: captured_at - Duration::days(5),
            verdict: WalletFreshnessVerdict::StalePublicationTruth,
            reason: reason.to_string(),
            follow_top_n: 15,
            publication_truth_available: runtime_mode.is_some(),
            publication_runtime_mode: runtime_mode.map(str::to_string),
            publication_recent_under_gate: recent_under_gate,
            latest_publication_ts: Some(captured_at - Duration::minutes(30)),
            publication_age_seconds: Some(1800),
            latest_publication_window_start: Some(captured_at - Duration::days(5)),
            published_scoring_source: scoring_source.map(str::to_string),
            published_wallet_ids: published_wallet_ids.clone(),
            active_follow_wallet_ids: active_follow_wallet_ids.clone(),
            current_raw_top_wallet_ids: current_raw_top_wallet_ids.clone(),
            published_vs_current_raw: comparison(&published_wallet_ids, &current_raw_top_wallet_ids),
            active_follow_vs_current_raw: comparison(
                &active_follow_wallet_ids,
                &current_raw_top_wallet_ids,
            ),
            active_follow_vs_published: comparison(&active_follow_wallet_ids, &published_wallet_ids),
            raw_truth: WalletFreshnessRawTruthStatus {
                sufficient: raw_window_complete,
                reason: if raw_window_complete {
                    "full_scoring_window_raw_truth_available".to_string()
                } else {
                    "observed_swaps_coverage_ends_before_freshness_gate".to_string()
                },
                observed_swaps_loaded: current_raw_top_wallet_ids.len(),
                eligible_wallet_count: current_raw_top_wallet_ids.len(),
                top_wallet_count: current_raw_top_wallet_ids.len(),
                short_retention_configured: false,
                covered_since: Some(captured_at - Duration::days(5)),
                covered_through_cursor: None,
                covered_through_lag_seconds: Some(60),
                tail_fresh_within_runtime_lag: raw_window_complete,
                runtime_freshness_lag_seconds: 60,
                total_observed_swaps_rows: current_raw_top_wallet_ids.len(),
            },
            rotation: WalletFreshnessRotationSignal {
                signal_available: false,
                reason: Some("not_needed_for_audit_fixture".to_string()),
                cycles_requested: 3,
                cycles_completed: 1,
                sample_interval_seconds: 60,
                overlap_with_previous_cycle: None,
                entered_since_previous_cycle: Vec::new(),
                left_since_previous_cycle: Vec::new(),
                stable_wallets_across_cycles: Vec::new(),
                unique_wallet_count_across_cycles: current_raw_top_wallet_ids.len(),
                samples: vec![WalletFreshnessRawCycleSample {
                    sample_now: captured_at,
                    window_start: captured_at - Duration::days(5),
                    observed_swaps_loaded: current_raw_top_wallet_ids.len(),
                    eligible_wallet_count: current_raw_top_wallet_ids.len(),
                    top_wallet_ids: current_raw_top_wallet_ids.clone(),
                }],
            },
        };
        let shadow_signal = WalletFreshnessShadowSignalEvidence {
            recent_window_start: captured_at - Duration::minutes(10),
            recent_window_end: captured_at,
            evidence_lookback_seconds: Some(600),
            selected_wallet_ids: active_follow_wallet_ids.clone(),
            selected_wallet_count: active_follow_wallet_ids.len(),
            selected_wallets_with_recent_raw_activity: active_follow_wallet_ids.len(),
            selected_wallets_with_recent_shadow_signal: 0,
            recent_raw_swap_count: 0,
            recent_shadow_signal_count: 0,
            recent_raw_activity_wallet_ids: active_follow_wallet_ids.clone(),
            recent_shadow_signal_wallet_ids: Vec::new(),
            recent_raw_activity_by_wallet: Vec::new(),
            recent_shadow_signal_by_wallet: Vec::new(),
            raw_activity_top_wallet_share: None,
            shadow_signal_top_wallet_share: None,
            raw_activity_broadly_distributed: false,
            shadow_signal_broadly_distributed: false,
            verdict: WalletShadowSignalVerdict::NoRecentSelectedRawActivity,
            reason: "fixture_no_recent_shadow_signals".to_string(),
        };
        let snapshot = WalletFreshnessCaptureSnapshot {
            capture_id: None,
            captured_at,
            capture_age_seconds: None,
            within_recent_horizon: None,
            recent_cycles: 3,
            audit,
            shadow_signal,
        };
        let write: DiscoveryWalletFreshnessCaptureWrite = snapshot.to_storage_write()?;
        store.append_discovery_wallet_freshness_capture(&write)?;
        Ok(())
    }

    fn comparison(left: &[String], right: &[String]) -> WalletUniverseComparison {
        let left_set: std::collections::BTreeSet<String> = left.iter().cloned().collect();
        let right_set: std::collections::BTreeSet<String> = right.iter().cloned().collect();
        let overlap_count = left_set.intersection(&right_set).count();
        WalletUniverseComparison {
            left_count: left.len(),
            right_count: right.len(),
            overlap_count,
            exact_match: left_set == right_set,
            only_left: left_set.difference(&right_set).cloned().collect(),
            only_right: right_set.difference(&left_set).cloned().collect(),
        }
    }

    #[test]
    fn parse_args_reads_required_publication_truth_withheld_audit_flags() -> Result<()> {
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
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: None,
        })?;
        seed_wallet_freshness_capture(
            &store,
            now - Duration::minutes(5),
            Some("healthy"),
            Some("raw_window_persisted_stream"),
            true,
            false,
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            "observed_swaps_coverage_ends_before_freshness_gate",
        )?;
        store.insert_observed_swap(&swap(
            "sig-stale-window",
            "wallet-stale",
            DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
            1,
        ))?;
        store.upsert_wallet_activity_days(&[WalletActivityDayRow {
            wallet_id: "wallet-stale".to_string(),
            activity_day: DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc)
                .date_naive(),
            last_seen: DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
        }])?;

        let report = run(&fixture_config(&db_path))?;
        let json = serde_json::to_value(&report)?;
        for key in [
            "publication_runtime_mode",
            "publication_reason",
            "publication_last_published_at_utc",
            "publication_last_published_window_start_utc",
            "publication_published_wallet_count",
            "publication_has_wallet_ids",
            "publication_truth_withheld",
            "latest_freshness_captured_at_utc",
            "latest_freshness_runtime_mode",
            "latest_freshness_reason",
            "latest_freshness_scoring_source",
            "latest_freshness_recent_under_gate",
            "latest_freshness_raw_window_complete",
            "latest_freshness_published_wallet_count",
            "latest_freshness_active_follow_wallet_count",
            "observed_swaps_max_ts_utc",
            "wallet_activity_days_max_day_utc",
            "observed_swaps_total_rows",
            "wallet_activity_days_total_rows",
            "proof_window_start_utc",
            "observed_swaps_rows_since_proof_window_start",
            "wallet_activity_days_rows_since_proof_window_start",
            "observed_swaps_frontier_before_proof_window_start",
            "wallet_activity_days_frontier_before_proof_window_start",
            "publication_truth_gate_surface",
            "replay_sol_leg_incomplete_surface_present",
            "raw_window_incomplete_no_recent_published_universe_surface_present",
            "fail_closed_runtime_surface_present",
            "publication_truth_can_be_withheld_when_replay_sol_leg_incomplete",
            "published_follow_universe_depends_on_recent_publishable_truth",
            "publication_truth_withheld_conclusion",
        ] {
            assert!(json.get(key).is_some(), "missing JSON key {key}");
        }
        Ok(())
    }

    #[test]
    fn fail_closed_incomplete_raw_window_fixture_yields_raw_window_incomplete_conclusion(
    ) -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: None,
        })?;
        seed_wallet_freshness_capture(
            &store,
            now,
            Some("healthy"),
            Some("raw_window_persisted_stream"),
            true,
            false,
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            "observed_swaps_coverage_ends_before_freshness_gate",
        )?;
        store.insert_observed_swap(&swap(
            "sig-old",
            "wallet-old",
            DateTime::parse_from_rfc3339("2026-03-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
            1,
        ))?;
        store.upsert_wallet_activity_days(&[WalletActivityDayRow {
            wallet_id: "wallet-old".to_string(),
            activity_day: DateTime::parse_from_rfc3339("2026-03-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc)
                .date_naive(),
            last_seen: DateTime::parse_from_rfc3339("2026-03-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
        }])?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.publication_truth_withheld_conclusion,
            CONCLUSION_RAW_WINDOW_INCOMPLETE
        );
        assert_eq!(report.publication_runtime_mode.as_deref(), Some("fail_closed"));
        assert_eq!(
            report.publication_reason.as_deref(),
            Some("publication_truth_withheld_while_replay_sol_leg_incomplete")
        );
        assert!(report.publication_truth_withheld);
        assert!(report.observed_swaps_frontier_before_proof_window_start);
        Ok(())
    }

    #[test]
    fn ambiguous_persisted_state_never_overclaims_stronger_than_allowed_conclusions() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "manual_fail_closed_for_ambiguous_fixture".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: None,
            published_wallet_ids: None,
        })?;
        seed_wallet_freshness_capture(
            &store,
            now,
            Some("healthy"),
            Some("raw_window"),
            true,
            true,
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            "fresh_current",
        )?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.publication_truth_withheld_conclusion,
            CONCLUSION_INSUFFICIENT_EVIDENCE
        );
        Ok(())
    }

    #[test]
    fn repeated_runs_with_same_inputs_are_deterministic() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: None,
        })?;
        seed_wallet_freshness_capture(
            &store,
            now,
            Some("healthy"),
            Some("raw_window_persisted_stream"),
            true,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            "observed_swaps_coverage_ends_before_freshness_gate",
        )?;

        let config = fixture_config(&db_path);
        let first = render_output(&run(&config)?, true)?;
        let second = render_output(&run(&config)?, true)?;
        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn conclusion_selection_does_not_require_heavy_rows_since_scans() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "publication_truth_withheld_while_replay_sol_leg_incomplete".to_string(),
            last_published_at: Some(now - Duration::days(15)),
            last_published_window_start: Some(now - Duration::days(20)),
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-top".to_string()]),
        })?;
        seed_wallet_freshness_capture(
            &store,
            now - Duration::days(15),
            Some("healthy"),
            Some("raw_window_persisted_stream"),
            true,
            false,
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            vec!["wallet-top".to_string()],
            "observed_swaps_coverage_ends_before_freshness_gate",
        )?;
        store.insert_observed_swap(&swap(
            "sig-in-window",
            "wallet-in-window",
            DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
            1,
        ))?;
        store.upsert_wallet_activity_days(&[WalletActivityDayRow {
            wallet_id: "wallet-in-window".to_string(),
            activity_day: DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc)
                .date_naive(),
            last_seen: DateTime::parse_from_rfc3339("2026-04-17T17:33:19Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
        }])?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(
            report.publication_truth_withheld_conclusion,
            CONCLUSION_FAIL_CLOSED_PERSISTS_AFTER_INCOMPLETE_REPLAY
        );
        assert_eq!(report.observed_swaps_rows_since_proof_window_start, None);
        assert_eq!(report.wallet_activity_days_rows_since_proof_window_start, None);
        Ok(())
    }

    #[test]
    fn render_output_json_round_trips_report_shape() -> Result<()> {
        let report = PublicationTruthWithheldAuditReport {
            publication_runtime_mode: Some("fail_closed".to_string()),
            publication_reason: Some("publication_truth_withheld_while_replay_sol_leg_incomplete".to_string()),
            publication_last_published_at_utc: None,
            publication_last_published_window_start_utc: None,
            publication_published_wallet_count: Some(0),
            publication_has_wallet_ids: false,
            publication_truth_withheld: true,
            latest_freshness_captured_at_utc: None,
            latest_freshness_runtime_mode: None,
            latest_freshness_reason: None,
            latest_freshness_scoring_source: None,
            latest_freshness_recent_under_gate: None,
            latest_freshness_raw_window_complete: None,
            latest_freshness_published_wallet_count: None,
            latest_freshness_active_follow_wallet_count: None,
            observed_swaps_max_ts_utc: None,
            wallet_activity_days_max_day_utc: None,
            observed_swaps_total_rows: None,
            wallet_activity_days_total_rows: None,
            proof_window_start_utc: DateTime::parse_from_rfc3339("2026-04-16T16:00:00Z")
                .expect("valid")
                .with_timezone(&Utc),
            observed_swaps_rows_since_proof_window_start: None,
            wallet_activity_days_rows_since_proof_window_start: None,
            observed_swaps_frontier_before_proof_window_start: false,
            wallet_activity_days_frontier_before_proof_window_start: false,
            publication_truth_gate_surface: PUBLICATION_TRUTH_GATE_SURFACE.to_string(),
            replay_sol_leg_incomplete_surface_present: true,
            raw_window_incomplete_no_recent_published_universe_surface_present: true,
            fail_closed_runtime_surface_present: true,
            publication_truth_can_be_withheld_when_replay_sol_leg_incomplete: true,
            published_follow_universe_depends_on_recent_publishable_truth: true,
            publication_truth_withheld_conclusion: CONCLUSION_INSUFFICIENT_EVIDENCE.to_string(),
        };
        let json = render_output(&report, true)?;
        let value: Value = serde_json::from_str(&json)?;
        assert_eq!(
            value.get("publication_truth_withheld_conclusion").and_then(Value::as_str),
            Some(CONCLUSION_INSUFFICIENT_EVIDENCE)
        );
        Ok(())
    }
}
