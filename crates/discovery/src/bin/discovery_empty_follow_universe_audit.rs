use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::wallet_freshness_audit::wallet_freshness_capture_from_row;
use copybot_storage::{DiscoveryWalletFreshnessCaptureRow, SqliteStore};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_empty_follow_universe_audit --db <path> --config <path> [--json]";

const CONCLUSION_PUBLICATION_TRUTH_EMPTY_OR_STALE: &str =
    "follow_universe_empty_because_publication_truth_is_empty_or_stale";
const CONCLUSION_RUNTIME_CLEARED_STARTUP_FOLLOW_SNAPSHOT: &str =
    "follow_universe_empty_because_runtime_cleared_startup_follow_snapshot_under_fail_closed";
const CONCLUSION_FOLLOWLIST_POPULATION_PATH_NOT_RUNNING: &str =
    "follow_universe_empty_because_followlist_population_path_is_not_running_or_not_persisting";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const STARTUP_FOLLOW_SNAPSHOT_SURFACE: &str =
    "copybot_app::main::startup_follow_snapshot_from_publication_truth";
const PUBLICATION_TRUTH_FOLLOW_SNAPSHOT_UPDATE_SURFACE: &str =
    "copybot_app::shadow_runtime_helpers::apply_follow_snapshot_update";
const ACTIVE_FOLLOWLIST_PERSISTENCE_SURFACE: &str =
    "copybot_storage::discovery::SqliteStore::persist_discovery_cycle_with_snapshot_metadata";

const APP_MAIN_SOURCE: &str = include_str!("../../../app/src/main.rs");
const APP_SHADOW_RUNTIME_HELPERS_SOURCE: &str =
    include_str!("../../../app/src/shadow_runtime_helpers.rs");
const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");
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
struct EmptyFollowUniverseAuditReport {
    config_path: String,
    active_follow_wallet_count: u64,
    followlist_total_rows: u64,
    followlist_active_min_added_at_utc: Option<DateTime<Utc>>,
    followlist_active_max_added_at_utc: Option<DateTime<Utc>>,
    followlist_inactive_with_removed_at_count: u64,
    latest_discovery_wallet_freshness_history_captured_at_utc: Option<DateTime<Utc>>,
    latest_discovery_wallet_freshness_history_runtime_mode: Option<String>,
    latest_discovery_wallet_freshness_history_scoring_source: Option<String>,
    latest_discovery_wallet_freshness_history_active_follow_wallet_count: Option<u64>,
    latest_discovery_wallet_freshness_history_selected_wallet_count: Option<u64>,
    latest_discovery_wallet_freshness_history_published_wallet_count: Option<u64>,
    latest_discovery_wallet_freshness_history_recent_under_gate: Option<bool>,
    latest_discovery_wallet_freshness_history_raw_window_complete: Option<bool>,
    latest_discovery_wallet_freshness_history_reason: Option<String>,
    startup_follow_snapshot_surface: String,
    nonpublished_fail_closed_clear_surface_present: bool,
    degraded_preserve_surface_present: bool,
    publication_truth_follow_snapshot_update_surface: String,
    active_followlist_persistence_surface: String,
    runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished: bool,
    followlist_population_path_surface_present: bool,
    discovery_cycle_can_write_active_follow_wallets: bool,
    publication_truth_can_recover_startup_follow_snapshot: bool,
    empty_follow_universe_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct FollowlistFacts {
    active_follow_wallet_count: u64,
    total_rows: u64,
    active_min_added_at_utc: Option<DateTime<Utc>>,
    active_max_added_at_utc: Option<DateTime<Utc>>,
    inactive_with_removed_at_count: u64,
}

#[derive(Debug, Clone, Default)]
struct LatestFreshnessHistoryFacts {
    captured_at_utc: Option<DateTime<Utc>>,
    runtime_mode: Option<String>,
    scoring_source: Option<String>,
    active_follow_wallet_count: Option<u64>,
    selected_wallet_count: Option<u64>,
    published_wallet_count: Option<u64>,
    recent_under_gate: Option<bool>,
    raw_window_complete: Option<bool>,
    reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct PublicationStateFacts {
    runtime_mode: Option<String>,
    reason: Option<String>,
    published_wallet_count: Option<u64>,
    has_complete_publication_truth: bool,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    nonpublished_fail_closed_clear_surface_present: bool,
    degraded_preserve_surface_present: bool,
    runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished: bool,
    followlist_population_path_surface_present: bool,
    discovery_cycle_can_write_active_follow_wallets: bool,
    publication_truth_can_recover_startup_follow_snapshot: bool,
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

fn run(config: &Config) -> Result<EmptyFollowUniverseAuditReport> {
    let _loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let _store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let conn = open_read_only_connection(&config.db_path)?;

    let followlist_facts = load_followlist_facts(&conn)?;
    let latest_history = load_latest_wallet_freshness_history_facts(&conn)?;
    let publication_state = load_publication_state_facts(&conn)?;
    let code_facts = inspect_code_path_facts();
    let conclusion = choose_empty_follow_universe_conclusion(
        &followlist_facts,
        &latest_history,
        &publication_state,
        &code_facts,
    );

    Ok(EmptyFollowUniverseAuditReport {
        config_path: config.config_path.display().to_string(),
        active_follow_wallet_count: followlist_facts.active_follow_wallet_count,
        followlist_total_rows: followlist_facts.total_rows,
        followlist_active_min_added_at_utc: followlist_facts.active_min_added_at_utc,
        followlist_active_max_added_at_utc: followlist_facts.active_max_added_at_utc,
        followlist_inactive_with_removed_at_count: followlist_facts.inactive_with_removed_at_count,
        latest_discovery_wallet_freshness_history_captured_at_utc: latest_history.captured_at_utc,
        latest_discovery_wallet_freshness_history_runtime_mode: latest_history.runtime_mode,
        latest_discovery_wallet_freshness_history_scoring_source: latest_history.scoring_source,
        latest_discovery_wallet_freshness_history_active_follow_wallet_count: latest_history
            .active_follow_wallet_count,
        latest_discovery_wallet_freshness_history_selected_wallet_count: latest_history
            .selected_wallet_count,
        latest_discovery_wallet_freshness_history_published_wallet_count: latest_history
            .published_wallet_count,
        latest_discovery_wallet_freshness_history_recent_under_gate: latest_history
            .recent_under_gate,
        latest_discovery_wallet_freshness_history_raw_window_complete: latest_history
            .raw_window_complete,
        latest_discovery_wallet_freshness_history_reason: latest_history.reason,
        startup_follow_snapshot_surface: STARTUP_FOLLOW_SNAPSHOT_SURFACE.to_string(),
        nonpublished_fail_closed_clear_surface_present: code_facts
            .nonpublished_fail_closed_clear_surface_present,
        degraded_preserve_surface_present: code_facts.degraded_preserve_surface_present,
        publication_truth_follow_snapshot_update_surface:
            PUBLICATION_TRUTH_FOLLOW_SNAPSHOT_UPDATE_SURFACE.to_string(),
        active_followlist_persistence_surface: ACTIVE_FOLLOWLIST_PERSISTENCE_SURFACE.to_string(),
        runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished: code_facts
            .runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished,
        followlist_population_path_surface_present: code_facts
            .followlist_population_path_surface_present,
        discovery_cycle_can_write_active_follow_wallets: code_facts
            .discovery_cycle_can_write_active_follow_wallets,
        publication_truth_can_recover_startup_follow_snapshot: code_facts
            .publication_truth_can_recover_startup_follow_snapshot,
        empty_follow_universe_conclusion: conclusion.to_string(),
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
            params![table_name],
            |row| row.get(0),
        )
        .optional()
        .context("failed checking sqlite table existence")?;
    Ok(exists.is_some())
}

fn load_followlist_facts(conn: &Connection) -> Result<FollowlistFacts> {
    if !sqlite_table_exists(conn, "followlist")? {
        return Ok(FollowlistFacts::default());
    }

    let (
        total_rows,
        active_follow_wallet_count,
        active_min_added_at_raw,
        active_max_added_at_raw,
        inactive_with_removed_at_count,
    ) = conn
        .query_row(
            "SELECT
                COUNT(*) AS total_rows,
                COALESCE(SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END), 0) AS active_follow_wallet_count,
                MIN(CASE WHEN active = 1 THEN added_at END) AS active_min_added_at,
                MAX(CASE WHEN active = 1 THEN added_at END) AS active_max_added_at,
                COALESCE(SUM(CASE WHEN active = 0 AND removed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS inactive_with_removed_at_count
             FROM followlist",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .context("failed loading followlist facts")?;

    Ok(FollowlistFacts {
        active_follow_wallet_count: active_follow_wallet_count.max(0) as u64,
        total_rows: total_rows.max(0) as u64,
        active_min_added_at_utc: parse_optional_db_ts(active_min_added_at_raw)?,
        active_max_added_at_utc: parse_optional_db_ts(active_max_added_at_raw)?,
        inactive_with_removed_at_count: inactive_with_removed_at_count.max(0) as u64,
    })
}

fn load_latest_wallet_freshness_history_facts(
    conn: &Connection,
) -> Result<LatestFreshnessHistoryFacts> {
    if !sqlite_table_exists(conn, "discovery_wallet_freshness_history")? {
        return Ok(LatestFreshnessHistoryFacts::default());
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
        return Ok(LatestFreshnessHistoryFacts::default());
    };
    let row = DiscoveryWalletFreshnessCaptureRow {
        capture_id: raw.0,
        captured_at: parse_db_ts(raw.1)?,
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

    Ok(LatestFreshnessHistoryFacts {
        captured_at_utc: Some(row.captured_at),
        runtime_mode: snapshot.audit.publication_runtime_mode,
        scoring_source: snapshot.audit.published_scoring_source,
        active_follow_wallet_count: Some(snapshot.audit.active_follow_wallet_ids.len() as u64),
        selected_wallet_count: Some(snapshot.audit.raw_truth.top_wallet_count as u64),
        published_wallet_count: Some(snapshot.audit.published_wallet_ids.len() as u64),
        recent_under_gate: Some(snapshot.audit.publication_recent_under_gate),
        raw_window_complete: Some(snapshot.audit.raw_truth.sufficient),
        reason: Some(row.reason),
    })
}

fn load_publication_state_facts(conn: &Connection) -> Result<PublicationStateFacts> {
    if !sqlite_table_exists(conn, "discovery_strategy_state")? {
        return Ok(PublicationStateFacts::default());
    }

    let raw = conn
        .query_row(
            "SELECT
                publication_runtime_mode,
                publication_reason,
                publication_last_published_at,
                publication_last_published_window_start,
                publication_wallet_ids_json
             FROM discovery_strategy_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()
        .context("failed reading discovery publication state")?;

    let Some((
        runtime_mode,
        reason,
        last_published_at_raw,
        last_published_window_start_raw,
        published_wallet_ids_raw,
    )) = raw
    else {
        return Ok(PublicationStateFacts::default());
    };
    let published_wallet_count = published_wallet_ids_raw
        .as_deref()
        .map(parse_wallet_ids_json)
        .transpose()?
        .map(|wallet_ids| wallet_ids.len() as u64)
        .or(Some(0));
    let has_complete_publication_truth = last_published_at_raw.is_some()
        && last_published_window_start_raw.is_some()
        && published_wallet_count.unwrap_or(0) > 0;

    Ok(PublicationStateFacts {
        runtime_mode: Some(runtime_mode),
        reason: Some(reason),
        published_wallet_count,
        has_complete_publication_truth,
    })
}

fn parse_wallet_ids_json(raw: &str) -> Result<Vec<String>> {
    serde_json::from_str(raw).with_context(|| format!("invalid wallet ids JSON payload: {raw}"))
}

fn parse_db_ts(raw: String) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S")
                .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        })
        .or_else(|_| {
            NaiveDate::parse_from_str(&raw, "%Y-%m-%d").map(|day| {
                DateTime::<Utc>::from_naive_utc_and_offset(
                    day.and_hms_opt(0, 0, 0).expect("midnight should exist"),
                    Utc,
                )
            })
        })
        .with_context(|| format!("invalid db timestamp value: {raw}"))
}

fn parse_optional_db_ts(raw: Option<String>) -> Result<Option<DateTime<Utc>>> {
    raw.map(parse_db_ts).transpose()
}

fn inspect_code_path_facts() -> CodePathFacts {
    let startup_recent_truth_recovery = APP_MAIN_SOURCE
        .contains("FollowSnapshot::from_active_wallets(truth.active_wallets())")
        && APP_MAIN_SOURCE.contains("RuntimePublicationTruthResolution::Recent(truth)");
    let fail_closed_clear_surface = APP_MAIN_SOURCE
        .contains("if fail_closed_runtime_surface && !discovery_output.published")
        && APP_MAIN_SOURCE.contains(
            "clearing runtime follow snapshot because discovery no longer has a publishable universe to keep in memory",
        );
    let fail_closed_runtime_helper_clears = APP_MAIN_SOURCE
        .contains("DiscoveryRuntimeMode::FailClosed | DiscoveryRuntimeMode::BootstrapDegraded")
        && APP_MAIN_SOURCE.contains("*follow_snapshot = Arc::new(FollowSnapshot::default());")
        && APP_MAIN_SOURCE.contains("open_shadow_lots.clear();");
    let degraded_preserve_surface_present = APP_MAIN_SOURCE
        .contains("if discovery_output.published {")
        && APP_MAIN_SOURCE.contains("if !fail_closed_runtime_surface {")
        && APP_MAIN_SOURCE.contains("apply_follow_snapshot_update(")
        && APP_SHADOW_RUNTIME_HELPERS_SOURCE
            .contains("pub(crate) fn apply_follow_snapshot_update(");
    let followlist_population_path_surface_present = STORAGE_DISCOVERY_SOURCE
        .contains("pub fn persist_discovery_cycle_with_snapshot_metadata(")
        && STORAGE_DISCOVERY_SOURCE
            .contains("INSERT OR IGNORE INTO followlist(wallet_id, added_at, reason, active)")
        && STORAGE_DISCOVERY_SOURCE.contains(
            "UPDATE followlist\n                         SET active = 0, removed_at = ?1, reason = ?2",
        );
    let discovery_cycle_can_write_active_follow_wallets = DISCOVERY_SOURCE.contains(
        "store.persist_discovery_cycle(&[], &[], &desired_wallets, true, true, now, reason)?",
    );

    CodePathFacts {
        nonpublished_fail_closed_clear_surface_present: fail_closed_clear_surface,
        degraded_preserve_surface_present,
        runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished:
            fail_closed_clear_surface && fail_closed_runtime_helper_clears,
        followlist_population_path_surface_present,
        discovery_cycle_can_write_active_follow_wallets,
        publication_truth_can_recover_startup_follow_snapshot: startup_recent_truth_recovery,
    }
}

fn choose_empty_follow_universe_conclusion(
    followlist: &FollowlistFacts,
    latest_history: &LatestFreshnessHistoryFacts,
    publication_state: &PublicationStateFacts,
    code_facts: &CodePathFacts,
) -> &'static str {
    if followlist.active_follow_wallet_count > 0 {
        return CONCLUSION_INSUFFICIENT_EVIDENCE;
    }

    let publication_state_present = publication_state.runtime_mode.is_some()
        || publication_state.reason.is_some()
        || publication_state.published_wallet_count.is_some();
    let current_publication_truth_empty_or_stale = publication_state_present
        && (publication_state
            .runtime_mode
            .as_deref()
            .is_some_and(|runtime_mode| runtime_mode != "healthy")
            || !publication_state.has_complete_publication_truth
            || publication_state.published_wallet_count.unwrap_or(0) == 0
            || publication_state.reason.as_deref().is_some_and(|reason| {
                reason.contains("no_recent_published_universe")
                    || reason.contains("publication_truth_withheld")
            }));

    let latest_history_present = latest_history.captured_at_utc.is_some();
    let latest_history_truth_empty_or_stale = latest_history_present
        && (latest_history
            .runtime_mode
            .as_deref()
            .is_some_and(|runtime_mode| runtime_mode != "healthy")
            || latest_history.recent_under_gate == Some(false)
            || latest_history.published_wallet_count == Some(0)
            || latest_history.active_follow_wallet_count == Some(0)
            || latest_history.reason.as_deref().is_some_and(|reason| {
                reason.contains("no_recent_published_universe")
                    || reason.contains("publication_truth_withheld")
                    || reason.contains("stale_publication_truth")
            }));

    if code_facts.publication_truth_can_recover_startup_follow_snapshot
        && (current_publication_truth_empty_or_stale || latest_history_truth_empty_or_stale)
    {
        return CONCLUSION_PUBLICATION_TRUTH_EMPTY_OR_STALE;
    }

    let prior_active_follow_universe_evidence = followlist.inactive_with_removed_at_count > 0
        || latest_history
            .active_follow_wallet_count
            .is_some_and(|count| count > 0);
    if code_facts.runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished
        && prior_active_follow_universe_evidence
    {
        return CONCLUSION_RUNTIME_CLEARED_STARTUP_FOLLOW_SNAPSHOT;
    }

    let current_publication_truth_expects_wallets = publication_state
        .runtime_mode
        .as_deref()
        .is_some_and(|runtime_mode| runtime_mode == "healthy")
        && publication_state.has_complete_publication_truth
        && publication_state.published_wallet_count.unwrap_or(0) > 0;
    if code_facts.followlist_population_path_surface_present
        && code_facts.discovery_cycle_can_write_active_follow_wallets
        && current_publication_truth_expects_wallets
        && followlist.total_rows == 0
    {
        return CONCLUSION_FOLLOWLIST_POPULATION_PATH_NOT_RUNNING;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &EmptyFollowUniverseAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing empty follow universe audit json");
    }

    Ok(format!(
        concat!(
            "event=discovery_empty_follow_universe_audit\n",
            "config_path={config_path}\n",
            "active_follow_wallet_count={active_follow_wallet_count}\n",
            "followlist_total_rows={followlist_total_rows}\n",
            "followlist_active_min_added_at_utc={followlist_active_min_added_at_utc}\n",
            "followlist_active_max_added_at_utc={followlist_active_max_added_at_utc}\n",
            "followlist_inactive_with_removed_at_count={followlist_inactive_with_removed_at_count}\n",
            "latest_discovery_wallet_freshness_history_captured_at_utc={latest_discovery_wallet_freshness_history_captured_at_utc}\n",
            "latest_discovery_wallet_freshness_history_runtime_mode={latest_discovery_wallet_freshness_history_runtime_mode}\n",
            "latest_discovery_wallet_freshness_history_scoring_source={latest_discovery_wallet_freshness_history_scoring_source}\n",
            "latest_discovery_wallet_freshness_history_active_follow_wallet_count={latest_discovery_wallet_freshness_history_active_follow_wallet_count}\n",
            "latest_discovery_wallet_freshness_history_selected_wallet_count={latest_discovery_wallet_freshness_history_selected_wallet_count}\n",
            "latest_discovery_wallet_freshness_history_published_wallet_count={latest_discovery_wallet_freshness_history_published_wallet_count}\n",
            "latest_discovery_wallet_freshness_history_recent_under_gate={latest_discovery_wallet_freshness_history_recent_under_gate}\n",
            "latest_discovery_wallet_freshness_history_raw_window_complete={latest_discovery_wallet_freshness_history_raw_window_complete}\n",
            "latest_discovery_wallet_freshness_history_reason={latest_discovery_wallet_freshness_history_reason}\n",
            "startup_follow_snapshot_surface={startup_follow_snapshot_surface}\n",
            "nonpublished_fail_closed_clear_surface_present={nonpublished_fail_closed_clear_surface_present}\n",
            "degraded_preserve_surface_present={degraded_preserve_surface_present}\n",
            "publication_truth_follow_snapshot_update_surface={publication_truth_follow_snapshot_update_surface}\n",
            "active_followlist_persistence_surface={active_followlist_persistence_surface}\n",
            "runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished={runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished}\n",
            "followlist_population_path_surface_present={followlist_population_path_surface_present}\n",
            "discovery_cycle_can_write_active_follow_wallets={discovery_cycle_can_write_active_follow_wallets}\n",
            "publication_truth_can_recover_startup_follow_snapshot={publication_truth_can_recover_startup_follow_snapshot}\n",
            "empty_follow_universe_conclusion={empty_follow_universe_conclusion}"
        ),
        config_path = report.config_path,
        active_follow_wallet_count = report.active_follow_wallet_count,
        followlist_total_rows = report.followlist_total_rows,
        followlist_active_min_added_at_utc = format_optional_ts(
            report.followlist_active_min_added_at_utc,
        ),
        followlist_active_max_added_at_utc = format_optional_ts(
            report.followlist_active_max_added_at_utc,
        ),
        followlist_inactive_with_removed_at_count = report.followlist_inactive_with_removed_at_count,
        latest_discovery_wallet_freshness_history_captured_at_utc = format_optional_ts(
            report.latest_discovery_wallet_freshness_history_captured_at_utc,
        ),
        latest_discovery_wallet_freshness_history_runtime_mode = format_optional_str(
            report.latest_discovery_wallet_freshness_history_runtime_mode.as_deref(),
        ),
        latest_discovery_wallet_freshness_history_scoring_source = format_optional_str(
            report
                .latest_discovery_wallet_freshness_history_scoring_source
                .as_deref(),
        ),
        latest_discovery_wallet_freshness_history_active_follow_wallet_count = format_optional_u64(
            report.latest_discovery_wallet_freshness_history_active_follow_wallet_count,
        ),
        latest_discovery_wallet_freshness_history_selected_wallet_count = format_optional_u64(
            report.latest_discovery_wallet_freshness_history_selected_wallet_count,
        ),
        latest_discovery_wallet_freshness_history_published_wallet_count = format_optional_u64(
            report.latest_discovery_wallet_freshness_history_published_wallet_count,
        ),
        latest_discovery_wallet_freshness_history_recent_under_gate = format_optional_bool(
            report.latest_discovery_wallet_freshness_history_recent_under_gate,
        ),
        latest_discovery_wallet_freshness_history_raw_window_complete = format_optional_bool(
            report.latest_discovery_wallet_freshness_history_raw_window_complete,
        ),
        latest_discovery_wallet_freshness_history_reason = format_optional_str(
            report.latest_discovery_wallet_freshness_history_reason.as_deref(),
        ),
        startup_follow_snapshot_surface = report.startup_follow_snapshot_surface,
        nonpublished_fail_closed_clear_surface_present = report
            .nonpublished_fail_closed_clear_surface_present,
        degraded_preserve_surface_present = report.degraded_preserve_surface_present,
        publication_truth_follow_snapshot_update_surface = report
            .publication_truth_follow_snapshot_update_surface,
        active_followlist_persistence_surface = report.active_followlist_persistence_surface,
        runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished = report
            .runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished,
        followlist_population_path_surface_present = report.followlist_population_path_surface_present,
        discovery_cycle_can_write_active_follow_wallets = report
            .discovery_cycle_can_write_active_follow_wallets,
        publication_truth_can_recover_startup_follow_snapshot = report
            .publication_truth_can_recover_startup_follow_snapshot,
        empty_follow_universe_conclusion = report.empty_follow_universe_conclusion,
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
    use chrono::Duration;
    use copybot_discovery::wallet_freshness_audit::{
        WalletFreshnessAuditReport, WalletFreshnessCaptureSnapshot, WalletFreshnessRawCycleSample,
        WalletFreshnessRawTruthStatus, WalletFreshnessRotationSignal,
        WalletFreshnessShadowSignalEvidence, WalletFreshnessVerdict, WalletShadowSignalVerdict,
        WalletUniverseComparison,
    };
    use copybot_storage::{DiscoveryPublicationStateUpdate, SqliteStore};
    use serde_json::Value;
    use tempfile::TempDir;

    fn repo_config_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }

    fn create_fixture() -> Result<(TempDir, PathBuf, SqliteStore)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let db_path = temp.path().join("empty-follow-universe-audit.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        Ok((temp, db_path, store))
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
            published_vs_current_raw: comparison(
                &published_wallet_ids,
                &current_raw_top_wallet_ids,
            ),
            active_follow_vs_current_raw: comparison(
                &active_follow_wallet_ids,
                &current_raw_top_wallet_ids,
            ),
            active_follow_vs_published: comparison(
                &active_follow_wallet_ids,
                &published_wallet_ids,
            ),
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
        store.append_discovery_wallet_freshness_capture(&snapshot.to_storage_write()?)?;
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

    fn fixture_config(db_path: &Path) -> Config {
        Config {
            db_path: db_path.to_path_buf(),
            config_path: repo_config_path(),
            json: true,
        }
    }

    #[test]
    fn parse_args_reads_required_empty_follow_universe_audit_flags() -> Result<()> {
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
        seed_wallet_freshness_capture(
            &store,
            now - Duration::minutes(5),
            Some("fail_closed"),
            Some("raw_window_incomplete_no_recent_published_universe"),
            false,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            "raw_window_incomplete_no_recent_published_universe",
        )?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some(
                "raw_window_incomplete_no_recent_published_universe".to_string(),
            ),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let report = run(&fixture_config(&db_path))?;
        let json = serde_json::to_value(report)?;
        for field in [
            "active_follow_wallet_count",
            "followlist_total_rows",
            "followlist_active_min_added_at_utc",
            "followlist_active_max_added_at_utc",
            "followlist_inactive_with_removed_at_count",
            "latest_discovery_wallet_freshness_history_captured_at_utc",
            "latest_discovery_wallet_freshness_history_runtime_mode",
            "latest_discovery_wallet_freshness_history_scoring_source",
            "latest_discovery_wallet_freshness_history_active_follow_wallet_count",
            "latest_discovery_wallet_freshness_history_selected_wallet_count",
            "latest_discovery_wallet_freshness_history_published_wallet_count",
            "latest_discovery_wallet_freshness_history_recent_under_gate",
            "latest_discovery_wallet_freshness_history_raw_window_complete",
            "latest_discovery_wallet_freshness_history_reason",
            "startup_follow_snapshot_surface",
            "nonpublished_fail_closed_clear_surface_present",
            "degraded_preserve_surface_present",
            "publication_truth_follow_snapshot_update_surface",
            "active_followlist_persistence_surface",
            "runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished",
            "followlist_population_path_surface_present",
            "discovery_cycle_can_write_active_follow_wallets",
            "publication_truth_can_recover_startup_follow_snapshot",
            "empty_follow_universe_conclusion",
        ] {
            assert!(json.get(field).is_some(), "missing json field: {field}");
        }
        Ok(())
    }

    #[test]
    fn stale_empty_publication_truth_yields_publication_truth_conclusion() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        seed_wallet_freshness_capture(
            &store,
            now - Duration::hours(3),
            Some("fail_closed"),
            Some("raw_window_incomplete_no_recent_published_universe"),
            false,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            "raw_window_incomplete_no_recent_published_universe",
        )?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_incomplete_no_recent_published_universe".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: Some(
                "raw_window_incomplete_no_recent_published_universe".to_string(),
            ),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let report = run(&fixture_config(&db_path))?;
        assert_eq!(report.active_follow_wallet_count, 0);
        assert_eq!(
            report.empty_follow_universe_conclusion,
            CONCLUSION_PUBLICATION_TRUTH_EMPTY_OR_STALE
        );
        Ok(())
    }

    #[test]
    fn ambiguous_persisted_truth_never_overclaims_stronger_than_fail_closed_or_insufficient(
    ) -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet("wallet-old", now - Duration::days(2), "seed-follow")?;
        store.deactivate_follow_wallet(
            "wallet-old",
            now - Duration::days(1),
            "manual-fixture-fail-closed-like-clear",
        )?;

        let report = run(&fixture_config(&db_path))?;
        assert!(
            report.empty_follow_universe_conclusion
                == CONCLUSION_RUNTIME_CLEARED_STARTUP_FOLLOW_SNAPSHOT
                || report.empty_follow_universe_conclusion == CONCLUSION_INSUFFICIENT_EVIDENCE,
            "unexpected stronger claim: {}",
            report.empty_follow_universe_conclusion
        );
        assert_ne!(
            report.empty_follow_universe_conclusion,
            CONCLUSION_PUBLICATION_TRUTH_EMPTY_OR_STALE
        );
        assert_ne!(
            report.empty_follow_universe_conclusion,
            CONCLUSION_FOLLOWLIST_POPULATION_PATH_NOT_RUNNING
        );
        Ok(())
    }

    #[test]
    fn repeated_runs_with_same_inputs_are_deterministic() -> Result<()> {
        let (_temp, db_path, store) = create_fixture()?;
        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        seed_wallet_freshness_capture(
            &store,
            now - Duration::minutes(10),
            Some("fail_closed"),
            Some("raw_window_incomplete_no_recent_published_universe"),
            false,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            "raw_window_incomplete_no_recent_published_universe",
        )?;
        let config = fixture_config(&db_path);

        let first = render_output(&run(&config)?, true)?;
        let second = render_output(&run(&config)?, true)?;
        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn current_source_grounding_strings_still_exist() {
        assert!(APP_MAIN_SOURCE.contains("fn startup_follow_snapshot_from_publication_truth("));
        assert!(APP_MAIN_SOURCE.contains("fn apply_fail_closed_runtime_follow_surface_if_needed("));
        assert!(APP_SHADOW_RUNTIME_HELPERS_SOURCE
            .contains("pub(crate) fn apply_follow_snapshot_update("));
        assert!(DISCOVERY_SOURCE.contains("fn fail_close_without_recent_universe("));
        assert!(STORAGE_DISCOVERY_SOURCE
            .contains("pub fn persist_discovery_cycle_with_snapshot_metadata("));
    }

    #[test]
    fn render_output_json_round_trips_required_conclusion_field() -> Result<()> {
        let report = EmptyFollowUniverseAuditReport {
            config_path: "/tmp/config.toml".to_string(),
            active_follow_wallet_count: 0,
            followlist_total_rows: 0,
            followlist_active_min_added_at_utc: None,
            followlist_active_max_added_at_utc: None,
            followlist_inactive_with_removed_at_count: 0,
            latest_discovery_wallet_freshness_history_captured_at_utc: None,
            latest_discovery_wallet_freshness_history_runtime_mode: None,
            latest_discovery_wallet_freshness_history_scoring_source: None,
            latest_discovery_wallet_freshness_history_active_follow_wallet_count: None,
            latest_discovery_wallet_freshness_history_selected_wallet_count: None,
            latest_discovery_wallet_freshness_history_published_wallet_count: None,
            latest_discovery_wallet_freshness_history_recent_under_gate: None,
            latest_discovery_wallet_freshness_history_raw_window_complete: None,
            latest_discovery_wallet_freshness_history_reason: None,
            startup_follow_snapshot_surface: STARTUP_FOLLOW_SNAPSHOT_SURFACE.to_string(),
            nonpublished_fail_closed_clear_surface_present: true,
            degraded_preserve_surface_present: true,
            publication_truth_follow_snapshot_update_surface:
                PUBLICATION_TRUTH_FOLLOW_SNAPSHOT_UPDATE_SURFACE.to_string(),
            active_followlist_persistence_surface: ACTIVE_FOLLOWLIST_PERSISTENCE_SURFACE
                .to_string(),
            runtime_can_clear_follow_snapshot_when_fail_closed_and_nonpublished: true,
            followlist_population_path_surface_present: true,
            discovery_cycle_can_write_active_follow_wallets: true,
            publication_truth_can_recover_startup_follow_snapshot: true,
            empty_follow_universe_conclusion: CONCLUSION_INSUFFICIENT_EVIDENCE.to_string(),
        };
        let json: Value = serde_json::from_str(&render_output(&report, true)?)?;
        assert_eq!(
            json["empty_follow_universe_conclusion"],
            Value::String(CONCLUSION_INSUFFICIENT_EVIDENCE.to_string())
        );
        Ok(())
    }
}
