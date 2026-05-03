use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use copybot_config::{load_from_path, AppConfig, DiscoveryConfig};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_raw_window_zero_eligible_audit --db <path> --config <path> [--now <rfc3339>] [--json]";

const RAW_WINDOW_SAMPLE_LIMIT: usize = 256;
const ACTIVE_FOLLOW_SAMPLE_LIMIT: usize = 1_000;
const TOKEN_QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;

const CONCLUSION_WINDOW_STILL_ACCUMULATING: &str =
    "raw_window_zero_eligible_because_window_still_accumulating";
const CONCLUSION_NO_WALLET_ACTIVITY: &str =
    "raw_window_zero_eligible_because_no_wallet_activity_in_required_window";
const CONCLUSION_FOLLOW_UNIVERSE_EMPTY: &str =
    "raw_window_zero_eligible_because_follow_universe_empty";
const CONCLUSION_WALLETS_FAIL_THRESHOLDS: &str =
    "raw_window_zero_eligible_because_wallets_fail_scoring_thresholds";
const CONCLUSION_QUALITY_FILTERS: &str =
    "raw_window_zero_eligible_because_quality_filters_exclude_all_candidates";
const CONCLUSION_RUNTIME_NOT_USING_RAW_WINDOW: &str =
    "raw_window_zero_eligible_because_runtime_not_using_raw_window";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const RUNTIME_CURSOR_LOAD_SURFACE: &str =
    "copybot_storage::SqliteStore::load_discovery_runtime_cursor_read_only";
const PUBLICATION_STATE_SURFACE: &str =
    "copybot_storage::SqliteStore::discovery_publication_state_read_only";
const RAW_WINDOW_SCORING_SURFACE: &str = "copybot_discovery::DiscoveryService::run_cycle";
const WALLET_ELIGIBILITY_SURFACE: &str = "copybot_discovery::followlist::rank_follow_candidates";
const TOP_WALLET_SELECTION_SURFACE: &str =
    "copybot_discovery::followlist::desired_wallets/top_wallet_labels";
const QUALITY_FILTER_SURFACE: &str =
    "copybot_discovery::DiscoveryService::build_wallet_snapshots/token_quality";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");
const FOLLOWLIST_SOURCE: &str = include_str!("../followlist.rs");

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!("{}", render_output(&report)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    config_path: PathBuf,
    now_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct RawWindowZeroEligibleAuditReport {
    config_path: String,
    db_path: String,
    now_utc: DateTime<Utc>,
    configured_sqlite_path: String,
    runtime_db_open_mode: String,
    scoring_window_days: u32,
    required_window_start_utc: DateTime<Utc>,
    runtime_cursor_ts_utc: Option<DateTime<Utc>>,
    runtime_cursor_slot: Option<u64>,
    runtime_cursor_signature: Option<String>,
    runtime_cursor_before_required_window_start: Option<bool>,
    publication_runtime_mode: Option<String>,
    publication_reason: Option<String>,
    publication_last_published_at_utc: Option<DateTime<Utc>>,
    publication_last_published_window_start_utc: Option<DateTime<Utc>>,
    publication_updated_at_utc: Option<DateTime<Utc>>,
    publication_published_scoring_source: Option<String>,
    publication_published_wallet_count: Option<u64>,
    current_scoring_source: Option<String>,
    observed_swaps_min_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_bounded_index_available: bool,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    recent_raw_db_path_from_config: Option<String>,
    recent_raw_db_path_exists: bool,
    recent_raw_accessible: bool,
    recent_raw_min_ts_utc: Option<DateTime<Utc>>,
    recent_raw_max_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_frontier_after_runtime_cursor: Option<bool>,
    recent_raw_frontier_after_runtime_cursor: Option<bool>,
    raw_window_has_any_rows: Option<bool>,
    raw_window_has_rows_after_required_window_start: Option<bool>,
    raw_window_first_ts_after_required_window_start: Option<DateTime<Utc>>,
    raw_window_last_sample_ts_after_required_window_start: Option<DateTime<Utc>>,
    raw_window_bounded_sample_limit: u64,
    raw_window_bounded_sample_count: Option<u64>,
    raw_window_bounded_sample_capped: Option<bool>,
    raw_window_sample_distinct_wallet_count: Option<u64>,
    raw_window_sample_distinct_token_out_count: Option<u64>,
    raw_window_coverage_starts_after_required_window_start: Option<bool>,
    token_quality_cache_sample_mint_count: Option<u64>,
    token_quality_cache_sample_rows: Option<u64>,
    token_quality_cache_sample_missing_rows: Option<u64>,
    token_quality_cache_sample_stale_rows: Option<u64>,
    latest_freshness_captured_at_utc: Option<DateTime<Utc>>,
    latest_freshness_reason: Option<String>,
    latest_freshness_raw_truth_reason: Option<String>,
    latest_freshness_raw_truth_sufficient: Option<bool>,
    latest_freshness_raw_truth_observed_swaps_loaded: Option<u64>,
    active_follow_wallets: Option<u64>,
    active_follow_wallets_capped: bool,
    eligible_wallets: Option<u64>,
    top_wallets_len: Option<u64>,
    wallet_threshold_min_score: f64,
    wallet_threshold_min_trades: u32,
    wallet_threshold_min_active_days: u32,
    wallet_threshold_min_buy_count: u32,
    wallet_threshold_min_tradable_ratio: f64,
    wallet_threshold_max_rug_ratio: f64,
    runtime_cursor_load_surface: String,
    publication_state_surface: String,
    raw_window_scoring_surface: String,
    wallet_eligibility_surface: String,
    top_wallet_selection_surface: String,
    quality_filter_surface: String,
    wallet_eligibility_filters_eligible_and_min_score: bool,
    top_wallet_selection_depends_on_ranked_eligible_wallets: bool,
    quality_filter_surface_present: bool,
    raw_window_zero_eligible_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct PublicationFacts {
    runtime_mode: Option<String>,
    reason: Option<String>,
    last_published_at_utc: Option<DateTime<Utc>>,
    last_published_window_start_utc: Option<DateTime<Utc>>,
    updated_at_utc: Option<DateTime<Utc>>,
    published_scoring_source: Option<String>,
    published_wallet_count: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct FrontierFacts {
    observed_swaps_bounded_index_available: bool,
    observed_swaps_min_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct RecentRawFacts {
    db_path: Option<PathBuf>,
    db_path_exists: bool,
    accessible: bool,
    min_ts_utc: Option<DateTime<Utc>>,
    max_ts_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct RawWindowSampleFacts {
    has_rows_after_required_window_start: Option<bool>,
    first_ts_after_required_window_start: Option<DateTime<Utc>>,
    last_sample_ts_after_required_window_start: Option<DateTime<Utc>>,
    bounded_sample_count: Option<u64>,
    bounded_sample_capped: Option<bool>,
    distinct_wallet_count: Option<u64>,
    distinct_token_out_count: Option<u64>,
    token_out_mints: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct TokenQualitySampleFacts {
    sample_mint_count: Option<u64>,
    cache_rows: Option<u64>,
    missing_rows: Option<u64>,
    stale_rows: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct LatestFreshnessFacts {
    captured_at_utc: Option<DateTime<Utc>>,
    reason: Option<String>,
    raw_truth_reason: Option<String>,
    raw_truth_sufficient: Option<bool>,
    raw_truth_observed_swaps_loaded: Option<u64>,
    active_follow_wallets: Option<u64>,
    eligible_wallets: Option<u64>,
    top_wallets_len: Option<u64>,
    published_scoring_source: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ActiveFollowFacts {
    active_follow_wallets: Option<u64>,
    capped: bool,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    wallet_eligibility_filters_eligible_and_min_score: bool,
    top_wallet_selection_depends_on_ranked_eligible_wallets: bool,
    quality_filter_surface_present: bool,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1), Utc::now())
}

fn parse_args_from<I>(args: I, default_now: DateTime<Utc>) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut db_path: Option<PathBuf> = None;
    let mut config_path: Option<PathBuf> = None;
    let mut now_utc = default_now;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db_path = Some(PathBuf::from(parse_string_arg("--db", args.next())?)),
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--now" => now_utc = parse_db_ts(&parse_string_arg("--now", args.next())?)?,
            "--json" => {}
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db"))?,
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        now_utc,
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

fn run(config: &Config) -> Result<RawWindowZeroEligibleAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let runtime_store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening runtime db {}", config.db_path.display()))?;
    let runtime_conn = open_read_only_connection(&config.db_path)?;

    let runtime_cursor = runtime_store.load_discovery_runtime_cursor_read_only()?;
    let publication = load_publication_facts(&runtime_store)?;
    let frontier = load_frontier_facts(&runtime_conn)?;
    let recent_raw = load_recent_raw_facts(&config.config_path, &loaded_config)?;
    let required_window_start_utc = required_window_start(&loaded_config.discovery, config.now_utc);
    let raw_window_sample = load_raw_window_sample_facts(&runtime_conn, required_window_start_utc)?;
    let token_quality_sample = load_token_quality_sample_facts(
        &runtime_conn,
        &raw_window_sample.token_out_mints,
        config.now_utc,
    )?;
    let latest_freshness = load_latest_freshness_facts(&runtime_conn)?;
    let active_follow = load_active_follow_facts(&runtime_conn)?;
    let code_paths = inspect_code_path_facts();
    let current_scoring_source = publication
        .published_scoring_source
        .clone()
        .or_else(|| latest_freshness.published_scoring_source.clone());
    let derived = DerivedFacts::new(
        runtime_cursor.as_ref(),
        required_window_start_utc,
        &frontier,
        &recent_raw,
    );
    let conclusion = choose_conclusion(ConclusionInputs {
        current_scoring_source: current_scoring_source.as_deref(),
        publication_reason: publication.reason.as_deref(),
        observed_swaps_min_ts_utc: frontier.observed_swaps_min_ts_utc,
        raw_window_sample: &raw_window_sample,
        token_quality_sample: &token_quality_sample,
        latest_freshness: &latest_freshness,
        active_follow: &active_follow,
        config: &loaded_config.discovery,
    });

    Ok(RawWindowZeroEligibleAuditReport {
        config_path: config.config_path.display().to_string(),
        db_path: config.db_path.display().to_string(),
        now_utc: config.now_utc,
        configured_sqlite_path: resolve_db_path(&config.config_path, &loaded_config.sqlite.path)
            .display()
            .to_string(),
        runtime_db_open_mode: "wal_aware_read_only".to_string(),
        scoring_window_days: loaded_config.discovery.scoring_window_days,
        required_window_start_utc,
        runtime_cursor_ts_utc: runtime_cursor.as_ref().map(|cursor| cursor.ts_utc),
        runtime_cursor_slot: runtime_cursor.as_ref().map(|cursor| cursor.slot),
        runtime_cursor_signature: runtime_cursor
            .as_ref()
            .map(|cursor| cursor.signature.clone()),
        runtime_cursor_before_required_window_start: derived
            .runtime_cursor_before_required_window_start,
        publication_runtime_mode: publication.runtime_mode,
        publication_reason: publication.reason,
        publication_last_published_at_utc: publication.last_published_at_utc,
        publication_last_published_window_start_utc: publication.last_published_window_start_utc,
        publication_updated_at_utc: publication.updated_at_utc,
        publication_published_scoring_source: publication.published_scoring_source,
        publication_published_wallet_count: publication.published_wallet_count,
        current_scoring_source,
        observed_swaps_min_ts_utc: frontier.observed_swaps_min_ts_utc,
        observed_swaps_max_ts_utc: frontier.observed_swaps_max_ts_utc,
        observed_swaps_bounded_index_available: frontier.observed_swaps_bounded_index_available,
        wallet_activity_days_max_day_utc: frontier.wallet_activity_days_max_day_utc,
        recent_raw_db_path_from_config: recent_raw
            .db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        recent_raw_db_path_exists: recent_raw.db_path_exists,
        recent_raw_accessible: recent_raw.accessible,
        recent_raw_min_ts_utc: recent_raw.min_ts_utc,
        recent_raw_max_ts_utc: recent_raw.max_ts_utc,
        observed_swaps_frontier_after_runtime_cursor: derived
            .observed_swaps_frontier_after_runtime_cursor,
        recent_raw_frontier_after_runtime_cursor: derived.recent_raw_frontier_after_runtime_cursor,
        raw_window_has_any_rows: frontier
            .observed_swaps_bounded_index_available
            .then_some(frontier.observed_swaps_min_ts_utc.is_some()),
        raw_window_has_rows_after_required_window_start: raw_window_sample
            .has_rows_after_required_window_start,
        raw_window_first_ts_after_required_window_start: raw_window_sample
            .first_ts_after_required_window_start,
        raw_window_last_sample_ts_after_required_window_start: raw_window_sample
            .last_sample_ts_after_required_window_start,
        raw_window_bounded_sample_limit: RAW_WINDOW_SAMPLE_LIMIT as u64,
        raw_window_bounded_sample_count: raw_window_sample.bounded_sample_count,
        raw_window_bounded_sample_capped: raw_window_sample.bounded_sample_capped,
        raw_window_sample_distinct_wallet_count: raw_window_sample.distinct_wallet_count,
        raw_window_sample_distinct_token_out_count: raw_window_sample.distinct_token_out_count,
        raw_window_coverage_starts_after_required_window_start: frontier
            .observed_swaps_min_ts_utc
            .map(|min_ts| min_ts > required_window_start_utc),
        token_quality_cache_sample_mint_count: token_quality_sample.sample_mint_count,
        token_quality_cache_sample_rows: token_quality_sample.cache_rows,
        token_quality_cache_sample_missing_rows: token_quality_sample.missing_rows,
        token_quality_cache_sample_stale_rows: token_quality_sample.stale_rows,
        latest_freshness_captured_at_utc: latest_freshness.captured_at_utc,
        latest_freshness_reason: latest_freshness.reason,
        latest_freshness_raw_truth_reason: latest_freshness.raw_truth_reason,
        latest_freshness_raw_truth_sufficient: latest_freshness.raw_truth_sufficient,
        latest_freshness_raw_truth_observed_swaps_loaded: latest_freshness
            .raw_truth_observed_swaps_loaded,
        active_follow_wallets: active_follow
            .active_follow_wallets
            .or(latest_freshness.active_follow_wallets),
        active_follow_wallets_capped: active_follow.capped,
        eligible_wallets: latest_freshness.eligible_wallets,
        top_wallets_len: latest_freshness.top_wallets_len,
        wallet_threshold_min_score: loaded_config.discovery.min_score,
        wallet_threshold_min_trades: loaded_config.discovery.min_trades,
        wallet_threshold_min_active_days: loaded_config.discovery.min_active_days,
        wallet_threshold_min_buy_count: loaded_config.discovery.min_buy_count,
        wallet_threshold_min_tradable_ratio: loaded_config.discovery.min_tradable_ratio,
        wallet_threshold_max_rug_ratio: loaded_config.discovery.max_rug_ratio,
        runtime_cursor_load_surface: RUNTIME_CURSOR_LOAD_SURFACE.to_string(),
        publication_state_surface: PUBLICATION_STATE_SURFACE.to_string(),
        raw_window_scoring_surface: RAW_WINDOW_SCORING_SURFACE.to_string(),
        wallet_eligibility_surface: WALLET_ELIGIBILITY_SURFACE.to_string(),
        top_wallet_selection_surface: TOP_WALLET_SELECTION_SURFACE.to_string(),
        quality_filter_surface: QUALITY_FILTER_SURFACE.to_string(),
        wallet_eligibility_filters_eligible_and_min_score: code_paths
            .wallet_eligibility_filters_eligible_and_min_score,
        top_wallet_selection_depends_on_ranked_eligible_wallets: code_paths
            .top_wallet_selection_depends_on_ranked_eligible_wallets,
        quality_filter_surface_present: code_paths.quality_filter_surface_present,
        raw_window_zero_eligible_conclusion: conclusion.to_string(),
    })
}

#[derive(Debug, Clone, Default)]
struct DerivedFacts {
    runtime_cursor_before_required_window_start: Option<bool>,
    observed_swaps_frontier_after_runtime_cursor: Option<bool>,
    recent_raw_frontier_after_runtime_cursor: Option<bool>,
}

impl DerivedFacts {
    fn new(
        runtime_cursor: Option<&DiscoveryRuntimeCursor>,
        required_window_start_utc: DateTime<Utc>,
        frontier: &FrontierFacts,
        recent_raw: &RecentRawFacts,
    ) -> Self {
        Self {
            runtime_cursor_before_required_window_start: runtime_cursor
                .map(|cursor| cursor.ts_utc < required_window_start_utc),
            observed_swaps_frontier_after_runtime_cursor: runtime_cursor
                .zip(frontier.observed_swaps_max_ts_utc)
                .map(|(cursor, frontier_ts)| frontier_ts > cursor.ts_utc),
            recent_raw_frontier_after_runtime_cursor: runtime_cursor
                .zip(recent_raw.max_ts_utc)
                .map(|(cursor, frontier_ts)| frontier_ts > cursor.ts_utc),
        }
    }
}

fn open_read_only_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed opening sqlite db read-only: {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .context("failed setting sqlite busy timeout")?;
    conn.pragma_update(None, "query_only", "ON")
        .context("failed enabling sqlite query_only")?;
    Ok(conn)
}

fn resolve_db_path(config_path: &Path, configured_db_path: &str) -> PathBuf {
    let configured_db_path = PathBuf::from(configured_db_path);
    if configured_db_path.is_absolute() {
        return configured_db_path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_db_path)
}

fn required_window_start(config: &DiscoveryConfig, now: DateTime<Utc>) -> DateTime<Utc> {
    now - Duration::days(config.scoring_window_days.max(1) as i64)
}

fn load_publication_facts(store: &SqliteStore) -> Result<PublicationFacts> {
    let Some(state) = store.discovery_publication_state_read_only()? else {
        return Ok(PublicationFacts::default());
    };
    Ok(PublicationFacts {
        runtime_mode: Some(state.runtime_mode.as_str().to_string()),
        reason: Some(state.reason),
        last_published_at_utc: state.last_published_at,
        last_published_window_start_utc: state.last_published_window_start,
        updated_at_utc: Some(state.updated_at),
        published_scoring_source: state.published_scoring_source,
        published_wallet_count: state
            .published_wallet_ids
            .map(|wallets| wallets.len() as u64),
    })
}

fn load_frontier_facts(conn: &Connection) -> Result<FrontierFacts> {
    let observed_swaps_bounded_index_available = observed_swaps_bounded_index_available(conn)?;
    Ok(FrontierFacts {
        observed_swaps_bounded_index_available,
        observed_swaps_min_ts_utc: query_observed_swaps_boundary_ts(conn, false)?,
        observed_swaps_max_ts_utc: query_observed_swaps_boundary_ts(conn, true)?,
        wallet_activity_days_max_day_utc: query_wallet_activity_days_max_day_utc(conn)?,
    })
}

fn load_recent_raw_facts(config_path: &Path, loaded_config: &AppConfig) -> Result<RecentRawFacts> {
    let db_path = resolve_db_path(config_path, &loaded_config.recent_raw_journal.path);
    if !db_path.exists() {
        return Ok(RecentRawFacts {
            db_path: Some(db_path),
            db_path_exists: false,
            accessible: false,
            ..RecentRawFacts::default()
        });
    }
    let conn = match open_read_only_connection(&db_path) {
        Ok(conn) => conn,
        Err(_) => {
            return Ok(RecentRawFacts {
                db_path: Some(db_path),
                db_path_exists: true,
                accessible: false,
                ..RecentRawFacts::default()
            });
        }
    };
    Ok(RecentRawFacts {
        db_path: Some(db_path),
        db_path_exists: true,
        accessible: true,
        min_ts_utc: query_observed_swaps_boundary_ts(&conn, false)?,
        max_ts_utc: query_observed_swaps_boundary_ts(&conn, true)?,
    })
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

fn sqlite_index_exists(conn: &Connection, index_name: &str) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?1 LIMIT 1",
            [index_name],
            |row| row.get(0),
        )
        .optional()
        .context("failed checking sqlite index existence")?;
    Ok(exists.is_some())
}

fn observed_swaps_bounded_index_available(conn: &Connection) -> Result<bool> {
    Ok(sqlite_table_exists(conn, "observed_swaps")?
        && sqlite_index_exists(conn, "idx_observed_swaps_ts_slot_signature")?)
}

fn query_observed_swaps_boundary_ts(
    conn: &Connection,
    descending: bool,
) -> Result<Option<DateTime<Utc>>> {
    if !observed_swaps_bounded_index_available(conn)? {
        return Ok(None);
    }
    let direction = if descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts
         FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
         ORDER BY ts {direction}, slot {direction}, signature {direction}
         LIMIT 1"
    );
    let raw: Option<String> = conn
        .query_row(&sql, [], |row| row.get(0))
        .optional()
        .context("failed loading indexed observed_swaps boundary timestamp")?;
    raw.as_deref().map(parse_db_ts).transpose()
}

fn query_wallet_activity_days_max_day_utc(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    if !sqlite_table_exists(conn, "wallet_activity_days")?
        || !sqlite_index_exists(conn, "idx_wallet_activity_days_day_wallet")?
    {
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT activity_day
             FROM wallet_activity_days INDEXED BY idx_wallet_activity_days_day_wallet
             ORDER BY activity_day DESC, wallet_id DESC
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed loading indexed wallet_activity_days max day")?;
    raw.as_deref().map(parse_day_or_ts).transpose()
}

fn load_raw_window_sample_facts(
    conn: &Connection,
    required_window_start_utc: DateTime<Utc>,
) -> Result<RawWindowSampleFacts> {
    if !sqlite_table_exists(conn, "observed_swaps")?
        || !sqlite_index_exists(conn, "idx_observed_swaps_ts_slot_signature")?
    {
        return Ok(RawWindowSampleFacts::default());
    }
    let mut stmt = conn
        .prepare(
            "SELECT ts, wallet_id, token_out
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             WHERE ts >= ?1
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?2",
        )
        .context("failed preparing bounded observed_swaps raw-window sample query")?;
    let limit = (RAW_WINDOW_SAMPLE_LIMIT + 1) as i64;
    let mut rows = stmt
        .query(params![required_window_start_utc.to_rfc3339(), limit])
        .context("failed querying bounded observed_swaps raw-window sample")?;

    let mut sample_count = 0usize;
    let mut first_ts = None;
    let mut last_ts = None;
    let mut wallets = HashSet::new();
    let mut token_out_mints = HashSet::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating bounded observed_swaps raw-window sample")?
    {
        sample_count = sample_count.saturating_add(1);
        if sample_count > RAW_WINDOW_SAMPLE_LIMIT {
            break;
        }
        let ts_raw: String = row.get(0).context("failed reading observed_swaps.ts")?;
        let ts = parse_db_ts(&ts_raw)?;
        first_ts = first_ts.or(Some(ts));
        last_ts = Some(ts);
        let wallet_id: String = row
            .get(1)
            .context("failed reading observed_swaps.wallet_id")?;
        let token_out: String = row
            .get(2)
            .context("failed reading observed_swaps.token_out")?;
        wallets.insert(wallet_id);
        token_out_mints.insert(token_out);
    }
    let capped = sample_count > RAW_WINDOW_SAMPLE_LIMIT;
    let bounded_count = sample_count.min(RAW_WINDOW_SAMPLE_LIMIT) as u64;
    let mut token_out_mints = token_out_mints.into_iter().collect::<Vec<_>>();
    token_out_mints.sort();
    Ok(RawWindowSampleFacts {
        has_rows_after_required_window_start: Some(bounded_count > 0),
        first_ts_after_required_window_start: first_ts,
        last_sample_ts_after_required_window_start: last_ts,
        bounded_sample_count: Some(bounded_count),
        bounded_sample_capped: Some(capped),
        distinct_wallet_count: Some(wallets.len() as u64),
        distinct_token_out_count: Some(token_out_mints.len() as u64),
        token_out_mints,
    })
}

fn load_token_quality_sample_facts(
    conn: &Connection,
    token_out_mints: &[String],
    now_utc: DateTime<Utc>,
) -> Result<TokenQualitySampleFacts> {
    if token_out_mints.is_empty() || !sqlite_table_exists(conn, "token_quality_cache")? {
        return Ok(TokenQualitySampleFacts {
            sample_mint_count: Some(token_out_mints.len() as u64),
            cache_rows: Some(0),
            missing_rows: Some(token_out_mints.len() as u64),
            stale_rows: Some(0),
        });
    }
    let mut cache_rows = 0u64;
    let mut stale_rows = 0u64;
    for mint in token_out_mints {
        let fetched_at_raw: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM token_quality_cache WHERE mint = ?1",
                [mint],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed querying token_quality_cache for mint {mint}"))?;
        if let Some(raw) = fetched_at_raw {
            cache_rows = cache_rows.saturating_add(1);
            let fetched_at = parse_db_ts(&raw)?;
            if fetched_at + Duration::seconds(TOKEN_QUALITY_CACHE_TTL_SECONDS) < now_utc {
                stale_rows = stale_rows.saturating_add(1);
            }
        }
    }
    Ok(TokenQualitySampleFacts {
        sample_mint_count: Some(token_out_mints.len() as u64),
        cache_rows: Some(cache_rows),
        missing_rows: Some((token_out_mints.len() as u64).saturating_sub(cache_rows)),
        stale_rows: Some(stale_rows),
    })
}

fn load_latest_freshness_facts(conn: &Connection) -> Result<LatestFreshnessFacts> {
    if !sqlite_table_exists(conn, "discovery_wallet_freshness_history")? {
        return Ok(LatestFreshnessFacts::default());
    }
    let raw: Option<(String, String, i64, String, String, String, String)> = conn
        .query_row(
            "SELECT
                captured_at,
                reason,
                raw_truth_sufficient,
                raw_truth_reason,
                active_follow_wallet_ids_json,
                current_raw_top_wallet_ids_json,
                audit_json
             FROM discovery_wallet_freshness_history
             ORDER BY captured_at DESC, capture_id DESC
             LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()
        .context("failed loading latest discovery wallet freshness history row")?;
    let Some((
        captured_at_raw,
        reason,
        raw_truth_sufficient_raw,
        raw_truth_reason,
        active_follow_wallet_ids_json,
        current_raw_top_wallet_ids_json,
        audit_json,
    )) = raw
    else {
        return Ok(LatestFreshnessFacts::default());
    };
    let audit: JsonValue = serde_json::from_str(&audit_json)
        .context("failed deserializing discovery wallet freshness audit_json")?;
    let active_follow_wallets = parse_string_array_json_len(&active_follow_wallet_ids_json)
        .or_else(|| {
            audit
                .pointer("/active_follow_wallet_ids")
                .and_then(JsonValue::as_array)
                .map(|items| items.len() as u64)
        });
    let top_wallets_len =
        parse_string_array_json_len(&current_raw_top_wallet_ids_json).or_else(|| {
            audit
                .pointer("/current_raw_top_wallet_ids")
                .and_then(JsonValue::as_array)
                .map(|items| items.len() as u64)
        });
    let eligible_wallets = audit
        .pointer("/raw_truth/eligible_wallet_count")
        .and_then(JsonValue::as_u64);
    let raw_truth_observed_swaps_loaded = audit
        .pointer("/raw_truth/observed_swaps_loaded")
        .and_then(JsonValue::as_u64);
    let published_scoring_source = audit
        .pointer("/published_scoring_source")
        .and_then(JsonValue::as_str)
        .map(str::to_string);

    Ok(LatestFreshnessFacts {
        captured_at_utc: Some(parse_db_ts(&captured_at_raw)?),
        reason: Some(reason),
        raw_truth_reason: Some(raw_truth_reason),
        raw_truth_sufficient: Some(raw_truth_sufficient_raw != 0),
        raw_truth_observed_swaps_loaded,
        active_follow_wallets,
        eligible_wallets,
        top_wallets_len,
        published_scoring_source,
    })
}

fn parse_string_array_json_len(raw: &str) -> Option<u64> {
    serde_json::from_str::<Vec<String>>(raw)
        .ok()
        .map(|items| items.len() as u64)
}

fn load_active_follow_facts(conn: &Connection) -> Result<ActiveFollowFacts> {
    if !sqlite_table_exists(conn, "followlist")? {
        return Ok(ActiveFollowFacts::default());
    }
    let mut stmt = conn
        .prepare("SELECT wallet_id FROM followlist WHERE active = 1 LIMIT ?1")
        .context("failed preparing bounded active followlist sample query")?;
    let mut rows = stmt
        .query(params![(ACTIVE_FOLLOW_SAMPLE_LIMIT + 1) as i64])
        .context("failed querying bounded active followlist sample")?;
    let mut seen = 0usize;
    while rows
        .next()
        .context("failed iterating active followlist sample")?
        .is_some()
    {
        seen = seen.saturating_add(1);
        if seen > ACTIVE_FOLLOW_SAMPLE_LIMIT {
            return Ok(ActiveFollowFacts {
                active_follow_wallets: None,
                capped: true,
            });
        }
    }
    Ok(ActiveFollowFacts {
        active_follow_wallets: Some(seen as u64),
        capped: false,
    })
}

fn inspect_code_path_facts() -> CodePathFacts {
    CodePathFacts {
        wallet_eligibility_filters_eligible_and_min_score: FOLLOWLIST_SOURCE
            .contains(".filter(|item| item.eligible && item.score >= min_score)"),
        top_wallet_selection_depends_on_ranked_eligible_wallets: FOLLOWLIST_SOURCE
            .contains("pub(super) fn desired_wallets(ranked")
            && FOLLOWLIST_SOURCE.contains(".take(follow_top_n as usize)")
            && FOLLOWLIST_SOURCE.contains("pub(super) fn top_wallet_labels(ranked"),
        quality_filter_surface_present: DISCOVERY_SOURCE.contains("tradable_ratio")
            && DISCOVERY_SOURCE.contains("rug_ratio")
            && DISCOVERY_SOURCE.contains("token_quality"),
    }
}

struct ConclusionInputs<'a> {
    current_scoring_source: Option<&'a str>,
    publication_reason: Option<&'a str>,
    observed_swaps_min_ts_utc: Option<DateTime<Utc>>,
    raw_window_sample: &'a RawWindowSampleFacts,
    token_quality_sample: &'a TokenQualitySampleFacts,
    latest_freshness: &'a LatestFreshnessFacts,
    active_follow: &'a ActiveFollowFacts,
    config: &'a DiscoveryConfig,
}

fn choose_conclusion(inputs: ConclusionInputs<'_>) -> &'static str {
    let using_raw_window = inputs
        .current_scoring_source
        .is_some_and(|source| source.contains("raw_window"))
        || inputs
            .publication_reason
            .is_some_and(|reason| reason.contains("raw_window"))
        || inputs
            .latest_freshness
            .raw_truth_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("raw_window"));
    if !using_raw_window
        && (inputs.current_scoring_source.is_some() || inputs.publication_reason.is_some())
    {
        return CONCLUSION_RUNTIME_NOT_USING_RAW_WINDOW;
    }

    if inputs
        .raw_window_sample
        .has_rows_after_required_window_start
        == Some(false)
    {
        return CONCLUSION_NO_WALLET_ACTIVITY;
    }

    if inputs
        .raw_window_sample
        .has_rows_after_required_window_start
        .is_none()
        && inputs.observed_swaps_min_ts_utc.is_none()
    {
        return CONCLUSION_INSUFFICIENT_EVIDENCE;
    }

    let latest_zero_eligible = inputs.latest_freshness.eligible_wallets == Some(0)
        || inputs.latest_freshness.top_wallets_len == Some(0);
    let sample_has_wallet_activity =
        inputs.raw_window_sample.distinct_wallet_count.unwrap_or(0) > 0;
    if latest_zero_eligible && sample_has_wallet_activity {
        if quality_filter_evidence(&inputs) {
            return CONCLUSION_QUALITY_FILTERS;
        }
        return CONCLUSION_WALLETS_FAIL_THRESHOLDS;
    }

    if inputs.active_follow.active_follow_wallets == Some(0)
        || inputs.latest_freshness.active_follow_wallets == Some(0)
    {
        if inputs.latest_freshness.top_wallets_len == Some(0) {
            return CONCLUSION_FOLLOW_UNIVERSE_EMPTY;
        }
    }

    if inputs
        .raw_window_sample
        .has_rows_after_required_window_start
        == Some(true)
        && inputs
            .latest_freshness
            .raw_truth_sufficient
            .is_some_and(|sufficient| !sufficient)
    {
        return CONCLUSION_WINDOW_STILL_ACCUMULATING;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn quality_filter_evidence(inputs: &ConclusionInputs<'_>) -> bool {
    let quality_sensitive_config =
        inputs.config.min_tradable_ratio > 0.0 || inputs.config.max_rug_ratio < 1.0;
    if !quality_sensitive_config {
        return false;
    }
    inputs.token_quality_sample.missing_rows.unwrap_or(0) > 0
        || inputs.token_quality_sample.stale_rows.unwrap_or(0) > 0
}

fn parse_db_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        })
        .with_context(|| format!("invalid db timestamp value: {raw}"))
}

fn parse_day_or_ts(raw: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = parse_db_ts(raw) {
        return Ok(ts);
    }
    let day = chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("invalid db day/timestamp value: {raw}"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        day.and_hms_opt(0, 0, 0).expect("midnight should exist"),
        Utc,
    ))
}

fn render_output(report: &RawWindowZeroEligibleAuditReport) -> Result<String> {
    serde_json::to_string_pretty(report)
        .context("failed serializing raw window zero eligible audit report")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_config(
        temp: &TempDir,
        runtime_db_path: &Path,
        journal_db_path: &Path,
        min_score: f64,
        min_tradable_ratio: f64,
        max_rug_ratio: f64,
    ) -> Result<PathBuf> {
        let config_path = temp.path().join("audit.toml");
        std::fs::write(
            &config_path,
            format!(
                concat!(
                    "[sqlite]\n",
                    "path = \"{}\"\n\n",
                    "[recent_raw_journal]\n",
                    "path = \"{}\"\n",
                    "retention_safety_buffer_days = 2\n",
                    "writer_queue_capacity_batches = 64\n",
                    "replay_batch_size = 1024\n\n",
                    "[discovery]\n",
                    "scoring_window_days = 2\n",
                    "refresh_seconds = 600\n",
                    "metric_snapshot_interval_seconds = 3600\n",
                    "max_window_swaps_in_memory = 128\n",
                    "max_fetch_swaps_per_cycle = 128\n",
                    "max_fetch_pages_per_cycle = 8\n",
                    "fetch_time_budget_ms = 1000\n",
                    "observed_swaps_retention_days = 14\n",
                    "follow_top_n = 20\n",
                    "min_score = {}\n",
                    "min_trades = 1\n",
                    "min_active_days = 1\n",
                    "min_leader_notional_sol = 0.0\n",
                    "min_buy_count = 1\n",
                    "min_tradable_ratio = {}\n",
                    "max_rug_ratio = {}\n",
                    "thin_market_min_unique_traders = 1\n\n",
                    "[execution]\n",
                    "enabled = false\n",
                ),
                runtime_db_path.display(),
                journal_db_path.display(),
                min_score,
                min_tradable_ratio,
                max_rug_ratio,
            ),
        )
        .context("failed writing test config")?;
        Ok(config_path)
    }

    fn create_minimal_runtime_db(path: &Path) -> Result<Connection> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE observed_swaps (
                signature TEXT PRIMARY KEY,
                wallet_id TEXT NOT NULL,
                dex TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                qty_in REAL NOT NULL,
                qty_out REAL NOT NULL,
                slot INTEGER NOT NULL,
                ts TEXT NOT NULL
            );
            CREATE INDEX idx_observed_swaps_ts_slot_signature
                ON observed_swaps(ts, slot, signature);
            CREATE TABLE wallet_activity_days (
                wallet_id TEXT NOT NULL,
                activity_day TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                PRIMARY KEY(wallet_id, activity_day)
            );
            CREATE INDEX idx_wallet_activity_days_day_wallet
                ON wallet_activity_days(activity_day, wallet_id);
            CREATE TABLE discovery_runtime_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                cursor_ts TEXT NOT NULL,
                cursor_slot INTEGER NOT NULL,
                cursor_signature TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE discovery_strategy_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                publication_runtime_mode TEXT NOT NULL DEFAULT 'fail_closed',
                publication_reason TEXT NOT NULL DEFAULT '',
                publication_last_published_at TEXT,
                publication_last_published_window_start TEXT,
                publication_scoring_source TEXT,
                publication_wallet_ids_json TEXT,
                publication_policy_fingerprint TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE followlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_id TEXT NOT NULL,
                added_at TEXT NOT NULL,
                removed_at TEXT,
                reason TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE discovery_wallet_freshness_history (
                capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT NOT NULL,
                recent_cycles INTEGER NOT NULL,
                verdict TEXT NOT NULL,
                reason TEXT NOT NULL,
                publication_age_seconds INTEGER,
                raw_truth_sufficient INTEGER NOT NULL,
                raw_truth_reason TEXT NOT NULL,
                shadow_signal_verdict TEXT NOT NULL,
                shadow_signal_reason TEXT NOT NULL,
                published_wallet_ids_json TEXT NOT NULL,
                active_follow_wallet_ids_json TEXT NOT NULL,
                current_raw_top_wallet_ids_json TEXT NOT NULL,
                audit_json TEXT NOT NULL,
                shadow_signal_json TEXT NOT NULL
            );
            CREATE INDEX idx_discovery_wallet_freshness_history_captured_at
                ON discovery_wallet_freshness_history(captured_at DESC, capture_id DESC);
            CREATE TABLE token_quality_cache (
                mint TEXT PRIMARY KEY,
                holders INTEGER,
                liquidity_sol REAL,
                token_age_seconds INTEGER,
                fetched_at TEXT NOT NULL
            );",
        )?;
        Ok(conn)
    }

    fn insert_runtime_cursor(conn: &Connection, ts: DateTime<Utc>) -> Result<()> {
        conn.execute(
            "INSERT INTO discovery_runtime_state(
                id, cursor_ts, cursor_slot, cursor_signature, updated_at
             ) VALUES (1, ?1, 10, 'cursor-signature', ?1)",
            [ts.to_rfc3339()],
        )?;
        Ok(())
    }

    fn insert_publication_state(conn: &Connection, now: DateTime<Utc>, reason: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO discovery_strategy_state(
                id,
                publication_runtime_mode,
                publication_reason,
                publication_last_published_at,
                publication_last_published_window_start,
                publication_scoring_source,
                publication_wallet_ids_json,
                updated_at
             ) VALUES (1, 'fail_closed', ?1, ?2, ?3, 'raw_window', '[]', ?2)",
            params![
                reason,
                now.to_rfc3339(),
                (now - Duration::days(2)).to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn insert_swap(
        conn: &Connection,
        signature: &str,
        wallet_id: &str,
        token_out: &str,
        ts: DateTime<Utc>,
        slot: u64,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts
             ) VALUES (?1, ?2, 'raydium', 'So11111111111111111111111111111111111111112',
                       ?3, 1.0, 10.0, ?4, ?5)",
            params![
                signature,
                wallet_id,
                token_out,
                slot as i64,
                ts.to_rfc3339()
            ],
        )?;
        Ok(())
    }

    fn insert_freshness(
        conn: &Connection,
        captured_at: DateTime<Utc>,
        observed_swaps_loaded: u64,
        eligible_wallet_count: u64,
        top_wallet_ids: &[&str],
        raw_truth_sufficient: bool,
        raw_truth_reason: &str,
    ) -> Result<()> {
        let top_wallet_ids_json = serde_json::to_string(
            &top_wallet_ids
                .iter()
                .map(|wallet| wallet.to_string())
                .collect::<Vec<_>>(),
        )?;
        let audit_json = serde_json::json!({
            "published_scoring_source": "raw_window",
            "publication_runtime_mode": "fail_closed",
            "publication_recent_under_gate": false,
            "published_wallet_ids": [],
            "active_follow_wallet_ids": [],
            "current_raw_top_wallet_ids": top_wallet_ids,
            "raw_truth": {
                "sufficient": raw_truth_sufficient,
                "reason": raw_truth_reason,
                "observed_swaps_loaded": observed_swaps_loaded,
                "eligible_wallet_count": eligible_wallet_count,
                "top_wallet_count": top_wallet_ids.len(),
                "short_retention_configured": false,
                "covered_since": null,
                "covered_through_cursor": null,
                "covered_through_lag_seconds": null,
                "tail_fresh_within_runtime_lag": false,
                "runtime_freshness_lag_seconds": 0,
                "total_observed_swaps_rows": observed_swaps_loaded
            }
        });
        conn.execute(
            "INSERT INTO discovery_wallet_freshness_history(
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
             ) VALUES (?1, 1, 'stale', 'test_reason', 0, ?2, ?3, 'no_selected_wallets',
                       'test_shadow', '[]', '[]', ?4, ?5, '{}')",
            params![
                captured_at.to_rfc3339(),
                if raw_truth_sufficient { 1 } else { 0 },
                raw_truth_reason,
                top_wallet_ids_json,
                audit_json.to_string(),
            ],
        )?;
        Ok(())
    }

    fn fixture_config(
        temp: &TempDir,
        runtime_db_path: &Path,
        min_score: f64,
        min_tradable_ratio: f64,
        max_rug_ratio: f64,
        now_utc: DateTime<Utc>,
    ) -> Result<Config> {
        let journal_db_path = temp.path().join("missing-recent-raw.db");
        let config_path = write_config(
            temp,
            runtime_db_path,
            &journal_db_path,
            min_score,
            min_tradable_ratio,
            max_rug_ratio,
        )?;
        Ok(Config {
            db_path: runtime_db_path.to_path_buf(),
            config_path,
            now_utc,
        })
    }

    #[test]
    fn parse_args_accepts_required_flags_stage1() -> Result<()> {
        let default_now = parse_db_ts("2026-04-23T00:00:00Z")?;
        let parsed = parse_args_from(
            [
                "--db".to_string(),
                "runtime.db".to_string(),
                "--config".to_string(),
                "config.toml".to_string(),
                "--now".to_string(),
                "2026-04-23T16:30:00Z".to_string(),
                "--json".to_string(),
            ],
            default_now,
        )?
        .expect("config");
        assert_eq!(parsed.db_path, PathBuf::from("runtime.db"));
        assert_eq!(parsed.config_path, PathBuf::from("config.toml"));
        assert_eq!(parsed.now_utc, parse_db_ts("2026-04-23T16:30:00Z")?);
        Ok(())
    }

    #[test]
    fn no_raw_rows_in_required_window_reports_no_wallet_activity_stage1() -> Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("runtime.db");
        let conn = create_minimal_runtime_db(&db_path)?;
        let now = parse_db_ts("2026-04-23T16:30:00Z")?;
        insert_runtime_cursor(&conn, now - Duration::days(1))?;
        insert_publication_state(
            &conn,
            now,
            "raw_window_unusable_no_recent_published_universe",
        )?;
        insert_swap(
            &conn,
            "sig-before-window",
            "wallet-before",
            "token-before",
            now - Duration::days(6),
            1,
        )?;
        let config = fixture_config(&temp, &db_path, 0.0, 0.0, 1.0, now)?;
        let report = run(&config)?;
        assert_eq!(
            report.raw_window_has_rows_after_required_window_start,
            Some(false)
        );
        assert_eq!(
            report.raw_window_zero_eligible_conclusion,
            CONCLUSION_NO_WALLET_ACTIVITY
        );
        Ok(())
    }

    #[test]
    fn raw_rows_exist_but_zero_eligible_reports_threshold_investigation_stage1() -> Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("runtime.db");
        let conn = create_minimal_runtime_db(&db_path)?;
        let now = parse_db_ts("2026-04-23T16:30:00Z")?;
        insert_runtime_cursor(&conn, now - Duration::minutes(5))?;
        insert_publication_state(&conn, now, "raw_window")?;
        insert_swap(
            &conn,
            "sig-window-a",
            "wallet-a",
            "token-a",
            now - Duration::hours(1),
            2,
        )?;
        insert_freshness(&conn, now, 1, 0, &[], true, "raw_window_zero_eligible")?;
        let config = fixture_config(&temp, &db_path, 0.95, 0.0, 1.0, now)?;
        let report = run(&config)?;
        assert_eq!(
            report.raw_window_has_rows_after_required_window_start,
            Some(true)
        );
        assert_eq!(report.eligible_wallets, Some(0));
        assert_eq!(
            report.raw_window_zero_eligible_conclusion,
            CONCLUSION_WALLETS_FAIL_THRESHOLDS
        );
        Ok(())
    }

    #[test]
    fn missing_required_inputs_reports_insufficient_evidence_stage1() -> Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("runtime.db");
        let conn = create_minimal_runtime_db(&db_path)?;
        conn.execute_batch(
            "DROP TABLE observed_swaps;
             DROP TABLE wallet_activity_days;",
        )?;
        let now = parse_db_ts("2026-04-23T16:30:00Z")?;
        let config = fixture_config(&temp, &db_path, 0.0, 0.0, 1.0, now)?;
        let report = run(&config)?;
        assert_eq!(report.observed_swaps_max_ts_utc, None);
        assert_eq!(
            report.raw_window_zero_eligible_conclusion,
            CONCLUSION_INSUFFICIENT_EVIDENCE
        );
        Ok(())
    }

    #[test]
    fn conservative_conclusion_prefers_quality_filter_when_quality_evidence_is_present_stage1(
    ) -> Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("runtime.db");
        let conn = create_minimal_runtime_db(&db_path)?;
        let now = parse_db_ts("2026-04-23T16:30:00Z")?;
        insert_runtime_cursor(&conn, now - Duration::minutes(5))?;
        insert_publication_state(&conn, now, "raw_window")?;
        insert_swap(
            &conn,
            "sig-quality-a",
            "wallet-quality",
            "token-quality-missing",
            now - Duration::hours(1),
            3,
        )?;
        insert_freshness(&conn, now, 1, 0, &[], true, "raw_window_zero_eligible")?;
        let config = fixture_config(&temp, &db_path, 0.0, 0.5, 1.0, now)?;
        let report = run(&config)?;
        assert_eq!(report.token_quality_cache_sample_missing_rows, Some(1));
        assert_eq!(
            report.raw_window_zero_eligible_conclusion,
            CONCLUSION_QUALITY_FILTERS
        );
        Ok(())
    }

    #[test]
    fn json_output_contains_required_fields_stage1() -> Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("runtime.db");
        let conn = create_minimal_runtime_db(&db_path)?;
        let now = parse_db_ts("2026-04-23T16:30:00Z")?;
        insert_runtime_cursor(&conn, now - Duration::minutes(5))?;
        insert_publication_state(&conn, now, "raw_window")?;
        insert_swap(
            &conn,
            "sig-json-a",
            "wallet-json",
            "token-json",
            now - Duration::hours(1),
            4,
        )?;
        insert_freshness(&conn, now, 1, 0, &[], true, "raw_window_zero_eligible")?;
        let config = fixture_config(&temp, &db_path, 0.0, 0.0, 1.0, now)?;
        let report = run(&config)?;
        let value = serde_json::to_value(&report)?;
        for key in [
            "config_path",
            "db_path",
            "now_utc",
            "configured_sqlite_path",
            "runtime_db_open_mode",
            "required_window_start_utc",
            "runtime_cursor_ts_utc",
            "publication_runtime_mode",
            "publication_reason",
            "current_scoring_source",
            "observed_swaps_max_ts_utc",
            "observed_swaps_bounded_index_available",
            "recent_raw_max_ts_utc",
            "raw_window_has_rows_after_required_window_start",
            "raw_window_bounded_sample_count",
            "raw_window_sample_distinct_wallet_count",
            "active_follow_wallets",
            "eligible_wallets",
            "top_wallets_len",
            "wallet_eligibility_surface",
            "top_wallet_selection_surface",
            "quality_filter_surface",
            "raw_window_zero_eligible_conclusion",
        ] {
            assert!(value.get(key).is_some(), "missing JSON field {key}");
        }
        Ok(())
    }

    #[test]
    fn repeated_runs_with_fixed_inputs_are_identical_stage1() -> Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("runtime.db");
        let conn = create_minimal_runtime_db(&db_path)?;
        let now = parse_db_ts("2026-04-23T16:30:00Z")?;
        insert_runtime_cursor(&conn, now - Duration::minutes(5))?;
        insert_publication_state(&conn, now, "raw_window")?;
        insert_swap(
            &conn,
            "sig-repeat-a",
            "wallet-repeat",
            "token-repeat",
            now - Duration::hours(1),
            5,
        )?;
        insert_freshness(&conn, now, 1, 0, &[], true, "raw_window_zero_eligible")?;
        let config = fixture_config(&temp, &db_path, 0.0, 0.0, 1.0, now)?;
        let first = render_output(&run(&config)?)?;
        let second = render_output(&run(&config)?)?;
        assert_eq!(first, second);
        Ok(())
    }
}
