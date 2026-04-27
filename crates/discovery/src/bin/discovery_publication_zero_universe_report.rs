use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_storage::{DiscoveryPublicationStateRow, SqliteStore};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

const USAGE: &str = "usage: discovery_publication_zero_universe_report --config <path> --db-path <path> --now <rfc3339> --json [--sample-limit <n>]";
const DEFAULT_SAMPLE_LIMIT: usize = 10;
const MAX_SAMPLE_LIMIT: usize = 50;
const REJECT_COUNTS_UNAVAILABLE_REASON: &str =
    "publication_zero_universe_full_selector_scan_required";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .context("failed serializing publication zero-universe report")?
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: PathBuf,
    now: DateTime<Utc>,
    sample_limit: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct PublicationZeroUniverseReport {
    event: &'static str,
    config_path: String,
    db_path: String,
    now_utc: DateTime<Utc>,
    runtime_db_open_mode: &'static str,
    elapsed_ms: u64,
    selector_full_scan_used: bool,
    bounded_operator: bool,
    row_limits: RowLimitsReport,
    publication: PublicationStateReport,
    raw_window: RawWindowSelectorInputsReport,
    active_follow_wallets: Option<u64>,
    active_follow_wallets_capped: bool,
    eligible_wallets: Option<u64>,
    top_wallets: Vec<String>,
    reject_counts_proven: bool,
    reject_counts_unavailable_reason: Option<String>,
    reject_counts: RejectCountsReport,
    rejected_wallet_samples: Vec<RejectedWalletSample>,
    selector_zero_universe_claimed: bool,
    zero_universe_reason: String,
    production_green: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RowLimitsReport {
    sample_limit_requested: usize,
    sample_limit_applied: usize,
    observed_swaps_query_limit: usize,
    active_follow_query_limit: usize,
    rejected_wallet_sample_limit_applied: usize,
    selector_window_scan: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct PublicationStateReport {
    publication_runtime_mode: Option<String>,
    reason: Option<String>,
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    published_scoring_source: Option<String>,
    published_wallet_count: u64,
    freshness: PublicationFreshnessReport,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct PublicationFreshnessReport {
    age_seconds: Option<i64>,
    fresh_under_refresh_gate: Option<bool>,
    refresh_seconds: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct RawWindowSelectorInputsReport {
    window_start: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    persisted_raw_truth_captured_at: Option<DateTime<Utc>>,
    persisted_raw_truth_sufficient: Option<bool>,
    persisted_raw_truth_reason: Option<String>,
    observed_swaps_count: Option<u64>,
    observed_swaps_sample_count: u64,
    observed_swaps_sample_capped: bool,
    observed_swaps_window_min_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_window_max_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_sample_min_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_sample_max_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_sample_buy_count: u64,
    observed_swaps_sample_sell_count: u64,
    wallets_seen: Option<u64>,
    observed_swaps_sample_distinct_wallet_count: u64,
    observed_swaps_distinct_buy_mint_count: Option<u64>,
    observed_swaps_sample_distinct_buy_mint_count: u64,
    raw_window_healthy: bool,
    raw_window_health_reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RejectCountsReport {
    min_trades: Option<u64>,
    min_active_days: Option<u64>,
    min_buy_count: Option<u64>,
    min_tradable_ratio: Option<u64>,
    min_score: Option<u64>,
    require_open_positions_for_publication: Option<u64>,
    token_quality_missing: Option<u64>,
    token_quality_stale: Option<u64>,
    token_quality_failed: Option<u64>,
    other_rejects: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct RejectedWalletSample {
    wallet_id: String,
    reject_reasons_proven: bool,
    reject_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct LatestFreshnessFacts {
    captured_at: Option<DateTime<Utc>>,
    raw_truth_sufficient: Option<bool>,
    raw_truth_reason: Option<String>,
    raw_truth_observed_swaps_loaded: Option<u64>,
    raw_truth_eligible_wallet_count: Option<u64>,
    raw_truth_top_wallet_ids: Vec<String>,
    raw_truth_wallets_seen: Option<u64>,
    raw_truth_distinct_buy_mint_count: Option<u64>,
    active_follow_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ObservedSwapsSample {
    sample_count: u64,
    sample_capped: bool,
    sample_min_ts: Option<DateTime<Utc>>,
    sample_max_ts: Option<DateTime<Utc>>,
    window_min_ts: Option<DateTime<Utc>>,
    window_max_ts: Option<DateTime<Utc>>,
    buy_count: u64,
    sell_count: u64,
    distinct_wallet_count: u64,
    distinct_buy_mint_count: u64,
    sampled_wallets: Vec<String>,
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
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;
    let mut sample_limit = DEFAULT_SAMPLE_LIMIT;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--sample-limit" => {
                sample_limit = parse_usize_arg("--sample-limit", args.next())?;
                if sample_limit > MAX_SAMPLE_LIMIT {
                    bail!("--sample-limit must be <= {MAX_SAMPLE_LIMIT}");
                }
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    if !json {
        bail!("missing required --json\n{USAGE}");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config\n{USAGE}"))?,
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db-path\n{USAGE}"))?,
        now: now.ok_or_else(|| anyhow!("missing required --now\n{USAGE}"))?,
        sample_limit,
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

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    parse_string_arg(flag, value)?
        .parse::<usize>()
        .with_context(|| format!("invalid integer for {flag}"))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    parse_ts(&raw).with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid rfc3339 timestamp: {raw}"))
}

fn run(config: &Config) -> Result<PublicationZeroUniverseReport> {
    let started = Instant::now();
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only(&config.db_path).with_context(|| {
        format!(
            "failed opening runtime db read-only {}",
            config.db_path.display()
        )
    })?;
    let conn = open_read_only_connection(&config.db_path)?;
    let window_start =
        config.now - Duration::days(loaded_config.discovery.scoring_window_days.max(1) as i64);
    let publication = publication_report(
        store.discovery_publication_state_read_only()?,
        config.now,
        loaded_config.discovery.refresh_seconds,
    );
    let latest_freshness = load_latest_freshness_facts(&conn)?;
    let observed_sample =
        load_observed_swaps_sample(&conn, window_start, config.now, config.sample_limit)?;
    let raw_window =
        raw_window_selector_inputs_report(window_start, &latest_freshness, &observed_sample);
    let active_follow_wallets = latest_freshness
        .captured_at
        .map(|_| latest_freshness.active_follow_wallet_ids.len() as u64);
    let eligible_wallets = latest_freshness.raw_truth_eligible_wallet_count;
    let top_wallets = latest_freshness.raw_truth_top_wallet_ids.clone();
    let selector_zero_universe_claimed = raw_window.raw_window_healthy
        && eligible_wallets == Some(0)
        && top_wallets.is_empty()
        && active_follow_wallets == Some(0);
    let zero_universe_reason = zero_universe_reason(
        &raw_window,
        eligible_wallets,
        &top_wallets,
        active_follow_wallets,
    );
    let rejected_wallet_samples =
        bounded_rejected_wallet_samples(&observed_sample.sampled_wallets, config.sample_limit);

    Ok(PublicationZeroUniverseReport {
        event: "discovery_publication_zero_universe_report",
        config_path: config.config_path.display().to_string(),
        db_path: config.db_path.display().to_string(),
        now_utc: config.now,
        runtime_db_open_mode: "wal_aware_read_only",
        elapsed_ms: elapsed_ms(started),
        selector_full_scan_used: false,
        bounded_operator: true,
        row_limits: RowLimitsReport {
            sample_limit_requested: config.sample_limit,
            sample_limit_applied: config.sample_limit,
            observed_swaps_query_limit: config.sample_limit.saturating_add(1),
            active_follow_query_limit: 0,
            rejected_wallet_sample_limit_applied: config.sample_limit,
            selector_window_scan: "bounded_indexed_observed_swaps_sql_and_persisted_truth_only",
        },
        publication,
        raw_window,
        active_follow_wallets,
        active_follow_wallets_capped: false,
        eligible_wallets,
        top_wallets,
        reject_counts_proven: false,
        reject_counts_unavailable_reason: Some(REJECT_COUNTS_UNAVAILABLE_REASON.to_string()),
        reject_counts: RejectCountsReport::unproven(),
        rejected_wallet_samples,
        selector_zero_universe_claimed,
        zero_universe_reason,
        production_green: false,
    })
}

fn open_read_only_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed opening sqlite db read-only: {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .context("failed setting sqlite busy_timeout")?;
    conn.pragma_update(None, "query_only", "ON")
        .context("failed enabling sqlite query_only")?;
    Ok(conn)
}

fn publication_report(
    state: Option<DiscoveryPublicationStateRow>,
    now: DateTime<Utc>,
    refresh_seconds: u64,
) -> PublicationStateReport {
    let age_seconds = state
        .as_ref()
        .and_then(|state| state.last_published_at)
        .map(|published_at| now.signed_duration_since(published_at).num_seconds().max(0));
    PublicationStateReport {
        publication_runtime_mode: state
            .as_ref()
            .map(|state| state.runtime_mode.as_str().to_string()),
        reason: state.as_ref().map(|state| state.reason.clone()),
        last_published_at: state.as_ref().and_then(|state| state.last_published_at),
        last_published_window_start: state
            .as_ref()
            .and_then(|state| state.last_published_window_start),
        updated_at: state.as_ref().map(|state| state.updated_at),
        published_scoring_source: state
            .as_ref()
            .and_then(|state| state.published_scoring_source.clone()),
        published_wallet_count: state
            .as_ref()
            .and_then(|state| state.published_wallet_ids.as_ref().map(Vec::len))
            .unwrap_or(0) as u64,
        freshness: PublicationFreshnessReport {
            age_seconds,
            fresh_under_refresh_gate: age_seconds.map(|age| age <= refresh_seconds as i64),
            refresh_seconds,
        },
    }
}

fn raw_window_selector_inputs_report(
    window_start: DateTime<Utc>,
    latest_freshness: &LatestFreshnessFacts,
    observed_sample: &ObservedSwapsSample,
) -> RawWindowSelectorInputsReport {
    let (raw_window_healthy, raw_window_health_reason) =
        classify_raw_window_from_persisted_truth(latest_freshness);
    RawWindowSelectorInputsReport {
        window_start,
        metrics_window_start: window_start,
        persisted_raw_truth_captured_at: latest_freshness.captured_at,
        persisted_raw_truth_sufficient: latest_freshness.raw_truth_sufficient,
        persisted_raw_truth_reason: latest_freshness.raw_truth_reason.clone(),
        observed_swaps_count: latest_freshness.raw_truth_observed_swaps_loaded,
        observed_swaps_sample_count: observed_sample.sample_count,
        observed_swaps_sample_capped: observed_sample.sample_capped,
        observed_swaps_window_min_ts_utc: observed_sample.window_min_ts,
        observed_swaps_window_max_ts_utc: observed_sample.window_max_ts,
        observed_swaps_sample_min_ts_utc: observed_sample.sample_min_ts,
        observed_swaps_sample_max_ts_utc: observed_sample.sample_max_ts,
        observed_swaps_sample_buy_count: observed_sample.buy_count,
        observed_swaps_sample_sell_count: observed_sample.sell_count,
        wallets_seen: latest_freshness.raw_truth_wallets_seen,
        observed_swaps_sample_distinct_wallet_count: observed_sample.distinct_wallet_count,
        observed_swaps_distinct_buy_mint_count: latest_freshness.raw_truth_distinct_buy_mint_count,
        observed_swaps_sample_distinct_buy_mint_count: observed_sample.distinct_buy_mint_count,
        raw_window_healthy,
        raw_window_health_reason,
    }
}

fn classify_raw_window_from_persisted_truth(
    latest_freshness: &LatestFreshnessFacts,
) -> (bool, String) {
    match latest_freshness.raw_truth_sufficient {
        Some(true) => (
            true,
            "publication_zero_universe_raw_window_persisted_truth_sufficient".to_string(),
        ),
        Some(false) => (
            false,
            latest_freshness
                .raw_truth_reason
                .clone()
                .unwrap_or_else(|| {
                    "publication_zero_universe_raw_window_persisted_truth_insufficient".to_string()
                }),
        ),
        None => (
            false,
            "publication_zero_universe_raw_window_health_unproven_no_persisted_truth".to_string(),
        ),
    }
}

fn zero_universe_reason(
    raw_window: &RawWindowSelectorInputsReport,
    eligible_wallets: Option<u64>,
    top_wallets: &[String],
    active_follow_wallets: Option<u64>,
) -> String {
    if !raw_window.raw_window_healthy {
        return raw_window.raw_window_health_reason.clone();
    }
    if eligible_wallets.is_none() {
        return "publication_zero_universe_eligible_wallet_count_unproven".to_string();
    }
    if active_follow_wallets.is_none() {
        return "publication_zero_universe_active_follow_wallet_count_unproven".to_string();
    }
    if eligible_wallets == Some(0) && active_follow_wallets == Some(0) && top_wallets.is_empty() {
        return "publication_zero_universe_persisted_truth_zero_publishable_universe".to_string();
    }
    "publication_zero_universe_not_reproduced_from_persisted_truth".to_string()
}

fn load_latest_freshness_facts(conn: &Connection) -> Result<LatestFreshnessFacts> {
    if !sqlite_table_exists(conn, "discovery_wallet_freshness_history")? {
        return Ok(LatestFreshnessFacts::default());
    }
    let raw: Option<(String, i64, String, String, String, String)> = conn
        .query_row(
            "SELECT
                captured_at,
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
                ))
            },
        )
        .optional()
        .context("failed loading latest discovery wallet freshness history row")?;
    let Some((
        captured_at_raw,
        raw_truth_sufficient_raw,
        raw_truth_reason,
        active_follow_wallet_ids_json,
        current_raw_top_wallet_ids_json,
        audit_json,
    )) = raw
    else {
        return Ok(LatestFreshnessFacts::default());
    };
    let audit: Value = serde_json::from_str(&audit_json)
        .context("failed deserializing discovery wallet freshness audit_json")?;
    Ok(LatestFreshnessFacts {
        captured_at: Some(parse_ts(&captured_at_raw)?),
        raw_truth_sufficient: Some(raw_truth_sufficient_raw != 0),
        raw_truth_reason: Some(raw_truth_reason),
        raw_truth_observed_swaps_loaded: audit
            .pointer("/raw_truth/observed_swaps_loaded")
            .and_then(Value::as_u64),
        raw_truth_eligible_wallet_count: audit
            .pointer("/raw_truth/eligible_wallet_count")
            .and_then(Value::as_u64),
        raw_truth_top_wallet_ids: parse_string_array_json(&current_raw_top_wallet_ids_json)
            .or_else(|| {
                audit
                    .pointer("/current_raw_top_wallet_ids")
                    .and_then(Value::as_array)
                    .map(|items| string_array_from_values(items))
            })
            .unwrap_or_default(),
        raw_truth_wallets_seen: audit
            .pointer("/raw_truth/wallets_seen")
            .and_then(Value::as_u64),
        raw_truth_distinct_buy_mint_count: audit
            .pointer("/raw_truth/observed_swaps_distinct_buy_mint_count")
            .and_then(Value::as_u64),
        active_follow_wallet_ids: parse_string_array_json(&active_follow_wallet_ids_json)
            .or_else(|| {
                audit
                    .pointer("/active_follow_wallet_ids")
                    .and_then(Value::as_array)
                    .map(|items| string_array_from_values(items))
            })
            .unwrap_or_default(),
    })
}

fn load_observed_swaps_sample(
    conn: &Connection,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    sample_limit: usize,
) -> Result<ObservedSwapsSample> {
    if sample_limit == 0 || !sqlite_table_exists(conn, "observed_swaps")? {
        return Ok(ObservedSwapsSample::default());
    }
    let window_min_ts = query_observed_swaps_boundary_ts(conn, window_start, now, false)?;
    let window_max_ts = query_observed_swaps_boundary_ts(conn, window_start, now, true)?;
    let query_limit = sample_limit.saturating_add(1).min(i64::MAX as usize) as i64;
    let mut stmt = conn
        .prepare(
            "SELECT ts, wallet_id, token_in, token_out
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             WHERE ts >= ?1 AND ts <= ?2
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?3",
        )
        .context("failed preparing bounded observed_swaps sample query")?;
    let mut rows = stmt
        .query(params![
            window_start.to_rfc3339(),
            now.to_rfc3339(),
            query_limit
        ])
        .context("failed querying bounded observed_swaps sample")?;
    let mut fetched = 0usize;
    let mut sample_count = 0u64;
    let mut sample_min_ts = None;
    let mut sample_max_ts = None;
    let mut buy_count = 0u64;
    let mut sell_count = 0u64;
    let mut wallets = BTreeSet::new();
    let mut buy_mints = BTreeSet::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating bounded observed_swaps sample")?
    {
        fetched = fetched.saturating_add(1);
        if fetched > sample_limit {
            break;
        }
        let ts_raw: String = row.get(0).context("failed reading observed_swaps.ts")?;
        let ts = parse_ts(&ts_raw)?;
        sample_min_ts = Some(sample_min_ts.map_or(ts, |current: DateTime<Utc>| current.min(ts)));
        sample_max_ts = Some(sample_max_ts.map_or(ts, |current: DateTime<Utc>| current.max(ts)));
        let wallet_id: String = row
            .get(1)
            .context("failed reading observed_swaps.wallet_id")?;
        let token_in: String = row
            .get(2)
            .context("failed reading observed_swaps.token_in")?;
        let token_out: String = row
            .get(3)
            .context("failed reading observed_swaps.token_out")?;
        if is_sol_buy(&token_in, &token_out) {
            buy_count = buy_count.saturating_add(1);
            buy_mints.insert(token_out);
        } else if is_sol_sell(&token_in, &token_out) {
            sell_count = sell_count.saturating_add(1);
        }
        wallets.insert(wallet_id);
        sample_count = sample_count.saturating_add(1);
    }
    Ok(ObservedSwapsSample {
        sample_count,
        sample_capped: fetched > sample_limit,
        sample_min_ts,
        sample_max_ts,
        window_min_ts,
        window_max_ts,
        buy_count,
        sell_count,
        distinct_wallet_count: wallets.len() as u64,
        distinct_buy_mint_count: buy_mints.len() as u64,
        sampled_wallets: wallets.into_iter().collect(),
    })
}

fn query_observed_swaps_boundary_ts(
    conn: &Connection,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    descending: bool,
) -> Result<Option<DateTime<Utc>>> {
    if !sqlite_table_exists(conn, "observed_swaps")? {
        return Ok(None);
    }
    let direction = if descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts
         FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
         WHERE ts >= ?1 AND ts <= ?2
         ORDER BY ts {direction}, slot {direction}, signature {direction}
         LIMIT 1"
    );
    let raw: Option<String> = conn
        .query_row(&sql, [window_start.to_rfc3339(), now.to_rfc3339()], |row| {
            row.get(0)
        })
        .optional()
        .context("failed loading observed_swaps indexed boundary timestamp")?;
    raw.as_deref().map(parse_ts).transpose()
}

fn bounded_rejected_wallet_samples(
    sampled_wallets: &[String],
    sample_limit: usize,
) -> Vec<RejectedWalletSample> {
    sampled_wallets
        .iter()
        .take(sample_limit)
        .map(|wallet_id| RejectedWalletSample {
            wallet_id: wallet_id.clone(),
            reject_reasons_proven: false,
            reject_reasons: vec![REJECT_COUNTS_UNAVAILABLE_REASON.to_string()],
        })
        .collect()
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

fn parse_string_array_json(raw: &str) -> Option<Vec<String>> {
    serde_json::from_str::<Vec<String>>(raw).ok()
}

fn string_array_from_values(items: &[Value]) -> Vec<String> {
    items
        .iter()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn is_sol_buy(token_in: &str, token_out: &str) -> bool {
    token_in == sol_mint() && token_out != sol_mint()
}

fn is_sol_sell(token_in: &str, token_out: &str) -> bool {
    token_out == sol_mint() && token_in != sol_mint()
}

fn sol_mint() -> &'static str {
    "So11111111111111111111111111111111111111112"
}

impl RejectCountsReport {
    fn unproven() -> Self {
        Self {
            min_trades: None,
            min_active_days: None,
            min_buy_count: None,
            min_tradable_ratio: None,
            min_score: None,
            require_open_positions_for_publication: None,
            token_quality_missing: None,
            token_quality_stale: None,
            token_quality_failed: None,
            other_rejects: None,
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use copybot_core_types::SwapEvent;
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode, DiscoveryWalletFreshnessCaptureWrite,
    };
    use std::fs;
    use tempfile::TempDir;

    const NOW: &str = "2026-04-24T12:00:00Z";

    #[test]
    fn persisted_zero_universe_is_reported_without_full_selector_scan() -> Result<()> {
        let fixture = TestFixture::new(TestCase::PersistedZeroUniverse)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.bounded_operator);
        assert!(!report.selector_full_scan_used);
        assert!(report.raw_window.raw_window_healthy);
        assert!(report.selector_zero_universe_claimed);
        assert_eq!(report.eligible_wallets, Some(0));
        assert!(report.top_wallets.is_empty());
        assert_eq!(report.active_follow_wallets, Some(0));
        assert_eq!(
            report.zero_universe_reason,
            "publication_zero_universe_persisted_truth_zero_publishable_universe"
        );
        assert!(!report.reject_counts_proven);
        assert_eq!(
            report.reject_counts_unavailable_reason.as_deref(),
            Some(REJECT_COUNTS_UNAVAILABLE_REASON)
        );
        Ok(())
    }

    #[test]
    fn raw_window_health_unproven_does_not_claim_zero_universe() -> Result<()> {
        let fixture = TestFixture::new(TestCase::NoPersistedTruth)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(!report.raw_window.raw_window_healthy);
        assert!(!report.selector_zero_universe_claimed);
        assert_eq!(
            report.zero_universe_reason,
            "publication_zero_universe_raw_window_health_unproven_no_persisted_truth"
        );
        assert_eq!(report.raw_window.observed_swaps_sample_count, 1);
        Ok(())
    }

    #[test]
    fn insufficient_persisted_raw_truth_does_not_claim_selector_blocker() -> Result<()> {
        let fixture = TestFixture::new(TestCase::InsufficientPersistedTruth)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(!report.raw_window.raw_window_healthy);
        assert!(!report.selector_zero_universe_claimed);
        assert_eq!(report.zero_universe_reason, "raw_truth_incomplete_fixture");
        Ok(())
    }

    #[test]
    fn sample_limit_bounds_actual_sql_work() -> Result<()> {
        let fixture = TestFixture::new(TestCase::ManyRows)?;
        let report = run(&fixture.config(1))?;
        assert_eq!(report.row_limits.observed_swaps_query_limit, 2);
        assert_eq!(report.row_limits.active_follow_query_limit, 0);
        assert_eq!(report.raw_window.observed_swaps_sample_count, 1);
        assert!(report.raw_window.observed_swaps_sample_capped);
        assert_eq!(report.rejected_wallet_samples.len(), 1);
        assert!(report.bounded_operator);
        assert!(!report.selector_full_scan_used);
        Ok(())
    }

    #[test]
    fn reject_counts_are_not_faked_without_full_selector_scan() -> Result<()> {
        let fixture = TestFixture::new(TestCase::PersistedZeroUniverse)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(!report.reject_counts_proven);
        assert_eq!(
            report.reject_counts_unavailable_reason.as_deref(),
            Some(REJECT_COUNTS_UNAVAILABLE_REASON)
        );
        assert_eq!(report.reject_counts.min_trades, None);
        assert_eq!(report.reject_counts.min_buy_count, None);
        Ok(())
    }

    #[test]
    fn production_green_is_always_false() -> Result<()> {
        let fixture = TestFixture::new(TestCase::PersistedZeroUniverse)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(!report.production_green);
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum TestCase {
        PersistedZeroUniverse,
        NoPersistedTruth,
        InsufficientPersistedTruth,
        ManyRows,
    }

    struct TestFixture {
        _tempdir: TempDir,
        db_path: PathBuf,
        config_path: PathBuf,
        now: DateTime<Utc>,
    }

    impl TestFixture {
        fn new(test_case: TestCase) -> Result<Self> {
            let tempdir = TempDir::new().context("failed creating tempdir")?;
            let db_path = tempdir.path().join("publication-zero-universe.sqlite");
            let config_path = tempdir.path().join("publication-zero-universe.toml");
            let now = parse_ts(NOW)?;
            write_test_config(&config_path, &db_path)?;
            create_sqlite_fixture(&db_path, test_case, now)?;
            Ok(Self {
                _tempdir: tempdir,
                db_path,
                config_path,
                now,
            })
        }

        fn config(&self, sample_limit: usize) -> Config {
            Config {
                config_path: self.config_path.clone(),
                db_path: self.db_path.clone(),
                now: self.now,
                sample_limit,
            }
        }
    }

    fn write_test_config(path: &Path, db_path: &Path) -> Result<()> {
        let raw = format!(
            concat!(
                "[sqlite]\n",
                "path = \"{}\"\n\n",
                "[discovery]\n",
                "scoring_window_days = 5\n",
                "refresh_seconds = 600\n",
                "metric_snapshot_interval_seconds = 1800\n",
                "max_window_swaps_in_memory = 10000\n",
                "max_fetch_swaps_per_cycle = 10000\n",
                "max_fetch_pages_per_cycle = 5\n",
                "fetch_time_budget_ms = 1000\n",
                "observed_swaps_retention_days = 14\n"
            ),
            db_path.display(),
        );
        fs::write(path, raw).with_context(|| format!("failed writing {}", path.display()))?;
        Ok(())
    }

    fn create_sqlite_fixture(path: &Path, test_case: TestCase, now: DateTime<Utc>) -> Result<()> {
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(path)
            .with_context(|| format!("failed creating sqlite fixture {}", path.display()))?;
        store.run_migrations(&migration_dir).with_context(|| {
            format!("failed running migrations from {}", migration_dir.display())
        })?;
        seed_publication_state(&store, now)?;
        match test_case {
            TestCase::PersistedZeroUniverse => {
                seed_swaps(&store, now, 1)?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
            TestCase::NoPersistedTruth => {
                seed_swaps(&store, now, 1)?;
            }
            TestCase::InsufficientPersistedTruth => {
                seed_swaps(&store, now, 1)?;
                seed_freshness_history(&store, now, false, "raw_truth_incomplete_fixture")?;
            }
            TestCase::ManyRows => {
                seed_swaps(&store, now, 3)?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
        }
        Ok(())
    }

    fn seed_publication_state(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window".to_string(),
            last_published_at: Some(now - Duration::minutes(20)),
            last_published_window_start: Some(now - Duration::days(5)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;
        Ok(())
    }

    fn seed_swaps(store: &SqliteStore, now: DateTime<Utc>, count: usize) -> Result<()> {
        let swaps = (0..count)
            .map(|index| {
                buy_swap(
                    &format!("sig-{index}"),
                    &format!("wallet-{index}"),
                    &format!("Token{index}"),
                    now - Duration::minutes(10) + Duration::seconds(index as i64),
                    index as u64 + 1,
                )
            })
            .collect::<Vec<_>>();
        store.insert_observed_swaps_batch_with_activity_days(&swaps)?;
        Ok(())
    }

    fn seed_freshness_history(
        store: &SqliteStore,
        now: DateTime<Utc>,
        raw_truth_sufficient: bool,
        raw_truth_reason: &str,
    ) -> Result<()> {
        let audit_json = serde_json::json!({
            "raw_truth": {
                "observed_swaps_loaded": 123u64,
                "eligible_wallet_count": 0u64,
                "wallets_seen": 3u64,
                "observed_swaps_distinct_buy_mint_count": 3u64
            },
            "active_follow_wallet_ids": [],
            "current_raw_top_wallet_ids": []
        });
        store.append_discovery_wallet_freshness_capture(&DiscoveryWalletFreshnessCaptureWrite {
            captured_at: now,
            recent_cycles: 1,
            verdict: "insufficient_evidence".to_string(),
            reason: "fixture".to_string(),
            publication_age_seconds: Some(0),
            raw_truth_sufficient,
            raw_truth_reason: raw_truth_reason.to_string(),
            shadow_signal_verdict: "insufficient_evidence".to_string(),
            shadow_signal_reason: "fixture".to_string(),
            published_wallet_ids: Vec::new(),
            active_follow_wallet_ids: Vec::new(),
            current_raw_top_wallet_ids: Vec::new(),
            audit_json: serde_json::to_string(&audit_json)?,
            shadow_signal_json: "{}".to_string(),
        })?;
        Ok(())
    }

    fn buy_swap(
        signature: &str,
        wallet: &str,
        token_out: &str,
        ts_utc: DateTime<Utc>,
        slot: u64,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: sol_mint().to_string(),
            token_out: token_out.to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }
}
