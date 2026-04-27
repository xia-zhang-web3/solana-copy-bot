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
const PERSISTED_METRICS_BUCKET_MISMATCH_REASON: &str =
    "publication_zero_universe_persisted_metrics_bucket_mismatch";

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
    persisted_metrics: PersistedMetricsGateReport,
    reject_counts_proven: bool,
    reject_counts_unavailable_reason: Option<String>,
    reject_counts: RejectCountsReport,
    rejected_wallet_samples: Vec<RejectedWalletSample>,
    near_miss_metrics_proven: bool,
    near_miss_unavailable_reason: Option<String>,
    max_observed_metrics: MaxObservedMetricsReport,
    single_gate_relief_counts: SingleGateReliefCountsReport,
    bounded_near_miss_wallet_samples: Vec<NearMissWalletSample>,
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
struct MaxObservedMetricsReport {
    max_trades: Option<u64>,
    max_active_days: Option<u64>,
    max_buy_count: Option<u64>,
    max_tradable_ratio: Option<f64>,
    max_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct SingleGateReliefCountsReport {
    would_pass_if_min_trades_ignored: Option<u64>,
    would_pass_if_min_active_days_ignored: Option<u64>,
    would_pass_if_min_buy_count_ignored: Option<u64>,
    would_pass_if_min_tradable_ratio_ignored: Option<u64>,
    would_pass_if_min_score_ignored: Option<u64>,
    would_pass_if_open_position_gate_ignored: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct PersistedMetricsGateReport {
    evidence_source: &'static str,
    expected_metrics_window_start: DateTime<Utc>,
    latest_metrics_window_start: Option<DateTime<Utc>>,
    latest_metrics_window_start_raw: Option<String>,
    latest_metrics_window_matches_expected: bool,
    metrics_rows: u64,
    threshold_counts_proven: bool,
    unavailable_reason: Option<String>,
    counts_model: &'static str,
    min_trades_threshold: u32,
    min_active_days_threshold: u32,
    min_buy_count_threshold: u32,
    min_tradable_ratio_threshold: f64,
    min_score_threshold: f64,
    require_open_positions_for_publication: bool,
    active_day_evidence_source: &'static str,
    open_position_evidence_source: &'static str,
    token_quality_evidence_source: &'static str,
    token_quality_distinct_buy_mints_checked: u64,
    token_quality_missing_mints: u64,
    token_quality_stale_mints: u64,
    token_quality_deferred_mints: u64,
    token_quality_missing_wallets: u64,
    token_quality_stale_wallets: u64,
    token_quality_deferred_wallets: u64,
    post_threshold_candidate_wallets: u64,
    post_threshold_candidate_wallets_with_open_positions: u64,
    unexplained_candidate_wallets_after_persisted_gates: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct RejectedWalletSample {
    wallet_id: String,
    reject_reasons_proven: bool,
    reject_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct NearMissWalletSample {
    wallet_id: String,
    trades: u32,
    active_days: u32,
    buy_count: u32,
    tradable_ratio: f64,
    score: f64,
    has_open_position: bool,
    failing_gates: Vec<String>,
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

#[derive(Debug, Clone)]
struct ThresholdConfig {
    min_trades: u32,
    min_active_days: u32,
    min_buy_count: u32,
    min_tradable_ratio: f64,
    min_score: f64,
    require_open_positions_for_publication: bool,
}

#[derive(Debug, Clone)]
struct PersistedMetricWallet {
    wallet_id: String,
    trades: u32,
    active_days: u32,
    buy_total: u32,
    tradable_ratio: f64,
    score: f64,
    has_open_position: bool,
}

#[derive(Debug, Clone)]
struct PersistedMetricsGateResult {
    report: PersistedMetricsGateReport,
    reject_counts_proven: bool,
    reject_counts_unavailable_reason: Option<String>,
    reject_counts: RejectCountsReport,
    rejected_wallet_samples: Vec<RejectedWalletSample>,
    near_miss_metrics_proven: bool,
    near_miss_unavailable_reason: Option<String>,
    max_observed_metrics: MaxObservedMetricsReport,
    single_gate_relief_counts: SingleGateReliefCountsReport,
    bounded_near_miss_wallet_samples: Vec<NearMissWalletSample>,
}

#[derive(Debug, Clone, Default)]
struct TokenQualityEvidence {
    distinct_mints_checked: u64,
    missing_mints: u64,
    stale_mints: u64,
    deferred_mints: u64,
    missing_wallets: BTreeSet<String>,
    stale_wallets: BTreeSet<String>,
    deferred_wallets: BTreeSet<String>,
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
    let expected_metrics_window_start = expected_metrics_window_start(
        config.now,
        loaded_config.discovery.metric_snapshot_interval_seconds,
        loaded_config.discovery.scoring_window_days,
    );
    let thresholds = ThresholdConfig {
        min_trades: loaded_config.discovery.min_trades,
        min_active_days: loaded_config.discovery.min_active_days,
        min_buy_count: loaded_config.discovery.min_buy_count,
        min_tradable_ratio: loaded_config.discovery.min_tradable_ratio,
        min_score: loaded_config.discovery.min_score,
        require_open_positions_for_publication: loaded_config
            .discovery
            .require_open_positions_for_publication,
    };
    let metrics_gate = load_persisted_metrics_gate_report(
        &conn,
        &thresholds,
        expected_metrics_window_start,
        config.sample_limit,
    )?;
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
    let rejected_wallet_samples = if metrics_gate.reject_counts_proven {
        metrics_gate.rejected_wallet_samples.clone()
    } else {
        bounded_rejected_wallet_samples(&observed_sample.sampled_wallets, config.sample_limit)
    };

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
            selector_window_scan:
                "bounded_indexed_observed_swaps_sql_persisted_truth_and_latest_metrics_bucket_only",
        },
        publication,
        raw_window,
        active_follow_wallets,
        active_follow_wallets_capped: false,
        eligible_wallets,
        top_wallets,
        persisted_metrics: metrics_gate.report,
        reject_counts_proven: metrics_gate.reject_counts_proven,
        reject_counts_unavailable_reason: metrics_gate.reject_counts_unavailable_reason,
        reject_counts: metrics_gate.reject_counts,
        rejected_wallet_samples,
        near_miss_metrics_proven: metrics_gate.near_miss_metrics_proven,
        near_miss_unavailable_reason: metrics_gate.near_miss_unavailable_reason,
        max_observed_metrics: metrics_gate.max_observed_metrics,
        single_gate_relief_counts: metrics_gate.single_gate_relief_counts,
        bounded_near_miss_wallet_samples: metrics_gate.bounded_near_miss_wallet_samples,
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

fn expected_metrics_window_start(
    now: DateTime<Utc>,
    metric_snapshot_interval_seconds: u64,
    scoring_window_days: u32,
) -> DateTime<Utc> {
    let interval_seconds = metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(scoring_window_days.max(1) as i64)
}

fn wallet_metrics_window_start_query_variants(window_start: DateTime<Utc>) -> (String, String) {
    let canonical = window_start.to_rfc3339();
    let legacy_z = canonical
        .strip_suffix("+00:00")
        .map(|prefix| format!("{prefix}Z"))
        .unwrap_or_else(|| canonical.clone());
    (canonical, legacy_z)
}

fn load_persisted_metrics_gate_report(
    conn: &Connection,
    thresholds: &ThresholdConfig,
    expected_metrics_window_start: DateTime<Utc>,
    sample_limit: usize,
) -> Result<PersistedMetricsGateResult> {
    let latest_metrics_window = load_latest_wallet_metrics_window_start(conn)?;
    let latest_metrics_window_matches_expected =
        latest_metrics_window
            .as_ref()
            .is_some_and(|(_, latest_metrics_window_start)| {
                *latest_metrics_window_start == expected_metrics_window_start
            });
    let Some((latest_metrics_window_start_raw, latest_metrics_window_start)) =
        latest_metrics_window.clone()
    else {
        return Ok(unproven_persisted_metrics_gate_report(
            thresholds,
            expected_metrics_window_start,
            None,
            None,
            false,
            PERSISTED_METRICS_BUCKET_MISMATCH_REASON,
        ));
    };

    if !latest_metrics_window_matches_expected {
        return Ok(unproven_persisted_metrics_gate_report(
            thresholds,
            expected_metrics_window_start,
            Some(latest_metrics_window_start_raw),
            Some(latest_metrics_window_start),
            false,
            PERSISTED_METRICS_BUCKET_MISMATCH_REASON,
        ));
    }

    if !sqlite_table_exists(conn, "wallet_activity_days")?
        || !sqlite_table_exists(conn, "wallet_scoring_open_lots")?
    {
        return Ok(unproven_persisted_metrics_gate_report(
            thresholds,
            expected_metrics_window_start,
            Some(latest_metrics_window_start_raw),
            Some(latest_metrics_window_start),
            latest_metrics_window_matches_expected,
            "publication_zero_universe_persisted_metric_evidence_tables_missing",
        ));
    }

    let window_start_variants =
        wallet_metrics_window_start_query_variants(expected_metrics_window_start);
    let wallets = load_metric_wallets_for_expected_bucket(
        conn,
        &window_start_variants,
        expected_metrics_window_start,
    )?;
    if wallets.is_empty() {
        return Ok(unproven_persisted_metrics_gate_report(
            thresholds,
            expected_metrics_window_start,
            Some(latest_metrics_window_start_raw),
            Some(latest_metrics_window_start),
            latest_metrics_window_matches_expected,
            PERSISTED_METRICS_BUCKET_MISMATCH_REASON,
        ));
    }
    let token_quality =
        load_token_quality_evidence(conn, &window_start_variants, expected_metrics_window_start)?;
    let (
        reject_counts,
        post_threshold_candidate_wallets,
        post_threshold_open_wallets,
        unexplained_candidate_wallets_after_persisted_gates,
    ) = classify_persisted_metric_gate_counts(&wallets, thresholds, &token_quality);
    let rejected_wallet_samples = persisted_metric_rejected_wallet_samples(
        &wallets,
        thresholds,
        &token_quality,
        sample_limit,
    );
    let max_observed_metrics = max_observed_metrics_report(&wallets);
    let single_gate_relief_counts =
        single_gate_relief_counts_report(&wallets, thresholds, &token_quality);
    let bounded_near_miss_wallet_samples =
        bounded_near_miss_wallet_samples(&wallets, thresholds, &token_quality, sample_limit);

    Ok(PersistedMetricsGateResult {
        report: PersistedMetricsGateReport {
            evidence_source: "latest_wallet_metrics_bucket_and_persisted_scoring_evidence",
            expected_metrics_window_start,
            latest_metrics_window_start: Some(latest_metrics_window_start),
            latest_metrics_window_start_raw: Some(latest_metrics_window_start_raw),
            latest_metrics_window_matches_expected,
            metrics_rows: wallets.len() as u64,
            threshold_counts_proven: true,
            unavailable_reason: None,
            counts_model: "ordered_persisted_metric_threshold_attribution_no_selector_full_scan",
            min_trades_threshold: thresholds.min_trades,
            min_active_days_threshold: thresholds.min_active_days,
            min_buy_count_threshold: thresholds.min_buy_count,
            min_tradable_ratio_threshold: thresholds.min_tradable_ratio,
            min_score_threshold: thresholds.min_score,
            require_open_positions_for_publication: thresholds
                .require_open_positions_for_publication,
            active_day_evidence_source: "wallet_activity_days",
            open_position_evidence_source: "wallet_scoring_open_lots",
            token_quality_evidence_source: "wallet_scoring_buy_facts.quality_source",
            token_quality_distinct_buy_mints_checked: token_quality.distinct_mints_checked,
            token_quality_missing_mints: token_quality.missing_mints,
            token_quality_stale_mints: token_quality.stale_mints,
            token_quality_deferred_mints: token_quality.deferred_mints,
            token_quality_missing_wallets: token_quality.missing_wallets.len() as u64,
            token_quality_stale_wallets: token_quality.stale_wallets.len() as u64,
            token_quality_deferred_wallets: token_quality.deferred_wallets.len() as u64,
            post_threshold_candidate_wallets,
            post_threshold_candidate_wallets_with_open_positions: post_threshold_open_wallets,
            unexplained_candidate_wallets_after_persisted_gates,
        },
        reject_counts_proven: true,
        reject_counts_unavailable_reason: None,
        reject_counts,
        rejected_wallet_samples,
        near_miss_metrics_proven: true,
        near_miss_unavailable_reason: None,
        max_observed_metrics,
        single_gate_relief_counts,
        bounded_near_miss_wallet_samples,
    })
}

fn unproven_persisted_metrics_gate_report(
    thresholds: &ThresholdConfig,
    expected_metrics_window_start: DateTime<Utc>,
    latest_metrics_window_start_raw: Option<String>,
    latest_metrics_window_start: Option<DateTime<Utc>>,
    latest_metrics_window_matches_expected: bool,
    reason: &str,
) -> PersistedMetricsGateResult {
    PersistedMetricsGateResult {
        report: PersistedMetricsGateReport {
            evidence_source: "latest_wallet_metrics_bucket_and_persisted_scoring_evidence",
            expected_metrics_window_start,
            latest_metrics_window_start,
            latest_metrics_window_start_raw,
            latest_metrics_window_matches_expected,
            metrics_rows: 0,
            threshold_counts_proven: false,
            unavailable_reason: Some(reason.to_string()),
            counts_model: "unavailable_no_selector_full_scan",
            min_trades_threshold: thresholds.min_trades,
            min_active_days_threshold: thresholds.min_active_days,
            min_buy_count_threshold: thresholds.min_buy_count,
            min_tradable_ratio_threshold: thresholds.min_tradable_ratio,
            min_score_threshold: thresholds.min_score,
            require_open_positions_for_publication: thresholds
                .require_open_positions_for_publication,
            active_day_evidence_source: "wallet_activity_days",
            open_position_evidence_source: "wallet_scoring_open_lots",
            token_quality_evidence_source: "wallet_scoring_buy_facts.quality_source",
            token_quality_distinct_buy_mints_checked: 0,
            token_quality_missing_mints: 0,
            token_quality_stale_mints: 0,
            token_quality_deferred_mints: 0,
            token_quality_missing_wallets: 0,
            token_quality_stale_wallets: 0,
            token_quality_deferred_wallets: 0,
            post_threshold_candidate_wallets: 0,
            post_threshold_candidate_wallets_with_open_positions: 0,
            unexplained_candidate_wallets_after_persisted_gates: 0,
        },
        reject_counts_proven: false,
        reject_counts_unavailable_reason: Some(reason.to_string()),
        reject_counts: RejectCountsReport::unproven(),
        rejected_wallet_samples: Vec::new(),
        near_miss_metrics_proven: false,
        near_miss_unavailable_reason: Some(reason.to_string()),
        max_observed_metrics: MaxObservedMetricsReport::unproven(),
        single_gate_relief_counts: SingleGateReliefCountsReport::unproven(),
        bounded_near_miss_wallet_samples: Vec::new(),
    }
}

fn load_latest_wallet_metrics_window_start(
    conn: &Connection,
) -> Result<Option<(String, DateTime<Utc>)>> {
    if !sqlite_table_exists(conn, "wallet_metrics")? {
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT window_start FROM wallet_metrics ORDER BY window_start DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed loading latest persisted wallet_metrics window_start")?;
    raw.map(|raw| {
        let parsed = parse_ts(&raw)?;
        Ok((raw, parsed))
    })
    .transpose()
}

fn load_metric_wallets_for_expected_bucket(
    conn: &Connection,
    metrics_window_start_variants: &(String, String),
    metrics_window_start: DateTime<Utc>,
) -> Result<Vec<PersistedMetricWallet>> {
    let active_day_start = metrics_window_start
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let mut stmt = conn
        .prepare(
            "WITH selected_ids AS (
                SELECT wallet_id, MAX(id) AS metric_id
                FROM wallet_metrics
                WHERE window_start = ?1 OR window_start = ?2
                GROUP BY wallet_id
             ),
             selected_metrics AS (
                SELECT wm.wallet_id, wm.trades, wm.buy_total, wm.tradable_ratio, wm.score
                FROM wallet_metrics wm
                JOIN selected_ids selected ON selected.metric_id = wm.id
             )
             SELECT
                selected_metrics.wallet_id,
                selected_metrics.trades,
                selected_metrics.buy_total,
                selected_metrics.tradable_ratio,
                selected_metrics.score,
                COALESCE((
                    SELECT COUNT(*)
                    FROM wallet_activity_days wad
                    WHERE wad.wallet_id = selected_metrics.wallet_id
                      AND wad.activity_day >= ?3
                ), 0) AS active_days,
                EXISTS(
                    SELECT 1
                    FROM wallet_scoring_open_lots open_lots
                    WHERE open_lots.wallet_id = selected_metrics.wallet_id
                    LIMIT 1
                ) AS has_open_position
             FROM selected_metrics
             ORDER BY selected_metrics.score ASC, selected_metrics.wallet_id ASC",
        )
        .context("failed preparing persisted wallet metric gate query")?;
    let rows = stmt
        .query_map(
            params![
                metrics_window_start_variants.0,
                metrics_window_start_variants.1,
                active_day_start
            ],
            |row| {
                let trades: i64 = row.get(1)?;
                let buy_total: i64 = row.get(2)?;
                let active_days: i64 = row.get(5)?;
                let has_open_position: i64 = row.get(6)?;
                Ok(PersistedMetricWallet {
                    wallet_id: row.get(0)?,
                    trades: nonnegative_i64_to_u32(trades),
                    buy_total: nonnegative_i64_to_u32(buy_total),
                    tradable_ratio: row.get(3)?,
                    score: row.get(4)?,
                    active_days: nonnegative_i64_to_u32(active_days),
                    has_open_position: has_open_position != 0,
                })
            },
        )
        .context("failed querying persisted wallet metric gate rows")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("failed collecting persisted wallet metric gate rows")?;
    Ok(rows)
}

fn load_token_quality_evidence(
    conn: &Connection,
    metrics_window_start_variants: &(String, String),
    metrics_window_start: DateTime<Utc>,
) -> Result<TokenQualityEvidence> {
    if !sqlite_table_exists(conn, "wallet_scoring_buy_facts")? {
        return Ok(TokenQualityEvidence::default());
    }
    let mut stmt = conn
        .prepare(
            "WITH selected_wallets AS (
                SELECT DISTINCT wallet_id
                FROM wallet_metrics
                WHERE window_start = ?1 OR window_start = ?2
             )
             SELECT DISTINCT buy_facts.wallet_id, buy_facts.token, buy_facts.quality_source
             FROM wallet_scoring_buy_facts buy_facts
             JOIN selected_wallets selected
               ON selected.wallet_id = buy_facts.wallet_id
             WHERE buy_facts.ts >= ?3",
        )
        .context("failed preparing persisted token-quality evidence query")?;
    let mut rows = stmt
        .query(params![
            metrics_window_start_variants.0,
            metrics_window_start_variants.1,
            metrics_window_start.to_rfc3339()
        ])
        .context("failed querying persisted token-quality evidence")?;
    let mut all_mints = BTreeSet::new();
    let mut missing_mints = BTreeSet::new();
    let mut stale_mints = BTreeSet::new();
    let mut deferred_mints = BTreeSet::new();
    let mut missing_wallets = BTreeSet::new();
    let mut stale_wallets = BTreeSet::new();
    let mut deferred_wallets = BTreeSet::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating persisted token-quality evidence")?
    {
        let wallet_id: String = row
            .get(0)
            .context("failed reading wallet_scoring_buy_facts.wallet_id")?;
        let token: String = row
            .get(1)
            .context("failed reading wallet_scoring_buy_facts.token")?;
        let quality_source: String = row
            .get(2)
            .context("failed reading wallet_scoring_buy_facts.quality_source")?;
        all_mints.insert(token.clone());
        match quality_source.as_str() {
            "missing" => {
                missing_mints.insert(token);
                missing_wallets.insert(wallet_id);
            }
            "stale" => {
                stale_mints.insert(token);
                stale_wallets.insert(wallet_id);
            }
            "deferred" => {
                deferred_mints.insert(token);
                deferred_wallets.insert(wallet_id);
            }
            _ => {}
        }
    }
    Ok(TokenQualityEvidence {
        distinct_mints_checked: all_mints.len() as u64,
        missing_mints: missing_mints.len() as u64,
        stale_mints: stale_mints.len() as u64,
        deferred_mints: deferred_mints.len() as u64,
        missing_wallets,
        stale_wallets,
        deferred_wallets,
    })
}

fn classify_persisted_metric_gate_counts(
    wallets: &[PersistedMetricWallet],
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
) -> (RejectCountsReport, u64, u64, u64) {
    let mut remaining = wallets.iter().collect::<Vec<_>>();
    let before = remaining.len();
    remaining.retain(|wallet| wallet.trades >= thresholds.min_trades);
    let min_trades = (before - remaining.len()) as u64;

    let before = remaining.len();
    remaining.retain(|wallet| wallet.active_days >= thresholds.min_active_days);
    let min_active_days = (before - remaining.len()) as u64;

    let before = remaining.len();
    remaining.retain(|wallet| wallet.buy_total >= thresholds.min_buy_count);
    let min_buy_count = (before - remaining.len()) as u64;

    let before = remaining.len();
    remaining.retain(|wallet| wallet.tradable_ratio >= thresholds.min_tradable_ratio);
    let min_tradable_ratio = (before - remaining.len()) as u64;

    let before = remaining.len();
    remaining.retain(|wallet| wallet.score >= thresholds.min_score);
    let min_score = (before - remaining.len()) as u64;

    let post_threshold_candidate_wallets = remaining.len() as u64;
    let post_threshold_open_wallets = remaining
        .iter()
        .filter(|wallet| wallet.has_open_position)
        .count() as u64;
    let require_open_positions_for_publication =
        if thresholds.require_open_positions_for_publication {
            let before = remaining.len();
            remaining.retain(|wallet| wallet.has_open_position);
            (before - remaining.len()) as u64
        } else {
            0
        };
    let unexplained_candidate_wallets_after_persisted_gates = remaining.len() as u64;

    (
        RejectCountsReport {
            min_trades: Some(min_trades),
            min_active_days: Some(min_active_days),
            min_buy_count: Some(min_buy_count),
            min_tradable_ratio: Some(min_tradable_ratio),
            min_score: Some(min_score),
            require_open_positions_for_publication: Some(require_open_positions_for_publication),
            token_quality_missing: Some(token_quality.missing_wallets.len() as u64),
            token_quality_stale: Some(token_quality.stale_wallets.len() as u64),
            token_quality_failed: Some(token_quality.deferred_wallets.len() as u64),
            other_rejects: Some(0),
        },
        post_threshold_candidate_wallets,
        post_threshold_open_wallets,
        unexplained_candidate_wallets_after_persisted_gates,
    )
}

fn persisted_metric_rejected_wallet_samples(
    wallets: &[PersistedMetricWallet],
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
    sample_limit: usize,
) -> Vec<RejectedWalletSample> {
    wallets
        .iter()
        .filter_map(|wallet| {
            let reject_reasons =
                persisted_metric_wallet_reject_reasons(wallet, thresholds, token_quality);
            if reject_reasons.is_empty() {
                None
            } else {
                Some(RejectedWalletSample {
                    wallet_id: wallet.wallet_id.clone(),
                    reject_reasons_proven: true,
                    reject_reasons,
                })
            }
        })
        .take(sample_limit)
        .collect()
}

fn persisted_metric_wallet_reject_reasons(
    wallet: &PersistedMetricWallet,
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
) -> Vec<String> {
    let mut reject_reasons = Vec::new();
    if wallet.trades < thresholds.min_trades {
        reject_reasons.push("min_trades".to_string());
    }
    if wallet.active_days < thresholds.min_active_days {
        reject_reasons.push("min_active_days".to_string());
    }
    if wallet.buy_total < thresholds.min_buy_count {
        reject_reasons.push("min_buy_count".to_string());
    }
    if wallet.tradable_ratio < thresholds.min_tradable_ratio {
        reject_reasons.push("min_tradable_ratio".to_string());
    }
    let hard_metric_rejected = !reject_reasons.is_empty();
    if !hard_metric_rejected && wallet.score < thresholds.min_score {
        reject_reasons.push("min_score".to_string());
    }
    if thresholds.require_open_positions_for_publication
        && !hard_metric_rejected
        && !wallet.has_open_position
    {
        reject_reasons.push("require_open_positions_for_publication".to_string());
    }
    if token_quality.missing_wallets.contains(&wallet.wallet_id) {
        reject_reasons.push("token_quality_missing".to_string());
    }
    if token_quality.stale_wallets.contains(&wallet.wallet_id) {
        reject_reasons.push("token_quality_stale".to_string());
    }
    if token_quality.deferred_wallets.contains(&wallet.wallet_id) {
        reject_reasons.push("token_quality_deferred".to_string());
    }
    reject_reasons
}

fn max_observed_metrics_report(wallets: &[PersistedMetricWallet]) -> MaxObservedMetricsReport {
    MaxObservedMetricsReport {
        max_trades: wallets.iter().map(|wallet| wallet.trades as u64).max(),
        max_active_days: wallets.iter().map(|wallet| wallet.active_days as u64).max(),
        max_buy_count: wallets.iter().map(|wallet| wallet.buy_total as u64).max(),
        max_tradable_ratio: wallets
            .iter()
            .map(|wallet| wallet.tradable_ratio)
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)),
        max_score: wallets
            .iter()
            .map(|wallet| wallet.score)
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)),
    }
}

fn single_gate_relief_counts_report(
    wallets: &[PersistedMetricWallet],
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
) -> SingleGateReliefCountsReport {
    SingleGateReliefCountsReport {
        would_pass_if_min_trades_ignored: Some(count_single_gate_relief_wallets(
            wallets,
            thresholds,
            token_quality,
            "min_trades",
        )),
        would_pass_if_min_active_days_ignored: Some(count_single_gate_relief_wallets(
            wallets,
            thresholds,
            token_quality,
            "min_active_days",
        )),
        would_pass_if_min_buy_count_ignored: Some(count_single_gate_relief_wallets(
            wallets,
            thresholds,
            token_quality,
            "min_buy_count",
        )),
        would_pass_if_min_tradable_ratio_ignored: Some(count_single_gate_relief_wallets(
            wallets,
            thresholds,
            token_quality,
            "min_tradable_ratio",
        )),
        would_pass_if_min_score_ignored: Some(count_single_gate_relief_wallets(
            wallets,
            thresholds,
            token_quality,
            "min_score",
        )),
        would_pass_if_open_position_gate_ignored: Some(count_single_gate_relief_wallets(
            wallets,
            thresholds,
            token_quality,
            "require_open_positions_for_publication",
        )),
    }
}

fn count_single_gate_relief_wallets(
    wallets: &[PersistedMetricWallet],
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
    ignored_gate: &str,
) -> u64 {
    wallets
        .iter()
        .filter(|wallet| {
            let failing_gates = persisted_metric_wallet_independent_failing_gates(
                wallet,
                thresholds,
                token_quality,
            );
            failing_gates.len() == 1
                && failing_gates
                    .first()
                    .is_some_and(|gate| gate == ignored_gate)
        })
        .count() as u64
}

fn bounded_near_miss_wallet_samples(
    wallets: &[PersistedMetricWallet],
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
    sample_limit: usize,
) -> Vec<NearMissWalletSample> {
    let mut samples = wallets
        .iter()
        .filter_map(|wallet| {
            let failing_gates = persisted_metric_wallet_independent_failing_gates(
                wallet,
                thresholds,
                token_quality,
            );
            (!failing_gates.is_empty()).then(|| NearMissWalletSample {
                wallet_id: wallet.wallet_id.clone(),
                trades: wallet.trades,
                active_days: wallet.active_days,
                buy_count: wallet.buy_total,
                tradable_ratio: wallet.tradable_ratio,
                score: wallet.score,
                has_open_position: wallet.has_open_position,
                failing_gates,
            })
        })
        .collect::<Vec<_>>();
    samples.sort_by(|left, right| {
        left.failing_gates
            .len()
            .cmp(&right.failing_gates.len())
            .then_with(|| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.wallet_id.cmp(&right.wallet_id))
    });
    samples.truncate(sample_limit);
    samples
}

fn persisted_metric_wallet_independent_failing_gates(
    wallet: &PersistedMetricWallet,
    thresholds: &ThresholdConfig,
    token_quality: &TokenQualityEvidence,
) -> Vec<String> {
    let mut failing_gates = Vec::new();
    if wallet.trades < thresholds.min_trades {
        failing_gates.push("min_trades".to_string());
    }
    if wallet.active_days < thresholds.min_active_days {
        failing_gates.push("min_active_days".to_string());
    }
    if wallet.buy_total < thresholds.min_buy_count {
        failing_gates.push("min_buy_count".to_string());
    }
    if wallet.tradable_ratio < thresholds.min_tradable_ratio {
        failing_gates.push("min_tradable_ratio".to_string());
    }
    if wallet.score < thresholds.min_score {
        failing_gates.push("min_score".to_string());
    }
    if thresholds.require_open_positions_for_publication && !wallet.has_open_position {
        failing_gates.push("require_open_positions_for_publication".to_string());
    }
    if token_quality.missing_wallets.contains(&wallet.wallet_id) {
        failing_gates.push("token_quality_missing".to_string());
    }
    if token_quality.stale_wallets.contains(&wallet.wallet_id) {
        failing_gates.push("token_quality_stale".to_string());
    }
    if token_quality.deferred_wallets.contains(&wallet.wallet_id) {
        failing_gates.push("token_quality_deferred".to_string());
    }
    failing_gates
}

fn nonnegative_i64_to_u32(value: i64) -> u32 {
    value.max(0).min(u32::MAX as i64) as u32
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

impl MaxObservedMetricsReport {
    fn unproven() -> Self {
        Self {
            max_trades: None,
            max_active_days: None,
            max_buy_count: None,
            max_tradable_ratio: None,
            max_score: None,
        }
    }
}

impl SingleGateReliefCountsReport {
    fn unproven() -> Self {
        Self {
            would_pass_if_min_trades_ignored: None,
            would_pass_if_min_active_days_ignored: None,
            would_pass_if_min_buy_count_ignored: None,
            would_pass_if_min_tradable_ratio_ignored: None,
            would_pass_if_min_score_ignored: None,
            would_pass_if_open_position_gate_ignored: None,
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
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode,
        DiscoveryWalletFreshnessCaptureWrite, WalletActivityDayRow, WalletMetricRow,
        WalletUpsertRow,
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
        assert!(report.reject_counts_proven);
        assert_eq!(report.reject_counts_unavailable_reason, None);
        assert_eq!(
            report.persisted_metrics.expected_metrics_window_start,
            fixture_expected_metrics_window_start(fixture.now)
        );
        assert!(
            report
                .persisted_metrics
                .latest_metrics_window_matches_expected
        );
        assert_eq!(report.persisted_metrics.metrics_rows, 5);
        assert_eq!(report.reject_counts.min_trades, Some(1));
        assert_eq!(report.reject_counts.min_active_days, Some(1));
        assert_eq!(report.reject_counts.min_buy_count, Some(1));
        assert_eq!(report.reject_counts.min_tradable_ratio, Some(1));
        assert_eq!(report.reject_counts.min_score, Some(1));
        assert_eq!(report.reject_counts.token_quality_missing, Some(1));
        assert_eq!(report.reject_counts.token_quality_stale, Some(1));
        assert_eq!(report.reject_counts.other_rejects, Some(0));
        assert!(report.near_miss_metrics_proven);
        assert_eq!(report.near_miss_unavailable_reason, None);
        assert_eq!(report.max_observed_metrics.max_trades, Some(8));
        assert_eq!(report.max_observed_metrics.max_active_days, Some(3));
        assert_eq!(report.max_observed_metrics.max_buy_count, Some(10));
        assert_eq!(
            report
                .single_gate_relief_counts
                .would_pass_if_min_trades_ignored,
            Some(1)
        );
        assert_eq!(
            report
                .single_gate_relief_counts
                .would_pass_if_min_score_ignored,
            Some(1)
        );
        assert!(!report.bounded_near_miss_wallet_samples.is_empty());
        Ok(())
    }

    #[test]
    fn unaligned_now_still_matches_bucketed_expected_metrics_window() -> Result<()> {
        let fixture = TestFixture::new(TestCase::UnalignedExpectedBucket)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.reject_counts_proven);
        assert_eq!(
            report.persisted_metrics.expected_metrics_window_start,
            parse_ts("2026-04-19T12:00:00Z")?
        );
        assert_eq!(
            report.persisted_metrics.latest_metrics_window_start,
            Some(parse_ts("2026-04-19T12:00:00Z")?)
        );
        assert!(
            report
                .persisted_metrics
                .latest_metrics_window_matches_expected
        );
        assert!(!report.selector_full_scan_used);
        Ok(())
    }

    #[test]
    fn stale_latest_metrics_bucket_keeps_reject_counts_unproven() -> Result<()> {
        let fixture = TestFixture::new(TestCase::StaleMetricsBucket)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(!report.reject_counts_proven);
        assert!(!report.persisted_metrics.threshold_counts_proven);
        assert_eq!(
            report.reject_counts_unavailable_reason.as_deref(),
            Some(PERSISTED_METRICS_BUCKET_MISMATCH_REASON)
        );
        assert_eq!(report.reject_counts.min_trades, None);
        assert!(!report.near_miss_metrics_proven);
        assert_eq!(
            report.near_miss_unavailable_reason.as_deref(),
            Some(PERSISTED_METRICS_BUCKET_MISMATCH_REASON)
        );
        assert_eq!(report.max_observed_metrics.max_trades, None);
        assert!(report.bounded_near_miss_wallet_samples.is_empty());
        assert!(
            !report
                .persisted_metrics
                .latest_metrics_window_matches_expected
        );
        assert!(report
            .rejected_wallet_samples
            .iter()
            .all(|sample| !sample.reject_reasons_proven));
        assert!(!report.selector_full_scan_used);
        Ok(())
    }

    #[test]
    fn near_miss_metrics_show_min_trades_dominates() -> Result<()> {
        let fixture = TestFixture::new(TestCase::MinTradesDominates)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.near_miss_metrics_proven);
        assert_eq!(report.max_observed_metrics.max_trades, Some(1));
        assert_eq!(
            report
                .single_gate_relief_counts
                .would_pass_if_min_trades_ignored,
            Some(3)
        );
        assert_eq!(
            report
                .single_gate_relief_counts
                .would_pass_if_min_active_days_ignored,
            Some(0)
        );
        assert_eq!(report.bounded_near_miss_wallet_samples.len(), 3);
        assert!(report
            .bounded_near_miss_wallet_samples
            .iter()
            .all(|sample| sample.failing_gates == vec!["min_trades".to_string()]));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn near_miss_metrics_surface_one_gate_away_wallet() -> Result<()> {
        let fixture = TestFixture::new(TestCase::OneGateAway)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.near_miss_metrics_proven);
        assert_eq!(
            report
                .single_gate_relief_counts
                .would_pass_if_min_score_ignored,
            Some(1)
        );
        let sample = report
            .bounded_near_miss_wallet_samples
            .first()
            .expect("one-gate-away wallet sample should be present");
        assert_eq!(sample.wallet_id, "wallet-one-gate-score");
        assert_eq!(sample.failing_gates, vec!["min_score".to_string()]);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn near_miss_wallet_samples_are_capped_by_sample_limit() -> Result<()> {
        let fixture = TestFixture::new(TestCase::MinTradesDominates)?;
        let report = run(&fixture.config(1))?;
        assert!(report.near_miss_metrics_proven);
        assert_eq!(report.bounded_near_miss_wallet_samples.len(), 1);
        assert!(!report.production_green);
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
    fn persisted_metrics_threshold_counts_are_reported_without_full_selector_scan() -> Result<()> {
        let fixture = TestFixture::new(TestCase::PersistedZeroUniverse)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.reject_counts_proven);
        assert_eq!(
            report.persisted_metrics.counts_model,
            "ordered_persisted_metric_threshold_attribution_no_selector_full_scan"
        );
        assert_eq!(report.reject_counts.min_trades, Some(1));
        assert_eq!(report.reject_counts.min_buy_count, Some(1));
        assert_eq!(report.rejected_wallet_samples.len(), 5);
        assert!(report
            .rejected_wallet_samples
            .iter()
            .all(|sample| sample.reject_reasons_proven));
        assert!(!report.selector_full_scan_used);
        Ok(())
    }

    #[test]
    fn require_open_positions_gate_count_uses_persisted_open_lots() -> Result<()> {
        let fixture = TestFixture::new(TestCase::OpenPositionGate)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.reject_counts_proven);
        assert_eq!(
            report
                .persisted_metrics
                .require_open_positions_for_publication,
            true
        );
        assert_eq!(
            report.reject_counts.require_open_positions_for_publication,
            Some(1)
        );
        assert_eq!(report.persisted_metrics.post_threshold_candidate_wallets, 1);
        assert_eq!(
            report
                .persisted_metrics
                .post_threshold_candidate_wallets_with_open_positions,
            0
        );
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn post_gate_survivor_is_not_counted_as_other_reject() -> Result<()> {
        let fixture = TestFixture::new(TestCase::SurvivingCandidate)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert!(report.reject_counts_proven);
        assert_eq!(report.reject_counts.other_rejects, Some(0));
        assert_eq!(
            report
                .persisted_metrics
                .unexplained_candidate_wallets_after_persisted_gates,
            1
        );
        assert_eq!(report.persisted_metrics.post_threshold_candidate_wallets, 1);
        assert!(report.rejected_wallet_samples.is_empty());
        assert!(!report.production_green);
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
        OpenPositionGate,
        UnalignedExpectedBucket,
        StaleMetricsBucket,
        SurvivingCandidate,
        MinTradesDominates,
        OneGateAway,
    }

    impl TestCase {
        fn now_raw(self) -> &'static str {
            match self {
                Self::UnalignedExpectedBucket => "2026-04-24T12:17:45Z",
                _ => NOW,
            }
        }
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
            let now = parse_ts(test_case.now_raw())?;
            write_test_config(
                &config_path,
                &db_path,
                matches!(test_case, TestCase::OpenPositionGate),
            )?;
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

    fn write_test_config(path: &Path, db_path: &Path, require_open_positions: bool) -> Result<()> {
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
                "observed_swaps_retention_days = 14\n",
                "require_open_positions_for_publication = {}\n"
            ),
            db_path.display(),
            require_open_positions,
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
                seed_persisted_metrics(path, &store, now, false)?;
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
            TestCase::OpenPositionGate => {
                seed_swaps(&store, now, 1)?;
                seed_open_position_gate_metrics(&store, now)?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
            TestCase::UnalignedExpectedBucket => {
                seed_swaps(&store, now, 1)?;
                seed_persisted_metrics(path, &store, now, false)?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
            TestCase::StaleMetricsBucket => {
                seed_swaps(&store, now, 1)?;
                seed_persisted_metrics_at_window(
                    path,
                    &store,
                    fixture_expected_metrics_window_start(now) - Duration::minutes(30),
                    now,
                    false,
                )?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
            TestCase::SurvivingCandidate => {
                seed_swaps(&store, now, 1)?;
                seed_surviving_candidate_metrics(&store, now)?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
            TestCase::MinTradesDominates => {
                seed_swaps(&store, now, 1)?;
                seed_min_trades_dominates_metrics(&store, now)?;
                seed_freshness_history(
                    &store,
                    now,
                    true,
                    "full_scoring_window_raw_truth_available",
                )?;
            }
            TestCase::OneGateAway => {
                seed_swaps(&store, now, 1)?;
                seed_one_gate_away_metrics(&store, now)?;
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

    fn fixture_expected_metrics_window_start(now: DateTime<Utc>) -> DateTime<Utc> {
        expected_metrics_window_start(now, 1_800, 5)
    }

    fn seed_persisted_metrics(
        db_path: &Path,
        store: &SqliteStore,
        now: DateTime<Utc>,
        include_open_lot: bool,
    ) -> Result<()> {
        seed_persisted_metrics_at_window(
            db_path,
            store,
            fixture_expected_metrics_window_start(now),
            now,
            include_open_lot,
        )
    }

    fn seed_persisted_metrics_at_window(
        db_path: &Path,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        include_open_lot: bool,
    ) -> Result<()> {
        let rows = [
            MetricFixture {
                wallet_id: "wallet-min-trades",
                trades: 1,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.9,
                quality_source: None,
            },
            MetricFixture {
                wallet_id: "wallet-min-active-days",
                trades: 8,
                active_days: 1,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.9,
                quality_source: None,
            },
            MetricFixture {
                wallet_id: "wallet-min-buy-count",
                trades: 8,
                active_days: 3,
                buy_total: 1,
                tradable_ratio: 1.0,
                score: 0.9,
                quality_source: Some("missing"),
            },
            MetricFixture {
                wallet_id: "wallet-min-tradable-ratio",
                trades: 8,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 0.1,
                score: 0.0,
                quality_source: Some("stale"),
            },
            MetricFixture {
                wallet_id: "wallet-min-score",
                trades: 8,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.1,
                quality_source: None,
            },
        ];
        seed_metric_rows(store, window_start, now, &rows)?;
        seed_quality_buy_facts(db_path, window_start, now, &rows, include_open_lot)?;
        Ok(())
    }

    fn seed_open_position_gate_metrics(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let window_start = fixture_expected_metrics_window_start(now);
        let rows = [MetricFixture {
            wallet_id: "wallet-open-position-required",
            trades: 8,
            active_days: 3,
            buy_total: 10,
            tradable_ratio: 1.0,
            score: 0.9,
            quality_source: None,
        }];
        seed_metric_rows(store, window_start, now, &rows)?;
        Ok(())
    }

    fn seed_surviving_candidate_metrics(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let window_start = fixture_expected_metrics_window_start(now);
        let rows = [MetricFixture {
            wallet_id: "wallet-survives-persisted-gates",
            trades: 8,
            active_days: 3,
            buy_total: 10,
            tradable_ratio: 1.0,
            score: 0.9,
            quality_source: None,
        }];
        seed_metric_rows(store, window_start, now, &rows)?;
        Ok(())
    }

    fn seed_min_trades_dominates_metrics(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let window_start = fixture_expected_metrics_window_start(now);
        let rows = [
            MetricFixture {
                wallet_id: "wallet-min-trades-a",
                trades: 1,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.9,
                quality_source: None,
            },
            MetricFixture {
                wallet_id: "wallet-min-trades-b",
                trades: 1,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.8,
                quality_source: None,
            },
            MetricFixture {
                wallet_id: "wallet-min-trades-c",
                trades: 1,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.7,
                quality_source: None,
            },
        ];
        seed_metric_rows(store, window_start, now, &rows)?;
        Ok(())
    }

    fn seed_one_gate_away_metrics(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let window_start = fixture_expected_metrics_window_start(now);
        let rows = [
            MetricFixture {
                wallet_id: "wallet-one-gate-score",
                trades: 8,
                active_days: 3,
                buy_total: 10,
                tradable_ratio: 1.0,
                score: 0.1,
                quality_source: None,
            },
            MetricFixture {
                wallet_id: "wallet-multiple-gates",
                trades: 1,
                active_days: 0,
                buy_total: 1,
                tradable_ratio: 0.1,
                score: 0.9,
                quality_source: None,
            },
        ];
        seed_metric_rows(store, window_start, now, &rows)?;
        Ok(())
    }

    fn seed_metric_rows(
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        rows: &[MetricFixture],
    ) -> Result<()> {
        let wallets = rows
            .iter()
            .map(|row| WalletUpsertRow {
                wallet_id: row.wallet_id.to_string(),
                first_seen: window_start,
                last_seen: now - Duration::minutes(5),
                status: "active".to_string(),
            })
            .collect::<Vec<_>>();
        let metrics = rows
            .iter()
            .map(|row| WalletMetricRow {
                wallet_id: row.wallet_id.to_string(),
                window_start,
                pnl: 0.0,
                win_rate: 0.0,
                trades: row.trades,
                closed_trades: row.trades,
                hold_median_seconds: 0,
                score: row.score,
                buy_total: row.buy_total,
                tradable_ratio: row.tradable_ratio,
                rug_ratio: 0.0,
            })
            .collect::<Vec<_>>();
        store.persist_discovery_cycle(&wallets, &metrics, &[], false, false, now, "fixture")?;

        let mut activity_days = Vec::new();
        for row in rows {
            for offset in 0..row.active_days {
                let activity_day = window_start
                    .date_naive()
                    .checked_add_signed(Duration::days(offset as i64))
                    .context("failed computing activity day fixture")?;
                activity_days.push(WalletActivityDayRow {
                    wallet_id: row.wallet_id.to_string(),
                    activity_day,
                    last_seen: now - Duration::minutes(5),
                });
            }
        }
        store.upsert_wallet_activity_days(&activity_days)?;
        Ok(())
    }

    fn seed_quality_buy_facts(
        db_path: &Path,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        rows: &[MetricFixture],
        include_open_lot: bool,
    ) -> Result<()> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("failed opening fixture db {}", db_path.display()))?;
        for (index, row) in rows.iter().enumerate() {
            let Some(quality_source) = row.quality_source else {
                continue;
            };
            let signature = format!("quality-sig-{index}");
            let token = format!("QualityToken{index}");
            let ts = now - Duration::minutes(20) + Duration::seconds(index as i64);
            conn.execute(
                "INSERT INTO wallet_scoring_buy_facts(
                    buy_signature,
                    wallet_id,
                    token,
                    ts,
                    activity_day,
                    notional_sol,
                    market_volume_5m_sol,
                    market_unique_traders_5m,
                    market_liquidity_proxy_sol,
                    quality_source,
                    quality_token_age_seconds,
                    quality_holders,
                    quality_liquidity_sol,
                    rug_check_after_ts,
                    rug_volume_lookahead_sol,
                    rug_unique_traders_lookahead
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 1.0, 10.0, 10, 5.0, ?6, NULL, NULL, NULL, ?7, NULL, NULL)",
                params![
                    signature,
                    row.wallet_id,
                    token,
                    ts.to_rfc3339(),
                    window_start.date_naive().format("%Y-%m-%d").to_string(),
                    quality_source,
                    (ts + Duration::minutes(30)).to_rfc3339(),
                ],
            )
            .context("failed inserting fixture wallet_scoring_buy_facts row")?;
            if include_open_lot {
                conn.execute(
                    "INSERT INTO wallet_scoring_open_lots(
                        buy_signature,
                        wallet_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts
                     ) VALUES (?1, ?2, ?3, 1.0, 1.0, ?4)",
                    params![signature, row.wallet_id, token, ts.to_rfc3339()],
                )
                .context("failed inserting fixture wallet_scoring_open_lots row")?;
            }
        }
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    struct MetricFixture {
        wallet_id: &'static str,
        trades: u32,
        active_days: u32,
        buy_total: u32,
        tradable_ratio: f64,
        score: f64,
        quality_source: Option<&'static str>,
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
