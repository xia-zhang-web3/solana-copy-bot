use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

const USAGE: &str = "usage: discovery_collect_buy_mints_freshness_blocker_report --config <path> --db-path <runtime sqlite> --now <rfc3339> --json [--sample-limit <n>]";
const DEFAULT_SAMPLE_LIMIT: usize = 10;
const MAX_SAMPLE_LIMIT: usize = 50;
const RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON: &str = "raw_window_zero_publishable_universe";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .context("failed serializing collect-buy-mints freshness blocker report")?
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
struct CollectBuyMintsFreshnessBlockerReport {
    event: &'static str,
    config_path: String,
    db_path: String,
    now_utc: DateTime<Utc>,
    runtime_db_open_mode: &'static str,
    elapsed_ms: u64,
    selector_full_scan_used: bool,
    bounded_operator: bool,
    publication_state: PublicationStateReport,
    latest_persisted_freshness_capture: Option<FreshnessCaptureReport>,
    raw_window_persisted_health: RawWindowPersistedHealthReport,
    observed_swaps_bounded_window: ObservedSwapsBoundedWindowReport,
    expected_metrics_window_start: DateTime<Utc>,
    latest_wallet_metrics_window_start: Option<DateTime<Utc>>,
    latest_wallet_metrics_window_matches_expected: bool,
    persisted_rebuild: Option<PersistedRebuildReport>,
    collect_buy_mints_checkpoint_exists: bool,
    collect_buy_mints_appears_to_restart_from_fresh_each_cycle: bool,
    fresh_zero_universe_persisted_evidence_consistent: bool,
    cached_raw_window_summary_conflicts_with_persisted_truth: bool,
    blocker_reason: String,
    next_safe_operator_action: String,
    production_green: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct PublicationStateReport {
    publication_runtime_mode: Option<String>,
    reason: Option<String>,
    updated_at: Option<DateTime<Utc>>,
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    published_scoring_source: Option<String>,
    published_wallet_count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct FreshnessCaptureReport {
    capture_id: i64,
    captured_at: DateTime<Utc>,
    age_seconds: i64,
    recent_cycles: u64,
    verdict: String,
    reason: String,
    publication_age_seconds: Option<i64>,
    raw_truth_sufficient: bool,
    raw_truth_reason: String,
    shadow_signal_verdict: String,
    shadow_signal_reason: String,
    published_wallet_count: u64,
    active_follow_wallet_count: u64,
    current_raw_top_wallet_count: u64,
    raw_truth_observed_swaps_loaded: Option<u64>,
    raw_truth_eligible_wallet_count: Option<u64>,
    raw_truth_wallets_seen: Option<u64>,
    raw_truth_distinct_buy_mint_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct RawWindowPersistedHealthReport {
    persisted_capture_present: bool,
    raw_truth_sufficient: Option<bool>,
    raw_truth_reason: Option<String>,
    persisted_capture_age_seconds: Option<i64>,
    fresh_under_refresh_gate: Option<bool>,
    refresh_seconds: u64,
    raw_window_healthy: bool,
    raw_window_health_reason: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct ObservedSwapsBoundedWindowReport {
    table_present: bool,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    sample_limit: usize,
    query_limit: usize,
    sample_count: u64,
    sample_capped: bool,
    sample_min_ts_utc: Option<DateTime<Utc>>,
    sample_max_ts_utc: Option<DateTime<Utc>>,
    window_min_ts_utc: Option<DateTime<Utc>>,
    window_max_ts_utc: Option<DateTime<Utc>>,
    sample_buy_count: u64,
    sample_sell_count: u64,
    sample_distinct_wallet_count: u64,
    sample_distinct_buy_mint_count: u64,
    full_table_count_used: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct PersistedRebuildReport {
    phase: String,
    window_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    phase_cursor: Option<ReportCursor>,
    prepass_rows_processed: u64,
    prepass_pages_processed: u64,
    replay_rows_processed: u64,
    replay_pages_processed: u64,
    chunks_completed: u64,
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    state_json_bytes: usize,
    state_json_parse_ok: bool,
    collect_buy_mints: CollectBuyMintsCheckpointReport,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct CollectBuyMintsCheckpointReport {
    mode: Option<String>,
    cursor_token: Option<String>,
    prepass_complete: Option<bool>,
    unique_buy_mints_count: Option<u64>,
    buy_mint_counts_count: Option<u64>,
    reconcile_source_window_start: Option<DateTime<Utc>>,
    reconcile_source_horizon_end: Option<DateTime<Utc>>,
    reconcile_expired_head_cursor: Option<ReportCursor>,
    reconcile_new_tail_cursor: Option<ReportCursor>,
    reconcile_expired_head_cursor_token: Option<String>,
    reconcile_new_tail_cursor_token: Option<String>,
    reconcile_expired_head_pending_mints: Option<u64>,
    reconcile_new_tail_slice_end_token: Option<String>,
    reconcile_new_tail_pending_mints: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReportCursor {
    ts_utc: DateTime<Utc>,
    slot: u64,
    signature: String,
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

fn run(config: &Config) -> Result<CollectBuyMintsFreshnessBlockerReport> {
    let started = Instant::now();
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let conn = open_read_only_connection(&config.db_path)?;
    let expected_metrics_window_start = expected_metrics_window_start(
        config.now,
        loaded_config.discovery.metric_snapshot_interval_seconds,
        loaded_config.discovery.scoring_window_days,
    );
    let publication_state = load_publication_state(&conn)?;
    let latest_persisted_freshness_capture = load_latest_freshness_capture(&conn, config.now)?;
    let raw_window_persisted_health = raw_window_persisted_health_report(
        latest_persisted_freshness_capture.as_ref(),
        loaded_config.discovery.refresh_seconds,
    );
    let observed_swaps_bounded_window = load_observed_swaps_bounded_window(
        &conn,
        expected_metrics_window_start,
        config.now,
        config.sample_limit,
    )?;
    let latest_wallet_metrics_window_start = load_latest_wallet_metrics_window_start(&conn)?;
    let latest_wallet_metrics_window_matches_expected =
        latest_wallet_metrics_window_start == Some(expected_metrics_window_start);
    let persisted_rebuild = load_persisted_rebuild(&conn)?;
    let collect_buy_mints_checkpoint_exists = persisted_rebuild.as_ref().is_some_and(|state| {
        state.phase == "collect_buy_mints"
            || state.collect_buy_mints.mode.is_some()
            || state.collect_buy_mints.cursor_token.is_some()
            || state.collect_buy_mints.prepass_complete.is_some()
    });
    let collect_buy_mints_appears_to_restart_from_fresh_each_cycle =
        collect_buy_mints_restart_from_fresh_each_cycle(persisted_rebuild.as_ref());
    let fresh_zero_universe_persisted_evidence_consistent =
        fresh_zero_universe_persisted_evidence_consistent(
            &publication_state,
            latest_persisted_freshness_capture.as_ref(),
            &raw_window_persisted_health,
        );
    let cached_raw_window_summary_conflicts_with_persisted_truth =
        !fresh_zero_universe_persisted_evidence_consistent
            && cached_raw_window_summary_conflicts_with_persisted_truth(
                &publication_state,
                &raw_window_persisted_health,
            );
    let blocker_reason = classify_blocker_reason(
        &publication_state,
        &raw_window_persisted_health,
        persisted_rebuild.as_ref(),
        collect_buy_mints_checkpoint_exists,
        collect_buy_mints_appears_to_restart_from_fresh_each_cycle,
        fresh_zero_universe_persisted_evidence_consistent,
        cached_raw_window_summary_conflicts_with_persisted_truth,
    );
    let next_safe_operator_action = next_safe_operator_action(&blocker_reason);

    Ok(CollectBuyMintsFreshnessBlockerReport {
        event: "discovery_collect_buy_mints_freshness_blocker_report",
        config_path: config.config_path.display().to_string(),
        db_path: config.db_path.display().to_string(),
        now_utc: config.now,
        runtime_db_open_mode: "wal_aware_read_only",
        elapsed_ms: elapsed_ms(started),
        selector_full_scan_used: false,
        bounded_operator: true,
        publication_state,
        latest_persisted_freshness_capture,
        raw_window_persisted_health,
        observed_swaps_bounded_window,
        expected_metrics_window_start,
        latest_wallet_metrics_window_start,
        latest_wallet_metrics_window_matches_expected,
        persisted_rebuild,
        collect_buy_mints_checkpoint_exists,
        collect_buy_mints_appears_to_restart_from_fresh_each_cycle,
        fresh_zero_universe_persisted_evidence_consistent,
        cached_raw_window_summary_conflicts_with_persisted_truth,
        blocker_reason,
        next_safe_operator_action,
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

fn load_publication_state(conn: &Connection) -> Result<PublicationStateReport> {
    if !sqlite_table_exists(conn, "discovery_strategy_state")? {
        return Ok(PublicationStateReport {
            publication_runtime_mode: None,
            reason: None,
            updated_at: None,
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: None,
            published_wallet_count: 0,
        });
    }
    let raw: Option<(
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    )> = conn
        .query_row(
            "SELECT
                publication_runtime_mode,
                publication_reason,
                publication_last_published_at,
                publication_last_published_window_start,
                publication_scoring_source,
                publication_wallet_ids_json,
                updated_at
             FROM discovery_strategy_state
             WHERE id = 1",
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
        .context("failed loading discovery publication state")?;
    let Some((
        runtime_mode,
        reason,
        last_published_at_raw,
        last_published_window_start_raw,
        published_scoring_source,
        published_wallet_ids_json,
        updated_at_raw,
    )) = raw
    else {
        return Ok(PublicationStateReport {
            publication_runtime_mode: None,
            reason: None,
            updated_at: None,
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: None,
            published_wallet_count: 0,
        });
    };
    Ok(PublicationStateReport {
        publication_runtime_mode: Some(runtime_mode),
        reason: Some(reason),
        updated_at: Some(parse_ts(&updated_at_raw)?),
        last_published_at: last_published_at_raw.as_deref().map(parse_ts).transpose()?,
        last_published_window_start: last_published_window_start_raw
            .as_deref()
            .map(parse_ts)
            .transpose()?,
        published_scoring_source,
        published_wallet_count: published_wallet_ids_json
            .as_deref()
            .and_then(parse_string_array_json)
            .map(|wallet_ids| wallet_ids.len() as u64)
            .unwrap_or(0),
    })
}

fn load_latest_freshness_capture(
    conn: &Connection,
    now: DateTime<Utc>,
) -> Result<Option<FreshnessCaptureReport>> {
    if !sqlite_table_exists(conn, "discovery_wallet_freshness_history")? {
        return Ok(None);
    }
    let raw: Option<(
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
    )> = conn
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
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                ))
            },
        )
        .optional()
        .context("failed loading latest discovery wallet freshness capture")?;
    let Some((
        capture_id,
        captured_at_raw,
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
    )) = raw
    else {
        return Ok(None);
    };
    let captured_at = parse_ts(&captured_at_raw)?;
    let audit: Value = serde_json::from_str(&audit_json)
        .context("failed deserializing latest freshness audit_json")?;
    Ok(Some(FreshnessCaptureReport {
        capture_id,
        captured_at,
        age_seconds: now.signed_duration_since(captured_at).num_seconds().max(0),
        recent_cycles: recent_cycles.max(0) as u64,
        verdict,
        reason,
        publication_age_seconds,
        raw_truth_sufficient: raw_truth_sufficient != 0,
        raw_truth_reason,
        shadow_signal_verdict,
        shadow_signal_reason,
        published_wallet_count: parse_string_array_json(&published_wallet_ids_json)
            .map(|items| items.len() as u64)
            .unwrap_or(0),
        active_follow_wallet_count: parse_string_array_json(&active_follow_wallet_ids_json)
            .map(|items| items.len() as u64)
            .unwrap_or(0),
        current_raw_top_wallet_count: parse_string_array_json(&current_raw_top_wallet_ids_json)
            .map(|items| items.len() as u64)
            .unwrap_or(0),
        raw_truth_observed_swaps_loaded: audit
            .pointer("/raw_truth/observed_swaps_loaded")
            .and_then(Value::as_u64),
        raw_truth_eligible_wallet_count: audit
            .pointer("/raw_truth/eligible_wallet_count")
            .and_then(Value::as_u64),
        raw_truth_wallets_seen: audit
            .pointer("/raw_truth/wallets_seen")
            .and_then(Value::as_u64),
        raw_truth_distinct_buy_mint_count: audit
            .pointer("/raw_truth/observed_swaps_distinct_buy_mint_count")
            .and_then(Value::as_u64),
    }))
}

fn raw_window_persisted_health_report(
    latest_capture: Option<&FreshnessCaptureReport>,
    refresh_seconds: u64,
) -> RawWindowPersistedHealthReport {
    let fresh_under_refresh_gate =
        latest_capture.map(|capture| capture.age_seconds <= refresh_seconds as i64);
    let (raw_window_healthy, raw_window_health_reason) = match latest_capture {
        Some(capture) if capture.raw_truth_sufficient => (
            true,
            "collect_buy_mints_report_persisted_raw_truth_sufficient".to_string(),
        ),
        Some(capture) => (false, capture.raw_truth_reason.clone()),
        None => (
            false,
            "collect_buy_mints_report_no_persisted_freshness_capture".to_string(),
        ),
    };
    RawWindowPersistedHealthReport {
        persisted_capture_present: latest_capture.is_some(),
        raw_truth_sufficient: latest_capture.map(|capture| capture.raw_truth_sufficient),
        raw_truth_reason: latest_capture.map(|capture| capture.raw_truth_reason.clone()),
        persisted_capture_age_seconds: latest_capture.map(|capture| capture.age_seconds),
        fresh_under_refresh_gate,
        refresh_seconds,
        raw_window_healthy,
        raw_window_health_reason,
    }
}

fn load_observed_swaps_bounded_window(
    conn: &Connection,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    sample_limit: usize,
) -> Result<ObservedSwapsBoundedWindowReport> {
    if sample_limit == 0 || !sqlite_table_exists(conn, "observed_swaps")? {
        return Ok(ObservedSwapsBoundedWindowReport {
            table_present: sqlite_table_exists(conn, "observed_swaps")?,
            window_start,
            window_end: now,
            sample_limit,
            query_limit: sample_limit.saturating_add(1),
            sample_count: 0,
            sample_capped: false,
            sample_min_ts_utc: None,
            sample_max_ts_utc: None,
            window_min_ts_utc: None,
            window_max_ts_utc: None,
            sample_buy_count: 0,
            sample_sell_count: 0,
            sample_distinct_wallet_count: 0,
            sample_distinct_buy_mint_count: 0,
            full_table_count_used: false,
        });
    }
    let window_min_ts = query_observed_swaps_boundary_ts(conn, window_start, now, false)?;
    let window_max_ts = query_observed_swaps_boundary_ts(conn, window_start, now, true)?;
    let query_limit = sample_limit.saturating_add(1).min(i64::MAX as usize) as i64;
    let mut stmt = conn
        .prepare(
            "SELECT ts, wallet_id, token_in, token_out
             FROM observed_swaps
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
    Ok(ObservedSwapsBoundedWindowReport {
        table_present: true,
        window_start,
        window_end: now,
        sample_limit,
        query_limit: sample_limit.saturating_add(1),
        sample_count,
        sample_capped: fetched > sample_limit,
        sample_min_ts_utc: sample_min_ts,
        sample_max_ts_utc: sample_max_ts,
        window_min_ts_utc: window_min_ts,
        window_max_ts_utc: window_max_ts,
        sample_buy_count: buy_count,
        sample_sell_count: sell_count,
        sample_distinct_wallet_count: wallets.len() as u64,
        sample_distinct_buy_mint_count: buy_mints.len() as u64,
        full_table_count_used: false,
    })
}

fn query_observed_swaps_boundary_ts(
    conn: &Connection,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    descending: bool,
) -> Result<Option<DateTime<Utc>>> {
    let direction = if descending { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT ts
         FROM observed_swaps
         WHERE ts >= ?1 AND ts <= ?2
         ORDER BY ts {direction}, slot {direction}, signature {direction}
         LIMIT 1"
    );
    let raw: Option<String> = conn
        .query_row(&sql, [window_start.to_rfc3339(), now.to_rfc3339()], |row| {
            row.get(0)
        })
        .optional()
        .context("failed loading observed_swaps bounded boundary timestamp")?;
    raw.as_deref().map(parse_ts).transpose()
}

fn load_latest_wallet_metrics_window_start(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
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
        .context("failed loading latest wallet_metrics window_start")?;
    raw.as_deref().map(parse_ts).transpose()
}

fn load_persisted_rebuild(conn: &Connection) -> Result<Option<PersistedRebuildReport>> {
    if !sqlite_table_exists(conn, "discovery_persisted_rebuild_state")? {
        return Ok(None);
    }
    let raw: Option<(
        String,
        String,
        String,
        String,
        Option<String>,
        Option<i64>,
        Option<String>,
        i64,
        i64,
        i64,
        i64,
        i64,
        String,
        String,
        String,
    )> = conn
        .query_row(
            "SELECT
                phase,
                window_start,
                horizon_end,
                metrics_window_start,
                phase_cursor_ts,
                phase_cursor_slot,
                phase_cursor_signature,
                prepass_rows_processed,
                prepass_pages_processed,
                replay_rows_processed,
                replay_pages_processed,
                chunks_completed,
                state_json,
                started_at,
                updated_at
             FROM discovery_persisted_rebuild_state
             WHERE id = 1",
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
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                ))
            },
        )
        .optional()
        .context("failed loading discovery persisted rebuild state")?;
    let Some((
        phase,
        window_start_raw,
        horizon_end_raw,
        metrics_window_start_raw,
        phase_cursor_ts_raw,
        phase_cursor_slot_raw,
        phase_cursor_signature,
        prepass_rows_processed,
        prepass_pages_processed,
        replay_rows_processed,
        replay_pages_processed,
        chunks_completed,
        state_json,
        started_at_raw,
        updated_at_raw,
    )) = raw
    else {
        return Ok(None);
    };
    let state_json_bytes = state_json.len();
    let payload: Option<Value> = serde_json::from_str(&state_json).ok();
    let collect_buy_mints = payload
        .as_ref()
        .map(collect_buy_mints_checkpoint_report)
        .unwrap_or_else(CollectBuyMintsCheckpointReport::unparsed);
    Ok(Some(PersistedRebuildReport {
        phase,
        window_start: parse_ts(&window_start_raw)?,
        horizon_end: parse_ts(&horizon_end_raw)?,
        metrics_window_start: parse_ts(&metrics_window_start_raw)?,
        phase_cursor: parse_optional_cursor(
            phase_cursor_ts_raw,
            phase_cursor_slot_raw,
            phase_cursor_signature,
        )?,
        prepass_rows_processed: nonnegative_i64_to_u64(prepass_rows_processed),
        prepass_pages_processed: nonnegative_i64_to_u64(prepass_pages_processed),
        replay_rows_processed: nonnegative_i64_to_u64(replay_rows_processed),
        replay_pages_processed: nonnegative_i64_to_u64(replay_pages_processed),
        chunks_completed: nonnegative_i64_to_u64(chunks_completed),
        started_at: parse_ts(&started_at_raw)?,
        updated_at: parse_ts(&updated_at_raw)?,
        state_json_bytes,
        state_json_parse_ok: payload.is_some(),
        collect_buy_mints,
    }))
}

fn collect_buy_mints_checkpoint_report(payload: &Value) -> CollectBuyMintsCheckpointReport {
    CollectBuyMintsCheckpointReport {
        mode: payload
            .get("collect_buy_mints_mode")
            .and_then(Value::as_str)
            .map(normalize_collect_buy_mints_mode),
        cursor_token: payload
            .get("collect_buy_mints_cursor_token")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        prepass_complete: payload
            .get("collect_buy_mints_prepass_complete")
            .and_then(Value::as_bool),
        unique_buy_mints_count: payload
            .get("unique_buy_mints")
            .and_then(Value::as_array)
            .map(|items| items.len() as u64),
        buy_mint_counts_count: payload
            .get("buy_mint_counts")
            .and_then(Value::as_object)
            .map(|items| items.len() as u64),
        reconcile_source_window_start: payload
            .get("collect_buy_mints_reconcile_source_window_start")
            .and_then(Value::as_str)
            .map(parse_ts)
            .transpose()
            .ok()
            .flatten(),
        reconcile_source_horizon_end: payload
            .get("collect_buy_mints_reconcile_source_horizon_end")
            .and_then(Value::as_str)
            .map(parse_ts)
            .transpose()
            .ok()
            .flatten(),
        reconcile_expired_head_cursor: payload
            .get("collect_buy_mints_reconcile_expired_head_cursor")
            .and_then(parse_cursor_value),
        reconcile_new_tail_cursor: payload
            .get("collect_buy_mints_reconcile_new_tail_cursor")
            .and_then(parse_cursor_value),
        reconcile_expired_head_cursor_token: payload
            .get("collect_buy_mints_reconcile_expired_head_cursor_token")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        reconcile_new_tail_cursor_token: payload
            .get("collect_buy_mints_reconcile_new_tail_cursor_token")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        reconcile_expired_head_pending_mints: payload
            .get("collect_buy_mints_reconcile_expired_head_pending_mints")
            .and_then(Value::as_array)
            .map(|items| items.len() as u64),
        reconcile_new_tail_slice_end_token: payload
            .get("collect_buy_mints_reconcile_new_tail_slice_end_token")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        reconcile_new_tail_pending_mints: payload
            .get("collect_buy_mints_reconcile_new_tail_pending_mints")
            .and_then(Value::as_array)
            .map(|items| items.len() as u64),
    }
}

impl CollectBuyMintsCheckpointReport {
    fn unparsed() -> Self {
        Self {
            mode: None,
            cursor_token: None,
            prepass_complete: None,
            unique_buy_mints_count: None,
            buy_mint_counts_count: None,
            reconcile_source_window_start: None,
            reconcile_source_horizon_end: None,
            reconcile_expired_head_cursor: None,
            reconcile_new_tail_cursor: None,
            reconcile_expired_head_cursor_token: None,
            reconcile_new_tail_cursor_token: None,
            reconcile_expired_head_pending_mints: None,
            reconcile_new_tail_slice_end_token: None,
            reconcile_new_tail_pending_mints: None,
        }
    }
}

fn classify_blocker_reason(
    publication_state: &PublicationStateReport,
    raw_window: &RawWindowPersistedHealthReport,
    persisted_rebuild: Option<&PersistedRebuildReport>,
    checkpoint_exists: bool,
    restart_from_fresh_each_cycle: bool,
    fresh_zero_universe_consistent: bool,
    cached_conflict: bool,
) -> String {
    if let Some(rebuild) = persisted_rebuild {
        if rebuild.phase == "collect_buy_mints" {
            if restart_from_fresh_each_cycle {
                return "collect_buy_mints_checkpoint_not_advancing".to_string();
            }
            if rebuild.collect_buy_mints.mode.as_deref() == Some("fresh_scan")
                && rebuild.collect_buy_mints.prepass_complete != Some(true)
            {
                return "collect_buy_mints_fresh_scan_incomplete".to_string();
            }
        }
    }

    if raw_window.fresh_under_refresh_gate == Some(false) {
        return "raw_window_persisted_evidence_stale".to_string();
    }
    if fresh_zero_universe_consistent {
        return "raw_window_zero_publishable_universe_confirmed".to_string();
    }
    if cached_conflict {
        return "cached_raw_window_summary_conflicts_with_persisted_truth".to_string();
    }
    if !checkpoint_exists {
        return "collect_buy_mints_checkpoint_missing".to_string();
    }
    if publication_state.reason.is_none() || raw_window.raw_truth_sufficient.is_none() {
        return "unproven_missing_evidence".to_string();
    }
    "unproven_missing_evidence".to_string()
}

fn next_safe_operator_action(blocker_reason: &str) -> String {
    match blocker_reason {
        "collect_buy_mints_fresh_scan_incomplete" => {
            "inspect persisted collect_buy_mints cursor/prepass fields and fix bounded resume/persistence if the cursor is not advancing; do not change selector thresholds".to_string()
        }
        "collect_buy_mints_checkpoint_missing" => {
            "prove why discovery_persisted_rebuild_state is absent before the collect_buy_mints phase can persist progress; do not run a selector full scan from this report".to_string()
        }
        "collect_buy_mints_checkpoint_not_advancing" => {
            "target checkpoint persistence/resume for collect_buy_mints fresh_scan because the report sees a collect phase without a durable cursor/prepass advance".to_string()
        }
        "raw_window_persisted_evidence_stale" => {
            "refresh or repair the persisted wallet freshness capture path; do not infer raw-window health from cached publication summary".to_string()
        }
        "cached_raw_window_summary_conflicts_with_persisted_truth" => {
            "audit cached raw-window publication summary versus persisted freshness truth; do not treat cached zero-universe as current raw-window proof".to_string()
        }
        "raw_window_zero_publishable_universe_confirmed" => {
            "current persisted freshness evidence confirms a fail-closed zero publishable universe; investigate selector inputs from bounded persisted metrics without changing thresholds".to_string()
        }
        _ => "collect more read-only persisted evidence before changing selector/scoring behavior".to_string(),
    }
}

fn collect_buy_mints_restart_from_fresh_each_cycle(
    persisted_rebuild: Option<&PersistedRebuildReport>,
) -> bool {
    let Some(rebuild) = persisted_rebuild else {
        return false;
    };
    rebuild.phase == "collect_buy_mints"
        && rebuild.collect_buy_mints.mode.as_deref() == Some("fresh_scan")
        && rebuild.collect_buy_mints.prepass_complete != Some(true)
        && rebuild.collect_buy_mints.cursor_token.is_none()
        && rebuild.prepass_rows_processed == 0
        && rebuild.prepass_pages_processed == 0
}

fn cached_raw_window_summary_conflicts_with_persisted_truth(
    publication_state: &PublicationStateReport,
    raw_window: &RawWindowPersistedHealthReport,
) -> bool {
    publication_state.reason.as_deref() == Some(RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON)
        && raw_window.raw_truth_sufficient != Some(true)
}

fn fresh_zero_universe_persisted_evidence_consistent(
    publication_state: &PublicationStateReport,
    latest_capture: Option<&FreshnessCaptureReport>,
    raw_window: &RawWindowPersistedHealthReport,
) -> bool {
    let Some(capture) = latest_capture else {
        return false;
    };
    publication_state.reason.as_deref() == Some(RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON)
        && raw_window.fresh_under_refresh_gate == Some(true)
        && !capture.raw_truth_sufficient
        && capture.raw_truth_reason == RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        && capture.published_wallet_count == 0
        && capture.active_follow_wallet_count == 0
        && capture.current_raw_top_wallet_count == 0
        && capture
            .raw_truth_observed_swaps_loaded
            .is_some_and(|count| count > 0)
        && capture.raw_truth_eligible_wallet_count == Some(0)
}

fn parse_optional_cursor(
    ts_raw: Option<String>,
    slot_raw: Option<i64>,
    signature: Option<String>,
) -> Result<Option<ReportCursor>> {
    match (ts_raw, slot_raw, signature) {
        (None, None, None) => Ok(None),
        (Some(ts_raw), Some(slot_raw), Some(signature)) => Ok(Some(ReportCursor {
            ts_utc: parse_ts(&ts_raw)?,
            slot: nonnegative_i64_to_u64(slot_raw),
            signature,
        })),
        _ => bail!("partial cursor fields in discovery_persisted_rebuild_state"),
    }
}

fn parse_cursor_value(value: &Value) -> Option<ReportCursor> {
    Some(ReportCursor {
        ts_utc: parse_ts(value.get("ts_utc")?.as_str()?).ok()?,
        slot: value.get("slot")?.as_u64()?,
        signature: value.get("signature")?.as_str()?.to_string(),
    })
}

fn normalize_collect_buy_mints_mode(raw: &str) -> String {
    match raw {
        "FreshScan" | "fresh_scan" => "fresh_scan",
        "ReconcileExpiredHead" | "reconcile_expired_head" => "reconcile_expired_head",
        "ReconcileNewTail" | "reconcile_new_tail" => "reconcile_new_tail",
        other => other,
    }
    .to_string()
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

fn is_sol_buy(token_in: &str, token_out: &str) -> bool {
    token_in == sol_mint() && token_out != sol_mint()
}

fn is_sol_sell(token_in: &str, token_out: &str) -> bool {
    token_out == sol_mint() && token_in != sol_mint()
}

fn sol_mint() -> &'static str {
    "So11111111111111111111111111111111111111112"
}

fn nonnegative_i64_to_u64(value: i64) -> u64 {
    value.max(0) as u64
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    const NOW: &str = "2026-04-24T12:17:45Z";

    #[test]
    fn collect_buy_mints_fresh_scan_incomplete_is_reported() -> Result<()> {
        let fixture = TestFixture::new(TestCase::FreshScanIncomplete)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(
            report.blocker_reason,
            "collect_buy_mints_fresh_scan_incomplete"
        );
        assert!(report.collect_buy_mints_checkpoint_exists);
        assert!(!report.collect_buy_mints_appears_to_restart_from_fresh_each_cycle);
        assert_eq!(
            report
                .persisted_rebuild
                .as_ref()
                .and_then(|state| state.collect_buy_mints.cursor_token.clone())
                .as_deref(),
            Some("TokenCursor042")
        );
        assert!(!report.selector_full_scan_used);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn checkpoint_missing_is_reported() -> Result<()> {
        let fixture = TestFixture::new(TestCase::CheckpointMissing)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(
            report.blocker_reason,
            "collect_buy_mints_checkpoint_missing"
        );
        assert!(!report.collect_buy_mints_checkpoint_exists);
        assert!(report.persisted_rebuild.is_none());
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn checkpoint_not_advancing_is_reported() -> Result<()> {
        let fixture = TestFixture::new(TestCase::CheckpointNotAdvancing)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(
            report.blocker_reason,
            "collect_buy_mints_checkpoint_not_advancing"
        );
        assert!(report.collect_buy_mints_checkpoint_exists);
        assert!(report.collect_buy_mints_appears_to_restart_from_fresh_each_cycle);
        Ok(())
    }

    #[test]
    fn stale_persisted_freshness_is_reported_without_selector_claim() -> Result<()> {
        let fixture = TestFixture::new(TestCase::StaleFreshness)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(report.blocker_reason, "raw_window_persisted_evidence_stale");
        assert_eq!(
            report.raw_window_persisted_health.fresh_under_refresh_gate,
            Some(false)
        );
        assert!(!report.selector_full_scan_used);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn fresh_consistent_zero_universe_evidence_is_not_cached_conflict() -> Result<()> {
        let fixture = TestFixture::new(TestCase::FreshZeroUniverseConsistent)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(
            report.blocker_reason,
            "raw_window_zero_publishable_universe_confirmed"
        );
        assert!(report.fresh_zero_universe_persisted_evidence_consistent);
        assert!(!report.cached_raw_window_summary_conflicts_with_persisted_truth);
        assert_eq!(
            report.publication_state.reason.as_deref(),
            Some(RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON)
        );
        let capture = report
            .latest_persisted_freshness_capture
            .as_ref()
            .expect("zero-universe freshness capture should be present");
        assert!(!capture.raw_truth_sufficient);
        assert_eq!(
            capture.raw_truth_reason,
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        );
        assert_eq!(capture.published_wallet_count, 0);
        assert_eq!(capture.active_follow_wallet_count, 0);
        assert_eq!(capture.current_raw_top_wallet_count, 0);
        assert!(capture
            .raw_truth_observed_swaps_loaded
            .is_some_and(|count| count > 0));
        assert_eq!(capture.raw_truth_eligible_wallet_count, Some(0));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn stale_zero_universe_evidence_still_reports_stale() -> Result<()> {
        let fixture = TestFixture::new(TestCase::StaleZeroUniverse)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(report.blocker_reason, "raw_window_persisted_evidence_stale");
        assert!(!report.fresh_zero_universe_persisted_evidence_consistent);
        assert_eq!(
            report.raw_window_persisted_health.fresh_under_refresh_gate,
            Some(false)
        );
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn mismatched_zero_universe_publication_and_capture_reason_is_reported_as_conflict(
    ) -> Result<()> {
        let fixture = TestFixture::new(TestCase::CachedConflict)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(
            report.blocker_reason,
            "cached_raw_window_summary_conflicts_with_persisted_truth"
        );
        assert!(!report.fresh_zero_universe_persisted_evidence_consistent);
        assert!(report.cached_raw_window_summary_conflicts_with_persisted_truth);
        assert_eq!(
            report.publication_state.reason.as_deref(),
            Some(RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON)
        );
        assert_eq!(
            report.raw_window_persisted_health.raw_truth_sufficient,
            Some(false)
        );
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn observed_swaps_sample_is_bounded() -> Result<()> {
        let fixture = TestFixture::new(TestCase::ManyObservedRows)?;
        let report = run(&fixture.config(1))?;
        assert_eq!(report.observed_swaps_bounded_window.query_limit, 2);
        assert_eq!(report.observed_swaps_bounded_window.sample_count, 1);
        assert!(report.observed_swaps_bounded_window.sample_capped);
        assert!(!report.observed_swaps_bounded_window.full_table_count_used);
        Ok(())
    }

    #[test]
    fn metrics_window_match_uses_bucketed_now() -> Result<()> {
        let fixture = TestFixture::new(TestCase::FreshScanIncomplete)?;
        let report = run(&fixture.config(DEFAULT_SAMPLE_LIMIT))?;
        assert_eq!(
            report.expected_metrics_window_start,
            parse_ts("2026-04-19T12:00:00Z")?
        );
        assert_eq!(
            report.latest_wallet_metrics_window_start,
            Some(parse_ts("2026-04-19T12:00:00Z")?)
        );
        assert!(report.latest_wallet_metrics_window_matches_expected);
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum TestCase {
        FreshScanIncomplete,
        CheckpointMissing,
        CheckpointNotAdvancing,
        StaleFreshness,
        FreshZeroUniverseConsistent,
        StaleZeroUniverse,
        CachedConflict,
        ManyObservedRows,
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
            let db_path = tempdir.path().join("collect-buy-mints-report.sqlite");
            let config_path = tempdir.path().join("collect-buy-mints-report.toml");
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
                "scoring_window_days = 2\n",
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
        let conn = Connection::open(path)
            .with_context(|| format!("failed creating sqlite fixture {}", path.display()))?;
        create_fixture_tables(&conn)?;
        seed_publication_state(&conn, now, test_case)?;
        seed_freshness_capture(&conn, now, test_case)?;
        seed_observed_swaps(&conn, now, test_case)?;
        seed_wallet_metrics(&conn, now)?;
        match test_case {
            TestCase::FreshScanIncomplete | TestCase::ManyObservedRows => {
                seed_rebuild_state(&conn, now, "TokenCursor042", 100, 2)?
            }
            TestCase::CheckpointNotAdvancing => seed_rebuild_state(&conn, now, "", 0, 0)?,
            TestCase::CheckpointMissing
            | TestCase::StaleFreshness
            | TestCase::FreshZeroUniverseConsistent
            | TestCase::StaleZeroUniverse
            | TestCase::CachedConflict => {}
        }
        Ok(())
    }

    fn create_fixture_tables(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "CREATE TABLE discovery_strategy_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                publication_runtime_mode TEXT NOT NULL,
                publication_reason TEXT NOT NULL,
                publication_last_published_at TEXT,
                publication_last_published_window_start TEXT,
                publication_scoring_source TEXT,
                publication_wallet_ids_json TEXT,
                updated_at TEXT NOT NULL
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
            CREATE TABLE observed_swaps (
                signature TEXT PRIMARY KEY,
                wallet_id TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                slot INTEGER NOT NULL,
                ts TEXT NOT NULL
            );
            CREATE TABLE wallet_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_id TEXT NOT NULL,
                window_start TEXT NOT NULL
            );
            CREATE TABLE discovery_persisted_rebuild_state (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                phase TEXT NOT NULL,
                window_start TEXT NOT NULL,
                horizon_end TEXT NOT NULL,
                metrics_window_start TEXT NOT NULL,
                phase_cursor_ts TEXT,
                phase_cursor_slot INTEGER,
                phase_cursor_signature TEXT,
                prepass_rows_processed INTEGER NOT NULL DEFAULT 0,
                prepass_pages_processed INTEGER NOT NULL DEFAULT 0,
                replay_rows_processed INTEGER NOT NULL DEFAULT 0,
                replay_pages_processed INTEGER NOT NULL DEFAULT 0,
                chunks_completed INTEGER NOT NULL DEFAULT 0,
                state_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
        )?;
        Ok(())
    }

    fn seed_publication_state(
        conn: &Connection,
        now: DateTime<Utc>,
        test_case: TestCase,
    ) -> Result<()> {
        let reason = if matches!(
            test_case,
            TestCase::FreshZeroUniverseConsistent
                | TestCase::StaleZeroUniverse
                | TestCase::CachedConflict
        ) {
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        } else {
            "raw_window"
        };
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
             ) VALUES (1, 'fail_closed', ?1, ?2, ?3, 'raw_window', ?4, ?5)",
            params![
                reason,
                (now - Duration::minutes(20)).to_rfc3339(),
                expected_metrics_window_start(now, 1_800, 5).to_rfc3339(),
                serde_json::to_string(&Vec::<String>::new())?,
                now.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn seed_freshness_capture(
        conn: &Connection,
        now: DateTime<Utc>,
        test_case: TestCase,
    ) -> Result<()> {
        let captured_at = if matches!(
            test_case,
            TestCase::StaleFreshness | TestCase::StaleZeroUniverse
        ) {
            now - Duration::minutes(30)
        } else {
            now - Duration::minutes(1)
        };
        let raw_truth_sufficient = !matches!(
            test_case,
            TestCase::CheckpointMissing
                | TestCase::FreshZeroUniverseConsistent
                | TestCase::StaleZeroUniverse
                | TestCase::CachedConflict
        );
        let raw_truth_reason = if matches!(
            test_case,
            TestCase::FreshZeroUniverseConsistent | TestCase::StaleZeroUniverse
        ) {
            RAW_WINDOW_ZERO_PUBLISHABLE_UNIVERSE_REASON
        } else if raw_truth_sufficient {
            "full_scoring_window_raw_truth_available"
        } else {
            "collect_buy_mints_fresh_scan_incomplete"
        };
        let audit_json = serde_json::json!({
            "raw_truth": {
                "observed_swaps_loaded": 123u64,
                "eligible_wallet_count": 0u64,
                "wallets_seen": 7715u64,
                "observed_swaps_distinct_buy_mint_count": 42u64
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
             ) VALUES (?1, 1, 'insufficient_evidence', 'fixture', 0, ?2, ?3, 'insufficient_evidence', 'fixture', ?4, ?5, ?6, ?7, '{}')",
            params![
                captured_at.to_rfc3339(),
                if raw_truth_sufficient { 1 } else { 0 },
                raw_truth_reason,
                serde_json::to_string(&Vec::<String>::new())?,
                serde_json::to_string(&Vec::<String>::new())?,
                serde_json::to_string(&Vec::<String>::new())?,
                serde_json::to_string(&audit_json)?,
            ],
        )?;
        Ok(())
    }

    fn seed_observed_swaps(
        conn: &Connection,
        now: DateTime<Utc>,
        test_case: TestCase,
    ) -> Result<()> {
        let count = if matches!(test_case, TestCase::ManyObservedRows) {
            3
        } else {
            1
        };
        let window_start = expected_metrics_window_start(now, 1_800, 5);
        for index in 0..count {
            conn.execute(
                "INSERT INTO observed_swaps(signature, wallet_id, token_in, token_out, slot, ts)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    format!("sig-{index}"),
                    format!("wallet-{index}"),
                    sol_mint(),
                    format!("Token{index}"),
                    index as i64 + 1,
                    (window_start + Duration::minutes(index as i64 + 1)).to_rfc3339(),
                ],
            )?;
        }
        Ok(())
    }

    fn seed_wallet_metrics(conn: &Connection, now: DateTime<Utc>) -> Result<()> {
        conn.execute(
            "INSERT INTO wallet_metrics(wallet_id, window_start) VALUES ('wallet-1', ?1)",
            [expected_metrics_window_start(now, 1_800, 5).to_rfc3339()],
        )?;
        Ok(())
    }

    fn seed_rebuild_state(
        conn: &Connection,
        now: DateTime<Utc>,
        cursor_token: &str,
        prepass_rows: i64,
        prepass_pages: i64,
    ) -> Result<()> {
        let cursor_token_json = if cursor_token.is_empty() {
            Value::Null
        } else {
            Value::String(cursor_token.to_string())
        };
        let state_json = serde_json::json!({
            "unique_buy_mints": ["TokenA"],
            "buy_mint_counts": {"TokenA": 1},
            "collect_buy_mints_cursor_token": cursor_token_json,
            "collect_buy_mints_prepass_complete": false,
            "collect_buy_mints_mode": "FreshScan",
            "token_quality_cache": {},
            "by_wallet": {},
            "token_states": {},
            "token_recent_sol_trades": {},
            "pending_rug_checks": [],
            "token_pending_buy_starts": {},
            "completed_snapshots": []
        });
        let window_start = expected_metrics_window_start(now, 1_800, 5);
        conn.execute(
            "INSERT INTO discovery_persisted_rebuild_state(
                id,
                phase,
                window_start,
                horizon_end,
                metrics_window_start,
                phase_cursor_ts,
                phase_cursor_slot,
                phase_cursor_signature,
                prepass_rows_processed,
                prepass_pages_processed,
                replay_rows_processed,
                replay_pages_processed,
                chunks_completed,
                state_json,
                started_at,
                updated_at
             ) VALUES (1, 'collect_buy_mints', ?1, ?2, ?1, NULL, NULL, NULL, ?3, ?4, 0, 0, 1, ?5, ?6, ?7)",
            params![
                window_start.to_rfc3339(),
                now.to_rfc3339(),
                prepass_rows,
                prepass_pages,
                serde_json::to_string(&state_json)?,
                (now - Duration::minutes(10)).to_rfc3339(),
                now.to_rfc3339(),
            ],
        )?;
        Ok(())
    }
}
