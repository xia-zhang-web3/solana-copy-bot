use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use copybot_config::load_from_path;
use copybot_storage::DiscoveryRuntimeCursor;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_discovery_scoring_fact_writer_blocker_report --config <path> [--now <rfc3339>] [--json]";

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

const BLOCKER_WRITE_DISABLED: &str = "discovery_scoring_aggregates_write_disabled";
const BLOCKER_READ_DISABLED: &str = "discovery_scoring_aggregates_read_disabled";
const BLOCKER_MATERIALIZATION_GAP_PENDING: &str = "discovery_scoring_materialization_gap_pending";
const BLOCKER_TABLES_EMPTY_DESPITE_ENABLED_WRITER: &str =
    "discovery_scoring_tables_empty_despite_enabled_writer";
const BLOCKER_TABLES_PRESENT_BUT_METRICS_STILL_ZERO: &str =
    "discovery_scoring_tables_present_but_metrics_still_zero";
const BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE: &str = "unproven_missing_runtime_evidence";

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
    config_path: PathBuf,
    now: DateTime<Utc>,
    json: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
struct RuntimeEvidence {
    observed_swaps_total_rows: Option<u64>,
    observed_swaps_window_rows: Option<u64>,
    observed_swaps_window_buy_rows: Option<u64>,
    observed_swaps_window_sell_rows: Option<u64>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_total_rows: Option<u64>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    latest_wallet_metrics_window_start: Option<DateTime<Utc>>,
    latest_wallet_metrics_rows: Option<u64>,
    latest_wallet_metrics_max_score: Option<f64>,
    latest_wallet_metrics_max_trades: Option<u64>,
    latest_wallet_metrics_max_buy_total: Option<u64>,
    latest_wallet_metrics_max_tradable_ratio: Option<f64>,
    wallet_scoring_days_rows: Option<u64>,
    wallet_scoring_buy_facts_rows: Option<u64>,
    wallet_scoring_close_facts_rows: Option<u64>,
    wallet_scoring_open_lots_rows: Option<u64>,
    wallet_scoring_carryover_lots_rows: Option<u64>,
    discovery_scoring_covered_since: Option<DateTime<Utc>>,
    discovery_scoring_covered_through: Option<DateTime<Utc>>,
    discovery_scoring_materialization_gap_cursor: Option<DiscoveryRuntimeCursor>,
}

#[derive(Debug, Clone, Serialize)]
struct ScoringFactWriterBlockerReport {
    config_path: String,
    db_path: String,
    production_green: bool,
    scoring_aggregates_write_enabled: bool,
    scoring_aggregates_enabled: bool,
    observed_swaps_total_rows: Option<u64>,
    observed_swaps_window_rows: Option<u64>,
    observed_swaps_window_buy_rows: Option<u64>,
    observed_swaps_window_sell_rows: Option<u64>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_total_rows: Option<u64>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    latest_wallet_metrics_window_start: Option<DateTime<Utc>>,
    latest_wallet_metrics_rows: Option<u64>,
    latest_wallet_metrics_max_score: Option<f64>,
    latest_wallet_metrics_max_trades: Option<u64>,
    latest_wallet_metrics_max_buy_total: Option<u64>,
    latest_wallet_metrics_max_tradable_ratio: Option<f64>,
    wallet_scoring_days_rows: Option<u64>,
    wallet_scoring_buy_facts_rows: Option<u64>,
    wallet_scoring_close_facts_rows: Option<u64>,
    wallet_scoring_open_lots_rows: Option<u64>,
    wallet_scoring_carryover_lots_rows: Option<u64>,
    discovery_scoring_covered_since: Option<DateTime<Utc>>,
    discovery_scoring_covered_through: Option<DateTime<Utc>>,
    discovery_scoring_materialization_gap_cursor: Option<DiscoveryRuntimeCursor>,
    blocker_reason: String,
    runtime_evidence_errors: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct ConfigFlags {
    scoring_aggregates_write_enabled: bool,
    scoring_aggregates_enabled: bool,
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
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?));
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        now: now.unwrap_or_else(Utc::now),
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

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn run(config: &Config) -> Result<ScoringFactWriterBlockerReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(&config.config_path, &loaded_config.sqlite.path);
    let flags = ConfigFlags {
        scoring_aggregates_write_enabled: loaded_config.discovery.scoring_aggregates_write_enabled,
        scoring_aggregates_enabled: loaded_config.discovery.scoring_aggregates_enabled,
    };
    let window_start =
        config.now - Duration::days(i64::from(loaded_config.discovery.scoring_window_days));

    let (evidence, runtime_evidence_errors) =
        load_runtime_evidence(&db_path, window_start, config.now);
    Ok(build_report(
        config.config_path.display().to_string(),
        db_path.display().to_string(),
        flags,
        evidence,
        runtime_evidence_errors,
    ))
}

fn load_runtime_evidence(
    db_path: &Path,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> (RuntimeEvidence, Vec<String>) {
    let mut evidence = RuntimeEvidence::default();
    let mut errors = Vec::new();
    let conn = match open_read_only_connection(db_path) {
        Ok(conn) => conn,
        Err(error) => {
            errors.push(format!(
                "failed_opening_configured_sqlite_read_only:{}",
                compact_error(error)
            ));
            return (evidence, errors);
        }
    };

    errors.push(
        "observed_swaps_total_rows_unavailable:observed_swaps_full_table_count_unbounded_scan_avoided"
            .to_string(),
    );
    errors.push(
        "observed_swaps_max_ts_utc_unavailable:observed_swaps_full_table_max_unbounded_scan_avoided"
            .to_string(),
    );

    match count_observed_swaps_in_window(&conn, window_start, now) {
        Ok(counts) => {
            evidence.observed_swaps_window_rows = Some(counts.total_rows);
            evidence.observed_swaps_window_buy_rows = Some(counts.buy_rows);
            evidence.observed_swaps_window_sell_rows = Some(counts.sell_rows);
        }
        Err(error) => errors.push(format!(
            "observed_swaps_window_counts_unavailable:{}",
            compact_error(error)
        )),
    }

    match wallet_activity_days_coverage_snapshot(&conn) {
        Ok(snapshot) => {
            evidence.wallet_activity_days_total_rows = Some(snapshot.row_count);
            evidence.wallet_activity_days_max_day_utc = snapshot.covered_through_day_utc;
        }
        Err(error) => errors.push(format!(
            "wallet_activity_days_coverage_unavailable:{}",
            compact_error(error)
        )),
    }

    match latest_wallet_metrics_window_start(&conn) {
        Ok(Some(window_start)) => {
            evidence.latest_wallet_metrics_window_start = Some(window_start);
            match wallet_metrics_aggregate_for_window(&conn, window_start) {
                Ok(aggregate) => {
                    evidence.latest_wallet_metrics_rows = Some(aggregate.row_count);
                    evidence.latest_wallet_metrics_max_score = aggregate.max_score;
                    evidence.latest_wallet_metrics_max_trades = aggregate.max_trades;
                    evidence.latest_wallet_metrics_max_buy_total = aggregate.max_buy_total;
                    evidence.latest_wallet_metrics_max_tradable_ratio =
                        aggregate.max_tradable_ratio;
                }
                Err(error) => errors.push(format!(
                    "latest_wallet_metrics_aggregate_unavailable:{}",
                    compact_error(error)
                )),
            }
        }
        Ok(None) => {}
        Err(error) => errors.push(format!(
            "latest_wallet_metrics_window_unavailable:{}",
            compact_error(error)
        )),
    }

    match count_sqlite_table_read_only(&conn, "wallet_scoring_days") {
        Ok(row_count) => evidence.wallet_scoring_days_rows = Some(row_count),
        Err(error) => errors.push(format!(
            "wallet_scoring_days_count_unavailable:{}",
            compact_error(error)
        )),
    }
    match count_sqlite_table_read_only(&conn, "wallet_scoring_buy_facts") {
        Ok(row_count) => evidence.wallet_scoring_buy_facts_rows = Some(row_count),
        Err(error) => errors.push(format!(
            "wallet_scoring_buy_facts_count_unavailable:{}",
            compact_error(error)
        )),
    }
    match count_sqlite_table_read_only(&conn, "wallet_scoring_close_facts") {
        Ok(row_count) => evidence.wallet_scoring_close_facts_rows = Some(row_count),
        Err(error) => errors.push(format!(
            "wallet_scoring_close_facts_count_unavailable:{}",
            compact_error(error)
        )),
    }
    match count_sqlite_table_read_only(&conn, "wallet_scoring_open_lots") {
        Ok(row_count) => evidence.wallet_scoring_open_lots_rows = Some(row_count),
        Err(error) => errors.push(format!(
            "wallet_scoring_open_lots_count_unavailable:{}",
            compact_error(error)
        )),
    }
    match count_sqlite_table_read_only(&conn, "wallet_scoring_carryover_lots") {
        Ok(row_count) => evidence.wallet_scoring_carryover_lots_rows = Some(row_count),
        Err(error) => errors.push(format!(
            "wallet_scoring_carryover_lots_count_unavailable:{}",
            compact_error(error)
        )),
    }

    match load_discovery_scoring_state_ts(&conn, "covered_since_ts") {
        Ok(value) => evidence.discovery_scoring_covered_since = value,
        Err(error) => errors.push(format!(
            "discovery_scoring_covered_since_unavailable:{}",
            compact_error(error)
        )),
    }
    match load_discovery_scoring_state_ts(&conn, "covered_through_ts") {
        Ok(value) => evidence.discovery_scoring_covered_through = value,
        Err(error) => errors.push(format!(
            "discovery_scoring_covered_through_unavailable:{}",
            compact_error(error)
        )),
    }
    match load_discovery_scoring_materialization_gap_cursor(&conn) {
        Ok(value) => evidence.discovery_scoring_materialization_gap_cursor = value,
        Err(error) => errors.push(format!(
            "discovery_scoring_materialization_gap_cursor_unavailable:{}",
            compact_error(error)
        )),
    }

    (evidence, errors)
}

fn count_sqlite_table_read_only(conn: &Connection, table_name: &str) -> Result<u64> {
    const ALLOWED_TABLES: &[&str] = &[
        "wallet_scoring_days",
        "wallet_scoring_buy_facts",
        "wallet_scoring_close_facts",
        "wallet_scoring_open_lots",
        "wallet_scoring_carryover_lots",
    ];
    if !ALLOWED_TABLES.contains(&table_name) {
        bail!("unsupported scoring table count: {table_name}");
    }

    let sql = format!("SELECT COUNT(*) FROM {table_name}");
    let count: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .with_context(|| format!("failed querying {table_name} COUNT(*)"))?;
    Ok(count.max(0) as u64)
}

#[derive(Debug, Clone, Copy, Default)]
struct ObservedSwapWindowCounts {
    total_rows: u64,
    buy_rows: u64,
    sell_rows: u64,
}

fn count_observed_swaps_in_window(
    conn: &Connection,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<ObservedSwapWindowCounts> {
    let (total_rows, buy_rows, sell_rows): (i64, i64, i64) = conn
        .query_row(
            "SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN token_in = ?3 AND token_out != ?3 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN token_out = ?3 AND token_in != ?3 THEN 1 ELSE 0 END), 0)
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             WHERE ts >= ?1
               AND ts <= ?2",
            params![window_start.to_rfc3339(), now.to_rfc3339(), SOL_MINT],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .context("failed querying observed_swaps aggregate window counts")?;
    Ok(ObservedSwapWindowCounts {
        total_rows: total_rows.max(0) as u64,
        buy_rows: buy_rows.max(0) as u64,
        sell_rows: sell_rows.max(0) as u64,
    })
}

#[derive(Debug, Clone, Copy)]
struct WalletActivityDaysCoverageSnapshot {
    covered_through_day_utc: Option<DateTime<Utc>>,
    row_count: u64,
}

fn wallet_activity_days_coverage_snapshot(
    conn: &Connection,
) -> Result<WalletActivityDaysCoverageSnapshot> {
    let (max_day_raw, row_count): (Option<String>, i64) = conn
        .query_row(
            "SELECT MAX(activity_day), COUNT(*)
             FROM wallet_activity_days",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("failed querying wallet_activity_days coverage snapshot")?;
    Ok(WalletActivityDaysCoverageSnapshot {
        covered_through_day_utc: parse_optional_day_start_utc(
            max_day_raw,
            "wallet_activity_days.activity_day",
        )?,
        row_count: row_count.max(0) as u64,
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct WalletMetricsAggregate {
    row_count: u64,
    max_score: Option<f64>,
    max_trades: Option<u64>,
    max_buy_total: Option<u64>,
    max_tradable_ratio: Option<f64>,
}

fn latest_wallet_metrics_window_start(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT window_start
             FROM wallet_metrics INDEXED BY idx_wallet_metrics_window_start
             ORDER BY window_start DESC
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed querying latest wallet_metrics window_start")?;
    raw.map(|raw| parse_rfc3339_utc(&raw, "wallet_metrics.window_start"))
        .transpose()
}

fn wallet_metrics_aggregate_for_window(
    conn: &Connection,
    window_start: DateTime<Utc>,
) -> Result<WalletMetricsAggregate> {
    let (canonical, legacy_z) = wallet_metrics_window_start_query_variants(window_start);
    let (row_count, max_score, max_trades, max_buy_total, max_tradable_ratio): (
        i64,
        Option<f64>,
        Option<i64>,
        Option<i64>,
        Option<f64>,
    ) = conn
        .query_row(
            "SELECT
                COUNT(*),
                MAX(wallet_metrics.score),
                MAX(wallet_metrics.trades),
                MAX(wallet_metrics.buy_total),
                MAX(wallet_metrics.tradable_ratio)
             FROM wallet_metrics
             JOIN (
                SELECT
                    wallet_id,
                    COALESCE(
                        MAX(CASE WHEN window_start = ?1 THEN id END),
                        MAX(id)
                    ) AS selected_id
                FROM wallet_metrics INDEXED BY idx_wallet_metrics_window_start
                WHERE window_start IN (?1, ?2)
                GROUP BY wallet_id
             ) AS selected_wallet_metrics
                ON selected_wallet_metrics.selected_id = wallet_metrics.id",
            params![canonical, legacy_z],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .context("failed querying wallet_metrics aggregate for latest window")?;
    Ok(WalletMetricsAggregate {
        row_count: row_count.max(0) as u64,
        max_score,
        max_trades: max_trades.map(|value| value.max(0) as u64),
        max_buy_total: max_buy_total.map(|value| value.max(0) as u64),
        max_tradable_ratio,
    })
}

fn wallet_metrics_window_start_query_variants(window_start: DateTime<Utc>) -> (String, String) {
    let canonical = window_start.to_rfc3339();
    let legacy_z = canonical
        .strip_suffix("+00:00")
        .map(|prefix| format!("{prefix}Z"))
        .unwrap_or_else(|| canonical.clone());
    (canonical, legacy_z)
}

fn load_discovery_scoring_state_ts(
    conn: &Connection,
    state_key: &str,
) -> Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT state_value
             FROM discovery_scoring_state
             WHERE state_key = ?1",
            params![state_key],
            |row| row.get(0),
        )
        .optional()
        .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))?;
    raw.map(|raw| parse_rfc3339_utc(&raw, &format!("discovery_scoring_state.{state_key}")))
        .transpose()
}

fn load_discovery_scoring_state_value(
    conn: &Connection,
    state_key: &str,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state_value
         FROM discovery_scoring_state
         WHERE state_key = ?1",
        params![state_key],
        |row| row.get(0),
    )
    .optional()
    .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))
}

fn load_discovery_scoring_materialization_gap_cursor(
    conn: &Connection,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    let Some(ts_utc) = load_discovery_scoring_state_ts(conn, "materialization_gap_since_ts")?
    else {
        return Ok(None);
    };
    let slot_raw = load_discovery_scoring_state_value(conn, "materialization_gap_since_slot")?;
    let signature =
        load_discovery_scoring_state_value(conn, "materialization_gap_since_signature")?;
    match (slot_raw, signature) {
        (Some(slot_raw), Some(signature)) => {
            let slot = slot_raw.parse::<u64>().with_context(|| {
                format!(
                    "invalid discovery_scoring_state.materialization_gap_since_slot value: {slot_raw}"
                )
            })?;
            Ok(Some(DiscoveryRuntimeCursor {
                ts_utc,
                slot,
                signature,
            }))
        }
        _ => Ok(Some(DiscoveryRuntimeCursor {
            ts_utc,
            slot: 0,
            signature: String::new(),
        })),
    }
}

fn open_read_only_connection(db_path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed opening sqlite read-only: {}", db_path.display()))?;
    conn.execute_batch("PRAGMA query_only = ON")
        .context("failed setting sqlite query_only pragma")?;
    Ok(conn)
}

fn build_report(
    config_path: String,
    db_path: String,
    flags: ConfigFlags,
    evidence: RuntimeEvidence,
    runtime_evidence_errors: Vec<String>,
) -> ScoringFactWriterBlockerReport {
    let blocker_reason = select_blocker_reason(flags, &evidence, &runtime_evidence_errors);
    ScoringFactWriterBlockerReport {
        config_path,
        db_path,
        production_green: false,
        scoring_aggregates_write_enabled: flags.scoring_aggregates_write_enabled,
        scoring_aggregates_enabled: flags.scoring_aggregates_enabled,
        observed_swaps_total_rows: evidence.observed_swaps_total_rows,
        observed_swaps_window_rows: evidence.observed_swaps_window_rows,
        observed_swaps_window_buy_rows: evidence.observed_swaps_window_buy_rows,
        observed_swaps_window_sell_rows: evidence.observed_swaps_window_sell_rows,
        observed_swaps_max_ts_utc: evidence.observed_swaps_max_ts_utc,
        wallet_activity_days_total_rows: evidence.wallet_activity_days_total_rows,
        wallet_activity_days_max_day_utc: evidence.wallet_activity_days_max_day_utc,
        latest_wallet_metrics_window_start: evidence.latest_wallet_metrics_window_start,
        latest_wallet_metrics_rows: evidence.latest_wallet_metrics_rows,
        latest_wallet_metrics_max_score: evidence.latest_wallet_metrics_max_score,
        latest_wallet_metrics_max_trades: evidence.latest_wallet_metrics_max_trades,
        latest_wallet_metrics_max_buy_total: evidence.latest_wallet_metrics_max_buy_total,
        latest_wallet_metrics_max_tradable_ratio: evidence.latest_wallet_metrics_max_tradable_ratio,
        wallet_scoring_days_rows: evidence.wallet_scoring_days_rows,
        wallet_scoring_buy_facts_rows: evidence.wallet_scoring_buy_facts_rows,
        wallet_scoring_close_facts_rows: evidence.wallet_scoring_close_facts_rows,
        wallet_scoring_open_lots_rows: evidence.wallet_scoring_open_lots_rows,
        wallet_scoring_carryover_lots_rows: evidence.wallet_scoring_carryover_lots_rows,
        discovery_scoring_covered_since: evidence.discovery_scoring_covered_since,
        discovery_scoring_covered_through: evidence.discovery_scoring_covered_through,
        discovery_scoring_materialization_gap_cursor: evidence
            .discovery_scoring_materialization_gap_cursor,
        blocker_reason,
        runtime_evidence_errors,
    }
}

fn select_blocker_reason(
    flags: ConfigFlags,
    evidence: &RuntimeEvidence,
    runtime_evidence_errors: &[String],
) -> String {
    if !flags.scoring_aggregates_write_enabled {
        return BLOCKER_WRITE_DISABLED.to_string();
    }
    if !flags.scoring_aggregates_enabled {
        return BLOCKER_READ_DISABLED.to_string();
    }
    if evidence
        .discovery_scoring_materialization_gap_cursor
        .is_some()
    {
        return BLOCKER_MATERIALIZATION_GAP_PENDING.to_string();
    }
    if has_blocking_runtime_evidence_errors(runtime_evidence_errors) {
        return BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE.to_string();
    }

    let Some(scoring_rows) = total_scoring_fact_rows(evidence) else {
        return BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE.to_string();
    };
    let observed_swaps_rows = evidence
        .observed_swaps_window_rows
        .or(evidence.observed_swaps_total_rows)
        .unwrap_or(0);
    if scoring_rows == 0
        && observed_swaps_rows > 0
        && evidence.wallet_activity_days_total_rows.unwrap_or(0) > 0
    {
        return BLOCKER_TABLES_EMPTY_DESPITE_ENABLED_WRITER.to_string();
    }
    if scoring_rows > 0 && evidence.latest_wallet_metrics_max_score == Some(0.0) {
        return BLOCKER_TABLES_PRESENT_BUT_METRICS_STILL_ZERO.to_string();
    }

    BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE.to_string()
}

fn has_blocking_runtime_evidence_errors(runtime_evidence_errors: &[String]) -> bool {
    runtime_evidence_errors
        .iter()
        .any(|error| !is_expected_unbounded_observed_swaps_coverage_skip(error))
}

fn is_expected_unbounded_observed_swaps_coverage_skip(error: &str) -> bool {
    error.starts_with("observed_swaps_total_rows_unavailable:")
        || error.starts_with("observed_swaps_max_ts_utc_unavailable:")
}

fn total_scoring_fact_rows(evidence: &RuntimeEvidence) -> Option<u64> {
    Some(
        evidence.wallet_scoring_days_rows?
            + evidence.wallet_scoring_buy_facts_rows?
            + evidence.wallet_scoring_close_facts_rows?
            + evidence.wallet_scoring_open_lots_rows?
            + evidence.wallet_scoring_carryover_lots_rows?,
    )
}

fn render_output(report: &ScoringFactWriterBlockerReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report).context("failed serializing report as json");
    }
    Ok(render_human(report))
}

fn render_human(report: &ScoringFactWriterBlockerReport) -> String {
    [
        "event=copybot_discovery_scoring_fact_writer_blocker_report".to_string(),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
        format!("production_green={}", report.production_green),
        format!(
            "scoring_aggregates_write_enabled={}",
            report.scoring_aggregates_write_enabled
        ),
        format!(
            "scoring_aggregates_enabled={}",
            report.scoring_aggregates_enabled
        ),
        format!(
            "observed_swaps_total_rows={}",
            format_optional_u64(report.observed_swaps_total_rows)
        ),
        format!(
            "observed_swaps_window_rows={}",
            format_optional_u64(report.observed_swaps_window_rows)
        ),
        format!(
            "observed_swaps_window_buy_rows={}",
            format_optional_u64(report.observed_swaps_window_buy_rows)
        ),
        format!(
            "observed_swaps_window_sell_rows={}",
            format_optional_u64(report.observed_swaps_window_sell_rows)
        ),
        format!(
            "wallet_activity_days_total_rows={}",
            format_optional_u64(report.wallet_activity_days_total_rows)
        ),
        format!(
            "latest_wallet_metrics_window_start={}",
            format_optional_ts(report.latest_wallet_metrics_window_start)
        ),
        format!(
            "latest_wallet_metrics_rows={}",
            format_optional_u64(report.latest_wallet_metrics_rows)
        ),
        format!(
            "latest_wallet_metrics_max_score={}",
            format_optional_f64(report.latest_wallet_metrics_max_score)
        ),
        format!(
            "wallet_scoring_days_rows={}",
            format_optional_u64(report.wallet_scoring_days_rows)
        ),
        format!(
            "wallet_scoring_buy_facts_rows={}",
            format_optional_u64(report.wallet_scoring_buy_facts_rows)
        ),
        format!(
            "wallet_scoring_close_facts_rows={}",
            format_optional_u64(report.wallet_scoring_close_facts_rows)
        ),
        format!(
            "wallet_scoring_open_lots_rows={}",
            format_optional_u64(report.wallet_scoring_open_lots_rows)
        ),
        format!(
            "wallet_scoring_carryover_lots_rows={}",
            format_optional_u64(report.wallet_scoring_carryover_lots_rows)
        ),
        format!("blocker_reason={}", report.blocker_reason),
        format!(
            "runtime_evidence_errors={}",
            if report.runtime_evidence_errors.is_empty() {
                "none".to_string()
            } else {
                report.runtime_evidence_errors.join(" | ")
            }
        ),
    ]
    .join("\n")
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_f64(value: Option<f64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "unknown".to_string())
}

fn resolve_db_path(config_path: &Path, sqlite_path: &str) -> PathBuf {
    let configured = Path::new(sqlite_path.trim());
    if configured.is_absolute() {
        return configured.to_path_buf();
    }
    match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(configured),
        _ => configured.to_path_buf(),
    }
}

fn compact_error(error: anyhow::Error) -> String {
    format!("{error:#}").replace('\n', " ")
}

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        })
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
}

fn parse_optional_day_start_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| {
        let day = NaiveDate::parse_from_str(&raw, "%Y-%m-%d")
            .with_context(|| format!("invalid {field_name} day value: {raw}"))?;
        let start = day
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid {field_name} day start value: {raw}"))?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(start, Utc))
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::{SwapEvent, WalletMetricRow};
    use copybot_storage::{DiscoveryAggregateWriteConfig, SqliteStore};
    use rusqlite::{params, Connection};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn enabled_flags() -> ConfigFlags {
        ConfigFlags {
            scoring_aggregates_write_enabled: true,
            scoring_aggregates_enabled: true,
        }
    }

    fn test_ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn temp_db_path(name: &str) -> PathBuf {
        let id = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        env::temp_dir().join(format!(
            "copybot_scoring_fact_writer_blocker_report_{}_{}_{}.sqlite",
            std::process::id(),
            name,
            id
        ))
    }

    fn cleanup_db(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }

    fn migrations_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
    }

    fn buy_swap(signature: &str, wallet: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: format!("{wallet}-token"),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn sell_swap(signature: &str, wallet: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: format!("{wallet}-token"),
            token_out: SOL_MINT.to_string(),
            amount_in: 5.0,
            amount_out: 0.5,
            signature: signature.to_string(),
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn seed_observed_activity_and_zero_metrics(db_path: &Path, now: DateTime<Utc>) -> Result<()> {
        let mut store = SqliteStore::open(db_path)?;
        store.run_migrations(&migrations_dir())?;
        let wallet = "wallet-a";
        store.upsert_wallet(wallet, now - Duration::days(1), now, "active")?;
        store.insert_observed_swaps_batch_with_activity_days(&[
            buy_swap("buy-a", wallet, 1, now - Duration::minutes(2)),
            sell_swap("sell-a", wallet, 2, now - Duration::minutes(1)),
        ])?;
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: wallet.to_string(),
            window_start: now - Duration::days(2),
            pnl: 0.0,
            win_rate: 0.0,
            trades: 2,
            closed_trades: 0,
            hold_median_seconds: 0,
            score: 0.0,
            buy_total: 1,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
        Ok(())
    }

    fn read_report_from_fixture(
        db_path: &Path,
        now: DateTime<Utc>,
    ) -> ScoringFactWriterBlockerReport {
        let (evidence, errors) = load_runtime_evidence(db_path, now - Duration::days(2), now);
        build_report(
            "config.toml".to_string(),
            db_path.display().to_string(),
            enabled_flags(),
            evidence,
            errors,
        )
    }

    fn assert_observed_full_coverage_skipped(errors: &[String]) {
        assert!(
            errors.iter().any(|error| error.starts_with(
                "observed_swaps_total_rows_unavailable:observed_swaps_full_table_count_unbounded_scan_avoided"
            )),
            "expected observed_swaps total row scan to be skipped: {errors:?}"
        );
        assert!(
            errors.iter().any(|error| error.starts_with(
                "observed_swaps_max_ts_utc_unavailable:observed_swaps_full_table_max_unbounded_scan_avoided"
            )),
            "expected observed_swaps max timestamp scan to be skipped: {errors:?}"
        );
        assert!(
            !has_blocking_runtime_evidence_errors(errors),
            "expected only non-blocking bounded-operator skips: {errors:?}"
        );
    }

    fn populated_runtime_with_scoring_counts(
        days: Option<u64>,
        buy: Option<u64>,
        close: Option<u64>,
        open: Option<u64>,
        carryover: Option<u64>,
    ) -> RuntimeEvidence {
        RuntimeEvidence {
            observed_swaps_total_rows: Some(42),
            observed_swaps_window_rows: Some(42),
            observed_swaps_window_buy_rows: Some(30),
            observed_swaps_window_sell_rows: Some(12),
            observed_swaps_max_ts_utc: None,
            wallet_activity_days_total_rows: Some(7),
            wallet_activity_days_max_day_utc: None,
            latest_wallet_metrics_window_start: None,
            latest_wallet_metrics_rows: Some(7),
            latest_wallet_metrics_max_score: Some(0.0),
            latest_wallet_metrics_max_trades: Some(59),
            latest_wallet_metrics_max_buy_total: Some(35),
            latest_wallet_metrics_max_tradable_ratio: Some(1.0),
            wallet_scoring_days_rows: days,
            wallet_scoring_buy_facts_rows: buy,
            wallet_scoring_close_facts_rows: close,
            wallet_scoring_open_lots_rows: open,
            wallet_scoring_carryover_lots_rows: carryover,
            discovery_scoring_covered_since: None,
            discovery_scoring_covered_through: None,
            discovery_scoring_materialization_gap_cursor: None,
        }
    }

    #[test]
    fn disabled_write_config_reports_write_disabled() {
        let reason = select_blocker_reason(
            ConfigFlags {
                scoring_aggregates_write_enabled: false,
                scoring_aggregates_enabled: false,
            },
            &populated_runtime_with_scoring_counts(Some(0), Some(0), Some(0), Some(0), Some(0)),
            &[],
        );
        assert_eq!(reason, BLOCKER_WRITE_DISABLED);
    }

    #[test]
    fn disabled_read_config_reports_read_disabled() {
        let reason = select_blocker_reason(
            ConfigFlags {
                scoring_aggregates_write_enabled: true,
                scoring_aggregates_enabled: false,
            },
            &populated_runtime_with_scoring_counts(Some(0), Some(0), Some(0), Some(0), Some(0)),
            &[],
        );
        assert_eq!(reason, BLOCKER_READ_DISABLED);
    }

    #[test]
    fn materialization_gap_present_takes_precedence() {
        let mut evidence =
            populated_runtime_with_scoring_counts(Some(0), Some(0), Some(0), Some(0), Some(0));
        evidence.discovery_scoring_materialization_gap_cursor = Some(DiscoveryRuntimeCursor {
            ts_utc: DateTime::parse_from_rfc3339("2026-04-27T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            slot: 42,
            signature: "sig".to_string(),
        });
        let reason = select_blocker_reason(enabled_flags(), &evidence, &[]);
        assert_eq!(reason, BLOCKER_MATERIALIZATION_GAP_PENDING);
    }

    #[test]
    fn enabled_config_with_empty_scoring_tables_reports_empty_writer_blocker() {
        let evidence =
            populated_runtime_with_scoring_counts(Some(0), Some(0), Some(0), Some(0), Some(0));
        let reason = select_blocker_reason(enabled_flags(), &evidence, &[]);
        assert_eq!(reason, BLOCKER_TABLES_EMPTY_DESPITE_ENABLED_WRITER);
    }

    #[test]
    fn scoring_tables_present_but_metrics_still_zero_reports_zero_metrics_blocker() {
        let evidence =
            populated_runtime_with_scoring_counts(Some(3), Some(2), Some(1), Some(0), Some(0));
        let reason = select_blocker_reason(enabled_flags(), &evidence, &[]);
        assert_eq!(reason, BLOCKER_TABLES_PRESENT_BUT_METRICS_STILL_ZERO);
    }

    #[test]
    fn missing_runtime_evidence_is_unproven() {
        let evidence =
            populated_runtime_with_scoring_counts(Some(0), Some(0), Some(0), None, Some(0));
        let reason = select_blocker_reason(enabled_flags(), &evidence, &[]);
        assert_eq!(reason, BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE);
        let reason_with_error =
            select_blocker_reason(enabled_flags(), &evidence, &["missing table".to_string()]);
        assert_eq!(reason_with_error, BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE);
    }

    #[test]
    fn report_never_marks_production_green() {
        let report = build_report(
            "config.toml".to_string(),
            "runtime.sqlite".to_string(),
            enabled_flags(),
            populated_runtime_with_scoring_counts(Some(0), Some(0), Some(0), Some(0), Some(0)),
            Vec::new(),
        );
        assert!(!report.production_green);
        assert_eq!(
            report.blocker_reason,
            BLOCKER_TABLES_EMPTY_DESPITE_ENABLED_WRITER
        );
    }

    #[test]
    fn read_only_fixture_reports_empty_scoring_tables_despite_enabled_writer() -> Result<()> {
        let db_path = temp_db_path("empty-scoring");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        seed_observed_activity_and_zero_metrics(&db_path, now)?;

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(
            report.blocker_reason,
            BLOCKER_TABLES_EMPTY_DESPITE_ENABLED_WRITER
        );
        assert_eq!(report.observed_swaps_total_rows, None);
        assert_eq!(report.observed_swaps_max_ts_utc, None);
        assert_eq!(report.observed_swaps_window_rows, Some(2));
        assert_eq!(report.observed_swaps_window_buy_rows, Some(1));
        assert_eq!(report.observed_swaps_window_sell_rows, Some(1));
        assert_eq!(report.wallet_activity_days_total_rows, Some(1));
        assert_eq!(report.wallet_scoring_days_rows, Some(0));
        assert_eq!(report.wallet_scoring_buy_facts_rows, Some(0));
        assert_eq!(report.wallet_scoring_close_facts_rows, Some(0));
        assert_eq!(report.wallet_scoring_open_lots_rows, Some(0));
        assert_eq!(report.wallet_scoring_carryover_lots_rows, Some(0));
        assert!(!report.production_green);
        assert_observed_full_coverage_skipped(&report.runtime_evidence_errors);
        Ok(())
    }

    #[test]
    fn observed_swaps_total_coverage_unavailable_does_not_require_full_scan() -> Result<()> {
        let db_path = temp_db_path("observed-coverage-unavailable");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        seed_observed_activity_and_zero_metrics(&db_path, now)?;

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(report.observed_swaps_total_rows, None);
        assert_eq!(report.observed_swaps_max_ts_utc, None);
        assert_eq!(report.observed_swaps_window_rows, Some(2));
        assert_eq!(
            report.blocker_reason,
            BLOCKER_TABLES_EMPTY_DESPITE_ENABLED_WRITER
        );
        assert!(!report.production_green);
        assert_observed_full_coverage_skipped(&report.runtime_evidence_errors);
        Ok(())
    }

    #[test]
    fn read_only_fixture_reports_materialization_gap_pending() -> Result<()> {
        let db_path = temp_db_path("materialization-gap");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        {
            seed_observed_activity_and_zero_metrics(&db_path, now)?;
            let store = SqliteStore::open(&db_path)?;
            store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(3),
                slot: 3,
                signature: "gap-sig".to_string(),
            })?;
        }

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(report.blocker_reason, BLOCKER_MATERIALIZATION_GAP_PENDING);
        assert!(report
            .discovery_scoring_materialization_gap_cursor
            .is_some());
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn read_only_fixture_reports_scoring_tables_present_but_metrics_still_zero() -> Result<()> {
        let db_path = temp_db_path("scoring-present-zero-metrics");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        {
            seed_observed_activity_and_zero_metrics(&db_path, now)?;
            let store = SqliteStore::open(&db_path)?;
            store.apply_discovery_scoring_batch(
                &[buy_swap(
                    "scoring-buy-a",
                    "wallet-a",
                    3,
                    now - Duration::minutes(1),
                )],
                &DiscoveryAggregateWriteConfig::default(),
            )?;
        }

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(
            report.blocker_reason,
            BLOCKER_TABLES_PRESENT_BUT_METRICS_STILL_ZERO
        );
        assert!(report.wallet_scoring_days_rows.unwrap_or(0) > 0);
        assert!(report.wallet_scoring_buy_facts_rows.unwrap_or(0) > 0);
        assert_eq!(report.latest_wallet_metrics_max_score, Some(0.0));
        assert!(!report.production_green);
        assert_observed_full_coverage_skipped(&report.runtime_evidence_errors);
        Ok(())
    }

    #[test]
    fn read_only_fixture_counts_carryover_lots_independently() -> Result<()> {
        let db_path = temp_db_path("carryover-independent-count");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        {
            seed_observed_activity_and_zero_metrics(&db_path, now)?;
            let conn = Connection::open(&db_path)?;
            conn.execute(
                "INSERT INTO wallet_scoring_carryover_lots(
                    wallet_id, token, qty, cost_sol, oldest_opened_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["wallet-a", "token-a", 1.0_f64, 0.5_f64, now.to_rfc3339()],
            )?;
        }

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(report.wallet_scoring_open_lots_rows, Some(0));
        assert_eq!(report.wallet_scoring_carryover_lots_rows, Some(1));
        assert_eq!(
            report.blocker_reason,
            BLOCKER_TABLES_PRESENT_BUT_METRICS_STILL_ZERO
        );
        assert_observed_full_coverage_skipped(&report.runtime_evidence_errors);
        Ok(())
    }

    #[test]
    fn read_only_fixture_counts_scoring_tables_without_loading_rows() -> Result<()> {
        let db_path = temp_db_path("malformed-scoring-row-count-only");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        {
            seed_observed_activity_and_zero_metrics(&db_path, now)?;
            let conn = Connection::open(&db_path)?;
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
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, NULL, NULL, ?11, NULL, NULL)",
                params![
                    "malformed-buy",
                    "wallet-a",
                    "token-a",
                    "not-a-timestamp",
                    "not-a-day",
                    1.0_f64,
                    0.0_f64,
                    0_i64,
                    0.0_f64,
                    "not-a-valid-quality-source",
                    "also-not-a-timestamp",
                ],
            )?;
        }

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(report.wallet_scoring_buy_facts_rows, Some(1));
        assert_eq!(
            report.blocker_reason,
            BLOCKER_TABLES_PRESENT_BUT_METRICS_STILL_ZERO
        );
        assert!(
            !report
                .runtime_evidence_errors
                .iter()
                .any(|error| error.contains("wallet_scoring_buy_facts")),
            "COUNT(*) path must not parse malformed scoring fact rows: {:?}",
            report.runtime_evidence_errors
        );
        Ok(())
    }

    #[test]
    fn read_only_fixture_table_count_failures_are_independent() -> Result<()> {
        let db_path = temp_db_path("independent-table-failure");
        cleanup_db(&db_path);
        let now = test_ts("2026-04-27T12:00:00Z");
        {
            seed_observed_activity_and_zero_metrics(&db_path, now)?;
            let conn = Connection::open(&db_path)?;
            conn.execute("DROP TABLE wallet_scoring_carryover_lots", [])?;
        }

        let report = read_report_from_fixture(&db_path, now);
        cleanup_db(&db_path);

        assert_eq!(report.wallet_scoring_days_rows, Some(0));
        assert_eq!(report.wallet_scoring_buy_facts_rows, Some(0));
        assert_eq!(report.wallet_scoring_close_facts_rows, Some(0));
        assert_eq!(report.wallet_scoring_open_lots_rows, Some(0));
        assert_eq!(report.wallet_scoring_carryover_lots_rows, None);
        assert!(
            report
                .runtime_evidence_errors
                .iter()
                .any(|error| error.contains("wallet_scoring_carryover_lots_count_unavailable")),
            "missing carryover table should be reported exactly: {:?}",
            report.runtime_evidence_errors
        );
        assert!(
            !report
                .runtime_evidence_errors
                .iter()
                .any(|error| error.contains("wallet_scoring_open_lots_count_unavailable")),
            "open lots count should remain independently proven: {:?}",
            report.runtime_evidence_errors
        );
        assert_eq!(
            report.blocker_reason,
            BLOCKER_UNPROVEN_MISSING_RUNTIME_EVIDENCE
        );
        Ok(())
    }
}
