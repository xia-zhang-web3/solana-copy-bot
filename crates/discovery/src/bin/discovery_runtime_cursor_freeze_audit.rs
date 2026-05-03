use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, AppConfig, DiscoveryConfig};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_runtime_cursor_freeze_audit --db <path> --config <path> [--now <rfc3339>] [--json]";

const CONCLUSION_NO_SOURCE_FRONTIER_BEYOND_CURSOR: &str =
    "runtime_cursor_frozen_because_no_source_frontier_beyond_cursor";
const CONCLUSION_SOURCE_FRONTIER_EXISTS_BUT_RUNTIME_ADVANCE_NOT_REACHED: &str =
    "runtime_cursor_frozen_because_source_frontier_exists_but_runtime_advance_path_not_reached";
const CONCLUSION_HORIZON_AHEAD_OF_PERSISTED_CURSOR: &str =
    "runtime_cursor_frozen_because_runtime_window_horizon_now_ahead_of_persisted_cursor";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const RUNTIME_CURSOR_LOAD_SURFACE: &str =
    "copybot_storage::SqliteStore::load_discovery_runtime_cursor";
const RUNTIME_CURSOR_ADVANCE_PERSIST_SURFACE: &str =
    "copybot_storage::SqliteStore::upsert_discovery_runtime_cursor";
const RAW_WINDOW_UNUSABLE_NO_RECENT_PUBLISHED_UNIVERSE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::run_cycle";
const RUNTIME_CURSOR_ADVANCE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::run_cycle.observed_swaps_after_cursor_fetch";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");

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
struct RuntimeCursorFreezeAuditReport {
    config_path: String,
    db_path: String,
    now_utc: DateTime<Utc>,
    configured_sqlite_path: String,
    runtime_cursor_ts_utc: Option<DateTime<Utc>>,
    runtime_cursor_slot: Option<u64>,
    runtime_cursor_signature: Option<String>,
    publication_runtime_mode: Option<String>,
    publication_reason: Option<String>,
    publication_last_published_at_utc: Option<DateTime<Utc>>,
    publication_updated_at_utc: Option<DateTime<Utc>>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
    recent_raw_journal_db_path_from_config: Option<String>,
    recent_raw_journal_db_path_exists: bool,
    recent_raw_journal_accessible: bool,
    recent_raw_journal_observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    required_window_start_utc: DateTime<Utc>,
    runtime_cursor_before_required_window_start: Option<bool>,
    observed_swaps_frontier_after_runtime_cursor: Option<bool>,
    recent_raw_frontier_after_runtime_cursor: Option<bool>,
    runtime_cursor_lag_seconds_vs_observed_swaps_frontier: Option<i64>,
    runtime_cursor_lag_days_vs_observed_swaps_frontier: Option<i64>,
    runtime_cursor_load_surface: String,
    runtime_cursor_advance_persist_surface: String,
    runtime_cursor_advance_surface: String,
    raw_window_unusable_no_recent_published_universe_surface: String,
    cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild: bool,
    cursor_advancement_can_run_before_publication_state_selection: bool,
    runtime_cursor_freeze_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct PublicationFacts {
    runtime_mode: Option<String>,
    reason: Option<String>,
    last_published_at_utc: Option<DateTime<Utc>>,
    updated_at_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct FrontierFacts {
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    wallet_activity_days_max_day_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct RecentRawJournalFacts {
    db_path: Option<PathBuf>,
    db_path_exists: bool,
    accessible: bool,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct DerivedFacts {
    runtime_cursor_before_required_window_start: Option<bool>,
    observed_swaps_frontier_after_runtime_cursor: Option<bool>,
    recent_raw_frontier_after_runtime_cursor: Option<bool>,
    runtime_cursor_lag_seconds_vs_observed_swaps_frontier: Option<i64>,
    runtime_cursor_lag_days_vs_observed_swaps_frontier: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild: bool,
    cursor_advancement_can_run_before_publication_state_selection: bool,
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
            "--now" => {
                now_utc = parse_db_ts(&parse_string_arg("--now", args.next())?)?;
            }
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

fn run(config: &Config) -> Result<RuntimeCursorFreezeAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let runtime_store = SqliteStore::open_read_only(&config.db_path)
        .with_context(|| format!("failed opening runtime db {}", config.db_path.display()))?;
    let runtime_conn = open_read_only_connection(&config.db_path)?;

    let runtime_cursor = runtime_store.load_discovery_runtime_cursor_read_only()?;
    let publication = load_publication_facts(&runtime_store)?;
    let frontier = load_frontier_facts(&runtime_conn)?;
    let recent_raw = load_recent_raw_journal_facts(&config.config_path, &loaded_config)?;
    let required_window_start_utc = required_window_start(&loaded_config.discovery, config.now_utc);
    let derived = derive_facts(
        runtime_cursor.as_ref(),
        &frontier,
        &recent_raw,
        required_window_start_utc,
    );
    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(&derived, &frontier, &recent_raw);

    Ok(RuntimeCursorFreezeAuditReport {
        config_path: config.config_path.display().to_string(),
        db_path: config.db_path.display().to_string(),
        now_utc: config.now_utc,
        configured_sqlite_path: resolve_db_path(&config.config_path, &loaded_config.sqlite.path)
            .display()
            .to_string(),
        runtime_cursor_ts_utc: runtime_cursor.as_ref().map(|cursor| cursor.ts_utc),
        runtime_cursor_slot: runtime_cursor.as_ref().map(|cursor| cursor.slot),
        runtime_cursor_signature: runtime_cursor
            .as_ref()
            .map(|cursor| cursor.signature.clone()),
        publication_runtime_mode: publication.runtime_mode,
        publication_reason: publication.reason,
        publication_last_published_at_utc: publication.last_published_at_utc,
        publication_updated_at_utc: publication.updated_at_utc,
        observed_swaps_max_ts_utc: frontier.observed_swaps_max_ts_utc,
        wallet_activity_days_max_day_utc: frontier.wallet_activity_days_max_day_utc,
        recent_raw_journal_db_path_from_config: recent_raw
            .db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        recent_raw_journal_db_path_exists: recent_raw.db_path_exists,
        recent_raw_journal_accessible: recent_raw.accessible,
        recent_raw_journal_observed_swaps_max_ts_utc: recent_raw.observed_swaps_max_ts_utc,
        required_window_start_utc,
        runtime_cursor_before_required_window_start: derived
            .runtime_cursor_before_required_window_start,
        observed_swaps_frontier_after_runtime_cursor: derived
            .observed_swaps_frontier_after_runtime_cursor,
        recent_raw_frontier_after_runtime_cursor: derived.recent_raw_frontier_after_runtime_cursor,
        runtime_cursor_lag_seconds_vs_observed_swaps_frontier: derived
            .runtime_cursor_lag_seconds_vs_observed_swaps_frontier,
        runtime_cursor_lag_days_vs_observed_swaps_frontier: derived
            .runtime_cursor_lag_days_vs_observed_swaps_frontier,
        runtime_cursor_load_surface: RUNTIME_CURSOR_LOAD_SURFACE.to_string(),
        runtime_cursor_advance_persist_surface: RUNTIME_CURSOR_ADVANCE_PERSIST_SURFACE.to_string(),
        runtime_cursor_advance_surface: RUNTIME_CURSOR_ADVANCE_SURFACE.to_string(),
        raw_window_unusable_no_recent_published_universe_surface:
            RAW_WINDOW_UNUSABLE_NO_RECENT_PUBLISHED_UNIVERSE_SURFACE.to_string(),
        cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild: code_paths
            .cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild,
        cursor_advancement_can_run_before_publication_state_selection: code_paths
            .cursor_advancement_can_run_before_publication_state_selection,
        runtime_cursor_freeze_conclusion: conclusion.to_string(),
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

fn load_publication_facts(store: &SqliteStore) -> Result<PublicationFacts> {
    let Some(state) = store.discovery_publication_state_read_only()? else {
        return Ok(PublicationFacts::default());
    };
    Ok(PublicationFacts {
        runtime_mode: Some(state.runtime_mode.as_str().to_string()),
        reason: Some(state.reason),
        last_published_at_utc: state.last_published_at,
        updated_at_utc: Some(state.updated_at),
    })
}

fn load_frontier_facts(conn: &Connection) -> Result<FrontierFacts> {
    Ok(FrontierFacts {
        observed_swaps_max_ts_utc: query_observed_swaps_max_ts_utc(conn)?,
        wallet_activity_days_max_day_utc: query_wallet_activity_days_max_day_utc(conn)?,
    })
}

fn load_recent_raw_journal_facts(
    config_path: &Path,
    loaded_config: &AppConfig,
) -> Result<RecentRawJournalFacts> {
    let db_path = resolve_db_path(config_path, &loaded_config.recent_raw_journal.path);
    if !db_path.exists() {
        return Ok(RecentRawJournalFacts {
            db_path: Some(db_path),
            db_path_exists: false,
            accessible: false,
            observed_swaps_max_ts_utc: None,
        });
    }

    let conn = match open_read_only_connection(&db_path) {
        Ok(conn) => conn,
        Err(_) => {
            return Ok(RecentRawJournalFacts {
                db_path: Some(db_path),
                db_path_exists: true,
                accessible: false,
                observed_swaps_max_ts_utc: None,
            });
        }
    };
    let observed_swaps_max_ts_utc = query_observed_swaps_max_ts_utc(&conn)?;
    Ok(RecentRawJournalFacts {
        db_path: Some(db_path),
        db_path_exists: true,
        accessible: true,
        observed_swaps_max_ts_utc,
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

fn query_observed_swaps_max_ts_utc(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    if !sqlite_table_exists(conn, "observed_swaps")?
        || !sqlite_index_exists(conn, "idx_observed_swaps_ts_slot_signature")?
    {
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT ts
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("failed loading indexed observed_swaps max ts")?;
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

fn required_window_start(config: &DiscoveryConfig, now: DateTime<Utc>) -> DateTime<Utc> {
    now - Duration::days(config.scoring_window_days.max(1) as i64)
}

fn derive_facts(
    runtime_cursor: Option<&DiscoveryRuntimeCursor>,
    frontier: &FrontierFacts,
    recent_raw: &RecentRawJournalFacts,
    required_window_start_utc: DateTime<Utc>,
) -> DerivedFacts {
    let runtime_cursor_before_required_window_start =
        runtime_cursor.map(|cursor| cursor.ts_utc < required_window_start_utc);
    let observed_swaps_frontier_after_runtime_cursor = runtime_cursor
        .zip(frontier.observed_swaps_max_ts_utc)
        .map(|(cursor, frontier_ts)| frontier_ts > cursor.ts_utc);
    let recent_raw_frontier_after_runtime_cursor = runtime_cursor
        .zip(recent_raw.observed_swaps_max_ts_utc)
        .map(|(cursor, frontier_ts)| frontier_ts > cursor.ts_utc);
    let runtime_cursor_lag_seconds_vs_observed_swaps_frontier = runtime_cursor
        .zip(frontier.observed_swaps_max_ts_utc)
        .map(|(cursor, frontier_ts)| (frontier_ts - cursor.ts_utc).num_seconds());
    let runtime_cursor_lag_days_vs_observed_swaps_frontier =
        runtime_cursor_lag_seconds_vs_observed_swaps_frontier
            .map(|seconds| seconds.div_euclid(86_400));

    DerivedFacts {
        runtime_cursor_before_required_window_start,
        observed_swaps_frontier_after_runtime_cursor,
        recent_raw_frontier_after_runtime_cursor,
        runtime_cursor_lag_seconds_vs_observed_swaps_frontier,
        runtime_cursor_lag_days_vs_observed_swaps_frontier,
    }
}

fn inspect_code_path_facts() -> CodePathFacts {
    let cursor_advancement_can_run_before_publication_state_selection = DISCOVERY_SOURCE
        .contains("if fetch_progress.query_rows > 0")
        && DISCOVERY_SOURCE.contains("store.upsert_discovery_runtime_cursor(&persisted)")
        && DISCOVERY_SOURCE.contains("PreparedCycleState::Unusable")
        && source_order(
            "if fetch_progress.query_rows > 0",
            "PreparedCycleState::Unusable",
        )
        .unwrap_or(false);
    CodePathFacts {
        cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild:
            !cursor_advancement_can_run_before_publication_state_selection,
        cursor_advancement_can_run_before_publication_state_selection,
    }
}

fn source_order(first: &str, second: &str) -> Option<bool> {
    let first_index = DISCOVERY_SOURCE.find(first)?;
    let second_index = DISCOVERY_SOURCE.find(second)?;
    Some(first_index < second_index)
}

fn choose_conclusion(
    derived: &DerivedFacts,
    frontier: &FrontierFacts,
    recent_raw: &RecentRawJournalFacts,
) -> &'static str {
    let cursor_before_window = derived.runtime_cursor_before_required_window_start == Some(true);
    let source_frontier_after_cursor = derived.observed_swaps_frontier_after_runtime_cursor
        == Some(true)
        || derived.recent_raw_frontier_after_runtime_cursor == Some(true);
    let source_frontier_known = frontier.observed_swaps_max_ts_utc.is_some()
        || recent_raw.observed_swaps_max_ts_utc.is_some();
    let no_known_source_frontier_beyond_cursor = source_frontier_known
        && derived.observed_swaps_frontier_after_runtime_cursor != Some(true)
        && derived.recent_raw_frontier_after_runtime_cursor != Some(true);

    if cursor_before_window && source_frontier_after_cursor {
        return CONCLUSION_SOURCE_FRONTIER_EXISTS_BUT_RUNTIME_ADVANCE_NOT_REACHED;
    }
    if cursor_before_window && no_known_source_frontier_beyond_cursor {
        return CONCLUSION_NO_SOURCE_FRONTIER_BEYOND_CURSOR;
    }
    if cursor_before_window {
        return CONCLUSION_HORIZON_AHEAD_OF_PERSISTED_CURSOR;
    }
    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &RuntimeCursorFreezeAuditReport) -> Result<String> {
    serde_json::to_string_pretty(report).context("failed to serialize runtime cursor freeze report")
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode, WalletActivityDayRow,
    };
    use tempfile::TempDir;

    fn create_store(path: &Path) -> Result<SqliteStore> {
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(path)?;
        store.run_migrations(&migration_dir)?;
        Ok(store)
    }

    fn write_config(
        temp: &TempDir,
        runtime_db_path: &Path,
        journal_db_path: &Path,
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
                    "min_score = 0.0\n",
                    "min_trades = 1\n",
                    "min_active_days = 1\n",
                    "min_leader_notional_sol = 0.0\n",
                    "min_buy_count = 1\n",
                    "min_tradable_ratio = 0.0\n",
                    "max_rug_ratio = 1.0\n",
                    "thin_market_min_unique_traders = 1\n\n",
                    "[execution]\n",
                    "enabled = false\n",
                ),
                runtime_db_path.display(),
                journal_db_path.display(),
            ),
        )
        .context("failed writing test config")?;
        Ok(config_path)
    }

    fn swap(signature: &str, wallet: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
        SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRuntimeCursorAudit11111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn seed_publication_state(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_unusable_no_recent_published_universe".to_string(),
            last_published_at: Some(now - Duration::days(2)),
            last_published_window_start: Some(now - Duration::days(6)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })
    }

    fn fixture_config(db_path: PathBuf, config_path: PathBuf, now_utc: DateTime<Utc>) -> Config {
        Config {
            db_path,
            config_path,
            now_utc,
        }
    }

    fn seed_source_frontier_after_cursor_fixture() -> Result<(TempDir, Config)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let runtime_db_path = temp.path().join("runtime.db");
        let journal_db_path = temp.path().join("recent_raw.db");
        let runtime_store = create_store(&runtime_db_path)?;
        let journal_store = create_store(&journal_db_path)?;
        let now = parse_db_ts("2026-04-23T08:59:49Z")?;
        let cursor_ts = parse_db_ts("2026-04-17T17:33:19Z")?;
        let frontier_ts = parse_db_ts("2026-04-19T09:00:00Z")?;
        let cursor = DiscoveryRuntimeCursor {
            ts_utc: cursor_ts,
            slot: 17,
            signature: "cursor-old".to_string(),
        };
        runtime_store.upsert_discovery_runtime_cursor(&cursor)?;
        runtime_store.insert_observed_swap(&swap("sig-frontier", "wallet-a", frontier_ts, 19))?;
        runtime_store.upsert_wallet_activity_days(&[WalletActivityDayRow {
            wallet_id: "wallet-a".to_string(),
            activity_day: frontier_ts.date_naive(),
            last_seen: frontier_ts,
        }])?;
        journal_store.insert_recent_raw_journal_batch(
            &[swap("sig-recent-raw-frontier", "wallet-b", frontier_ts, 20)],
            now,
        )?;
        seed_publication_state(&runtime_store, now)?;
        let config_path = write_config(&temp, &runtime_db_path, &journal_db_path)?;
        Ok((temp, fixture_config(runtime_db_path, config_path, now)))
    }

    fn seed_no_frontier_beyond_cursor_fixture() -> Result<(TempDir, Config)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let runtime_db_path = temp.path().join("runtime.db");
        let missing_journal_db_path = temp.path().join("missing-recent-raw.db");
        let runtime_store = create_store(&runtime_db_path)?;
        let now = parse_db_ts("2026-04-23T08:59:49Z")?;
        let cursor_ts = parse_db_ts("2026-04-17T17:33:19Z")?;
        let cursor = DiscoveryRuntimeCursor {
            ts_utc: cursor_ts,
            slot: 17,
            signature: "cursor-current".to_string(),
        };
        runtime_store.upsert_discovery_runtime_cursor(&cursor)?;
        runtime_store.insert_observed_swap(&swap("sig-cursor", "wallet-a", cursor_ts, 17))?;
        seed_publication_state(&runtime_store, now)?;
        let config_path = write_config(&temp, &runtime_db_path, &missing_journal_db_path)?;
        Ok((temp, fixture_config(runtime_db_path, config_path, now)))
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
                "2026-04-23T08:59:49Z".to_string(),
                "--json".to_string(),
            ],
            default_now,
        )?
        .expect("config");
        assert_eq!(parsed.db_path, PathBuf::from("runtime.db"));
        assert_eq!(parsed.config_path, PathBuf::from("config.toml"));
        assert_eq!(parsed.now_utc, parse_db_ts("2026-04-23T08:59:49Z")?);
        Ok(())
    }

    #[test]
    fn source_frontier_after_cursor_and_horizon_ahead_reports_advance_path_not_reached_stage1(
    ) -> Result<()> {
        let (_temp, config) = seed_source_frontier_after_cursor_fixture()?;
        let report = run(&config)?;
        assert_eq!(
            report.runtime_cursor_before_required_window_start,
            Some(true)
        );
        assert_eq!(
            report.observed_swaps_frontier_after_runtime_cursor,
            Some(true)
        );
        assert_eq!(report.recent_raw_frontier_after_runtime_cursor, Some(true));
        assert_eq!(
            report.runtime_cursor_freeze_conclusion,
            CONCLUSION_SOURCE_FRONTIER_EXISTS_BUT_RUNTIME_ADVANCE_NOT_REACHED
        );
        assert!(report.cursor_advancement_can_run_before_publication_state_selection);
        assert!(!report.cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild);
        Ok(())
    }

    #[test]
    fn no_source_frontier_beyond_cursor_reports_no_frontier_stage1() -> Result<()> {
        let (_temp, config) = seed_no_frontier_beyond_cursor_fixture()?;
        let report = run(&config)?;
        assert_eq!(
            report.runtime_cursor_before_required_window_start,
            Some(true)
        );
        assert_eq!(
            report.observed_swaps_frontier_after_runtime_cursor,
            Some(false)
        );
        assert_eq!(report.recent_raw_frontier_after_runtime_cursor, None);
        assert_eq!(
            report.runtime_cursor_freeze_conclusion,
            CONCLUSION_NO_SOURCE_FRONTIER_BEYOND_CURSOR
        );
        Ok(())
    }

    #[test]
    fn missing_optional_journal_path_still_succeeds_with_null_journal_facts_stage1() -> Result<()> {
        let (_temp, config) = seed_no_frontier_beyond_cursor_fixture()?;
        let report = run(&config)?;
        assert!(!report.recent_raw_journal_db_path_exists);
        assert!(!report.recent_raw_journal_accessible);
        assert_eq!(report.recent_raw_journal_observed_swaps_max_ts_utc, None);
        assert!(matches!(
            report.runtime_cursor_freeze_conclusion.as_str(),
            CONCLUSION_NO_SOURCE_FRONTIER_BEYOND_CURSOR
                | CONCLUSION_HORIZON_AHEAD_OF_PERSISTED_CURSOR
                | CONCLUSION_INSUFFICIENT_EVIDENCE
        ));
        Ok(())
    }

    #[test]
    fn json_output_contains_required_fields_stage1() -> Result<()> {
        let (_temp, config) = seed_source_frontier_after_cursor_fixture()?;
        let report = run(&config)?;
        let value = serde_json::to_value(&report)?;
        for key in [
            "config_path",
            "db_path",
            "now_utc",
            "configured_sqlite_path",
            "runtime_cursor_ts_utc",
            "runtime_cursor_slot",
            "runtime_cursor_signature",
            "publication_runtime_mode",
            "publication_reason",
            "publication_last_published_at_utc",
            "publication_updated_at_utc",
            "observed_swaps_max_ts_utc",
            "wallet_activity_days_max_day_utc",
            "recent_raw_journal_db_path_from_config",
            "recent_raw_journal_db_path_exists",
            "recent_raw_journal_accessible",
            "recent_raw_journal_observed_swaps_max_ts_utc",
            "required_window_start_utc",
            "runtime_cursor_before_required_window_start",
            "observed_swaps_frontier_after_runtime_cursor",
            "recent_raw_frontier_after_runtime_cursor",
            "runtime_cursor_lag_seconds_vs_observed_swaps_frontier",
            "runtime_cursor_lag_days_vs_observed_swaps_frontier",
            "runtime_cursor_load_surface",
            "runtime_cursor_advance_persist_surface",
            "runtime_cursor_advance_surface",
            "raw_window_unusable_no_recent_published_universe_surface",
            "cursor_advancement_depends_on_successful_raw_window_scoring_or_rebuild",
            "cursor_advancement_can_run_before_publication_state_selection",
            "runtime_cursor_freeze_conclusion",
        ] {
            assert!(value.get(key).is_some(), "missing JSON field {key}");
        }
        Ok(())
    }

    #[test]
    fn repeated_runs_with_fixed_inputs_are_identical_stage1() -> Result<()> {
        let (_temp, config) = seed_source_frontier_after_cursor_fixture()?;
        let first = render_output(&run(&config)?)?;
        let second = render_output(&run(&config)?)?;
        assert_eq!(first, second);
        Ok(())
    }
}
