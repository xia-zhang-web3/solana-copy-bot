use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use copybot_core_types::SwapEvent;
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use serde::Serialize;
use std::cmp::Ordering;
use std::env;
use std::path::PathBuf;
use std::time::{Duration as StdDuration, Instant};

const USAGE: &str = "usage: copybot_recent_raw_journal_catch_up --runtime-db-path <path> --recent-raw-db-path <path> [--max-rows <n>] [--max-seconds <n>] [--json]";

const EVENT: &str = "recent_raw_journal_catch_up";
const DEFAULT_MAX_ROWS: usize = 1_000;
const DEFAULT_MAX_SECONDS: u64 = 30;

const REASON_PROGRESS: &str = "recent_raw_journal_catch_up_bounded_page_committed";
const REASON_PARTIAL_PROGRESS_TIME_BUDGET: &str =
    "recent_raw_journal_catch_up_partial_progress_time_budget_exhausted";
const REASON_NO_PROGRESS: &str = "recent_raw_journal_catch_up_no_runtime_rows_after_journal_cursor";
const REASON_RUNTIME_DB_MISSING: &str = "recent_raw_journal_catch_up_runtime_db_path_missing";
const REASON_RECENT_RAW_DB_MISSING: &str = "recent_raw_journal_catch_up_recent_raw_db_path_missing";
const REASON_RUNTIME_DB_OPEN_FAILED: &str = "recent_raw_journal_catch_up_runtime_db_open_failed";
const REASON_RECENT_RAW_DB_OPEN_FAILED: &str =
    "recent_raw_journal_catch_up_recent_raw_db_open_failed";
const REASON_MISSING_JOURNAL_STATE: &str =
    "recent_raw_journal_catch_up_missing_recent_raw_journal_state";
const REASON_MISSING_JOURNAL_CURSOR: &str =
    "recent_raw_journal_catch_up_missing_recent_raw_journal_cursor";
const REASON_RUNTIME_QUERY_FAILED: &str =
    "recent_raw_journal_catch_up_runtime_observed_swaps_page_query_failed";
const REASON_RUNTIME_PAGE_NON_ADVANCING: &str =
    "recent_raw_journal_catch_up_runtime_page_non_advancing_cursor";
const REASON_TIME_BUDGET_BEFORE_WRITE: &str =
    "recent_raw_journal_catch_up_time_budget_exhausted_before_journal_write";
const REASON_JOURNAL_WRITE_FAILED: &str = "recent_raw_journal_catch_up_journal_write_failed";
const REASON_TIME_BUDGET_BEFORE_PROGRESS: &str =
    "recent_raw_journal_catch_up_time_budget_exhausted_before_progress";
const REASON_JOURNAL_WRITE_ZERO_PROCESSED: &str =
    "recent_raw_journal_catch_up_journal_write_processed_zero_rows";
const REASON_NON_ADVANCING_CURSOR: &str = "recent_raw_journal_catch_up_non_advancing_cursor";

fn main() {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(2);
        }
    };

    let report = run(&config);
    match render_output(&report, config.json) {
        Ok(output) => println!("{output}"),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(2);
        }
    }

    if report.verdict == "fail_closed" {
        std::process::exit(2);
    }
}

#[derive(Debug, Clone)]
struct Config {
    runtime_db_path: PathBuf,
    recent_raw_db_path: PathBuf,
    max_rows: usize,
    max_seconds: u64,
    json: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RecentRawJournalCatchUpReport {
    event: &'static str,
    runtime_db_path: String,
    recent_raw_db_path: String,
    max_rows: usize,
    max_seconds: u64,
    elapsed_ms: u64,
    production_green: bool,
    verdict: String,
    reason: String,
    phase: String,
    changed: bool,
    journal_cursor_before: Option<DiscoveryRuntimeCursor>,
    journal_cursor_after: Option<DiscoveryRuntimeCursor>,
    runtime_page_first_cursor: Option<DiscoveryRuntimeCursor>,
    runtime_page_last_cursor: Option<DiscoveryRuntimeCursor>,
    runtime_rows_read: usize,
    journal_rows_processed: usize,
    journal_rows_inserted: usize,
    read_time_budget_exhausted: bool,
    write_time_budget_exhausted: bool,
    catch_up_complete: bool,
    error: Option<String>,
}

impl RecentRawJournalCatchUpReport {
    fn new(config: &Config) -> Self {
        Self {
            event: EVENT,
            runtime_db_path: config.runtime_db_path.display().to_string(),
            recent_raw_db_path: config.recent_raw_db_path.display().to_string(),
            max_rows: config.max_rows,
            max_seconds: config.max_seconds,
            elapsed_ms: 0,
            production_green: false,
            verdict: "fail_closed".to_string(),
            reason: "recent_raw_journal_catch_up_not_run".to_string(),
            phase: "init".to_string(),
            changed: false,
            journal_cursor_before: None,
            journal_cursor_after: None,
            runtime_page_first_cursor: None,
            runtime_page_last_cursor: None,
            runtime_rows_read: 0,
            journal_rows_processed: 0,
            journal_rows_inserted: 0,
            read_time_budget_exhausted: false,
            write_time_budget_exhausted: false,
            catch_up_complete: false,
            error: None,
        }
    }

    fn finish(mut self, started: Instant) -> Self {
        self.elapsed_ms = elapsed_ms(started);
        self
    }

    fn fail(mut self, phase: &str, reason: &str, error: Option<String>, started: Instant) -> Self {
        self.verdict = "fail_closed".to_string();
        self.phase = phase.to_string();
        self.reason = reason.to_string();
        self.error = error;
        self.finish(started)
    }
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut runtime_db_path: Option<PathBuf> = None;
    let mut recent_raw_db_path: Option<PathBuf> = None;
    let mut max_rows = DEFAULT_MAX_ROWS;
    let mut max_seconds = DEFAULT_MAX_SECONDS;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--runtime-db-path" => {
                runtime_db_path = Some(PathBuf::from(parse_string_arg(
                    "--runtime-db-path",
                    args.next(),
                )?));
            }
            "--recent-raw-db-path" => {
                recent_raw_db_path = Some(PathBuf::from(parse_string_arg(
                    "--recent-raw-db-path",
                    args.next(),
                )?));
            }
            "--max-rows" => max_rows = parse_positive_usize_arg("--max-rows", args.next())?,
            "--max-seconds" => max_seconds = parse_positive_u64_arg("--max-seconds", args.next())?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        runtime_db_path: runtime_db_path
            .ok_or_else(|| anyhow!("missing required --runtime-db-path"))?,
        recent_raw_db_path: recent_raw_db_path
            .ok_or_else(|| anyhow!("missing required --recent-raw-db-path"))?,
        max_rows,
        max_seconds,
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

fn parse_positive_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    let parsed = raw
        .parse::<usize>()
        .with_context(|| format!("invalid integer for {flag}: {raw}"))?;
    if parsed == 0 {
        bail!("{flag} must be positive");
    }
    Ok(parsed)
}

fn parse_positive_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    let parsed = raw
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}: {raw}"))?;
    if parsed == 0 {
        bail!("{flag} must be positive");
    }
    Ok(parsed)
}

fn run(config: &Config) -> RecentRawJournalCatchUpReport {
    let started = Instant::now();
    let deadline = started
        .checked_add(StdDuration::from_secs(config.max_seconds))
        .unwrap_or(started);
    let mut report = RecentRawJournalCatchUpReport::new(config);

    if !config.runtime_db_path.exists() {
        return report.fail("open_runtime_db", REASON_RUNTIME_DB_MISSING, None, started);
    }
    if !config.recent_raw_db_path.exists() {
        return report.fail(
            "open_recent_raw_journal_db",
            REASON_RECENT_RAW_DB_MISSING,
            None,
            started,
        );
    }

    let runtime_store = match SqliteStore::open_read_only(&config.runtime_db_path) {
        Ok(store) => store,
        Err(error) => {
            return report.fail(
                "open_runtime_db",
                REASON_RUNTIME_DB_OPEN_FAILED,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    let recent_raw_state_store = match SqliteStore::open_read_only(&config.recent_raw_db_path) {
        Ok(store) => store,
        Err(error) => {
            return report.fail(
                "open_recent_raw_journal_db",
                REASON_RECENT_RAW_DB_OPEN_FAILED,
                Some(format!("{error:#}")),
                started,
            );
        }
    };

    let journal_state =
        match recent_raw_state_store.recent_raw_journal_state_cached_read_only_required() {
            Ok(state) => state,
            Err(error) => {
                return report.fail(
                    "load_recent_raw_journal_state",
                    REASON_MISSING_JOURNAL_STATE,
                    Some(format!("{error:#}")),
                    started,
                );
            }
        };
    drop(recent_raw_state_store);
    let Some(journal_cursor_before) = journal_state.covered_through_cursor else {
        return report.fail(
            "load_recent_raw_journal_state",
            REASON_MISSING_JOURNAL_CURSOR,
            None,
            started,
        );
    };
    report.journal_cursor_before = Some(journal_cursor_before.clone());

    let (swaps, read_time_budget_exhausted) = match runtime_store
        .load_observed_swaps_after_cursor_page_read_only(
            &journal_cursor_before,
            config.max_rows,
            deadline,
        ) {
        Ok(result) => result,
        Err(error) => {
            return report.fail(
                "read_runtime_observed_swaps_page",
                REASON_RUNTIME_QUERY_FAILED,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    report.read_time_budget_exhausted = read_time_budget_exhausted;
    report.runtime_rows_read = swaps.len();
    report.runtime_page_first_cursor = swaps.first().map(cursor_from_swap);
    report.runtime_page_last_cursor = swaps.last().map(cursor_from_swap);

    if swaps.is_empty() {
        if read_time_budget_exhausted {
            return report.fail(
                "read_runtime_observed_swaps_page",
                REASON_TIME_BUDGET_BEFORE_PROGRESS,
                None,
                started,
            );
        }
        report.verdict = "no_progress".to_string();
        report.reason = REASON_NO_PROGRESS.to_string();
        report.phase = "read_runtime_observed_swaps_page".to_string();
        report.catch_up_complete = true;
        return report.finish(started);
    }

    let runtime_page_last_cursor = report
        .runtime_page_last_cursor
        .as_ref()
        .expect("runtime page cursor present when swaps are present");
    if cursor_cmp(runtime_page_last_cursor, &journal_cursor_before) != Ordering::Greater {
        return report.fail(
            "read_runtime_observed_swaps_page",
            REASON_RUNTIME_PAGE_NON_ADVANCING,
            None,
            started,
        );
    }

    if Instant::now() >= deadline {
        return report.fail(
            "write_recent_raw_journal_page",
            REASON_TIME_BUDGET_BEFORE_WRITE,
            None,
            started,
        );
    }

    let recent_raw_store = match SqliteStore::open(&config.recent_raw_db_path) {
        Ok(store) => store,
        Err(error) => {
            return report.fail(
                "open_recent_raw_journal_db_for_write",
                REASON_RECENT_RAW_DB_OPEN_FAILED,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    let (summary, write_time_budget_exhausted) = match recent_raw_store
        .insert_recent_raw_journal_batch_bulk_with_deadline(&swaps, Utc::now(), deadline)
    {
        Ok(result) => result,
        Err(error) => {
            return report.fail(
                "write_recent_raw_journal_page",
                REASON_JOURNAL_WRITE_FAILED,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    report.write_time_budget_exhausted = write_time_budget_exhausted;
    report.journal_rows_processed = summary.recent_raw_bulk_rows_processed;
    report.journal_rows_inserted = summary.recent_raw_bulk_rows_inserted;
    report.journal_cursor_after = summary.covered_through_cursor.clone();

    if summary.recent_raw_bulk_rows_processed == 0 {
        let reason = if write_time_budget_exhausted {
            REASON_TIME_BUDGET_BEFORE_PROGRESS
        } else {
            REASON_JOURNAL_WRITE_ZERO_PROCESSED
        };
        return report.fail("write_recent_raw_journal_page", reason, None, started);
    }

    let Some(journal_cursor_after) = report.journal_cursor_after.as_ref() else {
        return report.fail(
            "verify_recent_raw_journal_state_advance",
            REASON_NON_ADVANCING_CURSOR,
            None,
            started,
        );
    };
    if cursor_cmp(journal_cursor_after, &journal_cursor_before) != Ordering::Greater {
        return report.fail(
            "verify_recent_raw_journal_state_advance",
            REASON_NON_ADVANCING_CURSOR,
            None,
            started,
        );
    }

    report.changed = true;
    report.phase = "write_recent_raw_journal_page".to_string();
    if write_time_budget_exhausted && summary.recent_raw_bulk_rows_processed < swaps.len() {
        report.verdict = "partial_progress".to_string();
        report.reason = REASON_PARTIAL_PROGRESS_TIME_BUDGET.to_string();
        report.catch_up_complete = false;
    } else {
        report.verdict = "progress".to_string();
        report.reason = REASON_PROGRESS.to_string();
        report.catch_up_complete = swaps.len() < config.max_rows && !read_time_budget_exhausted;
    }
    report.finish(started)
}

fn render_output(report: &RecentRawJournalCatchUpReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report).context("failed to serialize catch-up report");
    }
    Ok(format!(
        "{event}: verdict={verdict} reason={reason} changed={changed} runtime_rows_read={runtime_rows_read} journal_rows_processed={journal_rows_processed} journal_rows_inserted={journal_rows_inserted} production_green=false",
        event = report.event,
        verdict = report.verdict,
        reason = report.reason,
        changed = report.changed,
        runtime_rows_read = report.runtime_rows_read,
        journal_rows_processed = report.journal_rows_processed,
        journal_rows_inserted = report.journal_rows_inserted,
    ))
}

fn cursor_from_swap(swap: &SwapEvent) -> DiscoveryRuntimeCursor {
    DiscoveryRuntimeCursor {
        ts_utc: swap.ts_utc,
        slot: swap.slot,
        signature: swap.signature.clone(),
    }
}

fn cursor_cmp(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use chrono::{DateTime, Duration};
    use copybot_core_types::SwapEvent;
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = env::temp_dir().join(format!("{prefix}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn migration_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
    }

    fn migrated_store(path: &Path) -> Result<SqliteStore> {
        let mut store = SqliteStore::open(path)?;
        store.run_migrations(&migration_dir())?;
        Ok(store)
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid timestamp")
            .with_timezone(&Utc)
    }

    fn swap(signature: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: "wallet-catch-up".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenCatchUp111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    fn seed_runtime(path: &Path, swaps: &[SwapEvent]) -> Result<()> {
        let store = migrated_store(path)?;
        store.insert_observed_swaps_batch_with_activity_days(swaps)?;
        Ok(())
    }

    fn seed_journal(path: &Path, swaps: &[SwapEvent]) -> Result<()> {
        let store = migrated_store(path)?;
        store.insert_recent_raw_journal_batch_bulk_with_deadline(
            swaps,
            Utc::now(),
            Instant::now() + StdDuration::from_secs(5),
        )?;
        Ok(())
    }

    fn config(runtime_db_path: PathBuf, recent_raw_db_path: PathBuf) -> Config {
        Config {
            runtime_db_path,
            recent_raw_db_path,
            max_rows: 100,
            max_seconds: 5,
            json: true,
        }
    }

    #[test]
    fn catch_up_copies_bounded_runtime_rows_and_advances_journal_cursor_after_commit() -> Result<()>
    {
        let dir = temp_dir("recent-raw-catch-up-success");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base = ts("2026-04-29T08:00:00Z");
        let base_swap = swap("sig-catch-up-base", base, 100);
        let runtime_swaps = vec![
            base_swap.clone(),
            swap("sig-catch-up-a", base + Duration::seconds(1), 101),
            swap("sig-catch-up-b", base + Duration::seconds(2), 102),
        ];
        seed_runtime(&runtime_db, &runtime_swaps)?;
        seed_journal(&recent_raw_db, std::slice::from_ref(&base_swap))?;

        let report = run(&config(runtime_db, recent_raw_db.clone()));

        assert_eq!(report.verdict, "progress");
        assert_eq!(report.reason, REASON_PROGRESS);
        assert!(report.changed);
        assert_eq!(report.runtime_rows_read, 2);
        assert_eq!(report.journal_rows_processed, 2);
        assert_eq!(report.journal_rows_inserted, 2);
        assert_eq!(
            report
                .journal_cursor_after
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-catch-up-b")
        );
        let read_only = SqliteStore::open_read_only(&recent_raw_db)?;
        let state = read_only.recent_raw_journal_state_cached_read_only_required()?;
        assert_eq!(
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-catch-up-b")
        );
        Ok(())
    }

    #[test]
    fn catch_up_respects_max_rows_page_boundary() -> Result<()> {
        let dir = temp_dir("recent-raw-catch-up-page");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base = ts("2026-04-29T08:00:00Z");
        let base_swap = swap("sig-catch-up-page-base", base, 200);
        let runtime_swaps = vec![
            base_swap.clone(),
            swap("sig-catch-up-page-a", base + Duration::seconds(1), 201),
            swap("sig-catch-up-page-b", base + Duration::seconds(2), 202),
        ];
        seed_runtime(&runtime_db, &runtime_swaps)?;
        seed_journal(&recent_raw_db, std::slice::from_ref(&base_swap))?;
        let mut config = config(runtime_db, recent_raw_db.clone());
        config.max_rows = 1;

        let report = run(&config);

        assert_eq!(report.verdict, "progress");
        assert_eq!(report.runtime_rows_read, 1);
        assert_eq!(report.journal_rows_processed, 1);
        assert_eq!(
            report
                .journal_cursor_after
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-catch-up-page-a")
        );
        let read_only = SqliteStore::open_read_only(&recent_raw_db)?;
        let state = read_only.recent_raw_journal_state_cached_read_only_required()?;
        assert_eq!(
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-catch-up-page-a")
        );
        Ok(())
    }

    #[test]
    fn catch_up_reports_no_progress_when_runtime_has_no_rows_after_journal_cursor() -> Result<()> {
        let dir = temp_dir("recent-raw-catch-up-no-progress");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base_swap = swap(
            "sig-catch-up-no-progress-base",
            ts("2026-04-29T08:00:00Z"),
            300,
        );
        seed_runtime(&runtime_db, std::slice::from_ref(&base_swap))?;
        seed_journal(&recent_raw_db, std::slice::from_ref(&base_swap))?;

        let report = run(&config(runtime_db, recent_raw_db));

        assert_eq!(report.verdict, "no_progress");
        assert_eq!(report.reason, REASON_NO_PROGRESS);
        assert!(!report.changed);
        assert_eq!(report.runtime_rows_read, 0);
        assert!(report.production_green == false);
        Ok(())
    }

    #[test]
    fn catch_up_fails_closed_when_recent_raw_journal_state_is_missing() -> Result<()> {
        let dir = temp_dir("recent-raw-catch-up-missing-state");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base_swap = swap(
            "sig-catch-up-missing-state-base",
            ts("2026-04-29T08:00:00Z"),
            400,
        );
        seed_runtime(&runtime_db, std::slice::from_ref(&base_swap))?;
        let _recent_raw_store = migrated_store(&recent_raw_db)?;

        let report = run(&config(runtime_db, recent_raw_db));

        assert_eq!(report.verdict, "fail_closed");
        assert_eq!(report.reason, REASON_MISSING_JOURNAL_STATE);
        assert!(!report.changed);
        assert!(report.production_green == false);
        Ok(())
    }

    #[test]
    fn catch_up_fails_closed_on_runtime_page_query_error() -> Result<()> {
        let dir = temp_dir("recent-raw-catch-up-query-error");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base_swap = swap(
            "sig-catch-up-query-error-base",
            ts("2026-04-29T08:00:00Z"),
            450,
        );
        let conn = rusqlite::Connection::open(&runtime_db)?;
        conn.execute_batch(
            "CREATE TABLE observed_swaps(
                signature TEXT PRIMARY KEY,
                wallet_id TEXT NOT NULL,
                dex TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                qty_in REAL NOT NULL,
                qty_out REAL NOT NULL,
                qty_in_raw TEXT,
                qty_in_decimals INTEGER,
                qty_out_raw TEXT,
                qty_out_decimals INTEGER,
                slot INTEGER NOT NULL,
                ts TEXT NOT NULL
            );",
        )?;
        seed_journal(&recent_raw_db, std::slice::from_ref(&base_swap))?;

        let report = run(&config(runtime_db, recent_raw_db));

        assert_eq!(report.verdict, "fail_closed");
        assert_eq!(report.reason, REASON_RUNTIME_QUERY_FAILED);
        assert!(!report.changed);
        assert!(report.production_green == false);
        Ok(())
    }

    #[test]
    fn catch_up_write_error_does_not_advance_recent_raw_journal_state() -> Result<()> {
        let dir = temp_dir("recent-raw-catch-up-write-failure");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base = ts("2026-04-29T08:00:00Z");
        let base_swap = swap("sig-catch-up-write-failure-base", base, 500);
        let next_swap = swap(
            "sig-catch-up-write-failure-next",
            base + Duration::seconds(1),
            501,
        );
        seed_runtime(&runtime_db, &[base_swap.clone(), next_swap])?;
        seed_journal(&recent_raw_db, std::slice::from_ref(&base_swap))?;
        let conn = rusqlite::Connection::open(&recent_raw_db)?;
        conn.execute_batch(
            "CREATE TRIGGER recent_raw_catch_up_fail_insert
                 BEFORE INSERT ON observed_swaps
                 BEGIN
                     SELECT RAISE(FAIL, 'synthetic recent_raw catch-up write failure');
                 END;",
        )
        .context("failed to install write failure trigger")?;

        let report = run(&config(runtime_db, recent_raw_db.clone()));

        assert_eq!(report.verdict, "fail_closed");
        assert_eq!(report.reason, REASON_JOURNAL_WRITE_FAILED);
        assert!(!report.changed);
        let read_only = SqliteStore::open_read_only(&recent_raw_db)?;
        let state = read_only.recent_raw_journal_state_cached_read_only_required()?;
        assert_eq!(
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-catch-up-write-failure-base")
        );
        Ok(())
    }

    #[test]
    fn catch_up_json_output_never_reports_production_green() -> Result<()> {
        let dir = temp_dir("recent-raw-catch-up-json");
        let runtime_db = dir.join("runtime.sqlite");
        let recent_raw_db = dir.join("recent_raw.sqlite");
        let base_swap = swap("sig-catch-up-json-base", ts("2026-04-29T08:00:00Z"), 600);
        seed_runtime(&runtime_db, std::slice::from_ref(&base_swap))?;
        seed_journal(&recent_raw_db, std::slice::from_ref(&base_swap))?;
        let report = run(&config(runtime_db, recent_raw_db));

        let rendered = render_output(&report, true)?;
        let value: serde_json::Value = serde_json::from_str(&rendered)?;
        assert_eq!(value["production_green"], false);
        Ok(())
    }
}
