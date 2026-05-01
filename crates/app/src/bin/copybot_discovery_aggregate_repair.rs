use anyhow::{anyhow, bail, Context, Result};
use copybot_config::{load_from_path, AppConfig};
use copybot_core_types::SwapEvent;
use copybot_storage::{
    is_retryable_sqlite_anyhow_error, DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor,
    SqliteStore,
};
use serde::Serialize;
#[cfg(test)]
use std::cell::RefCell;
use std::cmp::Ordering;
use std::env;
use std::path::PathBuf;
use std::time::{Duration as StdDuration, Instant};

const USAGE: &str = "usage: copybot_discovery_aggregate_repair --config <live config toml> --db-path <runtime sqlite db> [--max-pages <n>] [--page-size <n>] [--max-seconds <n>] [--json] [--dry-run]";

const DEFAULT_MAX_PAGES: usize = 256;
const DEFAULT_PAGE_SIZE: usize = 512;
const DEFAULT_MAX_SECONDS: u64 = 120;
const COMMIT_GROUP_MAX_PAGES: usize = 8;
const COMMIT_GROUP_MAX_ROWS: usize = 4_096;
const OPERATOR_BUSY_TIMEOUT: StdDuration = StdDuration::from_millis(250);

const REASON_NOT_RUN: &str = "discovery_aggregate_repair_not_run";
const REASON_DRY_RUN: &str = "discovery_aggregate_repair_dry_run";
const REASON_PROGRESS: &str = "discovery_aggregate_repair_bounded_progress";
const REASON_COMPLETED: &str = "discovery_aggregate_repair_target_reached_latch_cleared";
const REASON_MAX_PAGES: &str = "discovery_aggregate_repair_max_pages_reached";
const REASON_MAX_SECONDS: &str = "discovery_aggregate_repair_max_seconds_reached";
const REASON_NO_PROGRESS: &str = "discovery_aggregate_repair_no_rows_before_target";
const REASON_DB_MISSING: &str = "discovery_aggregate_repair_db_path_missing";
const REASON_DB_OPEN_FAILED: &str = "discovery_aggregate_repair_db_open_failed";
const REASON_CONFIG_LOAD_FAILED: &str = "discovery_aggregate_repair_config_load_failed";
const REASON_NO_GAP: &str = "discovery_aggregate_repair_materialization_gap_missing";
const REASON_NO_TARGET: &str = "discovery_aggregate_repair_persisted_target_missing";
const REASON_TARGET_GAP_MISMATCH: &str = "discovery_aggregate_repair_persisted_target_gap_mismatch";
const REASON_NO_COVERED_THROUGH: &str = "discovery_aggregate_repair_covered_through_missing";
const REASON_EXACT_GAP_MISSING: &str = "discovery_aggregate_repair_exact_gap_row_missing";
const REASON_LATCH_NOT_CLEARED: &str = "discovery_aggregate_repair_latch_not_cleared";
const REASON_TARGET_UNREACHABLE: &str =
    "discovery_aggregate_repair_target_row_missing_or_unreachable";
const REASON_CONCURRENT_COVERED_THROUGH_ADVANCED: &str =
    "discovery_aggregate_repair_concurrent_covered_through_advanced";
const REASON_SQLITE_LOCK_RETRYABLE: &str = "discovery_aggregate_repair_sqlite_lock_retryable";
const REASON_UNKNOWN_ERROR: &str = "discovery_aggregate_repair_unknown_error";

const ACTION_RERUN: &str = "rerun copybot_discovery_aggregate_repair with the same db-path";
const ACTION_READINESS: &str = "rerun aggregate_readiness_status and keep production green gated";
const ACTION_FIX_INPUT: &str = "inspect discovery_scoring_state and observed_swaps evidence";
const ACTION_DRY_RUN: &str = "run without --dry-run to apply bounded repair";

#[cfg(test)]
thread_local! {
    static TEST_ADVANCE_COVERED_THROUGH_BEFORE_COMMIT: RefCell<Option<DiscoveryRuntimeCursor>> =
        const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
struct Cli {
    config_path: PathBuf,
    db_path: PathBuf,
    max_pages: usize,
    page_size: usize,
    max_seconds: u64,
    dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AggregateRepairReport {
    ok: bool,
    production_green: bool,
    dry_run: bool,
    reason: String,
    materialization_gap_cursor: Option<DiscoveryRuntimeCursor>,
    persisted_repair_target: Option<DiscoveryRuntimeCursor>,
    start_covered_through: Option<DiscoveryRuntimeCursor>,
    end_covered_through: Option<DiscoveryRuntimeCursor>,
    pages_processed: usize,
    rows_processed: usize,
    commit_groups_processed: usize,
    reached_target: bool,
    latch_cleared: bool,
    elapsed_ms: u64,
    recommended_next_action: String,
    error: Option<String>,
}

impl AggregateRepairReport {
    fn new(cli: &Cli) -> Self {
        Self {
            ok: false,
            production_green: false,
            dry_run: cli.dry_run,
            reason: REASON_NOT_RUN.to_string(),
            materialization_gap_cursor: None,
            persisted_repair_target: None,
            start_covered_through: None,
            end_covered_through: None,
            pages_processed: 0,
            rows_processed: 0,
            commit_groups_processed: 0,
            reached_target: false,
            latch_cleared: false,
            elapsed_ms: 0,
            recommended_next_action: ACTION_FIX_INPUT.to_string(),
            error: None,
        }
    }

    fn finish(mut self, started: Instant) -> Self {
        self.elapsed_ms = elapsed_ms(started);
        self
    }

    fn fail(mut self, reason: &str, action: &str, error: Option<String>, started: Instant) -> Self {
        self.ok = false;
        self.reason = reason.to_string();
        self.recommended_next_action = action.to_string();
        self.error = error;
        self.finish(started)
    }
}

#[derive(Debug, Clone)]
struct CommitGroup {
    rows: Vec<SwapEvent>,
    pages: usize,
    reached_target: bool,
    saw_after_target: bool,
    gap_observed: bool,
    time_budget_exhausted: bool,
}

fn main() {
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(Some(cli)) => run(&cli),
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(2);
        }
    };

    if let Err(error) = render_report(&report) {
        eprintln!("{error:#}");
        std::process::exit(2);
    }
}

fn render_report(report: &AggregateRepairReport) -> Result<()> {
    println!("{}", serde_json::to_string(report)?);
    Ok(())
}

fn parse_args_from<I>(args: I) -> Result<Option<Cli>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut max_pages = DEFAULT_MAX_PAGES;
    let mut page_size = DEFAULT_PAGE_SIZE;
    let mut max_seconds = DEFAULT_MAX_SECONDS;
    let mut json = false;
    let mut dry_run = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--max-pages" => max_pages = parse_positive_usize_arg("--max-pages", args.next())?,
            "--page-size" => page_size = parse_positive_usize_arg("--page-size", args.next())?,
            "--max-seconds" => max_seconds = parse_u64_arg("--max-seconds", args.next())?,
            "--json" => json = true,
            "--dry-run" => dry_run = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if !json {
        bail!("--json is required");
    }

    Ok(Some(Cli {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db-path"))?,
        max_pages,
        page_size,
        max_seconds,
        dry_run,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
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

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}: {raw}"))
}

fn run(cli: &Cli) -> AggregateRepairReport {
    let started = Instant::now();
    let deadline = started
        .checked_add(StdDuration::from_secs(cli.max_seconds))
        .unwrap_or(started);
    let mut report = AggregateRepairReport::new(cli);

    if !cli.db_path.exists() {
        return report.fail(REASON_DB_MISSING, ACTION_FIX_INPUT, None, started);
    }

    let loaded_config = match load_from_path(&cli.config_path) {
        Ok(config) => config,
        Err(error) => {
            return report.fail(
                REASON_CONFIG_LOAD_FAILED,
                ACTION_FIX_INPUT,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    let aggregate_write_config = aggregate_write_config_from_app_config(&loaded_config);

    let store = match open_store(cli) {
        Ok(store) => store,
        Err(error) => {
            return report.fail(
                REASON_DB_OPEN_FAILED,
                ACTION_FIX_INPUT,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    if let Err(error) = store.set_busy_timeout(OPERATOR_BUSY_TIMEOUT) {
        return report.fail(
            REASON_DB_OPEN_FAILED,
            ACTION_FIX_INPUT,
            Some(format!("{error:#}")),
            started,
        );
    }

    let gap_cursor = match store.load_discovery_scoring_materialization_gap_cursor() {
        Ok(Some(cursor)) => cursor,
        Ok(None) => return report.fail(REASON_NO_GAP, ACTION_READINESS, None, started),
        Err(error) => {
            return report.fail(
                REASON_UNKNOWN_ERROR,
                ACTION_FIX_INPUT,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    report.materialization_gap_cursor = Some(gap_cursor.clone());

    let (target_gap_cursor, repair_target) =
        match store.load_discovery_scoring_materialization_gap_repair_target() {
            Ok(Some(target)) => target,
            Ok(None) => return report.fail(REASON_NO_TARGET, ACTION_FIX_INPUT, None, started),
            Err(error) => {
                return report.fail(
                    REASON_UNKNOWN_ERROR,
                    ACTION_FIX_INPUT,
                    Some(format!("{error:#}")),
                    started,
                );
            }
        };
    report.persisted_repair_target = Some(repair_target.clone());

    if compare_cursors(&target_gap_cursor, &gap_cursor) != Ordering::Equal {
        return report.fail(REASON_TARGET_GAP_MISMATCH, ACTION_FIX_INPUT, None, started);
    }

    let start_covered_through = match store.load_discovery_scoring_covered_through_cursor() {
        Ok(Some(cursor)) => cursor,
        Ok(None) => return report.fail(REASON_NO_COVERED_THROUGH, ACTION_FIX_INPUT, None, started),
        Err(error) => {
            return report.fail(
                REASON_UNKNOWN_ERROR,
                ACTION_FIX_INPUT,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    report.start_covered_through = Some(start_covered_through.clone());
    report.end_covered_through = Some(start_covered_through.clone());

    let exact_gap_exists = match store.observed_swap_exact_cursor_exists(&gap_cursor) {
        Ok(exists) => exists,
        Err(error) => {
            return report.fail(
                REASON_UNKNOWN_ERROR,
                ACTION_FIX_INPUT,
                Some(format!("{error:#}")),
                started,
            );
        }
    };
    if !exact_gap_exists {
        return report.fail(REASON_EXACT_GAP_MISSING, ACTION_FIX_INPUT, None, started);
    }

    if compare_cursors(&start_covered_through, &repair_target) != Ordering::Less {
        report.reached_target = true;
        return clear_latch_after_target_if_proven(report, &store, &gap_cursor, cli, started);
    }

    if cli.dry_run {
        report.ok = true;
        report.reason = REASON_DRY_RUN.to_string();
        report.recommended_next_action = ACTION_DRY_RUN.to_string();
        return report.finish(started);
    }

    let mut cursor = start_covered_through.clone();
    let mut gap_observed = compare_cursors(&cursor, &gap_cursor) != Ordering::Less;
    let mut stopped_for_budget = false;
    let mut stopped_for_page_limit = false;

    while compare_cursors(&cursor, &repair_target) == Ordering::Less
        && report.pages_processed < cli.max_pages
    {
        if Instant::now() >= deadline {
            stopped_for_budget = true;
            break;
        }
        let remaining_pages = cli.max_pages.saturating_sub(report.pages_processed);
        let group = match collect_commit_group(
            &store,
            &cursor,
            &gap_cursor,
            &repair_target,
            cli.page_size,
            remaining_pages,
            gap_observed,
            deadline,
        ) {
            Ok(group) => group,
            Err(error) => {
                return report.fail(
                    REASON_UNKNOWN_ERROR,
                    ACTION_FIX_INPUT,
                    Some(format!("{error:#}")),
                    started,
                );
            }
        };
        if group.time_budget_exhausted {
            stopped_for_budget = true;
        }
        if group.saw_after_target && !group.reached_target {
            return report.fail(REASON_TARGET_UNREACHABLE, ACTION_FIX_INPUT, None, started);
        }
        if group.rows.is_empty() {
            break;
        }
        if !gap_observed && group_passes_cursor(&group.rows, &gap_cursor) && !group.gap_observed {
            return report.fail(REASON_EXACT_GAP_MISSING, ACTION_FIX_INPUT, None, started);
        }

        if Instant::now() >= deadline {
            stopped_for_budget = true;
            break;
        }
        let last_cursor = cursor_for_swap(group.rows.last().expect("non-empty group"));
        if let Err(error) = maybe_advance_covered_through_before_commit_for_tests(&store) {
            return report.fail(
                REASON_UNKNOWN_ERROR,
                ACTION_FIX_INPUT,
                Some(format!("{error:#}")),
                started,
            );
        }
        let commit_outcome = match store.apply_discovery_scoring_repair_commit_group(
            &group.rows,
            &aggregate_write_config,
            &cursor,
            &last_cursor,
        ) {
            Ok(outcome) => outcome,
            Err(error) => return classify_write_error(report, error, started),
        };
        if commit_outcome != "committed" {
            let current_covered = match store.load_discovery_scoring_covered_through_cursor() {
                Ok(Some(cursor)) => cursor,
                Ok(None) => {
                    return report.fail(REASON_NO_COVERED_THROUGH, ACTION_FIX_INPUT, None, started);
                }
                Err(error) => {
                    return report.fail(
                        REASON_UNKNOWN_ERROR,
                        ACTION_FIX_INPUT,
                        Some(format!("{error:#}")),
                        started,
                    );
                }
            };
            report.end_covered_through = Some(current_covered.clone());
            if compare_cursors(&current_covered, &repair_target) != Ordering::Less {
                report.reached_target = true;
                return clear_latch_after_target_if_proven(
                    report,
                    &store,
                    &gap_cursor,
                    cli,
                    started,
                );
            }
            report.ok = true;
            report.reason = if commit_outcome == "concurrent_progress" {
                REASON_CONCURRENT_COVERED_THROUGH_ADVANCED.to_string()
            } else {
                REASON_PROGRESS.to_string()
            };
            report.recommended_next_action = ACTION_RERUN.to_string();
            return report.finish(started);
        }

        cursor = last_cursor;
        gap_observed |= group.gap_observed;
        report.end_covered_through = Some(cursor.clone());
        report.pages_processed = report.pages_processed.saturating_add(group.pages);
        report.rows_processed = report.rows_processed.saturating_add(group.rows.len());
        report.commit_groups_processed = report.commit_groups_processed.saturating_add(1);
        report.reached_target = compare_cursors(&cursor, &repair_target) != Ordering::Less;

        if report.reached_target || group.time_budget_exhausted {
            stopped_for_budget |= group.time_budget_exhausted;
            break;
        }
    }

    if report.pages_processed >= cli.max_pages && !report.reached_target {
        stopped_for_page_limit = true;
    }

    if report.reached_target {
        if gap_observed {
            return clear_latch_after_target_if_proven(report, &store, &gap_cursor, cli, started);
        }
        return report.fail(REASON_EXACT_GAP_MISSING, ACTION_FIX_INPUT, None, started);
    }

    if report.rows_processed > 0 {
        report.ok = true;
        report.reason = if stopped_for_page_limit {
            REASON_MAX_PAGES.to_string()
        } else if stopped_for_budget {
            REASON_MAX_SECONDS.to_string()
        } else {
            REASON_PROGRESS.to_string()
        };
        report.recommended_next_action = ACTION_RERUN.to_string();
        return report.finish(started);
    }

    if stopped_for_budget {
        report.fail(REASON_MAX_SECONDS, ACTION_RERUN, None, started)
    } else {
        report.fail(REASON_NO_PROGRESS, ACTION_FIX_INPUT, None, started)
    }
}

#[cfg(test)]
fn set_test_advance_covered_through_before_commit(cursor: Option<DiscoveryRuntimeCursor>) {
    TEST_ADVANCE_COVERED_THROUGH_BEFORE_COMMIT.with(|value| {
        *value.borrow_mut() = cursor;
    });
}

#[cfg(test)]
fn maybe_advance_covered_through_before_commit_for_tests(store: &SqliteStore) -> Result<()> {
    let cursor = TEST_ADVANCE_COVERED_THROUGH_BEFORE_COMMIT.with(|value| value.borrow_mut().take());
    if let Some(cursor) = cursor {
        store.set_discovery_scoring_covered_through_cursor(&cursor)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn maybe_advance_covered_through_before_commit_for_tests(_store: &SqliteStore) -> Result<()> {
    Ok(())
}

fn open_store(cli: &Cli) -> Result<SqliteStore> {
    if cli.dry_run {
        SqliteStore::open_read_only(&cli.db_path)
    } else {
        SqliteStore::open(&cli.db_path)
    }
}

fn aggregate_write_config_from_app_config(config: &AppConfig) -> DiscoveryAggregateWriteConfig {
    DiscoveryAggregateWriteConfig {
        max_tx_per_minute: config.discovery.max_tx_per_minute,
        rug_lookahead_seconds: config.discovery.rug_lookahead_seconds as u32,
        helius_http_url: None,
        min_token_age_hint_seconds: Some(config.shadow.min_token_age_seconds),
    }
}

fn clear_latch_after_target_if_proven(
    mut report: AggregateRepairReport,
    store: &SqliteStore,
    gap_cursor: &DiscoveryRuntimeCursor,
    cli: &Cli,
    started: Instant,
) -> AggregateRepairReport {
    if cli.dry_run {
        report.ok = true;
        report.reason = REASON_DRY_RUN.to_string();
        report.recommended_next_action = ACTION_DRY_RUN.to_string();
        return report.finish(started);
    }

    if let Err(error) =
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(gap_cursor)
    {
        return classify_write_error(report, error, started);
    }
    match store.load_discovery_scoring_materialization_gap_cursor() {
        Ok(None) => {
            report.latch_cleared = true;
            report.ok = true;
            report.reason = REASON_COMPLETED.to_string();
            report.recommended_next_action = ACTION_READINESS.to_string();
            report.finish(started)
        }
        Ok(Some(_)) => report.fail(REASON_LATCH_NOT_CLEARED, ACTION_FIX_INPUT, None, started),
        Err(error) => report.fail(
            REASON_UNKNOWN_ERROR,
            ACTION_FIX_INPUT,
            Some(format!("{error:#}")),
            started,
        ),
    }
}

fn classify_write_error(
    report: AggregateRepairReport,
    error: anyhow::Error,
    started: Instant,
) -> AggregateRepairReport {
    let retryable = is_retryable_sqlite_anyhow_error(&error);
    let reason = if retryable {
        REASON_SQLITE_LOCK_RETRYABLE
    } else {
        REASON_UNKNOWN_ERROR
    };
    report.fail(reason, ACTION_RERUN, Some(format!("{error:#}")), started)
}

fn collect_commit_group(
    store: &SqliteStore,
    start_cursor: &DiscoveryRuntimeCursor,
    gap_cursor: &DiscoveryRuntimeCursor,
    repair_target: &DiscoveryRuntimeCursor,
    page_size: usize,
    remaining_pages: usize,
    gap_already_observed: bool,
    deadline: Instant,
) -> Result<CommitGroup> {
    let mut rows = Vec::with_capacity(page_size.min(COMMIT_GROUP_MAX_ROWS));
    let mut pages = 0usize;
    let mut cursor = start_cursor.clone();
    let mut reached_target = false;
    let mut saw_after_target = false;
    let mut gap_observed = gap_already_observed;
    let mut time_budget_exhausted = false;
    let page_limit = page_size.max(1);
    let group_page_limit = remaining_pages.max(1).min(COMMIT_GROUP_MAX_PAGES);

    while pages < group_page_limit && rows.len() < COMMIT_GROUP_MAX_ROWS {
        if Instant::now() >= deadline {
            time_budget_exhausted = true;
            break;
        }
        let remaining_rows = COMMIT_GROUP_MAX_ROWS.saturating_sub(rows.len()).max(1);
        let current_page_limit = page_limit.min(remaining_rows);
        let mut page = Vec::with_capacity(current_page_limit);
        let page_result = store.for_each_observed_swap_after_cursor_with_budget(
            cursor.ts_utc,
            cursor.slot,
            cursor.signature.as_str(),
            current_page_limit,
            deadline,
            |swap| {
                let swap_cursor = cursor_for_swap(&swap);
                match compare_cursors(&swap_cursor, repair_target) {
                    Ordering::Greater => {
                        saw_after_target = true;
                        return Ok(());
                    }
                    Ordering::Equal => {
                        reached_target = true;
                    }
                    Ordering::Less => {}
                }
                if compare_cursors(&swap_cursor, gap_cursor) == Ordering::Equal {
                    gap_observed = true;
                }
                page.push(swap);
                Ok(())
            },
        )?;
        time_budget_exhausted |= page_result.time_budget_exhausted;
        if page.is_empty() {
            break;
        }
        cursor = cursor_for_swap(page.last().expect("non-empty page"));
        pages = pages.saturating_add(1);
        rows.extend(page);
        if reached_target
            || saw_after_target
            || page_result.time_budget_exhausted
            || page_result.rows_seen < current_page_limit
        {
            break;
        }
    }

    Ok(CommitGroup {
        rows,
        pages,
        reached_target,
        saw_after_target,
        gap_observed,
        time_budget_exhausted,
    })
}

fn group_passes_cursor(rows: &[SwapEvent], cursor: &DiscoveryRuntimeCursor) -> bool {
    rows.iter()
        .any(|swap| compare_cursors(&cursor_for_swap(swap), cursor) == Ordering::Greater)
}

fn cursor_for_swap(swap: &SwapEvent) -> DiscoveryRuntimeCursor {
    DiscoveryRuntimeCursor {
        ts_utc: swap.ts_utc,
        slot: swap.slot,
        signature: swap.signature.clone(),
    }
}

fn compare_cursors(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn elapsed_ms(started: Instant) -> u64 {
    let elapsed = started.elapsed().as_millis();
    elapsed.min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use rusqlite::Connection;
    use std::path::Path;

    fn unique_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{name}-{}-{}.sqlite",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        ))
    }

    fn remove_sqlite_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        let _ = std::fs::remove_file(format!("{}.toml", path.display()));
    }

    fn migrated_store(name: &str) -> Result<(PathBuf, SqliteStore)> {
        let db_path = unique_db_path(name);
        let mut store = SqliteStore::open(&db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((db_path, store))
    }

    fn test_swap(signature: &str, slot: u64, ts_utc: &str) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-aggregate-repair-operator-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-aggregate-repair-operator-test".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc: DateTime::parse_from_rfc3339(ts_utc)
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    fn seed_repair_fixture(
        name: &str,
        target_index: usize,
    ) -> Result<(
        PathBuf,
        DiscoveryRuntimeCursor,
        DiscoveryRuntimeCursor,
        Vec<SwapEvent>,
    )> {
        let (db_path, store) = migrated_store(name)?;
        let swaps = vec![
            test_swap("sig-covered", 100, "2026-04-29T00:00:00Z"),
            test_swap("sig-gap", 101, "2026-04-29T00:00:01Z"),
            test_swap("sig-mid-1", 102, "2026-04-29T00:00:02Z"),
            test_swap("sig-mid-2", 103, "2026-04-29T00:00:03Z"),
            test_swap("sig-target", 104, "2026-04-29T00:00:04Z"),
        ];
        store.insert_observed_swaps_batch(&swaps)?;
        store.apply_discovery_scoring_batch(
            &[swaps[0].clone()],
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        let covered_cursor = cursor_for_swap(&swaps[0]);
        let gap_cursor = cursor_for_swap(&swaps[1]);
        let target_cursor = cursor_for_swap(&swaps[target_index]);
        store.set_discovery_scoring_covered_through_cursor(&covered_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        store
            .set_discovery_scoring_materialization_gap_repair_target(&gap_cursor, &target_cursor)?;
        Ok((db_path, gap_cursor, target_cursor, swaps))
    }

    fn write_test_config(db_path: &Path, rug_lookahead_seconds: u64) -> Result<PathBuf> {
        let config_path = PathBuf::from(format!("{}.toml", db_path.display()));
        std::fs::write(
            &config_path,
            format!(
                "[discovery]\nmax_tx_per_minute = 17\nrug_lookahead_seconds = {rug_lookahead_seconds}\n[shadow]\nmin_token_age_seconds = 123\n"
            ),
        )
        .with_context(|| format!("failed writing test config {}", config_path.display()))?;
        Ok(config_path)
    }

    fn test_cli(db_path: &Path) -> Result<Cli> {
        test_cli_with_rug_lookahead(db_path, 1)
    }

    fn test_cli_with_rug_lookahead(db_path: &Path, rug_lookahead_seconds: u64) -> Result<Cli> {
        Ok(Cli {
            config_path: write_test_config(db_path, rug_lookahead_seconds)?,
            db_path: db_path.to_path_buf(),
            max_pages: 256,
            page_size: 2,
            max_seconds: 120,
            dry_run: false,
        })
    }

    fn repair_buy_fact_count(db_path: &Path) -> Result<i64> {
        let conn = Connection::open(db_path)?;
        conn.query_row(
            "SELECT COUNT(*)
             FROM wallet_scoring_buy_facts
             WHERE buy_signature IN ('sig-gap', 'sig-mid-1', 'sig-mid-2', 'sig-target')",
            [],
            |row| row.get(0),
        )
        .context("failed counting repair buy facts")
    }

    fn wallet_day_trades(db_path: &Path) -> Result<i64> {
        let conn = Connection::open(db_path)?;
        conn.query_row(
            "SELECT trades
             FROM wallet_scoring_days
             WHERE wallet_id = ?1
               AND activity_day = '2026-04-29'",
            ["wallet-aggregate-repair-operator-test"],
            |row| row.get(0),
        )
        .context("failed loading wallet day trades")
    }

    fn aggregate_config_for_tests() -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig {
            max_tx_per_minute: 17,
            rug_lookahead_seconds: 1,
            helius_http_url: None,
            min_token_age_hint_seconds: Some(123),
        }
    }

    #[test]
    fn cli_requires_config_path() {
        let result = parse_args_from([
            "--db-path".to_string(),
            "runtime.sqlite".to_string(),
            "--json".to_string(),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn valid_bounded_replay_advances_covered_through_but_does_not_clear_before_target() -> Result<()>
    {
        let (db_path, gap_cursor, target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-progress", 4)?;
        let mut cli = test_cli(&db_path)?;
        cli.max_pages = 1;

        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_MAX_PAGES);
        assert_eq!(report.pages_processed, 1);
        assert!(!report.reached_target);
        assert!(!report.latch_cleared);
        let store = SqliteStore::open(&db_path)?;
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor)
        );
        assert!(
            compare_cursors(
                &store
                    .load_discovery_scoring_covered_through_cursor()?
                    .expect("covered"),
                &target_cursor
            ) == Ordering::Less
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn reaches_target_and_clears_latch_only_via_guarded_clear() -> Result<()> {
        let (db_path, _gap_cursor, target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-complete", 4)?;

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_COMPLETED);
        assert!(report.reached_target);
        assert!(report.latch_cleared);
        assert!(!report.production_green);
        let store = SqliteStore::open(&db_path)?;
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(target_cursor)
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_repair_target()?,
            None
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn custom_config_rug_lookahead_seconds_is_used() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-custom-config", 4)?;
        let cli = test_cli_with_rug_lookahead(&db_path, 7)?;

        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        let conn = Connection::open(&db_path)?;
        let rug_check_after: String = conn.query_row(
            "SELECT rug_check_after_ts
             FROM wallet_scoring_buy_facts
             WHERE buy_signature = ?1",
            ["sig-gap"],
            |row| row.get(0),
        )?;
        assert_eq!(rug_check_after, "2026-04-29T00:00:08+00:00");
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn covered_through_at_target_with_latch_remaining_attempts_guarded_clear() -> Result<()> {
        let (db_path, _gap_cursor, target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-covered-at-target", 4)?;
        let store = SqliteStore::open(&db_path)?;
        store.set_discovery_scoring_covered_through_cursor(&target_cursor)?;
        assert!(store
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some());
        let cli = test_cli(&db_path)?;

        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_COMPLETED);
        assert!(report.reached_target);
        assert!(report.latch_cleared);
        assert!(!report.production_green);
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn missing_materialization_gap_fails_closed() -> Result<()> {
        let (db_path, store) = migrated_store("copybot-aggregate-repair-missing-gap")?;
        let swap = test_swap("sig-covered", 100, "2026-04-29T00:00:00Z");
        store.insert_observed_swaps_batch(&[swap.clone()])?;
        store.set_discovery_scoring_covered_through_cursor(&cursor_for_swap(&swap))?;

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_NO_GAP);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn missing_persisted_target_fails_closed() -> Result<()> {
        let (db_path, gap_cursor, _target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-missing-target", 4)?;
        let store = SqliteStore::open(&db_path)?;
        store.clear_discovery_scoring_materialization_gap_repair_target()?;
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor)
        );

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_NO_TARGET);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn missing_exact_gap_row_fails_closed() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-missing-exact-gap", 4)?;
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "DELETE FROM observed_swaps WHERE signature = ?1",
            ["sig-gap"],
        )?;

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_EXACT_GAP_MISSING);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn target_for_different_gap_fails_closed() -> Result<()> {
        let (db_path, gap_cursor, target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-target-mismatch", 4)?;
        let store = SqliteStore::open(&db_path)?;
        let other_gap = DiscoveryRuntimeCursor {
            ts_utc: gap_cursor.ts_utc - ChronoDuration::seconds(10),
            slot: gap_cursor.slot - 1,
            signature: "other-gap".to_string(),
        };
        store.clear_discovery_scoring_materialization_gap_repair_target()?;
        store
            .set_discovery_scoring_materialization_gap_repair_target(&other_gap, &target_cursor)?;

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_TARGET_GAP_MISMATCH);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn missing_covered_through_fails_closed() -> Result<()> {
        let (db_path, gap_cursor, target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-missing-covered", 4)?;
        let (fresh_db, fresh_store) =
            migrated_store("copybot-aggregate-repair-missing-covered-fresh")?;
        fresh_store.insert_observed_swaps_batch(&swaps)?;
        fresh_store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        fresh_store
            .set_discovery_scoring_materialization_gap_repair_target(&gap_cursor, &target_cursor)?;

        let cli = test_cli(&fresh_db)?;
        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_NO_COVERED_THROUGH);
        remove_sqlite_files(&db_path);
        remove_sqlite_files(&fresh_db);
        Ok(())
    }

    #[test]
    fn dry_run_writes_nothing() -> Result<()> {
        let (db_path, gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-dry-run", 4)?;
        let covered = cursor_for_swap(&swaps[0]);
        let mut cli = test_cli(&db_path)?;
        cli.dry_run = true;

        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_DRY_RUN);
        assert!(!report.production_green);
        let store = SqliteStore::open(&db_path)?;
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(covered)
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor)
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn max_pages_stops_incomplete_without_fake_success() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-max-pages", 4)?;
        let mut cli = test_cli(&db_path)?;
        cli.max_pages = 1;

        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_MAX_PAGES);
        assert!(!report.reached_target);
        assert!(!report.latch_cleared);
        assert!(!report.production_green);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn max_seconds_stops_incomplete_without_fake_success() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, _swaps) =
            seed_repair_fixture("copybot-aggregate-repair-max-seconds", 4)?;
        let mut cli = test_cli(&db_path)?;
        cli.max_seconds = 0;

        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_MAX_SECONDS);
        assert_eq!(report.rows_processed, 0);
        assert!(!report.production_green);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn current_equals_expected_start_commits_successfully() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-current-equals-expected", 4)?;
        let store = SqliteStore::open(&db_path)?;
        let expected_start = cursor_for_swap(&swaps[0]);
        let commit_end = cursor_for_swap(&swaps[1]);

        let outcome = store.apply_discovery_scoring_repair_commit_group(
            &[swaps[1].clone()],
            &aggregate_config_for_tests(),
            &expected_start,
            &commit_end,
        )?;

        assert_eq!(outcome, "committed");
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(commit_end)
        );
        assert_eq!(repair_buy_fact_count(&db_path)?, 1);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn concurrent_covered_through_advanced_inside_group_does_not_duplicate_rows() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-concurrent-inside-group", 4)?;
        let store = SqliteStore::open(&db_path)?;
        let config = aggregate_config_for_tests();
        let expected_start = cursor_for_swap(&swaps[0]);
        let concurrent_cursor = cursor_for_swap(&swaps[2]);
        let group_end = cursor_for_swap(&swaps[4]);
        store.apply_discovery_scoring_batch(&[swaps[1].clone(), swaps[2].clone()], &config)?;
        store.set_discovery_scoring_covered_through_cursor(&concurrent_cursor)?;
        assert_eq!(repair_buy_fact_count(&db_path)?, 2);
        assert_eq!(wallet_day_trades(&db_path)?, 3);

        let outcome = store.apply_discovery_scoring_repair_commit_group(
            &[
                swaps[1].clone(),
                swaps[2].clone(),
                swaps[3].clone(),
                swaps[4].clone(),
            ],
            &config,
            &expected_start,
            &group_end,
        )?;

        assert_eq!(outcome, "concurrent_progress");
        assert_eq!(repair_buy_fact_count(&db_path)?, 2);
        assert_eq!(wallet_day_trades(&db_path)?, 3);
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(concurrent_cursor)
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn concurrent_covered_through_already_at_group_end_noops_without_duplicate_rows() -> Result<()>
    {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-concurrent-at-group-end", 4)?;
        let store = SqliteStore::open(&db_path)?;
        let config = aggregate_config_for_tests();
        let expected_start = cursor_for_swap(&swaps[0]);
        let group_end = cursor_for_swap(&swaps[2]);
        store.apply_discovery_scoring_batch(&[swaps[1].clone(), swaps[2].clone()], &config)?;
        store.set_discovery_scoring_covered_through_cursor(&group_end)?;
        assert_eq!(repair_buy_fact_count(&db_path)?, 2);
        assert_eq!(wallet_day_trades(&db_path)?, 3);

        let outcome = store.apply_discovery_scoring_repair_commit_group(
            &[swaps[1].clone(), swaps[2].clone()],
            &config,
            &expected_start,
            &group_end,
        )?;

        assert_eq!(outcome, "already_covered");
        assert_eq!(repair_buy_fact_count(&db_path)?, 2);
        assert_eq!(wallet_day_trades(&db_path)?, 3);
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(group_end)
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn current_before_expected_start_fails_closed_without_writes() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-current-before-expected", 4)?;
        let store = SqliteStore::open(&db_path)?;
        let expected_start = cursor_for_swap(&swaps[1]);
        let commit_end = cursor_for_swap(&swaps[2]);

        let outcome = store.apply_discovery_scoring_repair_commit_group(
            &[swaps[2].clone()],
            &aggregate_config_for_tests(),
            &expected_start,
            &commit_end,
        );

        assert!(outcome.is_err());
        assert_eq!(repair_buy_fact_count(&db_path)?, 0);
        assert_eq!(wallet_day_trades(&db_path)?, 1);
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(cursor_for_swap(&swaps[0]))
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn operator_reports_concurrent_progress_as_non_terminal() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-operator-concurrent", 4)?;
        set_test_advance_covered_through_before_commit(Some(cursor_for_swap(&swaps[2])));
        let cli = test_cli(&db_path)?;

        let report = run(&cli);

        assert!(report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_CONCURRENT_COVERED_THROUGH_ADVANCED);
        assert_eq!(report.recommended_next_action, ACTION_RERUN);
        assert_eq!(repair_buy_fact_count(&db_path)?, 0);
        assert!(!report.production_green);
        set_test_advance_covered_through_before_commit(None);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn injected_failure_after_apply_before_finalize_rolls_back_apply() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-atomic-finalize-failure", 4)?;
        let covered = cursor_for_swap(&swaps[0]);
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_repair_rug_finalize
             BEFORE UPDATE OF rug_volume_lookahead_sol ON wallet_scoring_buy_facts
             WHEN OLD.buy_signature = 'sig-gap'
             BEGIN
                SELECT RAISE(ABORT, 'injected rug finalize failure');
             END;",
        )?;
        let cli = test_cli(&db_path)?;

        let report = run(&cli);

        assert!(!report.ok, "{report:?}");
        assert_eq!(report.reason, REASON_UNKNOWN_ERROR);
        assert_eq!(repair_buy_fact_count(&db_path)?, 0);
        let store = SqliteStore::open(&db_path)?;
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(covered)
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn retryable_sqlite_lock_is_non_terminal_and_leaves_covered_through_unchanged() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-lock", 4)?;
        let covered = cursor_for_swap(&swaps[0]);
        let blocker = Connection::open(&db_path)?;
        blocker.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        blocker.execute_batch("ROLLBACK")?;
        assert!(!report.ok);
        assert_eq!(report.reason, REASON_SQLITE_LOCK_RETRYABLE, "{report:?}");
        let store = SqliteStore::open(&db_path)?;
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(covered)
        );
        assert_eq!(repair_buy_fact_count(&db_path)?, 0);
        remove_sqlite_files(&db_path);
        Ok(())
    }

    #[test]
    fn unknown_error_remains_terminal_fail_closed() -> Result<()> {
        let (db_path, _gap_cursor, _target_cursor, swaps) =
            seed_repair_fixture("copybot-aggregate-repair-unknown-error", 4)?;
        let covered = cursor_for_swap(&swaps[0]);
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("DROP TABLE wallet_scoring_buy_facts")?;

        let cli = test_cli(&db_path)?;
        let report = run(&cli);

        assert!(!report.ok);
        assert_eq!(report.reason, REASON_UNKNOWN_ERROR, "{report:?}");
        let store = SqliteStore::open(&db_path)?;
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(covered)
        );
        remove_sqlite_files(&db_path);
        Ok(())
    }
}
