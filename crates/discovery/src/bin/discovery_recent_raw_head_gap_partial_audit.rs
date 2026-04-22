use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{load_from_path, DiscoveryConfig};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_recent_raw_head_gap_partial_audit --db <path> --config <path> [--json]";

const CONCLUSION_RUNTIME_CURSOR_GAP_REPLAY_STOPS_BEFORE_REQUIRED_WINDOW: &str =
    "recent_raw_head_gap_partial_because_runtime_cursor_gap_replay_stops_before_required_window";
const CONCLUSION_WINDOW_COMPLETENESS_TOO_STRICT_OR_STALE: &str =
    "recent_raw_head_gap_partial_because_window_completeness_derivation_is_too_strict_or_stale";
const CONCLUSION_REPAIR_ADVANCES_JOURNAL_BUT_NOT_RUNTIME_WINDOW_FRONTIER: &str =
    "recent_raw_head_gap_partial_because_repair_advances_journal_but_not_runtime_window_frontier";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const RECENT_RAW_REPAIR_SURFACE: &str =
    "copybot_discovery::DiscoveryService::repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options";
const RECENT_RAW_HEAD_GAP_PARTIAL_SURFACE: &str =
    "copybot_discovery::DiscoveryService::repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options";
const RUNTIME_WINDOW_COMPLETE_AFTER_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persisted_observed_swaps_cover_window";
const RUNTIME_WINDOW_COMPLETE_DERIVATION_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persisted_observed_swaps_cover_window";
const RAW_WINDOW_UNUSABLE_NO_RECENT_PUBLISHED_UNIVERSE_SURFACE: &str =
    "copybot_discovery::DiscoveryService::run_cycle";
const PUBLICATION_FAIL_CLOSED_SURFACE: &str =
    "copybot_discovery::DiscoveryService::persist_publication_state";

const DISCOVERY_SOURCE: &str = include_str!("../lib.rs");

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
struct RecentRawHeadGapPartialAuditReport {
    publication_runtime_mode: Option<String>,
    publication_reason: Option<String>,
    publication_last_published_at_utc: Option<DateTime<Utc>>,
    publication_updated_at_utc: Option<DateTime<Utc>>,
    runtime_window_complete_current: bool,
    runtime_cursor_ts_current: Option<DateTime<Utc>>,
    runtime_cursor_slot_current: Option<u64>,
    runtime_cursor_signature_current: Option<String>,
    recent_raw_db_path_from_config: String,
    recent_raw_observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    recent_raw_observed_swaps_min_ts_utc: Option<DateTime<Utc>>,
    runtime_required_window_start_utc: DateTime<Utc>,
    runtime_required_window_end_utc: DateTime<Utc>,
    recent_raw_covers_required_window_start: bool,
    recent_raw_covers_runtime_cursor: bool,
    recent_raw_head_gap_exists: bool,
    recent_raw_head_gap_partial_shape: bool,
    runtime_window_complete_derivation_surface: String,
    recent_raw_repair_surface: String,
    recent_raw_head_gap_partial_surface: String,
    runtime_window_complete_after_surface: String,
    raw_window_unusable_no_recent_published_universe_surface: String,
    publication_fail_closed_surface: String,
    recent_raw_repair_can_complete_without_runtime_window_complete: bool,
    recent_raw_head_gap_partial_conclusion: String,
}

#[derive(Debug, Clone, Default)]
struct PublicationStateFacts {
    runtime_mode: Option<String>,
    reason: Option<String>,
    last_published_at_utc: Option<DateTime<Utc>>,
    updated_at_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct RuntimeFacts {
    runtime_cursor: Option<DiscoveryRuntimeCursor>,
    first_cursor_in_required_window: Option<DiscoveryRuntimeCursor>,
    runtime_window_complete_current: bool,
}

#[derive(Debug, Clone, Default)]
struct RecentRawFacts {
    observed_swaps_min_ts_utc: Option<DateTime<Utc>>,
    observed_swaps_max_ts_utc: Option<DateTime<Utc>>,
    covered_through_cursor: Option<DiscoveryRuntimeCursor>,
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

fn run(config: &Config) -> Result<RecentRawHeadGapPartialAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let runtime_store = SqliteStore::open_read_only_immutable(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let runtime_conn = open_read_only_connection(&config.db_path)?;

    let publication_state = load_publication_state_facts(&runtime_store)?;
    let runtime_reference_end_utc = runtime_required_window_end_utc(
        &runtime_store,
        &runtime_conn,
        &publication_state,
    )?;
    let runtime_required_window_start_utc =
        runtime_required_window_start(&loaded_config.discovery, runtime_reference_end_utc);
    let runtime_facts = load_runtime_facts(
        &runtime_store,
        &runtime_conn,
        runtime_required_window_start_utc,
        runtime_reference_end_utc,
    )?;

    let recent_raw_db_path = resolve_db_path(
        &config.config_path,
        &loaded_config.recent_raw_journal.path,
    );
    let recent_raw_facts = load_recent_raw_facts_if_accessible(&recent_raw_db_path)?;

    let recent_raw_covers_required_window_start = recent_raw_facts
        .observed_swaps_min_ts_utc
        .is_some_and(|ts| ts <= runtime_required_window_start_utc);
    let recent_raw_covers_runtime_cursor = recent_raw_facts
        .covered_through_cursor
        .as_ref()
        .zip(runtime_facts.runtime_cursor.as_ref())
        .is_some_and(|(journal_cursor, runtime_cursor)| {
            runtime_cursor_cmp(journal_cursor, runtime_cursor).is_ge()
        });
    let recent_raw_head_gap_exists = !runtime_facts.runtime_window_complete_current
        && runtime_facts.runtime_cursor.is_some()
        && recent_raw_covers_required_window_start
        && recent_raw_covers_runtime_cursor
        && match runtime_facts.first_cursor_in_required_window.as_ref() {
            Some(first_cursor) => first_cursor.ts_utc > runtime_required_window_start_utc,
            None => true,
        };
    let recent_raw_head_gap_partial_shape = recent_raw_head_gap_exists
        && publication_state
            .reason
            .as_deref()
            .is_some_and(|reason| reason == "raw_window_unusable_no_recent_published_universe");

    let code_paths = inspect_code_path_facts();
    let conclusion = choose_conclusion(
        &publication_state,
        &runtime_facts,
        recent_raw_covers_required_window_start,
        recent_raw_covers_runtime_cursor,
        recent_raw_head_gap_exists,
        recent_raw_head_gap_partial_shape,
        &code_paths,
    );

    Ok(RecentRawHeadGapPartialAuditReport {
        publication_runtime_mode: publication_state.runtime_mode,
        publication_reason: publication_state.reason,
        publication_last_published_at_utc: publication_state.last_published_at_utc,
        publication_updated_at_utc: publication_state.updated_at_utc,
        runtime_window_complete_current: runtime_facts.runtime_window_complete_current,
        runtime_cursor_ts_current: runtime_facts.runtime_cursor.as_ref().map(|cursor| cursor.ts_utc),
        runtime_cursor_slot_current: runtime_facts.runtime_cursor.as_ref().map(|cursor| cursor.slot),
        runtime_cursor_signature_current: runtime_facts
            .runtime_cursor
            .as_ref()
            .map(|cursor| cursor.signature.clone()),
        recent_raw_db_path_from_config: recent_raw_db_path.display().to_string(),
        recent_raw_observed_swaps_max_ts_utc: recent_raw_facts.observed_swaps_max_ts_utc,
        recent_raw_observed_swaps_min_ts_utc: recent_raw_facts.observed_swaps_min_ts_utc,
        runtime_required_window_start_utc,
        runtime_required_window_end_utc: runtime_reference_end_utc,
        recent_raw_covers_required_window_start,
        recent_raw_covers_runtime_cursor,
        recent_raw_head_gap_exists,
        recent_raw_head_gap_partial_shape,
        runtime_window_complete_derivation_surface: RUNTIME_WINDOW_COMPLETE_DERIVATION_SURFACE
            .to_string(),
        recent_raw_repair_surface: RECENT_RAW_REPAIR_SURFACE.to_string(),
        recent_raw_head_gap_partial_surface: RECENT_RAW_HEAD_GAP_PARTIAL_SURFACE.to_string(),
        runtime_window_complete_after_surface: RUNTIME_WINDOW_COMPLETE_AFTER_SURFACE.to_string(),
        raw_window_unusable_no_recent_published_universe_surface:
            RAW_WINDOW_UNUSABLE_NO_RECENT_PUBLISHED_UNIVERSE_SURFACE.to_string(),
        publication_fail_closed_surface: PUBLICATION_FAIL_CLOSED_SURFACE.to_string(),
        recent_raw_repair_can_complete_without_runtime_window_complete: code_paths
            .recent_raw_repair_can_complete_without_runtime_window_complete,
        recent_raw_head_gap_partial_conclusion: conclusion.to_string(),
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

fn load_publication_state_facts(store: &SqliteStore) -> Result<PublicationStateFacts> {
    let Some(state) = store.discovery_publication_state_read_only()? else {
        return Ok(PublicationStateFacts::default());
    };
    Ok(PublicationStateFacts {
        runtime_mode: Some(state.runtime_mode.as_str().to_string()),
        reason: Some(state.reason),
        last_published_at_utc: state.last_published_at,
        updated_at_utc: Some(state.updated_at),
    })
}

fn runtime_required_window_end_utc(
    runtime_store: &SqliteStore,
    runtime_conn: &Connection,
    publication_state: &PublicationStateFacts,
) -> Result<DateTime<Utc>> {
    if let Some(runtime_cursor) = runtime_store.load_discovery_runtime_cursor_read_only()? {
        return Ok(runtime_cursor.ts_utc);
    }
    if let Some(max_ts) = query_max_swap_ts(runtime_conn)? {
        return Ok(max_ts);
    }
    if let Some(updated_at) = publication_state.updated_at_utc {
        return Ok(updated_at);
    }
    if let Some(last_published_at) = publication_state.last_published_at_utc {
        return Ok(last_published_at);
    }
    DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
        .map(|ts| ts.with_timezone(&Utc))
        .context("failed constructing epoch fallback")
}

fn runtime_required_window_start(config: &DiscoveryConfig, end: DateTime<Utc>) -> DateTime<Utc> {
    end - Duration::days(config.scoring_window_days.max(1) as i64)
}

fn load_runtime_facts(
    runtime_store: &SqliteStore,
    runtime_conn: &Connection,
    required_window_start: DateTime<Utc>,
    required_window_end: DateTime<Utc>,
) -> Result<RuntimeFacts> {
    let runtime_cursor = runtime_store.load_discovery_runtime_cursor_read_only()?;
    let oldest_observed_swaps_ts_utc = query_min_swap_ts(runtime_conn)?;
    let first_cursor_in_required_window = query_first_swap_cursor_between(
        runtime_conn,
        required_window_start,
        required_window_end,
    )?;
    let runtime_window_complete_current = oldest_observed_swaps_ts_utc
        .is_some_and(|ts| ts <= required_window_start)
        && first_cursor_in_required_window.is_some();

    Ok(RuntimeFacts {
        runtime_cursor,
        first_cursor_in_required_window,
        runtime_window_complete_current,
    })
}

fn load_recent_raw_facts_if_accessible(path: &Path) -> Result<RecentRawFacts> {
    if !path.exists() {
        return Ok(RecentRawFacts::default());
    }
    let conn = open_read_only_connection(path)?;
    Ok(RecentRawFacts {
        observed_swaps_min_ts_utc: query_min_swap_ts(&conn)?,
        observed_swaps_max_ts_utc: query_max_swap_ts(&conn)?,
        covered_through_cursor: query_last_swap_cursor(&conn)?,
    })
}

fn query_min_swap_ts(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    query_first_swap_cursor(conn).map(|cursor| cursor.map(|cursor| cursor.ts_utc))
}

fn query_max_swap_ts(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    query_last_swap_cursor(conn).map(|cursor| cursor.map(|cursor| cursor.ts_utc))
}

fn query_first_swap_cursor(conn: &Connection) -> Result<Option<DiscoveryRuntimeCursor>> {
    let raw: Option<(String, i64, String)> = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed querying first observed_swaps cursor")?;
    raw.map(|(ts_raw, slot_raw, signature)| {
        Ok(DiscoveryRuntimeCursor {
            ts_utc: parse_db_ts(&ts_raw)?,
            slot: slot_raw.max(0) as u64,
            signature,
        })
    })
    .transpose()
}

fn query_last_swap_cursor(conn: &Connection) -> Result<Option<DiscoveryRuntimeCursor>> {
    let raw: Option<(String, i64, String)> = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed querying last observed_swaps cursor")?;
    raw.map(|(ts_raw, slot_raw, signature)| {
        Ok(DiscoveryRuntimeCursor {
            ts_utc: parse_db_ts(&ts_raw)?,
            slot: slot_raw.max(0) as u64,
            signature,
        })
    })
    .transpose()
}

fn query_first_swap_cursor_between(
    conn: &Connection,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    let raw: Option<(String, i64, String)> = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps
             WHERE ts >= ?1
               AND ts <= ?2
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT 1",
            [window_start.to_rfc3339(), window_end.to_rfc3339()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed querying first observed_swaps cursor in required runtime window")?;
    raw.map(|(ts_raw, slot_raw, signature)| {
        Ok(DiscoveryRuntimeCursor {
            ts_utc: parse_db_ts(&ts_raw)?,
            slot: slot_raw.max(0) as u64,
            signature,
        })
    })
    .transpose()
}

fn parse_db_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid db timestamp value: {raw}"))
}

fn runtime_cursor_cmp(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

#[derive(Debug, Clone, Default)]
struct CodePathFacts {
    recent_raw_repair_can_complete_without_runtime_window_complete: bool,
}

fn inspect_code_path_facts() -> CodePathFacts {
    CodePathFacts {
        recent_raw_repair_can_complete_without_runtime_window_complete: DISCOVERY_SOURCE
            .contains("\"replayed_recent_raw_journal_head_gap_partial\""),
    }
}

fn choose_conclusion(
    publication_state: &PublicationStateFacts,
    runtime_facts: &RuntimeFacts,
    recent_raw_covers_required_window_start: bool,
    recent_raw_covers_runtime_cursor: bool,
    recent_raw_head_gap_exists: bool,
    recent_raw_head_gap_partial_shape: bool,
    code_paths: &CodePathFacts,
) -> &'static str {
    if recent_raw_head_gap_partial_shape
        && !runtime_facts.runtime_window_complete_current
        && recent_raw_covers_required_window_start
        && recent_raw_covers_runtime_cursor
        && recent_raw_head_gap_exists
        && code_paths.recent_raw_repair_can_complete_without_runtime_window_complete
    {
        return CONCLUSION_RUNTIME_CURSOR_GAP_REPLAY_STOPS_BEFORE_REQUIRED_WINDOW;
    }

    if publication_state
        .reason
        .as_deref()
        .is_some_and(|reason| reason == "raw_window_unusable_no_recent_published_universe")
        && recent_raw_covers_required_window_start
        && recent_raw_covers_runtime_cursor
        && code_paths.recent_raw_repair_can_complete_without_runtime_window_complete
        && runtime_facts.runtime_window_complete_current
    {
        return CONCLUSION_WINDOW_COMPLETENESS_TOO_STRICT_OR_STALE;
    }

    if publication_state
        .reason
        .as_deref()
        .is_some_and(|reason| reason == "raw_window_unusable_no_recent_published_universe")
        && recent_raw_covers_required_window_start
        && recent_raw_covers_runtime_cursor
        && !runtime_facts.runtime_window_complete_current
        && !recent_raw_head_gap_exists
        && code_paths.recent_raw_repair_can_complete_without_runtime_window_complete
    {
        return CONCLUSION_REPAIR_ADVANCES_JOURNAL_BUT_NOT_RUNTIME_WINDOW_FRONTIER;
    }

    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn render_output(report: &RecentRawHeadGapPartialAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing recent raw head-gap partial audit json");
    }

    Ok(format!(
        concat!(
            "event=discovery_recent_raw_head_gap_partial_audit\n",
            "publication_runtime_mode={publication_runtime_mode}\n",
            "publication_reason={publication_reason}\n",
            "publication_last_published_at_utc={publication_last_published_at_utc}\n",
            "publication_updated_at_utc={publication_updated_at_utc}\n",
            "runtime_window_complete_current={runtime_window_complete_current}\n",
            "runtime_cursor_ts_current={runtime_cursor_ts_current}\n",
            "runtime_cursor_slot_current={runtime_cursor_slot_current}\n",
            "runtime_cursor_signature_current={runtime_cursor_signature_current}\n",
            "recent_raw_db_path_from_config={recent_raw_db_path_from_config}\n",
            "recent_raw_observed_swaps_max_ts_utc={recent_raw_observed_swaps_max_ts_utc}\n",
            "recent_raw_observed_swaps_min_ts_utc={recent_raw_observed_swaps_min_ts_utc}\n",
            "runtime_required_window_start_utc={runtime_required_window_start_utc}\n",
            "runtime_required_window_end_utc={runtime_required_window_end_utc}\n",
            "recent_raw_covers_required_window_start={recent_raw_covers_required_window_start}\n",
            "recent_raw_covers_runtime_cursor={recent_raw_covers_runtime_cursor}\n",
            "recent_raw_head_gap_exists={recent_raw_head_gap_exists}\n",
            "recent_raw_head_gap_partial_shape={recent_raw_head_gap_partial_shape}\n",
            "runtime_window_complete_derivation_surface={runtime_window_complete_derivation_surface}\n",
            "recent_raw_repair_surface={recent_raw_repair_surface}\n",
            "recent_raw_head_gap_partial_surface={recent_raw_head_gap_partial_surface}\n",
            "runtime_window_complete_after_surface={runtime_window_complete_after_surface}\n",
            "raw_window_unusable_no_recent_published_universe_surface={raw_window_unusable_no_recent_published_universe_surface}\n",
            "publication_fail_closed_surface={publication_fail_closed_surface}\n",
            "recent_raw_repair_can_complete_without_runtime_window_complete={recent_raw_repair_can_complete_without_runtime_window_complete}\n",
            "recent_raw_head_gap_partial_conclusion={recent_raw_head_gap_partial_conclusion}"
        ),
        publication_runtime_mode = format_optional_str(report.publication_runtime_mode.as_deref()),
        publication_reason = format_optional_str(report.publication_reason.as_deref()),
        publication_last_published_at_utc = format_optional_ts(report.publication_last_published_at_utc),
        publication_updated_at_utc = format_optional_ts(report.publication_updated_at_utc),
        runtime_window_complete_current = report.runtime_window_complete_current,
        runtime_cursor_ts_current = format_optional_ts(report.runtime_cursor_ts_current),
        runtime_cursor_slot_current = format_optional_u64(report.runtime_cursor_slot_current),
        runtime_cursor_signature_current = format_optional_str(
            report.runtime_cursor_signature_current.as_deref()
        ),
        recent_raw_db_path_from_config = report.recent_raw_db_path_from_config,
        recent_raw_observed_swaps_max_ts_utc = format_optional_ts(report.recent_raw_observed_swaps_max_ts_utc),
        recent_raw_observed_swaps_min_ts_utc = format_optional_ts(report.recent_raw_observed_swaps_min_ts_utc),
        runtime_required_window_start_utc = report.runtime_required_window_start_utc.to_rfc3339(),
        runtime_required_window_end_utc = report.runtime_required_window_end_utc.to_rfc3339(),
        recent_raw_covers_required_window_start = report.recent_raw_covers_required_window_start,
        recent_raw_covers_runtime_cursor = report.recent_raw_covers_runtime_cursor,
        recent_raw_head_gap_exists = report.recent_raw_head_gap_exists,
        recent_raw_head_gap_partial_shape = report.recent_raw_head_gap_partial_shape,
        runtime_window_complete_derivation_surface = report.runtime_window_complete_derivation_surface,
        recent_raw_repair_surface = report.recent_raw_repair_surface,
        recent_raw_head_gap_partial_surface = report.recent_raw_head_gap_partial_surface,
        runtime_window_complete_after_surface = report.runtime_window_complete_after_surface,
        raw_window_unusable_no_recent_published_universe_surface = report
            .raw_window_unusable_no_recent_published_universe_surface,
        publication_fail_closed_surface = report.publication_fail_closed_surface,
        recent_raw_repair_can_complete_without_runtime_window_complete = report
            .recent_raw_repair_can_complete_without_runtime_window_complete,
        recent_raw_head_gap_partial_conclusion = report.recent_raw_head_gap_partial_conclusion,
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
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use copybot_storage::DiscoveryPublicationStateUpdate;
    use tempfile::TempDir;

    fn create_runtime_store(path: &Path) -> Result<SqliteStore> {
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
                    "scoring_window_days = 5\n",
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
            token_out: "TokenAuditHeadGap111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn seed_head_gap_partial_fixture() -> Result<(TempDir, Config)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let runtime_db_path = temp.path().join("runtime.db");
        let journal_db_path = temp.path().join("recent_raw.db");
        let runtime_store = create_runtime_store(&runtime_db_path)?;
        let journal_store = create_runtime_store(&journal_db_path)?;

        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid ts")
            .with_timezone(&Utc);
        let required_window_start = now - Duration::days(5);
        let journal_swaps = vec![
            swap("sig-gap-0", "wallet-gap-a", required_window_start, 100),
            swap(
                "sig-gap-1",
                "wallet-gap-b",
                required_window_start + Duration::days(1),
                101,
            ),
            swap(
                "sig-gap-2",
                "wallet-gap-c",
                required_window_start + Duration::days(2),
                102,
            ),
            swap(
                "sig-gap-3",
                "wallet-gap-d",
                now,
                103,
            ),
        ];
        journal_store.insert_recent_raw_journal_batch(&journal_swaps, now)?;
        for swap in journal_swaps.iter().skip(2) {
            runtime_store.insert_observed_swap(swap)?;
        }
        runtime_store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: journal_swaps
                .last()
                .expect("latest journal swap")
                .ts_utc,
            slot: journal_swaps.last().expect("latest journal swap").slot,
            signature: journal_swaps
                .last()
                .expect("latest journal swap")
                .signature
                .clone(),
        })?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_unusable_no_recent_published_universe".to_string(),
            last_published_at: Some(now - Duration::days(1)),
            last_published_window_start: Some(required_window_start - Duration::hours(1)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(Vec::new()),
        })?;

        let config_path = write_config(&temp, &runtime_db_path, &journal_db_path)?;
        Ok((
            temp,
            Config {
                db_path: runtime_db_path,
                config_path,
                json: true,
            },
        ))
    }

    fn seed_ambiguous_fixture() -> Result<(TempDir, Config)> {
        let temp = TempDir::new().context("failed to create tempdir")?;
        let runtime_db_path = temp.path().join("runtime.db");
        let missing_journal_db_path = temp.path().join("missing_recent_raw.db");
        let runtime_store = create_runtime_store(&runtime_db_path)?;

        let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
            .expect("valid ts")
            .with_timezone(&Utc);
        let runtime_swap = swap("sig-ambiguous-0", "wallet-ambiguous", now - Duration::hours(1), 200);
        runtime_store.insert_observed_swap(&runtime_swap)?;
        runtime_store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: runtime_swap.ts_utc,
            slot: runtime_swap.slot,
            signature: runtime_swap.signature.clone(),
        })?;
        runtime_store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::FailClosed,
            reason: "raw_window_unusable_no_recent_published_universe".to_string(),
            last_published_at: None,
            last_published_window_start: None,
            published_scoring_source: None,
            published_wallet_ids: Some(Vec::new()),
        })?;

        let config_path = write_config(&temp, &runtime_db_path, &missing_journal_db_path)?;
        Ok((
            temp,
            Config {
                db_path: runtime_db_path,
                config_path,
                json: true,
            },
        ))
    }

    #[test]
    fn parse_args_accepts_required_flags_stage1() -> Result<()> {
        let parsed = parse_args_from([
            "--db".to_string(),
            "runtime.db".to_string(),
            "--config".to_string(),
            "config.toml".to_string(),
            "--json".to_string(),
        ])?
        .expect("config");
        assert_eq!(parsed.db_path, PathBuf::from("runtime.db"));
        assert_eq!(parsed.config_path, PathBuf::from("config.toml"));
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn json_output_contains_required_fields_stage1() -> Result<()> {
        let (_temp, config) = seed_head_gap_partial_fixture()?;
        let report = run(&config)?;
        let value = serde_json::to_value(&report)?;
        for key in [
            "publication_runtime_mode",
            "publication_reason",
            "publication_last_published_at_utc",
            "publication_updated_at_utc",
            "runtime_window_complete_current",
            "runtime_cursor_ts_current",
            "runtime_cursor_slot_current",
            "runtime_cursor_signature_current",
            "recent_raw_db_path_from_config",
            "recent_raw_observed_swaps_max_ts_utc",
            "recent_raw_observed_swaps_min_ts_utc",
            "runtime_required_window_start_utc",
            "runtime_required_window_end_utc",
            "recent_raw_covers_required_window_start",
            "recent_raw_covers_runtime_cursor",
            "recent_raw_head_gap_exists",
            "recent_raw_head_gap_partial_shape",
            "runtime_window_complete_derivation_surface",
            "recent_raw_repair_surface",
            "recent_raw_head_gap_partial_surface",
            "runtime_window_complete_after_surface",
            "raw_window_unusable_no_recent_published_universe_surface",
            "publication_fail_closed_surface",
            "recent_raw_repair_can_complete_without_runtime_window_complete",
            "recent_raw_head_gap_partial_conclusion",
        ] {
            assert!(value.get(key).is_some(), "missing json field {key}");
        }
        Ok(())
    }

    #[test]
    fn head_gap_partial_fixture_yields_runtime_cursor_gap_replay_conclusion_stage1() -> Result<()> {
        let (_temp, config) = seed_head_gap_partial_fixture()?;
        let report = run(&config)?;
        assert_eq!(
            report.recent_raw_head_gap_partial_conclusion,
            CONCLUSION_RUNTIME_CURSOR_GAP_REPLAY_STOPS_BEFORE_REQUIRED_WINDOW
        );
        assert!(!report.runtime_window_complete_current);
        assert!(report.recent_raw_covers_required_window_start);
        assert!(report.recent_raw_covers_runtime_cursor);
        assert!(report.recent_raw_head_gap_exists);
        assert!(report.recent_raw_head_gap_partial_shape);
        Ok(())
    }

    #[test]
    fn ambiguous_fixture_does_not_overclaim_stage1() -> Result<()> {
        let (_temp, config) = seed_ambiguous_fixture()?;
        let report = run(&config)?;
        assert_eq!(
            report.recent_raw_head_gap_partial_conclusion,
            CONCLUSION_INSUFFICIENT_EVIDENCE
        );
        Ok(())
    }

    #[test]
    fn repeated_runs_are_deterministic_stage1() -> Result<()> {
        let (_temp, config) = seed_head_gap_partial_fixture()?;
        let first = serde_json::to_string_pretty(&run(&config)?)?;
        let second = serde_json::to_string_pretty(&run(&config)?)?;
        assert_eq!(first, second);
        Ok(())
    }
}
