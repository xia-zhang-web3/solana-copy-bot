use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_core_types::SwapEvent;
use copybot_discovery::raw_gap_fill_support::{
    compute_missing_segments, format_optional_cursor, format_optional_ts, max_cursor_opt,
    min_ts_opt, parse_transaction_to_swap, post_helius_json, reset_sqlite_path,
    resolve_gap_fill_plan, GapFillMissingSegment, GapFillPlan, ProgramIdConfig,
};
#[cfg(test)]
use copybot_discovery::restore_verdict::DiscoveryRuntimeRestoreVerdictKind;
use copybot_discovery::runtime_restore_ops::{
    copy_atomic, gap_fill_helius_archive_path, gap_fill_helius_latest_metadata_path,
    gap_fill_helius_latest_path, journal_snapshot_metadata_path, prune_rotated_archives,
    resolve_db_path, resolve_relative_to_config, write_json_atomic, GAP_FILL_HELIUS_ARCHIVE_PREFIX,
    GAP_FILL_HELIUS_ARCHIVE_SUFFIX,
};
#[cfg(test)]
use copybot_discovery::DiscoveryService;
#[cfg(test)]
use copybot_storage::{
    DiscoveryPublicationStateUpdate, DiscoveryRecentRawRestoreStateUpdate, WalletMetricRow,
};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;
#[cfg(test)]
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_raw_gap_fill_helius --config <path> [--db-path <path>] [--output <path>] [--window-start <rfc3339> --window-end <rfc3339>] [--helius-http-url <url>] [--json] [--now <rfc3339>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("{}", run(config)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    window_start: Option<DateTime<Utc>>,
    window_end: Option<DateTime<Utc>>,
    helius_http_url: Option<String>,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GapFillOutput {
    db_path: String,
    output_db_path: String,
    latest_db_path: Option<String>,
    metadata_path: String,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    required_window_start: Option<DateTime<Utc>>,
    derived_from_restore_state: bool,
    active_follow_wallets: usize,
    journal_available: bool,
    journal_covers_artifact_cursor: bool,
    journal_covered_since: Option<DateTime<Utc>>,
    journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    fetched_rows: usize,
    inserted_rows: usize,
    scanned_items: usize,
    scanned_pages: usize,
    gap_fill_covered_since: Option<DateTime<Utc>>,
    gap_fill_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    final_covered_since: Option<DateTime<Utc>>,
    final_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    source_lag_seconds: Option<u64>,
    missing_segments: Vec<GapFillMissingSegment>,
    sufficient_for_healthy_restore: bool,
}

#[derive(Debug, Clone)]
struct FetchGapFillResult {
    swaps: Vec<SwapEvent>,
    fetched_rows: usize,
    scanned_items: usize,
    scanned_pages: usize,
}

#[derive(Debug, Clone)]
struct TransactionsPage {
    entries: Vec<Value>,
    next_pagination_token: Option<String>,
}

trait HeliusHistoricalSource {
    fn get_transactions_for_address(
        &self,
        address: &str,
        pagination_token: Option<&str>,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        limit: usize,
    ) -> Result<TransactionsPage>;
}

struct HeliusTransactionsForAddressSource {
    client: Client,
    helius_http_url: String,
}

impl HeliusTransactionsForAddressSource {
    fn new(helius_http_url: String, request_timeout_ms: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(StdDuration::from_millis(request_timeout_ms.max(1)))
            .build()
            .context("failed building Helius gap-fill http client")?;
        Ok(Self {
            client,
            helius_http_url,
        })
    }
}

impl HeliusHistoricalSource for HeliusTransactionsForAddressSource {
    fn get_transactions_for_address(
        &self,
        address: &str,
        pagination_token: Option<&str>,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        limit: usize,
    ) -> Result<TransactionsPage> {
        let mut config = json!({
            "transactionDetails": "full",
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "sortOrder": "asc",
            "limit": limit.min(100),
            "filters": {
                "blockTime": {
                    "gte": window_start.timestamp(),
                    "lt": window_end.timestamp(),
                },
                "status": "succeeded",
                "tokenAccounts": "balanceChanged",
            }
        });
        if let Some(token) = pagination_token {
            config["paginationToken"] = Value::String(token.to_string());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransactionsForAddress",
            "params": [address, config],
        });
        let response = post_helius_json(&self.client, &self.helius_http_url, &payload)?;
        if let Some(error) = response.get("error") {
            bail!("helius getTransactionsForAddress returned error: {error}");
        }
        let result = response.get("result").ok_or_else(|| {
            anyhow!("missing result in helius getTransactionsForAddress response")
        })?;
        let entries = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("missing result.data array in helius response"))?
            .clone();
        let next_pagination_token = result
            .get("paginationToken")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .filter(|value| !value.trim().is_empty());
        Ok(TransactionsPage {
            entries,
            next_pagination_token,
        })
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
    let mut config_path: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut window_start: Option<DateTime<Utc>> = None;
    let mut window_end: Option<DateTime<Utc>> = None;
    let mut helius_http_url: Option<String> = None;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--window-start" => window_start = Some(parse_ts_arg("--window-start", args.next())?),
            "--window-end" => window_end = Some(parse_ts_arg("--window-end", args.next())?),
            "--helius-http-url" => {
                helius_http_url = Some(parse_string_arg("--helius-http-url", args.next())?)
            }
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if window_start.is_some() != window_end.is_some() {
        bail!("--window-start and --window-end must be provided together");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        output_path,
        window_start,
        window_end,
        helius_http_url,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let helius_http_url = config.helius_http_url.clone().unwrap_or_else(|| {
        loaded_config
            .recent_raw_gap_fill_helius
            .helius_http_url
            .clone()
    });
    if helius_http_url.trim().is_empty() {
        bail!(
            "recent_raw_gap_fill_helius.helius_http_url must be configured or overridden with --helius-http-url"
        );
    }
    let source = HeliusTransactionsForAddressSource::new(
        helius_http_url,
        loaded_config.recent_raw_gap_fill_helius.request_timeout_ms,
    )?;
    run_with_source(config, &source)
}

fn run_with_source<S: HeliusHistoricalSource>(config: Config, source: &S) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let runtime_store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening runtime db {}", db_path.display()))?;
    let active_wallets = runtime_store
        .list_active_follow_wallets()?
        .into_iter()
        .collect::<Vec<_>>();
    if active_wallets.is_empty() {
        bail!("gap fill requires at least one active follow wallet in the restored runtime db");
    }

    let plan = resolve_gap_fill_plan(&runtime_store, config.window_start, config.window_end)?;
    let program_ids = ProgramIdConfig {
        interested_program_ids: loaded_config
            .ingestion
            .subscribe_program_ids
            .iter()
            .chain(loaded_config.ingestion.raydium_program_ids.iter())
            .chain(loaded_config.ingestion.pumpswap_program_ids.iter())
            .cloned()
            .collect(),
        raydium_program_ids: loaded_config
            .ingestion
            .raydium_program_ids
            .iter()
            .cloned()
            .collect(),
        pumpswap_program_ids: loaded_config
            .ingestion
            .pumpswap_program_ids
            .iter()
            .cloned()
            .collect(),
    };

    let fetch = fetch_gap_fill_swaps(
        source,
        &active_wallets,
        &plan,
        &program_ids,
        loaded_config.recent_raw_gap_fill_helius.page_size,
        loaded_config
            .recent_raw_gap_fill_helius
            .max_pages_per_wallet,
    )?;
    let output = write_gap_fill_output(
        &config,
        &loaded_config.recent_raw_gap_fill_helius.output_dir,
        loaded_config.recent_raw_gap_fill_helius.output_retention,
        &db_path,
        active_wallets.len(),
        &plan,
        fetch,
        config.now,
    )?;

    if config.json {
        serde_json::to_string_pretty(&output)
            .context("failed serializing helius gap-fill output json")
    } else {
        Ok(render_human(&output))
    }
}

fn render_human(output: &GapFillOutput) -> String {
    [
        "event=discovery_raw_gap_fill_helius".to_string(),
        format!("db_path={}", output.db_path),
        format!("output_db_path={}", output.output_db_path),
        format!(
            "latest_db_path={}",
            output.latest_db_path.as_deref().unwrap_or("null")
        ),
        format!("metadata_path={}", output.metadata_path),
        format!(
            "requested_window_start={}",
            output.requested_window_start.to_rfc3339()
        ),
        format!(
            "requested_window_end={}",
            output.requested_window_end.to_rfc3339()
        ),
        format!(
            "derived_from_restore_state={}",
            output.derived_from_restore_state
        ),
        format!("active_follow_wallets={}", output.active_follow_wallets),
        format!("fetched_rows={}", output.fetched_rows),
        format!("inserted_rows={}", output.inserted_rows),
        format!("scanned_items={}", output.scanned_items),
        format!("scanned_pages={}", output.scanned_pages),
        format!(
            "gap_fill_covered_since={}",
            format_optional_ts(output.gap_fill_covered_since.as_ref())
        ),
        format!(
            "gap_fill_covered_through={}",
            format_optional_cursor(output.gap_fill_covered_through_cursor.as_ref())
        ),
        format!(
            "final_covered_since={}",
            format_optional_ts(output.final_covered_since.as_ref())
        ),
        format!(
            "final_covered_through={}",
            format_optional_cursor(output.final_covered_through_cursor.as_ref())
        ),
        format!(
            "source_lag_seconds={}",
            output
                .source_lag_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "missing_segments={}",
            if output.missing_segments.is_empty() {
                "[]".to_string()
            } else {
                serde_json::to_string(&output.missing_segments).unwrap_or_else(|_| "[]".to_string())
            }
        ),
        format!(
            "sufficient_for_healthy_restore={}",
            output.sufficient_for_healthy_restore
        ),
    ]
    .join("\n")
}

fn fetch_gap_fill_swaps<S: HeliusHistoricalSource>(
    source: &S,
    active_wallets: &[String],
    plan: &GapFillPlan,
    program_ids: &ProgramIdConfig,
    page_size: usize,
    max_pages_per_wallet: usize,
) -> Result<FetchGapFillResult> {
    let mut unique_tx_signatures = HashSet::new();
    let mut swaps = Vec::new();
    let mut fetched_rows = 0usize;
    let mut scanned_items = 0usize;
    let mut scanned_pages = 0usize;

    let mut wallets = active_wallets.to_vec();
    wallets.sort();

    for wallet in wallets {
        let mut pagination_token: Option<String> = None;
        for _ in 0..max_pages_per_wallet.max(1) {
            let page = source.get_transactions_for_address(
                &wallet,
                pagination_token.as_deref(),
                plan.requested_window_start,
                plan.requested_window_end,
                page_size,
            )?;
            scanned_pages = scanned_pages.saturating_add(1);
            if page.entries.is_empty() {
                break;
            }

            for entry in &page.entries {
                scanned_items = scanned_items.saturating_add(1);
                let Some(signature) = transaction_signature(entry) else {
                    continue;
                };
                let Some(block_time) = entry
                    .get("blockTime")
                    .and_then(Value::as_i64)
                    .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
                else {
                    continue;
                };
                if block_time < plan.requested_window_start
                    || block_time >= plan.requested_window_end
                {
                    continue;
                }
                if !unique_tx_signatures.insert(signature) {
                    continue;
                }
                let Some(swap) = parse_transaction_to_swap(entry, &wallet, program_ids)? else {
                    continue;
                };
                if swap.ts_utc >= plan.requested_window_start
                    && swap.ts_utc < plan.requested_window_end
                {
                    fetched_rows = fetched_rows.saturating_add(1);
                    swaps.push(swap);
                }
            }

            let Some(next_token) = page.next_pagination_token else {
                break;
            };
            if pagination_token.as_deref() == Some(next_token.as_str()) {
                break;
            }
            pagination_token = Some(next_token);
        }
    }

    swaps.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    Ok(FetchGapFillResult {
        swaps,
        fetched_rows,
        scanned_items,
        scanned_pages,
    })
}

fn transaction_signature(entry: &Value) -> Option<String> {
    entry
        .pointer("/transaction/signatures/0")
        .and_then(Value::as_str)
        .or_else(|| entry.get("signature").and_then(Value::as_str))
        .map(ToString::to_string)
}

fn write_gap_fill_output(
    config: &Config,
    configured_output_dir: &str,
    output_retention: usize,
    db_path: &Path,
    active_follow_wallets: usize,
    plan: &GapFillPlan,
    fetch: FetchGapFillResult,
    now: DateTime<Utc>,
) -> Result<GapFillOutput> {
    let (output_db_path, latest_db_path, metadata_path, output_dir) = if let Some(path) =
        config.output_path.as_ref()
    {
        (
            path.clone(),
            None,
            journal_snapshot_metadata_path(path),
            None::<PathBuf>,
        )
    } else {
        let dir = resolve_relative_to_config(&config.config_path, Path::new(configured_output_dir));
        (
            gap_fill_helius_archive_path(&dir, now),
            Some(gap_fill_helius_latest_path(&dir)),
            gap_fill_helius_latest_metadata_path(&dir),
            Some(dir),
        )
    };

    reset_sqlite_path(&output_db_path)?;
    let output_store = SqliteStore::open(&output_db_path).with_context(|| {
        format!(
            "failed opening Helius gap-fill output db {}",
            output_db_path.display()
        )
    })?;
    output_store.ensure_recent_raw_journal_tables()?;
    let inserted_rows = output_store
        .insert_recent_raw_journal_batch(&fetch.swaps, now)?
        .inserted_rows;
    let journal_state = output_store.recent_raw_journal_state_read_only()?;
    let missing_segments = compute_missing_segments(
        plan.requested_window_start,
        plan.requested_window_end,
        &journal_state,
    );
    let source_lag_seconds = missing_segments.first().map(|segment| {
        segment
            .end
            .signed_duration_since(segment.start)
            .num_seconds()
            .max(0) as u64
    });
    let final_covered_since = min_ts_opt(journal_state.covered_since, plan.journal_covered_since);
    let final_covered_through_cursor = max_cursor_opt(
        journal_state.covered_through_cursor.as_ref(),
        plan.journal_covered_through_cursor.as_ref(),
    );
    let sufficient_for_healthy_restore = plan.journal_available
        && plan.journal_covers_artifact_cursor
        && final_covered_since.is_some_and(|covered_since| {
            plan.required_window_start
                .is_some_and(|required| covered_since <= required)
        });

    let output = GapFillOutput {
        db_path: db_path.display().to_string(),
        output_db_path: output_db_path.display().to_string(),
        latest_db_path: latest_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        metadata_path: metadata_path.display().to_string(),
        requested_window_start: plan.requested_window_start,
        requested_window_end: plan.requested_window_end,
        required_window_start: plan.required_window_start,
        derived_from_restore_state: plan.derived_from_restore_state,
        active_follow_wallets,
        journal_available: plan.journal_available,
        journal_covers_artifact_cursor: plan.journal_covers_artifact_cursor,
        journal_covered_since: plan.journal_covered_since,
        journal_covered_through_cursor: plan.journal_covered_through_cursor.clone(),
        fetched_rows: fetch.fetched_rows,
        inserted_rows,
        scanned_items: fetch.scanned_items,
        scanned_pages: fetch.scanned_pages,
        gap_fill_covered_since: journal_state.covered_since,
        gap_fill_covered_through_cursor: journal_state.covered_through_cursor.clone(),
        final_covered_since,
        final_covered_through_cursor,
        source_lag_seconds,
        missing_segments,
        sufficient_for_healthy_restore,
    };

    if let Some(dir) = output_dir.as_ref() {
        let latest_path = latest_db_path.as_ref().expect("latest path");
        copy_atomic(&output_db_path, latest_path)?;
        let archive_metadata_path = journal_snapshot_metadata_path(&output_db_path);
        write_json_atomic(&archive_metadata_path, &output)?;
        write_json_atomic(&metadata_path, &output)?;
        let pruned = prune_rotated_archives(
            dir,
            GAP_FILL_HELIUS_ARCHIVE_PREFIX,
            GAP_FILL_HELIUS_ARCHIVE_SUFFIX,
            output_retention.max(1),
        )?;
        for path in pruned {
            let metadata = journal_snapshot_metadata_path(&path);
            if metadata.exists() {
                let _ = fs::remove_file(metadata);
            }
        }
    } else {
        write_json_atomic(&metadata_path, &output)?;
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct RecordedHeliusRequest {
        address: String,
        pagination_token: Option<String>,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        limit: usize,
    }

    #[derive(Default)]
    struct FakeHeliusHistoricalSource {
        pages: HashMap<(String, Option<String>), TransactionsPage>,
        requests: Mutex<Vec<RecordedHeliusRequest>>,
    }

    impl FakeHeliusHistoricalSource {
        fn with_page(
            mut self,
            wallet: &str,
            pagination_token: Option<&str>,
            entries: Vec<Value>,
            next_pagination_token: Option<&str>,
        ) -> Self {
            self.pages.insert(
                (
                    wallet.to_string(),
                    pagination_token.map(ToString::to_string),
                ),
                TransactionsPage {
                    entries,
                    next_pagination_token: next_pagination_token.map(ToString::to_string),
                },
            );
            self
        }

        fn requests(&self) -> Vec<RecordedHeliusRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl HeliusHistoricalSource for FakeHeliusHistoricalSource {
        fn get_transactions_for_address(
            &self,
            address: &str,
            pagination_token: Option<&str>,
            window_start: DateTime<Utc>,
            window_end: DateTime<Utc>,
            limit: usize,
        ) -> Result<TransactionsPage> {
            self.requests.lock().unwrap().push(RecordedHeliusRequest {
                address: address.to_string(),
                pagination_token: pagination_token.map(ToString::to_string),
                window_start,
                window_end,
                limit,
            });
            Ok(self
                .pages
                .get(&(
                    address.to_string(),
                    pagination_token.map(ToString::to_string),
                ))
                .cloned()
                .unwrap_or(TransactionsPage {
                    entries: Vec::new(),
                    next_pagination_token: None,
                }))
        }
    }

    struct Fixture {
        temp: tempfile::TempDir,
        config_path: PathBuf,
        runtime_db_path: PathBuf,
    }

    #[test]
    fn parse_args_from_rejects_legacy_db_flag() {
        let error = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--legacy-db-path".to_string(),
            "/tmp/legacy.db".to_string(),
        ])
        .expect_err("legacy db flag must remain unsupported");
        assert!(error
            .to_string()
            .contains("unknown argument: --legacy-db-path"));
    }

    #[test]
    fn run_derives_exact_missing_bounded_window_from_restore_state() -> Result<()> {
        let fixture = make_fixture("gap-fill-helius-derive-window")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let source = FakeHeliusHistoricalSource::default();
        let output: GapFillOutput = serde_json::from_str(&run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(fixture.temp.path().join("derived-gap-fill-helius.sqlite")),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?)?;
        assert_eq!(
            output.requested_window_start.to_rfc3339(),
            "2026-03-17T13:43:40+00:00"
        );
        assert_eq!(
            output.requested_window_end.to_rfc3339(),
            "2026-03-22T13:43:40+00:00"
        );
        assert!(output.derived_from_restore_state);
        Ok(())
    }

    #[test]
    fn run_does_not_fetch_outside_required_horizon() -> Result<()> {
        let fixture = make_fixture("gap-fill-helius-bounded-fetch")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let window_start = now - Duration::days(7);
        let window_end = now - Duration::days(2);
        let source = FakeHeliusHistoricalSource::default().with_page(
            "wallet-gap",
            None,
            vec![
                swap_tx(
                    "inside-a",
                    "wallet-gap",
                    window_start,
                    90,
                    "raydium-program",
                ),
                swap_tx(
                    "inside-b",
                    "wallet-gap",
                    window_start + Duration::days(1),
                    91,
                    "raydium-program",
                ),
                swap_tx(
                    "too-old",
                    "wallet-gap",
                    window_start - Duration::minutes(1),
                    80,
                    "raydium-program",
                ),
            ],
            None,
        );

        let output: GapFillOutput = serde_json::from_str(&run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(fixture.temp.path().join("bounded-gap-fill-helius.sqlite")),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?)?;
        assert_eq!(output.fetched_rows, 2);
        assert_eq!(output.scanned_items, 3);
        assert_eq!(output.scanned_pages, 1);
        let requests = source.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].address, "wallet-gap");
        assert_eq!(requests[0].pagination_token, None);
        assert_eq!(requests[0].window_start, window_start);
        assert_eq!(requests[0].window_end, window_end);
        assert_eq!(requests[0].limit, 100);
        Ok(())
    }

    #[test]
    fn explicit_output_writes_standalone_sqlite_without_touching_latest_surfaces() -> Result<()> {
        let fixture = make_fixture("gap-fill-helius-explicit-output")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let gap_fill_path = fixture
            .temp
            .path()
            .join("standalone-helius-gap-fill.sqlite");
        let source = FakeHeliusHistoricalSource::default().with_page(
            "wallet-gap",
            None,
            vec![swap_tx(
                "inside-a",
                "wallet-gap",
                now - Duration::days(7),
                90,
                "raydium-program",
            )],
            None,
        );

        let output: GapFillOutput = serde_json::from_str(&run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(gap_fill_path.clone()),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?)?;
        assert_eq!(Path::new(&output.output_db_path), gap_fill_path.as_path());
        assert!(gap_fill_path.exists());
        assert!(gap_fill_path.with_extension("json").exists());
        assert!(!fixture
            .temp
            .path()
            .join("state/discovery_restore/gap_fill/latest.sqlite")
            .exists());
        assert!(!fixture
            .temp
            .path()
            .join("state/discovery_restore/gap_fill_helius/latest.sqlite")
            .exists());
        Ok(())
    }

    #[test]
    fn gap_fill_output_replays_into_runtime_store() -> Result<()> {
        let fixture = make_fixture("gap-fill-helius-output-replay")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let window_start = now - Duration::days(7);
        let gap_fill_path = fixture.temp.path().join("gap-fill-helius-replay.sqlite");
        let source = FakeHeliusHistoricalSource::default().with_page(
            "wallet-gap",
            None,
            vec![swap_tx(
                "inside-a",
                "wallet-gap",
                window_start,
                90,
                "raydium-program",
            )],
            None,
        );

        let _ = run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(gap_fill_path.clone()),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?;

        let replay_target_path = fixture.temp.path().join("replay-target-helius.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut replay_target = SqliteStore::open(&replay_target_path)?;
        replay_target.run_migrations(&migration_dir)?;
        let gap_fill_store = SqliteStore::open_read_only(&gap_fill_path)?;
        let artifact_cursor = artifact_cursor(now);
        let replay = gap_fill_store.replay_recent_raw_journal_into_runtime_store(
            &replay_target,
            window_start,
            &artifact_cursor,
            128,
        )?;
        assert_eq!(replay.replayed_rows, 1);
        assert_eq!(
            replay_target
                .load_observed_swaps_since(window_start - Duration::minutes(1))?
                .len(),
            1
        );
        Ok(())
    }

    #[test]
    fn healthy_verdict_is_possible_when_helius_output_closes_raw_coverage() -> Result<()> {
        let fixture = make_fixture("gap-fill-helius-healthy-verdict")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let journal_covered_since = now - Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            journal_covered_since,
        )?;
        let gap_fill_path = fixture.temp.path().join("gap-fill-helius-healthy.sqlite");
        let source = FakeHeliusHistoricalSource::default().with_page(
            "wallet-gap",
            None,
            vec![swap_tx(
                "inside-a",
                "wallet-gap",
                now - Duration::days(7),
                90,
                "raydium-program",
            )],
            None,
        );

        let _ = run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(gap_fill_path.clone()),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?;

        let loaded_config = load_from_path(&fixture.config_path)?;
        apply_gap_fill_replay_to_runtime(
            &fixture.runtime_db_path,
            &gap_fill_path,
            now,
            loaded_config.discovery.scoring_window_days as i64,
        )?;
        let runtime_store = SqliteStore::open(&fixture.runtime_db_path)?;
        let discovery = DiscoveryService::new(loaded_config.discovery, loaded_config.shadow);
        let verdict = discovery.runtime_restore_verdict(&runtime_store, now)?;
        assert_eq!(
            verdict.verdict,
            DiscoveryRuntimeRestoreVerdictKind::TradingReady.as_str()
        );
        Ok(())
    }

    #[test]
    fn partial_helius_output_remains_non_trading_ready() -> Result<()> {
        let fixture = make_fixture("gap-fill-helius-partial-verdict")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let journal_covered_since = now - Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            journal_covered_since,
        )?;
        let gap_fill_path = fixture.temp.path().join("gap-fill-helius-partial.sqlite");
        let source = FakeHeliusHistoricalSource::default().with_page(
            "wallet-gap",
            None,
            vec![swap_tx(
                "inside-a",
                "wallet-gap",
                now - Duration::days(3),
                90,
                "raydium-program",
            )],
            None,
        );

        let _ = run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(gap_fill_path.clone()),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?;

        let loaded_config = load_from_path(&fixture.config_path)?;
        apply_gap_fill_replay_to_runtime(
            &fixture.runtime_db_path,
            &gap_fill_path,
            now,
            loaded_config.discovery.scoring_window_days as i64,
        )?;
        let runtime_store = SqliteStore::open(&fixture.runtime_db_path)?;
        let discovery = DiscoveryService::new(loaded_config.discovery, loaded_config.shadow);
        let verdict = discovery.runtime_restore_verdict(&runtime_store, now)?;
        assert_eq!(
            verdict.verdict,
            DiscoveryRuntimeRestoreVerdictKind::FailClosed.as_str()
        );
        Ok(())
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir()?;
        let runtime_db_path = temp.path().join(format!("{name}-runtime.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        fs::write(
            &config_path,
            format!(
                "[system]\nmigrations_dir = \"{}\"\n\n[sqlite]\npath = \"{}\"\n\n[recent_raw_gap_fill_helius]\noutput_retention = 2\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[ingestion]\nsubscribe_program_ids = [\"raydium-program\"]\nraydium_program_ids = [\"raydium-program\"]\npumpswap_program_ids = [\"pump-program\"]\n\n[execution]\nenabled = false\n",
                migration_dir.display(),
                runtime_db_path.display()
            ),
        )?;
        Ok(Fixture {
            temp,
            config_path,
            runtime_db_path,
        })
    }

    fn seed_restored_runtime_with_short_journal(
        runtime_db_path: &Path,
        config_path: &Path,
        now: DateTime<Utc>,
        journal_covered_since: DateTime<Utc>,
    ) -> Result<()> {
        reset_sqlite_path(runtime_db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let source_db_path = runtime_db_path.with_file_name("source-artifact-helius.db");
        reset_sqlite_path(&source_db_path)?;
        let mut source_store = SqliteStore::open(&source_db_path)?;
        source_store.run_migrations(&migration_dir)?;
        seed_runtime_artifact_source(&source_store, now)?;
        let loaded_config = load_from_path(config_path)?;
        let discovery = DiscoveryService::new(loaded_config.discovery, loaded_config.shadow);
        let artifact = source_store
            .export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())?;

        let mut runtime_store = SqliteStore::open(runtime_db_path)?;
        runtime_store.run_migrations(&migration_dir)?;
        runtime_store.restore_discovery_runtime_artifact(&artifact, now, false)?;
        runtime_store.set_discovery_recent_raw_restore_state(
            &DiscoveryRecentRawRestoreStateUpdate {
                journal_available: true,
                journal_replayed: true,
                required_window_start: Some(now - Duration::days(7)),
                journal_covered_since: Some(journal_covered_since),
                journal_covered_through_cursor: Some(artifact.runtime_cursor.clone()),
                gap_fill_replayed: false,
                gap_fill_covered_since: None,
                gap_fill_covered_through_cursor: None,
                effective_covered_since: Some(journal_covered_since),
                effective_covered_through_cursor: Some(artifact.runtime_cursor.clone()),
                artifact_runtime_cursor: Some(artifact.runtime_cursor.clone()),
                journal_covers_artifact_cursor: true,
                raw_coverage_satisfied: false,
                gap_fill_replayed_rows: 0,
                replayed_rows: 1,
                reason: Some("recent_raw_journal_raw_coverage_unsatisfied".to_string()),
                replay_started_at: Some(now),
                replay_completed_at: Some(now),
            },
        )?;
        Ok(())
    }

    fn seed_runtime_artifact_source(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.upsert_wallet("wallet-gap", now - Duration::days(7), now, "candidate")?;
        let window_start = now - Duration::days(7);
        store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: "wallet-gap".to_string(),
            window_start,
            pnl: 12.0,
            win_rate: 0.6,
            trades: 12,
            closed_trades: 12,
            hold_median_seconds: 300,
            score: 0.88,
            buy_total: 12,
            tradable_ratio: 0.95,
            rug_ratio: 0.0,
        })?;
        store.activate_follow_wallet("wallet-gap", now, "seed-follow")?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
            reason: "seed-publication".to_string(),
            last_published_at: Some(now),
            last_published_window_start: Some(window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-gap".to_string()]),
        })?;
        store.upsert_discovery_runtime_cursor(&artifact_cursor(now))?;
        Ok(())
    }

    fn apply_gap_fill_replay_to_runtime(
        runtime_db_path: &Path,
        gap_fill_db_path: &Path,
        now: DateTime<Utc>,
        scoring_window_days: i64,
    ) -> Result<()> {
        let runtime_store = SqliteStore::open(runtime_db_path)?;
        let restore_state = runtime_store.discovery_recent_raw_restore_state_read_only()?;
        let required_window_start = now - Duration::days(scoring_window_days.max(1));
        let artifact_runtime_cursor = restore_state
            .artifact_runtime_cursor
            .ok_or_else(|| anyhow!("missing artifact runtime cursor in restore state"))?;
        let gap_fill_store = SqliteStore::open_read_only(gap_fill_db_path)?;
        let gap_fill_replay = gap_fill_store.replay_recent_raw_journal_into_runtime_store(
            &runtime_store,
            required_window_start,
            &artifact_runtime_cursor,
            128,
        )?;
        let effective_covered_since = min_ts_opt(
            restore_state.journal_covered_since,
            gap_fill_replay.journal_covered_since,
        );
        let effective_covered_through_cursor = max_cursor_opt(
            restore_state.journal_covered_through_cursor.as_ref(),
            gap_fill_replay.journal_covered_through_cursor.as_ref(),
        );
        let runtime_window_has_rows = !runtime_store
            .load_recent_observed_swaps_since(required_window_start, 1)?
            .0
            .is_empty();
        let raw_coverage_satisfied = restore_state.journal_available
            && restore_state.journal_covers_artifact_cursor
            && effective_covered_since
                .is_some_and(|covered_since| covered_since <= required_window_start)
            && runtime_window_has_rows;
        runtime_store.set_discovery_recent_raw_restore_state(
            &DiscoveryRecentRawRestoreStateUpdate {
                journal_available: restore_state.journal_available,
                journal_replayed: restore_state.journal_replayed,
                required_window_start: Some(required_window_start),
                journal_covered_since: restore_state.journal_covered_since,
                journal_covered_through_cursor: restore_state.journal_covered_through_cursor,
                gap_fill_replayed: true,
                gap_fill_covered_since: gap_fill_replay.journal_covered_since,
                gap_fill_covered_through_cursor: gap_fill_replay.journal_covered_through_cursor,
                effective_covered_since,
                effective_covered_through_cursor,
                artifact_runtime_cursor: Some(artifact_runtime_cursor),
                journal_covers_artifact_cursor: restore_state.journal_covers_artifact_cursor,
                raw_coverage_satisfied,
                gap_fill_replayed_rows: gap_fill_replay.replayed_rows,
                replayed_rows: restore_state
                    .replayed_rows
                    .saturating_add(gap_fill_replay.replayed_rows),
                reason: Some(if raw_coverage_satisfied {
                    "recent_raw_journal_gap_fill_replay_completed".to_string()
                } else {
                    format!(
                        "recent_raw_gap_fill_raw_coverage_unsatisfied:{}",
                        gap_fill_db_path.display()
                    )
                }),
                replay_started_at: Some(now),
                replay_completed_at: Some(now),
            },
        )?;
        Ok(())
    }

    fn artifact_cursor(now: DateTime<Utc>) -> DiscoveryRuntimeCursor {
        DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(30),
            slot: 120,
            signature: "artifact-cursor".to_string(),
        }
    }

    fn swap_tx(
        signature: &str,
        wallet: &str,
        ts: DateTime<Utc>,
        slot: u64,
        program_id: &str,
    ) -> Value {
        json!({
            "slot": slot,
            "blockTime": ts.timestamp(),
            "transaction": {
                "signatures": [signature],
                "message": {
                    "accountKeys": [
                        { "pubkey": wallet, "signer": true },
                        { "pubkey": program_id, "signer": false }
                    ],
                    "instructions": [
                        { "programId": program_id }
                    ]
                }
            },
            "meta": {
                "err": Value::Null,
                "preBalances": [1_500_000_000u64],
                "postBalances": [499_995_000u64],
                "preTokenBalances": [],
                "postTokenBalances": [
                    {
                        "owner": wallet,
                        "mint": "TokenOut11111111111111111111111111111111111",
                        "uiTokenAmount": {
                            "uiAmountString": "100.5",
                            "amount": "100500000",
                            "decimals": 6
                        }
                    }
                ],
                "logMessages": ["Program raydium-program invoke [1]"]
            }
        })
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }
}
