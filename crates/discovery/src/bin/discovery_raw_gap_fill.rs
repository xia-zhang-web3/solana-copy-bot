use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use copybot_discovery::runtime_restore_ops::{
    copy_atomic, gap_fill_archive_path, gap_fill_latest_metadata_path, gap_fill_latest_path,
    journal_snapshot_metadata_path, prune_rotated_archives, resolve_db_path, write_json_atomic,
    GAP_FILL_ARCHIVE_PREFIX, GAP_FILL_ARCHIVE_SUFFIX,
};
use copybot_storage::{DiscoveryRuntimeCursor, SqliteStore};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;
#[cfg(test)]
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_raw_gap_fill --config <path> [--db-path <path>] [--output <path>] [--window-start <rfc3339> --window-end <rfc3339>] [--helius-http-url <url>] [--json] [--now <rfc3339>]";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

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
    scanned_signatures: usize,
    gap_fill_covered_since: Option<DateTime<Utc>>,
    gap_fill_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    final_covered_since: Option<DateTime<Utc>>,
    final_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    source_lag_seconds: Option<u64>,
    missing_segments: Vec<GapFillMissingSegment>,
    sufficient_for_healthy_restore: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GapFillMissingSegment {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    reason: String,
}

#[derive(Debug, Clone)]
struct GapFillPlan {
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    required_window_start: Option<DateTime<Utc>>,
    derived_from_restore_state: bool,
    journal_available: bool,
    journal_covers_artifact_cursor: bool,
    journal_covered_since: Option<DateTime<Utc>>,
    journal_covered_through_cursor: Option<DiscoveryRuntimeCursor>,
}

#[derive(Debug, Clone)]
struct FetchGapFillResult {
    swaps: Vec<SwapEvent>,
    fetched_rows: usize,
    scanned_signatures: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoricalSignatureEntry {
    signature: String,
    slot: u64,
    block_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct ProgramIdConfig {
    interested_program_ids: HashSet<String>,
    raydium_program_ids: HashSet<String>,
    pumpswap_program_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
struct ParsedUiAmount {
    amount: f64,
    raw_amount: Option<String>,
    decimals: Option<u8>,
}

#[derive(Debug, Clone, Default)]
struct MintDelta {
    amount_delta: f64,
    raw_delta: Option<i128>,
    decimals: Option<u8>,
    exact_unavailable: bool,
}

trait HistoricalRawSource {
    fn list_signatures_for_address(
        &self,
        address: &str,
        before: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistoricalSignatureEntry>>;

    fn fetch_transaction(&self, signature: &str) -> Result<Option<Value>>;
}

struct HeliusHistoricalRawSource {
    client: Client,
    helius_http_url: String,
}

impl HeliusHistoricalRawSource {
    fn new(helius_http_url: String, request_timeout_ms: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(StdDuration::from_millis(request_timeout_ms.max(1)))
            .build()
            .context("failed building recent raw gap-fill http client")?;
        Ok(Self {
            client,
            helius_http_url,
        })
    }
}

impl HistoricalRawSource for HeliusHistoricalRawSource {
    fn list_signatures_for_address(
        &self,
        address: &str,
        before: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistoricalSignatureEntry>> {
        let mut options = json!({ "limit": limit.min(1_000) });
        if let Some(before) = before {
            options["before"] = Value::String(before.to_string());
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [address, options],
        });
        let response = post_helius_json(&self.client, &self.helius_http_url, &payload)?;
        let entries = rpc_result(&response)
            .as_array()
            .ok_or_else(|| anyhow!("missing signatures array in helius response"))?;
        entries
            .iter()
            .map(|entry| {
                let signature = entry
                    .get("signature")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing signature in helius response entry"))?
                    .to_string();
                let slot = entry
                    .get("slot")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let block_time = entry
                    .get("blockTime")
                    .and_then(Value::as_i64)
                    .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0));
                Ok(HistoricalSignatureEntry {
                    signature,
                    slot,
                    block_time,
                })
            })
            .collect()
    }

    fn fetch_transaction(&self, signature: &str) -> Result<Option<Value>> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        });
        let response = post_helius_json(&self.client, &self.helius_http_url, &payload)?;
        if response.get("error").is_some() {
            return Ok(None);
        }
        match response.get("result") {
            Some(value) if !value.is_null() => Ok(Some(value.clone())),
            _ => Ok(None),
        }
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
    let helius_http_url = config
        .helius_http_url
        .clone()
        .unwrap_or_else(|| loaded_config.recent_raw_gap_fill.helius_http_url.clone());
    if helius_http_url.trim().is_empty() {
        bail!(
            "recent_raw_gap_fill.helius_http_url must be configured or overridden with --helius-http-url"
        );
    }
    let source = HeliusHistoricalRawSource::new(
        helius_http_url,
        loaded_config.recent_raw_gap_fill.request_timeout_ms,
    )?;
    run_with_source(config, &source)
}

fn run_with_source<S: HistoricalRawSource>(config: Config, source: &S) -> Result<String> {
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
        loaded_config.recent_raw_gap_fill.signature_page_size,
        loaded_config
            .recent_raw_gap_fill
            .max_signature_pages_per_wallet,
    )?;
    let output = write_gap_fill_output(
        &config,
        &loaded_config.recent_raw_gap_fill.output_dir,
        loaded_config.recent_raw_gap_fill.output_retention,
        &db_path,
        active_wallets.len(),
        &plan,
        fetch,
        config.now,
    )?;

    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing gap-fill output json")
    } else {
        Ok(render_human(&output))
    }
}

fn render_human(output: &GapFillOutput) -> String {
    [
        "event=discovery_raw_gap_fill".to_string(),
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
        format!("scanned_signatures={}", output.scanned_signatures),
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

fn resolve_gap_fill_plan(
    runtime_store: &SqliteStore,
    explicit_window_start: Option<DateTime<Utc>>,
    explicit_window_end: Option<DateTime<Utc>>,
) -> Result<GapFillPlan> {
    let restore_state = runtime_store.discovery_recent_raw_restore_state_read_only()?;
    if let (Some(requested_window_start), Some(requested_window_end)) =
        (explicit_window_start, explicit_window_end)
    {
        if requested_window_end <= requested_window_start {
            bail!("gap-fill window end must be strictly after start");
        }
        return Ok(GapFillPlan {
            requested_window_start,
            requested_window_end,
            required_window_start: restore_state.required_window_start,
            derived_from_restore_state: false,
            journal_available: restore_state.journal_available,
            journal_covers_artifact_cursor: restore_state.journal_covers_artifact_cursor,
            journal_covered_since: restore_state.journal_covered_since,
            journal_covered_through_cursor: restore_state.journal_covered_through_cursor,
        });
    }

    if restore_state.raw_coverage_satisfied {
        bail!("recent raw coverage is already satisfied; bounded gap fill is not required");
    }

    let required_window_start = restore_state.required_window_start.ok_or_else(|| {
        anyhow!("restore state is missing required_window_start; rerun runtime restore first")
    })?;
    let requested_window_end = restore_state.journal_covered_since.ok_or_else(|| {
        anyhow!(
            "restore state is missing journal_covered_since; bounded gap-fill cannot derive a missing window"
        )
    })?;
    if requested_window_end <= required_window_start {
        bail!(
            "restore state does not expose a bounded missing window: journal_covered_since ({}) must be after required_window_start ({})",
            requested_window_end.to_rfc3339(),
            required_window_start.to_rfc3339()
        );
    }

    Ok(GapFillPlan {
        requested_window_start: required_window_start,
        requested_window_end,
        required_window_start: Some(required_window_start),
        derived_from_restore_state: true,
        journal_available: restore_state.journal_available,
        journal_covers_artifact_cursor: restore_state.journal_covers_artifact_cursor,
        journal_covered_since: restore_state.journal_covered_since,
        journal_covered_through_cursor: restore_state.journal_covered_through_cursor,
    })
}

fn fetch_gap_fill_swaps<S: HistoricalRawSource>(
    source: &S,
    active_wallets: &[String],
    plan: &GapFillPlan,
    program_ids: &ProgramIdConfig,
    signature_page_size: usize,
    max_signature_pages_per_wallet: usize,
) -> Result<FetchGapFillResult> {
    let mut unique_tx_signatures = HashSet::new();
    let mut swaps = Vec::new();
    let mut fetched_rows = 0usize;
    let mut scanned_signatures = 0usize;

    let mut wallets = active_wallets.to_vec();
    wallets.sort();

    for wallet in wallets {
        let mut before: Option<String> = None;
        for _ in 0..max_signature_pages_per_wallet.max(1) {
            let entries = source.list_signatures_for_address(
                &wallet,
                before.as_deref(),
                signature_page_size,
            )?;
            if entries.is_empty() {
                break;
            }

            let mut oldest_seen_in_page: Option<DateTime<Utc>> = None;
            for entry in &entries {
                scanned_signatures = scanned_signatures.saturating_add(1);
                if let Some(block_time) = entry.block_time {
                    oldest_seen_in_page = Some(
                        oldest_seen_in_page
                            .map(|current| current.min(block_time))
                            .unwrap_or(block_time),
                    );
                    if block_time < plan.requested_window_start
                        || block_time >= plan.requested_window_end
                    {
                        continue;
                    }
                } else {
                    continue;
                }

                if !unique_tx_signatures.insert(entry.signature.clone()) {
                    continue;
                }
                let Some(result) = source.fetch_transaction(&entry.signature)? else {
                    continue;
                };
                let Some(swap) = parse_transaction_to_swap(&result, &wallet, program_ids)? else {
                    continue;
                };
                if swap.ts_utc >= plan.requested_window_start
                    && swap.ts_utc < plan.requested_window_end
                {
                    fetched_rows = fetched_rows.saturating_add(1);
                    swaps.push(swap);
                }
            }

            before = entries.last().map(|entry| entry.signature.clone());
            if oldest_seen_in_page.is_some_and(|ts| ts < plan.requested_window_start) {
                break;
            }
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
        scanned_signatures,
    })
}

fn parse_transaction_to_swap(
    result: &Value,
    expected_wallet: &str,
    program_ids: &ProgramIdConfig,
) -> Result<Option<SwapEvent>> {
    let meta = match result.get("meta") {
        Some(value) if !value.is_null() => value,
        _ => return Ok(None),
    };
    if meta.get("err").map(|err| !err.is_null()).unwrap_or(false) {
        return Ok(None);
    }

    let account_keys = extract_account_keys(result);
    if account_keys.is_empty() {
        return Ok(None);
    }
    let signer_index = account_keys
        .iter()
        .position(|(_, is_signer)| *is_signer)
        .unwrap_or(0);
    let signer = account_keys
        .get(signer_index)
        .map(|(pubkey, _)| pubkey.as_str())
        .ok_or_else(|| anyhow!("missing signer in parsed account keys"))?;
    if signer != expected_wallet {
        return Ok(None);
    }

    let mut normalized_program_ids = extract_program_ids(result, meta);
    if normalized_program_ids.is_empty() {
        if program_ids.interested_program_ids.is_empty() {
            return Ok(None);
        }
        normalized_program_ids = program_ids.interested_program_ids.clone();
    } else if !program_ids.interested_program_ids.is_empty()
        && !normalized_program_ids
            .iter()
            .any(|program| program_ids.interested_program_ids.contains(program))
    {
        return Ok(None);
    }

    let (token_in, amount_in, token_out, amount_out) =
        match infer_swap_from_json_balances(meta, signer_index, signer) {
            Some(value) => value,
            None => return Ok(None),
        };
    let logs = value_to_string_vec(meta.get("logMessages")).unwrap_or_default();
    let dex = detect_dex(
        &normalized_program_ids,
        &logs,
        &program_ids.raydium_program_ids,
        &program_ids.pumpswap_program_ids,
    )
    .ok_or_else(|| anyhow!("unable to classify dex for {}", expected_wallet))?;
    let ts_utc = result
        .get("blockTime")
        .and_then(Value::as_i64)
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);
    let slot = result
        .get("slot")
        .and_then(Value::as_u64)
        .unwrap_or_default();

    Ok(Some(SwapEvent {
        wallet: signer.to_string(),
        dex,
        token_in,
        token_out,
        amount_in: amount_in.amount,
        amount_out: amount_out.amount,
        signature: result
            .pointer("/transaction/signatures/0")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        slot,
        ts_utc,
        exact_amounts: build_exact_swap_amounts(&amount_in, &amount_out),
    }))
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
    let (output_db_path, latest_db_path, metadata_path, output_dir) =
        if let Some(path) = config.output_path.as_ref() {
            (
                path.clone(),
                None,
                journal_snapshot_metadata_path(path),
                None::<PathBuf>,
            )
        } else {
            let dir = copybot_discovery::runtime_restore_ops::resolve_relative_to_config(
                &config.config_path,
                Path::new(configured_output_dir),
            );
            (
                gap_fill_archive_path(&dir, now),
                Some(gap_fill_latest_path(&dir)),
                gap_fill_latest_metadata_path(&dir),
                Some(dir),
            )
        };

    reset_sqlite_path(&output_db_path)?;
    let output_store = SqliteStore::open(&output_db_path).with_context(|| {
        format!(
            "failed opening gap-fill output db {}",
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
        scanned_signatures: fetch.scanned_signatures,
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
            GAP_FILL_ARCHIVE_PREFIX,
            GAP_FILL_ARCHIVE_SUFFIX,
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

fn reset_sqlite_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    for suffix in ["", "-wal", "-shm"] {
        let candidate = if suffix.is_empty() {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}{}", path.display(), suffix))
        };
        if candidate.exists() {
            fs::remove_file(&candidate)
                .with_context(|| format!("failed removing {}", candidate.display()))?;
        }
    }
    Ok(())
}

fn compute_missing_segments(
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    journal_state: &copybot_storage::RecentRawJournalStateRow,
) -> Vec<GapFillMissingSegment> {
    if journal_state.row_count == 0 {
        return vec![GapFillMissingSegment {
            start: requested_window_start,
            end: requested_window_end,
            reason: "source_uncovered_window".to_string(),
        }];
    }
    let Some(covered_since) = journal_state.covered_since else {
        return vec![GapFillMissingSegment {
            start: requested_window_start,
            end: requested_window_end,
            reason: "source_uncovered_window".to_string(),
        }];
    };
    if covered_since > requested_window_start {
        return vec![GapFillMissingSegment {
            start: requested_window_start,
            end: covered_since,
            reason: "source_lag".to_string(),
        }];
    }
    Vec::new()
}

fn min_ts_opt(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_cursor_opt(
    left: Option<&DiscoveryRuntimeCursor>,
    right: Option<&DiscoveryRuntimeCursor>,
) -> Option<DiscoveryRuntimeCursor> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if cmp_cursor(left, right) != Ordering::Less {
                Some(left.clone())
            } else {
                Some(right.clone())
            }
        }
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn cmp_cursor(left: &DiscoveryRuntimeCursor, right: &DiscoveryRuntimeCursor) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_cursor(value: Option<&DiscoveryRuntimeCursor>) -> String {
    value
        .map(|cursor| {
            format!(
                "{}:{}:{}",
                cursor.ts_utc.to_rfc3339(),
                cursor.slot,
                cursor.signature
            )
        })
        .unwrap_or_else(|| "null".to_string())
}

fn post_helius_json(client: &Client, helius_http_url: &str, payload: &Value) -> Result<Value> {
    let response = client
        .post(helius_http_url)
        .json(payload)
        .send()
        .with_context(|| format!("failed helius rpc request to {helius_http_url}"))?;
    let status = response.status();
    let body = response
        .text()
        .context("failed reading helius rpc response body")?;
    let parsed: Value =
        serde_json::from_str(&body).context("failed parsing helius rpc response json")?;
    if !status.is_success() {
        return Err(anyhow!("helius rpc returned http status {status}: {body}"));
    }
    Ok(parsed)
}

fn rpc_result(response: &Value) -> &Value {
    response.get("result").unwrap_or(&Value::Null)
}

fn extract_account_keys(result: &Value) -> Vec<(String, bool)> {
    result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .filter_map(|item| {
                    if let Some(pubkey) = item.as_str() {
                        return Some((pubkey.to_string(), false));
                    }
                    let pubkey = item.get("pubkey").and_then(Value::as_str)?;
                    let signer = item.get("signer").and_then(Value::as_bool).unwrap_or(false);
                    Some((pubkey.to_string(), signer))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn extract_program_ids(result: &Value, meta: &Value) -> HashSet<String> {
    let mut set = HashSet::new();

    if let Some(ixs) = result
        .pointer("/transaction/message/instructions")
        .and_then(Value::as_array)
    {
        for ix in ixs {
            if let Some(program_id) = ix.get("programId").and_then(Value::as_str) {
                set.insert(program_id.to_string());
            }
        }
    }

    if let Some(inner) = meta.get("innerInstructions").and_then(Value::as_array) {
        for group in inner {
            if let Some(ixs) = group.get("instructions").and_then(Value::as_array) {
                for ix in ixs {
                    if let Some(program_id) = ix.get("programId").and_then(Value::as_str) {
                        set.insert(program_id.to_string());
                    }
                }
            }
        }
    }

    for log in value_to_string_vec(meta.get("logMessages"))
        .unwrap_or_default()
        .iter()
    {
        if let Some(program_id) = extract_program_id_from_log(log) {
            set.insert(program_id);
        }
    }

    set
}

fn extract_program_id_from_log(log: &str) -> Option<String> {
    let mut parts = log.split_whitespace();
    if parts.next()? != "Program" {
        return None;
    }
    let program_id = parts.next()?.trim();
    if program_id.is_empty() {
        None
    } else {
        Some(program_id.to_string())
    }
}

fn infer_swap_from_json_balances(
    meta: &Value,
    signer_index: usize,
    signer: &str,
) -> Option<(String, ParsedUiAmount, String, ParsedUiAmount)> {
    const TOKEN_EPS: f64 = 1e-12;
    const SOL_EPS: f64 = 1e-8;
    let mut mint_deltas: HashMap<String, MintDelta> = HashMap::new();

    let pre = meta
        .get("preTokenBalances")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let post = meta
        .get("postTokenBalances")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    for item in pre {
        if item.get("owner").and_then(Value::as_str) == Some(signer) {
            let mint = item.get("mint").and_then(Value::as_str)?.to_string();
            let amount = parse_ui_amount_json(item.get("uiTokenAmount"))?;
            mint_deltas.entry(mint).or_default().apply_sub(&amount);
        }
    }
    for item in post {
        if item.get("owner").and_then(Value::as_str) == Some(signer) {
            let mint = item.get("mint").and_then(Value::as_str)?.to_string();
            let amount = parse_ui_amount_json(item.get("uiTokenAmount"))?;
            mint_deltas.entry(mint).or_default().apply_add(&amount);
        }
    }

    let mut token_in_candidates = Vec::new();
    let mut token_out_candidates = Vec::new();
    for (mint, delta) in &mint_deltas {
        if delta.amount_delta < -TOKEN_EPS {
            token_in_candidates.push((mint.clone(), delta.candidate()));
        } else if delta.amount_delta > TOKEN_EPS {
            token_out_candidates.push((mint.clone(), delta.candidate()));
        }
    }
    token_in_candidates.sort_by(|a, b| {
        b.1.amount
            .partial_cmp(&a.1.amount)
            .unwrap_or(Ordering::Equal)
    });
    token_out_candidates.sort_by(|a, b| {
        b.1.amount
            .partial_cmp(&a.1.amount)
            .unwrap_or(Ordering::Equal)
    });

    let sol_token_delta = mint_deltas
        .get(SOL_MINT)
        .map(|delta| delta.amount_delta)
        .unwrap_or(0.0);
    if sol_token_delta < -TOKEN_EPS {
        if let Some((out_mint, out_amt)) = dominant_non_sol_leg(&token_out_candidates) {
            return Some((
                SOL_MINT.to_string(),
                ParsedUiAmount {
                    amount: sol_token_delta.abs(),
                    raw_amount: mint_deltas
                        .get(SOL_MINT)
                        .and_then(|delta| delta.raw_delta.map(|value| value.abs().to_string())),
                    decimals: mint_deltas.get(SOL_MINT).and_then(|delta| delta.decimals),
                },
                out_mint,
                out_amt,
            ));
        }
    }
    if sol_token_delta > TOKEN_EPS {
        if let Some((in_mint, in_amt)) = dominant_non_sol_leg(&token_in_candidates) {
            return Some((
                in_mint,
                in_amt,
                SOL_MINT.to_string(),
                ParsedUiAmount {
                    amount: sol_token_delta,
                    raw_amount: mint_deltas
                        .get(SOL_MINT)
                        .and_then(|delta| delta.raw_delta.map(|value| value.abs().to_string())),
                    decimals: mint_deltas.get(SOL_MINT).and_then(|delta| delta.decimals),
                },
            ));
        }
    }

    let sol_delta = signer_sol_delta(meta, signer_index);
    let sol_amount = sol_delta.as_ref().map(|value| value.amount).unwrap_or(0.0);
    let sol_exact = sol_delta.map(|value| ParsedUiAmount {
        amount: value.amount.abs(),
        raw_amount: value.raw_amount,
        decimals: value.decimals,
    });
    if sol_amount < -SOL_EPS {
        if let Some((out_mint, out_amt)) = dominant_non_sol_leg(&token_out_candidates) {
            return Some((SOL_MINT.to_string(), sol_exact.clone()?, out_mint, out_amt));
        }
    }
    if sol_amount > SOL_EPS {
        if let Some((in_mint, in_amt)) = dominant_non_sol_leg(&token_in_candidates) {
            return Some((in_mint, in_amt, SOL_MINT.to_string(), sol_exact?));
        }
    }

    None
}

fn signer_sol_delta(meta: &Value, signer_index: usize) -> Option<ParsedUiAmount> {
    let pre_sol = meta
        .get("preBalances")
        .and_then(Value::as_array)
        .and_then(|balances| balances.get(signer_index))
        .and_then(Value::as_u64)?;
    let post_sol = meta
        .get("postBalances")
        .and_then(Value::as_array)
        .and_then(|balances| balances.get(signer_index))
        .and_then(Value::as_u64)?;
    let delta = post_sol as i128 - pre_sol as i128;
    Some(ParsedUiAmount {
        amount: delta as f64 / 1_000_000_000.0,
        raw_amount: None,
        decimals: None,
    })
}

fn dominant_non_sol_leg(entries: &[(String, ParsedUiAmount)]) -> Option<(String, ParsedUiAmount)> {
    const EPS: f64 = 1e-12;
    const SECOND_LEG_AMBIGUITY_RATIO: f64 = 0.15;
    let non_sol: Vec<(String, ParsedUiAmount)> = entries
        .iter()
        .filter(|(mint, value)| mint != SOL_MINT && value.amount > EPS)
        .cloned()
        .collect();
    let (primary_mint, primary_value) = non_sol.first()?.clone();
    if non_sol.len() >= 2 {
        let second_value = non_sol[1].1.amount;
        if second_value > primary_value.amount * SECOND_LEG_AMBIGUITY_RATIO {
            return None;
        }
    }
    Some((primary_mint, primary_value))
}

fn parse_ui_amount_json(ui_amount: Option<&Value>) -> Option<ParsedUiAmount> {
    let ui_amount = ui_amount?;
    if let Some(amount) = ui_amount.get("uiAmountString").and_then(Value::as_str) {
        let parsed = amount.parse::<f64>().ok()?;
        return parsed.is_finite().then(|| ParsedUiAmount {
            amount: parsed,
            raw_amount: ui_amount
                .get("amount")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            decimals: ui_amount
                .get("decimals")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok()),
        });
    }
    if let Some(amount) = ui_amount.get("uiAmount").and_then(Value::as_f64) {
        return amount.is_finite().then_some(ParsedUiAmount {
            amount,
            raw_amount: ui_amount
                .get("amount")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            decimals: ui_amount
                .get("decimals")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok()),
        });
    }
    let raw = ui_amount.get("amount").and_then(Value::as_str)?;
    let decimals = ui_amount.get("decimals").and_then(Value::as_u64)?;
    if decimals > 18 {
        return None;
    }
    let parsed_raw = raw.parse::<f64>().ok()?;
    let amount = parsed_raw / 10f64.powi(decimals as i32);
    amount.is_finite().then(|| ParsedUiAmount {
        amount,
        raw_amount: Some(raw.to_string()),
        decimals: u8::try_from(decimals).ok(),
    })
}

fn build_exact_swap_amounts(
    amount_in: &ParsedUiAmount,
    amount_out: &ParsedUiAmount,
) -> Option<ExactSwapAmounts> {
    Some(ExactSwapAmounts {
        amount_in_raw: amount_in.raw_amount.clone()?,
        amount_in_decimals: amount_in.decimals?,
        amount_out_raw: amount_out.raw_amount.clone()?,
        amount_out_decimals: amount_out.decimals?,
    })
}

fn value_to_string_vec(value: Option<&Value>) -> Option<Vec<String>> {
    Some(
        value?
            .as_array()?
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
    )
}

fn detect_dex(
    program_ids: &HashSet<String>,
    logs: &[String],
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
) -> Option<String> {
    if program_ids
        .iter()
        .any(|program| raydium_program_ids.contains(program))
        || logs
            .iter()
            .any(|log| log.to_ascii_lowercase().contains("raydium"))
    {
        return Some("raydium".to_string());
    }
    if program_ids
        .iter()
        .any(|program| pumpswap_program_ids.contains(program))
        || logs
            .iter()
            .any(|log| log.to_ascii_lowercase().contains("pump"))
    {
        return Some("pumpswap".to_string());
    }
    None
}

impl MintDelta {
    fn apply_sub(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta -= amount.amount;
        self.apply_raw_delta(amount, -1);
    }

    fn apply_add(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta += amount.amount;
        self.apply_raw_delta(amount, 1);
    }

    fn apply_raw_delta(&mut self, amount: &ParsedUiAmount, sign: i8) {
        if self.exact_unavailable {
            return;
        }
        let Some(raw_amount) = amount.raw_amount.as_deref() else {
            self.invalidate_exact();
            return;
        };
        let Some(decimals) = amount.decimals else {
            self.invalidate_exact();
            return;
        };
        let Some(parsed_raw) = raw_amount.parse::<u64>().ok() else {
            self.invalidate_exact();
            return;
        };
        let parsed_raw = i128::from(parsed_raw);
        let Some(signed_raw) = parsed_raw.checked_mul(i128::from(sign)) else {
            self.invalidate_exact();
            return;
        };
        match self.decimals {
            Some(existing) if existing != decimals => self.invalidate_exact(),
            Some(_) => {
                if let Some(current) = self.raw_delta {
                    match current.checked_add(signed_raw) {
                        Some(next) => self.raw_delta = Some(next),
                        None => self.invalidate_exact(),
                    }
                } else {
                    self.invalidate_exact();
                }
            }
            None => {
                self.decimals = Some(decimals);
                self.raw_delta = Some(signed_raw);
            }
        }
    }

    fn invalidate_exact(&mut self) {
        self.raw_delta = None;
        self.decimals = None;
        self.exact_unavailable = true;
    }

    fn candidate(&self) -> ParsedUiAmount {
        ParsedUiAmount {
            amount: self.amount_delta.abs(),
            raw_amount: self.raw_delta.map(|value| value.abs().to_string()),
            decimals: self.decimals,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRecentRawRestoreStateUpdate,
        DiscoveryRuntimeCursor, WalletMetricRow,
    };

    #[derive(Default)]
    struct FakeHistoricalRawSource {
        signature_pages: HashMap<(String, Option<String>), Vec<HistoricalSignatureEntry>>,
        transactions: HashMap<String, Value>,
        fetched_signatures: std::sync::Mutex<Vec<String>>,
    }

    impl FakeHistoricalRawSource {
        fn with_signature_page(
            mut self,
            wallet: &str,
            before: Option<&str>,
            entries: Vec<HistoricalSignatureEntry>,
        ) -> Self {
            self.signature_pages.insert(
                (wallet.to_string(), before.map(ToString::to_string)),
                entries,
            );
            self
        }

        fn with_transaction(mut self, signature: &str, tx: Value) -> Self {
            self.transactions.insert(signature.to_string(), tx);
            self
        }

        fn fetched_signatures(&self) -> Vec<String> {
            self.fetched_signatures.lock().unwrap().clone()
        }
    }

    impl HistoricalRawSource for FakeHistoricalRawSource {
        fn list_signatures_for_address(
            &self,
            address: &str,
            before: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<HistoricalSignatureEntry>> {
            Ok(self
                .signature_pages
                .get(&(address.to_string(), before.map(ToString::to_string)))
                .cloned()
                .unwrap_or_default())
        }

        fn fetch_transaction(&self, signature: &str) -> Result<Option<Value>> {
            self.fetched_signatures
                .lock()
                .unwrap()
                .push(signature.to_string());
            Ok(self.transactions.get(signature).cloned())
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
        let fixture = make_fixture("gap-fill-derive-window")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let source = FakeHistoricalRawSource::default();
        let output: GapFillOutput = serde_json::from_str(&run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(fixture.temp.path().join("derived-gap-fill.sqlite")),
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
    fn run_does_not_fetch_transactions_outside_required_horizon() -> Result<()> {
        let fixture = make_fixture("gap-fill-bounded-fetch")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let window_start = now - chrono::Duration::days(7);
        let window_end = now - chrono::Duration::days(2);
        let source = FakeHistoricalRawSource::default()
            .with_signature_page(
                "wallet-gap",
                None,
                vec![
                    signature_entry("too-new", window_end + chrono::Duration::minutes(5), 100),
                    signature_entry("in-window-a", window_start, 90),
                    signature_entry("in-window-b", window_start + chrono::Duration::days(1), 91),
                    signature_entry("too-old", window_start - chrono::Duration::minutes(1), 80),
                ],
            )
            .with_transaction(
                "in-window-a",
                swap_tx(
                    "in-window-a",
                    "wallet-gap",
                    window_start,
                    90,
                    "raydium-program",
                ),
            )
            .with_transaction(
                "in-window-b",
                swap_tx(
                    "in-window-b",
                    "wallet-gap",
                    window_start + chrono::Duration::days(1),
                    91,
                    "raydium-program",
                ),
            )
            .with_transaction(
                "too-new",
                swap_tx(
                    "too-new",
                    "wallet-gap",
                    window_end + chrono::Duration::minutes(5),
                    100,
                    "raydium-program",
                ),
            )
            .with_transaction(
                "too-old",
                swap_tx(
                    "too-old",
                    "wallet-gap",
                    window_start - chrono::Duration::minutes(1),
                    80,
                    "raydium-program",
                ),
            );

        let output: GapFillOutput = serde_json::from_str(&run_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                output_path: Some(fixture.temp.path().join("bounded-gap-fill.sqlite")),
                window_start: None,
                window_end: None,
                helius_http_url: Some("https://example.invalid".to_string()),
                json: true,
                now,
            },
            &source,
        )?)?;
        assert_eq!(output.fetched_rows, 2);
        assert_eq!(
            source.fetched_signatures(),
            vec!["in-window-a".to_string(), "in-window-b".to_string()]
        );
        Ok(())
    }

    #[test]
    fn gap_fill_output_replays_into_runtime_store() -> Result<()> {
        let fixture = make_fixture("gap-fill-output-replay")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let window_start = now - chrono::Duration::days(7);
        let gap_fill_path = fixture.temp.path().join("gap-fill-replay.sqlite");
        let source = FakeHistoricalRawSource::default()
            .with_signature_page(
                "wallet-gap",
                None,
                vec![signature_entry("in-window-a", window_start, 90)],
            )
            .with_transaction(
                "in-window-a",
                swap_tx(
                    "in-window-a",
                    "wallet-gap",
                    window_start,
                    90,
                    "raydium-program",
                ),
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

        let replay_target_path = fixture.temp.path().join("replay-target.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut replay_target = SqliteStore::open(&replay_target_path)?;
        replay_target.run_migrations(&migration_dir)?;
        let gap_fill_store = SqliteStore::open_read_only(&gap_fill_path)?;
        let artifact_cursor = DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::minutes(30),
            slot: 120,
            signature: "artifact-cursor".to_string(),
        };
        let replay = gap_fill_store.replay_recent_raw_journal_into_runtime_store(
            &replay_target,
            window_start,
            &artifact_cursor,
            128,
        )?;
        assert_eq!(replay.replayed_rows, 1);
        assert_eq!(
            replay_target
                .load_observed_swaps_since(window_start - chrono::Duration::minutes(1))?
                .len(),
            1
        );
        Ok(())
    }

    #[test]
    fn retention_keeps_latest_gap_fill_archives_after_self_overwrite() -> Result<()> {
        let fixture = make_fixture("gap-fill-retention")?;
        let first_now = parse_ts("2026-03-24T10:00:00Z")?;
        let second_now = parse_ts("2026-03-24T10:10:00Z")?;
        let third_now = parse_ts("2026-03-24T10:20:00Z")?;
        for current_now in [first_now, second_now, third_now] {
            seed_restored_runtime_with_short_journal(
                &fixture.runtime_db_path,
                &fixture.config_path,
                current_now,
                current_now - chrono::Duration::days(2),
            )?;
            let source = FakeHistoricalRawSource::default()
                .with_signature_page(
                    "wallet-gap",
                    None,
                    vec![signature_entry(
                        format!("sig-{}", current_now.timestamp()).as_str(),
                        current_now - chrono::Duration::days(7),
                        90,
                    )],
                )
                .with_transaction(
                    format!("sig-{}", current_now.timestamp()).as_str(),
                    swap_tx(
                        format!("sig-{}", current_now.timestamp()).as_str(),
                        "wallet-gap",
                        current_now - chrono::Duration::days(7),
                        90,
                        "raydium-program",
                    ),
                );
            let _ = run_with_source(
                Config {
                    config_path: fixture.config_path.clone(),
                    db_path: Some(fixture.runtime_db_path.clone()),
                    output_path: None,
                    window_start: None,
                    window_end: None,
                    helius_http_url: Some("https://example.invalid".to_string()),
                    json: true,
                    now: current_now,
                },
                &source,
            )?;
        }

        let output_dir = fixture.temp.path().join("state/discovery_restore/gap_fill");
        let archives = fs::read_dir(&output_dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with("discovery_raw_gap_fill_") && name.ends_with(".sqlite"))
            .collect::<Vec<_>>();
        assert_eq!(archives.len(), 2);
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
                "[system]\nmigrations_dir = \"{}\"\n\n[sqlite]\npath = \"{}\"\n\n[recent_raw_gap_fill]\noutput_retention = 2\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[ingestion]\nsubscribe_program_ids = [\"raydium-program\"]\nraydium_program_ids = [\"raydium-program\"]\npumpswap_program_ids = [\"pump-program\"]\n\n[execution]\nenabled = false\n",
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
        let source_db_path = runtime_db_path.with_file_name("source-artifact.db");
        reset_sqlite_path(&source_db_path)?;
        let mut source_store = SqliteStore::open(&source_db_path)?;
        source_store.run_migrations(&migration_dir)?;
        seed_runtime_artifact_source(&source_store, now)?;
        let loaded_config = load_from_path(config_path)?;
        let discovery =
            copybot_discovery::DiscoveryService::new(loaded_config.discovery, loaded_config.shadow);
        let artifact = source_store
            .export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())?;

        let mut runtime_store = SqliteStore::open(runtime_db_path)?;
        runtime_store.run_migrations(&migration_dir)?;
        runtime_store.restore_discovery_runtime_artifact(&artifact, now, false)?;
        runtime_store.set_discovery_recent_raw_restore_state(
            &DiscoveryRecentRawRestoreStateUpdate {
                journal_available: true,
                journal_replayed: true,
                required_window_start: Some(now - chrono::Duration::days(7)),
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
        store.upsert_wallet(
            "wallet-gap",
            now - chrono::Duration::days(7),
            now,
            "candidate",
        )?;
        let window_start = now - chrono::Duration::days(7);
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

    fn artifact_cursor(now: DateTime<Utc>) -> DiscoveryRuntimeCursor {
        DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::minutes(30),
            slot: 120,
            signature: "artifact-cursor".to_string(),
        }
    }

    fn signature_entry(signature: &str, ts: DateTime<Utc>, slot: u64) -> HistoricalSignatureEntry {
        HistoricalSignatureEntry {
            signature: signature.to_string(),
            slot,
            block_time: Some(ts),
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
                    "accountKeys": [{ "pubkey": wallet, "signer": true }],
                    "instructions": [{ "programId": program_id }]
                }
            },
            "meta": {
                "err": null,
                "preBalances": [1_000_000_000u64],
                "postBalances": [500_000_000u64],
                "preTokenBalances": [],
                "postTokenBalances": [{
                    "owner": wallet,
                    "mint": "TokenMintA",
                    "uiTokenAmount": {
                        "uiAmountString": "1000",
                        "amount": "1000000000",
                        "decimals": 6
                    }
                }],
                "logMessages": [format!("Program {} invoke [1]", program_id), "raydium swap"]
            }
        })
    }

    fn parse_ts(value: &str) -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(value)
            .map(|ts| ts.with_timezone(&Utc))
            .with_context(|| format!("invalid timestamp: {value}"))
    }
}
