use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_core_types::SwapEvent;
use copybot_discovery::raw_gap_fill_support::{
    compute_missing_segments, format_optional_cursor, format_optional_ts, max_cursor_opt,
    min_ts_opt, parse_program_scoped_transaction_to_swap, reset_sqlite_path, resolve_gap_fill_plan,
    transaction_mentions_target_programs, GapFillMissingSegment, GapFillPlan, ProgramIdConfig,
};
#[cfg(test)]
use copybot_discovery::restore_verdict::DiscoveryRuntimeRestoreVerdictKind;
use copybot_discovery::runtime_restore_ops::{
    copy_atomic, journal_snapshot_metadata_path, program_history_gap_fill_archive_path,
    program_history_gap_fill_latest_metadata_path, program_history_gap_fill_latest_path,
    prune_rotated_archives, resolve_db_path, resolve_relative_to_config, write_json_atomic,
    PROGRAM_HISTORY_GAP_FILL_ARCHIVE_PREFIX, PROGRAM_HISTORY_GAP_FILL_ARCHIVE_SUFFIX,
};
#[cfg(test)]
use copybot_discovery::DiscoveryService;
use copybot_storage::SqliteStore;
#[cfg(test)]
use copybot_storage::{
    DiscoveryPublicationStateUpdate, DiscoveryRecentRawRestoreStateUpdate, DiscoveryRuntimeCursor,
    WalletMetricRow,
};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration as StdDuration, Instant};
#[cfg(test)]
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history --config <path> [--db-path <path>] [--output <path>] [--window-start <rfc3339> --window-end <rfc3339>] [--http-url <url>] [--max-slots-to-scan <n>] [--sampling-segments <n>] [--max-blocks-to-fetch <n>] [--max-candidate-transactions-to-parse <n>] [--json] [--now <rfc3339>]";
const AVG_SLOT_MS: f64 = 400.0;

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
    http_url: Option<String>,
    max_slots_to_scan_override: Option<usize>,
    sampling_segments_override: Option<usize>,
    max_blocks_to_fetch_override: Option<usize>,
    max_candidate_transactions_to_parse_override: Option<usize>,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProgramHistoryGapFillOutput {
    db_path: String,
    output_db_path: String,
    latest_db_path: Option<String>,
    metadata_path: String,
    provider_name: String,
    source_kind: String,
    rpc_methods: Vec<String>,
    target_programs: Vec<String>,
    target_program_ids: Vec<String>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    required_window_start: Option<DateTime<Utc>>,
    derived_from_restore_state: bool,
    journal_available: bool,
    journal_covers_artifact_cursor: bool,
    journal_covered_since: Option<DateTime<Utc>>,
    journal_covered_through_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    resolved_start_slot: Option<u64>,
    resolved_end_slot: Option<u64>,
    slot_span: Option<u64>,
    coverage_method: String,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    phase_b_like_cost_budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    block_batch_size: usize,
    max_blocks_to_fetch: usize,
    max_candidate_transactions_to_parse: usize,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
    scanned_batches: usize,
    scanned_slots: usize,
    listed_block_slots: usize,
    scanned_blocks: usize,
    scanned_transactions: usize,
    candidate_program_transactions: usize,
    parsed_candidate_transactions: usize,
    parsed_candidate_swaps: usize,
    fetched_rows: usize,
    inserted_rows: usize,
    rows_withheld_due_to_incomplete_outcome: usize,
    gap_fill_covered_since: Option<DateTime<Utc>>,
    gap_fill_covered_through_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    final_covered_since: Option<DateTime<Utc>>,
    final_covered_through_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    source_lag_seconds: Option<u64>,
    missing_segments: Vec<GapFillMissingSegment>,
    replayable_output: bool,
    sufficient_for_healthy_restore: bool,
    verdict: String,
    reason: String,
    early_stop_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GapFillVerdict {
    CompleteSufficientForHealthyRestore,
    CompleteButInsufficientForHealthyRestore,
    NotProvenDueToScanBudget,
    NotProvenDueToCostBudget,
    NotProvenDueToProviderThrottling,
    NonViableSourceContract,
}

impl GapFillVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::CompleteSufficientForHealthyRestore => "complete_sufficient_for_healthy_restore",
            Self::CompleteButInsufficientForHealthyRestore => {
                "complete_but_insufficient_for_healthy_restore"
            }
            Self::NotProvenDueToScanBudget => "not_proven_due_to_scan_budget",
            Self::NotProvenDueToCostBudget => "not_proven_due_to_cost_budget",
            Self::NotProvenDueToProviderThrottling => "not_proven_due_to_provider_throttling",
            Self::NonViableSourceContract => "non_viable_source_contract",
        }
    }

    fn replayable_output(self) -> bool {
        matches!(
            self,
            Self::CompleteSufficientForHealthyRestore
                | Self::CompleteButInsufficientForHealthyRestore
        )
    }
}

#[derive(Debug, Clone)]
struct ResolvedSlotBounds {
    start_slot: u64,
    end_slot: u64,
}

#[derive(Debug, Clone)]
struct SlotScanWindow {
    start_slot: u64,
    end_slot: u64,
}

#[derive(Debug, Clone)]
struct ScanPlan {
    coverage_method: &'static str,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    windows: Vec<SlotScanWindow>,
}

#[derive(Debug, Clone, Copy)]
struct ScanSettings {
    block_batch_size: usize,
    block_time_probe_slots: u64,
    max_slots_to_scan: usize,
    sampling_segments: usize,
    max_blocks_to_fetch: usize,
    max_candidate_transactions_to_parse: usize,
}

#[derive(Debug, Clone)]
struct FetchResult {
    resolved_start_slot: Option<u64>,
    resolved_end_slot: Option<u64>,
    slot_span: Option<u64>,
    coverage_method: String,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    phase_b_like_cost_budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    block_batch_size: usize,
    max_blocks_to_fetch: usize,
    max_candidate_transactions_to_parse: usize,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
    scanned_batches: usize,
    scanned_slots: usize,
    listed_block_slots: usize,
    scanned_blocks: usize,
    scanned_transactions: usize,
    candidate_program_transactions: usize,
    parsed_candidate_transactions: usize,
    parsed_candidate_swaps: usize,
    swaps: Vec<SwapEvent>,
    verdict: GapFillVerdict,
    reason: String,
    early_stop_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ScanSummary {
    scanned_batches: usize,
    scanned_slots: usize,
    listed_block_slots: usize,
    scanned_blocks: usize,
    scanned_transactions: usize,
    candidate_program_transactions: usize,
    parsed_candidate_transactions: usize,
    parsed_candidate_swaps: usize,
    phase_b_like_cost_budget_exhausted: bool,
    early_stop_reason: Option<String>,
    swaps_by_signature: HashMap<String, SwapEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceErrorKind {
    ProviderThrottled,
    SourceContractFailure,
}

#[derive(Debug)]
struct SourceError {
    kind: SourceErrorKind,
    message: String,
}

impl SourceError {
    fn provider_throttled(message: impl Into<String>) -> Self {
        Self {
            kind: SourceErrorKind::ProviderThrottled,
            message: message.into(),
        }
    }

    fn source_contract_failure(message: impl Into<String>) -> Self {
        Self {
            kind: SourceErrorKind::SourceContractFailure,
            message: message.into(),
        }
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl StdError for SourceError {}

#[derive(Debug, Clone)]
struct QuickNodeRequestPolicy {
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
}

#[derive(Debug)]
struct RequestPacer {
    min_interval: StdDuration,
    next_allowed_at: Option<Instant>,
}

impl RequestPacer {
    fn new(max_requests_per_second: usize) -> Self {
        Self {
            min_interval: StdDuration::from_secs_f64(1.0 / max_requests_per_second.max(1) as f64),
            next_allowed_at: None,
        }
    }

    fn claim_delay(&mut self, now: Instant) -> StdDuration {
        match self.next_allowed_at {
            Some(next_allowed_at) if next_allowed_at > now => {
                let delay = next_allowed_at.duration_since(now);
                self.next_allowed_at = Some(next_allowed_at + self.min_interval);
                delay
            }
            _ => {
                self.next_allowed_at = Some(now + self.min_interval);
                StdDuration::ZERO
            }
        }
    }
}

#[derive(Debug)]
struct HttpRpcResponse {
    status_code: u16,
    body: String,
}

trait ProgramHistorySource {
    fn provider_name(&self) -> &'static str;
    fn source_kind(&self) -> &'static str;
    fn rpc_methods(&self) -> Vec<String>;
    fn latest_finalized_slot(&self) -> Result<u64>;
    fn block_time(&self, slot: u64) -> Result<Option<DateTime<Utc>>>;
    fn list_blocks(&self, start_slot: u64, end_slot: u64) -> Result<Vec<u64>>;
    fn get_block(&self, slot: u64) -> Result<Option<Value>>;
}

struct QuickNodeBlocksRpcSource {
    client: Client,
    http_url: String,
    request_policy: QuickNodeRequestPolicy,
    request_pacer: Mutex<RequestPacer>,
    block_time_cache: Mutex<HashMap<u64, Option<DateTime<Utc>>>>,
}

impl QuickNodeBlocksRpcSource {
    fn new(
        http_url: String,
        request_timeout_ms: u64,
        max_requests_per_second: usize,
        retry_429_max_attempts: usize,
        retry_429_backoff_ms: u64,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(StdDuration::from_millis(request_timeout_ms.max(1)))
            .build()
            .context("failed building program-history gap-fill http client")?;
        Ok(Self {
            client,
            http_url,
            request_policy: QuickNodeRequestPolicy {
                max_requests_per_second,
                retry_429_max_attempts,
                retry_429_backoff_ms,
            },
            request_pacer: Mutex::new(RequestPacer::new(max_requests_per_second)),
            block_time_cache: Mutex::new(HashMap::new()),
        })
    }
}

impl ProgramHistorySource for QuickNodeBlocksRpcSource {
    fn provider_name(&self) -> &'static str {
        "quicknode"
    }

    fn source_kind(&self) -> &'static str {
        "quicknode_blocks_rpc"
    }

    fn rpc_methods(&self) -> Vec<String> {
        vec![
            "getSlot".to_string(),
            "getBlockTime".to_string(),
            "getBlocks".to_string(),
            "getBlock".to_string(),
        ]
    }

    fn latest_finalized_slot(&self) -> Result<u64> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{ "commitment": "finalized" }],
        });
        let response = post_json_rpc(self, &payload)?;
        rpc_result(&response)
            .as_u64()
            .ok_or_else(|| anyhow!("missing getSlot result"))
    }

    fn block_time(&self, slot: u64) -> Result<Option<DateTime<Utc>>> {
        if let Some(cached) = self
            .block_time_cache
            .lock()
            .expect("block_time_cache poisoned")
            .get(&slot)
            .cloned()
        {
            return Ok(cached);
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlockTime",
            "params": [slot],
        });
        let response = post_json_rpc(self, &payload)?;
        let block_time = rpc_result(&response)
            .as_i64()
            .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0));
        self.block_time_cache
            .lock()
            .expect("block_time_cache poisoned")
            .insert(slot, block_time);
        Ok(block_time)
    }

    fn list_blocks(&self, start_slot: u64, end_slot: u64) -> Result<Vec<u64>> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlocks",
            "params": [start_slot, end_slot, { "commitment": "finalized" }],
        });
        let response = post_json_rpc(self, &payload)?;
        rpc_result(&response)
            .as_array()
            .ok_or_else(|| anyhow!("missing getBlocks result array"))
            .map(|entries| entries.iter().filter_map(Value::as_u64).collect())
    }

    fn get_block(&self, slot: u64) -> Result<Option<Value>> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [
                slot,
                {
                    "encoding": "jsonParsed",
                    "transactionDetails": "full",
                    "rewards": false,
                    "commitment": "finalized",
                    "maxSupportedTransactionVersion": 0
                }
            ],
        });
        let response = post_json_rpc(self, &payload)?;
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
    let mut http_url: Option<String> = None;
    let mut max_slots_to_scan_override: Option<usize> = None;
    let mut sampling_segments_override: Option<usize> = None;
    let mut max_blocks_to_fetch_override: Option<usize> = None;
    let mut max_candidate_transactions_to_parse_override: Option<usize> = None;
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
            "--http-url" => http_url = Some(parse_string_arg("--http-url", args.next())?),
            "--max-slots-to-scan" => {
                max_slots_to_scan_override =
                    Some(parse_usize_arg("--max-slots-to-scan", args.next())?)
            }
            "--sampling-segments" => {
                sampling_segments_override =
                    Some(parse_usize_arg("--sampling-segments", args.next())?)
            }
            "--max-blocks-to-fetch" => {
                max_blocks_to_fetch_override =
                    Some(parse_usize_arg("--max-blocks-to-fetch", args.next())?)
            }
            "--max-candidate-transactions-to-parse" => {
                max_candidate_transactions_to_parse_override = Some(parse_usize_arg(
                    "--max-candidate-transactions-to-parse",
                    args.next(),
                )?)
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
        http_url,
        max_slots_to_scan_override,
        sampling_segments_override,
        max_blocks_to_fetch_override,
        max_candidate_transactions_to_parse_override,
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

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    let parsed = raw
        .parse::<usize>()
        .with_context(|| format!("invalid {flag} integer: {raw}"))?;
    if parsed == 0 {
        bail!("{flag} must be >= 1");
    }
    Ok(parsed)
}

fn run(config: Config) -> Result<String> {
    let json_output = config.json;
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let http_url = config
        .http_url
        .clone()
        .unwrap_or_else(|| loaded_config.program_history_gap_fill.http_url.clone());
    let source = QuickNodeBlocksRpcSource::new(
        http_url,
        loaded_config.program_history_gap_fill.request_timeout_ms,
        loaded_config
            .program_history_gap_fill
            .max_requests_per_second,
        loaded_config
            .program_history_gap_fill
            .retry_429_max_attempts,
        loaded_config.program_history_gap_fill.retry_429_backoff_ms,
    )?;
    let output = run_with_source(config, &loaded_config, &source)?;
    if json_output {
        serde_json::to_string_pretty(&output)
            .context("failed serializing program-history gap-fill output json")
    } else {
        Ok(render_human(&output))
    }
}

fn run_with_source<S: ProgramHistorySource>(
    config: Config,
    loaded_config: &copybot_config::AppConfig,
    source: &S,
) -> Result<ProgramHistoryGapFillOutput> {
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let runtime_store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening runtime db {}", db_path.display()))?;
    let plan = resolve_gap_fill_plan(&runtime_store, config.window_start, config.window_end)?;
    let program_ids = gap_fill_program_ids(loaded_config);
    let target_program_ids = sorted_program_ids(&program_ids);
    let settings = resolve_scan_settings(&config, loaded_config)?;

    let fetch = fetch_program_history_gap_fill(
        source,
        &program_ids,
        &plan,
        settings,
        loaded_config
            .program_history_gap_fill
            .max_requests_per_second,
        loaded_config
            .program_history_gap_fill
            .retry_429_max_attempts,
        loaded_config.program_history_gap_fill.retry_429_backoff_ms,
    );

    match fetch {
        Ok(fetch) => write_gap_fill_output(
            &config,
            &loaded_config.program_history_gap_fill.output_dir,
            loaded_config.program_history_gap_fill.output_retention,
            &db_path,
            source,
            &target_program_ids,
            &plan,
            fetch,
            config.now,
        ),
        Err(error) => {
            let (verdict, reason) =
                classify_source_error("program_history_gap_fill_failed", &error);
            write_gap_fill_output(
                &config,
                &loaded_config.program_history_gap_fill.output_dir,
                loaded_config.program_history_gap_fill.output_retention,
                &db_path,
                source,
                &target_program_ids,
                &plan,
                FetchResult {
                    resolved_start_slot: None,
                    resolved_end_slot: None,
                    slot_span: None,
                    coverage_method: "unresolved".to_string(),
                    scan_budget_slots: settings.max_slots_to_scan,
                    budget_exhausted: false,
                    phase_b_like_cost_budget_exhausted: false,
                    sampling_segments: settings.sampling_segments,
                    sampling_window_slots: 0,
                    block_batch_size: settings.block_batch_size,
                    max_blocks_to_fetch: settings.max_blocks_to_fetch,
                    max_candidate_transactions_to_parse: settings
                        .max_candidate_transactions_to_parse,
                    max_requests_per_second: loaded_config
                        .program_history_gap_fill
                        .max_requests_per_second,
                    retry_429_max_attempts: loaded_config
                        .program_history_gap_fill
                        .retry_429_max_attempts,
                    retry_429_backoff_ms: loaded_config
                        .program_history_gap_fill
                        .retry_429_backoff_ms,
                    scanned_batches: 0,
                    scanned_slots: 0,
                    listed_block_slots: 0,
                    scanned_blocks: 0,
                    scanned_transactions: 0,
                    candidate_program_transactions: 0,
                    parsed_candidate_transactions: 0,
                    parsed_candidate_swaps: 0,
                    swaps: Vec::new(),
                    verdict,
                    reason,
                    early_stop_reason: None,
                },
                config.now,
            )
        }
    }
}

fn gap_fill_program_ids(config: &copybot_config::AppConfig) -> ProgramIdConfig {
    ProgramIdConfig {
        interested_program_ids: config
            .program_history_gap_fill
            .raydium_program_ids
            .iter()
            .chain(config.program_history_gap_fill.pumpswap_program_ids.iter())
            .cloned()
            .collect(),
        raydium_program_ids: config
            .program_history_gap_fill
            .raydium_program_ids
            .iter()
            .cloned()
            .collect(),
        pumpswap_program_ids: config
            .program_history_gap_fill
            .pumpswap_program_ids
            .iter()
            .cloned()
            .collect(),
    }
}

fn sorted_program_ids(program_ids: &ProgramIdConfig) -> Vec<String> {
    let mut ids = program_ids
        .interested_program_ids
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn resolve_scan_settings(
    config: &Config,
    loaded_config: &copybot_config::AppConfig,
) -> Result<ScanSettings> {
    let settings = ScanSettings {
        block_batch_size: loaded_config.program_history_gap_fill.block_batch_size,
        block_time_probe_slots: loaded_config
            .program_history_gap_fill
            .block_time_probe_slots,
        max_slots_to_scan: config
            .max_slots_to_scan_override
            .unwrap_or(loaded_config.program_history_gap_fill.max_slots_to_scan),
        sampling_segments: config
            .sampling_segments_override
            .unwrap_or(loaded_config.program_history_gap_fill.sampling_segments),
        max_blocks_to_fetch: config
            .max_blocks_to_fetch_override
            .unwrap_or(loaded_config.program_history_gap_fill.max_blocks_to_fetch),
        max_candidate_transactions_to_parse: config
            .max_candidate_transactions_to_parse_override
            .unwrap_or(
                loaded_config
                    .program_history_gap_fill
                    .max_candidate_transactions_to_parse,
            ),
    };
    if settings.sampling_segments > settings.max_slots_to_scan {
        bail!(
            "program_history_gap_fill.sampling_segments ({}) must be <= max_slots_to_scan ({})",
            settings.sampling_segments,
            settings.max_slots_to_scan
        );
    }
    Ok(settings)
}

fn fetch_program_history_gap_fill<S: ProgramHistorySource>(
    source: &S,
    program_ids: &ProgramIdConfig,
    plan: &GapFillPlan,
    settings: ScanSettings,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
) -> Result<FetchResult> {
    let bounds = resolve_slot_bounds(
        source,
        plan.requested_window_start,
        plan.requested_window_end,
        settings.block_time_probe_slots,
    )?;
    let slot_span = bounds
        .end_slot
        .saturating_sub(bounds.start_slot)
        .saturating_add(1);
    let scan_plan = build_scan_plan(bounds.clone(), settings);
    let summary = scan_slot_windows(
        source,
        program_ids,
        plan.requested_window_start,
        plan.requested_window_end,
        &scan_plan.windows,
        settings,
    )?;
    let mut swaps = summary.swaps_by_signature.into_values().collect::<Vec<_>>();
    swaps.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    let (verdict, reason) = classify_fetch_outcome(
        plan,
        scan_plan.budget_exhausted,
        summary.phase_b_like_cost_budget_exhausted,
        summary.parsed_candidate_swaps,
    );

    Ok(FetchResult {
        resolved_start_slot: Some(bounds.start_slot),
        resolved_end_slot: Some(bounds.end_slot),
        slot_span: Some(slot_span),
        coverage_method: scan_plan.coverage_method.to_string(),
        scan_budget_slots: scan_plan.scan_budget_slots,
        budget_exhausted: scan_plan.budget_exhausted,
        phase_b_like_cost_budget_exhausted: summary.phase_b_like_cost_budget_exhausted,
        sampling_segments: scan_plan.sampling_segments,
        sampling_window_slots: scan_plan.sampling_window_slots,
        block_batch_size: settings.block_batch_size,
        max_blocks_to_fetch: settings.max_blocks_to_fetch,
        max_candidate_transactions_to_parse: settings.max_candidate_transactions_to_parse,
        max_requests_per_second,
        retry_429_max_attempts,
        retry_429_backoff_ms,
        scanned_batches: summary.scanned_batches,
        scanned_slots: summary.scanned_slots,
        listed_block_slots: summary.listed_block_slots,
        scanned_blocks: summary.scanned_blocks,
        scanned_transactions: summary.scanned_transactions,
        candidate_program_transactions: summary.candidate_program_transactions,
        parsed_candidate_transactions: summary.parsed_candidate_transactions,
        parsed_candidate_swaps: summary.parsed_candidate_swaps,
        swaps,
        verdict,
        reason,
        early_stop_reason: summary.early_stop_reason,
    })
}

fn classify_fetch_outcome(
    plan: &GapFillPlan,
    budget_exhausted: bool,
    cost_budget_exhausted: bool,
    parsed_candidate_swaps: usize,
) -> (GapFillVerdict, String) {
    if cost_budget_exhausted {
        return (
            GapFillVerdict::NotProvenDueToCostBudget,
            "program_history_gap_fill_phase_b_like_cost_budget_exhausted".to_string(),
        );
    }
    if budget_exhausted {
        return (
            GapFillVerdict::NotProvenDueToScanBudget,
            "program_history_gap_fill_slot_span_exceeds_scan_budget".to_string(),
        );
    }
    if parsed_candidate_swaps == 0 {
        return (
            GapFillVerdict::CompleteButInsufficientForHealthyRestore,
            format!(
                "program_history_gap_fill_completed_without_parseable_swaps:{}..{}",
                plan.requested_window_start.to_rfc3339(),
                plan.requested_window_end.to_rfc3339()
            ),
        );
    }
    (
        GapFillVerdict::CompleteButInsufficientForHealthyRestore,
        "program_history_gap_fill_completed".to_string(),
    )
}

fn build_scan_plan(bounds: ResolvedSlotBounds, settings: ScanSettings) -> ScanPlan {
    let scan_budget_slots = settings.max_slots_to_scan.max(1);
    let slot_span = bounds
        .end_slot
        .saturating_sub(bounds.start_slot)
        .saturating_add(1);
    if slot_span <= scan_budget_slots as u64 {
        return ScanPlan {
            coverage_method: "program_history_full_slot_scan",
            scan_budget_slots,
            budget_exhausted: false,
            sampling_segments: 0,
            sampling_window_slots: 0,
            windows: vec![SlotScanWindow {
                start_slot: bounds.start_slot,
                end_slot: bounds.end_slot,
            }],
        };
    }

    let segment_count = settings.sampling_segments.max(1).min(scan_budget_slots);
    let window_slots = (scan_budget_slots / segment_count).max(1) as u64;
    let max_start_offset = slot_span.saturating_sub(window_slots);
    let mut windows = Vec::with_capacity(segment_count);
    for index in 0..segment_count {
        let offset = if segment_count == 1 {
            0
        } else {
            max_start_offset.saturating_mul(index as u64) / (segment_count.saturating_sub(1) as u64)
        };
        let start_slot = bounds.start_slot.saturating_add(offset);
        let end_slot = bounds
            .end_slot
            .min(start_slot.saturating_add(window_slots.saturating_sub(1)));
        if windows.last().is_some_and(|window: &SlotScanWindow| {
            window.start_slot == start_slot && window.end_slot == end_slot
        }) {
            continue;
        }
        windows.push(SlotScanWindow {
            start_slot,
            end_slot,
        });
    }

    ScanPlan {
        coverage_method: "program_history_staged_slot_sampling",
        scan_budget_slots,
        budget_exhausted: true,
        sampling_segments: windows.len(),
        sampling_window_slots: window_slots as usize,
        windows,
    }
}

fn resolve_slot_bounds<S: ProgramHistorySource>(
    source: &S,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    probe_slots: u64,
) -> Result<ResolvedSlotBounds> {
    let latest_slot = source.latest_finalized_slot()?;
    let (latest_anchor_slot, latest_anchor_time) =
        nearest_slot_with_time(source, latest_slot, probe_slots)?;
    let start_slot = resolve_slot_for_timestamp(
        source,
        latest_anchor_slot,
        latest_anchor_time,
        window_start,
        probe_slots,
    )?;
    let mut end_slot = resolve_slot_for_timestamp(
        source,
        latest_anchor_slot,
        latest_anchor_time,
        window_end,
        probe_slots,
    )?;
    if end_slot < start_slot {
        end_slot = start_slot;
    }
    Ok(ResolvedSlotBounds {
        start_slot,
        end_slot,
    })
}

fn resolve_slot_for_timestamp<S: ProgramHistorySource>(
    source: &S,
    latest_anchor_slot: u64,
    latest_anchor_time: DateTime<Utc>,
    target: DateTime<Utc>,
    probe_slots: u64,
) -> Result<u64> {
    let diff_ms = latest_anchor_time
        .signed_duration_since(target)
        .num_milliseconds() as f64;
    let estimated_delta_slots = (diff_ms.abs() / AVG_SLOT_MS).round() as u64;
    let mut guess = if diff_ms >= 0.0 {
        latest_anchor_slot.saturating_sub(estimated_delta_slots)
    } else {
        latest_anchor_slot.saturating_add(estimated_delta_slots)
    };

    for _ in 0..6 {
        let (anchor_slot, anchor_time) = nearest_slot_with_time(source, guess, probe_slots)?;
        let anchor_diff_ms = anchor_time.signed_duration_since(target).num_milliseconds();
        if anchor_diff_ms.abs() <= 30_000 {
            guess = anchor_slot;
            break;
        }
        let adjust_slots = ((anchor_diff_ms.abs() as f64) / AVG_SLOT_MS)
            .round()
            .max(1.0) as u64;
        guess = if anchor_diff_ms > 0 {
            anchor_slot.saturating_sub(adjust_slots)
        } else {
            anchor_slot.saturating_add(adjust_slots)
        };
    }

    let lower = guess.saturating_sub(probe_slots);
    let upper = guess.saturating_add(probe_slots);
    let mut candidate: Option<(u64, DateTime<Utc>)> = None;
    for slot in lower..=upper {
        let Some(block_time) = source.block_time(slot)? else {
            continue;
        };
        if block_time < target {
            continue;
        }
        match candidate {
            Some((best_slot, best_time))
                if block_time > best_time
                    || (block_time == best_time && slot.cmp(&best_slot) == Ordering::Greater) => {}
            _ => candidate = Some((slot, block_time)),
        }
    }
    candidate
        .map(|(slot, _)| slot)
        .ok_or_else(|| anyhow!("unable to resolve slot for {}", target.to_rfc3339()))
}

fn nearest_slot_with_time<S: ProgramHistorySource>(
    source: &S,
    slot: u64,
    probe_slots: u64,
) -> Result<(u64, DateTime<Utc>)> {
    for offset in 0..=probe_slots {
        let lower = slot.saturating_sub(offset);
        if let Some(ts) = source.block_time(lower)? {
            return Ok((lower, ts));
        }
        let upper = slot.saturating_add(offset);
        if upper != lower {
            if let Some(ts) = source.block_time(upper)? {
                return Ok((upper, ts));
            }
        }
    }
    bail!("failed to find a slot with block time near slot={slot}");
}

fn scan_slot_windows<S: ProgramHistorySource>(
    source: &S,
    program_ids: &ProgramIdConfig,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    windows: &[SlotScanWindow],
    settings: ScanSettings,
) -> Result<ScanSummary> {
    let mut combined = ScanSummary::default();
    for window in windows {
        let summary = scan_slot_range(
            source,
            program_ids,
            requested_window_start,
            requested_window_end,
            window.start_slot,
            window.end_slot,
            settings,
        )?;
        combined.scanned_batches = combined
            .scanned_batches
            .saturating_add(summary.scanned_batches);
        combined.scanned_slots = combined.scanned_slots.saturating_add(summary.scanned_slots);
        combined.listed_block_slots = combined
            .listed_block_slots
            .saturating_add(summary.listed_block_slots);
        combined.scanned_blocks = combined
            .scanned_blocks
            .saturating_add(summary.scanned_blocks);
        combined.scanned_transactions = combined
            .scanned_transactions
            .saturating_add(summary.scanned_transactions);
        combined.candidate_program_transactions = combined
            .candidate_program_transactions
            .saturating_add(summary.candidate_program_transactions);
        combined.parsed_candidate_transactions = combined
            .parsed_candidate_transactions
            .saturating_add(summary.parsed_candidate_transactions);
        combined.parsed_candidate_swaps = combined
            .parsed_candidate_swaps
            .saturating_add(summary.parsed_candidate_swaps);
        if summary.phase_b_like_cost_budget_exhausted {
            combined.phase_b_like_cost_budget_exhausted = true;
        }
        if combined.early_stop_reason.is_none() {
            combined.early_stop_reason = summary.early_stop_reason.clone();
        }
        for (signature, swap) in summary.swaps_by_signature {
            combined.swaps_by_signature.entry(signature).or_insert(swap);
        }
        if combined.phase_b_like_cost_budget_exhausted {
            break;
        }
    }
    Ok(combined)
}

fn scan_slot_range<S: ProgramHistorySource>(
    source: &S,
    program_ids: &ProgramIdConfig,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    start_slot: u64,
    end_slot: u64,
    settings: ScanSettings,
) -> Result<ScanSummary> {
    let mut summary = ScanSummary::default();
    if end_slot < start_slot {
        return Ok(summary);
    }
    let batch_size = settings.block_batch_size.max(1) as u64;
    let mut batch_start = start_slot;
    while batch_start <= end_slot {
        let batch_end = end_slot.min(batch_start.saturating_add(batch_size.saturating_sub(1)));
        summary.scanned_batches = summary.scanned_batches.saturating_add(1);
        summary.scanned_slots = summary
            .scanned_slots
            .saturating_add(batch_end.saturating_sub(batch_start).saturating_add(1) as usize);
        let available_block_slots = source.list_blocks(batch_start, batch_end)?;
        summary.listed_block_slots = summary
            .listed_block_slots
            .saturating_add(available_block_slots.len());
        for slot in available_block_slots {
            if summary.scanned_blocks >= settings.max_blocks_to_fetch {
                summary.phase_b_like_cost_budget_exhausted = true;
                summary.early_stop_reason =
                    Some("program_history_gap_fill_block_fetch_budget_exhausted".to_string());
                return Ok(summary);
            }
            summary.scanned_blocks = summary.scanned_blocks.saturating_add(1);
            let Some(block) = source.get_block(slot)? else {
                continue;
            };
            let block_time = block
                .get("blockTime")
                .and_then(Value::as_i64)
                .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0));
            let Some(transactions) = block.get("transactions").and_then(Value::as_array) else {
                continue;
            };
            for transaction in transactions {
                summary.scanned_transactions = summary.scanned_transactions.saturating_add(1);
                let tx = with_block_context(transaction, slot, block_time);
                let tx_time = tx
                    .get("blockTime")
                    .and_then(Value::as_i64)
                    .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0));
                let Some(tx_time) = tx_time else {
                    continue;
                };
                if tx_time < requested_window_start || tx_time >= requested_window_end {
                    continue;
                }
                if !transaction_mentions_target_programs(&tx, program_ids) {
                    continue;
                }
                summary.candidate_program_transactions =
                    summary.candidate_program_transactions.saturating_add(1);
                if !candidate_can_support_swap_parse(&tx) {
                    continue;
                }
                if summary.parsed_candidate_transactions
                    >= settings.max_candidate_transactions_to_parse
                {
                    summary.phase_b_like_cost_budget_exhausted = true;
                    summary.early_stop_reason = Some(
                        "program_history_gap_fill_candidate_parse_budget_exhausted".to_string(),
                    );
                    return Ok(summary);
                }
                summary.parsed_candidate_transactions =
                    summary.parsed_candidate_transactions.saturating_add(1);
                if let Some(swap) = parse_program_scoped_transaction_to_swap(&tx, program_ids)? {
                    summary.parsed_candidate_swaps =
                        summary.parsed_candidate_swaps.saturating_add(1);
                    summary
                        .swaps_by_signature
                        .entry(swap.signature.clone())
                        .or_insert(swap);
                }
            }
        }
        batch_start = batch_end.saturating_add(1);
        if batch_start == 0 {
            break;
        }
    }
    Ok(summary)
}

fn candidate_can_support_swap_parse(transaction: &Value) -> bool {
    let Some(meta) = transaction.get("meta").filter(|value| !value.is_null()) else {
        return false;
    };
    if meta
        .get("err")
        .map(|value| !value.is_null())
        .unwrap_or(false)
    {
        return false;
    }
    let pre_token_balances = meta
        .get("preTokenBalances")
        .and_then(Value::as_array)
        .map(|entries| !entries.is_empty())
        .unwrap_or(false);
    let post_token_balances = meta
        .get("postTokenBalances")
        .and_then(Value::as_array)
        .map(|entries| !entries.is_empty())
        .unwrap_or(false);
    pre_token_balances || post_token_balances
}

fn with_block_context(transaction: &Value, slot: u64, block_time: Option<DateTime<Utc>>) -> Value {
    let mut tx = transaction.clone();
    if tx.get("slot").is_none() {
        tx["slot"] = Value::from(slot);
    }
    if tx.get("blockTime").is_none() {
        tx["blockTime"] = block_time
            .map(|value| Value::from(value.timestamp()))
            .unwrap_or(Value::Null);
    }
    tx
}

fn write_gap_fill_output<S: ProgramHistorySource>(
    config: &Config,
    configured_output_dir: &str,
    output_retention: usize,
    db_path: &Path,
    source: &S,
    target_program_ids: &[String],
    plan: &GapFillPlan,
    fetch: FetchResult,
    now: DateTime<Utc>,
) -> Result<ProgramHistoryGapFillOutput> {
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
            program_history_gap_fill_archive_path(&dir, now),
            Some(program_history_gap_fill_latest_path(&dir)),
            program_history_gap_fill_latest_metadata_path(&dir),
            Some(dir),
        )
    };

    reset_sqlite_path(&output_db_path)?;
    let output_store = SqliteStore::open(&output_db_path).with_context(|| {
        format!(
            "failed opening program-history gap-fill output db {}",
            output_db_path.display()
        )
    })?;
    output_store.ensure_recent_raw_journal_tables()?;

    let replayable_output = fetch.verdict.replayable_output();
    let inserted_rows = if replayable_output {
        output_store
            .insert_recent_raw_journal_batch(&fetch.swaps, now)?
            .inserted_rows
    } else {
        0
    };
    let journal_state = output_store.recent_raw_journal_state_read_only()?;
    let mut missing_segments = compute_missing_segments(
        plan.requested_window_start,
        plan.requested_window_end,
        &journal_state,
    );
    if !replayable_output {
        missing_segments = vec![GapFillMissingSegment {
            start: plan.requested_window_start,
            end: plan.requested_window_end,
            reason: fetch.reason.clone(),
        }];
    }
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
    let sufficient_for_healthy_restore = replayable_output
        && plan.journal_available
        && plan.journal_covers_artifact_cursor
        && final_covered_since.is_some_and(|covered_since| {
            plan.required_window_start
                .is_some_and(|required| covered_since <= required)
        });
    let verdict = if sufficient_for_healthy_restore {
        GapFillVerdict::CompleteSufficientForHealthyRestore
    } else {
        fetch.verdict
    };

    let output = ProgramHistoryGapFillOutput {
        db_path: db_path.display().to_string(),
        output_db_path: output_db_path.display().to_string(),
        latest_db_path: latest_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        metadata_path: metadata_path.display().to_string(),
        provider_name: source.provider_name().to_string(),
        source_kind: source.source_kind().to_string(),
        rpc_methods: source.rpc_methods(),
        target_programs: vec!["raydium".to_string(), "pumpswap".to_string()],
        target_program_ids: target_program_ids.to_vec(),
        requested_window_start: plan.requested_window_start,
        requested_window_end: plan.requested_window_end,
        required_window_start: plan.required_window_start,
        derived_from_restore_state: plan.derived_from_restore_state,
        journal_available: plan.journal_available,
        journal_covers_artifact_cursor: plan.journal_covers_artifact_cursor,
        journal_covered_since: plan.journal_covered_since,
        journal_covered_through_cursor: plan.journal_covered_through_cursor.clone(),
        resolved_start_slot: fetch.resolved_start_slot,
        resolved_end_slot: fetch.resolved_end_slot,
        slot_span: fetch.slot_span,
        coverage_method: fetch.coverage_method,
        scan_budget_slots: fetch.scan_budget_slots,
        budget_exhausted: fetch.budget_exhausted,
        phase_b_like_cost_budget_exhausted: fetch.phase_b_like_cost_budget_exhausted,
        sampling_segments: fetch.sampling_segments,
        sampling_window_slots: fetch.sampling_window_slots,
        block_batch_size: fetch.block_batch_size,
        max_blocks_to_fetch: fetch.max_blocks_to_fetch,
        max_candidate_transactions_to_parse: fetch.max_candidate_transactions_to_parse,
        max_requests_per_second: fetch.max_requests_per_second,
        retry_429_max_attempts: fetch.retry_429_max_attempts,
        retry_429_backoff_ms: fetch.retry_429_backoff_ms,
        scanned_batches: fetch.scanned_batches,
        scanned_slots: fetch.scanned_slots,
        listed_block_slots: fetch.listed_block_slots,
        scanned_blocks: fetch.scanned_blocks,
        scanned_transactions: fetch.scanned_transactions,
        candidate_program_transactions: fetch.candidate_program_transactions,
        parsed_candidate_transactions: fetch.parsed_candidate_transactions,
        parsed_candidate_swaps: fetch.parsed_candidate_swaps,
        fetched_rows: fetch.swaps.len(),
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome: if replayable_output {
            0
        } else {
            fetch.swaps.len()
        },
        gap_fill_covered_since: journal_state.covered_since,
        gap_fill_covered_through_cursor: journal_state.covered_through_cursor.clone(),
        final_covered_since,
        final_covered_through_cursor,
        source_lag_seconds,
        missing_segments,
        replayable_output,
        sufficient_for_healthy_restore,
        verdict: verdict.as_str().to_string(),
        reason: if sufficient_for_healthy_restore {
            "program_history_gap_fill_completed_and_closes_required_raw_window".to_string()
        } else {
            fetch.reason
        },
        early_stop_reason: fetch.early_stop_reason,
    };

    if let Some(dir) = output_dir.as_ref() {
        let latest_path = latest_db_path.as_ref().expect("latest path");
        copy_atomic(&output_db_path, latest_path)?;
        let archive_metadata_path = journal_snapshot_metadata_path(&output_db_path);
        write_json_atomic(&archive_metadata_path, &output)?;
        write_json_atomic(&metadata_path, &output)?;
        let pruned = prune_rotated_archives(
            dir,
            PROGRAM_HISTORY_GAP_FILL_ARCHIVE_PREFIX,
            PROGRAM_HISTORY_GAP_FILL_ARCHIVE_SUFFIX,
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

fn classify_source_error(context: &str, error: &anyhow::Error) -> (GapFillVerdict, String) {
    match source_error_kind(error) {
        Some(SourceErrorKind::ProviderThrottled) => (
            GapFillVerdict::NotProvenDueToProviderThrottling,
            format!("{context}:{error}"),
        ),
        _ => (
            GapFillVerdict::NonViableSourceContract,
            format!("{context}:{error}"),
        ),
    }
}

fn source_error_kind(error: &anyhow::Error) -> Option<SourceErrorKind> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<SourceError>()
            .map(|source| source.kind)
    })
}

fn render_human(output: &ProgramHistoryGapFillOutput) -> String {
    [
        "event=discovery_raw_gap_fill_program_history".to_string(),
        format!("db_path={}", output.db_path),
        format!("output_db_path={}", output.output_db_path),
        format!(
            "latest_db_path={}",
            output.latest_db_path.as_deref().unwrap_or("null")
        ),
        format!("metadata_path={}", output.metadata_path),
        format!("provider_name={}", output.provider_name),
        format!("source_kind={}", output.source_kind),
        format!(
            "requested_window_start={}",
            output.requested_window_start.to_rfc3339()
        ),
        format!(
            "requested_window_end={}",
            output.requested_window_end.to_rfc3339()
        ),
        format!(
            "resolved_start_slot={}",
            output
                .resolved_start_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "resolved_end_slot={}",
            output
                .resolved_end_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "slot_span={}",
            output
                .slot_span
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("coverage_method={}", output.coverage_method),
        format!("scan_budget_slots={}", output.scan_budget_slots),
        format!("budget_exhausted={}", output.budget_exhausted),
        format!(
            "phase_b_like_cost_budget_exhausted={}",
            output.phase_b_like_cost_budget_exhausted
        ),
        format!("sampling_segments={}", output.sampling_segments),
        format!("sampling_window_slots={}", output.sampling_window_slots),
        format!("block_batch_size={}", output.block_batch_size),
        format!("max_blocks_to_fetch={}", output.max_blocks_to_fetch),
        format!(
            "max_candidate_transactions_to_parse={}",
            output.max_candidate_transactions_to_parse
        ),
        format!("max_requests_per_second={}", output.max_requests_per_second),
        format!("retry_429_max_attempts={}", output.retry_429_max_attempts),
        format!("retry_429_backoff_ms={}", output.retry_429_backoff_ms),
        format!("scanned_batches={}", output.scanned_batches),
        format!("scanned_slots={}", output.scanned_slots),
        format!("listed_block_slots={}", output.listed_block_slots),
        format!("scanned_blocks={}", output.scanned_blocks),
        format!("scanned_transactions={}", output.scanned_transactions),
        format!(
            "candidate_program_transactions={}",
            output.candidate_program_transactions
        ),
        format!(
            "parsed_candidate_transactions={}",
            output.parsed_candidate_transactions
        ),
        format!("parsed_candidate_swaps={}", output.parsed_candidate_swaps),
        format!("fetched_rows={}", output.fetched_rows),
        format!("inserted_rows={}", output.inserted_rows),
        format!(
            "rows_withheld_due_to_incomplete_outcome={}",
            output.rows_withheld_due_to_incomplete_outcome
        ),
        format!(
            "gap_fill_covered_since={}",
            format_optional_ts(output.gap_fill_covered_since.as_ref())
        ),
        format!(
            "gap_fill_covered_through_cursor={}",
            format_optional_cursor(output.gap_fill_covered_through_cursor.as_ref())
        ),
        format!(
            "final_covered_since={}",
            format_optional_ts(output.final_covered_since.as_ref())
        ),
        format!(
            "final_covered_through_cursor={}",
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
        format!("replayable_output={}", output.replayable_output),
        format!(
            "sufficient_for_healthy_restore={}",
            output.sufficient_for_healthy_restore
        ),
        format!(
            "early_stop_reason={}",
            output
                .early_stop_reason
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("verdict={}", output.verdict),
        format!("reason={}", output.reason),
    ]
    .join("\n")
}

fn throttle_backoff_for_attempt(base_backoff_ms: u64, attempt_index: usize) -> StdDuration {
    StdDuration::from_millis(base_backoff_ms.saturating_mul(attempt_index as u64))
}

fn throttle_response_message(status_code: u16, body: &str) -> Option<String> {
    if status_code == 429 {
        return Some(format!(
            "rpc returned http status 429 Too Many Requests: {body}"
        ));
    }
    let lowered = body.to_ascii_lowercase();
    if lowered.contains("too many requests") || lowered.contains("rate limit") {
        return Some(format!("rpc provider throttled request: {body}"));
    }
    None
}

fn maybe_sleep_with_pacer<F>(pacer: &Mutex<RequestPacer>, mut sleep_fn: F)
where
    F: FnMut(StdDuration),
{
    let delay = {
        pacer
            .lock()
            .expect("request_pacer poisoned")
            .claim_delay(Instant::now())
    };
    if !delay.is_zero() {
        sleep_fn(delay);
    }
}

fn execute_json_rpc_with_policy<F, G>(
    pacer: &Mutex<RequestPacer>,
    request_policy: &QuickNodeRequestPolicy,
    mut send: F,
    mut sleep_fn: G,
) -> Result<Value>
where
    F: FnMut() -> Result<HttpRpcResponse>,
    G: FnMut(StdDuration),
{
    for attempt in 0..=request_policy.retry_429_max_attempts {
        maybe_sleep_with_pacer(pacer, |duration| sleep_fn(duration));
        let response = send()?;
        if let Some(message) = throttle_response_message(response.status_code, &response.body) {
            if attempt < request_policy.retry_429_max_attempts {
                sleep_fn(throttle_backoff_for_attempt(
                    request_policy.retry_429_backoff_ms,
                    attempt + 1,
                ));
                continue;
            }
            return Err(SourceError::provider_throttled(format!(
                "{message}; max_requests_per_second={} retry_429_max_attempts={} retry_429_backoff_ms={}",
                request_policy.max_requests_per_second,
                request_policy.retry_429_max_attempts,
                request_policy.retry_429_backoff_ms
            ))
            .into());
        }

        if response.status_code < 200 || response.status_code >= 300 {
            return Err(SourceError::source_contract_failure(format!(
                "rpc returned http status {}: {}",
                response.status_code, response.body
            ))
            .into());
        }

        let parsed: Value = serde_json::from_str(&response.body).map_err(|error| {
            anyhow!(SourceError::source_contract_failure(format!(
                "failed parsing rpc response json: {error}"
            )))
        })?;
        if let Some(error) = parsed.get("error") {
            let error_message = error.to_string();
            if let Some(message) = throttle_response_message(response.status_code, &error_message) {
                if attempt < request_policy.retry_429_max_attempts {
                    sleep_fn(throttle_backoff_for_attempt(
                        request_policy.retry_429_backoff_ms,
                        attempt + 1,
                    ));
                    continue;
                }
                return Err(SourceError::provider_throttled(message).into());
            }
            return Err(SourceError::source_contract_failure(format!(
                "rpc returned error: {error}"
            ))
            .into());
        }
        return Ok(parsed);
    }

    Err(
        SourceError::provider_throttled("rpc provider throttled request and retries exhausted")
            .into(),
    )
}

fn post_json_rpc(source: &QuickNodeBlocksRpcSource, payload: &Value) -> Result<Value> {
    execute_json_rpc_with_policy(
        &source.request_pacer,
        &source.request_policy,
        || {
            let response = source
                .client
                .post(&source.http_url)
                .json(payload)
                .send()
                .map_err(|error| {
                    anyhow!(SourceError::source_contract_failure(format!(
                        "failed rpc request to {}: {error}",
                        source.http_url
                    )))
                })?;
            let status_code = response.status().as_u16();
            let body = response.text().map_err(|error| {
                anyhow!(SourceError::source_contract_failure(format!(
                    "failed reading rpc response body: {error}"
                )))
            })?;
            Ok(HttpRpcResponse { status_code, body })
        },
        sleep,
    )
}

fn rpc_result(response: &Value) -> &Value {
    response.get("result").unwrap_or(&Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct BlockRangeRequest {
        start_slot: u64,
        end_slot: u64,
    }

    #[derive(Default)]
    struct FakeProgramHistorySource {
        latest_slot: u64,
        block_times: HashMap<u64, DateTime<Utc>>,
        block_ranges: HashMap<(u64, u64), Vec<u64>>,
        blocks: HashMap<u64, Value>,
        list_requests: Mutex<Vec<BlockRangeRequest>>,
    }

    impl FakeProgramHistorySource {
        fn with_latest_slot(mut self, latest_slot: u64) -> Self {
            self.latest_slot = latest_slot;
            self
        }

        fn with_block_time(mut self, slot: u64, ts: DateTime<Utc>) -> Self {
            self.block_times.insert(slot, ts);
            self
        }

        fn with_block_range(mut self, start_slot: u64, end_slot: u64, slots: Vec<u64>) -> Self {
            self.block_ranges.insert((start_slot, end_slot), slots);
            self
        }

        fn with_block(mut self, slot: u64, block: Value) -> Self {
            self.blocks.insert(slot, block);
            self
        }

        fn list_requests(&self) -> Vec<BlockRangeRequest> {
            self.list_requests.lock().unwrap().clone()
        }
    }

    impl ProgramHistorySource for FakeProgramHistorySource {
        fn provider_name(&self) -> &'static str {
            "quicknode"
        }

        fn source_kind(&self) -> &'static str {
            "quicknode_blocks_rpc"
        }

        fn rpc_methods(&self) -> Vec<String> {
            vec![
                "getSlot".to_string(),
                "getBlockTime".to_string(),
                "getBlocks".to_string(),
                "getBlock".to_string(),
            ]
        }

        fn latest_finalized_slot(&self) -> Result<u64> {
            Ok(self.latest_slot)
        }

        fn block_time(&self, slot: u64) -> Result<Option<DateTime<Utc>>> {
            Ok(self.block_times.get(&slot).copied())
        }

        fn list_blocks(&self, start_slot: u64, end_slot: u64) -> Result<Vec<u64>> {
            self.list_requests.lock().unwrap().push(BlockRangeRequest {
                start_slot,
                end_slot,
            });
            Ok(self
                .block_ranges
                .get(&(start_slot, end_slot))
                .cloned()
                .unwrap_or_else(|| (start_slot..=end_slot).collect()))
        }

        fn get_block(&self, slot: u64) -> Result<Option<Value>> {
            Ok(self.blocks.get(&slot).cloned())
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
    fn derives_exact_bounded_missing_window_from_restore_state() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-derive-window")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let source = dense_source(now);

        let output = run_output_with_source(gap_fill_config(&fixture, None, None), &source)?;
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
    fn does_not_fetch_outside_requested_horizon() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-horizon")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::days(7);
        let window_end = now - Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start)
            .with_block_time(101, window_start + Duration::hours(1))
            .with_block_time(102, window_end)
            .with_block_range(100, 102, vec![100, 101, 102])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx("inside-a", "wallet-gap", 100, "raydium-program")],
                ),
            )
            .with_block(
                101,
                block_with_transactions(
                    101,
                    window_start + Duration::hours(1),
                    vec![swap_tx("inside-b", "wallet-gap", 101, "pumpswap-program")],
                ),
            )
            .with_block(
                102,
                block_with_transactions(
                    102,
                    window_end,
                    vec![swap_tx("too-new", "wallet-gap", 102, "raydium-program")],
                ),
            );

        let output = run_output_with_source(
            gap_fill_config(&fixture, Some(window_start), Some(window_end)),
            &source,
        )?;
        assert_eq!(output.candidate_program_transactions, 2);
        assert_eq!(output.parsed_candidate_transactions, 2);
        assert_eq!(output.parsed_candidate_swaps, 2);
        assert_eq!(output.inserted_rows, 2);
        let requests = source.list_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].start_slot, 100);
        assert_eq!(requests[0].end_slot, 102);
        Ok(())
    }

    #[test]
    fn writes_standalone_output_sqlite_without_touching_runtime_db() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-standalone-output")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let journal_covered_since = now - Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            journal_covered_since,
        )?;
        let output_path = fixture.temp.path().join("program-gap-fill-output.sqlite");
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, now - Duration::days(7))
            .with_block_time(101, journal_covered_since - Duration::minutes(1))
            .with_block_time(102, journal_covered_since)
            .with_block_range(100, 102, vec![100, 101])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    now - Duration::days(7),
                    vec![swap_tx("inside-a", "wallet-gap", 100, "raydium-program")],
                ),
            )
            .with_block(
                101,
                block_with_transactions(
                    101,
                    journal_covered_since - Duration::minutes(1),
                    vec![swap_tx("inside-b", "wallet-gap", 101, "pumpswap-program")],
                ),
            );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path.clone()),
                now,
                ..gap_fill_config(&fixture, None, None)
            },
            &source,
        )?;

        assert_eq!(output.output_db_path, output_path.display().to_string());
        assert_eq!(output.latest_db_path, None);
        assert!(Path::new(&output.metadata_path).exists());
        let runtime_store = SqliteStore::open_read_only(&fixture.runtime_db_path)?;
        assert!(runtime_store
            .load_observed_swaps_since(now - Duration::days(8))?
            .is_empty());
        let output_store = SqliteStore::open_read_only(&output_path)?;
        assert_eq!(
            output_store
                .load_observed_swaps_since(now - Duration::days(8))?
                .len(),
            2
        );
        Ok(())
    }

    #[test]
    fn output_is_replayable_into_runtime_store() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-replay")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::days(7);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - Duration::days(2),
        )?;
        let output_path = fixture.temp.path().join("program-gap-fill-replay.sqlite");
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start)
            .with_block_time(101, now - Duration::days(2))
            .with_block_range(100, 101, vec![100])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx("inside-a", "wallet-gap", 100, "raydium-program")],
                ),
            );

        let _ = run_output_with_source(
            Config {
                output_path: Some(output_path.clone()),
                now,
                ..gap_fill_config(&fixture, None, None)
            },
            &source,
        )?;

        let replay_target_path = fixture
            .temp
            .path()
            .join("program-gap-fill-replay-target.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut replay_target = SqliteStore::open(&replay_target_path)?;
        replay_target.run_migrations(&migration_dir)?;
        let gap_fill_store = SqliteStore::open_read_only(&output_path)?;
        let replay = gap_fill_store.replay_recent_raw_journal_into_runtime_store(
            &replay_target,
            window_start,
            &artifact_cursor(now),
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
    fn valid_program_gap_fill_plus_recent_journal_can_become_trading_ready() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-healthy")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let journal_covered_since = now - Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            journal_covered_since,
        )?;
        let output_path = fixture.temp.path().join("program-gap-fill-healthy.sqlite");
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(103)
            .with_block_time(100, now - Duration::days(7))
            .with_block_time(101, now - Duration::days(6))
            .with_block_time(102, journal_covered_since - Duration::minutes(1))
            .with_block_time(103, journal_covered_since)
            .with_block_range(100, 103, vec![100, 101, 102])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    now - Duration::days(7),
                    vec![swap_tx("inside-a", "wallet-gap", 100, "raydium-program")],
                ),
            )
            .with_block(
                101,
                block_with_transactions(
                    101,
                    now - Duration::days(6),
                    vec![swap_tx("inside-b", "wallet-gap", 101, "pumpswap-program")],
                ),
            )
            .with_block(
                102,
                block_with_transactions(
                    102,
                    journal_covered_since - Duration::minutes(1),
                    vec![swap_tx("inside-c", "wallet-gap", 102, "raydium-program")],
                ),
            );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path.clone()),
                now,
                ..gap_fill_config(&fixture, None, None)
            },
            &source,
        )?;
        assert!(output.replayable_output);
        assert!(output.sufficient_for_healthy_restore);

        let loaded_config = load_from_path(&fixture.config_path)?;
        apply_gap_fill_replay_to_runtime(
            &fixture.runtime_db_path,
            &output_path,
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
    fn partial_or_budget_limited_program_gap_fill_stays_non_trading_ready() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-partial")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let journal_covered_since = now - Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            journal_covered_since,
        )?;
        let output_path = fixture.temp.path().join("program-gap-fill-partial.sqlite");
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(40_200)
            .with_block_time(100, now - Duration::days(7))
            .with_block_time(40_199, journal_covered_since)
            .with_block_range(100, 100, vec![100])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    now - Duration::days(7),
                    vec![swap_tx("inside-a", "wallet-gap", 100, "raydium-program")],
                ),
            );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path.clone()),
                max_slots_to_scan_override: Some(8),
                sampling_segments_override: Some(2),
                now,
                ..gap_fill_config(&fixture, None, None)
            },
            &source,
        )?;
        assert_eq!(output.verdict, "not_proven_due_to_scan_budget");
        assert!(!output.replayable_output);
        assert_eq!(output.inserted_rows, 0);

        let loaded_config = load_from_path(&fixture.config_path)?;
        apply_gap_fill_replay_to_runtime(
            &fixture.runtime_db_path,
            &output_path,
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

    fn gap_fill_config(
        fixture: &Fixture,
        window_start: Option<DateTime<Utc>>,
        window_end: Option<DateTime<Utc>>,
    ) -> Config {
        Config {
            config_path: fixture.config_path.clone(),
            db_path: Some(fixture.runtime_db_path.clone()),
            output_path: None,
            window_start,
            window_end,
            http_url: Some("https://quicknode.example/?api-key=test".to_string()),
            max_slots_to_scan_override: None,
            sampling_segments_override: None,
            max_blocks_to_fetch_override: None,
            max_candidate_transactions_to_parse_override: None,
            json: true,
            now: parse_ts("2026-03-24T13:43:40Z").expect("static ts"),
        }
    }

    fn run_output_with_source<S: ProgramHistorySource>(
        config: Config,
        source: &S,
    ) -> Result<ProgramHistoryGapFillOutput> {
        let loaded_config = load_from_path(&config.config_path)?;
        run_with_source(config, &loaded_config, source)
    }

    fn dense_source(now: DateTime<Utc>) -> FakeProgramHistorySource {
        let start = now - Duration::days(7);
        let end = now - Duration::days(2);
        FakeProgramHistorySource::default()
            .with_latest_slot(1_050_000)
            .with_block_time(1_000_000, start)
            .with_block_time(1_000_001, start + Duration::minutes(1))
            .with_block_time(1_050_000, end)
            .with_block_range(1_000_000, 1_000_001, vec![1_000_000, 1_000_001])
            .with_block(
                1_000_000,
                block_with_transactions(
                    1_000_000,
                    start,
                    vec![swap_tx("sig-a", "wallet-gap", 1_000_000, "raydium-program")],
                ),
            )
            .with_block(
                1_000_001,
                block_with_transactions(
                    1_000_001,
                    start + Duration::minutes(1),
                    vec![swap_tx(
                        "sig-b",
                        "wallet-gap",
                        1_000_001,
                        "pumpswap-program",
                    )],
                ),
            )
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir()?;
        let runtime_db_path = temp.path().join(format!("{name}-runtime.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        fs::write(
            &config_path,
            format!(
                "[system]\nmigrations_dir = \"{}\"\n\n[sqlite]\npath = \"{}\"\n\n[program_history_gap_fill]\noutput_retention = 2\nraydium_program_ids = [\"raydium-program\"]\npumpswap_program_ids = [\"pumpswap-program\"]\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[execution]\nenabled = false\n",
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
        let source_db_path = runtime_db_path.with_file_name("source-artifact-program-history.db");
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

    fn block_with_transactions(slot: u64, ts: DateTime<Utc>, txs: Vec<Value>) -> Value {
        json!({
            "blockTime": ts.timestamp(),
            "transactions": txs,
            "parentSlot": slot.saturating_sub(1),
        })
    }

    fn swap_tx(signature: &str, wallet: &str, slot: u64, program_id: &str) -> Value {
        json!({
            "slot": slot,
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
                "logMessages": [format!("Program {program_id} invoke [1]")]
            }
        })
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }
}
