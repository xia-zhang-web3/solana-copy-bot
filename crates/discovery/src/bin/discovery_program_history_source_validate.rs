use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::raw_gap_fill_support::{
    parse_program_scoped_transaction_to_swap, resolve_gap_fill_plan,
    transaction_mentions_target_programs, GapFillMissingSegment, ProgramIdConfig,
};
use copybot_discovery::runtime_restore_ops::{resolve_db_path, write_json_atomic};
use copybot_storage::SqliteStore;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;
use std::error::Error as StdError;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration as StdDuration, Instant};
#[cfg(test)]
use tempfile::tempdir;

const USAGE: &str = "usage: discovery_program_history_source_validate --config <path> [--db-path <path>] [--window-start <rfc3339> --window-end <rfc3339>] [--http-url <url>] [--max-slots-to-scan <n>] [--sampling-segments <n>] [--report-path <path>] [--json]";
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
    window_start: Option<DateTime<Utc>>,
    window_end: Option<DateTime<Utc>>,
    http_url: Option<String>,
    max_slots_to_scan_override: Option<usize>,
    sampling_segments_override: Option<usize>,
    report_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationReport {
    provider_name: String,
    source_kind: String,
    rpc_methods: Vec<String>,
    target_programs: Vec<String>,
    target_program_ids: Vec<String>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    required_window_start: Option<DateTime<Utc>>,
    derived_from_restore_state: bool,
    journal_covered_since: Option<DateTime<Utc>>,
    resolved_start_slot: Option<u64>,
    resolved_end_slot: Option<u64>,
    slot_span: Option<u64>,
    coverage_method: String,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
    scanned_batches: usize,
    scanned_slots: usize,
    scanned_blocks: usize,
    scanned_transactions: usize,
    candidate_program_transactions: usize,
    parsed_candidate_swap_rows: usize,
    earliest_seen: Option<DateTime<Utc>>,
    latest_seen: Option<DateTime<Utc>>,
    missing_segments: Vec<GapFillMissingSegment>,
    sufficient_for_next_step: bool,
    verdict: String,
    reason: String,
    report_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationVerdict {
    Viable,
    NotProvenDueToBudget,
    NotProvenDueToProviderThrottling,
    NotProvenDueToSparseProgramHistory,
    NonViableSourceContract,
}

impl ValidationVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Viable => "viable",
            Self::NotProvenDueToBudget => "not_proven_due_to_budget",
            Self::NotProvenDueToProviderThrottling => "not_proven_due_to_provider_throttling",
            Self::NotProvenDueToSparseProgramHistory => "not_proven_due_to_sparse_program_history",
            Self::NonViableSourceContract => "non_viable_source_contract",
        }
    }
}

#[derive(Debug, Clone)]
struct ValidationPlan {
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    required_window_start: Option<DateTime<Utc>>,
    derived_from_restore_state: bool,
    journal_covered_since: Option<DateTime<Utc>>,
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
struct ValidationScanPlan {
    coverage_method: &'static str,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    windows: Vec<SlotScanWindow>,
}

#[derive(Debug, Clone, Default)]
struct ValidationScanSummary {
    scanned_batches: usize,
    scanned_slots: usize,
    scanned_blocks: usize,
    scanned_transactions: usize,
    candidate_program_transactions: usize,
    parsed_candidate_swap_rows: usize,
    earliest_seen: Option<DateTime<Utc>>,
    latest_seen: Option<DateTime<Utc>>,
    missing_segments: Vec<GapFillMissingSegment>,
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
            .context("failed building program history validation http client")?;
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
            .map(|entries| entries.iter().filter_map(Value::as_u64).collect::<Vec<_>>())
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
    let mut window_start: Option<DateTime<Utc>> = None;
    let mut window_end: Option<DateTime<Utc>> = None;
    let mut http_url: Option<String> = None;
    let mut max_slots_to_scan_override: Option<usize> = None;
    let mut sampling_segments_override: Option<usize> = None;
    let mut report_path: Option<PathBuf> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
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
            "--report-path" => {
                report_path = Some(PathBuf::from(parse_string_arg(
                    "--report-path",
                    args.next(),
                )?))
            }
            "--json" => json = true,
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
        window_start,
        window_end,
        http_url,
        max_slots_to_scan_override,
        sampling_segments_override,
        report_path,
        json,
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
        .unwrap_or_else(|| loaded_config.program_history_validation.http_url.clone());
    let source = QuickNodeBlocksRpcSource::new(
        http_url,
        loaded_config.program_history_validation.request_timeout_ms,
        loaded_config
            .program_history_validation
            .max_requests_per_second,
        loaded_config
            .program_history_validation
            .retry_429_max_attempts,
        loaded_config
            .program_history_validation
            .retry_429_backoff_ms,
    )?;
    let report = run_with_source(config, &loaded_config, &source)?;
    if let Some(path) = report.report_path.as_ref() {
        write_json_atomic(Path::new(path), &report)?;
    }
    if json_output {
        serde_json::to_string_pretty(&report).context("failed serializing validation report json")
    } else {
        Ok(render_human(&report))
    }
}

fn run_with_source<S: ProgramHistorySource>(
    config: Config,
    loaded_config: &copybot_config::AppConfig,
    source: &S,
) -> Result<ValidationReport> {
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let runtime_store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening runtime db {}", db_path.display()))?;
    let plan = resolve_validation_plan(&runtime_store, config.window_start, config.window_end)?;
    let program_ids = validation_program_ids(loaded_config);
    let target_program_ids = sorted_program_ids(&program_ids);

    let base_report = ValidationReport {
        provider_name: source.provider_name().to_string(),
        source_kind: source.source_kind().to_string(),
        rpc_methods: source.rpc_methods(),
        target_programs: vec!["raydium".to_string(), "pumpswap".to_string()],
        target_program_ids,
        requested_window_start: plan.requested_window_start,
        requested_window_end: plan.requested_window_end,
        required_window_start: plan.required_window_start,
        derived_from_restore_state: plan.derived_from_restore_state,
        journal_covered_since: plan.journal_covered_since,
        resolved_start_slot: None,
        resolved_end_slot: None,
        slot_span: None,
        coverage_method: "unresolved".to_string(),
        scan_budget_slots: 0,
        budget_exhausted: false,
        sampling_segments: 0,
        sampling_window_slots: 0,
        max_requests_per_second: loaded_config
            .program_history_validation
            .max_requests_per_second,
        retry_429_max_attempts: loaded_config
            .program_history_validation
            .retry_429_max_attempts,
        retry_429_backoff_ms: loaded_config
            .program_history_validation
            .retry_429_backoff_ms,
        scanned_batches: 0,
        scanned_slots: 0,
        scanned_blocks: 0,
        scanned_transactions: 0,
        candidate_program_transactions: 0,
        parsed_candidate_swap_rows: 0,
        earliest_seen: None,
        latest_seen: None,
        missing_segments: Vec::new(),
        sufficient_for_next_step: false,
        verdict: ValidationVerdict::NotProvenDueToBudget.as_str().to_string(),
        reason: "validation_not_started".to_string(),
        report_path: config
            .report_path
            .as_ref()
            .map(|path| path.display().to_string()),
    };

    let bounds = match resolve_slot_bounds(
        source,
        plan.requested_window_start,
        plan.requested_window_end,
        loaded_config
            .program_history_validation
            .block_time_probe_slots,
    ) {
        Ok(bounds) => bounds,
        Err(error) => {
            let mut report = base_report;
            report.missing_segments.push(GapFillMissingSegment {
                start: plan.requested_window_start,
                end: plan.requested_window_end,
                reason: "slot_bounds_unresolved".to_string(),
            });
            let (verdict, reason) = classify_source_error("slot_bounds_unresolved", &error);
            report.verdict = verdict.as_str().to_string();
            report.reason = reason;
            return Ok(report);
        }
    };

    let slot_span = bounds
        .end_slot
        .saturating_sub(bounds.start_slot)
        .saturating_add(1);
    let mut report = base_report;
    report.resolved_start_slot = Some(bounds.start_slot);
    report.resolved_end_slot = Some(bounds.end_slot);
    report.slot_span = Some(slot_span);

    let scan_limit = config
        .max_slots_to_scan_override
        .unwrap_or(loaded_config.program_history_validation.max_slots_to_scan);
    let sampling_segments = config
        .sampling_segments_override
        .unwrap_or(loaded_config.program_history_validation.sampling_segments);
    let scan_plan = build_scan_plan(bounds.clone(), scan_limit, sampling_segments);
    report.coverage_method = scan_plan.coverage_method.to_string();
    report.scan_budget_slots = scan_plan.scan_budget_slots;
    report.budget_exhausted = scan_plan.budget_exhausted;
    report.sampling_segments = scan_plan.sampling_segments;
    report.sampling_window_slots = scan_plan.sampling_window_slots;
    if scan_plan.budget_exhausted {
        report.missing_segments.push(GapFillMissingSegment {
            start: plan.requested_window_start,
            end: plan.requested_window_end,
            reason: "validation_budget_exhausted_staged_sampling".to_string(),
        });
    }

    let summary = match scan_slot_windows(
        source,
        &program_ids,
        plan.requested_window_start,
        plan.requested_window_end,
        &scan_plan.windows,
        loaded_config.program_history_validation.block_batch_size,
    ) {
        Ok(summary) => summary,
        Err(error) => {
            let (verdict, reason) = classify_source_error("scan_failed", &error);
            report.verdict = verdict.as_str().to_string();
            report.reason = reason;
            return Ok(report);
        }
    };

    report.scanned_batches = summary.scanned_batches;
    report.scanned_slots = summary.scanned_slots;
    report.scanned_blocks = summary.scanned_blocks;
    report.scanned_transactions = summary.scanned_transactions;
    report.candidate_program_transactions = summary.candidate_program_transactions;
    report.parsed_candidate_swap_rows = summary.parsed_candidate_swap_rows;
    report.earliest_seen = summary.earliest_seen;
    report.latest_seen = summary.latest_seen;
    report.missing_segments.extend(summary.missing_segments);

    let (verdict, reason, sufficient) = classify_validation_outcome(
        &report,
        plan.requested_window_start,
        plan.requested_window_end,
    );
    report.verdict = verdict.as_str().to_string();
    report.reason = reason;
    report.sufficient_for_next_step = sufficient;
    if config.json {
        // no-op; final rendering will serialize the struct
    }
    Ok(report)
}

fn resolve_validation_plan(
    runtime_store: &SqliteStore,
    explicit_window_start: Option<DateTime<Utc>>,
    explicit_window_end: Option<DateTime<Utc>>,
) -> Result<ValidationPlan> {
    let plan = resolve_gap_fill_plan(runtime_store, explicit_window_start, explicit_window_end)?;
    Ok(ValidationPlan {
        requested_window_start: plan.requested_window_start,
        requested_window_end: plan.requested_window_end,
        required_window_start: plan.required_window_start,
        derived_from_restore_state: plan.derived_from_restore_state,
        journal_covered_since: plan.journal_covered_since,
    })
}

fn validation_program_ids(config: &copybot_config::AppConfig) -> ProgramIdConfig {
    ProgramIdConfig {
        interested_program_ids: config
            .program_history_validation
            .raydium_program_ids
            .iter()
            .chain(
                config
                    .program_history_validation
                    .pumpswap_program_ids
                    .iter(),
            )
            .cloned()
            .collect(),
        raydium_program_ids: config
            .program_history_validation
            .raydium_program_ids
            .iter()
            .cloned()
            .collect(),
        pumpswap_program_ids: config
            .program_history_validation
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

fn classify_source_error(context: &str, error: &anyhow::Error) -> (ValidationVerdict, String) {
    match source_error_kind(error) {
        Some(SourceErrorKind::ProviderThrottled) => (
            ValidationVerdict::NotProvenDueToProviderThrottling,
            format!("{context}:{error}"),
        ),
        _ => (
            ValidationVerdict::NonViableSourceContract,
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

fn build_scan_plan(
    bounds: ResolvedSlotBounds,
    max_slots_to_scan: usize,
    sampling_segments: usize,
) -> ValidationScanPlan {
    let scan_budget_slots = max_slots_to_scan.max(1);
    let slot_span = bounds
        .end_slot
        .saturating_sub(bounds.start_slot)
        .saturating_add(1);
    if slot_span <= scan_budget_slots as u64 {
        return ValidationScanPlan {
            coverage_method: "full_slot_scan",
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

    let segment_count = sampling_segments.max(1).min(scan_budget_slots);
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

    ValidationScanPlan {
        coverage_method: "staged_slot_sampling",
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
    block_batch_size: usize,
) -> Result<ValidationScanSummary> {
    let mut combined = ValidationScanSummary::default();
    for window in windows {
        let summary = scan_slot_range(
            source,
            program_ids,
            requested_window_start,
            requested_window_end,
            window.start_slot,
            window.end_slot,
            block_batch_size,
        )?;
        combined.scanned_batches = combined
            .scanned_batches
            .saturating_add(summary.scanned_batches);
        combined.scanned_slots = combined.scanned_slots.saturating_add(summary.scanned_slots);
        combined.scanned_blocks = combined
            .scanned_blocks
            .saturating_add(summary.scanned_blocks);
        combined.scanned_transactions = combined
            .scanned_transactions
            .saturating_add(summary.scanned_transactions);
        combined.candidate_program_transactions = combined
            .candidate_program_transactions
            .saturating_add(summary.candidate_program_transactions);
        combined.parsed_candidate_swap_rows = combined
            .parsed_candidate_swap_rows
            .saturating_add(summary.parsed_candidate_swap_rows);
        combined.earliest_seen = match (combined.earliest_seen, summary.earliest_seen) {
            (Some(current), Some(next)) => Some(current.min(next)),
            (None, next) => next,
            (current, None) => current,
        };
        combined.latest_seen = match (combined.latest_seen, summary.latest_seen) {
            (Some(current), Some(next)) => Some(current.max(next)),
            (None, next) => next,
            (current, None) => current,
        };
        combined.missing_segments.extend(summary.missing_segments);
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
    block_batch_size: usize,
) -> Result<ValidationScanSummary> {
    let mut summary = ValidationScanSummary::default();
    if end_slot < start_slot {
        return Ok(summary);
    }
    let batch_size = block_batch_size.max(1) as u64;
    let mut batch_start = start_slot;
    while batch_start <= end_slot {
        let batch_end = end_slot.min(batch_start.saturating_add(batch_size.saturating_sub(1)));
        summary.scanned_batches = summary.scanned_batches.saturating_add(1);
        summary.scanned_slots = summary
            .scanned_slots
            .saturating_add(batch_end.saturating_sub(batch_start).saturating_add(1) as usize);
        let block_slots = source.list_blocks(batch_start, batch_end)?;
        summary.scanned_blocks = summary.scanned_blocks.saturating_add(block_slots.len());
        for slot in block_slots {
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
            summary.scanned_transactions = summary
                .scanned_transactions
                .saturating_add(transactions.len());
            for transaction in transactions {
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
                summary.earliest_seen = match summary.earliest_seen {
                    Some(current) => Some(current.min(tx_time)),
                    None => Some(tx_time),
                };
                summary.latest_seen = match summary.latest_seen {
                    Some(current) => Some(current.max(tx_time)),
                    None => Some(tx_time),
                };
                if parse_program_scoped_transaction_to_swap(&tx, program_ids)?.is_some() {
                    summary.parsed_candidate_swap_rows =
                        summary.parsed_candidate_swap_rows.saturating_add(1);
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

fn classify_validation_outcome(
    report: &ValidationReport,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
) -> (ValidationVerdict, String, bool) {
    if report.slot_span.is_some_and(|span| span == 0) {
        return (
            ValidationVerdict::NonViableSourceContract,
            "resolved_slot_span_is_zero".to_string(),
            false,
        );
    }
    if report.scanned_blocks == 0 {
        return (
            ValidationVerdict::NonViableSourceContract,
            format!(
                "no_blocks_returned_for_scanned_windows:coverage_method={}",
                report.coverage_method
            ),
            false,
        );
    }
    if report.budget_exhausted {
        let detail = if report.parsed_candidate_swap_rows > 0 {
            "budget_exhausted_after_parseable_program_history_seen"
        } else if report.candidate_program_transactions > 0 {
            "budget_exhausted_after_target_program_transactions_without_parseable_rows"
        } else {
            "budget_exhausted_before_program_history_proof"
        };
        return (
            ValidationVerdict::NotProvenDueToBudget,
            format!(
                "{detail}:slot_span={} scan_budget_slots={} sampling_segments={} sampling_window_slots={}",
                report.slot_span.unwrap_or(0),
                report.scan_budget_slots,
                report.sampling_segments,
                report.sampling_window_slots
            ),
            false,
        );
    }
    if report.candidate_program_transactions == 0 {
        return (
            ValidationVerdict::NotProvenDueToSparseProgramHistory,
            format!(
                "no_target_program_transactions_seen_in_window:{}..{}",
                requested_window_start.to_rfc3339(),
                requested_window_end.to_rfc3339()
            ),
            false,
        );
    }
    if report.parsed_candidate_swap_rows == 0 {
        return (
            ValidationVerdict::NotProvenDueToSparseProgramHistory,
            "target_program_transactions_seen_but_no_parseable_swap_rows".to_string(),
            false,
        );
    }
    (
        ValidationVerdict::Viable,
        "program_scoped_historical_raw_observed_and_parseable".to_string(),
        true,
    )
}

fn render_human(report: &ValidationReport) -> String {
    [
        "event=discovery_program_history_source_validate".to_string(),
        format!("provider_name={}", report.provider_name),
        format!("source_kind={}", report.source_kind),
        format!("target_programs={}", report.target_programs.join(",")),
        format!("target_program_ids={}", report.target_program_ids.join(",")),
        format!(
            "requested_window_start={}",
            report.requested_window_start.to_rfc3339()
        ),
        format!(
            "requested_window_end={}",
            report.requested_window_end.to_rfc3339()
        ),
        format!(
            "resolved_start_slot={}",
            report
                .resolved_start_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "resolved_end_slot={}",
            report
                .resolved_end_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "slot_span={}",
            report
                .slot_span
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("coverage_method={}", report.coverage_method),
        format!("scan_budget_slots={}", report.scan_budget_slots),
        format!("budget_exhausted={}", report.budget_exhausted),
        format!("sampling_segments={}", report.sampling_segments),
        format!("sampling_window_slots={}", report.sampling_window_slots),
        format!("max_requests_per_second={}", report.max_requests_per_second),
        format!("retry_429_max_attempts={}", report.retry_429_max_attempts),
        format!("retry_429_backoff_ms={}", report.retry_429_backoff_ms),
        format!("scanned_batches={}", report.scanned_batches),
        format!("scanned_slots={}", report.scanned_slots),
        format!("scanned_blocks={}", report.scanned_blocks),
        format!("scanned_transactions={}", report.scanned_transactions),
        format!(
            "candidate_program_transactions={}",
            report.candidate_program_transactions
        ),
        format!(
            "parsed_candidate_swap_rows={}",
            report.parsed_candidate_swap_rows
        ),
        format!(
            "earliest_seen={}",
            report
                .earliest_seen
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_seen={}",
            report
                .latest_seen
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "missing_segments={}",
            if report.missing_segments.is_empty() {
                "[]".to_string()
            } else {
                serde_json::to_string(&report.missing_segments).unwrap_or_else(|_| "[]".to_string())
            }
        ),
        format!(
            "sufficient_for_next_step={}",
            report.sufficient_for_next_step
        ),
        format!("verdict={}", report.verdict),
        format!("reason={}", report.reason),
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
    use copybot_discovery::DiscoveryService;
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRecentRawRestoreStateUpdate,
        DiscoveryRuntimeCursor, WalletMetricRow,
    };
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

    struct ThrottledProgramHistorySource;

    impl ProgramHistorySource for ThrottledProgramHistorySource {
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
            Err(SourceError::provider_throttled(
                "rpc returned http status 429 Too Many Requests: 125/second request limit reached",
            )
            .into())
        }

        fn block_time(&self, _slot: u64) -> Result<Option<DateTime<Utc>>> {
            unreachable!("latest_finalized_slot should fail first")
        }

        fn list_blocks(&self, _start_slot: u64, _end_slot: u64) -> Result<Vec<u64>> {
            unreachable!("latest_finalized_slot should fail first")
        }

        fn get_block(&self, _slot: u64) -> Result<Option<Value>> {
            unreachable!("latest_finalized_slot should fail first")
        }
    }

    struct Fixture {
        _temp: tempfile::TempDir,
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
    fn request_pacer_claim_delay_enforces_spacing_contract() {
        let mut pacer = RequestPacer::new(100);
        let now = Instant::now();
        assert_eq!(pacer.claim_delay(now), StdDuration::ZERO);
        let second_delay = pacer.claim_delay(now);
        assert!(second_delay >= StdDuration::from_millis(9));
    }

    #[test]
    fn execute_json_rpc_with_policy_retries_provider_throttling() -> Result<()> {
        let pacer = Mutex::new(RequestPacer::new(125));
        let policy = QuickNodeRequestPolicy {
            max_requests_per_second: 100,
            retry_429_max_attempts: 2,
            retry_429_backoff_ms: 7,
        };
        let mut attempts = 0usize;
        let mut sleep_calls = Vec::new();
        let response = execute_json_rpc_with_policy(
            &pacer,
            &policy,
            || {
                attempts = attempts.saturating_add(1);
                if attempts < 3 {
                    return Ok(HttpRpcResponse {
                        status_code: 429,
                        body: "125/second request limit reached".to_string(),
                    });
                }
                Ok(HttpRpcResponse {
                    status_code: 200,
                    body: r#"{"jsonrpc":"2.0","result":123}"#.to_string(),
                })
            },
            |duration| sleep_calls.push(duration),
        )?;
        assert_eq!(attempts, 3);
        assert_eq!(rpc_result(&response).as_i64(), Some(123));
        assert!(sleep_calls
            .iter()
            .any(|duration| *duration >= StdDuration::from_millis(7)));
        Ok(())
    }

    #[test]
    fn run_derives_exact_missing_bounded_window_from_restore_state() -> Result<()> {
        let fixture = make_fixture("program-history-derive-window")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let source = dense_source(now);
        let report = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: None,
                window_end: None,
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &source,
        )?;
        assert_eq!(
            report.requested_window_start.to_rfc3339(),
            "2026-03-17T13:43:40+00:00"
        );
        assert_eq!(
            report.requested_window_end.to_rfc3339(),
            "2026-03-22T13:43:40+00:00"
        );
        assert!(report.derived_from_restore_state);
        Ok(())
    }

    #[test]
    fn tool_does_not_count_transactions_outside_requested_horizon() -> Result<()> {
        let fixture = make_fixture("program-history-horizon")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let window_start = now - chrono::Duration::days(7);
        let window_end = now - chrono::Duration::days(2);
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start)
            .with_block_time(101, window_start + chrono::Duration::hours(1))
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
                    window_start + chrono::Duration::hours(1),
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
        let report = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &source,
        )?;
        assert_eq!(report.candidate_program_transactions, 2);
        assert_eq!(report.parsed_candidate_swap_rows, 2);
        let requests = source.list_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].start_slot, 100);
        assert_eq!(requests[0].end_slot, 102);
        Ok(())
    }

    #[test]
    fn program_filters_are_applied() -> Result<()> {
        let fixture = make_fixture("program-history-filters")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - chrono::Duration::days(7);
        let window_end = now - chrono::Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start)
            .with_block_time(101, window_end - chrono::Duration::minutes(1))
            .with_block_time(102, window_end)
            .with_block_range(100, 102, vec![100, 101, 102])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx("ray", "wallet-gap", 100, "raydium-program")],
                ),
            )
            .with_block(
                101,
                block_with_transactions(
                    101,
                    window_end - chrono::Duration::minutes(1),
                    vec![swap_tx("other", "wallet-gap", 101, "other-program")],
                ),
            )
            .with_block(102, block_with_transactions(102, window_end, vec![]));
        let report = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &source,
        )?;
        assert_eq!(report.candidate_program_transactions, 1);
        assert_eq!(report.parsed_candidate_swap_rows, 1);
        Ok(())
    }

    #[test]
    fn output_marks_viable_vs_budget_exhausted_sources() -> Result<()> {
        let fixture = make_fixture("program-history-verdicts")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - chrono::Duration::days(7);
        let window_end = now - chrono::Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let viable_source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start)
            .with_block_time(101, window_end - chrono::Duration::minutes(1))
            .with_block_time(102, window_end)
            .with_block_range(100, 102, vec![100, 101, 102])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx("ray", "wallet-gap", 100, "raydium-program")],
                ),
            )
            .with_block(
                101,
                block_with_transactions(
                    101,
                    window_end - chrono::Duration::minutes(1),
                    vec![swap_tx("pump", "wallet-gap", 101, "pumpswap-program")],
                ),
            )
            .with_block(102, block_with_transactions(102, window_end, vec![]));
        let viable = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &viable_source,
        )?;
        assert_eq!(viable.verdict, "viable");
        assert!(viable.sufficient_for_next_step);

        let budget_exhausted_source = FakeProgramHistorySource::default()
            .with_latest_slot(40_201)
            .with_block_time(100, window_start)
            .with_block_time(40_200, window_end - chrono::Duration::minutes(1))
            .with_block_time(40_201, window_end)
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx("ray-budget", "wallet-gap", 100, "raydium-program")],
                ),
            );
        let budget_exhausted = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &budget_exhausted_source,
        )?;
        assert_eq!(budget_exhausted.verdict, "not_proven_due_to_budget");
        assert!(!budget_exhausted.sufficient_for_next_step);
        assert!(budget_exhausted.budget_exhausted);
        assert_eq!(budget_exhausted.coverage_method, "staged_slot_sampling");
        assert!(budget_exhausted.reason.contains("budget_exhausted"));
        Ok(())
    }

    #[test]
    fn source_contract_failure_stays_non_viable() -> Result<()> {
        let fixture = make_fixture("program-history-source-failure")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - chrono::Duration::days(7);
        let window_end = now - chrono::Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let failing_source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start);
        let report = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &failing_source,
        )?;
        assert_eq!(report.verdict, "non_viable_source_contract");
        assert!(!report.sufficient_for_next_step);
        assert!(report.reason.contains("slot_bounds_unresolved"));
        Ok(())
    }

    #[test]
    fn provider_throttling_is_not_reported_as_non_viable_source_contract() -> Result<()> {
        let fixture = make_fixture("program-history-provider-throttle")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - chrono::Duration::days(7);
        let window_end = now - chrono::Duration::days(2);
        seed_restored_runtime_with_short_journal(
            &fixture.runtime_db_path,
            &fixture.config_path,
            now,
            now - chrono::Duration::days(2),
        )?;
        let report = run_report_with_source(
            Config {
                config_path: fixture.config_path.clone(),
                db_path: Some(fixture.runtime_db_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                http_url: Some("https://quicknode.example/?api-key=test".to_string()),
                max_slots_to_scan_override: None,
                sampling_segments_override: None,
                report_path: None,
                json: true,
            },
            &ThrottledProgramHistorySource,
        )?;
        assert_eq!(report.verdict, "not_proven_due_to_provider_throttling");
        assert!(!report.sufficient_for_next_step);
        assert!(report.reason.contains("slot_bounds_unresolved"));
        Ok(())
    }

    #[test]
    fn quicknode_first_operator_config_path_is_valid() -> Result<()> {
        let template_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../ops/server_templates/live.server.toml.example");
        let config = load_from_path(&template_path)?;
        assert_eq!(
            config.program_history_validation.source,
            "quicknode_blocks_rpc"
        );
        assert!(!config.program_history_validation.http_url.trim().is_empty());
        Ok(())
    }

    fn run_report_with_source<S: ProgramHistorySource>(
        config: Config,
        source: &S,
    ) -> Result<ValidationReport> {
        let loaded_config = load_from_path(&config.config_path)?;
        run_with_source(config, &loaded_config, source)
    }

    fn dense_source(now: DateTime<Utc>) -> FakeProgramHistorySource {
        let start = now - chrono::Duration::days(7);
        let end = now - chrono::Duration::days(2);
        FakeProgramHistorySource::default()
            .with_latest_slot(1_050_000)
            .with_block_time(1_000_000, start)
            .with_block_time(1_000_001, start + chrono::Duration::minutes(1))
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
                    start + chrono::Duration::minutes(1),
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
        std::fs::write(
            &config_path,
            format!(
                "[system]\nmigrations_dir = \"{}\"\n\n[sqlite]\npath = \"{}\"\n\n[program_history_validation]\nhttp_url = \"https://quicknode.example/?api-key=test\"\nrequest_timeout_ms = 1000\nblock_batch_size = 512\nmax_slots_to_scan = 20000\nblock_time_probe_slots = 128\nraydium_program_ids = [\"raydium-program\"]\npumpswap_program_ids = [\"pumpswap-program\"]\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[execution]\nenabled = false\n",
                migration_dir.display(),
                runtime_db_path.display()
            ),
        )?;
        Ok(Fixture {
            _temp: temp,
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
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let source_db_path = runtime_db_path.with_file_name("source-program-history.db");
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
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::minutes(30),
            slot: 120,
            signature: "artifact-cursor".to_string(),
        })?;
        Ok(())
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
