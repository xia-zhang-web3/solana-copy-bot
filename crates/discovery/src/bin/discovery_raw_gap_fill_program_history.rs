use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
#[cfg(test)]
use chrono::Duration;
use copybot_config::load_from_path;
use copybot_core_types::SwapEvent;
use copybot_discovery::raw_gap_fill_support::{
    compute_missing_segments, format_optional_cursor, format_optional_ts, max_cursor_opt,
    min_ts_opt, parse_program_scoped_transaction_to_swap_with_context, resolve_gap_fill_plan,
    transaction_mentions_target_programs, GapFillMissingSegment,
    GapFillPlan, ProgramIdConfig,
};
#[cfg(test)]
use copybot_discovery::raw_gap_fill_support::reset_sqlite_path;
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

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history --config <path> [--db-path <path>] [--output <path>] [--window-start <rfc3339> --window-end <rfc3339>] [--http-url <url>] [--max-slots-to-scan <n>] [--sampling-segments <n>] [--block-fetch-concurrency <n>] [--max-slot-batches-per-attempt <n>] [--max-blocks-to-fetch <n>] [--max-candidate-transactions-to-parse <n>] [--json] [--now <rfc3339>]";
const AVG_SLOT_MS: f64 = 400.0;
const IN_PROGRESS_DB_NAME: &str = "in_progress.sqlite";
const DEFAULT_PROGRESS_DOMINANT_PHASE: &str = "unknown";
const DEFAULT_BLOCK_FETCH_ENCODING: &str = "json";
const DEFAULT_BLOCK_FETCH_CONCURRENCY: usize = 1;
const ZERO_PROGRESS_ESCAPE_THRESHOLD: usize = 3;
const ZERO_PROGRESS_ESCAPE_DISTANCE: u64 = 1;
const ZERO_PROGRESS_ESCAPED_SLOT_MISSING_REASON: &str =
    "program_history_gap_fill_skipped_persistently_provider_blocked_slot_after_bounded_retries";

fn default_boundary_adjustment_kind() -> String {
    BoundaryAdjustmentKind::Unresolved.as_str().to_string()
}

fn default_boundary_resolution_verdict() -> String {
    BoundaryResolutionVerdict::Unresolved.as_str().to_string()
}

fn default_progress_dominant_phase() -> String {
    DEFAULT_PROGRESS_DOMINANT_PHASE.to_string()
}

fn default_block_fetch_encoding() -> String {
    DEFAULT_BLOCK_FETCH_ENCODING.to_string()
}

fn default_block_fetch_concurrency() -> usize {
    DEFAULT_BLOCK_FETCH_CONCURRENCY
}

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
    block_fetch_concurrency_override: Option<usize>,
    max_slot_batches_per_attempt_override: Option<usize>,
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
    progress_db_path: Option<String>,
    progress_state_path: Option<String>,
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
    requested_start_slot_raw: Option<u64>,
    requested_end_slot_raw: Option<u64>,
    resolved_start_slot: Option<u64>,
    resolved_end_slot: Option<u64>,
    #[serde(default = "default_boundary_adjustment_kind")]
    start_slot_adjustment_kind: String,
    #[serde(default = "default_boundary_adjustment_kind")]
    end_slot_adjustment_kind: String,
    start_slot_adjustment_distance: Option<u64>,
    end_slot_adjustment_distance: Option<u64>,
    #[serde(default = "default_boundary_resolution_verdict")]
    boundary_resolution_verdict: String,
    #[serde(default)]
    requested_interval_fully_bracketed_after_adjustment: bool,
    #[serde(default)]
    requested_interval_partially_bracketed_after_adjustment: bool,
    slot_span: Option<u64>,
    #[serde(default)]
    resolved_bounds_reused_from_progress: bool,
    coverage_method: String,
    #[serde(default = "default_block_fetch_encoding")]
    block_fetch_encoding: String,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    phase_b_like_cost_budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    block_batch_size: usize,
    #[serde(default = "default_block_fetch_concurrency")]
    block_fetch_concurrency: usize,
    max_slot_batches_per_attempt: usize,
    max_blocks_to_fetch: usize,
    max_candidate_transactions_to_parse: usize,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
    current_phase: String,
    #[serde(default = "default_progress_dominant_phase")]
    dominant_phase: String,
    #[serde(default)]
    resolve_slot_bounds_ms: u64,
    attempt_number: usize,
    cumulative_across_attempts: bool,
    attempt_frontier_start_slot: Option<u64>,
    attempt_frontier_end_slot: Option<u64>,
    #[serde(default)]
    attempt_frontier_advanced_slots: usize,
    next_batch_start_slot: Option<u64>,
    #[serde(default)]
    zero_progress_retry_count: usize,
    #[serde(default)]
    zero_progress_blocked_start_slot: Option<u64>,
    #[serde(default)]
    zero_progress_escape_applied: bool,
    #[serde(default)]
    zero_progress_escape_distance: Option<u64>,
    progress_reset_reason: Option<String>,
    attempt_budget_exhausted: bool,
    #[serde(default)]
    attempt_block_list_ms: u64,
    #[serde(default)]
    attempt_block_fetch_ms: u64,
    #[serde(default)]
    attempt_candidate_filter_ms: u64,
    #[serde(default)]
    attempt_swap_parse_ms: u64,
    #[serde(default)]
    attempt_sqlite_stage_ms: u64,
    attempt_scanned_batches: usize,
    attempt_scanned_slots: usize,
    attempt_listed_block_slots: usize,
    attempt_scanned_blocks: usize,
    attempt_scanned_transactions: usize,
    attempt_candidate_program_transactions: usize,
    attempt_parsed_candidate_transactions: usize,
    attempt_parsed_candidate_swaps: usize,
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
    attempt_inserted_rows: usize,
    staged_rows: usize,
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
    NotProvenDueToAttemptBudget,
    NotProvenDueToScanBudget,
    NotProvenDueToCostBudget,
    NotProvenDueToProviderThrottling,
    NotProvenDueToRetryableProviderFailure,
    NonViableSourceContract,
}

impl GapFillVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::CompleteSufficientForHealthyRestore => "complete_sufficient_for_healthy_restore",
            Self::CompleteButInsufficientForHealthyRestore => {
                "complete_but_insufficient_for_healthy_restore"
            }
            Self::NotProvenDueToAttemptBudget => "not_proven_due_to_attempt_budget",
            Self::NotProvenDueToScanBudget => "not_proven_due_to_scan_budget",
            Self::NotProvenDueToCostBudget => "not_proven_due_to_cost_budget",
            Self::NotProvenDueToProviderThrottling => "not_proven_due_to_provider_throttling",
            Self::NotProvenDueToRetryableProviderFailure => {
                "not_proven_due_to_retryable_provider_failure"
            }
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
    start: SlotBoundaryResolution,
    end: SlotBoundaryResolution,
    boundary_resolution_verdict: BoundaryResolutionVerdict,
    requested_interval_fully_bracketed_after_adjustment: bool,
    requested_interval_partially_bracketed_after_adjustment: bool,
    boundary_missing_segments: Vec<GapFillMissingSegment>,
}

impl ResolvedSlotBounds {
    fn requested_start_slot_raw(&self) -> Option<u64> {
        self.start.requested_slot_raw
    }

    fn requested_end_slot_raw(&self) -> Option<u64> {
        self.end.requested_slot_raw
    }

    fn start_slot(&self) -> Option<u64> {
        self.start.resolved_slot
    }

    fn end_slot(&self) -> Option<u64> {
        self.end.resolved_slot
    }

    fn usable_slot_range(&self) -> Option<(u64, u64)> {
        let start_slot = self.start_slot()?;
        let end_slot = self.end_slot()?;
        (end_slot >= start_slot).then_some((start_slot, end_slot))
    }

    fn failure_reason(&self) -> String {
        match (self.start.resolved_slot, self.end.resolved_slot) {
            (None, None) => "slot_boundaries_unresolved_within_search_radius".to_string(),
            (None, Some(_)) => "start_slot_boundary_unresolved_within_search_radius".to_string(),
            (Some(_), None) => "end_slot_boundary_unresolved_within_search_radius".to_string(),
            (Some(start_slot), Some(end_slot)) if end_slot < start_slot => {
                "resolved_slot_bounds_crossed_after_boundary_adjustment".to_string()
            }
            _ => "slot_bounds_unresolved".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct SlotBoundaryResolution {
    requested_slot_raw: Option<u64>,
    resolved_slot: Option<u64>,
    resolved_block_time: Option<DateTime<Utc>>,
    adjustment_kind: BoundaryAdjustmentKind,
    adjustment_distance: Option<u64>,
}

impl SlotBoundaryResolution {
    fn unresolved(requested_slot_raw: Option<u64>) -> Self {
        Self {
            requested_slot_raw,
            resolved_slot: None,
            resolved_block_time: None,
            adjustment_kind: BoundaryAdjustmentKind::Unresolved,
            adjustment_distance: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryAdjustmentKind {
    Exact,
    AdvancedToNextAvailable,
    RewoundToPreviousAvailable,
    Unresolved,
}

impl BoundaryAdjustmentKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::AdvancedToNextAvailable => "advanced_to_next_available",
            Self::RewoundToPreviousAvailable => "rewound_to_previous_available",
            Self::Unresolved => "unresolved",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryResolutionVerdict {
    FullyBracketed,
    PartiallyBracketed,
    Unresolved,
}

impl BoundaryResolutionVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::FullyBracketed => "fully_bracketed",
            Self::PartiallyBracketed => "partially_bracketed",
            Self::Unresolved => "unresolved",
        }
    }
}

fn parse_boundary_adjustment_kind(raw: &str) -> BoundaryAdjustmentKind {
    match raw {
        "exact" => BoundaryAdjustmentKind::Exact,
        "advanced_to_next_available" => BoundaryAdjustmentKind::AdvancedToNextAvailable,
        "rewound_to_previous_available" => BoundaryAdjustmentKind::RewoundToPreviousAvailable,
        _ => BoundaryAdjustmentKind::Unresolved,
    }
}

fn parse_boundary_resolution_verdict(raw: &str) -> BoundaryResolutionVerdict {
    match raw {
        "fully_bracketed" => BoundaryResolutionVerdict::FullyBracketed,
        "partially_bracketed" => BoundaryResolutionVerdict::PartiallyBracketed,
        _ => BoundaryResolutionVerdict::Unresolved,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundarySide {
    Start,
    End,
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
    block_fetch_concurrency: usize,
    max_slot_batches_per_attempt: usize,
    max_blocks_to_fetch: usize,
    max_candidate_transactions_to_parse: usize,
}

#[derive(Debug, Clone)]
struct OutputPaths {
    output_db_path: PathBuf,
    latest_db_path: Option<PathBuf>,
    metadata_path: PathBuf,
    output_dir: Option<PathBuf>,
    progress_db_path: PathBuf,
    progress_state_path: PathBuf,
}

#[derive(Debug, Clone)]
struct FetchResult {
    requested_start_slot_raw: Option<u64>,
    requested_end_slot_raw: Option<u64>,
    resolved_start_slot: Option<u64>,
    resolved_end_slot: Option<u64>,
    start_slot_adjustment_kind: String,
    end_slot_adjustment_kind: String,
    start_slot_adjustment_distance: Option<u64>,
    end_slot_adjustment_distance: Option<u64>,
    boundary_resolution_verdict: String,
    requested_interval_fully_bracketed_after_adjustment: bool,
    requested_interval_partially_bracketed_after_adjustment: bool,
    slot_span: Option<u64>,
    boundary_missing_segments: Vec<GapFillMissingSegment>,
    resolved_bounds_reused_from_progress: bool,
    coverage_method: String,
    block_fetch_encoding: String,
    scan_budget_slots: usize,
    budget_exhausted: bool,
    phase_b_like_cost_budget_exhausted: bool,
    sampling_segments: usize,
    sampling_window_slots: usize,
    block_batch_size: usize,
    block_fetch_concurrency: usize,
    max_slot_batches_per_attempt: usize,
    max_blocks_to_fetch: usize,
    max_candidate_transactions_to_parse: usize,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
    current_phase: String,
    dominant_phase: String,
    resolve_slot_bounds_ms: u64,
    attempt_number: usize,
    cumulative_across_attempts: bool,
    attempt_frontier_start_slot: Option<u64>,
    attempt_frontier_end_slot: Option<u64>,
    attempt_frontier_advanced_slots: usize,
    next_batch_start_slot: Option<u64>,
    zero_progress_retry_count: usize,
    zero_progress_blocked_start_slot: Option<u64>,
    zero_progress_escape_applied: bool,
    zero_progress_escape_distance: Option<u64>,
    progress_reset_reason: Option<String>,
    attempt_budget_exhausted: bool,
    attempt_block_list_ms: u64,
    attempt_block_fetch_ms: u64,
    attempt_candidate_filter_ms: u64,
    attempt_swap_parse_ms: u64,
    attempt_sqlite_stage_ms: u64,
    attempt_scanned_batches: usize,
    attempt_scanned_slots: usize,
    attempt_listed_block_slots: usize,
    attempt_scanned_blocks: usize,
    attempt_scanned_transactions: usize,
    attempt_candidate_program_transactions: usize,
    attempt_parsed_candidate_transactions: usize,
    attempt_parsed_candidate_swaps: usize,
    scanned_batches: usize,
    scanned_slots: usize,
    listed_block_slots: usize,
    scanned_blocks: usize,
    scanned_transactions: usize,
    candidate_program_transactions: usize,
    parsed_candidate_transactions: usize,
    parsed_candidate_swaps: usize,
    attempt_inserted_rows: usize,
    staged_rows: usize,
    gap_fill_covered_since: Option<DateTime<Utc>>,
    gap_fill_covered_through_cursor: Option<copybot_storage::DiscoveryRuntimeCursor>,
    verdict: GapFillVerdict,
    reason: String,
    early_stop_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct AttemptScanResult {
    summary: ScanSummary,
    next_slot_to_scan: Option<u64>,
    attempt_budget_exhausted: bool,
    source_error: Option<SourceError>,
}

#[derive(Debug, Clone, Default)]
struct ScanSummary {
    frontier_end_slot: Option<u64>,
    block_list_ms: u64,
    block_fetch_ms: u64,
    candidate_filter_ms: u64,
    swap_parse_ms: u64,
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
    RetryableProviderFailure,
    SourceContractFailure,
}

#[derive(Debug, Clone)]
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

    fn retryable_provider_failure(message: impl Into<String>) -> Self {
        Self {
            kind: SourceErrorKind::RetryableProviderFailure,
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
        let block_time = post_json_rpc_allow_missing_block_time(self, &payload)?
            .and_then(|response| {
                rpc_result(&response)
                    .as_i64()
                    .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
            });
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
                    "encoding": "json",
                    "transactionDetails": "full",
                    "rewards": false,
                    "commitment": "finalized",
                    "maxSupportedTransactionVersion": 0
                }
            ],
        });
        let response = match post_json_rpc(self, &payload) {
            Ok(response) => response,
            Err(error) => {
                if let Some(reason_code) = retryable_block_fetch_reason_code(&error.to_string()) {
                    return Err(SourceError::retryable_provider_failure(format!(
                        "{reason_code}: {error}"
                    ))
                    .into());
                }
                return Err(error);
            }
        };
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
    let mut block_fetch_concurrency_override: Option<usize> = None;
    let mut max_slot_batches_per_attempt_override: Option<usize> = None;
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
            "--block-fetch-concurrency" => {
                block_fetch_concurrency_override =
                    Some(parse_usize_arg("--block-fetch-concurrency", args.next())?)
            }
            "--max-slot-batches-per-attempt" => {
                max_slot_batches_per_attempt_override = Some(parse_usize_arg(
                    "--max-slot-batches-per-attempt",
                    args.next(),
                )?)
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
        block_fetch_concurrency_override,
        max_slot_batches_per_attempt_override,
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

fn run_with_source<S: ProgramHistorySource + Sync>(
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
    let output_paths = resolve_output_paths(
        &config,
        &loaded_config.program_history_gap_fill.output_dir,
        config.now,
    );

    let fetch = fetch_program_history_gap_fill(
        source,
        &program_ids,
        &target_program_ids,
        &plan,
        settings,
        loaded_config
            .program_history_gap_fill
            .max_requests_per_second,
        loaded_config
            .program_history_gap_fill
            .retry_429_max_attempts,
        loaded_config.program_history_gap_fill.retry_429_backoff_ms,
        &output_paths,
        config.now,
    );

    match fetch {
        Ok(fetch) => write_gap_fill_output(
            &db_path,
            source,
            &target_program_ids,
            &plan,
            fetch,
            &output_paths,
            loaded_config.program_history_gap_fill.output_retention,
            config.now,
        ),
        Err(error) => {
            let (verdict, reason) =
                classify_source_error("program_history_gap_fill_failed", &error);
            write_gap_fill_output(
                &db_path,
                source,
                &target_program_ids,
                &plan,
                FetchResult {
                    requested_start_slot_raw: None,
                    requested_end_slot_raw: None,
                    resolved_start_slot: None,
                    resolved_end_slot: None,
                    start_slot_adjustment_kind: BoundaryAdjustmentKind::Unresolved
                        .as_str()
                        .to_string(),
                    end_slot_adjustment_kind: BoundaryAdjustmentKind::Unresolved
                        .as_str()
                        .to_string(),
                    start_slot_adjustment_distance: None,
                    end_slot_adjustment_distance: None,
                    boundary_resolution_verdict: BoundaryResolutionVerdict::Unresolved
                        .as_str()
                        .to_string(),
                    requested_interval_fully_bracketed_after_adjustment: false,
                    requested_interval_partially_bracketed_after_adjustment: false,
                    slot_span: None,
                    boundary_missing_segments: Vec::new(),
                    resolved_bounds_reused_from_progress: false,
                    coverage_method: "unresolved".to_string(),
                    block_fetch_encoding: "json".to_string(),
                    scan_budget_slots: settings.max_slots_to_scan,
                    budget_exhausted: false,
                    phase_b_like_cost_budget_exhausted: false,
                    sampling_segments: settings.sampling_segments,
                    sampling_window_slots: 0,
                    block_batch_size: settings.block_batch_size,
                    block_fetch_concurrency: settings.block_fetch_concurrency,
                    max_slot_batches_per_attempt: settings.max_slot_batches_per_attempt,
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
                    current_phase: "source_contract_failed".to_string(),
                    dominant_phase: "source_contract_failed".to_string(),
                    resolve_slot_bounds_ms: 0,
                    attempt_number: 1,
                    cumulative_across_attempts: false,
                    attempt_frontier_start_slot: None,
                    attempt_frontier_end_slot: None,
                    attempt_frontier_advanced_slots: 0,
                    next_batch_start_slot: None,
                    zero_progress_retry_count: 0,
                    zero_progress_blocked_start_slot: None,
                    zero_progress_escape_applied: false,
                    zero_progress_escape_distance: None,
                    progress_reset_reason: None,
                    attempt_budget_exhausted: false,
                    attempt_block_list_ms: 0,
                    attempt_block_fetch_ms: 0,
                    attempt_candidate_filter_ms: 0,
                    attempt_swap_parse_ms: 0,
                    attempt_sqlite_stage_ms: 0,
                    attempt_scanned_batches: 0,
                    attempt_scanned_slots: 0,
                    attempt_listed_block_slots: 0,
                    attempt_scanned_blocks: 0,
                    attempt_scanned_transactions: 0,
                    attempt_candidate_program_transactions: 0,
                    attempt_parsed_candidate_transactions: 0,
                    attempt_parsed_candidate_swaps: 0,
                    scanned_batches: 0,
                    scanned_slots: 0,
                    listed_block_slots: 0,
                    scanned_blocks: 0,
                    scanned_transactions: 0,
                    candidate_program_transactions: 0,
                    parsed_candidate_transactions: 0,
                    parsed_candidate_swaps: 0,
                    attempt_inserted_rows: 0,
                    staged_rows: 0,
                    gap_fill_covered_since: None,
                    gap_fill_covered_through_cursor: None,
                    verdict,
                    reason,
                    early_stop_reason: None,
                },
                &output_paths,
                loaded_config.program_history_gap_fill.output_retention,
                config.now,
            )
        }
    }
}

fn resolve_output_paths(
    config: &Config,
    configured_output_dir: &str,
    now: DateTime<Utc>,
) -> OutputPaths {
    if let Some(path) = config.output_path.as_ref() {
        let progress_db_path = path_with_inserted_suffix(path, ".progress");
        return OutputPaths {
            output_db_path: path.clone(),
            latest_db_path: None,
            metadata_path: journal_snapshot_metadata_path(path),
            output_dir: None,
            progress_state_path: journal_snapshot_metadata_path(&progress_db_path),
            progress_db_path,
        };
    }

    let dir = resolve_relative_to_config(&config.config_path, Path::new(configured_output_dir));
    OutputPaths {
        output_db_path: program_history_gap_fill_archive_path(&dir, now),
        latest_db_path: Some(program_history_gap_fill_latest_path(&dir)),
        metadata_path: program_history_gap_fill_latest_metadata_path(&dir),
        output_dir: Some(dir.clone()),
        progress_db_path: dir.join(IN_PROGRESS_DB_NAME),
        progress_state_path: dir.join("in_progress.json"),
    }
}

fn path_with_inserted_suffix(path: &Path, suffix: &str) -> PathBuf {
    let file_stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("output");
    let mut file_name = format!("{file_stem}{suffix}");
    if let Some(extension) = path.extension().and_then(|value| value.to_str()) {
        file_name.push('.');
        file_name.push_str(extension);
    }
    path.with_file_name(file_name)
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("failed removing {}", path.display()))?;
    }
    Ok(())
}

fn clear_progress_artifacts(paths: &OutputPaths) -> Result<()> {
    remove_file_if_exists(&paths.progress_db_path)?;
    remove_file_if_exists(&paths.progress_state_path)?;
    Ok(())
}

fn load_existing_progress(
    paths: &OutputPaths,
) -> Result<(Option<ProgramHistoryGapFillOutput>, Option<String>)> {
    let state_exists = paths.progress_state_path.exists();
    let db_exists = paths.progress_db_path.exists();
    if !state_exists && !db_exists {
        return Ok((None, None));
    }
    if !state_exists || !db_exists {
        clear_progress_artifacts(paths)?;
        return Ok((
            None,
            Some("program_history_gap_fill_progress_reset_orphaned_state".to_string()),
        ));
    }
    let raw_state = fs::read_to_string(&paths.progress_state_path)
        .with_context(|| format!("failed reading {}", paths.progress_state_path.display()))?;
    match serde_json::from_str::<ProgramHistoryGapFillOutput>(&raw_state) {
        Ok(progress) => Ok((Some(progress), None)),
        Err(_) => {
            clear_progress_artifacts(paths)?;
            Ok((
                None,
                Some("program_history_gap_fill_progress_reset_unreadable_state".to_string()),
            ))
        }
    }
}

fn progress_matches_preflight(
    progress: &ProgramHistoryGapFillOutput,
    source_kind: &str,
    target_program_ids: &[String],
    plan: &GapFillPlan,
) -> bool {
    !progress.replayable_output
        && progress.source_kind == source_kind
        && progress.target_program_ids == target_program_ids
        && progress.requested_window_start == plan.requested_window_start
        && progress.requested_window_end == plan.requested_window_end
        && progress.required_window_start == plan.required_window_start
}

fn resume_progress_with_bounds(
    paths: &OutputPaths,
    progress: Option<ProgramHistoryGapFillOutput>,
    bounds: &ResolvedSlotBounds,
) -> Result<(Option<ProgramHistoryGapFillOutput>, Option<String>)> {
    let Some(progress) = progress else {
        return Ok((None, None));
    };
    let expected_progress_db_path = paths.progress_db_path.display().to_string();
    let compatible = !progress.replayable_output
        && progress.resolved_start_slot == bounds.start_slot()
        && progress.resolved_end_slot == bounds.end_slot()
        && progress.next_batch_start_slot.is_some()
        && progress
            .progress_db_path
            .as_ref()
            .map(|value| value == &expected_progress_db_path)
            .unwrap_or_else(|| progress.output_db_path == expected_progress_db_path);

    if compatible {
        Ok((Some(progress), None))
    } else {
        clear_progress_artifacts(paths)?;
        Ok((
            None,
            Some("program_history_gap_fill_progress_reset_incompatible_state".to_string()),
        ))
    }
}

fn bounds_from_progress(progress: &ProgramHistoryGapFillOutput) -> Option<ResolvedSlotBounds> {
    Some(ResolvedSlotBounds {
        start: SlotBoundaryResolution {
            requested_slot_raw: progress.requested_start_slot_raw,
            resolved_slot: progress.resolved_start_slot,
            resolved_block_time: None,
            adjustment_kind: parse_boundary_adjustment_kind(&progress.start_slot_adjustment_kind),
            adjustment_distance: progress.start_slot_adjustment_distance,
        },
        end: SlotBoundaryResolution {
            requested_slot_raw: progress.requested_end_slot_raw,
            resolved_slot: progress.resolved_end_slot,
            resolved_block_time: None,
            adjustment_kind: parse_boundary_adjustment_kind(&progress.end_slot_adjustment_kind),
            adjustment_distance: progress.end_slot_adjustment_distance,
        },
        boundary_resolution_verdict: parse_boundary_resolution_verdict(
            &progress.boundary_resolution_verdict,
        ),
        requested_interval_fully_bracketed_after_adjustment: progress
            .requested_interval_fully_bracketed_after_adjustment,
        requested_interval_partially_bracketed_after_adjustment: progress
            .requested_interval_partially_bracketed_after_adjustment,
        boundary_missing_segments: persisted_explicit_missing_segments(progress),
    })
}

fn persisted_explicit_missing_segments(
    progress: &ProgramHistoryGapFillOutput,
) -> Vec<GapFillMissingSegment> {
    progress
        .missing_segments
        .iter()
        .filter(|segment| should_persist_explicit_missing_segment_reason(&segment.reason))
        .cloned()
        .collect()
}

fn should_persist_explicit_missing_segment_reason(reason: &str) -> bool {
    reason == "requested_window_prefix_uncovered_after_start_slot_adjustment"
        || reason == "requested_window_suffix_uncovered_after_end_slot_adjustment"
        || reason == ZERO_PROGRESS_ESCAPED_SLOT_MISSING_REASON
}

fn merge_missing_segments(
    segments: &mut Vec<GapFillMissingSegment>,
    extras: impl IntoIterator<Item = GapFillMissingSegment>,
) {
    for extra in extras {
        if !segments.iter().any(|segment| segment == &extra) {
            segments.push(extra);
        }
    }
}

fn zero_progress_escape_missing_segment<S: ProgramHistorySource>(
    source: &S,
    blocked_slot: u64,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
) -> GapFillMissingSegment {
    let avg_slot = ChronoDuration::milliseconds(AVG_SLOT_MS.round() as i64);
    let slot_time = source.block_time(blocked_slot).ok().flatten();
    let next_slot_time = blocked_slot
        .checked_add(1)
        .and_then(|slot| source.block_time(slot).ok().flatten());
    let previous_slot_time = blocked_slot
        .checked_sub(1)
        .and_then(|slot| source.block_time(slot).ok().flatten());

    let mut start = slot_time
        .or_else(|| previous_slot_time.map(|ts| ts + avg_slot))
        .unwrap_or(requested_window_start);
    let mut end = next_slot_time
        .or_else(|| slot_time.map(|ts| ts + avg_slot))
        .unwrap_or(start);

    if start < requested_window_start {
        start = requested_window_start;
    }
    if start > requested_window_end {
        start = requested_window_end;
    }
    if end < start {
        end = start;
    }
    if end > requested_window_end {
        end = requested_window_end;
    }

    GapFillMissingSegment {
        start,
        end,
        reason: ZERO_PROGRESS_ESCAPED_SLOT_MISSING_REASON.to_string(),
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
        block_fetch_concurrency: config.block_fetch_concurrency_override.unwrap_or(
            loaded_config
                .program_history_gap_fill
                .block_fetch_concurrency,
        ),
        max_slot_batches_per_attempt: config.max_slot_batches_per_attempt_override.unwrap_or(
            loaded_config
                .program_history_gap_fill
                .max_slot_batches_per_attempt,
        ),
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

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn dominant_phase_name(
    resolve_slot_bounds_ms: u64,
    block_list_ms: u64,
    block_fetch_ms: u64,
    candidate_filter_ms: u64,
    swap_parse_ms: u64,
    sqlite_stage_ms: u64,
) -> String {
    [
        ("resolve_slot_bounds", resolve_slot_bounds_ms),
        ("block_listing", block_list_ms),
        ("block_fetch", block_fetch_ms),
        ("candidate_filter", candidate_filter_ms),
        ("swap_parse", swap_parse_ms),
        ("sqlite_stage", sqlite_stage_ms),
    ]
    .into_iter()
    .max_by_key(|(_, value)| *value)
    .map(|(name, value)| {
        if value == 0 {
            "idle".to_string()
        } else {
            name.to_string()
        }
    })
    .unwrap_or_else(|| "idle".to_string())
}

fn fetch_program_history_gap_fill<S: ProgramHistorySource + Sync>(
    source: &S,
    program_ids: &ProgramIdConfig,
    target_program_ids: &[String],
    plan: &GapFillPlan,
    settings: ScanSettings,
    max_requests_per_second: usize,
    retry_429_max_attempts: usize,
    retry_429_backoff_ms: u64,
    output_paths: &OutputPaths,
    now: DateTime<Utc>,
) -> Result<FetchResult> {
    let (existing_progress, mut progress_reset_reason) = load_existing_progress(output_paths)?;
    let preflight_progress = match existing_progress {
        Some(progress)
            if progress_matches_preflight(
                &progress,
                source.source_kind(),
                target_program_ids,
                plan,
            ) =>
        {
            Some(progress)
        }
        Some(_) => {
            clear_progress_artifacts(output_paths)?;
            progress_reset_reason =
                Some("program_history_gap_fill_progress_reset_incompatible_state".to_string());
            None
        }
        None => None,
    };
    let resolve_slot_bounds_started_at = Instant::now();
    let (bounds, resolved_bounds_reused_from_progress) =
        if let Some(progress) = preflight_progress.as_ref() {
            match bounds_from_progress(progress) {
                Some(bounds)
                    if bounds.usable_slot_range().is_some() =>
                {
                    (bounds, true)
                }
                _ => (
                    resolve_slot_bounds(
                        source,
                        plan.requested_window_start,
                        plan.requested_window_end,
                        settings.block_time_probe_slots,
                    )?,
                    false,
                ),
            }
        } else {
            (
                resolve_slot_bounds(
                    source,
                    plan.requested_window_start,
                    plan.requested_window_end,
                    settings.block_time_probe_slots,
                )?,
                false,
            )
        };
    let resolve_slot_bounds_ms = if resolved_bounds_reused_from_progress {
        0
    } else {
        elapsed_ms(resolve_slot_bounds_started_at)
    };
    let (resume_progress, bounds_progress_reset_reason) =
        resume_progress_with_bounds(output_paths, preflight_progress, &bounds)?;
    if bounds_progress_reset_reason.is_some() {
        progress_reset_reason = bounds_progress_reset_reason;
    }
    let (start_slot, end_slot) = match bounds.usable_slot_range() {
        Some(range) => range,
        None => {
            return Ok(FetchResult {
                requested_start_slot_raw: bounds.requested_start_slot_raw(),
                requested_end_slot_raw: bounds.requested_end_slot_raw(),
                resolved_start_slot: bounds.start_slot(),
                resolved_end_slot: bounds.end_slot(),
                start_slot_adjustment_kind: bounds.start.adjustment_kind.as_str().to_string(),
                end_slot_adjustment_kind: bounds.end.adjustment_kind.as_str().to_string(),
                start_slot_adjustment_distance: bounds.start.adjustment_distance,
                end_slot_adjustment_distance: bounds.end.adjustment_distance,
                boundary_resolution_verdict: bounds.boundary_resolution_verdict.as_str().to_string(),
                requested_interval_fully_bracketed_after_adjustment: bounds
                    .requested_interval_fully_bracketed_after_adjustment,
                requested_interval_partially_bracketed_after_adjustment: bounds
                    .requested_interval_partially_bracketed_after_adjustment,
                slot_span: None,
                boundary_missing_segments: bounds.boundary_missing_segments.clone(),
                resolved_bounds_reused_from_progress,
                coverage_method: "unresolved".to_string(),
                block_fetch_encoding: "json".to_string(),
                scan_budget_slots: settings.max_slots_to_scan,
                budget_exhausted: false,
                phase_b_like_cost_budget_exhausted: false,
                sampling_segments: settings.sampling_segments,
                sampling_window_slots: 0,
                block_batch_size: settings.block_batch_size,
                block_fetch_concurrency: settings.block_fetch_concurrency,
                max_slot_batches_per_attempt: settings.max_slot_batches_per_attempt,
                max_blocks_to_fetch: settings.max_blocks_to_fetch,
                max_candidate_transactions_to_parse: settings.max_candidate_transactions_to_parse,
                max_requests_per_second,
                retry_429_max_attempts,
                retry_429_backoff_ms,
                current_phase: "source_contract_failed".to_string(),
                dominant_phase: "source_contract_failed".to_string(),
                resolve_slot_bounds_ms,
                attempt_number: 1,
                cumulative_across_attempts: false,
                attempt_frontier_start_slot: None,
                attempt_frontier_end_slot: None,
                attempt_frontier_advanced_slots: 0,
                next_batch_start_slot: None,
                zero_progress_retry_count: 0,
                zero_progress_blocked_start_slot: None,
                zero_progress_escape_applied: false,
                zero_progress_escape_distance: None,
                progress_reset_reason,
                attempt_budget_exhausted: false,
                attempt_block_list_ms: 0,
                attempt_block_fetch_ms: 0,
                attempt_candidate_filter_ms: 0,
                attempt_swap_parse_ms: 0,
                attempt_sqlite_stage_ms: 0,
                attempt_scanned_batches: 0,
                attempt_scanned_slots: 0,
                attempt_listed_block_slots: 0,
                attempt_scanned_blocks: 0,
                attempt_scanned_transactions: 0,
                attempt_candidate_program_transactions: 0,
                attempt_parsed_candidate_transactions: 0,
                attempt_parsed_candidate_swaps: 0,
                scanned_batches: 0,
                scanned_slots: 0,
                listed_block_slots: 0,
                scanned_blocks: 0,
                scanned_transactions: 0,
                candidate_program_transactions: 0,
                parsed_candidate_transactions: 0,
                parsed_candidate_swaps: 0,
                attempt_inserted_rows: 0,
                staged_rows: 0,
                gap_fill_covered_since: None,
                gap_fill_covered_through_cursor: None,
                verdict: GapFillVerdict::NonViableSourceContract,
                reason: format!(
                    "slot_bounds_unresolved:{}",
                    bounds.failure_reason()
                ),
                early_stop_reason: None,
            });
        }
    };
    let slot_span = end_slot.saturating_sub(start_slot).saturating_add(1);
    let scan_plan = build_scan_plan(start_slot, end_slot, settings);
    if scan_plan.budget_exhausted {
        let sampled_scan = scan_slot_windows(
            source,
            program_ids,
            plan.requested_window_start,
            plan.requested_window_end,
            &scan_plan.windows,
            settings,
        );
        let (summary, verdict, reason) = match sampled_scan {
            Ok(summary) => {
                let (verdict, reason) = classify_fetch_outcome(
                    plan,
                    scan_plan.budget_exhausted,
                    summary.phase_b_like_cost_budget_exhausted,
                    summary.parsed_candidate_swaps,
                );
                (summary, verdict, reason)
            }
            Err(error) => {
                let (verdict, reason) =
                    classify_source_error("program_history_gap_fill_sampled_scan_failed", &error);
                (ScanSummary::default(), verdict, reason)
            }
        };
        return Ok(FetchResult {
            requested_start_slot_raw: bounds.requested_start_slot_raw(),
            requested_end_slot_raw: bounds.requested_end_slot_raw(),
            resolved_start_slot: Some(start_slot),
            resolved_end_slot: Some(end_slot),
            start_slot_adjustment_kind: bounds.start.adjustment_kind.as_str().to_string(),
            end_slot_adjustment_kind: bounds.end.adjustment_kind.as_str().to_string(),
            start_slot_adjustment_distance: bounds.start.adjustment_distance,
            end_slot_adjustment_distance: bounds.end.adjustment_distance,
            boundary_resolution_verdict: bounds.boundary_resolution_verdict.as_str().to_string(),
            requested_interval_fully_bracketed_after_adjustment: bounds
                .requested_interval_fully_bracketed_after_adjustment,
            requested_interval_partially_bracketed_after_adjustment: bounds
                .requested_interval_partially_bracketed_after_adjustment,
            slot_span: Some(slot_span),
            boundary_missing_segments: bounds.boundary_missing_segments.clone(),
            resolved_bounds_reused_from_progress,
            coverage_method: scan_plan.coverage_method.to_string(),
            block_fetch_encoding: "json".to_string(),
            scan_budget_slots: scan_plan.scan_budget_slots,
            budget_exhausted: scan_plan.budget_exhausted,
            phase_b_like_cost_budget_exhausted: summary.phase_b_like_cost_budget_exhausted,
            sampling_segments: scan_plan.sampling_segments,
            sampling_window_slots: scan_plan.sampling_window_slots,
            block_batch_size: settings.block_batch_size,
            block_fetch_concurrency: settings.block_fetch_concurrency,
            max_slot_batches_per_attempt: settings.max_slot_batches_per_attempt,
            max_blocks_to_fetch: settings.max_blocks_to_fetch,
            max_candidate_transactions_to_parse: settings.max_candidate_transactions_to_parse,
            max_requests_per_second,
            retry_429_max_attempts,
            retry_429_backoff_ms,
            current_phase: "scan_budget_exhausted".to_string(),
            dominant_phase: dominant_phase_name(
                resolve_slot_bounds_ms,
                summary.block_list_ms,
                summary.block_fetch_ms,
                summary.candidate_filter_ms,
                summary.swap_parse_ms,
                0,
            ),
            resolve_slot_bounds_ms,
            attempt_number: 1,
            cumulative_across_attempts: false,
            attempt_frontier_start_slot: Some(start_slot),
            attempt_frontier_end_slot: summary.frontier_end_slot,
            attempt_frontier_advanced_slots: summary
                .frontier_end_slot
                .and_then(|end_slot| {
                    end_slot
                        .checked_sub(start_slot)
                        .map(|delta| delta as usize + 1)
            })
            .unwrap_or(0),
            next_batch_start_slot: None,
            zero_progress_retry_count: 0,
            zero_progress_blocked_start_slot: None,
            zero_progress_escape_applied: false,
            zero_progress_escape_distance: None,
            progress_reset_reason,
            attempt_budget_exhausted: false,
            attempt_block_list_ms: summary.block_list_ms,
            attempt_block_fetch_ms: summary.block_fetch_ms,
            attempt_candidate_filter_ms: summary.candidate_filter_ms,
            attempt_swap_parse_ms: summary.swap_parse_ms,
            attempt_sqlite_stage_ms: 0,
            attempt_scanned_batches: summary.scanned_batches,
            attempt_scanned_slots: summary.scanned_slots,
            attempt_listed_block_slots: summary.listed_block_slots,
            attempt_scanned_blocks: summary.scanned_blocks,
            attempt_scanned_transactions: summary.scanned_transactions,
            attempt_candidate_program_transactions: summary.candidate_program_transactions,
            attempt_parsed_candidate_transactions: summary.parsed_candidate_transactions,
            attempt_parsed_candidate_swaps: summary.parsed_candidate_swaps,
            scanned_batches: summary.scanned_batches,
            scanned_slots: summary.scanned_slots,
            listed_block_slots: summary.listed_block_slots,
            scanned_blocks: summary.scanned_blocks,
            scanned_transactions: summary.scanned_transactions,
            candidate_program_transactions: summary.candidate_program_transactions,
            parsed_candidate_transactions: summary.parsed_candidate_transactions,
            parsed_candidate_swaps: summary.parsed_candidate_swaps,
            attempt_inserted_rows: 0,
            staged_rows: 0,
            gap_fill_covered_since: None,
            gap_fill_covered_through_cursor: None,
            verdict,
            reason,
            early_stop_reason: summary.early_stop_reason,
        });
    }

    let attempt_number = resume_progress
        .as_ref()
        .map_or(1usize, |output| output.attempt_number.saturating_add(1));
    let frontier_start_slot = resume_progress
        .as_ref()
        .and_then(|output| output.next_batch_start_slot)
        .unwrap_or(start_slot);

    let attempt = if frontier_start_slot > end_slot {
        AttemptScanResult {
            summary: ScanSummary::default(),
            next_slot_to_scan: None,
            attempt_budget_exhausted: false,
            source_error: None,
        }
    } else {
        scan_slot_range_attempt(
            source,
            program_ids,
            plan.requested_window_start,
            plan.requested_window_end,
            frontier_start_slot,
            end_slot,
            settings,
        )?
    };

    let mut attempt_swaps = attempt
        .summary
        .swaps_by_signature
        .into_values()
        .collect::<Vec<_>>();
    attempt_swaps.sort_by(|left, right| {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    });

    let progress_store = SqliteStore::open(&output_paths.progress_db_path).with_context(|| {
        format!(
            "failed opening program-history gap-fill progress db {}",
            output_paths.progress_db_path.display()
        )
    })?;
    progress_store.ensure_recent_raw_journal_tables()?;
    let sqlite_stage_started_at = Instant::now();
    let attempt_inserted_rows = progress_store
        .insert_recent_raw_journal_batch(&attempt_swaps, now)?
        .inserted_rows;
    let attempt_sqlite_stage_ms = elapsed_ms(sqlite_stage_started_at);
    let progress_state = progress_store.recent_raw_journal_state_read_only()?;

    let previous_scanned_batches = resume_progress
        .as_ref()
        .map_or(0, |output| output.scanned_batches);
    let previous_scanned_slots = resume_progress
        .as_ref()
        .map_or(0, |output| output.scanned_slots);
    let previous_listed_block_slots = resume_progress
        .as_ref()
        .map_or(0, |output| output.listed_block_slots);
    let previous_scanned_blocks = resume_progress
        .as_ref()
        .map_or(0, |output| output.scanned_blocks);
    let previous_scanned_transactions = resume_progress
        .as_ref()
        .map_or(0, |output| output.scanned_transactions);
    let previous_candidate_program_transactions = resume_progress
        .as_ref()
        .map_or(0, |output| output.candidate_program_transactions);
    let previous_parsed_candidate_transactions = resume_progress
        .as_ref()
        .map_or(0, |output| output.parsed_candidate_transactions);
    let previous_parsed_candidate_swaps = resume_progress
        .as_ref()
        .map_or(0, |output| output.parsed_candidate_swaps);
    let previous_zero_progress_retry_count = resume_progress
        .as_ref()
        .map_or(0, |output| output.zero_progress_retry_count);
    let previous_zero_progress_blocked_start_slot = resume_progress
        .as_ref()
        .and_then(|output| output.zero_progress_blocked_start_slot);

    let scanned_batches = previous_scanned_batches.saturating_add(attempt.summary.scanned_batches);
    let scanned_slots = previous_scanned_slots.saturating_add(attempt.summary.scanned_slots);
    let listed_block_slots =
        previous_listed_block_slots.saturating_add(attempt.summary.listed_block_slots);
    let scanned_blocks = previous_scanned_blocks.saturating_add(attempt.summary.scanned_blocks);
    let scanned_transactions =
        previous_scanned_transactions.saturating_add(attempt.summary.scanned_transactions);
    let candidate_program_transactions = previous_candidate_program_transactions
        .saturating_add(attempt.summary.candidate_program_transactions);
    let parsed_candidate_transactions = previous_parsed_candidate_transactions
        .saturating_add(attempt.summary.parsed_candidate_transactions);
    let parsed_candidate_swaps =
        previous_parsed_candidate_swaps.saturating_add(attempt.summary.parsed_candidate_swaps);
    let attempt_frontier_advanced_slots = attempt
        .summary
        .frontier_end_slot
        .and_then(|end_slot| {
            end_slot
                .checked_sub(frontier_start_slot)
                .map(|delta| delta as usize + 1)
        })
        .unwrap_or(0);
    let mut boundary_missing_segments = bounds.boundary_missing_segments.clone();
    if let Some(progress) = resume_progress.as_ref() {
        merge_missing_segments(
            &mut boundary_missing_segments,
            persisted_explicit_missing_segments(progress),
        );
    }

    let zero_progress_source_error = attempt.source_error.as_ref().filter(|source_error| {
        matches!(
            source_error.kind,
            SourceErrorKind::ProviderThrottled | SourceErrorKind::RetryableProviderFailure
        )
    });
    let zero_progress_blocked_start_slot = zero_progress_source_error
        .and_then(|_| attempt.next_slot_to_scan)
        .filter(|slot| *slot == frontier_start_slot)
        .filter(|_| attempt_frontier_advanced_slots == 0);
    let mut zero_progress_retry_count = zero_progress_blocked_start_slot
        .map(|blocked_slot| {
            if previous_zero_progress_blocked_start_slot == Some(blocked_slot) {
                previous_zero_progress_retry_count.saturating_add(1)
            } else {
                1
            }
        })
        .unwrap_or(0);
    let mut zero_progress_escape_applied = false;
    let mut zero_progress_escape_distance = None;

    let (verdict, reason, current_phase, next_batch_start_slot) =
        if let Some(source_error) = attempt.source_error.as_ref() {
            let blocked_slot = zero_progress_blocked_start_slot;
            let escape_applies = blocked_slot.is_some()
                && zero_progress_retry_count >= ZERO_PROGRESS_ESCAPE_THRESHOLD;
            if escape_applies {
                let blocked_slot = blocked_slot.expect("blocked slot present when escape applies");
                zero_progress_escape_applied = true;
                zero_progress_escape_distance = Some(ZERO_PROGRESS_ESCAPE_DISTANCE);
                let escaped_next_slot = blocked_slot.saturating_add(ZERO_PROGRESS_ESCAPE_DISTANCE);
                merge_missing_segments(
                    &mut boundary_missing_segments,
                    [zero_progress_escape_missing_segment(
                        source,
                        blocked_slot,
                        plan.requested_window_start,
                        plan.requested_window_end,
                    )],
                );
                (
                    match source_error.kind {
                        SourceErrorKind::ProviderThrottled => {
                            GapFillVerdict::NotProvenDueToProviderThrottling
                        }
                        SourceErrorKind::RetryableProviderFailure => {
                            GapFillVerdict::NotProvenDueToRetryableProviderFailure
                        }
                        SourceErrorKind::SourceContractFailure => {
                            GapFillVerdict::NonViableSourceContract
                        }
                    },
                    format!(
                        "program_history_gap_fill_zero_progress_blocked_slot_skipped_after_bounded_retries:slot={blocked_slot}:retry_count={zero_progress_retry_count}"
                    ),
                    "awaiting_next_attempt".to_string(),
                    Some(escaped_next_slot),
                )
            } else {
                match source_error.kind {
                SourceErrorKind::ProviderThrottled => (
                    GapFillVerdict::NotProvenDueToProviderThrottling,
                    format!(
                        "program_history_gap_fill_provider_throttled:{}",
                        source_error
                    ),
                    "awaiting_next_attempt".to_string(),
                    Some(
                        attempt
                            .next_slot_to_scan
                            .unwrap_or(frontier_start_slot.min(end_slot)),
                    ),
                ),
                SourceErrorKind::RetryableProviderFailure => (
                    GapFillVerdict::NotProvenDueToRetryableProviderFailure,
                    source_error.message.clone(),
                    "awaiting_next_attempt".to_string(),
                    Some(
                        attempt
                            .next_slot_to_scan
                            .unwrap_or(frontier_start_slot.min(end_slot)),
                    ),
                ),
                SourceErrorKind::SourceContractFailure => (
                    GapFillVerdict::NonViableSourceContract,
                    format!(
                        "program_history_gap_fill_source_contract_failed:{}",
                        source_error
                    ),
                    "source_contract_failed".to_string(),
                    Some(
                        attempt
                            .next_slot_to_scan
                            .unwrap_or(frontier_start_slot.min(end_slot)),
                    ),
                ),
                }
            }
        } else if attempt.summary.phase_b_like_cost_budget_exhausted {
            zero_progress_retry_count = 0;
            (
                GapFillVerdict::NotProvenDueToCostBudget,
                "program_history_gap_fill_phase_b_like_cost_budget_exhausted".to_string(),
                "awaiting_next_attempt".to_string(),
                Some(
                    attempt
                        .next_slot_to_scan
                        .unwrap_or(frontier_start_slot.min(end_slot)),
                ),
            )
        } else if attempt.attempt_budget_exhausted {
            zero_progress_retry_count = 0;
            (
                GapFillVerdict::NotProvenDueToAttemptBudget,
                "program_history_gap_fill_attempt_budget_exhausted".to_string(),
                "awaiting_next_attempt".to_string(),
                Some(
                    attempt
                        .next_slot_to_scan
                        .unwrap_or(frontier_start_slot.min(end_slot)),
                ),
            )
        } else {
            zero_progress_retry_count = 0;
            let has_persisted_blocked_slot_gap = boundary_missing_segments
                .iter()
                .any(|segment| segment.reason == ZERO_PROGRESS_ESCAPED_SLOT_MISSING_REASON);
            let (verdict, reason, current_phase, next_batch_start_slot) =
                if has_persisted_blocked_slot_gap
                    && (frontier_start_slot > end_slot
                        || attempt.summary.frontier_end_slot == Some(end_slot))
                {
                    (
                        GapFillVerdict::NotProvenDueToProviderThrottling,
                        "program_history_gap_fill_incomplete_due_to_persistently_blocked_slot_gap"
                            .to_string(),
                        "completed_with_explicit_missing_segments".to_string(),
                        Some(end_slot.saturating_add(1)),
                    )
                } else {
                    let (verdict, reason) =
                        classify_fetch_outcome(plan, false, false, progress_state.row_count);
                    (
                        verdict,
                        reason,
                        if frontier_start_slot > end_slot {
                            "publishing_output".to_string()
                        } else {
                            "completed".to_string()
                        },
                        None,
                    )
                };
            (
                verdict,
                reason,
                current_phase,
                next_batch_start_slot,
            )
        };
    let dominant_phase = dominant_phase_name(
        resolve_slot_bounds_ms,
        attempt.summary.block_list_ms,
        attempt.summary.block_fetch_ms,
        attempt.summary.candidate_filter_ms,
        attempt.summary.swap_parse_ms,
        attempt_sqlite_stage_ms,
    );

    Ok(FetchResult {
        requested_start_slot_raw: bounds.requested_start_slot_raw(),
        requested_end_slot_raw: bounds.requested_end_slot_raw(),
        resolved_start_slot: Some(start_slot),
        resolved_end_slot: Some(end_slot),
        start_slot_adjustment_kind: bounds.start.adjustment_kind.as_str().to_string(),
        end_slot_adjustment_kind: bounds.end.adjustment_kind.as_str().to_string(),
        start_slot_adjustment_distance: bounds.start.adjustment_distance,
        end_slot_adjustment_distance: bounds.end.adjustment_distance,
        boundary_resolution_verdict: bounds.boundary_resolution_verdict.as_str().to_string(),
        requested_interval_fully_bracketed_after_adjustment: bounds
            .requested_interval_fully_bracketed_after_adjustment,
        requested_interval_partially_bracketed_after_adjustment: bounds
            .requested_interval_partially_bracketed_after_adjustment,
        slot_span: Some(slot_span),
        boundary_missing_segments,
        resolved_bounds_reused_from_progress,
        coverage_method: scan_plan.coverage_method.to_string(),
        block_fetch_encoding: "json".to_string(),
        scan_budget_slots: scan_plan.scan_budget_slots,
        budget_exhausted: false,
        phase_b_like_cost_budget_exhausted: attempt.summary.phase_b_like_cost_budget_exhausted,
        sampling_segments: scan_plan.sampling_segments,
        sampling_window_slots: scan_plan.sampling_window_slots,
        block_batch_size: settings.block_batch_size,
        block_fetch_concurrency: settings.block_fetch_concurrency,
        max_slot_batches_per_attempt: settings.max_slot_batches_per_attempt,
        max_blocks_to_fetch: settings.max_blocks_to_fetch,
        max_candidate_transactions_to_parse: settings.max_candidate_transactions_to_parse,
        max_requests_per_second,
        retry_429_max_attempts,
        retry_429_backoff_ms,
        current_phase,
        dominant_phase,
        resolve_slot_bounds_ms,
        attempt_number,
        cumulative_across_attempts: attempt_number > 1,
        attempt_frontier_start_slot: if frontier_start_slot > end_slot {
            None
        } else {
            Some(frontier_start_slot)
        },
        attempt_frontier_end_slot: attempt.summary.frontier_end_slot,
        attempt_frontier_advanced_slots,
        next_batch_start_slot,
        zero_progress_retry_count,
        zero_progress_blocked_start_slot,
        zero_progress_escape_applied,
        zero_progress_escape_distance,
        progress_reset_reason,
        attempt_budget_exhausted: attempt.attempt_budget_exhausted,
        attempt_block_list_ms: attempt.summary.block_list_ms,
        attempt_block_fetch_ms: attempt.summary.block_fetch_ms,
        attempt_candidate_filter_ms: attempt.summary.candidate_filter_ms,
        attempt_swap_parse_ms: attempt.summary.swap_parse_ms,
        attempt_sqlite_stage_ms,
        attempt_scanned_batches: attempt.summary.scanned_batches,
        attempt_scanned_slots: attempt.summary.scanned_slots,
        attempt_listed_block_slots: attempt.summary.listed_block_slots,
        attempt_scanned_blocks: attempt.summary.scanned_blocks,
        attempt_scanned_transactions: attempt.summary.scanned_transactions,
        attempt_candidate_program_transactions: attempt.summary.candidate_program_transactions,
        attempt_parsed_candidate_transactions: attempt.summary.parsed_candidate_transactions,
        attempt_parsed_candidate_swaps: attempt.summary.parsed_candidate_swaps,
        scanned_batches,
        scanned_slots,
        listed_block_slots,
        scanned_blocks,
        scanned_transactions,
        candidate_program_transactions,
        parsed_candidate_transactions,
        parsed_candidate_swaps,
        attempt_inserted_rows,
        staged_rows: progress_state.row_count,
        gap_fill_covered_since: progress_state.covered_since,
        gap_fill_covered_through_cursor: progress_state.covered_through_cursor.clone(),
        verdict,
        reason,
        early_stop_reason: attempt.summary.early_stop_reason,
    })
}

fn classify_fetch_outcome(
    plan: &GapFillPlan,
    budget_exhausted: bool,
    cost_budget_exhausted: bool,
    replayable_row_count: usize,
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
    if replayable_row_count == 0 {
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

fn build_scan_plan(start_slot: u64, end_slot: u64, settings: ScanSettings) -> ScanPlan {
    let scan_budget_slots = settings.max_slots_to_scan.max(1);
    let slot_span = end_slot.saturating_sub(start_slot).saturating_add(1);
    if slot_span <= scan_budget_slots as u64 {
        return ScanPlan {
            coverage_method: "program_history_full_slot_scan",
            scan_budget_slots,
            budget_exhausted: false,
            sampling_segments: 0,
            sampling_window_slots: 0,
            windows: vec![SlotScanWindow {
                start_slot,
                end_slot,
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
        let start_slot = start_slot.saturating_add(offset);
        let end_slot = end_slot.min(start_slot.saturating_add(window_slots.saturating_sub(1)));
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
    let start_requested_slot_raw = estimate_requested_slot_raw(
        source,
        latest_anchor_slot,
        latest_anchor_time,
        window_start,
        probe_slots,
    )?;
    let end_requested_slot_raw = estimate_requested_slot_raw(
        source,
        latest_anchor_slot,
        latest_anchor_time,
        window_end,
        probe_slots,
    )?;
    let start = resolve_boundary_slot(
        source,
        BoundarySide::Start,
        window_start,
        start_requested_slot_raw,
        probe_slots,
    )?;
    let end = resolve_boundary_slot(
        source,
        BoundarySide::End,
        window_end,
        end_requested_slot_raw,
        probe_slots,
    )?;
    Ok(finalize_resolved_slot_bounds(window_start, window_end, start, end))
}

fn estimate_requested_slot_raw<S: ProgramHistorySource>(
    source: &S,
    latest_anchor_slot: u64,
    latest_anchor_time: DateTime<Utc>,
    target: DateTime<Utc>,
    probe_slots: u64,
) -> Result<Option<u64>> {
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
        let (anchor_slot, anchor_time) = match nearest_slot_with_time(source, guess, probe_slots) {
            Ok(anchor) => anchor,
            Err(error) if source_error_kind(&error).is_none() => break,
            Err(error) => return Err(error),
        };
        let anchor_diff_ms = anchor_time.signed_duration_since(target).num_milliseconds();
        if anchor_diff_ms.abs() <= 30_000 {
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
    Ok(Some(guess))
}

fn resolve_boundary_slot<S: ProgramHistorySource>(
    source: &S,
    side: BoundarySide,
    target: DateTime<Utc>,
    requested_slot_raw: Option<u64>,
    probe_slots: u64,
) -> Result<SlotBoundaryResolution> {
    let Some(raw_slot) = requested_slot_raw else {
        return Ok(SlotBoundaryResolution::unresolved(None));
    };

    if let Some(block_time) = source.block_time(raw_slot)? {
        return Ok(SlotBoundaryResolution {
            requested_slot_raw: Some(raw_slot),
            resolved_slot: Some(raw_slot),
            resolved_block_time: Some(block_time),
            adjustment_kind: BoundaryAdjustmentKind::Exact,
            adjustment_distance: Some(0),
        });
    }

    let mut previous: Option<(u64, DateTime<Utc>, u64)> = None;
    let mut next: Option<(u64, DateTime<Utc>, u64)> = None;
    for distance in 1..=probe_slots {
        if previous.is_none() {
            let slot = raw_slot.saturating_sub(distance);
            if let Some(block_time) = source.block_time(slot)? {
                previous = Some((slot, block_time, distance));
            }
        }
        if next.is_none() {
            let slot = raw_slot.saturating_add(distance);
            if let Some(block_time) = source.block_time(slot)? {
                next = Some((slot, block_time, distance));
            }
        }
        if previous.is_some() && next.is_some() {
            break;
        }
    }

    let candidate = match side {
        BoundarySide::Start => previous
            .filter(|(_, block_time, _)| *block_time <= target)
            .map(|(slot, block_time, distance)| {
                (
                    slot,
                    block_time,
                    BoundaryAdjustmentKind::RewoundToPreviousAvailable,
                    distance,
                )
            })
            .or_else(|| {
                next.map(|(slot, block_time, distance)| {
                    (
                        slot,
                        block_time,
                        BoundaryAdjustmentKind::AdvancedToNextAvailable,
                        distance,
                    )
                })
            })
            .or_else(|| {
                previous.map(|(slot, block_time, distance)| {
                    (
                        slot,
                        block_time,
                        BoundaryAdjustmentKind::RewoundToPreviousAvailable,
                        distance,
                    )
                })
            }),
        BoundarySide::End => next
            .filter(|(_, block_time, _)| *block_time >= target)
            .map(|(slot, block_time, distance)| {
                (
                    slot,
                    block_time,
                    BoundaryAdjustmentKind::AdvancedToNextAvailable,
                    distance,
                )
            })
            .or_else(|| {
                previous.map(|(slot, block_time, distance)| {
                    (
                        slot,
                        block_time,
                        BoundaryAdjustmentKind::RewoundToPreviousAvailable,
                        distance,
                    )
                })
            })
            .or_else(|| {
                next.map(|(slot, block_time, distance)| {
                    (
                        slot,
                        block_time,
                        BoundaryAdjustmentKind::AdvancedToNextAvailable,
                        distance,
                    )
                })
            }),
    };

    Ok(match candidate {
        Some((slot, block_time, adjustment_kind, distance)) => SlotBoundaryResolution {
            requested_slot_raw: Some(raw_slot),
            resolved_slot: Some(slot),
            resolved_block_time: Some(block_time),
            adjustment_kind,
            adjustment_distance: Some(distance),
        },
        None => SlotBoundaryResolution::unresolved(Some(raw_slot)),
    })
}

fn finalize_resolved_slot_bounds(
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    start: SlotBoundaryResolution,
    end: SlotBoundaryResolution,
) -> ResolvedSlotBounds {
    let mut boundary_missing_segments = Vec::new();
    let (boundary_resolution_verdict, fully_bracketed, partially_bracketed) =
        match (
            start.resolved_slot,
            start.resolved_block_time,
            end.resolved_slot,
            end.resolved_block_time,
        ) {
            (Some(start_slot), Some(start_ts), Some(end_slot), Some(end_ts)) if end_slot >= start_slot => {
                if start_ts > window_start {
                    boundary_missing_segments.push(GapFillMissingSegment {
                        start: window_start,
                        end: start_ts.min(window_end),
                        reason: "requested_window_prefix_uncovered_after_start_slot_adjustment"
                            .to_string(),
                    });
                }
                if end_ts < window_end {
                    boundary_missing_segments.push(GapFillMissingSegment {
                        start: end_ts.max(window_start),
                        end: window_end,
                        reason: "requested_window_suffix_uncovered_after_end_slot_adjustment"
                            .to_string(),
                    });
                }
                let fully_bracketed = boundary_missing_segments.is_empty();
                (
                    if fully_bracketed {
                        BoundaryResolutionVerdict::FullyBracketed
                    } else {
                        BoundaryResolutionVerdict::PartiallyBracketed
                    },
                    fully_bracketed,
                    !fully_bracketed,
                )
            }
            _ => (BoundaryResolutionVerdict::Unresolved, false, false),
        };

    ResolvedSlotBounds {
        start,
        end,
        boundary_resolution_verdict,
        requested_interval_fully_bracketed_after_adjustment: fully_bracketed,
        requested_interval_partially_bracketed_after_adjustment: partially_bracketed,
        boundary_missing_segments,
    }
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

fn fetch_block_chunk<S: ProgramHistorySource + Sync>(
    source: &S,
    slots: &[u64],
) -> Result<Vec<(u64, Result<Option<Value>>)>> {
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(slots.len());
        for &slot in slots {
            handles.push((slot, scope.spawn(move || source.get_block(slot))));
        }
        handles
            .into_iter()
            .map(|(slot, handle)| {
                Ok((
                    slot,
                    handle
                        .join()
                        .map_err(|_| anyhow!("block fetch worker panicked"))?,
                ))
            })
            .collect::<Result<Vec<_>>>()
    })
}

fn scan_slot_windows<S: ProgramHistorySource + Sync>(
    source: &S,
    program_ids: &ProgramIdConfig,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    windows: &[SlotScanWindow],
    settings: ScanSettings,
) -> Result<ScanSummary> {
    let mut combined = ScanSummary::default();
    for window in windows {
        let attempt = scan_slot_range_attempt(
            source,
            program_ids,
            requested_window_start,
            requested_window_end,
            window.start_slot,
            window.end_slot,
            settings,
        )?;
        let attempt_budget_exhausted = attempt.attempt_budget_exhausted;
        let summary = attempt.summary;
        if summary.frontier_end_slot.is_some() {
            combined.frontier_end_slot = summary.frontier_end_slot;
        }
        combined.block_list_ms = combined.block_list_ms.saturating_add(summary.block_list_ms);
        combined.block_fetch_ms = combined
            .block_fetch_ms
            .saturating_add(summary.block_fetch_ms);
        combined.candidate_filter_ms = combined
            .candidate_filter_ms
            .saturating_add(summary.candidate_filter_ms);
        combined.swap_parse_ms = combined.swap_parse_ms.saturating_add(summary.swap_parse_ms);
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
        if combined.phase_b_like_cost_budget_exhausted || attempt_budget_exhausted {
            break;
        }
    }
    Ok(combined)
}

fn scan_slot_range_attempt<S: ProgramHistorySource + Sync>(
    source: &S,
    program_ids: &ProgramIdConfig,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    start_slot: u64,
    end_slot: u64,
    settings: ScanSettings,
) -> Result<AttemptScanResult> {
    let mut summary = ScanSummary::default();
    if end_slot < start_slot {
        return Ok(AttemptScanResult {
            summary,
            next_slot_to_scan: None,
            attempt_budget_exhausted: false,
            source_error: None,
        });
    }
    let batch_size = settings.block_batch_size.max(1) as u64;
    let mut batch_start = start_slot;
    while batch_start <= end_slot {
        if summary.scanned_batches >= settings.max_slot_batches_per_attempt {
            return Ok(AttemptScanResult {
                summary,
                next_slot_to_scan: Some(batch_start),
                attempt_budget_exhausted: true,
                source_error: None,
            });
        }
        let batch_end = end_slot.min(batch_start.saturating_add(batch_size.saturating_sub(1)));
        summary.scanned_batches = summary.scanned_batches.saturating_add(1);
        summary.scanned_slots = summary
            .scanned_slots
            .saturating_add(batch_end.saturating_sub(batch_start).saturating_add(1) as usize);
        let block_list_started_at = Instant::now();
        let available_block_slots = match source.list_blocks(batch_start, batch_end) {
            Ok(slots) => slots,
            Err(error) => {
                let source_error = error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<SourceError>())
                    .map(|cause| SourceError {
                        kind: cause.kind,
                        message: cause.message.clone(),
                    });
                summary.block_list_ms = summary
                    .block_list_ms
                    .saturating_add(elapsed_ms(block_list_started_at));
                return Ok(AttemptScanResult {
                    summary: ScanSummary {
                        frontier_end_slot: batch_start.checked_sub(1),
                        ..summary
                    },
                    next_slot_to_scan: Some(batch_start),
                    attempt_budget_exhausted: false,
                    source_error,
                });
            }
        };
        summary.block_list_ms = summary
            .block_list_ms
            .saturating_add(elapsed_ms(block_list_started_at));
        summary.frontier_end_slot = Some(batch_end);
        summary.listed_block_slots = summary
            .listed_block_slots
            .saturating_add(available_block_slots.len());
        let mut slot_index = 0usize;
        while slot_index < available_block_slots.len() {
            if summary.scanned_blocks >= settings.max_blocks_to_fetch {
                summary.phase_b_like_cost_budget_exhausted = true;
                summary.early_stop_reason =
                    Some("program_history_gap_fill_block_fetch_budget_exhausted".to_string());
                return Ok(AttemptScanResult {
                    summary,
                    next_slot_to_scan: available_block_slots.get(slot_index).copied(),
                    attempt_budget_exhausted: false,
                    source_error: None,
                });
            }
            let remaining_budget = settings
                .max_blocks_to_fetch
                .saturating_sub(summary.scanned_blocks);
            let chunk_end = available_block_slots.len().min(
                slot_index.saturating_add(
                    settings
                        .block_fetch_concurrency
                        .max(1)
                        .min(remaining_budget.max(1)),
                ),
            );
            let slot_chunk = &available_block_slots[slot_index..chunk_end];
            let block_fetch_started_at = Instant::now();
            let fetched_blocks = fetch_block_chunk(source, slot_chunk)?;
            summary.block_fetch_ms = summary
                .block_fetch_ms
                .saturating_add(elapsed_ms(block_fetch_started_at));
            for (slot, block_result) in fetched_blocks {
                summary.scanned_blocks = summary.scanned_blocks.saturating_add(1);
                let block = match block_result {
                    Ok(block) => block,
                    Err(error) => {
                        let source_error = error
                            .chain()
                            .find_map(|cause| cause.downcast_ref::<SourceError>())
                            .map(|cause| SourceError {
                                kind: cause.kind,
                                message: cause.message.clone(),
                            });
                        return Ok(AttemptScanResult {
                            summary: ScanSummary {
                                frontier_end_slot: slot.checked_sub(1),
                                ..summary
                            },
                            next_slot_to_scan: Some(slot),
                            attempt_budget_exhausted: false,
                            source_error,
                        });
                    }
                };
                let Some(block) = block else {
                    summary.frontier_end_slot = Some(slot);
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
                    let tx_time = transaction
                        .get("blockTime")
                        .and_then(Value::as_i64)
                        .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0));
                    let Some(tx_time) = tx_time.or(block_time) else {
                        continue;
                    };
                    if tx_time < requested_window_start || tx_time >= requested_window_end {
                        continue;
                    }
                    let candidate_filter_started_at = Instant::now();
                    let can_support_swap_parse = candidate_can_support_swap_parse(transaction);
                    if !can_support_swap_parse {
                        summary.candidate_filter_ms = summary
                            .candidate_filter_ms
                            .saturating_add(elapsed_ms(candidate_filter_started_at));
                        continue;
                    }
                    if !transaction_mentions_target_programs(transaction, program_ids) {
                        summary.candidate_filter_ms = summary
                            .candidate_filter_ms
                            .saturating_add(elapsed_ms(candidate_filter_started_at));
                        continue;
                    }
                    summary.candidate_filter_ms = summary
                        .candidate_filter_ms
                        .saturating_add(elapsed_ms(candidate_filter_started_at));
                    summary.candidate_program_transactions =
                        summary.candidate_program_transactions.saturating_add(1);
                    if summary.parsed_candidate_transactions
                        >= settings.max_candidate_transactions_to_parse
                    {
                        summary.phase_b_like_cost_budget_exhausted = true;
                        summary.early_stop_reason = Some(
                            "program_history_gap_fill_candidate_parse_budget_exhausted".to_string(),
                        );
                        return Ok(AttemptScanResult {
                            summary,
                            next_slot_to_scan: Some(slot),
                            attempt_budget_exhausted: false,
                            source_error: None,
                        });
                    }
                    summary.parsed_candidate_transactions =
                        summary.parsed_candidate_transactions.saturating_add(1);
                    let swap_parse_started_at = Instant::now();
                    if let Some(swap) = parse_program_scoped_transaction_to_swap_with_context(
                        transaction,
                        program_ids,
                        slot,
                        block_time,
                    )? {
                        summary.parsed_candidate_swaps =
                            summary.parsed_candidate_swaps.saturating_add(1);
                        summary
                            .swaps_by_signature
                            .entry(swap.signature.clone())
                            .or_insert(swap);
                    }
                    summary.swap_parse_ms = summary
                        .swap_parse_ms
                        .saturating_add(elapsed_ms(swap_parse_started_at));
                }
                summary.frontier_end_slot = Some(slot);
            }
            slot_index = chunk_end;
        }
        batch_start = batch_end.saturating_add(1);
        if batch_start == 0 {
            break;
        }
    }
    Ok(AttemptScanResult {
        summary,
        next_slot_to_scan: None,
        attempt_budget_exhausted: false,
        source_error: None,
    })
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

fn write_gap_fill_output<S: ProgramHistorySource>(
    db_path: &Path,
    source: &S,
    target_program_ids: &[String],
    plan: &GapFillPlan,
    fetch: FetchResult,
    output_paths: &OutputPaths,
    output_retention: usize,
    _now: DateTime<Utc>,
) -> Result<ProgramHistoryGapFillOutput> {
    let replayable_output = fetch.verdict.replayable_output();
    let output_db_path = if replayable_output {
        output_paths.output_db_path.clone()
    } else {
        output_paths.progress_db_path.clone()
    };
    let metadata_path = if replayable_output {
        output_paths.metadata_path.clone()
    } else {
        output_paths.progress_state_path.clone()
    };
    let gap_fill_state = copybot_storage::RecentRawJournalStateRow {
        covered_since: fetch.gap_fill_covered_since,
        covered_through_cursor: fetch.gap_fill_covered_through_cursor.clone(),
        row_count: fetch.staged_rows,
        ..Default::default()
    };
    let mut missing_segments = compute_missing_segments(
        plan.requested_window_start,
        plan.requested_window_end,
        &gap_fill_state,
    );
    merge_missing_segments(&mut missing_segments, fetch.boundary_missing_segments.clone());
    if !replayable_output {
        let mut explicit_segments = fetch.boundary_missing_segments.clone();
        explicit_segments.push(GapFillMissingSegment {
            start: plan.requested_window_start,
            end: plan.requested_window_end,
            reason: fetch.reason.clone(),
        });
        missing_segments = explicit_segments;
    }
    let source_lag_seconds = missing_segments.first().map(|segment| {
        segment
            .end
            .signed_duration_since(segment.start)
            .num_seconds()
            .max(0) as u64
    });
    let final_covered_since = if replayable_output {
        min_ts_opt(gap_fill_state.covered_since, plan.journal_covered_since)
    } else {
        plan.journal_covered_since
    };
    let final_covered_through_cursor = if replayable_output {
        max_cursor_opt(
            gap_fill_state.covered_through_cursor.as_ref(),
            plan.journal_covered_through_cursor.as_ref(),
        )
    } else {
        plan.journal_covered_through_cursor.clone()
    };
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
        latest_db_path: output_paths
            .latest_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        metadata_path: metadata_path.display().to_string(),
        progress_db_path: if replayable_output {
            None
        } else {
            Some(output_paths.progress_db_path.display().to_string())
        },
        progress_state_path: if replayable_output {
            None
        } else {
            Some(output_paths.progress_state_path.display().to_string())
        },
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
        requested_start_slot_raw: fetch.requested_start_slot_raw,
        requested_end_slot_raw: fetch.requested_end_slot_raw,
        resolved_start_slot: fetch.resolved_start_slot,
        resolved_end_slot: fetch.resolved_end_slot,
        start_slot_adjustment_kind: fetch.start_slot_adjustment_kind,
        end_slot_adjustment_kind: fetch.end_slot_adjustment_kind,
        start_slot_adjustment_distance: fetch.start_slot_adjustment_distance,
        end_slot_adjustment_distance: fetch.end_slot_adjustment_distance,
        boundary_resolution_verdict: fetch.boundary_resolution_verdict,
        requested_interval_fully_bracketed_after_adjustment: fetch
            .requested_interval_fully_bracketed_after_adjustment,
        requested_interval_partially_bracketed_after_adjustment: fetch
            .requested_interval_partially_bracketed_after_adjustment,
        slot_span: fetch.slot_span,
        resolved_bounds_reused_from_progress: fetch.resolved_bounds_reused_from_progress,
        coverage_method: fetch.coverage_method,
        block_fetch_encoding: fetch.block_fetch_encoding,
        scan_budget_slots: fetch.scan_budget_slots,
        budget_exhausted: fetch.budget_exhausted,
        phase_b_like_cost_budget_exhausted: fetch.phase_b_like_cost_budget_exhausted,
        sampling_segments: fetch.sampling_segments,
        sampling_window_slots: fetch.sampling_window_slots,
        block_batch_size: fetch.block_batch_size,
        block_fetch_concurrency: fetch.block_fetch_concurrency,
        max_slot_batches_per_attempt: fetch.max_slot_batches_per_attempt,
        max_blocks_to_fetch: fetch.max_blocks_to_fetch,
        max_candidate_transactions_to_parse: fetch.max_candidate_transactions_to_parse,
        max_requests_per_second: fetch.max_requests_per_second,
        retry_429_max_attempts: fetch.retry_429_max_attempts,
        retry_429_backoff_ms: fetch.retry_429_backoff_ms,
        current_phase: fetch.current_phase,
        dominant_phase: fetch.dominant_phase,
        resolve_slot_bounds_ms: fetch.resolve_slot_bounds_ms,
        attempt_number: fetch.attempt_number,
        cumulative_across_attempts: fetch.cumulative_across_attempts,
        attempt_frontier_start_slot: fetch.attempt_frontier_start_slot,
        attempt_frontier_end_slot: fetch.attempt_frontier_end_slot,
        attempt_frontier_advanced_slots: fetch.attempt_frontier_advanced_slots,
        next_batch_start_slot: fetch.next_batch_start_slot,
        zero_progress_retry_count: fetch.zero_progress_retry_count,
        zero_progress_blocked_start_slot: fetch.zero_progress_blocked_start_slot,
        zero_progress_escape_applied: fetch.zero_progress_escape_applied,
        zero_progress_escape_distance: fetch.zero_progress_escape_distance,
        progress_reset_reason: fetch.progress_reset_reason,
        attempt_budget_exhausted: fetch.attempt_budget_exhausted,
        attempt_block_list_ms: fetch.attempt_block_list_ms,
        attempt_block_fetch_ms: fetch.attempt_block_fetch_ms,
        attempt_candidate_filter_ms: fetch.attempt_candidate_filter_ms,
        attempt_swap_parse_ms: fetch.attempt_swap_parse_ms,
        attempt_sqlite_stage_ms: fetch.attempt_sqlite_stage_ms,
        attempt_scanned_batches: fetch.attempt_scanned_batches,
        attempt_scanned_slots: fetch.attempt_scanned_slots,
        attempt_listed_block_slots: fetch.attempt_listed_block_slots,
        attempt_scanned_blocks: fetch.attempt_scanned_blocks,
        attempt_scanned_transactions: fetch.attempt_scanned_transactions,
        attempt_candidate_program_transactions: fetch.attempt_candidate_program_transactions,
        attempt_parsed_candidate_transactions: fetch.attempt_parsed_candidate_transactions,
        attempt_parsed_candidate_swaps: fetch.attempt_parsed_candidate_swaps,
        scanned_batches: fetch.scanned_batches,
        scanned_slots: fetch.scanned_slots,
        listed_block_slots: fetch.listed_block_slots,
        scanned_blocks: fetch.scanned_blocks,
        scanned_transactions: fetch.scanned_transactions,
        candidate_program_transactions: fetch.candidate_program_transactions,
        parsed_candidate_transactions: fetch.parsed_candidate_transactions,
        parsed_candidate_swaps: fetch.parsed_candidate_swaps,
        fetched_rows: fetch.staged_rows,
        inserted_rows: if replayable_output {
            fetch.staged_rows
        } else {
            0
        },
        attempt_inserted_rows: fetch.attempt_inserted_rows,
        staged_rows: fetch.staged_rows,
        rows_withheld_due_to_incomplete_outcome: if replayable_output {
            0
        } else {
            fetch.staged_rows
        },
        gap_fill_covered_since: gap_fill_state.covered_since,
        gap_fill_covered_through_cursor: gap_fill_state.covered_through_cursor.clone(),
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

    if replayable_output {
        copy_atomic(&output_paths.progress_db_path, &output_paths.output_db_path)?;
        if let Some(dir) = output_paths.output_dir.as_ref() {
            let latest_path = output_paths.latest_db_path.as_ref().expect("latest path");
            copy_atomic(&output_paths.progress_db_path, latest_path)?;
            let archive_metadata_path =
                journal_snapshot_metadata_path(&output_paths.output_db_path);
            write_json_atomic(&archive_metadata_path, &output)?;
            write_json_atomic(&output_paths.metadata_path, &output)?;
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
            write_json_atomic(&output_paths.metadata_path, &output)?;
        }
        clear_progress_artifacts(output_paths)?;
    } else {
        write_json_atomic(&output_paths.progress_state_path, &output)?;
    }

    Ok(output)
}

fn classify_source_error(context: &str, error: &anyhow::Error) -> (GapFillVerdict, String) {
    match source_error_kind(error) {
        Some(SourceErrorKind::ProviderThrottled) => (
            GapFillVerdict::NotProvenDueToProviderThrottling,
            format!("{context}:{error}"),
        ),
        Some(SourceErrorKind::RetryableProviderFailure) => (
            GapFillVerdict::NotProvenDueToRetryableProviderFailure,
            format!("{context}:{error}"),
        ),
        _ => (
            GapFillVerdict::NonViableSourceContract,
            format!("{context}:{error}"),
        ),
    }
}

fn retryable_block_fetch_reason_code(message: &str) -> Option<&'static str> {
    if message.contains("rpc returned http status 408") {
        return Some("program_history_gap_fill_retryable_block_fetch_http_408");
    }
    if message.contains("rpc returned http status 503") {
        return Some("program_history_gap_fill_retryable_block_fetch_http_503");
    }
    if message.contains("failed reading rpc response body") {
        return Some("program_history_gap_fill_retryable_block_fetch_body_decode_failure");
    }
    None
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
        format!(
            "progress_db_path={}",
            output.progress_db_path.as_deref().unwrap_or("null")
        ),
        format!(
            "progress_state_path={}",
            output.progress_state_path.as_deref().unwrap_or("null")
        ),
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
            "requested_start_slot_raw={}",
            output
                .requested_start_slot_raw
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "requested_end_slot_raw={}",
            output
                .requested_end_slot_raw
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
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
            "start_slot_adjustment_kind={}",
            output.start_slot_adjustment_kind
        ),
        format!(
            "end_slot_adjustment_kind={}",
            output.end_slot_adjustment_kind
        ),
        format!(
            "start_slot_adjustment_distance={}",
            output
                .start_slot_adjustment_distance
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "end_slot_adjustment_distance={}",
            output
                .end_slot_adjustment_distance
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "boundary_resolution_verdict={}",
            output.boundary_resolution_verdict
        ),
        format!(
            "requested_interval_fully_bracketed_after_adjustment={}",
            output.requested_interval_fully_bracketed_after_adjustment
        ),
        format!(
            "requested_interval_partially_bracketed_after_adjustment={}",
            output.requested_interval_partially_bracketed_after_adjustment
        ),
        format!(
            "slot_span={}",
            output
                .slot_span
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "resolved_bounds_reused_from_progress={}",
            output.resolved_bounds_reused_from_progress
        ),
        format!("coverage_method={}", output.coverage_method),
        format!("block_fetch_encoding={}", output.block_fetch_encoding),
        format!("scan_budget_slots={}", output.scan_budget_slots),
        format!("budget_exhausted={}", output.budget_exhausted),
        format!(
            "phase_b_like_cost_budget_exhausted={}",
            output.phase_b_like_cost_budget_exhausted
        ),
        format!("sampling_segments={}", output.sampling_segments),
        format!("sampling_window_slots={}", output.sampling_window_slots),
        format!("block_batch_size={}", output.block_batch_size),
        format!("block_fetch_concurrency={}", output.block_fetch_concurrency),
        format!(
            "max_slot_batches_per_attempt={}",
            output.max_slot_batches_per_attempt
        ),
        format!("max_blocks_to_fetch={}", output.max_blocks_to_fetch),
        format!(
            "max_candidate_transactions_to_parse={}",
            output.max_candidate_transactions_to_parse
        ),
        format!("max_requests_per_second={}", output.max_requests_per_second),
        format!("retry_429_max_attempts={}", output.retry_429_max_attempts),
        format!("retry_429_backoff_ms={}", output.retry_429_backoff_ms),
        format!("current_phase={}", output.current_phase),
        format!("dominant_phase={}", output.dominant_phase),
        format!("resolve_slot_bounds_ms={}", output.resolve_slot_bounds_ms),
        format!("attempt_number={}", output.attempt_number),
        format!(
            "cumulative_across_attempts={}",
            output.cumulative_across_attempts
        ),
        format!(
            "attempt_frontier_start_slot={}",
            output
                .attempt_frontier_start_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "attempt_frontier_end_slot={}",
            output
                .attempt_frontier_end_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "attempt_frontier_advanced_slots={}",
            output.attempt_frontier_advanced_slots
        ),
        format!(
            "next_batch_start_slot={}",
            output
                .next_batch_start_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "zero_progress_retry_count={}",
            output.zero_progress_retry_count
        ),
        format!(
            "zero_progress_blocked_start_slot={}",
            output
                .zero_progress_blocked_start_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "zero_progress_escape_applied={}",
            output.zero_progress_escape_applied
        ),
        format!(
            "zero_progress_escape_distance={}",
            output
                .zero_progress_escape_distance
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "progress_reset_reason={}",
            output.progress_reset_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "attempt_budget_exhausted={}",
            output.attempt_budget_exhausted
        ),
        format!("attempt_block_list_ms={}", output.attempt_block_list_ms),
        format!("attempt_block_fetch_ms={}", output.attempt_block_fetch_ms),
        format!(
            "attempt_candidate_filter_ms={}",
            output.attempt_candidate_filter_ms
        ),
        format!("attempt_swap_parse_ms={}", output.attempt_swap_parse_ms),
        format!("attempt_sqlite_stage_ms={}", output.attempt_sqlite_stage_ms),
        format!("attempt_scanned_batches={}", output.attempt_scanned_batches),
        format!("attempt_scanned_slots={}", output.attempt_scanned_slots),
        format!(
            "attempt_listed_block_slots={}",
            output.attempt_listed_block_slots
        ),
        format!("attempt_scanned_blocks={}", output.attempt_scanned_blocks),
        format!(
            "attempt_scanned_transactions={}",
            output.attempt_scanned_transactions
        ),
        format!(
            "attempt_candidate_program_transactions={}",
            output.attempt_candidate_program_transactions
        ),
        format!(
            "attempt_parsed_candidate_transactions={}",
            output.attempt_parsed_candidate_transactions
        ),
        format!(
            "attempt_parsed_candidate_swaps={}",
            output.attempt_parsed_candidate_swaps
        ),
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
        format!("attempt_inserted_rows={}", output.attempt_inserted_rows),
        format!("staged_rows={}", output.staged_rows),
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

fn is_missing_block_time_rpc_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("was skipped") || lowered.contains("ledger jump to recent snapshot")
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

fn post_json_rpc_allow_missing_block_time(
    source: &QuickNodeBlocksRpcSource,
    payload: &Value,
) -> Result<Option<Value>> {
    let response = execute_json_rpc_with_policy(
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
    );
    match response {
        Ok(parsed) => Ok(Some(parsed)),
        Err(error) if source_error_kind(&error) == Some(SourceErrorKind::SourceContractFailure) => {
            if is_missing_block_time_rpc_error(&error.to_string()) {
                Ok(None)
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn rpc_result(response: &Value) -> &Value {
    response.get("result").unwrap_or(&Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Mutex,
    };

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
        get_block_failures: Mutex<HashMap<u64, VecDeque<SourceError>>>,
        get_block_delay: StdDuration,
        current_get_block_in_flight: AtomicUsize,
        max_get_block_in_flight: AtomicUsize,
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

        fn with_get_block_failure_once(mut self, slot: u64, error: SourceError) -> Self {
            self.get_block_failures
                .get_mut()
                .expect("get_block_failures poisoned")
                .entry(slot)
                .or_default()
                .push_back(error);
            self
        }

        fn with_get_block_delay(mut self, delay: StdDuration) -> Self {
            self.get_block_delay = delay;
            self
        }

        fn list_requests(&self) -> Vec<BlockRangeRequest> {
            self.list_requests.lock().unwrap().clone()
        }

        fn max_get_block_in_flight(&self) -> usize {
            self.max_get_block_in_flight.load(AtomicOrdering::SeqCst)
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
            let current = self
                .current_get_block_in_flight
                .fetch_add(1, AtomicOrdering::SeqCst)
                .saturating_add(1);
            loop {
                let observed = self.max_get_block_in_flight.load(AtomicOrdering::SeqCst);
                if observed >= current {
                    break;
                }
                if self
                    .max_get_block_in_flight
                    .compare_exchange(
                        observed,
                        current,
                        AtomicOrdering::SeqCst,
                        AtomicOrdering::SeqCst,
                    )
                    .is_ok()
                {
                    break;
                }
            }
            if !self.get_block_delay.is_zero() {
                sleep(self.get_block_delay);
            }
            let queued_failure = {
                let mut failures = self
                    .get_block_failures
                    .lock()
                    .expect("get_block_failures poisoned");
                let error = failures.get_mut(&slot).and_then(|queued| queued.pop_front());
                if failures.get(&slot).is_some_and(|queued| queued.is_empty()) {
                    failures.remove(&slot);
                }
                error
            };
            let result = match queued_failure {
                Some(error) => Err(error.into()),
                None => Ok(self.blocks.get(&slot).cloned()),
            };
            self.current_get_block_in_flight
                .fetch_sub(1, AtomicOrdering::SeqCst);
            result
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
    fn retryable_block_fetch_reason_code_classifies_only_transient_patterns_stage1() {
        assert_eq!(
            retryable_block_fetch_reason_code("rpc returned http status 408: request timeout"),
            Some("program_history_gap_fill_retryable_block_fetch_http_408")
        );
        assert_eq!(
            retryable_block_fetch_reason_code("rpc returned http status 503: upstream unavailable"),
            Some("program_history_gap_fill_retryable_block_fetch_http_503")
        );
        assert_eq!(
            retryable_block_fetch_reason_code(
                "failed reading rpc response body: error decoding response body"
            ),
            Some("program_history_gap_fill_retryable_block_fetch_body_decode_failure")
        );
        assert_eq!(
            retryable_block_fetch_reason_code(
                "failed parsing rpc response json: expected value at line 1 column 1"
            ),
            None
        );
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
        assert!(!output_path.exists());

        let loaded_config = load_from_path(&fixture.config_path)?;
        let runtime_store = SqliteStore::open(&fixture.runtime_db_path)?;
        let discovery = DiscoveryService::new(loaded_config.discovery, loaded_config.shadow);
        let verdict = discovery.runtime_restore_verdict(&runtime_store, now)?;
        assert_eq!(
            verdict.verdict,
            DiscoveryRuntimeRestoreVerdictKind::FailClosed.as_str()
        );
        Ok(())
    }

    #[test]
    fn heavy_gap_fill_returns_terminal_attempt_budget_without_external_timeout() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-attempt-budget")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-attempt-budget.sqlite");
        let source = resumable_source(window_start, window_end);

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path.clone()),
                window_start: Some(window_start),
                window_end: Some(window_end),
                max_slot_batches_per_attempt_override: Some(1),
                now,
                ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
            },
            &source,
        )?;

        assert_eq!(output.verdict, "not_proven_due_to_attempt_budget");
        assert_eq!(output.current_phase, "awaiting_next_attempt");
        assert_eq!(output.attempt_number, 1);
        assert_eq!(output.attempt_scanned_batches, 1);
        assert_eq!(output.scanned_batches, 1);
        assert!(!output.resolved_bounds_reused_from_progress);
        assert_eq!(output.block_fetch_encoding, "json");
        assert!(!output.dominant_phase.is_empty());
        assert!(output.attempt_frontier_advanced_slots > 0);
        assert!(!output.replayable_output);
        assert_eq!(output.inserted_rows, 0);
        assert_eq!(output.attempt_inserted_rows, 1);
        assert_eq!(output.staged_rows, 1);
        assert!(output.progress_db_path.is_some());
        assert!(Path::new(output.progress_db_path.as_deref().unwrap()).exists());
        assert!(Path::new(output.progress_state_path.as_deref().unwrap()).exists());
        assert!(!output_path.exists());
        Ok(())
    }

    #[test]
    fn block_fetch_concurrency_parallelizes_get_block_under_same_contract() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-block-fetch-concurrency")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(4);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-block-fetch-concurrency.sqlite");
        let source = FakeProgramHistorySource::default()
            .with_latest_slot(103)
            .with_get_block_delay(StdDuration::from_millis(20))
            .with_block_time(100, window_start)
            .with_block_time(103, window_end)
            .with_block_range(100, 103, vec![100, 101, 102, 103])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx(
                        "concurrent-a",
                        "wallet-gap",
                        100,
                        "raydium-program",
                    )],
                ),
            )
            .with_block(
                101,
                block_with_transactions(
                    101,
                    window_start + Duration::minutes(1),
                    vec![swap_tx(
                        "concurrent-b",
                        "wallet-gap",
                        101,
                        "pumpswap-program",
                    )],
                ),
            )
            .with_block(
                102,
                block_with_transactions(
                    102,
                    window_start + Duration::minutes(2),
                    vec![swap_tx(
                        "concurrent-c",
                        "wallet-gap",
                        102,
                        "raydium-program",
                    )],
                ),
            )
            .with_block(
                103,
                block_with_transactions(
                    103,
                    window_start + Duration::minutes(3),
                    vec![swap_tx(
                        "concurrent-d",
                        "wallet-gap",
                        103,
                        "pumpswap-program",
                    )],
                ),
            );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path),
                window_start: Some(window_start),
                window_end: Some(window_end),
                block_fetch_concurrency_override: Some(4),
                max_slot_batches_per_attempt_override: Some(1),
                max_blocks_to_fetch_override: Some(4),
                now,
                ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
            },
            &source,
        )?;

        assert_eq!(output.block_fetch_concurrency, 4);
        assert_eq!(output.attempt_scanned_blocks, 4);
        assert!(source.max_get_block_in_flight() >= 2);
        Ok(())
    }

    #[test]
    fn retryable_block_fetch_http_503_mid_attempt_stays_resumable_stage1() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-retryable-503")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-retryable-503.sqlite");
        let source = resumable_source(window_start, window_end).with_get_block_failure_once(
            1_105,
            SourceError::retryable_provider_failure(
                "program_history_gap_fill_retryable_block_fetch_http_503: rpc returned http status 503",
            ),
        );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path),
                window_start: Some(window_start),
                window_end: Some(window_end),
                max_slot_batches_per_attempt_override: Some(3),
                now,
                ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
            },
            &source,
        )?;

        assert_eq!(
            output.verdict,
            "not_proven_due_to_retryable_provider_failure"
        );
        assert_eq!(output.current_phase, "awaiting_next_attempt");
        assert_eq!(output.next_batch_start_slot, Some(1_105));
        assert_eq!(output.attempt_frontier_end_slot, Some(1_104));
        assert!(output.attempt_frontier_advanced_slots > 0);
        assert_eq!(output.attempt_inserted_rows, 1);
        assert_eq!(output.staged_rows, 1);
        assert!(!output.replayable_output);
        assert_eq!(
            output.reason,
            "program_history_gap_fill_retryable_block_fetch_http_503: rpc returned http status 503"
        );
        Ok(())
    }

    #[test]
    fn retryable_block_fetch_body_decode_failure_mid_attempt_stays_resumable_stage1(
    ) -> Result<()> {
        let fixture = make_fixture("program-gap-fill-retryable-body")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-retryable-body.sqlite");
        let source = resumable_source(window_start, window_end).with_get_block_failure_once(
            1_105,
            SourceError::retryable_provider_failure(
                "program_history_gap_fill_retryable_block_fetch_body_decode_failure: failed reading rpc response body: error decoding response body",
            ),
        );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path),
                window_start: Some(window_start),
                window_end: Some(window_end),
                max_slot_batches_per_attempt_override: Some(3),
                now,
                ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
            },
            &source,
        )?;

        assert_eq!(
            output.verdict,
            "not_proven_due_to_retryable_provider_failure"
        );
        assert_eq!(output.current_phase, "awaiting_next_attempt");
        assert_eq!(output.next_batch_start_slot, Some(1_105));
        assert!(output.attempt_frontier_advanced_slots > 0);
        assert_eq!(output.attempt_inserted_rows, 1);
        assert_eq!(output.staged_rows, 1);
        assert!(!output.replayable_output);
        assert!(output
            .reason
            .contains("program_history_gap_fill_retryable_block_fetch_body_decode_failure"));
        Ok(())
    }

    #[test]
    fn deterministic_block_fetch_source_contract_failure_stays_terminal_stage1() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-terminal-block-fetch")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-terminal-block-fetch.sqlite");
        let source = resumable_source(window_start, window_end).with_get_block_failure_once(
            1_105,
            SourceError::source_contract_failure(
                "rpc returned http status 500: malformed upstream contract",
            ),
        );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path),
                window_start: Some(window_start),
                window_end: Some(window_end),
                max_slot_batches_per_attempt_override: Some(3),
                now,
                ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
            },
            &source,
        )?;

        assert_eq!(output.verdict, "non_viable_source_contract");
        assert_eq!(output.current_phase, "source_contract_failed");
        assert_eq!(output.next_batch_start_slot, Some(1_105));
        assert_eq!(
            output.reason,
            "program_history_gap_fill_source_contract_failed:rpc returned http status 500: malformed upstream contract"
        );
        Ok(())
    }

    #[test]
    fn retryable_block_fetch_failure_rerun_reuses_progress_and_continues_stage1() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-retryable-resume")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-retryable-resume.sqlite");
        let source = resumable_source(window_start, window_end).with_get_block_failure_once(
            1_105,
            SourceError::retryable_provider_failure(
                "program_history_gap_fill_retryable_block_fetch_http_503: rpc returned http status 503",
            ),
        );

        let base_config = Config {
            output_path: Some(output_path),
            window_start: Some(window_start),
            window_end: Some(window_end),
            max_slot_batches_per_attempt_override: Some(3),
            now,
            ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
        };

        let first = run_output_with_source(base_config.clone(), &source)?;
        assert_eq!(
            first.verdict,
            "not_proven_due_to_retryable_provider_failure"
        );
        assert_eq!(first.next_batch_start_slot, Some(1_105));
        assert!(!first.resolved_bounds_reused_from_progress);

        let second = run_output_with_source(base_config, &source)?;
        assert_eq!(second.attempt_number, 2);
        assert!(second.cumulative_across_attempts);
        assert!(second.resolved_bounds_reused_from_progress);
        assert_eq!(second.attempt_frontier_start_slot, Some(1_105));
        assert!(second.replayable_output);
        assert_eq!(
            second.verdict,
            "complete_but_insufficient_for_healthy_restore"
        );
        assert_eq!(second.staged_rows, 3);
        assert_eq!(second.next_batch_start_slot, None);
        Ok(())
    }

    #[test]
    fn zero_progress_provider_throttling_same_slot_escapes_after_bounded_retries_stage1(
    ) -> Result<()> {
        let fixture = make_fixture("program-gap-fill-zero-progress-throttle")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(3);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-zero-progress-throttle.sqlite");

        let base_config = Config {
            output_path: Some(output_path),
            window_start: Some(window_start),
            window_end: Some(window_end),
            max_slot_batches_per_attempt_override: Some(1),
            now,
            ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
        };

        let throttled_source = || {
            FakeProgramHistorySource::default()
                .with_latest_slot(102)
                .with_block_time(100, window_start)
                .with_block_time(101, window_start + Duration::minutes(1))
                .with_block_time(102, window_end)
                .with_block_range(100, 102, vec![100, 101, 102])
                .with_block_range(101, 102, vec![101, 102])
                .with_block(100, block_with_transactions(100, window_start, vec![]))
                .with_block(
                    101,
                    block_with_transactions(
                        101,
                        window_start + Duration::minutes(1),
                        vec![swap_tx("after-escape-a", "wallet-gap", 101, "raydium-program")],
                    ),
                )
                .with_block(
                    102,
                    block_with_transactions(
                        102,
                        window_end - Duration::seconds(1),
                        vec![swap_tx("after-escape-b", "wallet-gap", 102, "pumpswap-program")],
                    ),
                )
                .with_get_block_failure_once(
                    100,
                    SourceError::provider_throttled(
                        "rpc provider throttled request: simulated 429/limit",
                    ),
                )
        };

        let first = run_output_with_source(base_config.clone(), &throttled_source())?;
        assert_eq!(first.verdict, "not_proven_due_to_provider_throttling");
        assert_eq!(first.attempt_frontier_advanced_slots, 0);
        assert_eq!(first.next_batch_start_slot, Some(100));
        assert_eq!(first.zero_progress_retry_count, 1);
        assert_eq!(first.zero_progress_blocked_start_slot, Some(100));
        assert!(!first.zero_progress_escape_applied);

        let second = run_output_with_source(base_config.clone(), &throttled_source())?;
        assert_eq!(second.verdict, "not_proven_due_to_provider_throttling");
        assert_eq!(second.attempt_frontier_advanced_slots, 0);
        assert_eq!(second.next_batch_start_slot, Some(100));
        assert_eq!(second.zero_progress_retry_count, 2);
        assert_eq!(second.zero_progress_blocked_start_slot, Some(100));
        assert!(!second.zero_progress_escape_applied);

        let third = run_output_with_source(base_config.clone(), &throttled_source())?;
        assert_eq!(third.verdict, "not_proven_due_to_provider_throttling");
        assert_eq!(third.attempt_frontier_advanced_slots, 0);
        assert_eq!(third.zero_progress_retry_count, 3);
        assert_eq!(third.zero_progress_blocked_start_slot, Some(100));
        assert!(third.zero_progress_escape_applied);
        assert_eq!(
            third.zero_progress_escape_distance,
            Some(ZERO_PROGRESS_ESCAPE_DISTANCE)
        );
        assert_eq!(third.next_batch_start_slot, Some(101));
        assert!(third.missing_segments.iter().any(|segment| {
            segment.reason == ZERO_PROGRESS_ESCAPED_SLOT_MISSING_REASON
        }));

        let advanced_source = FakeProgramHistorySource::default()
            .with_latest_slot(102)
            .with_block_time(100, window_start)
            .with_block_time(101, window_start + Duration::minutes(1))
            .with_block_time(102, window_end)
            .with_block_range(101, 102, vec![101, 102])
            .with_block(
                101,
                block_with_transactions(
                    101,
                    window_start + Duration::minutes(1),
                    vec![swap_tx("after-escape-a", "wallet-gap", 101, "raydium-program")],
                ),
            )
            .with_block(
                102,
                block_with_transactions(
                    102,
                    window_end - Duration::seconds(1),
                    vec![swap_tx("after-escape-b", "wallet-gap", 102, "pumpswap-program")],
                ),
            );
        let fourth = run_output_with_source(base_config, &advanced_source)?;
        assert!(fourth.resolved_bounds_reused_from_progress);
        assert_eq!(fourth.attempt_frontier_start_slot, Some(101));
        assert_eq!(fourth.verdict, "not_proven_due_to_provider_throttling");
        assert_eq!(fourth.current_phase, "completed_with_explicit_missing_segments");
        assert_eq!(fourth.next_batch_start_slot, Some(103));
        assert!(!fourth.replayable_output);
        assert_eq!(fourth.zero_progress_retry_count, 0);
        assert_eq!(fourth.zero_progress_blocked_start_slot, None);
        assert!(!fourth.zero_progress_escape_applied);
        assert!(fourth
            .missing_segments
            .iter()
            .any(|segment| segment.reason == ZERO_PROGRESS_ESCAPED_SLOT_MISSING_REASON));
        Ok(())
    }

    #[test]
    fn positive_progress_provider_throttling_does_not_trigger_zero_progress_escape_stage1(
    ) -> Result<()> {
        let fixture = make_fixture("program-gap-fill-positive-progress-throttle")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-positive-progress-throttle.sqlite");
        let source = resumable_source(window_start, window_end).with_get_block_failure_once(
            1_105,
            SourceError::provider_throttled("rpc provider throttled request: simulated 429/limit"),
        );

        let output = run_output_with_source(
            Config {
                output_path: Some(output_path),
                window_start: Some(window_start),
                window_end: Some(window_end),
                max_slot_batches_per_attempt_override: Some(3),
                now,
                ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
            },
            &source,
        )?;

        assert_eq!(output.verdict, "not_proven_due_to_provider_throttling");
        assert!(output.attempt_frontier_advanced_slots > 0);
        assert_eq!(output.zero_progress_retry_count, 0);
        assert_eq!(output.zero_progress_blocked_start_slot, None);
        assert!(!output.zero_progress_escape_applied);
        assert_eq!(output.next_batch_start_slot, Some(1_105));
        Ok(())
    }

    #[test]
    fn resumable_gap_fill_attempts_make_forward_progress_and_publish_only_on_completion(
    ) -> Result<()> {
        let fixture = make_fixture("program-gap-fill-resumable")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-resumable.sqlite");
        let source = resumable_source(window_start, window_end);

        let base_config = Config {
            output_path: Some(output_path.clone()),
            window_start: Some(window_start),
            window_end: Some(window_end),
            max_slot_batches_per_attempt_override: Some(1),
            now,
            ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
        };

        let first = run_output_with_source(base_config.clone(), &source)?;
        assert_eq!(first.verdict, "not_proven_due_to_attempt_budget");
        assert_eq!(first.attempt_number, 1);
        assert_eq!(first.staged_rows, 1);
        assert_eq!(first.next_batch_start_slot, Some(1105));
        assert!(!first.resolved_bounds_reused_from_progress);
        assert_eq!(first.block_fetch_encoding, "json");
        assert!(!first.replayable_output);

        let second = run_output_with_source(base_config.clone(), &source)?;
        assert_eq!(second.verdict, "not_proven_due_to_attempt_budget");
        assert_eq!(second.attempt_number, 2);
        assert!(second.cumulative_across_attempts);
        assert!(second.resolved_bounds_reused_from_progress);
        assert_eq!(second.resolve_slot_bounds_ms, 0);
        assert_eq!(second.block_fetch_encoding, "json");
        assert!(!second.dominant_phase.is_empty());
        assert!(second.attempt_frontier_advanced_slots > 0);
        assert_eq!(second.scanned_batches, 2);
        assert_eq!(second.staged_rows, 2);
        assert_eq!(second.next_batch_start_slot, Some(2110));
        assert!(!second.replayable_output);

        let third = run_output_with_source(base_config, &source)?;
        assert_eq!(third.attempt_number, 3);
        assert!(third.replayable_output);
        assert!(third.resolved_bounds_reused_from_progress);
        assert_eq!(
            third.verdict,
            "complete_but_insufficient_for_healthy_restore"
        );
        assert_eq!(third.scanned_batches, 3);
        assert_eq!(third.staged_rows, 3);
        assert_eq!(third.next_batch_start_slot, None);
        assert!(output_path.exists());
        let progress_db_path = path_with_inserted_suffix(&output_path, ".progress");
        assert!(!progress_db_path.exists());
        let output_store = SqliteStore::open_read_only(&output_path)?;
        assert_eq!(
            output_store
                .load_observed_swaps_since(window_start - Duration::minutes(1))?
                .len(),
            3
        );
        Ok(())
    }

    #[test]
    fn legacy_progress_state_missing_new_telemetry_fields_still_resumes() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-legacy-progress")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-legacy-progress.sqlite");
        let source = resumable_source(window_start, window_end);

        let base_config = Config {
            output_path: Some(output_path),
            window_start: Some(window_start),
            window_end: Some(window_end),
            max_slot_batches_per_attempt_override: Some(1),
            now,
            ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
        };

        let first = run_output_with_source(base_config.clone(), &source)?;
        let progress_state_path = PathBuf::from(
            first
                .progress_state_path
                .clone()
                .expect("first attempt must write progress state"),
        );
        let mut legacy_json: Value =
            serde_json::from_str(&fs::read_to_string(&progress_state_path)?)?;
        let Value::Object(ref mut fields) = legacy_json else {
            bail!("progress state json must remain an object");
        };
        for field in [
            "resolved_bounds_reused_from_progress",
            "block_fetch_encoding",
            "block_fetch_concurrency",
            "dominant_phase",
            "resolve_slot_bounds_ms",
            "attempt_frontier_advanced_slots",
            "attempt_block_list_ms",
            "attempt_block_fetch_ms",
            "attempt_candidate_filter_ms",
            "attempt_swap_parse_ms",
            "attempt_sqlite_stage_ms",
        ] {
            fields.remove(field);
        }
        fs::write(
            &progress_state_path,
            serde_json::to_vec_pretty(&legacy_json)?,
        )?;

        let second = run_output_with_source(base_config, &source)?;
        assert_eq!(second.attempt_number, 2);
        assert!(second.cumulative_across_attempts);
        assert_eq!(second.progress_reset_reason, None);
        assert!(second.resolved_bounds_reused_from_progress);
        Ok(())
    }

    #[test]
    fn unreadable_progress_state_resets_safely_without_operator_surgery() -> Result<()> {
        let fixture = make_fixture("program-gap-fill-unreadable-progress")?;
        let now = parse_ts("2026-03-24T13:43:40Z")?;
        let window_start = now - Duration::minutes(14);
        let window_end = now;
        init_runtime_db(&fixture.runtime_db_path)?;
        let output_path = fixture
            .temp
            .path()
            .join("program-gap-fill-unreadable-progress.sqlite");
        let source = resumable_source(window_start, window_end);

        let base_config = Config {
            output_path: Some(output_path),
            window_start: Some(window_start),
            window_end: Some(window_end),
            max_slot_batches_per_attempt_override: Some(1),
            now,
            ..gap_fill_config(&fixture, Some(window_start), Some(window_end))
        };

        let first = run_output_with_source(base_config.clone(), &source)?;
        let progress_state_path = PathBuf::from(
            first
                .progress_state_path
                .clone()
                .expect("first attempt must write progress state"),
        );
        fs::write(&progress_state_path, "{not-json")?;

        let second = run_output_with_source(base_config, &source)?;
        assert_eq!(second.attempt_number, 1);
        assert!(!second.cumulative_across_attempts);
        assert_eq!(
            second.progress_reset_reason.as_deref(),
            Some("program_history_gap_fill_progress_reset_unreadable_state")
        );
        assert!(!second.replayable_output);
        Ok(())
    }

    #[test]
    fn skipped_start_boundary_partial_bracket_is_reported_consistently_stage1() -> Result<()> {
        let base = parse_ts("2026-03-24T00:00:00Z")?;
        let window_start = boundary_slot_time(base, 100);
        let window_end = boundary_slot_time(base, 110);
        let source = boundary_source(
            base,
            120,
            (101..=120).collect(),
        );
        let bounds = resolve_slot_bounds(&source, window_start, window_end, 4)?;
        assert_eq!(bounds.requested_start_slot_raw(), Some(100));
        assert_eq!(bounds.requested_end_slot_raw(), Some(110));
        assert_eq!(bounds.start_slot(), Some(101));
        assert_eq!(bounds.end_slot(), Some(110));
        assert_eq!(
            bounds.start.adjustment_kind,
            BoundaryAdjustmentKind::AdvancedToNextAvailable
        );
        assert_eq!(bounds.end.adjustment_kind, BoundaryAdjustmentKind::Exact);
        assert_eq!(
            bounds.boundary_resolution_verdict,
            BoundaryResolutionVerdict::PartiallyBracketed
        );
        assert!(bounds.requested_interval_partially_bracketed_after_adjustment);
        assert!(bounds
            .boundary_missing_segments
            .iter()
            .any(|segment| segment.reason
                == "requested_window_prefix_uncovered_after_start_slot_adjustment"));
        Ok(())
    }

    #[test]
    fn unresolved_boundaries_fail_explicitly_stage1() -> Result<()> {
        let base = parse_ts("2026-03-24T00:00:00Z")?;
        let window_start = boundary_slot_time(base, 100);
        let window_end = boundary_slot_time(base, 110);
        let source = boundary_source(base, 120, vec![80, 120]);
        let bounds = resolve_slot_bounds(&source, window_start, window_end, 2)?;
        assert_eq!(
            bounds.boundary_resolution_verdict,
            BoundaryResolutionVerdict::Unresolved
        );
        assert!(bounds.usable_slot_range().is_none());
        assert_eq!(
            bounds.failure_reason(),
            "slot_boundaries_unresolved_within_search_radius"
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
            block_fetch_concurrency_override: None,
            max_slot_batches_per_attempt_override: None,
            max_blocks_to_fetch_override: None,
            max_candidate_transactions_to_parse_override: None,
            json: true,
            now: parse_ts("2026-03-24T13:43:40Z").expect("static ts"),
        }
    }

    fn run_output_with_source<S: ProgramHistorySource + Sync>(
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

    fn resumable_source(
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> FakeProgramHistorySource {
        FakeProgramHistorySource::default()
            .with_latest_slot(2_200)
            .with_block_time(100, window_start)
            .with_block_time(1_105, window_start + Duration::minutes(7))
            .with_block_time(2_110, window_end - Duration::minutes(1))
            .with_block_time(2_200, window_end)
            .with_block_range(100, 1_104, vec![100])
            .with_block_range(1_105, 2_109, vec![1_105])
            .with_block_range(2_110, 2_200, vec![2_110])
            .with_block(
                100,
                block_with_transactions(
                    100,
                    window_start,
                    vec![swap_tx("resume-a", "wallet-gap", 100, "raydium-program")],
                ),
            )
            .with_block(
                1_105,
                block_with_transactions(
                    1_105,
                    window_start + Duration::minutes(7),
                    vec![swap_tx("resume-b", "wallet-gap", 1_105, "pumpswap-program")],
                ),
            )
            .with_block(
                2_110,
                block_with_transactions(
                    2_110,
                    window_end - Duration::minutes(1),
                    vec![swap_tx("resume-c", "wallet-gap", 2_110, "raydium-program")],
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

    fn init_runtime_db(runtime_db_path: &Path) -> Result<()> {
        reset_sqlite_path(runtime_db_path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(runtime_db_path)?;
        store.run_migrations(&migration_dir)?;
        Ok(())
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

    fn boundary_slot_time(base: DateTime<Utc>, slot: u64) -> DateTime<Utc> {
        base + Duration::milliseconds(slot as i64 * 400)
    }

    fn boundary_source(
        base: DateTime<Utc>,
        latest_slot: u64,
        available_slots: Vec<u64>,
    ) -> FakeProgramHistorySource {
        let mut source = FakeProgramHistorySource::default().with_latest_slot(latest_slot);
        for slot in available_slots {
            source = source.with_block_time(slot, boundary_slot_time(base, slot));
        }
        source
    }
}
