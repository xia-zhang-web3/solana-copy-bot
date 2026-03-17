use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_core_types::SwapEvent;
use copybot_storage::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, DiscoveryScoringBoundarySeedSnapshot,
    RiskEventRow, SqliteStore,
};
use serde::Deserialize;
use signal_hook::consts::signal::SIGINT;
#[cfg(unix)]
use signal_hook::consts::signal::SIGTERM;
use signal_hook::flag;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

const DEFAULT_BATCH_SIZE: usize = 5_000;
const BACKFILL_SOURCE_PROTECTION_TTL_MINUTES: i64 = 240;
const DEFAULT_RUNTIME_PRESSURE_SERVICE: &str = "solana-copy-bot";
const DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO: f64 = 0.25;
const DEFAULT_RUNTIME_PRESSURE_JOURNAL_LINES: usize = 250;
const DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS: i64 = 1_000;
const DEFAULT_RUNTIME_PRESSURE_SAMPLE_AGE_GRACE_SECONDS: u64 = 5;
const RUNTIME_INFRA_STOP_EVENT_TYPE: &str = "shadow_risk_infra_stop";
const RUNTIME_INFRA_CLEARED_EVENT_TYPE: &str = "shadow_risk_infra_cleared";
const SLEEP_INTERRUPT_POLL_MS: u64 = 100;

#[derive(Debug, Clone)]
struct Cursor {
    ts: DateTime<Utc>,
    slot: u64,
    signature: String,
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    start_ts: DateTime<Utc>,
    seeded_reset_max_start_ts: Option<DateTime<Utc>>,
    end_ts: Option<DateTime<Utc>>,
    batch_size: usize,
    sleep_ms: u64,
    max_batches_per_run: Option<usize>,
    max_runtime_seconds: Option<u64>,
    reset: bool,
    seeded_reset: bool,
    stop_after_seed_install: bool,
    mark_covered: bool,
    resume_after: Option<Cursor>,
    abort_on_runtime_pressure: bool,
    runtime_pressure_service: String,
    runtime_pressure_log_path: Option<PathBuf>,
    max_yellowstone_fill_ratio: f64,
    max_ingestion_lag_ms_p95: u64,
    max_runtime_pressure_sample_age_seconds: u64,
    abort_on_runtime_infra_stop: bool,
    aggregate_write_config: DiscoveryAggregateWriteConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RuntimeInfraStopDetails {
    #[serde(default)]
    reason: String,
    #[serde(default)]
    yellowstone_output_queue_depth: Option<u64>,
    #[serde(default)]
    yellowstone_output_queue_capacity: Option<u64>,
    #[serde(default)]
    yellowstone_output_queue_fill_ratio: Option<f64>,
    #[serde(default)]
    yellowstone_output_oldest_age_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct ActiveRuntimeInfraStop {
    rowid: i64,
    event_ts: String,
    reason: String,
    yellowstone_output_queue_depth: Option<u64>,
    yellowstone_output_queue_capacity: Option<u64>,
    yellowstone_output_queue_fill_ratio: Option<f64>,
    yellowstone_output_oldest_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RuntimePressureLogPayload {
    #[serde(default)]
    message: String,
    #[serde(default)]
    yellowstone_output_queue_depth: Option<u64>,
    #[serde(default)]
    yellowstone_output_queue_capacity: Option<u64>,
    #[serde(default)]
    yellowstone_output_queue_fill_ratio: Option<f64>,
    #[serde(default)]
    yellowstone_output_oldest_age_ms: Option<u64>,
    #[serde(default)]
    ingestion_lag_ms_p95: Option<u64>,
}

#[derive(Debug, Clone)]
struct RuntimePressureSample {
    source: String,
    sample_ts: DateTime<Utc>,
    yellowstone_output_queue_depth: Option<u64>,
    yellowstone_output_queue_capacity: Option<u64>,
    yellowstone_output_queue_fill_ratio: Option<f64>,
    yellowstone_output_oldest_age_ms: Option<u64>,
    ingestion_lag_ms_p95: Option<u64>,
}

#[derive(Debug, Default)]
struct RuntimePressureMonitor {
    refreshed_at: Option<DateTime<Utc>>,
    cached_sample: Option<RuntimePressureSample>,
}

#[derive(Debug, Clone, Copy, Default)]
struct BatchStageTimings {
    scan_ms: u64,
    prepare_ms: u64,
    apply_ms: u64,
    rug_finalize_ms: u64,
    progress_update_ms: u64,
}

#[derive(Debug, Clone)]
struct ReplayLoopOutcome {
    stop_reason: RunStopReason,
    cursor: Cursor,
    total_rows: usize,
    batches: usize,
    gap_cursor_observed: bool,
    stage_totals: BatchStageTimings,
}

#[derive(Debug, Clone)]
struct BoundarySeedBuildOutcome {
    replay: ReplayLoopOutcome,
    seed_snapshot: Option<DiscoveryScoringBoundarySeedSnapshot>,
}

#[derive(Debug, Clone)]
enum SeededReplayOutcome {
    Stopped(ReplayLoopOutcome),
    ReplayAfterSeed(ReplayLoopOutcome),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunStopReason {
    CompletedSourceExhausted,
    CompletedRequestedEndTs,
    StoppedAfterSeedInstall,
    StoppedDueToBatchBudget,
    StoppedDueToRuntimeBudget,
    StoppedDueToFastGuard,
    StoppedDueToInfraGuard,
    StoppedDueToTerminationSignal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillTestFailpoint {
    None,
    AfterBoundaryExportBeforeSeedInstall,
    AfterCommittedSeedInstallBeforeReplayAfterSeed,
}

#[cfg(test)]
static BACKFILL_TEST_FAILPOINT: AtomicU8 = AtomicU8::new(0);

#[cfg(test)]
fn set_backfill_test_failpoint(failpoint: BackfillTestFailpoint) {
    let raw = match failpoint {
        BackfillTestFailpoint::None => 0,
        BackfillTestFailpoint::AfterBoundaryExportBeforeSeedInstall => 1,
        BackfillTestFailpoint::AfterCommittedSeedInstallBeforeReplayAfterSeed => 2,
    };
    BACKFILL_TEST_FAILPOINT.store(raw, Ordering::SeqCst);
}

#[cfg(test)]
fn maybe_fire_backfill_test_failpoint(failpoint: BackfillTestFailpoint) -> Result<()> {
    let expected = match failpoint {
        BackfillTestFailpoint::None => 0,
        BackfillTestFailpoint::AfterBoundaryExportBeforeSeedInstall => 1,
        BackfillTestFailpoint::AfterCommittedSeedInstallBeforeReplayAfterSeed => 2,
    };
    if BACKFILL_TEST_FAILPOINT
        .compare_exchange(expected, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        bail!("test failpoint triggered: {}", failpoint.as_str());
    }
    Ok(())
}

#[cfg(not(test))]
fn maybe_fire_backfill_test_failpoint(_failpoint: BackfillTestFailpoint) -> Result<()> {
    Ok(())
}

impl BackfillTestFailpoint {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::AfterBoundaryExportBeforeSeedInstall => {
                "after_boundary_export_before_seed_install"
            }
            Self::AfterCommittedSeedInstallBeforeReplayAfterSeed => {
                "after_committed_seed_install_before_replay_after_seed"
            }
        }
    }
}

impl RunStopReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::CompletedSourceExhausted => "completed_source_exhausted",
            Self::CompletedRequestedEndTs => "completed_requested_end_ts",
            Self::StoppedAfterSeedInstall => "stopped_after_seed_install",
            Self::StoppedDueToBatchBudget => "stopped_due_to_batch_budget",
            Self::StoppedDueToRuntimeBudget => "stopped_due_to_runtime_budget",
            Self::StoppedDueToFastGuard => "stopped_due_to_fast_guard",
            Self::StoppedDueToInfraGuard => "stopped_due_to_infra_guard",
            Self::StoppedDueToTerminationSignal => "stopped_due_to_termination_signal",
        }
    }
}

impl ActiveRuntimeInfraStop {
    fn from_risk_event(row: RiskEventRow) -> Self {
        let details = row
            .details_json
            .as_deref()
            .and_then(|details_json| {
                serde_json::from_str::<RuntimeInfraStopDetails>(details_json).ok()
            })
            .unwrap_or_default();
        let reason = if details.reason.trim().is_empty() {
            row.details_json.unwrap_or_default()
        } else {
            details.reason
        };
        Self {
            rowid: row.rowid,
            event_ts: row.ts,
            reason,
            yellowstone_output_queue_depth: details.yellowstone_output_queue_depth,
            yellowstone_output_queue_capacity: details.yellowstone_output_queue_capacity,
            yellowstone_output_queue_fill_ratio: details.yellowstone_output_queue_fill_ratio,
            yellowstone_output_oldest_age_ms: details.yellowstone_output_oldest_age_ms,
        }
    }

    fn abort_message(&self) -> String {
        format!(
            "runtime health guard aborted backfill due to active {RUNTIME_INFRA_STOP_EVENT_TYPE} rowid={} event_ts={} reason={}",
            self.rowid, self.event_ts, self.reason
        )
    }

    fn log_event(&self) {
        println!(
            "event=runtime_pressure_abort source={} rowid={} event_ts={} reason={} yellowstone_output_queue_depth={} yellowstone_output_queue_capacity={} yellowstone_output_queue_fill_ratio={} yellowstone_output_oldest_age_ms={}",
            RUNTIME_INFRA_STOP_EVENT_TYPE,
            self.rowid,
            self.event_ts,
            self.reason,
            self.yellowstone_output_queue_depth
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            self.yellowstone_output_queue_capacity
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            self.yellowstone_output_queue_fill_ratio
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "null".to_string()),
            self.yellowstone_output_oldest_age_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
        );
    }
}

fn latest_runtime_pressure_sample(config: &Config) -> Result<Option<RuntimePressureSample>> {
    let (source, text) = runtime_pressure_source_text(config)?;
    Ok(parse_runtime_pressure_sample_text(&text, source))
}

impl RuntimePressureMonitor {
    fn latest_sample<'a>(
        &'a mut self,
        config: &Config,
        now: DateTime<Utc>,
    ) -> Result<Option<&'a RuntimePressureSample>> {
        let refresh_due = self
            .refreshed_at
            .map(|refreshed_at| {
                now.signed_duration_since(refreshed_at).num_milliseconds()
                    >= DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS
            })
            .unwrap_or(true);
        if refresh_due {
            self.cached_sample = latest_runtime_pressure_sample(config)?;
            self.refreshed_at = Some(now);
        }
        Ok(self.cached_sample.as_ref())
    }
}

fn runtime_pressure_source_text(config: &Config) -> Result<(String, String)> {
    if let Some(log_path) = config.runtime_pressure_log_path.as_ref() {
        let text = fs::read_to_string(log_path).with_context(|| {
            format!("failed reading runtime pressure log {}", log_path.display())
        })?;
        return Ok((format!("log:{}", log_path.display()), text));
    }

    let output = Command::new("journalctl")
        .arg("-u")
        .arg(&config.runtime_pressure_service)
        .arg("-n")
        .arg(DEFAULT_RUNTIME_PRESSURE_JOURNAL_LINES.to_string())
        .arg("--no-pager")
        .arg("-o")
        .arg("cat")
        .output()
        .with_context(|| {
            format!(
                "failed collecting runtime pressure journal for service {}",
                config.runtime_pressure_service
            )
        })?;
    if !output.status.success() {
        bail!(
            "journalctl failed for runtime pressure service {} with status {}",
            config.runtime_pressure_service,
            output.status
        );
    }
    let text = String::from_utf8(output.stdout)
        .context("runtime pressure journal output was not valid utf-8")?;
    Ok((
        format!("journalctl:{}", config.runtime_pressure_service),
        text,
    ))
}

fn parse_runtime_pressure_sample_text(text: &str, source: String) -> Option<RuntimePressureSample> {
    text.lines()
        .filter_map(parse_runtime_pressure_sample_line)
        .last()
        .map(|sample| RuntimePressureSample { source, ..sample })
}

fn parse_runtime_pressure_sample_line(line: &str) -> Option<RuntimePressureSample> {
    let json_start = line.find('{')?;
    let sample_ts = parse_runtime_pressure_sample_ts(&line[..json_start])?;
    let payload = serde_json::from_str::<RuntimePressureLogPayload>(&line[json_start..]).ok()?;
    if payload.message != "ingestion pipeline metrics" {
        return None;
    }
    let yellowstone_output_queue_fill_ratio =
        payload.yellowstone_output_queue_fill_ratio.or_else(|| {
            match (
                payload.yellowstone_output_queue_depth,
                payload.yellowstone_output_queue_capacity,
            ) {
                (_, Some(0)) | (None, _) | (_, None) => None,
                (Some(depth), Some(capacity)) => Some(depth as f64 / capacity as f64),
            }
        });
    Some(RuntimePressureSample {
        source: String::new(),
        sample_ts,
        yellowstone_output_queue_depth: payload.yellowstone_output_queue_depth,
        yellowstone_output_queue_capacity: payload.yellowstone_output_queue_capacity,
        yellowstone_output_queue_fill_ratio,
        yellowstone_output_oldest_age_ms: payload.yellowstone_output_oldest_age_ms,
        ingestion_lag_ms_p95: payload.ingestion_lag_ms_p95,
    })
}

fn parse_runtime_pressure_sample_ts(prefix: &str) -> Option<DateTime<Utc>> {
    prefix.split_whitespace().rev().find_map(|token| {
        DateTime::parse_from_rfc3339(token)
            .ok()
            .map(|ts| ts.with_timezone(&Utc))
    })
}

fn runtime_pressure_breach_reason(
    config: &Config,
    sample: &RuntimePressureSample,
) -> Option<String> {
    let mut reasons = Vec::new();
    if let Some(fill_ratio) = sample.yellowstone_output_queue_fill_ratio {
        if fill_ratio >= config.max_yellowstone_fill_ratio {
            reasons.push(format!(
                "yellowstone_output_queue_fill_ratio={fill_ratio:.4} threshold={:.4}",
                config.max_yellowstone_fill_ratio
            ));
        }
    }
    if let Some(ingestion_lag_ms_p95) = sample.ingestion_lag_ms_p95 {
        if ingestion_lag_ms_p95 >= config.max_ingestion_lag_ms_p95 {
            reasons.push(format!(
                "ingestion_lag_ms_p95={} threshold_ms={}",
                ingestion_lag_ms_p95, config.max_ingestion_lag_ms_p95
            ));
        }
    }
    if reasons.is_empty() {
        return None;
    }
    Some(reasons.join(" "))
}

fn runtime_pressure_sample_stale_reason(
    config: &Config,
    sample: &RuntimePressureSample,
    now: DateTime<Utc>,
) -> Option<String> {
    let age_seconds = now
        .signed_duration_since(sample.sample_ts)
        .num_seconds()
        .max(0) as u64;
    if age_seconds <= config.max_runtime_pressure_sample_age_seconds {
        return None;
    }
    Some(format!(
        "runtime_pressure_sample_stale sample_ts={} age_seconds={} max_age_seconds={}",
        sample.sample_ts.to_rfc3339(),
        age_seconds,
        config.max_runtime_pressure_sample_age_seconds
    ))
}

fn bounded_run_stop_reason(
    config: &Config,
    run_started_at: DateTime<Utc>,
    batches: usize,
    now: DateTime<Utc>,
) -> Option<RunStopReason> {
    if config
        .max_batches_per_run
        .is_some_and(|max_batches| batches >= max_batches)
    {
        return Some(RunStopReason::StoppedDueToBatchBudget);
    }
    if config
        .max_runtime_seconds
        .is_some_and(|max_runtime_seconds| {
            now.signed_duration_since(run_started_at)
                .num_seconds()
                .max(0) as u64
                >= max_runtime_seconds
        })
    {
        return Some(RunStopReason::StoppedDueToRuntimeBudget);
    }
    None
}

fn run_outcome(stop_reason: RunStopReason, coverage_marked: bool) -> &'static str {
    if coverage_marked {
        "completed_and_marked_covered"
    } else {
        stop_reason.as_str()
    }
}

fn log_run_summary(
    stop_reason: RunStopReason,
    coverage_marked: bool,
    total_rows: usize,
    batches: usize,
    cursor: &Cursor,
    stage_totals: &BatchStageTimings,
) {
    println!(
        "summary outcome={} stop_reason={} coverage_marked={} total_rows={} batches={} final_cursor_ts={} final_cursor_slot={} final_cursor_signature={} scan_ms={} prepare_ms={} apply_ms={} rug_finalize_ms={} progress_update_ms={}",
        run_outcome(stop_reason, coverage_marked),
        stop_reason.as_str(),
        coverage_marked,
        total_rows,
        batches,
        cursor.ts.to_rfc3339(),
        cursor.slot,
        cursor.signature,
        stage_totals.scan_ms,
        stage_totals.prepare_ms,
        stage_totals.apply_ms,
        stage_totals.rug_finalize_ms,
        stage_totals.progress_update_ms,
    );
}

fn log_runtime_pressure_abort_event(config: &Config, sample: &RuntimePressureSample, reason: &str) {
    println!(
        "event=runtime_pressure_fast_abort source={} sample_ts={} reason={} yellowstone_output_queue_depth={} yellowstone_output_queue_capacity={} yellowstone_output_queue_fill_ratio={} yellowstone_output_oldest_age_ms={} ingestion_lag_ms_p95={} max_yellowstone_fill_ratio={:.4} max_ingestion_lag_ms_p95={} max_runtime_pressure_sample_age_seconds={}",
        sample.source,
        sample.sample_ts.to_rfc3339(),
        reason,
        sample
            .yellowstone_output_queue_depth
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        sample
            .yellowstone_output_queue_capacity
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        sample
            .yellowstone_output_queue_fill_ratio
            .map(|value| format!("{value:.4}"))
            .unwrap_or_else(|| "null".to_string()),
        sample
            .yellowstone_output_oldest_age_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        sample
            .ingestion_lag_ms_p95
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        config.max_yellowstone_fill_ratio,
        config.max_ingestion_lag_ms_p95,
        config.max_runtime_pressure_sample_age_seconds,
    );
}

fn cursor_matches_runtime(left: &Cursor, right: &DiscoveryRuntimeCursor) -> bool {
    left.ts == right.ts_utc && left.slot == right.slot && left.signature == right.signature
}

fn cmp_runtime_cursor(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn main() -> Result<()> {
    let config = parse_args()?;
    run(config)
}

fn parse_args() -> Result<Config> {
    let mut args = env::args().skip(1);
    let Some(db_path_raw) = args.next() else {
        bail!(
            "usage: backfill_discovery_scoring <db_path> --config <path> --start-ts <rfc3339> [--end-ts <rfc3339>] [--batch-size N] [--sleep-ms N] [--max-batches-per-run N] [--max-runtime-seconds N] (--reset | --seeded-reset --resume-ts <ts> --resume-slot <slot> --resume-signature <sig> | --resume-ts <ts> --resume-slot <slot> --resume-signature <sig>) [--stop-after-seed-install] [--mark-covered] [--abort-on-runtime-pressure] [--max-yellowstone-fill-ratio N] [--max-ingestion-lag-ms-p95 N] [--max-runtime-pressure-sample-age-seconds N] [--runtime-pressure-service NAME] [--runtime-pressure-log-path PATH] [--abort-on-runtime-infra-stop] [--helius-http-url URL] [--min-token-age-hint-seconds N]"
        );
    };

    let mut config_path: Option<PathBuf> = None;
    let mut start_ts: Option<DateTime<Utc>> = None;
    let mut end_ts: Option<DateTime<Utc>> = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut sleep_ms = 0u64;
    let mut max_batches_per_run: Option<usize> = None;
    let mut max_runtime_seconds: Option<u64> = None;
    let mut reset = false;
    let mut seeded_reset = false;
    let mut stop_after_seed_install = false;
    let mut mark_covered = false;
    let mut abort_on_runtime_pressure = false;
    let mut runtime_pressure_service = env::var("SERVICE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string());
    let mut runtime_pressure_log_path: Option<PathBuf> = None;
    let mut max_yellowstone_fill_ratio_override: Option<f64> = None;
    let mut max_ingestion_lag_ms_p95_override: Option<u64> = None;
    let mut max_runtime_pressure_sample_age_seconds_override: Option<u64> = None;
    let mut abort_on_runtime_infra_stop = false;
    let mut resume_ts: Option<DateTime<Utc>> = None;
    let mut resume_slot: Option<u64> = None;
    let mut resume_signature: Option<String> = None;
    let mut helius_http_url_override: Option<String> = None;
    let mut min_token_age_hint_seconds_override: Option<u64> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--start-ts" => start_ts = Some(parse_ts_arg("--start-ts", args.next())?),
            "--end-ts" => end_ts = Some(parse_ts_arg("--end-ts", args.next())?),
            "--batch-size" => batch_size = parse_usize_arg("--batch-size", args.next())?.max(1),
            "--sleep-ms" => sleep_ms = parse_u64_arg("--sleep-ms", args.next())?,
            "--max-batches-per-run" => {
                max_batches_per_run = Some(parse_usize_arg("--max-batches-per-run", args.next())?)
            }
            "--max-runtime-seconds" => {
                max_runtime_seconds = Some(parse_u64_arg("--max-runtime-seconds", args.next())?)
            }
            "--reset" => reset = true,
            "--seeded-reset" => seeded_reset = true,
            "--stop-after-seed-install" => stop_after_seed_install = true,
            "--mark-covered" => mark_covered = true,
            "--abort-on-runtime-pressure" => abort_on_runtime_pressure = true,
            "--max-yellowstone-fill-ratio" => {
                max_yellowstone_fill_ratio_override =
                    Some(parse_f64_arg("--max-yellowstone-fill-ratio", args.next())?)
            }
            "--max-ingestion-lag-ms-p95" => {
                max_ingestion_lag_ms_p95_override =
                    Some(parse_u64_arg("--max-ingestion-lag-ms-p95", args.next())?)
            }
            "--max-runtime-pressure-sample-age-seconds" => {
                max_runtime_pressure_sample_age_seconds_override = Some(parse_u64_arg(
                    "--max-runtime-pressure-sample-age-seconds",
                    args.next(),
                )?)
            }
            "--runtime-pressure-service" => {
                runtime_pressure_service =
                    parse_string_arg("--runtime-pressure-service", args.next())?
            }
            "--runtime-pressure-log-path" => {
                runtime_pressure_log_path = Some(PathBuf::from(parse_string_arg(
                    "--runtime-pressure-log-path",
                    args.next(),
                )?))
            }
            "--abort-on-runtime-infra-stop" => abort_on_runtime_infra_stop = true,
            "--resume-ts" => resume_ts = Some(parse_ts_arg("--resume-ts", args.next())?),
            "--resume-slot" => resume_slot = Some(parse_u64_arg("--resume-slot", args.next())?),
            "--resume-signature" => {
                resume_signature = Some(parse_string_arg("--resume-signature", args.next())?)
            }
            "--helius-http-url" => {
                helius_http_url_override = Some(parse_string_arg("--helius-http-url", args.next())?)
            }
            "--min-token-age-hint-seconds" => {
                min_token_age_hint_seconds_override =
                    Some(parse_u64_arg("--min-token-age-hint-seconds", args.next())?)
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let config_path = config_path.ok_or_else(|| anyhow!("missing required --config"))?;
    let start_ts = start_ts.ok_or_else(|| anyhow!("missing required --start-ts"))?;
    if let Some(end_ts) = end_ts {
        if end_ts < start_ts {
            bail!("--end-ts must be >= --start-ts");
        }
    }
    if mark_covered && end_ts.is_some() {
        bail!("--mark-covered requires a full forward run without --end-ts");
    }

    let resume_after = match (resume_ts, resume_slot, resume_signature) {
        (None, None, None) => None,
        (Some(ts), Some(slot), Some(signature)) => Some(Cursor {
            ts,
            slot,
            signature,
        }),
        _ => {
            bail!("resume requires the full triple: --resume-ts, --resume-slot, --resume-signature")
        }
    };

    if reset && seeded_reset {
        bail!("--reset cannot be combined with --seeded-reset");
    }
    if reset && resume_after.is_some() {
        bail!("--reset cannot be combined with --resume-*");
    }
    if seeded_reset && resume_after.is_none() {
        bail!("--seeded-reset requires exact --resume-* cursor");
    }
    if !reset && !seeded_reset && resume_after.is_none() {
        bail!("refusing non-idempotent replay without either --reset, --seeded-reset with exact --resume-*, or exact --resume-* cursor");
    }
    if seeded_reset && end_ts.is_some() {
        bail!("--seeded-reset does not accept --end-ts; boundary start is defined by exact seeded boundary cursor");
    }
    if stop_after_seed_install && !seeded_reset {
        bail!("--stop-after-seed-install requires --seeded-reset");
    }
    if max_batches_per_run.is_some_and(|value| value == 0) {
        bail!("--max-batches-per-run must be >= 1");
    }
    if max_runtime_seconds.is_some_and(|value| value == 0) {
        bail!("--max-runtime-seconds must be >= 1");
    }

    let loaded_config = load_from_path(&config_path)
        .with_context(|| format!("failed loading config {}", config_path.display()))?;
    if loaded_config.discovery.scoring_aggregates_write_enabled {
        bail!(
            "backfill requires discovery.scoring_aggregates_write_enabled=false in the target runtime config"
        );
    }
    if loaded_config.discovery.scoring_aggregates_enabled {
        bail!(
            "backfill requires discovery.scoring_aggregates_enabled=false in the target runtime config"
        );
    }
    let aggregate_write_config = DiscoveryAggregateWriteConfig {
        max_tx_per_minute: loaded_config.discovery.max_tx_per_minute,
        rug_lookahead_seconds: loaded_config.discovery.rug_lookahead_seconds as u32,
        helius_http_url: helius_http_url_override,
        min_token_age_hint_seconds: min_token_age_hint_seconds_override
            .or(Some(loaded_config.shadow.min_token_age_seconds)),
    };
    let seeded_reset_max_start_ts = Some(
        Utc::now()
            - Duration::days(i64::from(
                loaded_config.discovery.scoring_window_days.max(1),
            )),
    );
    let max_yellowstone_fill_ratio = max_yellowstone_fill_ratio_override
        .unwrap_or(DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO);
    if !max_yellowstone_fill_ratio.is_finite()
        || max_yellowstone_fill_ratio <= 0.0
        || max_yellowstone_fill_ratio > 1.0
    {
        bail!(
            "--max-yellowstone-fill-ratio must be finite and in (0, 1], got {}",
            max_yellowstone_fill_ratio
        );
    }
    let max_ingestion_lag_ms_p95 = max_ingestion_lag_ms_p95_override
        .unwrap_or(loaded_config.risk.shadow_infra_lag_p95_threshold_ms);
    if max_ingestion_lag_ms_p95 == 0 {
        bail!("--max-ingestion-lag-ms-p95 must be >= 1");
    }
    let max_runtime_pressure_sample_age_seconds = max_runtime_pressure_sample_age_seconds_override
        .unwrap_or(
            loaded_config
                .ingestion
                .telemetry_report_seconds
                .max(5)
                .saturating_add(DEFAULT_RUNTIME_PRESSURE_SAMPLE_AGE_GRACE_SECONDS),
        );
    if max_runtime_pressure_sample_age_seconds == 0 {
        bail!("--max-runtime-pressure-sample-age-seconds must be >= 1");
    }

    Ok(Config {
        db_path: PathBuf::from(db_path_raw),
        start_ts,
        seeded_reset_max_start_ts,
        end_ts,
        batch_size,
        sleep_ms,
        max_batches_per_run,
        max_runtime_seconds,
        reset,
        seeded_reset,
        stop_after_seed_install,
        mark_covered,
        resume_after,
        abort_on_runtime_pressure,
        runtime_pressure_service,
        runtime_pressure_log_path,
        max_yellowstone_fill_ratio,
        max_ingestion_lag_ms_p95,
        max_runtime_pressure_sample_age_seconds,
        abort_on_runtime_infra_stop,
        aggregate_write_config,
    })
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

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    parse_string_arg(flag, value)?
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    parse_string_arg(flag, value)?
        .parse::<usize>()
        .with_context(|| format!("invalid integer for {flag}"))
}

fn parse_f64_arg(flag: &str, value: Option<String>) -> Result<f64> {
    parse_string_arg(flag, value)?
        .parse::<f64>()
        .with_context(|| format!("invalid float for {flag}"))
}

fn sanitize_log_value(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_whitespace() || ch.is_control() {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

fn run(config: Config) -> Result<()> {
    let termination_requested = install_termination_signal_handlers()?;
    let mut store = SqliteStore::open(Path::new(&config.db_path))
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migrations_dir).with_context(|| {
        format!(
            "failed applying migrations from {}",
            migrations_dir.display()
        )
    })?;

    run_with_cleanup(&mut store, &config, termination_requested.as_ref())
}

fn run_with_store(store: &mut SqliteStore, config: &Config) -> Result<()> {
    run_with_store_stop_reason(store, config).map(|_| ())
}

fn run_with_store_stop_reason(store: &mut SqliteStore, config: &Config) -> Result<RunStopReason> {
    let termination_requested = AtomicBool::new(false);
    run_with_store_inner(store, config, &termination_requested)
}

fn run_with_cleanup(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
) -> Result<()> {
    let run_result = run_with_store_inner(store, config, termination_requested);
    let clear_result = store.clear_discovery_scoring_backfill_source_protection();
    match (run_result, clear_result) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(run_error), Err(clear_error)) => Err(run_error).context(format!(
            "backfill failed and cleanup of source protection also failed: {clear_error:#}"
        )),
    }
}

fn validate_resume_contract(
    store: &SqliteStore,
    config: &Config,
) -> Result<Option<(DateTime<Utc>, DiscoveryRuntimeCursor)>> {
    let Some(resume_after) = config.resume_after.as_ref() else {
        return Ok(None);
    };
    let Some((progress_start_ts, progress_cursor)) =
        store.load_discovery_scoring_backfill_progress()?
    else {
        if config.seeded_reset {
            bail!(
                "seeded reset requires persisted backfill progress proving exact lower-bound lineage from the current partial materialized state"
            );
        }
        bail!(
            "resumed backfill requires persisted backfill progress proving continuous lineage from start_ts; use --reset for a new rebuild"
        );
    };
    if config.seeded_reset {
        if !cursor_matches_runtime(resume_after, &progress_cursor) {
            bail!(
                "seeded reset resume cursor does not match persisted backfill progress for the current partial lineage"
            );
        }
        return Ok(Some((progress_start_ts, progress_cursor)));
    }
    if progress_start_ts != config.start_ts
        || !cursor_matches_runtime(resume_after, &progress_cursor)
    {
        if config.mark_covered {
            bail!(
                "--mark-covered resume cursor does not match persisted backfill progress for the requested start_ts"
            );
        }
        bail!(
            "resumed backfill cursor does not match persisted backfill progress for the requested start_ts"
        );
    }
    Ok(Some((progress_start_ts, progress_cursor)))
}

fn validate_seeded_reset_boundary_contract(config: &Config) -> Result<()> {
    if !config.seeded_reset {
        return Ok(());
    }
    let Some(max_start_ts) = config.seeded_reset_max_start_ts else {
        bail!(
            "seeded reset requires an effective scoring horizon boundary derived from discovery.scoring_window_days"
        );
    };
    if config.start_ts > max_start_ts {
        bail!(
            "seeded reset start_ts {} exceeds the effective scoring horizon start at launch {}; exact seeded narrowing only allows the effective scoring horizon start or earlier",
            config.start_ts.to_rfc3339(),
            max_start_ts.to_rfc3339(),
        );
    }
    Ok(())
}

fn run_seeded_boundary_install_and_replay(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
    persisted_progress_start_ts: DateTime<Utc>,
    persisted_progress_cursor: &DiscoveryRuntimeCursor,
    run_started_at: DateTime<Utc>,
) -> Result<SeededReplayOutcome> {
    let resume_after = config
        .resume_after
        .as_ref()
        .ok_or_else(|| anyhow!("seeded reset requires exact --resume-* cursor"))?;
    if !cursor_matches_runtime(resume_after, persisted_progress_cursor) {
        bail!("seeded reset resume cursor does not match persisted backfill progress for the current partial lineage");
    }
    if resume_after.ts >= config.start_ts {
        bail!(
            "seeded reset requires the current partial replay cursor to stay strictly before the requested boundary start_ts"
        );
    }
    if let Some(gap_cursor) = store.load_discovery_scoring_materialization_gap_cursor()? {
        if cmp_runtime_cursor(&gap_cursor, persisted_progress_cursor) != std::cmp::Ordering::Greater
        {
            bail!(
                "latched discovery scoring continuity gap at {} / {} / {} is at or before the current partial replay cursor; exact seeded lower-bound state is unsupported until the gap is repaired",
                gap_cursor.ts_utc.to_rfc3339(),
                gap_cursor.slot,
                gap_cursor.signature
            );
        }
    }

    let boundary_build = run_boundary_build_phase(
        store,
        config,
        termination_requested,
        runtime_pressure_monitor,
        persisted_progress_start_ts,
        resume_after.clone(),
        run_started_at,
    )?;

    if !matches!(
        boundary_build.replay.stop_reason,
        RunStopReason::CompletedRequestedEndTs | RunStopReason::CompletedSourceExhausted
    ) {
        return Ok(SeededReplayOutcome::Stopped(boundary_build.replay));
    }

    let preserved_gap_cursor = match store.load_discovery_scoring_materialization_gap_cursor()? {
        Some(gap_cursor)
            if cmp_runtime_cursor(
                &gap_cursor,
                &DiscoveryRuntimeCursor {
                    ts_utc: boundary_build.replay.cursor.ts,
                    slot: boundary_build.replay.cursor.slot,
                    signature: boundary_build.replay.cursor.signature.clone(),
                },
            ) != std::cmp::Ordering::Greater =>
        {
            if !boundary_build.replay.gap_cursor_observed {
                bail!(
                    "latched discovery scoring continuity gap at {} / {} / {} is at or before the exact seeded boundary cursor but was not observed during boundary construction",
                    gap_cursor.ts_utc.to_rfc3339(),
                    gap_cursor.slot,
                    gap_cursor.signature
                );
            }
            None
        }
        other => other,
    };

    let seed_snapshot = boundary_build
        .seed_snapshot
        .ok_or_else(|| anyhow!("boundary build completed without a durable seed snapshot"))?;
    println!(
        "event=seed_boundary_exported boundary_start_ts={} boundary_cursor_ts={} boundary_cursor_slot={} boundary_cursor_signature={} seed_lot_count={} durable=false",
        seed_snapshot.boundary_start_ts.to_rfc3339(),
        seed_snapshot.boundary_cursor.ts_utc.to_rfc3339(),
        seed_snapshot.boundary_cursor.slot,
        seed_snapshot.boundary_cursor.signature,
        seed_snapshot.open_lots.len(),
    );
    maybe_fire_backfill_test_failpoint(
        BackfillTestFailpoint::AfterBoundaryExportBeforeSeedInstall,
    )?;

    store.reset_discovery_scoring_tables_and_install_boundary_seed_snapshot(
        &seed_snapshot,
        preserved_gap_cursor.as_ref(),
    )?;
    refresh_backfill_source_protection(store, config.start_ts)?;
    println!(
        "event=seed_boundary_installed boundary_start_ts={} boundary_cursor_ts={} boundary_cursor_slot={} boundary_cursor_signature={} seed_lot_count={} durable=true replay_resume_semantics=strictly_after_boundary_cursor",
        seed_snapshot.boundary_start_ts.to_rfc3339(),
        seed_snapshot.boundary_cursor.ts_utc.to_rfc3339(),
        seed_snapshot.boundary_cursor.slot,
        seed_snapshot.boundary_cursor.signature,
        seed_snapshot.open_lots.len(),
    );
    maybe_fire_backfill_test_failpoint(
        BackfillTestFailpoint::AfterCommittedSeedInstallBeforeReplayAfterSeed,
    )?;

    if config.stop_after_seed_install {
        println!(
            "event=seed_boundary_stop_requested boundary_start_ts={} boundary_cursor_ts={} boundary_cursor_slot={} boundary_cursor_signature={} seed_lot_count={} durable=true requested_stop=after_seed_install",
            seed_snapshot.boundary_start_ts.to_rfc3339(),
            seed_snapshot.boundary_cursor.ts_utc.to_rfc3339(),
            seed_snapshot.boundary_cursor.slot,
            seed_snapshot.boundary_cursor.signature,
            seed_snapshot.open_lots.len(),
        );
        return Ok(SeededReplayOutcome::Stopped(ReplayLoopOutcome {
            stop_reason: RunStopReason::StoppedAfterSeedInstall,
            cursor: Cursor {
                ts: seed_snapshot.boundary_cursor.ts_utc,
                slot: seed_snapshot.boundary_cursor.slot,
                signature: seed_snapshot.boundary_cursor.signature.clone(),
            },
            total_rows: boundary_build.replay.total_rows,
            batches: boundary_build.replay.batches,
            gap_cursor_observed: boundary_build.replay.gap_cursor_observed,
            stage_totals: boundary_build.replay.stage_totals,
        }));
    }

    Ok(SeededReplayOutcome::ReplayAfterSeed(run_replay_phase(
        store,
        config,
        termination_requested,
        runtime_pressure_monitor,
        "replay_after_seed",
        config.start_ts,
        Cursor {
            ts: seed_snapshot.boundary_cursor.ts_utc,
            slot: seed_snapshot.boundary_cursor.slot,
            signature: seed_snapshot.boundary_cursor.signature.clone(),
        },
        None,
        true,
        run_started_at,
        boundary_build.replay.total_rows,
        boundary_build.replay.batches,
    )?))
}

fn run_boundary_build_phase(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
    progress_start_ts: DateTime<Utc>,
    starting_cursor: Cursor,
    run_started_at: DateTime<Utc>,
) -> Result<BoundarySeedBuildOutcome> {
    let gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let mut gap_cursor_observed = false;
    let mut cursor = starting_cursor.clone();
    let mut total_rows = 0usize;
    let mut batches = 0usize;
    let mut stage_totals = BatchStageTimings::default();
    let mut builder = store.begin_discovery_scoring_boundary_lot_builder()?;

    let stop_reason = loop {
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            &cursor,
            runtime_pressure_monitor,
        )?;
        if let Some(reason) = bounded_run_stop_reason(config, run_started_at, batches, Utc::now()) {
            break reason;
        }

        let scan_started_at = Instant::now();
        let mut page = Vec::<SwapEvent>::with_capacity(config.batch_size);
        let mut reached_end_ts = false;
        let rows_seen = store.for_each_observed_swap_after_cursor(
            cursor.ts,
            cursor.slot,
            cursor.signature.as_str(),
            config.batch_size,
            |swap| {
                if swap.ts_utc >= config.start_ts {
                    reached_end_ts = true;
                    return Ok(());
                }
                if gap_cursor.as_ref().is_some_and(|gap_cursor| {
                    gap_cursor.ts_utc == swap.ts_utc
                        && gap_cursor.slot == swap.slot
                        && gap_cursor.signature == swap.signature
                }) {
                    gap_cursor_observed = true;
                }
                page.push(swap);
                Ok(())
            },
        )?;
        let scan_ms = scan_started_at.elapsed().as_millis() as u64;

        if page.is_empty() {
            break if reached_end_ts {
                RunStopReason::CompletedRequestedEndTs
            } else {
                RunStopReason::CompletedSourceExhausted
            };
        }

        let last_swap = page
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("boundary build page unexpectedly empty"))?;
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            &cursor,
            runtime_pressure_monitor,
        )?;
        refresh_backfill_source_protection(store, progress_start_ts)?;

        let storage_timings = store
            .advance_discovery_scoring_boundary_lot_builder_in_memory_with_timings(
                &mut builder,
                &page,
            )?;
        cursor = Cursor {
            ts: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        total_rows = total_rows.saturating_add(page.len());
        batches = batches.saturating_add(1);
        stage_totals.scan_ms = stage_totals.scan_ms.saturating_add(scan_ms);
        stage_totals.prepare_ms = stage_totals
            .prepare_ms
            .saturating_add(storage_timings.prepare_ms);
        stage_totals.apply_ms = stage_totals
            .apply_ms
            .saturating_add(storage_timings.apply_ms);

        println!(
            "event=boundary_batch_buffered phase=boundary_build replay_engine=lot_only_boundary durable=false rows={} total_rows={} batches={} cursor_ts={} cursor_slot={} cursor_signature={} scan_ms={} prepare_ms={} apply_ms={} rug_finalize_ms={} progress_update_ms={}",
            page.len(),
            total_rows,
            batches,
            cursor.ts.to_rfc3339(),
            cursor.slot,
            cursor.signature,
            scan_ms,
            storage_timings.prepare_ms,
            storage_timings.apply_ms,
            0,
            0,
        );

        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            &cursor,
            runtime_pressure_monitor,
        )?;
        if let Some(reason) = bounded_run_stop_reason(config, run_started_at, batches, Utc::now()) {
            break reason;
        }
        if reached_end_ts || rows_seen < config.batch_size {
            break if reached_end_ts {
                RunStopReason::CompletedRequestedEndTs
            } else {
                RunStopReason::CompletedSourceExhausted
            };
        }
        if config.sleep_ms > 0 {
            if let Some(reason) = sleep_with_interrupt(
                store,
                config,
                config.sleep_ms,
                termination_requested,
                total_rows,
                batches,
                &cursor,
                run_started_at,
                runtime_pressure_monitor,
            )? {
                break reason;
            }
        }
    };

    let replay = ReplayLoopOutcome {
        stop_reason,
        cursor: cursor.clone(),
        total_rows,
        batches,
        gap_cursor_observed,
        stage_totals,
    };
    let seed_snapshot = if matches!(
        stop_reason,
        RunStopReason::CompletedRequestedEndTs | RunStopReason::CompletedSourceExhausted
    ) {
        Some(store.export_discovery_scoring_boundary_lot_seed_snapshot(
            &builder,
            config.start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: cursor.ts,
                slot: cursor.slot,
                signature: cursor.signature,
            },
        )?)
    } else {
        None
    };

    Ok(BoundarySeedBuildOutcome {
        replay,
        seed_snapshot,
    })
}

fn run_replay_phase(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
    phase_label: &str,
    progress_start_ts: DateTime<Utc>,
    starting_cursor: Cursor,
    phase_end_ts: Option<DateTime<Utc>>,
    end_ts_inclusive: bool,
    run_started_at: DateTime<Utc>,
    starting_total_rows: usize,
    starting_batches: usize,
) -> Result<ReplayLoopOutcome> {
    let gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let mut gap_cursor_observed = false;
    let mut cursor = starting_cursor;
    let mut total_rows = starting_total_rows;
    let mut batches = starting_batches;
    let mut stage_totals = BatchStageTimings::default();
    let mut builder = match store.begin_discovery_scoring_replay_builder(
        cursor.ts,
        cursor.slot,
        cursor.signature.as_str(),
    ) {
        Ok(builder) => Some(builder),
        Err(error) if config.seeded_reset => return Err(error),
        Err(error) => {
            println!(
                "event=builder_replay_unavailable phase={} reason={}",
                phase_label,
                sanitize_log_value(&format!("{error:#}"))
            );
            None
        }
    };

    let stop_reason = loop {
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            &cursor,
            runtime_pressure_monitor,
        )?;
        if let Some(reason) = bounded_run_stop_reason(config, run_started_at, batches, Utc::now()) {
            break reason;
        }

        let scan_started_at = Instant::now();
        let mut page = Vec::<SwapEvent>::with_capacity(config.batch_size);
        let mut reached_end_ts = false;
        let rows_seen = store.for_each_observed_swap_after_cursor(
            cursor.ts,
            cursor.slot,
            cursor.signature.as_str(),
            config.batch_size,
            |swap| {
                if phase_end_ts.is_some_and(|end_ts| {
                    if end_ts_inclusive {
                        swap.ts_utc > end_ts
                    } else {
                        swap.ts_utc >= end_ts
                    }
                }) {
                    reached_end_ts = true;
                    return Ok(());
                }
                if gap_cursor.as_ref().is_some_and(|gap_cursor| {
                    gap_cursor.ts_utc == swap.ts_utc
                        && gap_cursor.slot == swap.slot
                        && gap_cursor.signature == swap.signature
                }) {
                    gap_cursor_observed = true;
                }
                page.push(swap);
                Ok(())
            },
        )?;
        let scan_ms = scan_started_at.elapsed().as_millis() as u64;

        if page.is_empty() {
            break if reached_end_ts {
                RunStopReason::CompletedRequestedEndTs
            } else {
                RunStopReason::CompletedSourceExhausted
            };
        }

        let last_swap = page
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("backfill page unexpectedly empty"))?;
        let next_cursor = Cursor {
            ts: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            &cursor,
            runtime_pressure_monitor,
        )?;
        refresh_backfill_source_protection(store, progress_start_ts)?;

        let (storage_timings, replay_engine): (_, &str) = if let Some(builder) = builder.as_mut() {
            (
                store.apply_discovery_scoring_builder_batch_and_checkpoint_with_timings(
                    builder,
                    &page,
                    &config.aggregate_write_config,
                    progress_start_ts,
                    &DiscoveryRuntimeCursor {
                        ts_utc: next_cursor.ts,
                        slot: next_cursor.slot,
                        signature: next_cursor.signature.clone(),
                    },
                )?,
                "builder",
            )
        } else {
            (
                store.apply_discovery_scoring_batch_and_checkpoint_with_timings(
                    &page,
                    &config.aggregate_write_config,
                    progress_start_ts,
                    &DiscoveryRuntimeCursor {
                        ts_utc: next_cursor.ts,
                        slot: next_cursor.slot,
                        signature: next_cursor.signature.clone(),
                    },
                )?,
                "sql",
            )
        };
        let rug_finalize_ms = if builder.is_some() {
            0
        } else {
            store.finalize_discovery_scoring_rug_facts_with_timing(last_swap.ts_utc)?
        };

        cursor = next_cursor;

        let batch_rows = page.len();
        total_rows = total_rows.saturating_add(batch_rows);
        batches = batches.saturating_add(1);
        let timings = BatchStageTimings {
            scan_ms,
            prepare_ms: storage_timings.prepare_ms,
            apply_ms: storage_timings.apply_ms,
            rug_finalize_ms,
            progress_update_ms: storage_timings.progress_update_ms,
        };
        stage_totals.scan_ms = stage_totals.scan_ms.saturating_add(timings.scan_ms);
        stage_totals.prepare_ms = stage_totals.prepare_ms.saturating_add(timings.prepare_ms);
        stage_totals.apply_ms = stage_totals.apply_ms.saturating_add(timings.apply_ms);
        stage_totals.rug_finalize_ms = stage_totals
            .rug_finalize_ms
            .saturating_add(timings.rug_finalize_ms);
        stage_totals.progress_update_ms = stage_totals
            .progress_update_ms
            .saturating_add(timings.progress_update_ms);
        println!(
            "event=batch_committed phase={} replay_engine={} rows={} total_rows={} batches={} cursor_ts={} cursor_slot={} cursor_signature={} scan_ms={} prepare_ms={} apply_ms={} rug_finalize_ms={} progress_update_ms={}",
            phase_label,
            replay_engine,
            batch_rows,
            total_rows,
            batches,
            cursor.ts.to_rfc3339(),
            cursor.slot,
            cursor.signature,
            timings.scan_ms,
            timings.prepare_ms,
            timings.apply_ms,
            timings.rug_finalize_ms,
            timings.progress_update_ms,
        );

        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            &cursor,
            runtime_pressure_monitor,
        )?;
        if let Some(reason) = bounded_run_stop_reason(config, run_started_at, batches, Utc::now()) {
            break reason;
        }
        if reached_end_ts || rows_seen < config.batch_size {
            break if reached_end_ts {
                RunStopReason::CompletedRequestedEndTs
            } else {
                RunStopReason::CompletedSourceExhausted
            };
        }
        if config.sleep_ms > 0 {
            if let Some(reason) = sleep_with_interrupt(
                store,
                config,
                config.sleep_ms,
                termination_requested,
                total_rows,
                batches,
                &cursor,
                run_started_at,
                runtime_pressure_monitor,
            )? {
                break reason;
            }
        }
    };

    Ok(ReplayLoopOutcome {
        stop_reason,
        cursor,
        total_rows,
        batches,
        gap_cursor_observed,
        stage_totals,
    })
}

fn run_with_store_inner(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
) -> Result<RunStopReason> {
    let run_started_at = Utc::now();
    let mut runtime_pressure_monitor = RuntimePressureMonitor::default();
    let initial_cursor = config.resume_after.clone().unwrap_or_else(|| Cursor {
        ts: config.start_ts,
        slot: 0,
        signature: String::new(),
    });
    abort_if_control_requested(
        store,
        config,
        termination_requested,
        0,
        0,
        &initial_cursor,
        &mut runtime_pressure_monitor,
    )?;
    if config.reset {
        store.reset_discovery_scoring_tables()?;
        println!("event=reset_discovery_scoring_tables");
    }
    validate_seeded_reset_boundary_contract(config)?;
    refresh_backfill_source_protection(store, config.start_ts)?;
    if !config.seeded_reset {
        validate_resume_contract(store, config)?;
    }
    let final_outcome;

    if config.seeded_reset {
        let (persisted_progress_start_ts, persisted_progress_cursor) = store
            .load_discovery_scoring_backfill_progress()?
            .ok_or_else(|| anyhow!("seeded reset requires persisted backfill progress"))?;
        let committed_seed_boundary =
            store.load_discovery_scoring_seed_boundary_install_marker()?;
        if let Some(seed_boundary_marker) = committed_seed_boundary.as_ref() {
            if seed_boundary_marker.boundary_start_ts == config.start_ts {
                if persisted_progress_start_ts != config.start_ts {
                    bail!(
                        "seeded reset found a durable seed_boundary_installed marker for {} but persisted backfill progress still points to {}; restart state is inconsistent",
                        config.start_ts.to_rfc3339(),
                        persisted_progress_start_ts.to_rfc3339(),
                    );
                }
                if cmp_runtime_cursor(
                    &persisted_progress_cursor,
                    &seed_boundary_marker.boundary_cursor,
                ) == std::cmp::Ordering::Less
                {
                    bail!(
                        "seeded reset found a durable seed_boundary_installed marker for {} but persisted replay progress is still behind the committed boundary cursor",
                        config.start_ts.to_rfc3339(),
                    );
                }
                println!(
                    "event=seed_boundary_resume_from_persisted_progress boundary_start_ts={} boundary_cursor_ts={} boundary_cursor_slot={} boundary_cursor_signature={} replay_resume_cursor_ts={} replay_resume_cursor_slot={} replay_resume_cursor_signature={}",
                    seed_boundary_marker.boundary_start_ts.to_rfc3339(),
                    seed_boundary_marker.boundary_cursor.ts_utc.to_rfc3339(),
                    seed_boundary_marker.boundary_cursor.slot,
                    seed_boundary_marker.boundary_cursor.signature,
                    persisted_progress_cursor.ts_utc.to_rfc3339(),
                    persisted_progress_cursor.slot,
                    persisted_progress_cursor.signature,
                );
                if config.stop_after_seed_install {
                    println!(
                        "event=seed_boundary_stop_requested boundary_start_ts={} boundary_cursor_ts={} boundary_cursor_slot={} boundary_cursor_signature={} durable=true requested_stop=after_seed_install already_committed=true",
                        seed_boundary_marker.boundary_start_ts.to_rfc3339(),
                        seed_boundary_marker.boundary_cursor.ts_utc.to_rfc3339(),
                        seed_boundary_marker.boundary_cursor.slot,
                        seed_boundary_marker.boundary_cursor.signature,
                    );
                    final_outcome = ReplayLoopOutcome {
                        stop_reason: RunStopReason::StoppedAfterSeedInstall,
                        cursor: Cursor {
                            ts: persisted_progress_cursor.ts_utc,
                            slot: persisted_progress_cursor.slot,
                            signature: persisted_progress_cursor.signature.clone(),
                        },
                        total_rows: 0,
                        batches: 0,
                        gap_cursor_observed: false,
                        stage_totals: BatchStageTimings::default(),
                    };
                } else {
                    final_outcome = run_replay_phase(
                        store,
                        config,
                        termination_requested,
                        &mut runtime_pressure_monitor,
                        "replay_after_seed",
                        config.start_ts,
                        Cursor {
                            ts: persisted_progress_cursor.ts_utc,
                            slot: persisted_progress_cursor.slot,
                            signature: persisted_progress_cursor.signature.clone(),
                        },
                        None,
                        true,
                        run_started_at,
                        0,
                        0,
                    )?;
                }
            } else if persisted_progress_start_ts == config.start_ts {
                bail!(
                    "seeded reset found persisted backfill progress at {} but the durable seed_boundary_installed marker belongs to {}; restart state is ambiguous",
                    config.start_ts.to_rfc3339(),
                    seed_boundary_marker.boundary_start_ts.to_rfc3339(),
                );
            } else {
                match run_seeded_boundary_install_and_replay(
                    store,
                    config,
                    termination_requested,
                    &mut runtime_pressure_monitor,
                    persisted_progress_start_ts,
                    &persisted_progress_cursor,
                    run_started_at,
                )? {
                    SeededReplayOutcome::Stopped(boundary_replay) => {
                        log_run_summary(
                            boundary_replay.stop_reason,
                            false,
                            boundary_replay.total_rows,
                            boundary_replay.batches,
                            &boundary_replay.cursor,
                            &boundary_replay.stage_totals,
                        );
                        return Ok(boundary_replay.stop_reason);
                    }
                    SeededReplayOutcome::ReplayAfterSeed(replay_after_seed) => {
                        final_outcome = replay_after_seed;
                    }
                }
            }
        } else if persisted_progress_start_ts == config.start_ts {
            bail!(
                "seeded reset found persisted backfill progress at {} without a durable seed_boundary_installed marker; restart state is ambiguous",
                config.start_ts.to_rfc3339(),
            );
        } else {
            match run_seeded_boundary_install_and_replay(
                store,
                config,
                termination_requested,
                &mut runtime_pressure_monitor,
                persisted_progress_start_ts,
                &persisted_progress_cursor,
                run_started_at,
            )? {
                SeededReplayOutcome::Stopped(boundary_replay) => {
                    log_run_summary(
                        boundary_replay.stop_reason,
                        false,
                        boundary_replay.total_rows,
                        boundary_replay.batches,
                        &boundary_replay.cursor,
                        &boundary_replay.stage_totals,
                    );
                    return Ok(boundary_replay.stop_reason);
                }
                SeededReplayOutcome::ReplayAfterSeed(replay_after_seed) => {
                    final_outcome = replay_after_seed;
                }
            }
        }
    } else {
        final_outcome = run_replay_phase(
            store,
            config,
            termination_requested,
            &mut runtime_pressure_monitor,
            "direct_replay",
            config.start_ts,
            initial_cursor,
            config.end_ts,
            true,
            run_started_at,
            0,
            0,
        )?;
    }

    let full_forward_completion = matches!(
        final_outcome.stop_reason,
        RunStopReason::CompletedSourceExhausted
    ) && config.end_ts.is_none();
    let skip_final_rug_finalize = matches!(
        final_outcome.stop_reason,
        RunStopReason::StoppedAfterSeedInstall
    );
    let final_finalize_ms = if skip_final_rug_finalize {
        println!(
            "event=final_rug_finalize_skipped phase=run_complete reason=stopped_after_seed_install watermark_ts={}",
            final_outcome.cursor.ts.to_rfc3339(),
        );
        0
    } else {
        let finalize_rug_facts_until = if matches!(
            final_outcome.stop_reason,
            RunStopReason::CompletedRequestedEndTs
        ) {
            config.end_ts.unwrap_or(final_outcome.cursor.ts)
        } else {
            final_outcome.cursor.ts
        };
        let final_finalize_ms =
            store.finalize_discovery_scoring_rug_facts_with_timing(finalize_rug_facts_until)?;
        println!(
            "event=final_rug_finalize phase=run_complete watermark_ts={} rug_finalize_ms={}",
            finalize_rug_facts_until.to_rfc3339(),
            final_finalize_ms,
        );
        final_finalize_ms
    };

    if full_forward_completion {
        if let Some(gap_cursor) = store.load_discovery_scoring_materialization_gap_cursor()? {
            if !final_outcome.gap_cursor_observed {
                bail!(
                    "latched discovery scoring continuity gap at {} / {} / {} was not observed during full forward replay; source rows may be missing or replay started too late",
                    gap_cursor.ts_utc.to_rfc3339(),
                    gap_cursor.slot,
                    gap_cursor.signature
                );
            }
            store.clear_discovery_scoring_materialization_gap_if_cursor_observed(&gap_cursor)?;
        }
    }

    let coverage_marked = config.mark_covered && full_forward_completion;
    if coverage_marked {
        store.set_discovery_scoring_covered_since(config.start_ts)?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: final_outcome.cursor.ts,
            slot: final_outcome.cursor.slot,
            signature: final_outcome.cursor.signature.clone(),
        })?;
        store.clear_discovery_scoring_backfill_progress()?;
        println!(
            "event=coverage_marked covered_since_ts={} covered_through_ts={} covered_through_slot={} covered_through_signature={}",
            config.start_ts.to_rfc3339(),
            final_outcome.cursor.ts.to_rfc3339(),
            final_outcome.cursor.slot,
            final_outcome.cursor.signature
        );
    } else {
        println!(
            "event=coverage_not_marked reason={}",
            if matches!(
                final_outcome.stop_reason,
                RunStopReason::StoppedAfterSeedInstall
            ) {
                "seed_install_stop_requested"
            } else if config.mark_covered && !full_forward_completion {
                "completion_required"
            } else {
                "not_requested"
            }
        );
    }

    if let Ok((busy, log_frames, checkpointed_frames)) = store.checkpoint_wal_truncate() {
        println!(
            "event=wal_checkpoint busy={} log_frames={} checkpointed_frames={}",
            busy, log_frames, checkpointed_frames
        );
    }

    let summary_stage_totals = BatchStageTimings {
        rug_finalize_ms: final_outcome
            .stage_totals
            .rug_finalize_ms
            .saturating_add(final_finalize_ms),
        ..final_outcome.stage_totals
    };
    log_run_summary(
        final_outcome.stop_reason,
        coverage_marked,
        final_outcome.total_rows,
        final_outcome.batches,
        &final_outcome.cursor,
        &summary_stage_totals,
    );

    Ok(final_outcome.stop_reason)
}

fn install_termination_signal_handlers() -> Result<Arc<AtomicBool>> {
    let termination_requested = Arc::new(AtomicBool::new(false));
    flag::register(SIGINT, Arc::clone(&termination_requested))
        .context("failed to install SIGINT handler")?;
    #[cfg(unix)]
    flag::register(SIGTERM, Arc::clone(&termination_requested))
        .context("failed to install SIGTERM handler")?;
    Ok(termination_requested)
}

fn abort_if_control_requested(
    store: &SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
    total_rows: usize,
    batches: usize,
    cursor: &Cursor,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
) -> Result<()> {
    abort_if_control_requested_at(
        store,
        config,
        termination_requested,
        total_rows,
        batches,
        cursor,
        runtime_pressure_monitor,
        Utc::now(),
    )
}

fn abort_if_control_requested_at(
    store: &SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
    total_rows: usize,
    batches: usize,
    cursor: &Cursor,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
    now: DateTime<Utc>,
) -> Result<()> {
    if termination_requested.load(Ordering::Relaxed) {
        println!("event=controlled_abort source=termination_signal");
        log_run_summary(
            RunStopReason::StoppedDueToTerminationSignal,
            false,
            total_rows,
            batches,
            cursor,
            &BatchStageTimings::default(),
        );
        bail!("termination signal received; aborting backfill after durable checkpoint");
    }
    if config.abort_on_runtime_pressure {
        let runtime_sample = runtime_pressure_monitor.latest_sample(config, now)?.ok_or_else(|| {
            anyhow!(
                "runtime pressure guard requested but no ingestion pipeline metrics sample was available from {}",
                if let Some(log_path) = config.runtime_pressure_log_path.as_ref() {
                    format!("log:{}", log_path.display())
                } else {
                    format!("journalctl:{}", config.runtime_pressure_service)
                }
            )
        })?;
        if let Some(reason) = runtime_pressure_sample_stale_reason(config, runtime_sample, now) {
            log_runtime_pressure_abort_event(config, runtime_sample, &reason);
            log_run_summary(
                RunStopReason::StoppedDueToFastGuard,
                false,
                total_rows,
                batches,
                cursor,
                &BatchStageTimings::default(),
            );
            bail!("runtime pressure fast guard aborted backfill: {}", reason);
        }
        if let Some(reason) = runtime_pressure_breach_reason(config, &runtime_sample) {
            log_runtime_pressure_abort_event(config, &runtime_sample, &reason);
            log_run_summary(
                RunStopReason::StoppedDueToFastGuard,
                false,
                total_rows,
                batches,
                cursor,
                &BatchStageTimings::default(),
            );
            bail!("runtime pressure fast guard aborted backfill: {}", reason);
        }
    }
    if config.abort_on_runtime_infra_stop {
        if let Some(infra_stop) = active_runtime_infra_stop(store)? {
            infra_stop.log_event();
            log_run_summary(
                RunStopReason::StoppedDueToInfraGuard,
                false,
                total_rows,
                batches,
                cursor,
                &BatchStageTimings::default(),
            );
            bail!(infra_stop.abort_message());
        }
    }
    Ok(())
}

fn active_runtime_infra_stop(store: &SqliteStore) -> Result<Option<ActiveRuntimeInfraStop>> {
    let Some(latest_stop) = store.latest_risk_event_by_type(RUNTIME_INFRA_STOP_EVENT_TYPE)? else {
        return Ok(None);
    };
    let latest_clear = store.latest_risk_event_by_type(RUNTIME_INFRA_CLEARED_EVENT_TYPE)?;
    if latest_clear
        .as_ref()
        .is_some_and(|latest_clear| latest_clear.rowid > latest_stop.rowid)
    {
        return Ok(None);
    }
    Ok(Some(ActiveRuntimeInfraStop::from_risk_event(latest_stop)))
}

fn sleep_with_interrupt(
    store: &SqliteStore,
    config: &Config,
    sleep_ms: u64,
    termination_requested: &AtomicBool,
    total_rows: usize,
    batches: usize,
    cursor: &Cursor,
    run_started_at: DateTime<Utc>,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
) -> Result<Option<RunStopReason>> {
    let mut remaining_ms = sleep_ms;
    while remaining_ms > 0 {
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            total_rows,
            batches,
            cursor,
            runtime_pressure_monitor,
        )?;
        if let Some(reason) = bounded_run_stop_reason(config, run_started_at, batches, Utc::now()) {
            return Ok(Some(reason));
        }
        let chunk_ms = remaining_ms.min(SLEEP_INTERRUPT_POLL_MS);
        thread::sleep(StdDuration::from_millis(chunk_ms));
        remaining_ms = remaining_ms.saturating_sub(chunk_ms);
    }
    Ok(None)
}

fn refresh_backfill_source_protection(
    store: &SqliteStore,
    protect_since: DateTime<Utc>,
) -> Result<()> {
    let expires_at = Utc::now() + Duration::minutes(BACKFILL_SOURCE_PROTECTION_TTL_MINUTES);
    store.set_discovery_scoring_backfill_source_protection(protect_since, expires_at)
}

#[cfg(test)]
mod tests {
    use super::{
        abort_if_control_requested_at, run_boundary_build_phase, run_outcome, run_with_cleanup,
        run_with_store, run_with_store_stop_reason, set_backfill_test_failpoint,
        BackfillTestFailpoint, Config, Cursor, RunStopReason, RuntimePressureMonitor,
        DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS,
        DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO, DEFAULT_RUNTIME_PRESSURE_SERVICE,
        RUNTIME_INFRA_CLEARED_EVENT_TYPE, RUNTIME_INFRA_STOP_EVENT_TYPE,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_storage::{DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore};
    use rusqlite::Connection;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex;
    use tempfile::tempdir;

    static FAILPOINT_TEST_MUTEX: Mutex<()> = Mutex::new(());

    fn runtime_pressure_log_line(
        sample_ts: DateTime<Utc>,
        fill_ratio: f64,
        depth: u64,
        capacity: u64,
        oldest_age_ms: u64,
        ingestion_lag_ms_p95: u64,
    ) -> String {
        format!(
            "{} INFO {{\"message\":\"ingestion pipeline metrics\",\"yellowstone_output_queue_depth\":{depth},\"yellowstone_output_queue_capacity\":{capacity},\"yellowstone_output_queue_fill_ratio\":{fill_ratio},\"yellowstone_output_oldest_age_ms\":{oldest_age_ms},\"ingestion_lag_ms_p95\":{ingestion_lag_ms_p95}}}",
            sample_ts.to_rfc3339(),
        )
    }

    fn fmt_f64(value: f64) -> String {
        format!("{value:.12}")
    }

    fn comparable_days(
        conn: &Connection,
    ) -> Result<Vec<(String, String, String, String, i64, String, String)>> {
        let mut stmt = conn.prepare(
            "SELECT wallet_id, activity_day, first_seen, last_seen, trades, spent_sol, max_buy_notional_sol
             FROM wallet_scoring_days
             ORDER BY wallet_id ASC, activity_day ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                fmt_f64(row.get::<_, f64>(5)?),
                fmt_f64(row.get::<_, f64>(6)?),
            ));
        }
        Ok(out)
    }

    fn comparable_tx_minutes(conn: &Connection) -> Result<Vec<(String, i64, i64)>> {
        let mut stmt = conn.prepare(
            "SELECT wallet_id, minute_bucket, tx_count
             FROM wallet_scoring_tx_minutes
             ORDER BY wallet_id ASC, minute_bucket ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ));
        }
        Ok(out)
    }

    fn comparable_buy_facts(conn: &Connection) -> Result<Vec<String>> {
        let mut stmt = conn.prepare(
            "SELECT buy_signature, wallet_id, token, ts, notional_sol, market_volume_5m_sol,
                    market_unique_traders_5m, market_liquidity_proxy_sol, quality_source,
                    quality_token_age_seconds, quality_holders, quality_liquidity_sol,
                    rug_check_after_ts, rug_volume_lookahead_sol, rug_unique_traders_lookahead
             FROM wallet_scoring_buy_facts
             ORDER BY buy_signature ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(format!(
                "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                fmt_f64(row.get::<_, f64>(4)?),
                fmt_f64(row.get::<_, f64>(5)?),
                row.get::<_, i64>(6)?,
                fmt_f64(row.get::<_, f64>(7)?),
                row.get::<_, String>(8)?,
                row.get::<_, Option<i64>>(9)?
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                row.get::<_, Option<i64>>(10)?
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                row.get::<_, Option<f64>>(11)?
                    .map(fmt_f64)
                    .unwrap_or_else(|| "null".to_string()),
                row.get::<_, String>(12)?,
                row.get::<_, Option<f64>>(13)?
                    .map(fmt_f64)
                    .unwrap_or_else(|| "null".to_string()),
                row.get::<_, Option<i64>>(14)?
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
            ));
        }
        Ok(out)
    }

    fn comparable_close_facts(
        conn: &Connection,
    ) -> Result<Vec<(String, i64, String, String, String, String, i64, i64)>> {
        let mut stmt = conn.prepare(
            "SELECT sell_signature, segment_index, wallet_id, token, closed_ts, pnl_sol, hold_seconds, win
             FROM wallet_scoring_close_facts
             ORDER BY sell_signature ASC, segment_index ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                fmt_f64(row.get::<_, f64>(5)?),
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ));
        }
        Ok(out)
    }

    fn comparable_open_lots(
        conn: &Connection,
    ) -> Result<Vec<(String, String, String, String, String, String)>> {
        let mut stmt = conn.prepare(
            "SELECT buy_signature, wallet_id, token, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             ORDER BY wallet_id ASC, token ASC, opened_ts ASC, buy_signature ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                fmt_f64(row.get::<_, f64>(3)?),
                fmt_f64(row.get::<_, f64>(4)?),
                row.get::<_, String>(5)?,
            ));
        }
        Ok(out)
    }

    fn assert_comparable_scoring_state_eq(left_db_path: &Path, right_db_path: &Path) -> Result<()> {
        let left = Connection::open(left_db_path)?;
        let right = Connection::open(right_db_path)?;
        assert_eq!(comparable_days(&left)?, comparable_days(&right)?);
        assert_eq!(
            comparable_tx_minutes(&left)?,
            comparable_tx_minutes(&right)?
        );
        assert_eq!(comparable_buy_facts(&left)?, comparable_buy_facts(&right)?);
        assert_eq!(
            comparable_close_facts(&left)?,
            comparable_close_facts(&right)?
        );
        assert_eq!(comparable_open_lots(&left)?, comparable_open_lots(&right)?);
        Ok(())
    }

    fn query_table_count(conn: &Connection, table: &str) -> Result<i64> {
        Ok(
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?,
        )
    }

    fn seeded_reset_config(
        db_path: &Path,
        start_ts: DateTime<Utc>,
        resume_after: Cursor,
    ) -> Config {
        Config {
            db_path: PathBuf::from(db_path),
            start_ts,
            seeded_reset_max_start_ts: Some(start_ts),
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            max_batches_per_run: None,
            max_runtime_seconds: None,
            reset: false,
            seeded_reset: true,
            stop_after_seed_install: false,
            mark_covered: false,
            resume_after: Some(resume_after),
            abort_on_runtime_pressure: false,
            runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
            runtime_pressure_log_path: None,
            max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
            max_ingestion_lag_ms_p95: 10_000,
            max_runtime_pressure_sample_age_seconds: 35,
            abort_on_runtime_infra_stop: false,
            aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
        }
    }

    fn build_full_builder_boundary_seed_snapshot(
        store: &mut SqliteStore,
        boundary_start_ts: DateTime<Utc>,
        starting_cursor: Cursor,
        batch_size: usize,
    ) -> Result<copybot_storage::DiscoveryScoringBoundarySeedSnapshot> {
        let mut cursor = starting_cursor.clone();
        let mut builder = store.begin_discovery_scoring_replay_builder(
            cursor.ts,
            cursor.slot,
            cursor.signature.as_str(),
        )?;
        loop {
            let mut page = Vec::<SwapEvent>::with_capacity(batch_size);
            let mut reached_end_ts = false;
            let rows_seen = store.for_each_observed_swap_after_cursor(
                cursor.ts,
                cursor.slot,
                cursor.signature.as_str(),
                batch_size,
                |swap| {
                    if swap.ts_utc >= boundary_start_ts {
                        reached_end_ts = true;
                        return Ok(());
                    }
                    page.push(swap);
                    Ok(())
                },
            )?;

            if page.is_empty() {
                break;
            }

            let last_swap = page
                .last()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("full builder boundary page unexpectedly empty"))?;
            store.advance_discovery_scoring_builder_batch_in_memory_with_timings(
                &mut builder,
                &page,
                &DiscoveryAggregateWriteConfig::default(),
            )?;
            cursor = Cursor {
                ts: last_swap.ts_utc,
                slot: last_swap.slot,
                signature: last_swap.signature.clone(),
            };
            if reached_end_ts || rows_seen < batch_size {
                break;
            }
        }

        store.export_discovery_scoring_builder_boundary_seed_snapshot(
            &builder,
            boundary_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: cursor.ts,
                slot: cursor.slot,
                signature: cursor.signature,
            },
        )
    }

    #[test]
    fn exact_seeded_boundary_cursor_replays_same_timestamp_rows_without_skip() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-same-ts-no-skip.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-pre".to_string(),
            wallet: "wallet-seeded".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeeded111111111111111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 700,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_a = SwapEvent {
            signature: "sig-seeded-boundary-a".to_string(),
            wallet: "wallet-seeded".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeeded111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 701,
            ts_utc: boundary_start_ts,
        };
        let boundary_b = SwapEvent {
            signature: "sig-seeded-boundary-b".to_string(),
            wallet: "wallet-seeded".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeeded111111111111111111111111111".to_string(),
            amount_in: 1.2,
            amount_out: 12.0,
            exact_amounts: None,
            slot: 702,
            ts_utc: boundary_start_ts,
        };
        store.insert_observed_swaps_batch(&[
            pre_boundary.clone(),
            boundary_a.clone(),
            boundary_b.clone(),
        ])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        run_with_store(
            &mut store,
            &seeded_reset_config(
                &db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                boundary_start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: boundary_b.ts_utc,
                    slot: boundary_b.slot,
                    signature: boundary_b.signature.clone(),
                },
            ))
        );
        drop(store);
        let conn = Connection::open(&db_path)?;
        let buy_fact_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM wallet_scoring_buy_facts", [], |row| {
                row.get(0)
            })?;
        assert_eq!(
            buy_fact_count, 2,
            "both boundary-ts rows must replay after seed install"
        );
        Ok(())
    }

    #[test]
    fn exact_seeded_boundary_cursor_prevents_double_apply_on_boundary_timestamp_rows() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-same-ts-no-double.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-pre-dup".to_string(),
            wallet: "wallet-seeded-dup".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededDup111111111111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 710,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_a = SwapEvent {
            signature: "sig-seeded-dup-a".to_string(),
            wallet: "wallet-seeded-dup".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededDup111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 711,
            ts_utc: boundary_start_ts,
        };
        let boundary_b = SwapEvent {
            signature: "sig-seeded-dup-b".to_string(),
            wallet: "wallet-seeded-dup".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededDup111111111111111111111111".to_string(),
            amount_in: 1.2,
            amount_out: 12.0,
            exact_amounts: None,
            slot: 712,
            ts_utc: boundary_start_ts,
        };
        let later_buy = SwapEvent {
            signature: "sig-seeded-dup-later".to_string(),
            wallet: "wallet-seeded-dup".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededDup111111111111111111111111".to_string(),
            amount_in: 1.4,
            amount_out: 14.0,
            exact_amounts: None,
            slot: 713,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[
            pre_boundary.clone(),
            boundary_a.clone(),
            boundary_b.clone(),
            later_buy.clone(),
        ])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        run_with_store(
            &mut store,
            &seeded_reset_config(
                &db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
        )?;

        drop(store);
        let conn = Connection::open(&db_path)?;
        let buy_fact_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_buy_facts
             WHERE buy_signature IN (
                'sig-seeded-dup-a',
                'sig-seeded-dup-b',
                'sig-seeded-dup-later'
             )",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(
            buy_fact_count, 3,
            "boundary rows must exist exactly once after seeded replay"
        );
        let open_lot_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM wallet_scoring_open_lots", [], |row| {
                row.get(0)
            })?;
        assert_eq!(open_lot_count, 4);
        Ok(())
    }

    #[test]
    fn lot_only_boundary_build_matches_full_builder_boundary_snapshot() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let full_db_path = temp.path().join("seeded-boundary-full-builder-snapshot.db");
        let lot_db_path = temp.path().join("seeded-boundary-lot-only-snapshot.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut full_store = SqliteStore::open(&full_db_path)?;
        let mut lot_store = SqliteStore::open(&lot_db_path)?;
        full_store.run_migrations(&migration_dir)?;
        lot_store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-parity-pre".to_string(),
            wallet: "wallet-seeded-parity".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededParity1111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 781,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:50:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let gap_sell = SwapEvent {
            signature: "sig-seeded-parity-gap-sell".to_string(),
            wallet: "wallet-seeded-parity".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededParity1111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 40.0,
            amount_out: 0.7,
            exact_amounts: None,
            slot: 782,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let gap_buy = SwapEvent {
            signature: "sig-seeded-parity-gap-buy".to_string(),
            wallet: "wallet-seeded-parity".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededParity1111111111111111111".to_string(),
            amount_in: 0.9,
            amount_out: 50.0,
            exact_amounts: None,
            slot: 783,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-parity-boundary".to_string(),
            wallet: "wallet-seeded-parity".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededParity1111111111111111111".to_string(),
            amount_in: 1.1,
            amount_out: 60.0,
            exact_amounts: None,
            slot: 784,
            ts_utc: boundary_start_ts,
        };
        let observed = vec![pre_boundary.clone(), gap_sell, gap_buy, boundary_buy];
        for store in [&mut full_store, &mut lot_store] {
            store.insert_observed_swaps_batch(&observed)?;
            store.apply_discovery_scoring_batch(
                std::slice::from_ref(&pre_boundary),
                &DiscoveryAggregateWriteConfig::default(),
            )?;
            store.set_discovery_scoring_backfill_progress(
                partial_start_ts,
                &DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            )?;
        }

        let full_snapshot = build_full_builder_boundary_seed_snapshot(
            &mut full_store,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
            2,
        )?;
        let termination_requested = AtomicBool::new(false);
        let mut runtime_pressure_monitor = RuntimePressureMonitor::default();
        let lot_boundary = run_boundary_build_phase(
            &mut lot_store,
            &seeded_reset_config(
                &lot_db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
            &termination_requested,
            &mut runtime_pressure_monitor,
            partial_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
            Utc::now(),
        )?;
        let lot_snapshot = lot_boundary
            .seed_snapshot
            .ok_or_else(|| anyhow::anyhow!("lot-only boundary build did not export a snapshot"))?;

        assert_eq!(full_snapshot.boundary_cursor, lot_snapshot.boundary_cursor);
        assert_eq!(full_snapshot.open_lots, lot_snapshot.open_lots);
        Ok(())
    }

    #[test]
    fn lot_only_seeded_path_matches_manual_full_builder_seeded_path() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let manual_db_path = temp.path().join("seeded-boundary-manual-full-builder.db");
        let lot_db_path = temp.path().join("seeded-boundary-lot-only-full-path.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut manual_store = SqliteStore::open(&manual_db_path)?;
        let mut lot_store = SqliteStore::open(&lot_db_path)?;
        manual_store.run_migrations(&migration_dir)?;
        lot_store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-e2e-pre".to_string(),
            wallet: "wallet-seeded-e2e".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededE2E1111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 790,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:50:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let gap_sell = SwapEvent {
            signature: "sig-seeded-e2e-gap-sell".to_string(),
            wallet: "wallet-seeded-e2e".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededE2E1111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 40.0,
            amount_out: 0.8,
            exact_amounts: None,
            slot: 791,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let gap_buy = SwapEvent {
            signature: "sig-seeded-e2e-gap-buy".to_string(),
            wallet: "wallet-seeded-e2e".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededE2E1111111111111111111111".to_string(),
            amount_in: 0.9,
            amount_out: 50.0,
            exact_amounts: None,
            slot: 792,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy_a = SwapEvent {
            signature: "sig-seeded-e2e-boundary-a".to_string(),
            wallet: "wallet-seeded-e2e".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededE2E1111111111111111111111".to_string(),
            amount_in: 1.1,
            amount_out: 60.0,
            exact_amounts: None,
            slot: 793,
            ts_utc: boundary_start_ts,
        };
        let boundary_buy_b = SwapEvent {
            signature: "sig-seeded-e2e-boundary-b".to_string(),
            wallet: "wallet-seeded-e2e-peer".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededE2EPeer11111111111111111111".to_string(),
            amount_in: 0.7,
            amount_out: 30.0,
            exact_amounts: None,
            slot: 794,
            ts_utc: boundary_start_ts,
        };
        let post_sell = SwapEvent {
            signature: "sig-seeded-e2e-post-sell".to_string(),
            wallet: "wallet-seeded-e2e".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededE2E1111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 90.0,
            amount_out: 1.9,
            exact_amounts: None,
            slot: 795,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let post_peer_sell = SwapEvent {
            signature: "sig-seeded-e2e-post-peer-sell".to_string(),
            wallet: "wallet-seeded-e2e-peer".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededE2EPeer11111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 10.0,
            amount_out: 0.3,
            exact_amounts: None,
            slot: 796,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:06:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let observed = vec![
            pre_boundary.clone(),
            gap_sell,
            gap_buy,
            boundary_buy_a,
            boundary_buy_b,
            post_sell,
            post_peer_sell,
        ];
        for store in [&mut manual_store, &mut lot_store] {
            store.insert_observed_swaps_batch(&observed)?;
            store.apply_discovery_scoring_batch(
                std::slice::from_ref(&pre_boundary),
                &DiscoveryAggregateWriteConfig::default(),
            )?;
            store.set_discovery_scoring_backfill_progress(
                partial_start_ts,
                &DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            )?;
        }

        let manual_seed_snapshot = build_full_builder_boundary_seed_snapshot(
            &mut manual_store,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
            2,
        )?;
        manual_store.reset_discovery_scoring_tables_and_install_boundary_seed_snapshot(
            &manual_seed_snapshot,
            None,
        )?;
        let manual_config = seeded_reset_config(
            &manual_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        let lot_config = seeded_reset_config(
            &lot_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );

        run_with_store(&mut manual_store, &manual_config)?;
        run_with_store(&mut lot_store, &lot_config)?;

        assert_comparable_scoring_state_eq(&manual_db_path, &lot_db_path)?;
        assert_eq!(
            manual_store.load_discovery_scoring_backfill_progress()?,
            lot_store.load_discovery_scoring_backfill_progress()?,
        );
        assert_eq!(
            manual_store.load_discovery_scoring_seed_boundary_install_marker()?,
            lot_store.load_discovery_scoring_seed_boundary_install_marker()?,
        );
        assert_eq!(
            manual_store.load_discovery_scoring_covered_since()?,
            lot_store.load_discovery_scoring_covered_since()?,
        );
        assert_eq!(
            manual_store.load_discovery_scoring_covered_through_cursor()?,
            lot_store.load_discovery_scoring_covered_through_cursor()?,
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_preserves_late_sell_fifo_accounting_across_boundary() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-fifo.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-fifo-pre".to_string(),
            wallet: "wallet-seeded-fifo".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededFifo11111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 720,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:50:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-fifo-boundary".to_string(),
            wallet: "wallet-seeded-fifo".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededFifo11111111111111111111111".to_string(),
            amount_in: 2.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 721,
            ts_utc: boundary_start_ts,
        };
        let sell = SwapEvent {
            signature: "sig-seeded-fifo-sell".to_string(),
            wallet: "wallet-seeded-fifo".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededFifo11111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 150.0,
            amount_out: 3.0,
            exact_amounts: None,
            slot: 722,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[
            pre_boundary.clone(),
            boundary_buy.clone(),
            sell.clone(),
        ])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        run_with_store(
            &mut store,
            &seeded_reset_config(
                &db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
        )?;

        let conn = Connection::open(&db_path)?;
        let first_segment: (f64, i64) = conn.query_row(
            "SELECT pnl_sol, hold_seconds
             FROM wallet_scoring_close_facts
             WHERE sell_signature = 'sig-seeded-fifo-sell'
               AND segment_index = 0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let second_segment: (f64, i64) = conn.query_row(
            "SELECT pnl_sol, hold_seconds
             FROM wallet_scoring_close_facts
             WHERE sell_signature = 'sig-seeded-fifo-sell'
               AND segment_index = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((first_segment.0 - 1.0).abs() < 1e-9);
        assert!((second_segment.0 - 0.0).abs() < 1e-9);
        assert!(first_segment.1 > second_segment.1);
        Ok(())
    }

    #[test]
    fn seeded_reset_aborts_when_carryover_lots_are_present_at_boundary() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-carryover-abort.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-carryover-pre".to_string(),
            wallet: "wallet-seeded-carryover".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCarryover11111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 730,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:50:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(std::slice::from_ref(&pre_boundary))?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "INSERT INTO wallet_scoring_carryover_lots(wallet_id, token, qty, cost_sol, oldest_opened_ts)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                "wallet-seeded-carryover",
                "TokenSeededCarryover11111111111111111",
                10.0,
                0.5,
                pre_boundary.ts_utc.to_rfc3339(),
            ),
        )?;

        let error = run_with_store(
            &mut store,
            &seeded_reset_config(
                &db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
        )
        .expect_err("carryover boundary state must fail exact seeded reset");
        assert!(
            format!("{error:#}").contains("wallet_scoring_carryover_lots"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_keeps_coverage_markers_unset_until_full_completion() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("seeded-boundary-no-coverage-before-complete.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-coverage-pre".to_string(),
            wallet: "wallet-seeded-coverage".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCoverage1111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 740,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-coverage-boundary".to_string(),
            wallet: "wallet-seeded-coverage".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCoverage1111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 741,
            ts_utc: boundary_start_ts,
        };
        let later_buy = SwapEvent {
            signature: "sig-seeded-coverage-later".to_string(),
            wallet: "wallet-seeded-coverage".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCoverage1111111111111111".to_string(),
            amount_in: 1.1,
            amount_out: 11.0,
            exact_amounts: None,
            slot: 742,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[
            pre_boundary.clone(),
            boundary_buy.clone(),
            later_buy.clone(),
        ])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        let mut config = seeded_reset_config(
            &db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        config.batch_size = 1;
        config.max_batches_per_run = Some(1);
        config.mark_covered = true;

        let stop_reason = run_with_store_stop_reason(&mut store, &config)?;
        assert_eq!(stop_reason, RunStopReason::StoppedDueToBatchBudget);
        assert_eq!(store.load_discovery_scoring_covered_since()?, None);
        assert_eq!(store.load_discovery_scoring_covered_through_cursor()?, None);
        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                boundary_start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: boundary_buy.ts_utc,
                    slot: boundary_buy.slot,
                    signature: boundary_buy.signature.clone(),
                },
            ))
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_stop_after_seed_install_writes_durable_marker_and_skips_post_seed_replay(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-stop-after-install.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-stop-pre".to_string(),
            wallet: "wallet-seeded-stop".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStop1111111111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 780,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-stop-boundary".to_string(),
            wallet: "wallet-seeded-stop".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStop1111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 781,
            ts_utc: boundary_start_ts,
        };
        let post_boundary_sell = SwapEvent {
            signature: "sig-seeded-stop-sell".to_string(),
            wallet: "wallet-seeded-stop".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededStop1111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 4.0,
            amount_out: 0.9,
            exact_amounts: None,
            slot: 782,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[
            pre_boundary.clone(),
            boundary_buy,
            post_boundary_sell,
        ])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        let mut config = seeded_reset_config(
            &db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        config.stop_after_seed_install = true;

        let stop_reason = run_with_store_stop_reason(&mut store, &config)?;
        assert_eq!(stop_reason, RunStopReason::StoppedAfterSeedInstall);
        assert_eq!(
            run_outcome(stop_reason, false),
            "stopped_after_seed_install"
        );
        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                boundary_start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ))
        );
        assert_eq!(
            store.load_discovery_scoring_seed_boundary_install_marker()?,
            Some(copybot_storage::DiscoveryScoringSeedBoundaryInstallMarker {
                boundary_start_ts,
                boundary_cursor: DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            })
        );
        assert_eq!(store.load_discovery_scoring_covered_since()?, None);
        assert_eq!(store.load_discovery_scoring_covered_through_cursor()?, None);

        drop(store);
        let conn = Connection::open(&db_path)?;
        assert_eq!(query_table_count(&conn, "wallet_scoring_buy_facts")?, 0);
        assert_eq!(query_table_count(&conn, "wallet_scoring_close_facts")?, 0);
        assert_eq!(query_table_count(&conn, "wallet_scoring_days")?, 0);
        assert_eq!(query_table_count(&conn, "wallet_scoring_tx_minutes")?, 0);
        assert_eq!(
            comparable_open_lots(&conn)?,
            vec![(
                pre_boundary.signature,
                pre_boundary.wallet,
                pre_boundary.token_out,
                fmt_f64(pre_boundary.amount_out),
                fmt_f64(pre_boundary.amount_in),
                pre_boundary.ts_utc.to_rfc3339(),
            )]
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_stop_after_seed_install_is_idempotent_after_committed_marker() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-stop-after-install-idempotent.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-stop-idempotent-pre".to_string(),
            wallet: "wallet-seeded-stop-idempotent".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStopIdempotent111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 783,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-stop-idempotent-boundary".to_string(),
            wallet: "wallet-seeded-stop-idempotent".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStopIdempotent111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 784,
            ts_utc: boundary_start_ts,
        };
        let later_sell = SwapEvent {
            signature: "sig-seeded-stop-idempotent-sell".to_string(),
            wallet: "wallet-seeded-stop-idempotent".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededStopIdempotent111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 4.0,
            amount_out: 0.9,
            exact_amounts: None,
            slot: 785,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[pre_boundary.clone(), boundary_buy, later_sell])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        let mut config = seeded_reset_config(
            &db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        config.stop_after_seed_install = true;

        let first_stop_reason = run_with_store_stop_reason(&mut store, &config)?;
        assert_eq!(first_stop_reason, RunStopReason::StoppedAfterSeedInstall);

        let first_progress = store.load_discovery_scoring_backfill_progress()?;
        let first_marker = store.load_discovery_scoring_seed_boundary_install_marker()?;
        let first_covered_since = store.load_discovery_scoring_covered_since()?;
        let first_covered_through = store.load_discovery_scoring_covered_through_cursor()?;

        drop(store);
        let conn = Connection::open(&db_path)?;
        let first_open_lots = comparable_open_lots(&conn)?;
        let first_buy_fact_count = query_table_count(&conn, "wallet_scoring_buy_facts")?;
        let first_close_fact_count = query_table_count(&conn, "wallet_scoring_close_facts")?;
        let first_days_count = query_table_count(&conn, "wallet_scoring_days")?;
        let first_tx_minutes_count = query_table_count(&conn, "wallet_scoring_tx_minutes")?;
        drop(conn);

        let mut resumed_store = SqliteStore::open(&db_path)?;
        let second_stop_reason = run_with_store_stop_reason(&mut resumed_store, &config)?;
        assert_eq!(second_stop_reason, RunStopReason::StoppedAfterSeedInstall);
        assert_eq!(
            resumed_store.load_discovery_scoring_backfill_progress()?,
            first_progress
        );
        assert_eq!(
            resumed_store.load_discovery_scoring_seed_boundary_install_marker()?,
            first_marker
        );
        assert_eq!(
            resumed_store.load_discovery_scoring_covered_since()?,
            first_covered_since
        );
        assert_eq!(
            resumed_store.load_discovery_scoring_covered_through_cursor()?,
            first_covered_through
        );

        drop(resumed_store);
        let conn = Connection::open(&db_path)?;
        assert_eq!(comparable_open_lots(&conn)?, first_open_lots);
        assert_eq!(
            query_table_count(&conn, "wallet_scoring_buy_facts")?,
            first_buy_fact_count
        );
        assert_eq!(
            query_table_count(&conn, "wallet_scoring_close_facts")?,
            first_close_fact_count
        );
        assert_eq!(
            query_table_count(&conn, "wallet_scoring_days")?,
            first_days_count
        );
        assert_eq!(
            query_table_count(&conn, "wallet_scoring_tx_minutes")?,
            first_tx_minutes_count
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_stop_after_seed_install_then_normal_resume_matches_uninterrupted_seeded_path(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let stopped_db_path = temp.path().join("seeded-stop-then-resume.db");
        let uninterrupted_db_path = temp.path().join("seeded-uninterrupted.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut stopped_store = SqliteStore::open(&stopped_db_path)?;
        let mut uninterrupted_store = SqliteStore::open(&uninterrupted_db_path)?;
        stopped_store.run_migrations(&migration_dir)?;
        uninterrupted_store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-stop-resume-pre".to_string(),
            wallet: "wallet-seeded-stop-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStopResume1111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 790,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let last_before_boundary = SwapEvent {
            signature: "sig-seeded-stop-resume-gap".to_string(),
            wallet: "wallet-seeded-stop-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStopResume1111111111111111".to_string(),
            amount_in: 0.7,
            amount_out: 7.0,
            exact_amounts: None,
            slot: 791,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-stop-resume-boundary".to_string(),
            wallet: "wallet-seeded-stop-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededStopResume1111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 792,
            ts_utc: boundary_start_ts,
        };
        let later_sell = SwapEvent {
            signature: "sig-seeded-stop-resume-sell".to_string(),
            wallet: "wallet-seeded-stop-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededStopResume1111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 9.0,
            amount_out: 1.9,
            exact_amounts: None,
            slot: 793,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let observed = vec![
            pre_boundary.clone(),
            last_before_boundary.clone(),
            boundary_buy,
            later_sell,
        ];
        stopped_store.insert_observed_swaps_batch(&observed)?;
        uninterrupted_store.insert_observed_swaps_batch(&observed)?;
        for store in [&mut stopped_store, &mut uninterrupted_store] {
            store.apply_discovery_scoring_batch(
                std::slice::from_ref(&pre_boundary),
                &DiscoveryAggregateWriteConfig::default(),
            )?;
            store.set_discovery_scoring_backfill_progress(
                partial_start_ts,
                &DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            )?;
        }

        let mut stop_config = seeded_reset_config(
            &stopped_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        stop_config.stop_after_seed_install = true;
        let resume_config = seeded_reset_config(
            &stopped_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        let uninterrupted_config = seeded_reset_config(
            &uninterrupted_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );

        let stop_reason = run_with_store_stop_reason(&mut stopped_store, &stop_config)?;
        assert_eq!(stop_reason, RunStopReason::StoppedAfterSeedInstall);
        assert_eq!(
            stopped_store.load_discovery_scoring_backfill_progress()?,
            Some((
                boundary_start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: last_before_boundary.ts_utc,
                    slot: last_before_boundary.slot,
                    signature: last_before_boundary.signature.clone(),
                },
            ))
        );
        assert_eq!(stopped_store.load_discovery_scoring_covered_since()?, None);
        assert_eq!(
            stopped_store.load_discovery_scoring_covered_through_cursor()?,
            None
        );

        run_with_store(&mut stopped_store, &resume_config)?;
        run_with_store(&mut uninterrupted_store, &uninterrupted_config)?;

        assert_eq!(
            stopped_store.load_discovery_scoring_backfill_progress()?,
            uninterrupted_store.load_discovery_scoring_backfill_progress()?,
        );
        assert_eq!(
            stopped_store.load_discovery_scoring_seed_boundary_install_marker()?,
            uninterrupted_store.load_discovery_scoring_seed_boundary_install_marker()?,
        );
        assert_eq!(
            stopped_store.load_discovery_scoring_covered_since()?,
            uninterrupted_store.load_discovery_scoring_covered_since()?,
        );
        assert_eq!(
            stopped_store.load_discovery_scoring_covered_through_cursor()?,
            uninterrupted_store.load_discovery_scoring_covered_through_cursor()?,
        );
        assert_comparable_scoring_state_eq(&stopped_db_path, &uninterrupted_db_path)?;
        Ok(())
    }

    #[test]
    fn seeded_reset_aborts_when_start_ts_exceeds_effective_horizon_start() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-horizon-validation.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-horizon-pre".to_string(),
            wallet: "wallet-seeded-horizon".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededHorizon11111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 750,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(std::slice::from_ref(&pre_boundary))?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        let mut config = seeded_reset_config(
            &db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        config.seeded_reset_max_start_ts = Some(boundary_start_ts - chrono::Duration::seconds(1));

        let error = run_with_store(&mut store, &config)
            .expect_err("seeded reset must reject start_ts after the effective scoring horizon");
        assert!(
            format!("{error:#}").contains("effective scoring horizon start"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_crash_before_seed_install_leaves_preseed_lineage_intact() -> Result<()> {
        let _guard = FAILPOINT_TEST_MUTEX
            .lock()
            .expect("failpoint mutex poisoned");
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-crash-before-install.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-crash-pre".to_string(),
            wallet: "wallet-seeded-crash".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCrash11111111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 760,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let between_cursor_and_boundary = SwapEvent {
            signature: "sig-seeded-crash-gap".to_string(),
            wallet: "wallet-seeded-crash".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCrash11111111111111111111".to_string(),
            amount_in: 0.7,
            amount_out: 7.0,
            exact_amounts: None,
            slot: 761,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-crash-boundary".to_string(),
            wallet: "wallet-seeded-crash".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededCrash11111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 762,
            ts_utc: boundary_start_ts,
        };
        store.insert_observed_swaps_batch(&[
            pre_boundary.clone(),
            between_cursor_and_boundary,
            boundary_buy,
        ])?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            partial_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        let before_days = {
            let conn = Connection::open(&db_path)?;
            comparable_days(&conn)?
        };
        let before_tx_minutes = {
            let conn = Connection::open(&db_path)?;
            comparable_tx_minutes(&conn)?
        };
        let before_buy_facts = {
            let conn = Connection::open(&db_path)?;
            comparable_buy_facts(&conn)?
        };
        let before_close_facts = {
            let conn = Connection::open(&db_path)?;
            comparable_close_facts(&conn)?
        };
        let before_open_lots = {
            let conn = Connection::open(&db_path)?;
            comparable_open_lots(&conn)?
        };

        set_backfill_test_failpoint(BackfillTestFailpoint::AfterBoundaryExportBeforeSeedInstall);
        let termination_requested = AtomicBool::new(false);
        let error = run_with_cleanup(
            &mut store,
            &seeded_reset_config(
                &db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
            &termination_requested,
        )
        .expect_err("seeded reset must abort at the after-export failpoint");
        set_backfill_test_failpoint(BackfillTestFailpoint::None);
        assert!(
            format!("{error:#}").contains("after_boundary_export_before_seed_install"),
            "unexpected error: {error:#}",
        );

        let conn = Connection::open(&db_path)?;
        assert_eq!(comparable_days(&conn)?, before_days);
        assert_eq!(comparable_tx_minutes(&conn)?, before_tx_minutes);
        assert_eq!(comparable_buy_facts(&conn)?, before_buy_facts);
        assert_eq!(comparable_close_facts(&conn)?, before_close_facts);
        assert_eq!(comparable_open_lots(&conn)?, before_open_lots);
        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                partial_start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            )),
            "crash before committed seed install must keep pre-seed durable lineage intact",
        );
        Ok(())
    }

    #[test]
    fn seeded_reset_resume_after_committed_seed_install_replays_from_narrowed_boundary_cursor(
    ) -> Result<()> {
        let _guard = FAILPOINT_TEST_MUTEX
            .lock()
            .expect("failpoint mutex poisoned");
        let temp = tempdir().context("failed to create tempdir")?;
        let crash_db_path = temp.path().join("seeded-boundary-crash-after-install.db");
        let clean_db_path = temp.path().join("seeded-boundary-clean-after-install.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut crash_store = SqliteStore::open(&crash_db_path)?;
        let mut clean_store = SqliteStore::open(&clean_db_path)?;
        crash_store.run_migrations(&migration_dir)?;
        clean_store.run_migrations(&migration_dir)?;

        let partial_start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-resume-pre".to_string(),
            wallet: "wallet-seeded-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededResume1111111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 770,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let last_before_boundary = SwapEvent {
            signature: "sig-seeded-resume-gap".to_string(),
            wallet: "wallet-seeded-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededResume1111111111111111111".to_string(),
            amount_in: 0.7,
            amount_out: 7.0,
            exact_amounts: None,
            slot: 771,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:59:59Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let boundary_buy = SwapEvent {
            signature: "sig-seeded-resume-boundary".to_string(),
            wallet: "wallet-seeded-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededResume1111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 772,
            ts_utc: boundary_start_ts,
        };
        let later_sell = SwapEvent {
            signature: "sig-seeded-resume-sell".to_string(),
            wallet: "wallet-seeded-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenSeededResume1111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 9.0,
            amount_out: 1.9,
            exact_amounts: None,
            slot: 773,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let observed = vec![
            pre_boundary.clone(),
            last_before_boundary.clone(),
            boundary_buy.clone(),
            later_sell.clone(),
        ];
        crash_store.insert_observed_swaps_batch(&observed)?;
        clean_store.insert_observed_swaps_batch(&observed)?;
        for store in [&mut crash_store, &mut clean_store] {
            store.apply_discovery_scoring_batch(
                std::slice::from_ref(&pre_boundary),
                &DiscoveryAggregateWriteConfig::default(),
            )?;
            store.set_discovery_scoring_backfill_progress(
                partial_start_ts,
                &DiscoveryRuntimeCursor {
                    ts_utc: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            )?;
        }

        let config = seeded_reset_config(
            &crash_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );
        let clean_config = seeded_reset_config(
            &clean_db_path,
            boundary_start_ts,
            Cursor {
                ts: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        );

        set_backfill_test_failpoint(
            BackfillTestFailpoint::AfterCommittedSeedInstallBeforeReplayAfterSeed,
        );
        let termination_requested = AtomicBool::new(false);
        let error = run_with_cleanup(&mut crash_store, &config, &termination_requested)
            .expect_err("seeded reset must abort after committed seed install");
        set_backfill_test_failpoint(BackfillTestFailpoint::None);
        assert!(
            format!("{error:#}").contains("after_committed_seed_install_before_replay_after_seed"),
            "unexpected error: {error:#}",
        );
        assert_eq!(
            crash_store.load_discovery_scoring_backfill_progress()?,
            Some((
                boundary_start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: last_before_boundary.ts_utc,
                    slot: last_before_boundary.slot,
                    signature: last_before_boundary.signature.clone(),
                },
            )),
            "committed seed install must durably narrow replay progress to the exact boundary cursor",
        );
        assert_eq!(
            crash_store.load_discovery_scoring_seed_boundary_install_marker()?,
            Some(copybot_storage::DiscoveryScoringSeedBoundaryInstallMarker {
                boundary_start_ts,
                boundary_cursor: DiscoveryRuntimeCursor {
                    ts_utc: last_before_boundary.ts_utc,
                    slot: last_before_boundary.slot,
                    signature: last_before_boundary.signature.clone(),
                },
            }),
            "committed seed install must publish an explicit durable seed boundary marker",
        );

        run_with_store(&mut clean_store, &clean_config)?;
        run_with_store(&mut crash_store, &config)?;

        assert_eq!(
            clean_store.load_discovery_scoring_backfill_progress()?,
            crash_store.load_discovery_scoring_backfill_progress()?,
        );
        assert_comparable_scoring_state_eq(&clean_db_path, &crash_db_path)?;
        Ok(())
    }

    #[test]
    fn seeded_reset_rejects_ambiguous_same_start_progress_without_durable_seed_marker() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("seeded-boundary-ambiguous-progress.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let boundary_start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let pre_boundary = SwapEvent {
            signature: "sig-seeded-ambiguous-pre".to_string(),
            wallet: "wallet-seeded-ambiguous".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenSeededAmbiguous1111111111111111".to_string(),
            amount_in: 0.5,
            amount_out: 5.0,
            exact_amounts: None,
            slot: 780,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(std::slice::from_ref(&pre_boundary))?;
        store.apply_discovery_scoring_batch(
            std::slice::from_ref(&pre_boundary),
            &DiscoveryAggregateWriteConfig::default(),
        )?;
        store.set_discovery_scoring_backfill_progress(
            boundary_start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: pre_boundary.ts_utc,
                slot: pre_boundary.slot,
                signature: pre_boundary.signature.clone(),
            },
        )?;

        let error = run_with_store(
            &mut store,
            &seeded_reset_config(
                &db_path,
                boundary_start_ts,
                Cursor {
                    ts: pre_boundary.ts_utc,
                    slot: pre_boundary.slot,
                    signature: pre_boundary.signature.clone(),
                },
            ),
        )
        .expect_err("seeded reset must reject ambiguous same-start progress without marker");
        assert!(
            format!("{error:#}").contains("without a durable seed_boundary_installed marker"),
            "unexpected error: {error:#}",
        );
        Ok(())
    }

    #[test]
    fn full_forward_repair_fails_if_latched_gap_row_is_no_longer_in_raw_source() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-gap-repair.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let surviving_swap = SwapEvent {
            signature: "sig-after-gap".to_string(),
            wallet: "wallet-gap".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenGap111111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 101,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[surviving_swap])?;
        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            slot: 100,
            signature: "sig-gap-missing".to_string(),
        })?;
        store.set_discovery_scoring_backfill_progress(
            DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            &DiscoveryRuntimeCursor {
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                slot: 0,
                signature: String::new(),
            },
        )?;

        let config = Config {
            db_path: db_path.clone(),
            start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            seeded_reset_max_start_ts: None,
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            max_batches_per_run: None,
            max_runtime_seconds: None,
            reset: false,
            seeded_reset: false,
            stop_after_seed_install: false,
            mark_covered: false,
            resume_after: Some(Cursor {
                ts: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                slot: 0,
                signature: String::new(),
            }),
            abort_on_runtime_pressure: false,
            runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
            runtime_pressure_log_path: None,
            max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
            max_ingestion_lag_ms_p95: 10_000,
            max_runtime_pressure_sample_age_seconds: 35,
            abort_on_runtime_infra_stop: false,
            aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
        };

        let error = run_with_store(&mut store, &config).expect_err(
            "full forward repair must fail closed when the exact latched gap row is no longer observable",
        );
        let message = format!("{error:#}");
        assert!(
            message.contains("latched discovery scoring continuity gap"),
            "unexpected error: {message}"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                slot: 100,
                signature: "sig-gap-missing".to_string(),
            }),
            "repair failure must keep the exact continuity blocker latched"
        );
        Ok(())
    }

    #[test]
    fn mark_covered_with_resume_requires_matching_persisted_backfill_progress() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-mark-covered-lineage.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let replay_swap = SwapEvent {
            signature: "sig-lineage".to_string(),
            wallet: "wallet-lineage".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenLineage1111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 200,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[replay_swap])?;

        let config = Config {
            db_path: db_path.clone(),
            start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            seeded_reset_max_start_ts: None,
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            max_batches_per_run: None,
            max_runtime_seconds: None,
            reset: false,
            seeded_reset: false,
            stop_after_seed_install: false,
            mark_covered: true,
            resume_after: Some(Cursor {
                ts: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                slot: 199,
                signature: "sig-resume".to_string(),
            }),
            abort_on_runtime_pressure: false,
            runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
            runtime_pressure_log_path: None,
            max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
            max_ingestion_lag_ms_p95: 10_000,
            max_runtime_pressure_sample_age_seconds: 35,
            abort_on_runtime_infra_stop: false,
            aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
        };

        let error = run_with_store(&mut store, &config)
            .expect_err("coverage marking must fail closed without persisted continuous lineage");
        assert!(
            format!("{error:#}").contains("persisted backfill progress"),
            "unexpected error: {error:#}"
        );
        assert_eq!(store.load_discovery_scoring_covered_since()?, None);
        Ok(())
    }

    #[test]
    fn resumed_backfill_requires_persisted_continuous_lineage_or_reset() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-resume-lineage-required.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        store.insert_observed_swaps_batch(&[SwapEvent {
            signature: "sig-mid-history".to_string(),
            wallet: "wallet-mid-history".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenMidHistory111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 321,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        }])?;

        let config = Config {
            db_path: db_path.clone(),
            start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            seeded_reset_max_start_ts: None,
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            max_batches_per_run: None,
            max_runtime_seconds: None,
            reset: false,
            seeded_reset: false,
            stop_after_seed_install: false,
            mark_covered: false,
            resume_after: Some(Cursor {
                ts: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                slot: 320,
                signature: "sig-midpoint".to_string(),
            }),
            abort_on_runtime_pressure: false,
            runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
            runtime_pressure_log_path: None,
            max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
            max_ingestion_lag_ms_p95: 10_000,
            max_runtime_pressure_sample_age_seconds: 35,
            abort_on_runtime_infra_stop: false,
            aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
        };

        let error = run_with_store(&mut store, &config)
            .expect_err("first resumed run without persisted lineage must fail closed");
        let message = format!("{error:#}");
        assert!(
            message.contains("resumed backfill requires persisted backfill progress"),
            "unexpected error: {message}"
        );
        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            None,
            "failed resumed run must not seed fake persisted lineage"
        );
        Ok(())
    }

    #[test]
    fn partial_backfill_persists_exact_resume_progress_without_coverage_markers() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-partial-progress.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let first_swap = SwapEvent {
            signature: "sig-partial-1".to_string(),
            wallet: "wallet-partial".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenPartial111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 401,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let second_swap = SwapEvent {
            signature: "sig-partial-2".to_string(),
            wallet: "wallet-partial".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenPartial111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            exact_amounts: None,
            slot: 402,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[first_swap.clone(), second_swap])?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts,
                seeded_reset_max_start_ts: None,
                end_ts: Some(first_swap.ts_utc),
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: false,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: first_swap.ts_utc,
                    slot: first_swap.slot,
                    signature: first_swap.signature.clone(),
                },
            ))
        );
        assert_eq!(store.load_discovery_scoring_covered_since()?, None);
        assert_eq!(store.load_discovery_scoring_covered_through_cursor()?, None);
        Ok(())
    }

    #[test]
    fn end_ts_boundary_empty_page_reports_requested_end_ts_completion() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-end-ts-boundary.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let first_swap = SwapEvent {
            signature: "sig-end-boundary-1".to_string(),
            wallet: "wallet-end-boundary".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenEndBoundary1111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 451,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let second_swap = SwapEvent {
            signature: "sig-end-boundary-2".to_string(),
            wallet: "wallet-end-boundary".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenEndBoundary1111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            exact_amounts: None,
            slot: 452,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:20:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[first_swap.clone(), second_swap])?;

        let stop_reason = run_with_store_stop_reason(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts,
                seeded_reset_max_start_ts: None,
                end_ts: Some(
                    DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                        .expect("ts")
                        .with_timezone(&Utc),
                ),
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: false,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(stop_reason, RunStopReason::CompletedRequestedEndTs);
        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: first_swap.ts_utc,
                    slot: first_swap.slot,
                    signature: first_swap.signature.clone(),
                },
            ))
        );
        Ok(())
    }

    #[test]
    fn resumed_backfill_from_partial_state_marks_coverage_and_clears_progress() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-resume-success.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let first_swap = SwapEvent {
            signature: "sig-resume-1".to_string(),
            wallet: "wallet-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenResume111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 501,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let second_swap = SwapEvent {
            signature: "sig-resume-2".to_string(),
            wallet: "wallet-resume".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenResume111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            exact_amounts: None,
            slot: 502,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[first_swap.clone(), second_swap.clone()])?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts,
                seeded_reset_max_start_ts: None,
                end_ts: Some(first_swap.ts_utc),
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: false,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;
        store.clear_discovery_scoring_backfill_source_protection()?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts,
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: false,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: true,
                resume_after: Some(Cursor {
                    ts: first_swap.ts_utc,
                    slot: first_swap.slot,
                    signature: first_swap.signature.clone(),
                }),
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(store.load_discovery_scoring_backfill_progress()?, None);
        assert_eq!(
            store.load_discovery_scoring_covered_since()?,
            Some(start_ts)
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: second_swap.ts_utc,
                slot: second_swap.slot,
                signature: second_swap.signature.clone(),
            })
        );
        Ok(())
    }

    #[test]
    fn max_batches_per_run_stops_after_exact_batch_budget_and_persists_progress() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-batch-budget.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let first_swap = SwapEvent {
            signature: "sig-budget-1".to_string(),
            wallet: "wallet-budget".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBudget111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 701,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let second_swap = SwapEvent {
            signature: "sig-budget-2".to_string(),
            wallet: "wallet-budget".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBudget111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            exact_amounts: None,
            slot: 702,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[first_swap.clone(), second_swap])?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts,
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: Some(1),
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: first_swap.ts_utc,
                    slot: first_swap.slot,
                    signature: first_swap.signature.clone(),
                },
            ))
        );
        assert_eq!(store.load_discovery_scoring_covered_since()?, None);
        assert_eq!(store.load_discovery_scoring_covered_through_cursor()?, None);
        Ok(())
    }

    #[test]
    fn max_runtime_seconds_stops_cleanly_and_persists_progress() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-runtime-budget.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let start_ts = DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let first_swap = SwapEvent {
            signature: "sig-runtime-budget-1".to_string(),
            wallet: "wallet-runtime-budget".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRuntimeBudget111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 703,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        let second_swap = SwapEvent {
            signature: "sig-runtime-budget-2".to_string(),
            wallet: "wallet-runtime-budget".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRuntimeBudget111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            exact_amounts: None,
            slot: 704,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[first_swap.clone(), second_swap])?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts,
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 1_100,
                max_batches_per_run: None,
                max_runtime_seconds: Some(1),
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                start_ts,
                DiscoveryRuntimeCursor {
                    ts_utc: first_swap.ts_utc,
                    slot: first_swap.slot,
                    signature: first_swap.signature.clone(),
                },
            ))
        );
        assert_eq!(store.load_discovery_scoring_covered_since()?, None);
        assert_eq!(store.load_discovery_scoring_covered_through_cursor()?, None);
        Ok(())
    }

    #[test]
    fn bounded_stop_clears_backfill_source_protection() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-bounded-stop-cleanup.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        store.insert_observed_swaps_batch(&[
            SwapEvent {
                signature: "sig-bounded-cleanup-1".to_string(),
                wallet: "wallet-bounded-cleanup".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "TokenBoundedCleanup111111111111111".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                exact_amounts: None,
                slot: 705,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            SwapEvent {
                signature: "sig-bounded-cleanup-2".to_string(),
                wallet: "wallet-bounded-cleanup".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "TokenBoundedCleanup111111111111111".to_string(),
                amount_in: 1.0,
                amount_out: 11.0,
                exact_amounts: None,
                slot: 706,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:10:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
        ])?;

        run_with_cleanup(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: Some(1),
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
            &AtomicBool::new(false),
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_protected_since(Utc::now())?,
            None,
            "bounded clean exit must clear source protection latch"
        );
        Ok(())
    }

    #[test]
    fn runtime_infra_stop_abort_blocks_backfill_before_next_batch() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-runtime-guard-active.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        store.insert_observed_swaps_batch(&[SwapEvent {
            signature: "sig-runtime-stop".to_string(),
            wallet: "wallet-runtime-stop".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRuntimeStop1111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 601,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        }])?;
        store.insert_risk_event(
            RUNTIME_INFRA_STOP_EVENT_TYPE,
            "warn",
            DateTime::parse_from_rfc3339("2026-03-06T10:01:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            Some(
                "{\"reason\":\"lag_p95_over_threshold_for=5m threshold_ms=2000\",\"yellowstone_output_queue_depth\":100,\"yellowstone_output_queue_capacity\":100,\"yellowstone_output_queue_fill_ratio\":1.0}",
            ),
        )?;

        let error = run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: true,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )
        .expect_err("active runtime infra stop must abort guarded backfill");

        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("runtime health guard aborted backfill"),
            "unexpected error: {error_text}"
        );
        assert_eq!(store.load_discovery_scoring_backfill_progress()?, None);
        Ok(())
    }

    #[test]
    fn runtime_infra_stop_guard_ignores_cleared_stop() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-runtime-guard-cleared.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        let swap = SwapEvent {
            signature: "sig-runtime-cleared".to_string(),
            wallet: "wallet-runtime-cleared".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRuntimeCleared1111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 602,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[swap.clone()])?;
        store.insert_risk_event(
            RUNTIME_INFRA_STOP_EVENT_TYPE,
            "warn",
            DateTime::parse_from_rfc3339("2026-03-06T10:01:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            Some("{\"reason\":\"no_ingestion_progress_for=20m\"}"),
        )?;
        store.insert_risk_event(
            RUNTIME_INFRA_CLEARED_EVENT_TYPE,
            "info",
            DateTime::parse_from_rfc3339("2026-03-06T10:02:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            Some("{\"state\":\"cleared\"}"),
        )?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: true,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                DiscoveryRuntimeCursor {
                    ts_utc: swap.ts_utc,
                    slot: swap.slot,
                    signature: swap.signature.clone(),
                },
            ))
        );
        Ok(())
    }

    #[test]
    fn controlled_abort_clears_backfill_source_protection() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-runtime-guard-cleanup.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;

        store.insert_observed_swaps_batch(&[SwapEvent {
            signature: "sig-runtime-cleanup".to_string(),
            wallet: "wallet-runtime-cleanup".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRuntimeCleanup1111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 603,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        }])?;

        let termination_requested = AtomicBool::new(true);
        let error = run_with_cleanup(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: false,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: None,
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
            &termination_requested,
        )
        .expect_err("termination-requested abort must fail closed");

        assert!(
            format!("{error:#}").contains("termination signal received"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            store.load_discovery_scoring_backfill_protected_since(Utc::now())?,
            None,
            "controlled abort cleanup must clear source protection latch"
        );
        Ok(())
    }

    #[test]
    fn active_runtime_pressure_signal_aborts_guarded_backfill() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-fast-guard-active.db");
        let log_path = temp.path().join("runtime-pressure.log");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        let sample_ts = Utc::now();
        store.insert_observed_swaps_batch(&[SwapEvent {
            signature: "sig-fast-guard-active".to_string(),
            wallet: "wallet-fast-guard-active".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenFastGuardActive1111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 604,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        }])?;
        fs::write(
            &log_path,
            runtime_pressure_log_line(sample_ts, 0.30, 300, 1000, 2500, 2_000),
        )?;

        let error = run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: true,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: Some(log_path),
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )
        .expect_err("active runtime pressure must abort guarded backfill");

        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("runtime pressure fast guard aborted backfill"),
            "unexpected error: {error_text}"
        );
        assert_eq!(store.load_discovery_scoring_backfill_progress()?, None);
        Ok(())
    }

    #[test]
    fn below_threshold_runtime_pressure_does_not_abort() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-fast-guard-healthy.db");
        let log_path = temp.path().join("runtime-pressure.log");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        let sample_ts = Utc::now();
        let swap = SwapEvent {
            signature: "sig-fast-guard-healthy".to_string(),
            wallet: "wallet-fast-guard-healthy".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenFastGuardHealthy111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 605,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[swap.clone()])?;
        fs::write(
            &log_path,
            runtime_pressure_log_line(sample_ts, 0.10, 100, 1000, 500, 1_500),
        )?;

        run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: true,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: Some(log_path),
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )?;

        assert_eq!(
            store.load_discovery_scoring_backfill_progress()?,
            Some((
                DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                DiscoveryRuntimeCursor {
                    ts_utc: swap.ts_utc,
                    slot: swap.slot,
                    signature: swap.signature.clone(),
                },
            ))
        );
        Ok(())
    }

    #[test]
    fn fast_pressure_guard_abort_clears_backfill_source_protection() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-fast-guard-cleanup.db");
        let log_path = temp.path().join("runtime-pressure.log");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        let sample_ts = Utc::now();
        store.insert_observed_swaps_batch(&[SwapEvent {
            signature: "sig-fast-guard-cleanup".to_string(),
            wallet: "wallet-fast-guard-cleanup".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenFastGuardCleanup11111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 606,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        }])?;
        fs::write(
            &log_path,
            runtime_pressure_log_line(sample_ts, 0.50, 500, 1000, 3500, 12_000),
        )?;

        let error = run_with_cleanup(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: true,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: Some(log_path),
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 35,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
            &AtomicBool::new(false),
        )
        .expect_err("fast pressure guard must fail closed");

        assert!(
            format!("{error:#}").contains("runtime pressure fast guard aborted backfill"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            store.load_discovery_scoring_backfill_protected_since(Utc::now())?,
            None,
            "fast pressure abort cleanup must clear source protection latch"
        );
        Ok(())
    }

    #[test]
    fn stale_runtime_pressure_sample_aborts_guarded_backfill() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-fast-guard-stale-sample.db");
        let log_path = temp.path().join("runtime-pressure.log");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        let sample_ts = Utc::now() - chrono::Duration::seconds(60);
        store.insert_observed_swaps_batch(&[SwapEvent {
            signature: "sig-fast-guard-stale-sample".to_string(),
            wallet: "wallet-fast-guard-stale-sample".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenFastGuardStaleSample111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            exact_amounts: None,
            slot: 607,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        }])?;
        fs::write(
            &log_path,
            runtime_pressure_log_line(sample_ts, 0.10, 100, 1000, 500, 1_500),
        )?;

        let error = run_with_store(
            &mut store,
            &Config {
                db_path: db_path.clone(),
                start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
                seeded_reset_max_start_ts: None,
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                max_batches_per_run: None,
                max_runtime_seconds: None,
                reset: true,
                seeded_reset: false,
                stop_after_seed_install: false,
                mark_covered: false,
                resume_after: None,
                abort_on_runtime_pressure: true,
                runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
                runtime_pressure_log_path: Some(log_path),
                max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
                max_ingestion_lag_ms_p95: 10_000,
                max_runtime_pressure_sample_age_seconds: 5,
                abort_on_runtime_infra_stop: false,
                aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
            },
        )
        .expect_err("stale runtime pressure sample must abort guarded backfill");

        assert!(
            format!("{error:#}").contains("runtime_pressure_sample_stale"),
            "unexpected error: {error:#}"
        );
        assert_eq!(store.load_discovery_scoring_backfill_progress()?, None);
        Ok(())
    }

    #[test]
    fn runtime_pressure_guard_reuses_cached_sample_until_refresh_interval() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("backfill-fast-guard-cached-sample.db");
        let log_path = temp.path().join("runtime-pressure.log");
        let store = SqliteStore::open(&db_path)?;
        let mut runtime_pressure_monitor = RuntimePressureMonitor::default();
        let termination_requested = AtomicBool::new(false);
        let now = Utc::now();
        fs::write(
            &log_path,
            runtime_pressure_log_line(now, 0.10, 100, 1000, 500, 1_500),
        )?;

        let config = Config {
            db_path,
            start_ts: DateTime::parse_from_rfc3339("2026-03-06T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            seeded_reset_max_start_ts: None,
            end_ts: None,
            batch_size: 1,
            sleep_ms: 0,
            max_batches_per_run: None,
            max_runtime_seconds: None,
            reset: false,
            seeded_reset: false,
            stop_after_seed_install: false,
            mark_covered: false,
            resume_after: None,
            abort_on_runtime_pressure: true,
            runtime_pressure_service: DEFAULT_RUNTIME_PRESSURE_SERVICE.to_string(),
            runtime_pressure_log_path: Some(log_path.clone()),
            max_yellowstone_fill_ratio: DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO,
            max_ingestion_lag_ms_p95: 10_000,
            max_runtime_pressure_sample_age_seconds: 35,
            abort_on_runtime_infra_stop: false,
            aggregate_write_config: DiscoveryAggregateWriteConfig::default(),
        };

        abort_if_control_requested_at(
            &store,
            &config,
            &termination_requested,
            0,
            0,
            &Cursor {
                ts: config.start_ts,
                slot: 0,
                signature: String::new(),
            },
            &mut runtime_pressure_monitor,
            now,
        )?;

        let cached_window_now =
            now + chrono::Duration::milliseconds(DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS / 2);
        fs::write(
            &log_path,
            runtime_pressure_log_line(cached_window_now, 0.50, 500, 1000, 3500, 12_000),
        )?;
        abort_if_control_requested_at(
            &store,
            &config,
            &termination_requested,
            0,
            0,
            &Cursor {
                ts: config.start_ts,
                slot: 0,
                signature: String::new(),
            },
            &mut runtime_pressure_monitor,
            cached_window_now,
        )?;

        let refresh_due_now =
            now + chrono::Duration::milliseconds(DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS + 50);
        let error = abort_if_control_requested_at(
            &store,
            &config,
            &termination_requested,
            0,
            0,
            &Cursor {
                ts: config.start_ts,
                slot: 0,
                signature: String::new(),
            },
            &mut runtime_pressure_monitor,
            refresh_due_now,
        )
        .expect_err("cached runtime pressure sample must refresh after fetch interval");

        assert!(
            format!("{error:#}").contains("runtime pressure fast guard aborted backfill"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }
}
