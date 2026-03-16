use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_core_types::SwapEvent;
use copybot_storage::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, RiskEventRow, SqliteStore,
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration as StdDuration;

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
    end_ts: Option<DateTime<Utc>>,
    batch_size: usize,
    sleep_ms: u64,
    reset: bool,
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

fn main() -> Result<()> {
    let config = parse_args()?;
    run(config)
}

fn parse_args() -> Result<Config> {
    let mut args = env::args().skip(1);
    let Some(db_path_raw) = args.next() else {
        bail!(
            "usage: backfill_discovery_scoring <db_path> --config <path> --start-ts <rfc3339> [--end-ts <rfc3339>] [--batch-size N] [--sleep-ms N] (--reset | --resume-ts <ts> --resume-slot <slot> --resume-signature <sig>) [--mark-covered] [--abort-on-runtime-pressure] [--max-yellowstone-fill-ratio N] [--max-ingestion-lag-ms-p95 N] [--max-runtime-pressure-sample-age-seconds N] [--runtime-pressure-service NAME] [--runtime-pressure-log-path PATH] [--abort-on-runtime-infra-stop] [--helius-http-url URL] [--min-token-age-hint-seconds N]"
        );
    };

    let mut config_path: Option<PathBuf> = None;
    let mut start_ts: Option<DateTime<Utc>> = None;
    let mut end_ts: Option<DateTime<Utc>> = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut sleep_ms = 0u64;
    let mut reset = false;
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
            "--reset" => reset = true,
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

    if reset && resume_after.is_some() {
        bail!("--reset cannot be combined with --resume-*");
    }
    if !reset && resume_after.is_none() {
        bail!("refusing non-idempotent replay without either --reset or exact --resume-* cursor");
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
        end_ts,
        batch_size,
        sleep_ms,
        reset,
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
    let termination_requested = AtomicBool::new(false);
    run_with_cleanup(store, config, &termination_requested)
}

fn run_with_cleanup(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
) -> Result<()> {
    let run_result = run_with_store_inner(store, config, termination_requested);
    let clear_result = store.clear_discovery_scoring_backfill_source_protection();
    match (run_result, clear_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(run_error), Err(clear_error)) => Err(run_error).context(format!(
            "backfill failed and cleanup of source protection also failed: {clear_error:#}"
        )),
    }
}

fn run_with_store_inner(
    store: &mut SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
) -> Result<()> {
    let mut runtime_pressure_monitor = RuntimePressureMonitor::default();
    abort_if_control_requested(
        store,
        config,
        termination_requested,
        &mut runtime_pressure_monitor,
    )?;
    if config.reset {
        store.reset_discovery_scoring_tables()?;
        println!("event=reset_discovery_scoring_tables");
    }
    refresh_backfill_source_protection(store, config.start_ts)?;

    if let Some(resume_after) = config.resume_after.as_ref() {
        let Some((progress_start_ts, progress_cursor)) =
            store.load_discovery_scoring_backfill_progress()?
        else {
            bail!(
                "resumed backfill requires persisted backfill progress proving continuous lineage from start_ts; use --reset for a new rebuild"
            );
        };
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
    }

    let mut cursor = config.resume_after.clone().unwrap_or_else(|| Cursor {
        ts: config.start_ts,
        slot: 0,
        signature: String::new(),
    });
    let gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let mut gap_cursor_observed = false;
    let mut total_rows = 0usize;
    let mut batches = 0usize;

    loop {
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            &mut runtime_pressure_monitor,
        )?;
        let mut page = Vec::<SwapEvent>::with_capacity(config.batch_size);
        let mut reached_end_ts = false;
        let rows_seen = store.for_each_observed_swap_after_cursor(
            cursor.ts,
            cursor.slot,
            cursor.signature.as_str(),
            config.batch_size,
            |swap| {
                if swap.ts_utc < config.start_ts {
                    return Ok(());
                }
                if config.end_ts.is_some_and(|end_ts| swap.ts_utc > end_ts) {
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

        if page.is_empty() {
            break;
        }

        let last_swap = page
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("backfill page unexpectedly empty"))?;
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            &mut runtime_pressure_monitor,
        )?;
        refresh_backfill_source_protection(store, config.start_ts)?;
        store.apply_discovery_scoring_batch(&page, &config.aggregate_write_config)?;
        store.finalize_discovery_scoring_rug_facts(last_swap.ts_utc)?;
        cursor = Cursor {
            ts: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        store.set_discovery_scoring_backfill_progress(
            config.start_ts,
            &DiscoveryRuntimeCursor {
                ts_utc: cursor.ts,
                slot: cursor.slot,
                signature: cursor.signature.clone(),
            },
        )?;
        total_rows = total_rows.saturating_add(page.len());
        batches = batches.saturating_add(1);
        println!(
            "event=batch_committed rows={} total_rows={} batches={} cursor_ts={} cursor_slot={} cursor_signature={}",
            page.len(),
            total_rows,
            batches,
            cursor.ts.to_rfc3339(),
            cursor.slot,
            cursor.signature
        );

        abort_if_control_requested(
            store,
            config,
            termination_requested,
            &mut runtime_pressure_monitor,
        )?;
        if reached_end_ts || rows_seen < config.batch_size {
            break;
        }
        if config.sleep_ms > 0 {
            sleep_with_interrupt(
                store,
                config,
                config.sleep_ms,
                termination_requested,
                &mut runtime_pressure_monitor,
            )?;
        }
    }

    store.finalize_discovery_scoring_rug_facts(config.end_ts.unwrap_or(cursor.ts))?;
    if config.end_ts.is_none() {
        if let Some(gap_cursor) = gap_cursor.as_ref() {
            if !gap_cursor_observed {
                bail!(
                    "latched discovery scoring continuity gap at {} / {} / {} was not observed during full forward replay; source rows may be missing or replay started too late",
                    gap_cursor.ts_utc.to_rfc3339(),
                    gap_cursor.slot,
                    gap_cursor.signature
                );
            }
            store.clear_discovery_scoring_materialization_gap_if_cursor_observed(gap_cursor)?;
        }
    }

    if config.mark_covered {
        store.set_discovery_scoring_covered_since(config.start_ts)?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: cursor.ts,
            slot: cursor.slot,
            signature: cursor.signature.clone(),
        })?;
        store.clear_discovery_scoring_backfill_progress()?;
        println!(
            "event=coverage_marked covered_since_ts={} covered_through_ts={} covered_through_slot={} covered_through_signature={}",
            config.start_ts.to_rfc3339(),
            cursor.ts.to_rfc3339(),
            cursor.slot,
            cursor.signature
        );
    } else {
        println!("event=coverage_not_marked");
    }

    if let Ok((busy, log_frames, checkpointed_frames)) = store.checkpoint_wal_truncate() {
        println!(
            "event=wal_checkpoint busy={} log_frames={} checkpointed_frames={}",
            busy, log_frames, checkpointed_frames
        );
    }

    println!(
        "summary total_rows={} batches={} final_cursor_ts={} final_cursor_slot={} final_cursor_signature={}",
        total_rows,
        batches,
        cursor.ts.to_rfc3339(),
        cursor.slot,
        cursor.signature
    );

    Ok(())
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
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
) -> Result<()> {
    abort_if_control_requested_at(
        store,
        config,
        termination_requested,
        runtime_pressure_monitor,
        Utc::now(),
    )
}

fn abort_if_control_requested_at(
    store: &SqliteStore,
    config: &Config,
    termination_requested: &AtomicBool,
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
    now: DateTime<Utc>,
) -> Result<()> {
    if termination_requested.load(Ordering::Relaxed) {
        println!("event=controlled_abort source=termination_signal");
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
            bail!("runtime pressure fast guard aborted backfill: {}", reason);
        }
        if let Some(reason) = runtime_pressure_breach_reason(config, &runtime_sample) {
            log_runtime_pressure_abort_event(config, &runtime_sample, &reason);
            bail!("runtime pressure fast guard aborted backfill: {}", reason);
        }
    }
    if config.abort_on_runtime_infra_stop {
        if let Some(infra_stop) = active_runtime_infra_stop(store)? {
            infra_stop.log_event();
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
    runtime_pressure_monitor: &mut RuntimePressureMonitor,
) -> Result<()> {
    let mut remaining_ms = sleep_ms;
    while remaining_ms > 0 {
        abort_if_control_requested(
            store,
            config,
            termination_requested,
            runtime_pressure_monitor,
        )?;
        let chunk_ms = remaining_ms.min(SLEEP_INTERRUPT_POLL_MS);
        thread::sleep(StdDuration::from_millis(chunk_ms));
        remaining_ms = remaining_ms.saturating_sub(chunk_ms);
    }
    Ok(())
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
        abort_if_control_requested_at, run_with_cleanup, run_with_store, Config, Cursor,
        RuntimePressureMonitor, DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS,
        DEFAULT_RUNTIME_PRESSURE_MAX_YELLOWSTONE_FILL_RATIO, DEFAULT_RUNTIME_PRESSURE_SERVICE,
        RUNTIME_INFRA_CLEARED_EVENT_TYPE, RUNTIME_INFRA_STOP_EVENT_TYPE,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_storage::{DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore};
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

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
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            reset: false,
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
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            reset: false,
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
            end_ts: None,
            batch_size: 128,
            sleep_ms: 0,
            reset: false,
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
                end_ts: Some(first_swap.ts_utc),
                batch_size: 1,
                sleep_ms: 0,
                reset: false,
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
                end_ts: Some(first_swap.ts_utc),
                batch_size: 1,
                sleep_ms: 0,
                reset: false,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: false,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
                end_ts: None,
                batch_size: 1,
                sleep_ms: 0,
                reset: true,
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
            end_ts: None,
            batch_size: 1,
            sleep_ms: 0,
            reset: false,
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
            &mut runtime_pressure_monitor,
            cached_window_now,
        )?;

        let refresh_due_now =
            now + chrono::Duration::milliseconds(DEFAULT_RUNTIME_PRESSURE_FETCH_INTERVAL_MS + 50);
        let error = abort_if_control_requested_at(
            &store,
            &config,
            &termination_requested,
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
