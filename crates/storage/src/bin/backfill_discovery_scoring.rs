use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_core_types::SwapEvent;
use copybot_storage::{DiscoveryAggregateWriteConfig, SqliteStore};
use std::env;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration as StdDuration;

const DEFAULT_BATCH_SIZE: usize = 5_000;
const BACKFILL_SOURCE_PROTECTION_TTL_MINUTES: i64 = 240;

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
    aggregate_write_config: DiscoveryAggregateWriteConfig,
}

fn main() -> Result<()> {
    let config = parse_args()?;
    run(config)
}

fn parse_args() -> Result<Config> {
    let mut args = env::args().skip(1);
    let Some(db_path_raw) = args.next() else {
        bail!(
            "usage: backfill_discovery_scoring <db_path> --config <path> --start-ts <rfc3339> [--end-ts <rfc3339>] [--batch-size N] [--sleep-ms N] (--reset | --resume-ts <ts> --resume-slot <slot> --resume-signature <sig>) [--mark-covered] [--helius-http-url URL] [--min-token-age-hint-seconds N]"
        );
    };

    let mut config_path: Option<PathBuf> = None;
    let mut start_ts: Option<DateTime<Utc>> = None;
    let mut end_ts: Option<DateTime<Utc>> = None;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut sleep_ms = 0u64;
    let mut reset = false;
    let mut mark_covered = false;
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

    Ok(Config {
        db_path: PathBuf::from(db_path_raw),
        start_ts,
        end_ts,
        batch_size,
        sleep_ms,
        reset,
        mark_covered,
        resume_after,
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

fn run(config: Config) -> Result<()> {
    let mut store = SqliteStore::open(Path::new(&config.db_path))
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migrations_dir).with_context(|| {
        format!(
            "failed applying migrations from {}",
            migrations_dir.display()
        )
    })?;

    let run_result = run_with_store(&mut store, &config);
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

fn run_with_store(store: &mut SqliteStore, config: &Config) -> Result<()> {
    if config.reset {
        store.reset_discovery_scoring_tables()?;
        println!("event=reset_discovery_scoring_tables");
    }
    refresh_backfill_source_protection(store, config.start_ts)?;

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
        refresh_backfill_source_protection(store, config.start_ts)?;
        store.apply_discovery_scoring_batch(&page, &config.aggregate_write_config)?;
        store.finalize_discovery_scoring_rug_facts(last_swap.ts_utc)?;
        cursor = Cursor {
            ts: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
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

        if reached_end_ts || rows_seen < config.batch_size {
            break;
        }
        if config.sleep_ms > 0 {
            thread::sleep(StdDuration::from_millis(config.sleep_ms));
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
        store.set_discovery_scoring_covered_through_cursor(
            &copybot_storage::DiscoveryRuntimeCursor {
                ts_utc: cursor.ts,
                slot: cursor.slot,
                signature: cursor.signature.clone(),
            },
        )?;
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

fn refresh_backfill_source_protection(
    store: &SqliteStore,
    protect_since: DateTime<Utc>,
) -> Result<()> {
    let expires_at = Utc::now() + Duration::minutes(BACKFILL_SOURCE_PROTECTION_TTL_MINUTES);
    store.set_discovery_scoring_backfill_source_protection(protect_since, expires_at)
}

#[cfg(test)]
mod tests {
    use super::{run_with_store, Config, Cursor};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_storage::{DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore};
    use std::path::Path;
    use tempfile::tempdir;

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
}
