use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{operator_status::DiscoveryOperatorStatus, DiscoveryService};
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_status --config <path> [--db-path <path>] [--json] [--now <rfc3339>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    json: bool,
    now: DateTime<Utc>,
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
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
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

fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    let configured_db_path = PathBuf::from(configured_db_path);
    if configured_db_path.is_absolute() {
        return configured_db_path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_db_path)
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let status = discovery.operator_status(&store, config.now)?;
    if config.json {
        serde_json::to_string_pretty(&status).context("failed serializing discovery status json")
    } else {
        Ok(render_human(&config.config_path, &db_path, &status))
    }
}

fn render_human(config_path: &Path, db_path: &Path, status: &DiscoveryOperatorStatus) -> String {
    [
        "event=discovery_status".to_string(),
        format!("config_path={}", config_path.display()),
        format!("db_path={}", db_path.display()),
        format!("now={}", status.now.to_rfc3339()),
        format!("window_start={}", status.window_start.to_rfc3339()),
        format!("runtime_state={}", status.runtime_state),
        format!("runtime_mode={}", status.runtime_mode),
        format!("scoring_source={}", status.scoring_source),
        format!("active_follow_wallets={}", status.active_follow_wallets),
        format!("recent_swaps_window={}", status.recent_swaps_window),
        format!("raw_window_state={}", status.raw_window_state),
        format!(
            "latest_publication_ts={}",
            format_optional_ts(status.publication.latest_publication_ts.as_ref())
        ),
        format!(
            "publication_age_seconds={}",
            format_optional_u64(status.publication.publication_age_seconds)
        ),
        format!(
            "publication_runtime_mode={}",
            format_optional_str(status.publication.runtime_mode.as_deref())
        ),
        format!(
            "publication_scoring_source={}",
            format_optional_str(status.publication.published_scoring_source.as_deref())
        ),
        format!(
            "recent_publication_truth_available={}",
            status.publication.recent_publication_truth_available
        ),
        format!(
            "bootstrap_degraded_publication_truth_available={}",
            status
                .publication
                .bootstrap_degraded_publication_truth_available
        ),
        format!(
            "bootstrap_degraded_active={}",
            status.publication.bootstrap_degraded_active
        ),
        format!(
            "bootstrap_degraded_reason={}",
            format_optional_str(status.publication.bootstrap_degraded_reason.as_deref())
        ),
        format!(
            "bootstrap_degraded_armed_at={}",
            format_optional_ts(status.publication.bootstrap_degraded_armed_at.as_ref())
        ),
        format!(
            "persisted_rebuild_phase={}",
            status
                .persisted_rebuild
                .as_ref()
                .map(|rebuild| rebuild.phase.as_str())
                .unwrap_or("null")
        ),
        format!(
            "bounded_rebuild_cursor={}",
            status
                .persisted_rebuild
                .as_ref()
                .and_then(|rebuild| rebuild.cursor.as_deref())
                .unwrap_or("null")
        ),
        format!(
            "recent_raw_journal_available={}",
            status.recent_raw_restore.journal_available
        ),
        format!(
            "recent_raw_journal_replayed={}",
            status.recent_raw_restore.journal_replayed
        ),
        format!(
            "recent_raw_raw_coverage_satisfied={}",
            status.recent_raw_restore.raw_coverage_satisfied
        ),
        format!(
            "recent_raw_journal_covers_artifact_cursor={}",
            status.recent_raw_restore.journal_covers_artifact_cursor
        ),
        format!(
            "recent_raw_required_window_start={}",
            format_optional_ts(status.recent_raw_restore.required_window_start.as_ref())
        ),
        format!(
            "recent_raw_journal_covered_since={}",
            format_optional_ts(status.recent_raw_restore.journal_covered_since.as_ref())
        ),
        format!(
            "recent_raw_journal_covered_through_cursor={}",
            format_optional_cursor(
                status
                    .recent_raw_restore
                    .journal_covered_through_cursor
                    .as_ref()
            )
        ),
        format!(
            "recent_raw_artifact_runtime_cursor={}",
            format_optional_cursor(status.recent_raw_restore.artifact_runtime_cursor.as_ref())
        ),
        format!(
            "recent_raw_replayed_rows={}",
            status.recent_raw_restore.replayed_rows
        ),
        format!(
            "recent_raw_reason={}",
            format_optional_str(status.recent_raw_restore.reason.as_deref())
        ),
        format!("offline_recovery_state={}", status.offline_recovery.state),
        format!(
            "offline_recovery_cursor={}",
            format_optional_cursor(status.offline_recovery.cursor.as_ref())
        ),
        format!(
            "aggregate_covered_through_cursor={}",
            format_optional_cursor(status.offline_recovery.covered_through_cursor.as_ref())
        ),
    ]
    .join("\n")
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_str(value: Option<&str>) -> String {
    value.unwrap_or("null").to_string()
}

fn format_optional_cursor(
    value: Option<&copybot_discovery::operator_status::DiscoveryOperatorCursor>,
) -> String {
    value
        .map(|cursor| {
            format!(
                "ts_utc={},slot={},signature={}",
                cursor.ts_utc.to_rfc3339(),
                cursor.slot,
                cursor.signature
            )
        })
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::{parse_args_from, render_human, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow,
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, SqliteStore, WalletMetricRow,
        WalletUpsertRow,
    };
    use serde_json::{json, Value};
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_accepts_json_db_override_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");
        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn run_json_classifies_healthy_runtime_truth() -> Result<()> {
        let fixture = make_fixture("discovery-status-healthy")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_active_follow_wallet(
            &fixture.store,
            "wallet_healthy",
            now - Duration::minutes(30),
        )?;
        fixture.store.insert_observed_swap(&swap(
            "wallet_healthy",
            "healthy-swap",
            now - Duration::minutes(5),
            "So11111111111111111111111111111111111111112",
            "TokenHealthy111111111111111111111111111111",
            1.0,
            100.0,
        ))?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
                reason: "healthy_refresh".to_string(),
                last_published_at: Some(now - Duration::minutes(1)),
                last_published_window_start: Some(now - Duration::days(7)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_healthy".to_string()]),
            })?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.get("runtime_state").and_then(Value::as_str),
            Some("healthy_runtime_truth")
        );
        assert_eq!(
            parsed.get("runtime_mode").and_then(Value::as_str),
            Some("healthy")
        );
        assert_eq!(
            parsed.get("scoring_source").and_then(Value::as_str),
            Some("raw_window")
        );
        assert_eq!(
            parsed.get("active_follow_wallets").and_then(Value::as_u64),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn run_json_classifies_degraded_recent_publication_truth() -> Result<()> {
        let fixture = make_fixture("discovery-status-degraded")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_active_follow_wallet(
            &fixture.store,
            "wallet_published",
            now - Duration::minutes(30),
        )?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
                reason: "recent_publication".to_string(),
                last_published_at: Some(now - Duration::minutes(2)),
                last_published_window_start: Some(now - Duration::days(7)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_published".to_string()]),
            })?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.get("runtime_state").and_then(Value::as_str),
            Some("degraded_recent_publication_truth")
        );
        assert_eq!(
            parsed.get("runtime_mode").and_then(Value::as_str),
            Some("degraded")
        );
        assert_eq!(
            parsed.get("scoring_source").and_then(Value::as_str),
            Some("published_universe_raw_window_unavailable")
        );
        Ok(())
    }

    #[test]
    fn run_json_classifies_bootstrap_degraded_publication_truth() -> Result<()> {
        let fixture = make_fixture("discovery-status-bootstrap-degraded")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_active_follow_wallet(
            &fixture.store,
            "wallet_bootstrap",
            now - Duration::minutes(30),
        )?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
                reason: "stale_imported_artifact".to_string(),
                last_published_at: Some(now - Duration::minutes(61)),
                last_published_window_start: Some(now - Duration::days(7)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_bootstrap".to_string()]),
            })?;
        fixture.store.set_discovery_bootstrap_degraded_state(
            true,
            Some("runtime_artifact_restore_bootstrap_degraded"),
            Some(now - Duration::minutes(5)),
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.get("runtime_state").and_then(Value::as_str),
            Some("bootstrap_degraded_publication_truth")
        );
        assert_eq!(
            parsed.get("runtime_mode").and_then(Value::as_str),
            Some("bootstrap_degraded")
        );
        assert_eq!(
            parsed.get("scoring_source").and_then(Value::as_str),
            Some("bootstrap_degraded_publication_truth_raw_window_unavailable")
        );
        assert_eq!(
            parsed
                .pointer("/publication/bootstrap_degraded_active")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/publication/bootstrap_degraded_publication_truth_available")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/publication/recent_publication_truth_available")
                .and_then(Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn run_json_surfaces_recent_raw_restore_state() -> Result<()> {
        let fixture = make_fixture("discovery-status-recent-raw-restore")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        fixture.store.set_discovery_recent_raw_restore_state(
            &copybot_storage::DiscoveryRecentRawRestoreStateUpdate {
                journal_available: true,
                journal_replayed: true,
                required_window_start: Some(now - Duration::days(7)),
                journal_covered_since: Some(now - Duration::days(7)),
                journal_covered_through_cursor: Some(copybot_storage::DiscoveryRuntimeCursor {
                    ts_utc: now - Duration::minutes(1),
                    slot: 77,
                    signature: "recent-raw-covered".to_string(),
                }),
                artifact_runtime_cursor: Some(copybot_storage::DiscoveryRuntimeCursor {
                    ts_utc: now - Duration::minutes(2),
                    slot: 76,
                    signature: "recent-raw-artifact".to_string(),
                }),
                journal_covers_artifact_cursor: true,
                raw_coverage_satisfied: true,
                replayed_rows: 42,
                reason: Some("recent_raw_journal_replay_completed".to_string()),
                replay_started_at: Some(now - Duration::minutes(1)),
                replay_completed_at: Some(now),
            },
        )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed
                .pointer("/recent_raw_restore/journal_available")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/recent_raw_restore/raw_coverage_satisfied")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            parsed
                .pointer("/recent_raw_restore/replayed_rows")
                .and_then(Value::as_u64),
            Some(42)
        );
        Ok(())
    }

    #[test]
    fn render_human_surfaces_fail_closed_rebuild_and_offline_recovery_progress() -> Result<()> {
        let fixture = make_fixture("discovery-status-fail-closed")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        let rebuild_cursor_token = "TokenRebuildCursor111111111111111111111111".to_string();
        fixture.store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryPersistedRebuildStateRow {
                phase: DiscoveryPersistedRebuildPhase::CollectBuyMints,
                window_start: now - Duration::days(7),
                horizon_end: now,
                metrics_window_start: now - Duration::days(7),
                phase_cursor: None,
                prepass_rows_processed: 32,
                prepass_pages_processed: 1,
                replay_rows_processed: 0,
                replay_pages_processed: 0,
                chunks_completed: 1,
                state_json: json!({
                    "unique_buy_mints": [],
                    "buy_mint_counts": {},
                    "collect_buy_mints_mode": "ReconcileNewTail",
                    "collect_buy_mints_reconcile_new_tail_cursor_token": rebuild_cursor_token,
                    "replay_mode": "WalletStatsThenSolLeg",
                    "token_quality_cache": {},
                    "token_quality_progress": {
                        "next_mint_index": 0,
                        "rpc_attempted": 0,
                        "rpc_spent_ms": 0
                    },
                    "by_wallet": {},
                    "token_states": {},
                    "token_recent_sol_trades": {},
                    "pending_rug_checks": [],
                    "token_pending_buy_starts": {},
                    "completed_snapshots": []
                })
                .to_string(),
                started_at: now - Duration::minutes(10),
                updated_at: now - Duration::minutes(1),
            },
        )?;
        let backfill_cursor = DiscoveryRuntimeCursor {
            ts_utc: now - Duration::hours(2),
            slot: 77,
            signature: "offline-backfill-cursor".to_string(),
        };
        fixture
            .store
            .set_discovery_scoring_backfill_progress(now - Duration::days(10), &backfill_cursor)?;
        fixture
            .store
            .set_discovery_scoring_backfill_source_protection(
                now - Duration::days(10),
                now + Duration::minutes(30),
            )?;
        fixture
            .store
            .set_discovery_scoring_covered_since(now - Duration::days(10))?;
        fixture
            .store
            .set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
                ts_utc: now - Duration::hours(3),
                slot: 66,
                signature: "covered-through".to_string(),
            })?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: false,
            now,
        })?;
        assert!(output.contains("runtime_state=fail_closed_rebuild_in_progress"));
        assert!(output.contains("persisted_rebuild_phase=collect_buy_mints"));
        assert!(output.contains("bounded_rebuild_cursor=collect_buy_mints_new_tail_token"));
        assert!(output.contains("offline_recovery_state=backfill_in_progress"));
        assert!(output.contains("offline_recovery_cursor=ts_utc="));

        let discovery = copybot_discovery::DiscoveryService::new(
            copybot_config::load_from_path(&fixture.config_path)?.discovery,
            copybot_config::load_from_path(&fixture.config_path)?.shadow,
        );
        let store = SqliteStore::open(Path::new(&fixture.db_path))?;
        let status = discovery.operator_status(&store, now)?;
        let rendered = render_human(&fixture.config_path, &fixture.db_path, &status);
        assert_eq!(rendered, output);
        Ok(())
    }

    struct Fixture {
        store: SqliteStore,
        db_path: PathBuf,
        config_path: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(format!("{name}.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        std::fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(Fixture {
            store,
            db_path,
            config_path,
            _temp: temp,
        })
    }

    fn seed_active_follow_wallet(
        store: &SqliteStore,
        wallet_id: &str,
        window_start: DateTime<Utc>,
    ) -> Result<()> {
        store.persist_discovery_cycle(
            &[WalletUpsertRow {
                wallet_id: wallet_id.to_string(),
                first_seen: window_start,
                last_seen: window_start + Duration::minutes(1),
                status: "active".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: wallet_id.to_string(),
                window_start,
                pnl: 1.0,
                win_rate: 1.0,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[wallet_id.to_string()],
            true,
            true,
            window_start + Duration::minutes(2),
            "seed_active_follow_wallet",
        )?;
        Ok(())
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn swap(
        wallet: &str,
        signature: &str,
        ts_utc: DateTime<Utc>,
        token_in: &str,
        token_out: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> copybot_core_types::SwapEvent {
        copybot_core_types::SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in,
            amount_out,
            signature: signature.to_string(),
            slot: ts_utc.timestamp().max(0) as u64,
            ts_utc,
            exact_amounts: None,
        }
    }
}
