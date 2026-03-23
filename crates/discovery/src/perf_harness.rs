use crate::{
    CollectBuyMintsMode, DiscoveryService, PersistedStreamBudgetExhaustedReason,
    PersistedStreamProgressTelemetry, PersistedStreamRebuildAdvanceOutcome,
    PersistedStreamRebuildRestoreOutcome, ReplayMode, SOL_MINT, STALE_RECONCILE_TOKEN_BATCH_CAP,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_storage::{DiscoveryPersistedRebuildPhase, SqliteStore};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

static NEXT_HARNESS_WORKSPACE_ID: AtomicU64 = AtomicU64::new(0);

const STANDARD_COMMAND: &str =
    "RUSTFLAGS='-Awarnings' cargo run -p copybot-discovery --quiet --bin discovery_perf_harness";

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryPerfHarnessReport {
    pub fixture_name: String,
    pub command: String,
    pub scenarios: Vec<DiscoveryPerfHarnessScenarioReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryPerfHarnessScenarioReport {
    pub scenario: String,
    pub description: String,
    pub fixture: DiscoveryPerfHarnessFixture,
    pub fetch_limit: usize,
    pub fetch_page_limit: usize,
    pub rebuild_time_budget_ms: u64,
    pub restore_outcome: String,
    pub completed: bool,
    pub cycles: Vec<DiscoveryPerfHarnessCycleReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryPerfHarnessFixture {
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub metrics_window_start: DateTime<Utc>,
    pub source_window_start: Option<DateTime<Utc>>,
    pub source_metrics_window_start: Option<DateTime<Utc>>,
    pub observed_rows: usize,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryPerfHarnessCycleReport {
    pub cycle: usize,
    pub phase: String,
    pub collect_buy_mints_mode: String,
    pub replay_mode: String,
    pub partial: bool,
    pub completed: bool,
    pub budget_exhausted_reason: Option<String>,
    pub chunks_completed: usize,
    pub cycle_rows_processed: usize,
    pub cycle_pages_processed: usize,
    pub prepass_rows_processed: usize,
    pub prepass_pages_processed: usize,
    pub replay_wallet_stats_rows_processed: usize,
    pub replay_wallet_stats_pages_processed: usize,
    pub replay_wallet_stats_wallets_processed: usize,
    pub replay_rows_processed: usize,
    pub replay_pages_processed: usize,
    pub observed_swaps_loaded: usize,
    pub unique_buy_mints: usize,
    pub wallets_buffered: usize,
    pub phase_cursor_signature: Option<String>,
    pub collect_buy_mints_cursor_token: Option<String>,
    pub collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    pub collect_buy_mints_reconcile_new_tail_pending_mints: usize,
    pub collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    pub replay_wallet_stats_wallet_cursor: Option<String>,
    pub quality_rpc_spent_ms: u64,
    pub cycle_elapsed_ms: u64,
    pub total_elapsed_ms: u64,
}

pub fn run_standard_harness() -> Result<DiscoveryPerfHarnessReport> {
    Ok(DiscoveryPerfHarnessReport {
        fixture_name: "package_c_discovery_bottlenecks_v1".to_string(),
        command: STANDARD_COMMAND.to_string(),
        scenarios: vec![
            run_stale_collect_buy_mints_harness()?,
            run_bounded_replay_harness()?,
        ],
    })
}

fn run_stale_collect_buy_mints_harness() -> Result<DiscoveryPerfHarnessScenarioReport> {
    let _workspace = HarnessWorkspace::create("stale-collect-buy-mints")?;
    let mut store = _workspace.open_store()?;
    run_migrations(&mut store)?;

    let mut config = harness_runtime_config();
    config.metric_snapshot_interval_seconds = 60;
    let fetch_limit = 20_000usize;
    let fetch_page_limit = 1usize;
    let rebuild_time_budget = StdDuration::from_secs(5);
    let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

    let source_now = parse_harness_timestamp("2026-03-19T02:10:50Z")?;
    let target_now = source_now + Duration::seconds(60);
    let source_window_start = source_now - Duration::days(config.scoring_window_days.max(1) as i64);
    let source_metrics_window_start = metrics_window_start_for_harness(&config, source_now);
    let target_window_start = target_now - Duration::days(config.scoring_window_days.max(1) as i64);
    let target_metrics_window_start = metrics_window_start_for_harness(&config, target_now);

    let survivor = "TokenHarnessCollectSurvivor1111111111111111111".to_string();
    let new_tail_token_count = STALE_RECONCILE_TOKEN_BATCH_CAP.saturating_add(40);
    let mut observed_rows = 0usize;
    store.insert_observed_swap(&swap(
        "wallet_harness_collect",
        "harness-collect-survivor",
        target_window_start + Duration::seconds(5),
        SOL_MINT,
        &survivor,
        1.0,
        10.0,
    ))?;
    observed_rows = observed_rows.saturating_add(1);
    for idx in 0..new_tail_token_count {
        let token = format!("TokenHarnessCollectNewTail{idx:04}111111111111111111");
        store.insert_observed_swap(&swap(
            "wallet_harness_collect",
            &format!("harness-collect-new-tail-{idx:04}"),
            source_now + Duration::seconds((idx % 30) as i64 + 1),
            SOL_MINT,
            &token,
            1.0,
            10.0,
        ))?;
        observed_rows = observed_rows.saturating_add(1);
    }
    store.insert_observed_swap(&swap(
        "wallet_harness_collect",
        "harness-collect-future-noise",
        target_now + Duration::seconds(5),
        SOL_MINT,
        "TokenHarnessCollectFutureNoise1111111111111111",
        1.0,
        10.0,
    ))?;
    observed_rows = observed_rows.saturating_add(1);

    let mut state = discovery.start_persisted_stream_rebuild_state(
        source_window_start,
        source_metrics_window_start,
        source_now,
    );
    state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
    state.payload.collect_buy_mints_prepass_complete = true;
    state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
    state.payload.unique_buy_mints = vec![survivor];
    if !discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
        &mut state,
        target_window_start,
        target_metrics_window_start,
        target_now,
    )? {
        return Err(anyhow!(
            "stale collect_buy_mints harness expected exact rollover carry-forward state"
        ));
    }
    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor = None;
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor_token = None;
    store.upsert_discovery_persisted_rebuild_state(
        &DiscoveryService::persisted_stream_rebuild_row(&state, target_now)?,
    )?;

    let restore_outcome = describe_restore_outcome(
        &discovery,
        &store,
        target_window_start,
        target_metrics_window_start,
        target_now,
    )?;
    let cycles = drive_rebuild_cycles(
        &discovery,
        &store,
        target_window_start,
        target_metrics_window_start,
        target_now,
        fetch_limit,
        fetch_page_limit,
        rebuild_time_budget,
        12,
        Some(DiscoveryPersistedRebuildPhase::CollectBuyMints),
    )?;
    let completed = cycles.last().map(|cycle| cycle.completed).unwrap_or(false);

    Ok(DiscoveryPerfHarnessScenarioReport {
        scenario: "stale_collect_buy_mints_convergence".to_string(),
        description:
            "stale new-tail collect_buy_mints reconciliation on an exact carry-forward checkpoint"
                .to_string(),
        fixture: DiscoveryPerfHarnessFixture {
            now: target_now,
            window_start: target_window_start,
            metrics_window_start: target_metrics_window_start,
            source_window_start: Some(source_window_start),
            source_metrics_window_start: Some(source_metrics_window_start),
            observed_rows,
            notes: vec![
                format!("survivor_tokens=1"),
                format!("stale_new_tail_tokens={new_tail_token_count}"),
                "page_budget=1 so each cycle exposes bounded exact-count progress".to_string(),
            ],
        },
        fetch_limit,
        fetch_page_limit,
        rebuild_time_budget_ms: rebuild_time_budget.as_millis() as u64,
        restore_outcome,
        completed,
        cycles,
    })
}

fn run_bounded_replay_harness() -> Result<DiscoveryPerfHarnessScenarioReport> {
    let _workspace = HarnessWorkspace::create("bounded-replay")?;
    let mut store = _workspace.open_store()?;
    run_migrations(&mut store)?;

    let config = harness_runtime_config();
    let fetch_limit = 3usize;
    let fetch_page_limit = 1usize;
    let rebuild_time_budget = StdDuration::from_secs(5);
    let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());

    let now = parse_harness_timestamp("2026-03-20T06:45:00Z")?;
    let (window_start, metrics_window_start, observed_rows) =
        seed_replay_harness_fixture(&store, &config, now)?;
    let mut state =
        discovery.start_persisted_stream_rebuild_state(window_start, metrics_window_start, now);
    state.phase = DiscoveryPersistedRebuildPhase::Replay;
    state.horizon_end = now;
    state.payload.replay_mode = ReplayMode::WalletStatsThenSolLeg;
    state.payload.unique_buy_mints = store.load_observed_buy_mints_in_window(window_start, now)?;
    store.upsert_discovery_persisted_rebuild_state(
        &DiscoveryService::persisted_stream_rebuild_row(&state, now)?,
    )?;

    let restore_outcome =
        describe_restore_outcome(&discovery, &store, window_start, metrics_window_start, now)?;
    let cycles = drive_rebuild_cycles(
        &discovery,
        &store,
        window_start,
        metrics_window_start,
        now,
        fetch_limit,
        fetch_page_limit,
        rebuild_time_budget,
        8,
        Some(DiscoveryPersistedRebuildPhase::Replay),
    )?;
    let completed = cycles.last().map(|cycle| cycle.completed).unwrap_or(false);

    Ok(DiscoveryPerfHarnessScenarioReport {
        scenario: "bounded_replay".to_string(),
        description:
            "wallet-stats-first replay on a bounded page budget with local deterministic noise"
                .to_string(),
        fixture: DiscoveryPerfHarnessFixture {
            now,
            window_start,
            metrics_window_start,
            source_window_start: None,
            source_metrics_window_start: None,
            observed_rows,
            notes: vec![
                "profitable_pairs=6".to_string(),
                "non_sol_noise_rows=120".to_string(),
                "wallet_stats page budget uses replay catch-up widening; sol-leg replay stays on the base page limit"
                    .to_string(),
            ],
        },
        fetch_limit,
        fetch_page_limit,
        rebuild_time_budget_ms: rebuild_time_budget.as_millis() as u64,
        restore_outcome,
        completed,
        cycles,
    })
}

fn drive_rebuild_cycles(
    discovery: &DiscoveryService,
    store: &SqliteStore,
    window_start: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    now: DateTime<Utc>,
    fetch_limit: usize,
    fetch_page_limit: usize,
    rebuild_time_budget: StdDuration,
    max_cycles: usize,
    focus_phase: Option<DiscoveryPersistedRebuildPhase>,
) -> Result<Vec<DiscoveryPerfHarnessCycleReport>> {
    let mut cycles = Vec::new();
    for cycle in 1..=max_cycles.max(1) {
        let outcome = discovery.advance_persisted_stream_rebuild(
            store,
            window_start,
            metrics_window_start,
            now,
            fetch_limit,
            fetch_page_limit,
            rebuild_time_budget,
        )?;
        let state = load_persisted_stream_state(store)?;
        match outcome {
            PersistedStreamRebuildAdvanceOutcome::InProgress { telemetry } => {
                let report = cycle_report(cycle, &telemetry, state.as_ref());
                let phase_exited = focus_phase
                    .map(|phase| report.phase != phase.as_str())
                    .unwrap_or(false);
                cycles.push(report);
                if phase_exited {
                    break;
                }
            }
            PersistedStreamRebuildAdvanceOutcome::Completed { telemetry, .. } => {
                cycles.push(cycle_report(cycle, &telemetry, state.as_ref()));
                break;
            }
        }
    }
    if cycles.is_empty() {
        return Err(anyhow!("discovery perf harness produced no cycle reports"));
    }
    Ok(cycles)
}

fn cycle_report(
    cycle: usize,
    telemetry: &PersistedStreamProgressTelemetry,
    state: Option<&crate::PersistedStreamRebuildState>,
) -> DiscoveryPerfHarnessCycleReport {
    DiscoveryPerfHarnessCycleReport {
        cycle,
        phase: telemetry.phase.as_str().to_string(),
        collect_buy_mints_mode: telemetry.collect_buy_mints_mode.as_str().to_string(),
        replay_mode: telemetry.replay_mode.as_str().to_string(),
        partial: telemetry.partial,
        completed: telemetry.completed,
        budget_exhausted_reason: telemetry
            .budget_exhausted_reason
            .map(PersistedStreamBudgetExhaustedReason::as_str)
            .map(str::to_string),
        chunks_completed: telemetry.chunks_completed,
        cycle_rows_processed: telemetry.cycle_rows_processed,
        cycle_pages_processed: telemetry.cycle_pages_processed,
        prepass_rows_processed: telemetry.prepass_rows_processed,
        prepass_pages_processed: telemetry.prepass_pages_processed,
        replay_wallet_stats_rows_processed: telemetry.replay_wallet_stats_rows_processed,
        replay_wallet_stats_pages_processed: telemetry.replay_wallet_stats_pages_processed,
        replay_wallet_stats_wallets_processed: telemetry
            .replay_wallet_stats_day_count_source_progress
            .fast_path_wallets_processed
            .saturating_add(
                telemetry
                    .replay_wallet_stats_day_count_source_progress
                    .fallback_wallets_processed,
            ),
        replay_rows_processed: telemetry.replay_rows_processed,
        replay_pages_processed: telemetry.replay_pages_processed,
        observed_swaps_loaded: telemetry.observed_swaps_loaded,
        unique_buy_mints: telemetry.unique_buy_mints,
        wallets_buffered: telemetry.wallets_buffered,
        phase_cursor_signature: telemetry
            .phase_cursor
            .as_ref()
            .map(|cursor| cursor.signature.clone()),
        collect_buy_mints_cursor_token: telemetry.collect_buy_mints_cursor_token.clone(),
        collect_buy_mints_reconcile_new_tail_cursor_token: telemetry
            .collect_buy_mints_reconcile_new_tail_cursor_token
            .clone(),
        collect_buy_mints_reconcile_new_tail_pending_mints: telemetry
            .collect_buy_mints_reconcile_new_tail_pending_mints,
        collect_buy_mints_reconcile_new_tail_slice_end_token: telemetry
            .collect_buy_mints_reconcile_new_tail_slice_end_token
            .clone(),
        replay_wallet_stats_wallet_cursor: state
            .and_then(|state| state.payload.replay_wallet_stats_wallet_cursor.clone()),
        quality_rpc_spent_ms: telemetry.quality_rpc_spent_ms,
        cycle_elapsed_ms: telemetry.cycle_elapsed_ms,
        total_elapsed_ms: telemetry.total_elapsed_ms,
    }
}

fn describe_restore_outcome(
    discovery: &DiscoveryService,
    store: &SqliteStore,
    window_start: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<String> {
    let (_, outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
        store,
        window_start,
        metrics_window_start,
        now,
    )?;
    Ok(match outcome {
        PersistedStreamRebuildRestoreOutcome::StartedFresh => "started_fresh",
        PersistedStreamRebuildRestoreOutcome::ResumedExisting => "resumed_existing",
        PersistedStreamRebuildRestoreOutcome::CarriedForwardMetricsWindow => {
            "carried_forward_metrics_window"
        }
        PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow => {
            "resumed_stale_metrics_window"
        }
    }
    .to_string())
}

fn seed_replay_harness_fixture(
    store: &SqliteStore,
    config: &DiscoveryConfig,
    now: DateTime<Utc>,
) -> Result<(DateTime<Utc>, DateTime<Utc>, usize)> {
    let window_start = now - Duration::days(config.scoring_window_days.max(1) as i64);
    let metrics_window_start = metrics_window_start_for_harness(config, now);
    let profitable_pairs = 6usize;
    let non_sol_noise_rows = 120usize;
    let mut observed_rows = 0usize;

    for idx in 0..profitable_pairs {
        let ts = window_start + Duration::minutes((idx * 12) as i64 + 1);
        let token = format!("TokenHarnessReplayTop{idx:04}11111111111111111111111");
        store.insert_observed_swap(&swap(
            "wallet_harness_replay_top",
            &format!("harness-replay-buy-{idx:04}"),
            ts,
            SOL_MINT,
            &token,
            1.0,
            100.0,
        ))?;
        store.insert_observed_swap(&swap(
            "wallet_harness_replay_top",
            &format!("harness-replay-sell-{idx:04}"),
            ts + Duration::minutes(5),
            &token,
            SOL_MINT,
            100.0,
            1.2,
        ))?;
        observed_rows = observed_rows.saturating_add(2);
    }

    for idx in 0..non_sol_noise_rows {
        let ts = window_start
            + Duration::minutes((idx % 240) as i64)
            + Duration::seconds((idx / 240) as i64);
        let token_in = format!("TokenHarnessReplayNoiseIn{:03}111111111111111111", idx % 17);
        let token_out = format!("TokenHarnessReplayNoiseOut{:03}1111111111111111", idx % 19);
        store.insert_observed_swap(&swap(
            &format!("wallet_harness_replay_noise_{:02}", idx % 11),
            &format!("harness-replay-noise-{idx:04}"),
            ts,
            &token_in,
            &token_out,
            2.0,
            3.0,
        ))?;
        observed_rows = observed_rows.saturating_add(1);
    }

    Ok((window_start, metrics_window_start, observed_rows))
}

fn load_persisted_stream_state(
    store: &SqliteStore,
) -> Result<Option<crate::PersistedStreamRebuildState>> {
    let row = store.load_discovery_persisted_rebuild_state()?;
    row.map(DiscoveryService::persisted_stream_rebuild_state_from_row)
        .transpose()
}

fn harness_runtime_config() -> DiscoveryConfig {
    let mut config = DiscoveryConfig::default();
    config.scoring_window_days = 7;
    config.decay_window_days = 7;
    config.observed_swaps_retention_days = 14;
    config.follow_top_n = 1;
    config.min_leader_notional_sol = 0.5;
    config.min_trades = 4;
    config.min_active_days = 1;
    config.min_score = 0.1;
    config.min_buy_count = 1;
    config.min_tradable_ratio = 0.0;
    config.metric_snapshot_interval_seconds = 3_600;
    config.max_window_swaps_in_memory = 8;
    config.max_fetch_swaps_per_cycle = 5;
    config.max_fetch_pages_per_cycle = 1;
    config.fetch_time_budget_ms = 60_000;
    config.thin_market_min_unique_traders = 1;
    config
}

fn permissive_shadow_quality() -> ShadowConfig {
    let mut config = ShadowConfig::default();
    config.min_token_age_seconds = 0;
    config.min_holders = 0;
    config.min_liquidity_sol = 0.0;
    config.min_volume_5m_sol = 0.0;
    config.min_unique_traders_5m = 0;
    config
}

fn metrics_window_start_for_harness(config: &DiscoveryConfig, now: DateTime<Utc>) -> DateTime<Utc> {
    let interval_seconds = config.metric_snapshot_interval_seconds.max(1) as i64;
    let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(config.scoring_window_days.max(1) as i64)
}

fn parse_harness_timestamp(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("failed parsing harness timestamp {raw}"))?
        .with_timezone(&Utc))
}

fn swap(
    wallet: &str,
    signature: &str,
    ts_utc: DateTime<Utc>,
    token_in: &str,
    token_out: &str,
    amount_in: f64,
    amount_out: f64,
) -> SwapEvent {
    SwapEvent {
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

fn run_migrations(store: &mut SqliteStore) -> Result<()> {
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store
        .run_migrations(&migration_dir)
        .context("failed running harness sqlite migrations")?;
    Ok(())
}

struct HarnessWorkspace {
    root: PathBuf,
}

impl HarnessWorkspace {
    fn create(name: &str) -> Result<Self> {
        let id = NEXT_HARNESS_WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "copybot-discovery-perf-harness-{name}-{}-{id}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root)
                .with_context(|| format!("failed clearing stale harness dir {}", root.display()))?;
        }
        fs::create_dir_all(&root)
            .with_context(|| format!("failed creating harness dir {}", root.display()))?;
        Ok(Self { root })
    }

    fn open_store(&self) -> Result<SqliteStore> {
        let db_path = self.root.join("harness.sqlite");
        SqliteStore::open(&db_path)
            .with_context(|| format!("failed opening harness sqlite db {}", db_path.display()))
    }
}

impl Drop for HarnessWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        run_bounded_replay_harness, run_stale_collect_buy_mints_harness, run_standard_harness,
    };
    use anyhow::Result;

    #[test]
    fn standard_harness_reports_both_discovery_bottleneck_scenarios() -> Result<()> {
        let report = run_standard_harness()?;
        assert_eq!(report.scenarios.len(), 2);
        assert_eq!(
            report.scenarios[0].scenario,
            "stale_collect_buy_mints_convergence"
        );
        assert_eq!(report.scenarios[1].scenario, "bounded_replay");
        assert!(
            report
                .scenarios
                .iter()
                .all(|scenario| !scenario.cycles.is_empty()),
            "each standard harness scenario should emit at least one comparable cycle sample"
        );
        Ok(())
    }

    #[test]
    fn stale_collect_buy_mints_harness_exposes_new_tail_progress_markers() -> Result<()> {
        let report = run_stale_collect_buy_mints_harness()?;
        assert!(
            report
                .cycles
                .iter()
                .any(|cycle| cycle.collect_buy_mints_mode == "reconcile_new_tail"),
            "stale collect_buy_mints harness should exercise the exact new-tail reconciliation path"
        );
        assert!(
            report.cycles.iter().any(|cycle| {
                cycle
                    .collect_buy_mints_reconcile_new_tail_cursor_token
                    .is_some()
                    || cycle.collect_buy_mints_reconcile_new_tail_pending_mints > 0
            }),
            "stale collect_buy_mints harness should expose a cursor or pending-batch progress marker"
        );
        Ok(())
    }

    #[test]
    fn bounded_replay_harness_exposes_wallet_stats_or_phase_cursor_progress() -> Result<()> {
        let report = run_bounded_replay_harness()?;
        assert!(report.cycles.iter().any(|cycle| cycle.phase == "replay"));
        assert!(
            report.cycles.iter().any(|cycle| {
                cycle.replay_wallet_stats_wallet_cursor.is_some()
                    || cycle.phase_cursor_signature.is_some()
                    || cycle.replay_wallet_stats_wallets_processed > 0
            }),
            "bounded replay harness should expose wallet-stats or replay cursor progress"
        );
        Ok(())
    }
}
