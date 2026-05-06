use super::*;

include!("app_loop_startup.rs");

pub(super) async fn run_app_loop(
    store: SqliteStore,
    mut ingestion: IngestionService,
    discovery: DiscoveryService,
    shadow: ShadowService,
    risk_config: RiskConfig,
    sqlite_path: String,
    heartbeat_seconds: u64,
    history_retention_config: copybot_config::HistoryRetentionConfig,
    recent_raw_journal_config: copybot_config::RecentRawJournalConfig,
    discovery_fetch_refresh_seconds: u64,
    discovery_refresh_seconds: u64,
    observed_swaps_retention_days: u32,
    ingestion_source: String,
    shadow_refresh_seconds: u64,
    shadow_causal_holdback_enabled: bool,
    shadow_causal_holdback_ms: u64,
    pause_new_trades_on_outage: bool,
    alert_dispatcher: Option<AlertDispatcher>,
) -> Result<()> {
    let system_event_store = copybot_storage_core::SqliteStore::open(Path::new(&sqlite_path))
        .context("failed to open app system-event storage core")?;
    system_event_store
        .ensure_system_event_tables()
        .context("failed to initialize app system-event storage core")?;
    let AppLoopStartup {
        mut interval,
        mut risk_refresh_interval,
        mut shadow_interval,
        follow_snapshot,
        mut open_shadow_lots,
        shadow_strategy_fail_closed,
        stale_lot_max_hold_hours,
        stale_lot_terminal_zero_price_hours,
        stale_lot_recovery_zero_price_enabled,
        mut shadow_risk_guard,
        mut shadow_drop_reason_counts,
        mut shadow_drop_stage_counts,
        mut shadow_queue_full_outcome_counts,
        mut app_consumer_loop_telemetry,
        mut recent_swap_signatures,
        mut recent_swap_signature_order,
        mut pending_irrelevant_swaps,
        mut discovery_critical_target_buy_mints,
        mut discovery_critical_target_buy_mints_backpressure_refresh_state,
        mut zero_universe_empty_target_noncritical_best_effort,
        mut shadow_scheduler,
        observed_swap_writer,
        latest_ingestion_runtime_snapshot,
        observed_swap_retention_runtime_health,
        observed_swap_retention_config,
        observed_swap_retention_sweep_interval,
        app_started_at,
        mut last_observed_swap_retention_sweep,
        history_retention,
        history_retention_sweep_interval,
        mut last_history_retention_sweep,
        mut last_sqlite_contention_snapshot,
        mut last_history_retention_skip_reason_key,
        mut last_observed_swap_retention_skip_reason_key,
        mut operator_emergency_stop,
        mut observed_swap_retention_handle,
        mut ingestion_error_streak,
        mut ingestion_backoff_until,
    } = initialize_app_loop_startup(
        &store,
        &ingestion,
        &discovery,
        risk_config,
        &sqlite_path,
        heartbeat_seconds,
        history_retention_config,
        &recent_raw_journal_config,
        discovery_fetch_refresh_seconds,
        discovery_refresh_seconds,
        observed_swaps_retention_days,
        ingestion_source,
        shadow_refresh_seconds,
        pause_new_trades_on_outage,
    )?;

    loop {
        operator_emergency_stop.refresh(&store, Utc::now())?;

        let shadow_queue_full = prepare_shadow_scheduler_before_select(
            &store,
            &sqlite_path,
            &shadow,
            &mut shadow_scheduler,
            &open_shadow_lots,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        )?;

        tokio::select! {
            shadow_result = shadow_scheduler.shadow_workers.join_next(), if !shadow_scheduler.shadow_workers.is_empty() => {
                handle_shadow_worker_join(
                    shadow_result,
                    &mut shadow_scheduler,
                    &mut open_shadow_lots,
                    &mut shadow_drop_reason_counts,
                    &mut shadow_drop_stage_counts,
                )?;
            }
            snapshot_result = async {
                if let Some(handle) = &mut shadow_scheduler.shadow_snapshot_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if shadow_scheduler.shadow_snapshot_handle.is_some() => {
                handle_shadow_snapshot_join(
                    &store,
                    snapshot_result,
                    &mut shadow_scheduler,
                    shadow_strategy_fail_closed,
                    &mut open_shadow_lots,
                    follow_snapshot.as_ref(),
                    &mut shadow_drop_reason_counts,
                    &mut shadow_drop_stage_counts,
                    &mut shadow_queue_full_outcome_counts,
                )?;
            }
            _ = interval.tick() => {
                handle_app_heartbeat_tick(
                    &system_event_store,
                    alert_dispatcher.as_ref(),
                    &latest_ingestion_runtime_snapshot,
                    &ingestion,
                    &observed_swap_writer,
                    &sqlite_path,
                    &history_retention,
                    &observed_swap_retention_runtime_health,
                    observed_swap_retention_config,
                    observed_swap_retention_sweep_interval,
                    history_retention_sweep_interval,
                    app_started_at,
                    &mut last_observed_swap_retention_sweep,
                    &mut last_history_retention_sweep,
                    &mut last_sqlite_contention_snapshot,
                    &mut last_history_retention_skip_reason_key,
                    &mut last_observed_swap_retention_skip_reason_key,
                    &mut observed_swap_retention_handle,
                    &mut app_consumer_loop_telemetry,
                )
                .await?;
            }
            _ = risk_refresh_interval.tick() => {
                handle_risk_refresh_tick(&store, &ingestion, &mut shadow_risk_guard)?;
            }
            observed_swap_retention_join = async {
                match observed_swap_retention_handle.as_mut() {
                    Some(handle) => Some(handle.await),
                    None => None,
                }
            }, if observed_swap_retention_handle.is_some() => {
                handle_observed_swap_retention_join(
                    observed_swap_retention_join,
                    &mut observed_swap_retention_handle,
                    observed_swap_retention_sweep_interval,
                    &mut last_observed_swap_retention_sweep,
                )?;
            }
            _ = async {
                if let Some(until) = ingestion_backoff_until {
                    time::sleep_until(until).await;
                }
            }, if ingestion_backoff_until.is_some() => {
                ingestion_backoff_until = None;
            }
            _ = time::sleep(OBSERVED_SWAP_WRITER_BACKPRESSURE_RETRY_INTERVAL), if !pending_irrelevant_swaps.is_empty() => {
                retry_pending_irrelevant_swaps(
                    &store,
                    &ingestion,
                    &observed_swap_writer,
                    &follow_snapshot,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &mut discovery_critical_target_buy_mints,
                    &mut discovery_critical_target_buy_mints_backpressure_refresh_state,
                    &mut zero_universe_empty_target_noncritical_best_effort,
                    &mut pending_irrelevant_swaps,
                    &mut recent_swap_signatures,
                    &mut recent_swap_signature_order,
                    &mut app_consumer_loop_telemetry,
                )
                .await?;
            }
            maybe_swap = ingestion.next_swap(), if ingestion_backoff_until.is_none() => {
                let ingestion_snapshot = ingestion.runtime_snapshot();
                handle_ingestion_swap_poll(
                    &store,
                    &observed_swap_writer,
                    &shadow,
                    &sqlite_path,
                    maybe_swap,
                    ingestion_snapshot,
                    &follow_snapshot,
                    &mut shadow_scheduler,
                    &open_shadow_lots,
                    shadow_strategy_fail_closed,
                    &mut shadow_risk_guard,
                    &operator_emergency_stop,
                    pause_new_trades_on_outage,
                    shadow_queue_full,
                    shadow_causal_holdback_enabled,
                    shadow_causal_holdback_ms,
                    &mut discovery_critical_target_buy_mints,
                    &mut discovery_critical_target_buy_mints_backpressure_refresh_state,
                    &mut zero_universe_empty_target_noncritical_best_effort,
                    &mut pending_irrelevant_swaps,
                    &mut recent_swap_signatures,
                    &mut recent_swap_signature_order,
                    &mut app_consumer_loop_telemetry,
                    &mut shadow_drop_reason_counts,
                    &mut shadow_drop_stage_counts,
                    &mut shadow_queue_full_outcome_counts,
                    &mut ingestion_error_streak,
                    &mut ingestion_backoff_until,
                )
                .await?;
            }
            _ = shadow_interval.tick() => {
                handle_shadow_interval_tick(
                    &store,
                    &sqlite_path,
                    &shadow,
                    &mut shadow_scheduler,
                    &mut open_shadow_lots,
                    shadow_strategy_fail_closed,
                    stale_lot_max_hold_hours,
                    stale_lot_terminal_zero_price_hours,
                    stale_lot_recovery_zero_price_enabled,
                )?;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    shutdown_app_loop_tasks(
        &mut observed_swap_retention_handle,
        &mut shadow_scheduler,
        observed_swap_writer,
        &system_event_store,
    )
}
