use super::*;
#[cfg(test)]
use crate::app_tests::b70_hooks::stop as app_loop_stop;
#[cfg(not(test))]
use tokio::signal::ctrl_c as app_loop_stop;

mod startup;

use startup::{initialize_app_loop_startup, AppLoopStartup};

const RUNTIME_FOLLOW_RELOAD_MAX_INTERVAL_SECS: u64 = 30;

pub(crate) fn runtime_follow_reload_interval_seconds(seconds: u64) -> u64 {
    seconds.max(1).min(RUNTIME_FOLLOW_RELOAD_MAX_INTERVAL_SECS)
}

fn runtime_follow_reload_interval(seconds: u64) -> time::Interval {
    let interval = Duration::from_secs(runtime_follow_reload_interval_seconds(seconds));
    let start = time::Instant::now() + interval;
    let mut ticker = time::interval_at(start, interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker
}

pub(super) async fn run_app_loop(
    store: SqliteStore,
    mut ingestion: IngestionService,
    discovery: DiscoveryService,
    shadow: ShadowService,
    execution_config: ExecutionConfig,
    risk_config: RiskConfig,
    ingestion_config: IngestionConfig,
    shadow_config: ShadowConfig,
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
        mut follow_snapshot,
        mut open_shadow_lots,
        mut shadow_strategy_fail_closed,
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
    let mut runtime_follow_reload_interval =
        runtime_follow_reload_interval(discovery_fetch_refresh_seconds);
    let materialize_execution_canary_quote_loss =
        execution_config.canary_enabled && execution_config.canary_tiny_submit_enabled;
    let stale_close_quote_pricer = StaleCloseQuotePricer::new(execution_config.clone());
    let entry_quote_shadow_diagnostic = EntryQuoteShadowDiagnostic::new(execution_config.clone());
    let exit_policy_shadow_quote = ExitPolicyShadowQuoteDiagnostic::new(execution_config.clone());
    let market_exit_shadow_quote = MarketExitShadowQuoteDiagnostic::new(execution_config.clone());
    let execution_canary_runner = ExecutionCanaryRunner::new(execution_config)
        .for_ingestion(&ingestion_config, &sqlite_path)?;
    execution_canary_runner.log_startup_status();
    let mut execution_canary_interval = time::interval(Duration::from_secs(
        execution_canary_runner.interval_seconds(),
    ));
    execution_canary_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    match recover_shadow_restart_gap(
        &store,
        &observed_swap_writer,
        &shadow,
        &ingestion_config,
        &shadow_config,
        &mut open_shadow_lots,
        &mut recent_swap_signatures,
        &mut recent_swap_signature_order,
        &mut shadow_drop_reason_counts,
        &mut shadow_drop_stage_counts,
    )
    .await
    {
        Ok(summary) => {
            if summary.skipped_reason.is_some()
                || summary.swaps_fetched > 0
                || summary.rpc_errors > 0
            {
                info!(
                    enabled = summary.enabled,
                    skipped_reason = summary.skipped_reason.unwrap_or("none"),
                    wallets_scanned = summary.wallets_scanned,
                    signatures_seen = summary.signatures_seen,
                    transactions_fetched = summary.transactions_fetched,
                    rpc_errors = summary.rpc_errors,
                    swaps_fetched = summary.swaps_fetched,
                    sell_candidates = summary.sell_candidates,
                    recovered_sells = summary.recovered_sells,
                    skipped_without_open_lot = summary.skipped_without_open_lot,
                    skipped_non_sell = summary.skipped_non_sell,
                    duplicate_recent = summary.duplicate_recent,
                    observed_persist_errors = summary.observed_persist_errors,
                    realized_pnl_sol = summary.realized_pnl_sol,
                    "shadow restart recovery completed"
                );
            }
        }
        Err(error) => {
            warn!(
                error = %error,
                "shadow restart recovery failed; continuing live ingestion"
            );
        }
    }

    let mut association_consumer = crate::association_consumer::AssociationConsumer::start(
        &mut ingestion,
        &ingestion_config,
        &sqlite_path,
    )
    .await?;
    let association_mode = association_consumer.is_some();
    let shadow_wake = association_consumer.as_ref().map(|c| c.shadow_wake.clone());
    #[cfg(test)]
    let mut ingestion = crate::app_tests::b70_hooks::consumer(&mut ingestion);
    let mut source_sell_recovery_tick =
        crate::source_sell_staging::SourceSellStaging::recovery_interval();
    let mut deferred_hot_buy: Option<crate::execution_quote_canary::job::HotQuoteOrigin> = None;
    let loop_result: Result<()> = async {
    loop {
        operator_emergency_stop.refresh(&store, Utc::now())?;
        // Observation-only delivery does not consume legacy SELL handoffs. Even
        // an empty recovery visit writes a cursor and can block the inbox loop.
        if !association_mode {
            shadow_scheduler.source_sells.recover(&store, &sqlite_path)?;
        }

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
            result=crate::association_consumer::poll(&mut association_consumer,&store), if association_mode=>{
                result?;
            }
            _ = async {}, if deferred_hot_buy.is_some() => {
                if let Some(origin) = deferred_hot_buy.take() {
                    let id = origin.signal_id();
                    if let Err(error) = execution_canary_runner.resume_hot_buy(
                        &store, origin, &follow_snapshot, shadow_strategy_fail_closed,
                        &mut shadow_risk_guard, &operator_emergency_stop,
                        pause_new_trades_on_outage, Utc::now()).await {
                        crate::telemetry::hot_quote::failure(&id, "hot_buy_resume_error", &error,
                            shadow_scheduler.active_task_count(), shadow_scheduler.buffered_shadow_task_count());
                    }
                }
            }
            _ = shadow_scheduler.hot_quotes.collect_next(), if shadow_scheduler.hot_quotes.can_collect() => {}
            _ = async {}, if deferred_hot_buy.is_none() && shadow_scheduler.hot_completion_ready() => {
                if let Some(completion) = shadow_scheduler.hot_quotes.take_completion() {
                    deferred_hot_buy = execution_canary_runner.complete_hot_observed_buy_quote(
                        &store, completion, &follow_snapshot, shadow_strategy_fail_closed,
                        &mut shadow_risk_guard, &operator_emergency_stop,
                        pause_new_trades_on_outage, Utc::now(), shadow_scheduler.active_task_count(),
                        shadow_scheduler.buffered_shadow_task_count());
                }
            }
            _ = source_sell_recovery_tick.tick(), if !association_mode => {
                shadow_scheduler.source_sells.recover(&store, &sqlite_path)?;
            }
            completion = shadow_scheduler.source_sells.finish_next(), if !shadow_scheduler.source_sells.is_empty() => {
                completion?; // Separate typed staging completion; no execution consumer.
            }
            shadow_result = shadow_scheduler.shadow_workers.join_next(), if !shadow_scheduler.shadow_workers.is_empty() => {
                if let Some(signal) = handle_shadow_worker_join(
                    &store,
                    shadow_result,
                    &mut shadow_scheduler,
                    &mut open_shadow_lots,
                    &mut shadow_drop_reason_counts,
                    &mut shadow_drop_stage_counts,
                )? {
                    handle_execution_canary_for_shadow_signal(
                        &execution_canary_runner,
                        &store,
                        signal,
                    )
                    .await;
                }
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
            _ = execution_canary_interval.tick(), if execution_canary_runner.is_enabled() => {
                match execution_canary_runner.process_tick(&store, Utc::now()).await {
                    Ok(summary) if summary.has_status_change() => {
                        crate::telemetry::record_execution_canary_tick(&summary);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(error = %error, "execution canary dry-run tick failed");
                    }
                }
                #[cfg(test)]
                crate::app_tests::b70_hooks::execution_tick(association_consumer.as_ref());
            }
            _ = runtime_follow_reload_interval.tick() => {
                let reload_now = Utc::now();
                let runtime_publication_truth =
                    startup_runtime_publication_truth(&discovery, &sqlite_path, reload_now)
                        .context("failed to reload runtime V2 follow publication")?;
                if let Some(reload) = runtime_follow_reload_from_publication_truth(
                    follow_snapshot.as_ref(),
                    shadow_strategy_fail_closed,
                    runtime_publication_truth.as_ref(),
                    reload_now,
                ) {
                    if reload.shadow_strategy_fail_closed {
                        open_shadow_lots.clear();
                        warn!(
                            previous_active_follow_wallets = follow_snapshot.active.len(),
                            source = reload.source,
                            "runtime follow universe fail-closed during live reload"
                        );
                    } else {
                        refresh_shadow_open_lot_index_or_warn(&store, &mut open_shadow_lots)?;
                        info!(
                            active_follow_wallets = reload.active_follow_wallets,
                            added_wallets = reload.added_wallets,
                            removed_wallets = reload.removed_wallets,
                            source = reload.source,
                            "runtime follow universe reloaded from V2 publication"
                        );
                    }
                    shadow_strategy_fail_closed = reload.shadow_strategy_fail_closed;
                    follow_snapshot = Arc::new(reload.follow_snapshot);
                }
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
            maybe_swap = ingestion.next_swap(), if ingestion_backoff_until.is_none() && !association_mode => {
                let ingestion_snapshot = ingestion.runtime_snapshot();
                handle_ingestion_swap_poll(
                    &store,
                    &observed_swap_writer,
                    &execution_canary_runner,
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
                    materialize_execution_canary_quote_loss,
                    &stale_close_quote_pricer,
                    &entry_quote_shadow_diagnostic,
                    &exit_policy_shadow_quote,
                    &market_exit_shadow_quote,
                    shadow_wake.as_ref(),
                ).await?;
            }
            _ = app_loop_stop() => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    Ok(())
    }.await;
    drop(association_consumer);
    if let Some(origin) = deferred_hot_buy.take() {
        crate::telemetry::hot_quote::record(
            &origin.signal_id(),
            "hot_quote_shutdown",
            shadow_scheduler.active_task_count(),
            shadow_scheduler.buffered_shadow_task_count(),
        );
    }
    shadow_scheduler.hot_quotes.shutdown().await;
    loop_result?;
    shadow_scheduler.source_sells.drain().await?;
    #[cfg(test)]
    crate::app_tests::b70_hooks::checked_shutdown(&mut shadow_scheduler).await?;
    shutdown_app_loop_tasks(
        &mut observed_swap_retention_handle,
        &mut shadow_scheduler,
        observed_swap_writer,
        &system_event_store,
    )
}

async fn handle_execution_canary_for_shadow_signal(
    execution_canary_runner: &ExecutionCanaryRunner,
    store: &SqliteStore,
    signal: copybot_shadow::ShadowSignalResult,
) {
    match execution_canary_runner
        .process_recorded_shadow_signal(store, &signal, Utc::now())
        .await
    {
        Ok(summary) if summary.has_status_change() => {
            crate::telemetry::record_execution_canary_shadow_signal(&summary, &signal);
        }
        Ok(_) => {}
        Err(error) => {
            warn!(
                error = %error,
                signal_id = %signal.signal_id,
                "execution canary shadow-signal task failed"
            );
        }
    }
    #[cfg(test)]
    crate::app_tests::b70_hooks::mark("recorded_owner_processed", &signal.signal_id);
}
