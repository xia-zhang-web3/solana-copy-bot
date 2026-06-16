use super::*;

pub(crate) async fn handle_app_heartbeat_tick(
    system_event_store: &copybot_storage_core::SqliteStore,
    alert_dispatcher: Option<&AlertDispatcher>,
    latest_ingestion_runtime_snapshot: &Arc<Mutex<Option<IngestionRuntimeSnapshot>>>,
    ingestion: &IngestionService,
    observed_swap_writer: &ObservedSwapWriter,
    sqlite_path: &str,
    history_retention: &HistoryRetentionRunner,
    observed_swap_retention_runtime_health: &ObservedSwapRetentionRuntimeHealthHandle,
    observed_swap_retention_config: ObservedSwapRetentionConfig,
    observed_swap_retention_sweep_interval: Duration,
    history_retention_sweep_interval: Duration,
    app_started_at: StdInstant,
    last_observed_swap_retention_sweep: &mut StdInstant,
    last_history_retention_sweep: &mut StdInstant,
    last_sqlite_contention_snapshot: &mut SqliteContentionSnapshot,
    last_history_retention_skip_reason_key: &mut Option<&'static str>,
    last_observed_swap_retention_skip_reason_key: &mut Option<&'static str>,
    observed_swap_retention_handle: &mut Option<
        JoinHandle<Result<ObservedSwapRetentionMaintenanceSummary>>,
    >,
    app_consumer_loop_telemetry: &mut AppConsumerLoopTelemetry,
) -> Result<()> {
    if let Err(error) = system_event_store.record_heartbeat("copybot-app", "alive") {
        if runtime_sqlite_write_error_requires_restart(&error) {
            return Err(error).context("runtime heartbeat write failed with fatal sqlite I/O");
        }
        warn!(error = %error, "heartbeat write failed");
    }
    if let Some(dispatcher) = &alert_dispatcher {
        match dispatcher.deliver_pending(system_event_store).await {
            Ok(delivered) if delivered > 0 => {
                info!(delivered, "delivered pending alert webhooks");
            }
            Ok(_) => {}
            Err(error) => {
                if alert_delivery_error_requires_restart(&error) {
                    return Err(error).context("alert delivery poll failed with fatal sqlite I/O");
                }
                warn!(error = %error, "alert delivery poll failed");
            }
        }
    }
    let maintenance_gate_started_at = StdInstant::now();
    let maintenance_gate_sqlite_contention = sqlite_contention_snapshot();
    let maintenance_gate_ingestion_snapshot = ingestion.runtime_snapshot();
    if let Ok(mut snapshot) = latest_ingestion_runtime_snapshot.lock() {
        *snapshot = maintenance_gate_ingestion_snapshot;
    }
    let maintenance_gate_writer_snapshot = observed_swap_writer.snapshot();
    if history_retention.enabled()
        && last_history_retention_sweep.elapsed() >= history_retention_sweep_interval
    {
        if let Some(reason) = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::HistoryRetention,
            app_started_at,
            maintenance_gate_started_at,
            &maintenance_gate_writer_snapshot,
            *last_sqlite_contention_snapshot,
            maintenance_gate_sqlite_contention,
            maintenance_gate_ingestion_snapshot,
        ) {
            let reason_key = sqlite_maintenance_block_reason_key(&reason);
            if *last_history_retention_skip_reason_key != Some(reason_key) {
                info!(
                    maintenance = SqliteMaintenanceTask::HistoryRetention.as_str(),
                    reason = %reason,
                    "sqlite maintenance blocked by runtime health gate"
                );
                *last_history_retention_skip_reason_key = Some(reason_key);
            }
        } else {
            info!(
                maintenance = SqliteMaintenanceTask::HistoryRetention.as_str(),
                observed_swap_writer_pending_requests =
                    maintenance_gate_writer_snapshot.pending_requests,
                observed_swap_writer_journal_queue_depth_batches =
                    maintenance_gate_writer_snapshot.journal_queue_depth_batches,
                yellowstone_output_queue_depth = maintenance_gate_ingestion_snapshot
                    .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                    .unwrap_or(0),
                yellowstone_output_queue_capacity = maintenance_gate_ingestion_snapshot
                    .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                    .unwrap_or(0),
                yellowstone_output_queue_fill_ratio = maintenance_gate_ingestion_snapshot
                    .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                    .unwrap_or(0.0),
                sqlite_write_retry_total = maintenance_gate_sqlite_contention.write_retry_total,
                sqlite_busy_error_total = maintenance_gate_sqlite_contention.busy_error_total,
                "starting sqlite maintenance task"
            );
            let history_now = Utc::now();
            let mut history_completed_full_sweep = true;
            match history_retention.apply(
                system_event_store,
                history_now,
                alert_dispatcher.is_some(),
            ) {
                Ok(summary) => {
                    history_completed_full_sweep = summary.completed_full_sweep;
                    if !summary.is_empty() {
                        info!(
                            risk_events_deleted = summary.risk_events_deleted,
                            risk_events_batches = summary.risk_events_batches,
                            copy_signals_deleted = summary.copy_signals_deleted,
                            copy_signals_batches = summary.copy_signals_batches,
                            orders_deleted = summary.orders_deleted,
                            execution_order_batches = summary.execution_order_batches,
                            fills_deleted = summary.fills_deleted,
                            shadow_closed_trades_deleted = summary.shadow_closed_trades_deleted,
                            shadow_closed_trades_batches = summary.shadow_closed_trades_batches,
                            execution_quote_canary_events_deleted =
                                summary.execution_quote_canary_events_deleted,
                            execution_quote_canary_provider_samples_deleted =
                                summary.execution_quote_canary_provider_samples_deleted,
                            execution_quote_canary_shadow_gate_events_deleted =
                                summary.execution_quote_canary_shadow_gate_events_deleted,
                            execution_quote_canary_event_batches =
                                summary.execution_quote_canary_event_batches,
                            execution_quote_canary_provider_sample_batches =
                                summary.execution_quote_canary_provider_sample_batches,
                            execution_quote_canary_shadow_gate_batches =
                                summary.execution_quote_canary_shadow_gate_batches,
                            completed_full_sweep = summary.completed_full_sweep,
                            "history retention sweep applied"
                        );
                    }
                }
                Err(error) => {
                    if history_retention_error_requires_restart(&error) {
                        return Err(error)
                            .context("history retention sweep failed with fatal sqlite I/O");
                    }
                    warn!(error = %error, "history retention sweep failed");
                }
            }
            *last_history_retention_skip_reason_key = None;
            *last_history_retention_sweep = if history_completed_full_sweep {
                StdInstant::now()
            } else {
                StdInstant::now()
                    .checked_sub(
                        history_retention_sweep_interval
                            .saturating_sub(SQLITE_MAINTENANCE_PARTIAL_RETRY_INTERVAL),
                    )
                    .unwrap_or_else(StdInstant::now)
            };
        }
    }
    if observed_swap_retention_handle.is_none()
        && last_observed_swap_retention_sweep.elapsed() >= observed_swap_retention_sweep_interval
    {
        if let Some(reason) = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            app_started_at,
            maintenance_gate_started_at,
            &maintenance_gate_writer_snapshot,
            *last_sqlite_contention_snapshot,
            maintenance_gate_sqlite_contention,
            maintenance_gate_ingestion_snapshot,
        ) {
            let reason_key = sqlite_maintenance_block_reason_key(&reason);
            if *last_observed_swap_retention_skip_reason_key != Some(reason_key) {
                info!(
                    maintenance = SqliteMaintenanceTask::ObservedSwapRetention.as_str(),
                    reason = %reason,
                    "sqlite maintenance blocked by runtime health gate"
                );
                *last_observed_swap_retention_skip_reason_key = Some(reason_key);
            }
        } else {
            let sqlite_path = sqlite_path.to_string();
            let observed_swap_retention_runtime_health =
                observed_swap_retention_runtime_health.clone();
            info!(
                maintenance = SqliteMaintenanceTask::ObservedSwapRetention.as_str(),
                observed_swap_writer_pending_requests =
                    maintenance_gate_writer_snapshot.pending_requests,
                observed_swap_writer_journal_queue_depth_batches =
                    maintenance_gate_writer_snapshot.journal_queue_depth_batches,
                yellowstone_output_queue_depth = maintenance_gate_ingestion_snapshot
                    .map(|snapshot| snapshot.yellowstone_output_queue_depth)
                    .unwrap_or(0),
                yellowstone_output_queue_capacity = maintenance_gate_ingestion_snapshot
                    .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
                    .unwrap_or(0),
                yellowstone_output_queue_fill_ratio = maintenance_gate_ingestion_snapshot
                    .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
                    .unwrap_or(0.0),
                sqlite_write_retry_total = maintenance_gate_sqlite_contention.write_retry_total,
                sqlite_busy_error_total = maintenance_gate_sqlite_contention.busy_error_total,
                "starting sqlite maintenance task"
            );
            *observed_swap_retention_handle = Some(tokio::task::spawn_blocking(move || {
                run_observed_swap_retention_maintenance_once(
                    &sqlite_path,
                    observed_swap_retention_config,
                    Some(observed_swap_retention_runtime_health),
                )
            }));
            *last_observed_swap_retention_sweep = StdInstant::now();
            *last_observed_swap_retention_skip_reason_key = None;
        }
    }
    let sqlite_contention = sqlite_contention_snapshot();
    let wal_size_bytes = std::fs::metadata(format!("{sqlite_path}-wal"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let observed_swap_writer_snapshot = observed_swap_writer.snapshot();
    let app_consumer_loop_snapshot = app_consumer_loop_telemetry.snapshot_and_reset();
    info!(
        observed_swap_writer_pending_requests = observed_swap_writer_snapshot.pending_requests,
        observed_swap_writer_write_ms_p95 = observed_swap_writer_snapshot.write_latency_ms_p95,
        observed_swap_writer_raw_batch_ms_p95 =
            observed_swap_writer_snapshot.raw_batch_write_ms_p95,
        observed_swap_writer_observed_swaps_insert_ms_p95 =
            observed_swap_writer_snapshot.observed_swaps_insert_ms_p95,
        observed_swap_writer_wallet_activity_days_ms_p95 =
            observed_swap_writer_snapshot.wallet_activity_days_ms_p95,
        observed_swap_writer_journal_enqueue_wait_ms_p95 =
            observed_swap_writer_snapshot.journal_enqueue_wait_ms_p95,
        observed_swap_writer_journal_batch_write_ms_p95 =
            observed_swap_writer_snapshot.journal_batch_write_ms_p95,
        observed_swap_writer_worker_busy_ms_p95 = observed_swap_writer_snapshot.worker_busy_ms_p95,
        observed_swap_writer_journal_queue_depth_batches =
            observed_swap_writer_snapshot.journal_queue_depth_batches,
        observed_swap_writer_journal_queue_row_debt =
            observed_swap_writer_snapshot.journal_queue_row_debt,
        observed_swap_writer_journal_queue_capacity_batches =
            observed_swap_writer_snapshot.journal_queue_capacity_batches,
        observed_swap_writer_journal_overflow_depth_batches =
            observed_swap_writer_snapshot.journal_overflow_depth_batches,
        observed_swap_writer_journal_overflow_row_debt =
            observed_swap_writer_snapshot.journal_overflow_row_debt,
        observed_swap_writer_journal_overflow_capacity_batches =
            observed_swap_writer_snapshot.journal_overflow_capacity_batches,
        observed_swap_writer_journal_overflow_row_debt_capacity =
            observed_swap_writer_snapshot.journal_overflow_row_debt_capacity,
        observed_swap_writer_journal_writer_inflight_rows =
            observed_swap_writer_snapshot.journal_writer_inflight_rows,
        observed_swap_writer_journal_sqlite_write_retry_total =
            observed_swap_writer_snapshot.journal_sqlite_write_retry_total,
        observed_swap_writer_journal_sqlite_busy_error_total =
            observed_swap_writer_snapshot.journal_sqlite_busy_error_total,
        "observed swap writer telemetry"
    );
    info!(
        app_consumer_swaps_seen = app_consumer_loop_snapshot.swaps_seen,
        app_follow_rejected = app_consumer_loop_snapshot.follow_rejected,
        app_follow_rejected_ratio = app_consumer_loop_snapshot.follow_rejected_ratio,
        app_consumer_loop_time_ms_p95 = app_consumer_loop_snapshot.processing_ms_p95,
        "app consumer loop telemetry"
    );
    info!(
        sqlite_write_retry_total = sqlite_contention.write_retry_total,
        sqlite_busy_error_total = sqlite_contention.busy_error_total,
        sqlite_wal_size_bytes = wal_size_bytes,
        "sqlite contention counters"
    );
    *last_sqlite_contention_snapshot = sqlite_contention;
    Ok(())
}
