fn discovery_aggregate_writer_loop(
    sqlite_path: String,
    receiver: std_mpsc::Receiver<DiscoveryAggregateWriteRequest>,
    startup_sender: std_mpsc::Sender<std::result::Result<(), String>>,
    config: ObservedSwapWriterConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
) -> Result<()> {
    let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for discovery aggregate writer: {sqlite_path}")
    })?;
    match run_aggregate_startup_replay(&sqlite_path, &store, &config) {
        Ok(_progress) => {
            telemetry.set_aggregate_gap_active(
                store
                    .load_discovery_scoring_materialization_gap_cursor()?
                    .is_some(),
            );
            let _ = startup_sender.send(Ok(()));
        }
        Err(error) => {
            let _ = startup_sender.send(Err(format!("{error:#}")));
            return Err(error);
        }
    }
    let mut gap_repair_epoch = DiscoveryAggregateGapRepairEpoch::default();

    loop {
        run_discovery_aggregate_gap_repair_slice(
            &sqlite_path,
            &store,
            &config,
            &telemetry,
            true,
            &mut gap_repair_epoch,
        )?;

        match receiver.recv_timeout(OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_POLL_INTERVAL) {
            Ok(request) => {
                let request = collect_discovery_aggregate_write_batch(
                    &receiver,
                    request,
                    config.aggregate_write_coalesce_max_batches,
                    &telemetry,
                );
                telemetry.set_aggregate_worker_busy(true);
                let result = process_discovery_aggregate_write_request(
                    &store,
                    &request.inserted_swaps,
                    request.batch_started,
                    &config.aggregate_write_config,
                    &telemetry,
                );
                telemetry.set_aggregate_worker_busy(false);
                result?;
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                run_discovery_aggregate_gap_repair_slice(
                    &sqlite_path,
                    &store,
                    &config,
                    &telemetry,
                    false,
                    &mut gap_repair_epoch,
                )?;
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    Ok(())
}
