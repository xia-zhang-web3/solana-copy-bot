impl ObservedSwapWriter {
    fn start_with_config(sqlite_path: String, config: ObservedSwapWriterConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let terminal_failure_message = Arc::new(Mutex::new(None));
        let normal_try_enqueue_soft_limit =
            observed_swap_writer_normal_try_enqueue_soft_limit(&config);
        let journal_queue_capacity_batches = config
            .recent_raw_journal
            .as_ref()
            .map(|journal| journal.writer_queue_capacity_batches.max(1))
            .unwrap_or(0);
        telemetry
            .journal_queue_capacity_batches
            .store(journal_queue_capacity_batches, Ordering::Relaxed);
        let journal_overflow_capacity_batches = config
            .recent_raw_journal
            .as_ref()
            .map(|journal| journal.overflow_capacity_batches)
            .unwrap_or(0);
        telemetry
            .journal_overflow_capacity_batches
            .store(journal_overflow_capacity_batches, Ordering::Relaxed);
        let journal_overflow_row_capacity = config
            .recent_raw_journal
            .as_ref()
            .map(|journal| recent_raw_journal_overflow_row_capacity(config.batch_max_size, journal))
            .unwrap_or(0);
        telemetry
            .journal_overflow_row_debt_capacity
            .store(journal_overflow_row_capacity, Ordering::Relaxed);
        let journal_channel = config.recent_raw_journal.as_ref().map(|journal| {
            std_mpsc::sync_channel::<RecentRawJournalWriteRequest>(
                journal.writer_queue_capacity_batches.max(1),
            )
        });
        let journal_sender = journal_channel.as_ref().map(|(sender, _)| sender.clone());
        let journal_receiver = journal_channel.map(|(_, receiver)| receiver);
        let journal_startup_channel = config
            .recent_raw_journal
            .as_ref()
            .map(|_| std_mpsc::channel::<std::result::Result<(), String>>());
        let journal_startup_sender = journal_startup_channel
            .as_ref()
            .map(|(sender, _)| sender.clone());
        let journal_startup_receiver = journal_startup_channel.map(|(_, receiver)| receiver);
        let raw_worker_config = config.clone();
        let raw_worker_sqlite_path = sqlite_path.clone();

        let raw_worker_telemetry = Arc::clone(&telemetry);
        let raw_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
        let raw_worker = thread::Builder::new()
            .name("copybot-observed-swap-writer".to_string())
            .spawn(move || {
                let result = observed_swap_writer_loop(
                    raw_worker_sqlite_path,
                    receiver,
                    journal_sender,
                    journal_startup_receiver,
                    raw_worker_config,
                    raw_worker_telemetry,
                    Arc::clone(&raw_worker_terminal_failure_message),
                );
                if let Err(error) = &result {
                    set_terminal_failure_message(
                        &raw_worker_terminal_failure_message,
                        format!("{error:#}"),
                    );
                }
                result
            })
            .context("failed to spawn observed swap writer thread")?;

        let journal_worker = if let Some(receiver) = journal_receiver {
            let journal_worker_telemetry = Arc::clone(&telemetry);
            let journal_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
            let journal_config = config
                .recent_raw_journal
                .clone()
                .ok_or_else(|| anyhow!("missing recent raw journal config"))?;
            let startup_sender = journal_startup_sender
                .ok_or_else(|| anyhow!("missing recent raw journal startup sender"))?;
            Some(
                thread::Builder::new()
                    .name("copybot-recent-raw-journal-writer".to_string())
                    .spawn(move || {
                        let result = recent_raw_journal_writer_loop(
                            receiver,
                            startup_sender,
                            journal_config,
                            journal_worker_telemetry,
                        );
                        if let Err(error) = &result {
                            set_terminal_failure_message(
                                &journal_worker_terminal_failure_message,
                                format!("{error:#}"),
                            );
                        }
                        result
                    })
                    .context("failed to spawn recent raw journal writer thread")?,
            )
        } else {
            None
        };

        Ok(Self {
            sender,
            normal_try_enqueue_soft_limit,
            raw_worker: Some(raw_worker),
            journal_worker,
            telemetry,
            terminal_failure_message,
        })
    }
}
