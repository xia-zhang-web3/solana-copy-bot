pub(crate) struct ObservedSwapWriter {
    sender: mpsc::Sender<ObservedSwapWriteRequest>,
    normal_try_enqueue_soft_limit: usize,
    raw_worker: Option<thread::JoinHandle<Result<()>>>,
    aggregate_worker: Option<thread::JoinHandle<Result<()>>>,
    journal_worker: Option<thread::JoinHandle<Result<()>>>,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
    terminal_failure_message: Arc<Mutex<Option<String>>>,
}

impl ObservedSwapWriter {
    fn terminal_failure_error(&self) -> Option<anyhow::Error> {
        self.terminal_failure_message
            .lock()
            .ok()
            .and_then(|message| message.clone())
            .map(|message| anyhow!(message).context(OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT))
    }

    pub(crate) fn ensure_running(&self) -> Result<()> {
        if let Some(error) = self.terminal_failure_error() {
            return Err(error);
        }
        Ok(())
    }

    async fn send_request(&self, request: ObservedSwapWriteRequest) -> Result<()> {
        self.ensure_running()?;
        let permit = self
            .sender
            .reserve()
            .await
            .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)?;
        self.telemetry.note_enqueued();
        permit.send(request);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn start(
        sqlite_path: String,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Result<Self> {
        Self::start_with_recent_raw_journal(
            sqlite_path,
            aggregate_writes_enabled,
            aggregate_write_config,
            None,
        )
    }

    pub(crate) fn start_with_recent_raw_journal(
        sqlite_path: String,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::production(
                aggregate_writes_enabled,
                aggregate_write_config,
                recent_raw_journal,
            ),
        )
    }
}
