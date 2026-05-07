use super::*;

pub(crate) struct ObservedSwapWriter {
    pub(in crate::observed_swap_writer) sender: mpsc::Sender<ObservedSwapWriteRequest>,
    pub(in crate::observed_swap_writer) normal_try_enqueue_soft_limit: usize,
    pub(in crate::observed_swap_writer) raw_worker: Option<thread::JoinHandle<Result<()>>>,
    pub(in crate::observed_swap_writer) journal_worker: Option<thread::JoinHandle<Result<()>>>,
    pub(in crate::observed_swap_writer) telemetry: Arc<ObservedSwapWriterTelemetry>,
    pub(in crate::observed_swap_writer) terminal_failure_message: Arc<Mutex<Option<String>>>,
}

impl ObservedSwapWriter {
    pub(in crate::observed_swap_writer) fn terminal_failure_error(&self) -> Option<anyhow::Error> {
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

    pub(in crate::observed_swap_writer) async fn send_request(
        &self,
        request: ObservedSwapWriteRequest,
    ) -> Result<()> {
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
    pub(crate) fn start(sqlite_path: String) -> Result<Self> {
        Self::start_with_recent_raw_journal(sqlite_path, None)
    }

    pub(crate) fn start_with_recent_raw_journal(
        sqlite_path: String,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::production(recent_raw_journal),
        )
    }
}
