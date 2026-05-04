impl ObservedSwapWriter {
    #[cfg(test)]
    pub(crate) fn start_for_test(
        sqlite_path: String,
        channel_capacity: usize,
        batch_max_size: usize,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::for_test(
                channel_capacity,
                batch_max_size,
                aggregate_writes_enabled,
                aggregate_write_config,
                None,
            ),
        )
    }

    #[cfg(test)]
    pub(crate) fn start_for_test_with_normal_try_enqueue_soft_limit(
        sqlite_path: String,
        channel_capacity: usize,
        batch_max_size: usize,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        normal_try_enqueue_soft_limit: usize,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::for_test(
                channel_capacity,
                batch_max_size,
                aggregate_writes_enabled,
                aggregate_write_config,
                None,
            )
            .with_normal_try_enqueue_soft_limit(normal_try_enqueue_soft_limit),
        )
    }

    #[allow(dead_code)]
    pub(crate) async fn enqueue(&self, swap: &SwapEvent) -> Result<()> {
        self.send_request(ObservedSwapWriteRequest {
            swap: swap.clone(),
            reply_tx: None,
            enqueued_at: Instant::now(),
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) fn try_enqueue(&self, swap: &SwapEvent) -> Result<bool> {
        if self.telemetry.pending_requests.load(Ordering::Relaxed)
            >= self.normal_try_enqueue_soft_limit
        {
            return Ok(false);
        }
        self.try_enqueue_without_soft_limit(swap)
    }

    pub(crate) fn try_enqueue_discovery_critical(&self, swap: &SwapEvent) -> Result<bool> {
        self.try_enqueue_without_soft_limit(swap)
    }

    fn try_enqueue_without_soft_limit(&self, swap: &SwapEvent) -> Result<bool> {
        self.ensure_running()?;
        match self.sender.try_reserve() {
            Ok(permit) => {
                self.telemetry.note_enqueued();
                permit.send(ObservedSwapWriteRequest {
                    swap: swap.clone(),
                    reply_tx: None,
                    enqueued_at: Instant::now(),
                });
                Ok(true)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow!(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT))
                    .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)
            }
        }
    }

    pub(crate) async fn write(&self, swap: &SwapEvent) -> Result<bool> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_request(ObservedSwapWriteRequest {
            swap: swap.clone(),
            reply_tx: Some(reply_tx),
            enqueued_at: Instant::now(),
        })
        .await?;
        reply_rx
            .await
            .context(OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT)?
    }

    pub(crate) fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        self.telemetry.snapshot()
    }

    pub(crate) fn health_handle(&self) -> ObservedSwapWriterHealthHandle {
        ObservedSwapWriterHealthHandle {
            telemetry: Arc::clone(&self.telemetry),
        }
    }
}
