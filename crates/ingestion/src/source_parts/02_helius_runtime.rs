use super::*;

#[allow(dead_code)]
impl HeliusWsSource {
    async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        loop {
            self.ensure_pipeline_running()?;

            if let Some(raw) = self.pop_ready_observation() {
                self.maybe_report_pipeline_metrics();
                return Ok(Some(raw));
            }

            let wait_for_ready = self.reorder_wait_duration();
            match self.recv_from_pipeline(wait_for_ready).await {
                OutputRecvOutcome::Item(item) => {
                    self.push_reorder_entry(item);
                    self.maybe_report_pipeline_metrics();
                }
                OutputRecvOutcome::TimedOut => {
                    self.maybe_report_pipeline_metrics();
                    continue;
                }
                OutputRecvOutcome::ChannelClosed => {
                    warn!("ingestion pipeline output channel closed; restarting pipeline");
                    self.pipeline = None;
                    if let Some(raw) = self.pop_earliest_observation() {
                        self.maybe_report_pipeline_metrics();
                        return Ok(Some(raw));
                    }
                    self.maybe_report_pipeline_metrics();
                    continue;
                }
            }
        }
    }

    fn ensure_pipeline_running(&mut self) -> Result<()> {
        let needs_restart = self
            .pipeline
            .as_ref()
            .map(|pipeline| {
                pipeline.ws_reader_task.is_finished()
                    || pipeline.fetcher_tasks.iter().any(|task| task.is_finished())
            })
            .unwrap_or(true);
        if needs_restart {
            if self.pipeline.is_some() {
                warn!("ingestion pipeline became unhealthy; recreating pipeline tasks");
            }
            self.pipeline = Some(self.spawn_pipeline()?);
        }
        Ok(())
    }

    fn spawn_pipeline(&self) -> Result<HeliusPipeline> {
        if self.runtime_config.ws_url.contains("REPLACE_ME")
            || self
                .runtime_config
                .http_endpoints
                .iter()
                .any(|endpoint| endpoint.url.contains("REPLACE_ME"))
            || self.runtime_config.http_endpoints.is_empty()
        {
            return Err(anyhow!(
                "configure ingestion.helius_ws_url and ingestion.helius_http_url / ingestion.helius_http_urls with real API key(s)"
            ));
        }

        let notification_queue = Arc::new(NotificationQueue::new(self.ws_queue_capacity));
        let (out_tx, out_rx) = mpsc::channel::<FetchedObservation>(self.output_queue_capacity);
        let ws_to_fetch_depth = Arc::new(AtomicUsize::new(0));
        let fetch_to_output_depth = Arc::new(AtomicUsize::new(0));

        let ws_reader_task = {
            let runtime_config = Arc::clone(&self.runtime_config);
            let notification_queue = Arc::clone(&notification_queue);
            let ws_to_fetch_depth = Arc::clone(&ws_to_fetch_depth);
            let queue_overflow_policy = self.queue_overflow_policy;
            tokio::spawn(async move {
                ws_reader_loop(
                    runtime_config,
                    notification_queue,
                    ws_to_fetch_depth,
                    queue_overflow_policy,
                )
                .await;
            })
        };

        let mut fetcher_tasks = Vec::with_capacity(self.fetch_concurrency);
        for worker_id in 0..self.fetch_concurrency {
            let runtime_config = Arc::clone(&self.runtime_config);
            let notification_queue = Arc::clone(&notification_queue);
            let out_tx = out_tx.clone();
            let ws_to_fetch_depth = Arc::clone(&ws_to_fetch_depth);
            let fetch_to_output_depth = Arc::clone(&fetch_to_output_depth);
            fetcher_tasks.push(tokio::spawn(async move {
                fetch_worker_loop(
                    worker_id,
                    runtime_config,
                    notification_queue,
                    out_tx,
                    ws_to_fetch_depth,
                    fetch_to_output_depth,
                )
                .await;
            }));
        }
        drop(out_tx);

        Ok(HeliusPipeline {
            output_rx: out_rx,
            ws_to_fetch_depth,
            fetch_to_output_depth,
            ws_reader_task,
            fetcher_tasks,
        })
    }

    async fn recv_from_pipeline(&mut self, wait: Option<Duration>) -> OutputRecvOutcome {
        let Some(pipeline) = self.pipeline.as_mut() else {
            return OutputRecvOutcome::ChannelClosed;
        };

        if let Some(wait) = wait {
            match time::timeout(wait, pipeline.output_rx.recv()).await {
                Ok(Some(item)) => {
                    decrement_atomic_usize(&pipeline.fetch_to_output_depth);
                    OutputRecvOutcome::Item(item)
                }
                Ok(None) => OutputRecvOutcome::ChannelClosed,
                Err(_) => OutputRecvOutcome::TimedOut,
            }
        } else {
            match pipeline.output_rx.recv().await {
                Some(item) => {
                    decrement_atomic_usize(&pipeline.fetch_to_output_depth);
                    OutputRecvOutcome::Item(item)
                }
                None => OutputRecvOutcome::ChannelClosed,
            }
        }
    }

    pub(super) fn push_reorder_entry(&mut self, fetched: FetchedObservation) {
        self.runtime_config
            .telemetry
            .push_fetch_latency(fetched.fetch_latency_ms);
        self.reorder.push(fetched);
        self.runtime_config
            .telemetry
            .note_reorder_buffer_size(self.reorder.len());
    }

    pub(super) fn pop_ready_observation(&mut self) -> Option<RawSwapObservation> {
        self.reorder
            .pop_ready()
            .map(|release| self.apply_reorder_release(release))
    }

    fn pop_earliest_observation(&mut self) -> Option<RawSwapObservation> {
        self.reorder
            .pop_earliest()
            .map(|release| self.apply_reorder_release(release))
    }

    fn reorder_wait_duration(&self) -> Option<Duration> {
        self.reorder.wait_duration()
    }

    fn apply_reorder_release(&self, release: ReorderRelease) -> RawSwapObservation {
        self.runtime_config
            .telemetry
            .push_reorder_hold(release.hold_ms);
        self.runtime_config
            .telemetry
            .push_ingestion_lag(release.lag_ms);
        release.raw
    }

    fn maybe_report_pipeline_metrics(&self) {
        let (ws_depth, output_depth) = self
            .pipeline
            .as_ref()
            .map(|pipeline| {
                (
                    pipeline.ws_to_fetch_depth.load(Ordering::Relaxed),
                    pipeline.fetch_to_output_depth.load(Ordering::Relaxed),
                )
            })
            .unwrap_or((0, 0));
        self.runtime_config.telemetry.maybe_report(
            self.telemetry_report_seconds,
            ws_depth,
            output_depth,
            self.reorder.len(),
        );
    }

    fn runtime_snapshot(&self) -> IngestionRuntimeSnapshot {
        self.runtime_config.telemetry.snapshot()
    }
}
