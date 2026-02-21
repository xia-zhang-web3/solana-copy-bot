use anyhow::{anyhow, Result};
use copybot_config::IngestionConfig;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::warn;

use super::core::decrement_atomic_usize;
use super::telemetry::IngestionTelemetry;
use super::yellowstone_pipeline::yellowstone_stream_loop;
use super::{
    FetchedObservation, IngestionRuntimeSnapshot, QueueOverflowPolicy, RawObservationQueue,
    RawSwapObservation, ReorderBuffer, ReorderRelease, YellowstoneGrpcSource, YellowstonePipeline,
    YellowstoneRecvOutcome, YellowstoneRuntimeConfig,
};

impl YellowstoneGrpcSource {
    pub fn new(config: &IngestionConfig) -> Result<Self> {
        let mut interested_program_ids: HashSet<String> =
            config.yellowstone_program_ids.iter().cloned().collect();
        if interested_program_ids.is_empty() {
            interested_program_ids.extend(config.subscribe_program_ids.iter().cloned());
        }
        if interested_program_ids.is_empty() {
            interested_program_ids.extend(config.raydium_program_ids.iter().cloned());
            interested_program_ids.extend(config.pumpswap_program_ids.iter().cloned());
        }

        if interested_program_ids.is_empty() {
            return Err(anyhow!(
                "yellowstone_grpc requires at least one program id (yellowstone_program_ids / subscribe_program_ids / raydium+pumpswap)"
            ));
        }

        let raw_queue_policy = config.queue_overflow_policy.trim();
        let queue_overflow_policy = QueueOverflowPolicy::parse(raw_queue_policy);
        let normalized_queue_policy = raw_queue_policy.to_ascii_lowercase();
        if !raw_queue_policy.is_empty()
            && normalized_queue_policy != "block"
            && normalized_queue_policy != "drop_oldest"
            && normalized_queue_policy != "drop-oldest"
        {
            warn!(
                policy = %raw_queue_policy,
                "unknown ingestion.queue_overflow_policy; falling back to block"
            );
        }

        let grpc_url = config.yellowstone_grpc_url.trim();
        if grpc_url.is_empty()
            || grpc_url.contains("REPLACE_ME")
            || !(grpc_url.starts_with("http://") || grpc_url.starts_with("https://"))
        {
            return Err(anyhow!(
                "yellowstone_grpc requires ingestion.yellowstone_grpc_url with explicit http(s):// endpoint"
            ));
        }

        let x_token = config.yellowstone_x_token.trim();
        if x_token.is_empty() || x_token.contains("REPLACE_ME") {
            return Err(anyhow!(
                "yellowstone_grpc requires ingestion.yellowstone_x_token (x-token auth)"
            ));
        }

        let runtime_config = YellowstoneRuntimeConfig {
            grpc_url: grpc_url.to_string(),
            x_token: x_token.to_string(),
            connect_timeout_ms: config.yellowstone_connect_timeout_ms.max(500),
            subscribe_timeout_ms: config.yellowstone_subscribe_timeout_ms.max(1_000),
            reconnect_initial_ms: config.yellowstone_reconnect_initial_ms.max(200),
            reconnect_max_ms: config
                .yellowstone_reconnect_max_ms
                .max(config.yellowstone_reconnect_initial_ms.max(200)),
            stream_buffer_capacity: config.yellowstone_stream_buffer_capacity.max(64),
            seen_signatures_limit: config.seen_signatures_limit.max(500),
            seen_signatures_ttl: Duration::from_millis(config.seen_signatures_ttl_ms.max(1_000)),
            interested_program_ids,
            raydium_program_ids: config.raydium_program_ids.iter().cloned().collect(),
            pumpswap_program_ids: config.pumpswap_program_ids.iter().cloned().collect(),
            telemetry: Arc::new(IngestionTelemetry::default()),
        };

        Ok(Self {
            runtime_config: Arc::new(runtime_config),
            queue_overflow_policy,
            reorder: ReorderBuffer::new(
                config.reorder_hold_ms.max(1),
                config.reorder_max_buffer.max(16),
            ),
            telemetry_report_seconds: config.telemetry_report_seconds.max(5),
            pipeline: None,
        })
    }

    pub(super) async fn next_observation(&mut self) -> Result<Option<RawSwapObservation>> {
        loop {
            self.ensure_pipeline_running()?;

            if let Some(raw) = self.pop_ready_observation() {
                self.maybe_report_pipeline_metrics();
                return Ok(Some(raw));
            }

            let wait_for_ready = self.reorder_wait_duration();
            match self.recv_from_pipeline(wait_for_ready).await {
                YellowstoneRecvOutcome::Item(item) => {
                    self.push_reorder_entry(item);
                    self.maybe_report_pipeline_metrics();
                }
                YellowstoneRecvOutcome::TimedOut => {
                    self.maybe_report_pipeline_metrics();
                    continue;
                }
                YellowstoneRecvOutcome::QueueClosed => {
                    warn!("yellowstone stream queue closed; restarting pipeline");
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
            .map(|pipeline| pipeline.stream_task.is_finished())
            .unwrap_or(true);
        if needs_restart {
            if self.pipeline.is_some() {
                warn!("yellowstone ingestion pipeline became unhealthy; recreating stream task");
            }
            self.pipeline = Some(self.spawn_pipeline()?);
        }
        Ok(())
    }

    fn spawn_pipeline(&self) -> Result<YellowstonePipeline> {
        if self.runtime_config.grpc_url.trim().is_empty()
            || self.runtime_config.grpc_url.contains("REPLACE_ME")
            || self.runtime_config.x_token.trim().is_empty()
        {
            return Err(anyhow!(
                "configure ingestion.yellowstone_grpc_url and ingestion.yellowstone_x_token with real QuickNode credentials"
            ));
        }

        let output_queue = Arc::new(RawObservationQueue::new(
            self.runtime_config.stream_buffer_capacity,
        ));
        let output_queue_depth = Arc::new(AtomicUsize::new(0));
        let stream_task = {
            let runtime_config = Arc::clone(&self.runtime_config);
            let output_queue = Arc::clone(&output_queue);
            let output_queue_depth = Arc::clone(&output_queue_depth);
            let queue_overflow_policy = self.queue_overflow_policy;
            tokio::spawn(async move {
                yellowstone_stream_loop(
                    runtime_config,
                    output_queue,
                    output_queue_depth,
                    queue_overflow_policy,
                )
                .await;
            })
        };

        Ok(YellowstonePipeline {
            output_queue,
            output_queue_depth,
            stream_task,
        })
    }

    async fn recv_from_pipeline(&mut self, wait: Option<Duration>) -> YellowstoneRecvOutcome {
        let Some(pipeline) = self.pipeline.as_ref() else {
            return YellowstoneRecvOutcome::QueueClosed;
        };

        if let Some(wait) = wait {
            match time::timeout(wait, pipeline.output_queue.pop()).await {
                Ok(Some(item)) => {
                    decrement_atomic_usize(&pipeline.output_queue_depth);
                    YellowstoneRecvOutcome::Item(item)
                }
                Ok(None) => YellowstoneRecvOutcome::QueueClosed,
                Err(_) => YellowstoneRecvOutcome::TimedOut,
            }
        } else {
            match pipeline.output_queue.pop().await {
                Some(item) => {
                    decrement_atomic_usize(&pipeline.output_queue_depth);
                    YellowstoneRecvOutcome::Item(item)
                }
                None => YellowstoneRecvOutcome::QueueClosed,
            }
        }
    }

    fn push_reorder_entry(&mut self, fetched: FetchedObservation) {
        self.reorder.push(fetched);
        self.runtime_config
            .telemetry
            .note_reorder_buffer_size(self.reorder.len());
    }

    fn pop_ready_observation(&mut self) -> Option<RawSwapObservation> {
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
        let queue_depth = self
            .pipeline
            .as_ref()
            .map(|pipeline| pipeline.output_queue_depth.load(Ordering::Relaxed))
            .unwrap_or(0);
        self.runtime_config.telemetry.maybe_report(
            self.telemetry_report_seconds,
            queue_depth,
            0,
            self.reorder.len(),
        );
    }

    pub(super) fn runtime_snapshot(&self) -> IngestionRuntimeSnapshot {
        self.runtime_config.telemetry.snapshot()
    }
}
