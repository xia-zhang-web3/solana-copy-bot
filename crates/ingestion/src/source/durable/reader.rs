//! One bounded reader task per connection; processing never owns tonic polling.
use super::DurableIngressTelemetry;
use crate::source::durable::reader_diagnostics;
#[path = "reader_terminal.rs"]
mod terminal_observer;
use crate::source::durable::capture_scope::{CaptureScope, Pending};
use anyhow::{ensure, Result};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use prost::Message;
use std::{sync::Arc, time::Instant};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use yellowstone_grpc_proto::prelude::*;

pub(super) enum End {
    Status(tonic::Status),
    Ping(anyhow::Error),
    Eof,
    Budget(&'static str),
}
pub(super) struct Captured {
    value: CapturedValue,
    received: Instant,
    charge: usize,
    telemetry: Arc<DurableIngressTelemetry>,
    queued: bool,
    _permit: OwnedSemaphorePermit,
    _count: OwnedSemaphorePermit,
    _pending: Vec<Pending>,
}
enum CapturedValue {
    EncodedBlock(Vec<u8>),
    Exact(SubscribeUpdate),
}
impl Captured {
    pub fn is_block(&self) -> bool {
        match &self.value {
            CapturedValue::EncodedBlock(_) => true,
            CapturedValue::Exact(update) => matches!(
                update.update_oneof.as_ref(),
                Some(subscribe_update::UpdateOneof::Block(_))
            ),
        }
    }
    pub fn update(&mut self) -> Result<&SubscribeUpdate> {
        if let CapturedValue::EncodedBlock(bytes) = &self.value {
            self.value = CapturedValue::Exact(SubscribeUpdate::decode(bytes.as_slice())?);
        }
        let CapturedValue::Exact(update) = &self.value else {
            unreachable!("decoded captured block")
        };
        Ok(update)
    }
    pub fn dequeue(&mut self) {
        if self.queued {
            self.telemetry
                .processing
                .input_dequeued(self.charge, self.received.elapsed());
            self.queued = false;
        }
    }
}
impl Drop for Captured {
    fn drop(&mut self) {
        if self.queued {
            self.telemetry.processing.input_released(self.charge);
        }
    }
}
pub(in crate::source) struct Reader {
    rx: mpsc::Receiver<Captured>,
    task: Option<JoinHandle<End>>,
    scope: Arc<CaptureScope>,
    diagnostics: Arc<reader_diagnostics::ReaderDiagnostics>,
}
struct Interrupted(Arc<CaptureScope>);
impl Drop for Interrupted {
    fn drop(&mut self) {
        self.0.interrupted();
    }
}
#[cfg(test)]
#[path = "../../source_tests/reader_measurement_input.rs"]
mod measurement_input;
#[cfg(test)]
#[path = "../../source_tests/reader_measurement_impl.rs"]
mod measurement_impl;
impl Reader {

    pub(super) fn start<S, K>(
        stream: S,
        sink: K,
        count: usize,
        bytes: usize,
        input: usize,
        telemetry: Arc<DurableIngressTelemetry>,
        scope: Arc<CaptureScope>,
    ) -> Result<Self>
    where
        S: Stream<Item = std::result::Result<SubscribeUpdate, tonic::Status>>
            + Unpin
            + Send
            + 'static,
        K: Sink<SubscribeRequest> + Unpin + Send + 'static,
        K::Error: std::error::Error + Send + Sync + 'static,
    {
        Self::start_diagnosed(
            stream,
            sink,
            count,
            bytes,
            input,
            telemetry,
            scope,
            Vec::new(),
        )
    }
    pub(super) fn start_diagnosed<S, K>(
        mut stream: S,
        mut sink: K,
        count: usize,
        bytes: usize,
        input: usize,
        telemetry: Arc<DurableIngressTelemetry>,
        scope: Arc<CaptureScope>,
        secrets: Vec<String>,
    ) -> Result<Self>
    where
        S: Stream<Item = std::result::Result<SubscribeUpdate, tonic::Status>>
            + Unpin
            + Send
            + 'static,
        K: Sink<SubscribeRequest> + Unpin + Send + 'static,
        K::Error: std::error::Error + Send + Sync + 'static,
    {
        ensure!(
            count > 0 && bytes > 0 && bytes <= u32::MAX as usize,
            "http_capture_queue_bounds"
        );
        let (tx, rx) = mpsc::channel(count);
        let permits = Arc::new(Semaphore::new(bytes));
        let counts = Arc::new(Semaphore::new(count));
        let task_scope = scope.clone();
        let reader_scope = scope.clone();
        let diagnostics = telemetry.reader_diagnostics.clone();
        let mut observer = terminal_observer::TerminalObserver::new(
            diagnostics.clone(),
            scope.diagnostic_identity(),
            secrets,
        );
        let task = tokio::spawn(async move {
            let _interrupted = Interrupted(task_scope);
            let end = async {
            while let Some(value) = stream.next().await {
                let update = match value {
                    Ok(v) => v,
                    Err(e) => return End::Status(e),
                };
                #[cfg(test)]
                let typed_available=crate::source::durable::continuity_telemetry::diagnostic_time();
                match update.update_oneof.as_ref() {
                    Some(subscribe_update::UpdateOneof::Ping(_)) => {
                        if let Err(e) = sink
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await
                        {
                            return End::Ping(anyhow::Error::new(e));
                        }
                        continue;
                    }
                    Some(subscribe_update::UpdateOneof::Transaction(t)) => {
                        telemetry.received_transaction(t.slot)
                    }
                    Some(subscribe_update::UpdateOneof::Block(b)) => {
                        telemetry.received_block(b.slot);
                        scope.frontier(b.slot);
                    }
                    _ => continue,
                }
                if let Some(subscribe_update::UpdateOneof::Transaction(t)) =
                    update.update_oneof.as_ref()
                {
                    if t.transaction.as_ref().is_some_and(|info| !scope.keep(info)) {
                        telemetry.processing.filtered_foreign_transaction();
                        continue;
                    }
                }
                let size = update.encoded_len();
                if size > input {
                    return End::Budget("TransportInputTooLarge");
                }
                let Some(charge) = size.checked_add(512) else {
                    return End::Budget("LiveCaptureBytes");
                };
                let Ok(charge_u32) = u32::try_from(charge) else {
                    return End::Budget("LiveCaptureBytes");
                };
                let Ok(permit) = permits.clone().try_acquire_many_owned(charge_u32) else {
                    return End::Budget("LiveCaptureBytes");
                };
                let Ok(count_permit) = counts.clone().try_acquire_owned() else {
                    return End::Budget("LiveCaptureCount");
                };
                telemetry.processing.input_enqueued(charge);
                let pending = match update.update_oneof.as_ref() {
                    Some(subscribe_update::UpdateOneof::Transaction(t)) => t
                        .transaction
                        .as_ref()
                        .map(|info| vec![scope.track(&info.signature)])
                        .unwrap_or_default(),
                    Some(subscribe_update::UpdateOneof::Block(block)) => block
                        .transactions
                        .iter()
                        .filter(|info| scope.wallet(info))
                        .map(|info| scope.track(&info.signature))
                        .collect(),
                    _ => Vec::new(),
                };
                let value = match update.update_oneof.as_ref() {
                    Some(subscribe_update::UpdateOneof::Block(block))
                        if !crate::source::yellowstone_block_association::needs_exact_float_storage(block) =>
                    {
                        let encoded=update.encode_to_vec();
                        #[cfg(test)]
                        measurement_input::observe(&encoded,block.slot,typed_available);
                        CapturedValue::EncodedBlock(encoded)
                    }
                    _ => CapturedValue::Exact(update),
                };
                let captured = Captured {
                    value,
                    received: Instant::now(),
                    charge,
                    telemetry: telemetry.clone(),
                    queued: true,
                    _permit: permit,
                    _count: count_permit,
                    _pending: pending,
                };
                if tx.try_send(captured).is_err() {
                    return End::Budget("LiveCaptureCount");
                }
            }
            End::Eof
            }.await;
            observer.finish(&end);
            end
        });
        Ok(Self {
            rx,
            task: Some(task),
            scope: reader_scope,
            diagnostics,
        })
    }
    pub(super) fn clear_verified(&self) {
        self.scope.clear_verified();
    }
    pub(super) async fn next(&mut self) -> Option<Captured> {
        self.rx.recv().await
    }
    pub(super) async fn end(&mut self) -> Result<End> {
        let result = self.task.take().expect("reader task").await;
        if let Err(error) = &result {
            self.diagnostics.record(reader_diagnostics::event(
                self.scope.diagnostic_identity(),
                if error.is_cancelled() {
                    "join_cancelled"
                } else {
                    "join_panicked"
                },
                "UNAVAILABLE_NO_END_RETURNED",
            ));
        }
        Ok(result?)
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        // Abort is scheduled; financial continuity must close synchronously.
        self.scope.reader_dropped();
        if let Some(task) = self.task.take() {
            let mut event = reader_diagnostics::event(
                self.scope.diagnostic_identity(),
                "owner_abort_requested",
                "NOT_TERMINAL",
            );
            event.task_finished_at_abort_request = Some(task.is_finished());
            self.diagnostics.record(event);
            task.abort();
        }
    }
}

#[cfg(test)]
#[path = "../../source_tests/reader_lifetime_fixture.rs"]
mod lifetime_fixture;

#[cfg(test)]
#[path = "../../source_tests/recovery_raw_memory_tests.rs"]
mod raw_memory_tests;

#[cfg(test)]
#[path = "../../source_tests/ingress42_correlation_tests.rs"]
mod ingress42_correlation_tests;

#[cfg(test)]
#[path = "../../source_tests/reader_diagnostic_tests.rs"]
mod reader_diagnostic_tests;
