//! One bounded reader task per connection; processing never owns tonic polling.
use super::DurableIngressTelemetry;
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
    pub update: SubscribeUpdate,
    received: Instant,
    charge: usize,
    telemetry: Arc<DurableIngressTelemetry>,
    queued: bool,
    _permit: OwnedSemaphorePermit,
    _count: OwnedSemaphorePermit,
    _pending: Vec<Pending>,
}
impl Captured {
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
}
struct Interrupted(Arc<CaptureScope>);
impl Drop for Interrupted {
    fn drop(&mut self) {
        self.0.interrupted();
    }
}
impl Reader {
    pub(super) fn start<S, K>(
        mut stream: S,
        mut sink: K,
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
        ensure!(
            count > 0 && bytes > 0 && bytes <= u32::MAX as usize,
            "http_capture_queue_bounds"
        );
        let (tx, rx) = mpsc::channel(count);
        let permits = Arc::new(Semaphore::new(bytes));
        let counts = Arc::new(Semaphore::new(count));
        let task_scope = scope.clone();
        let reader_scope = scope.clone();
        let task = tokio::spawn(async move {
            let _interrupted = Interrupted(task_scope);
            while let Some(value) = stream.next().await {
                let update = match value {
                    Ok(v) => v,
                    Err(e) => return End::Status(e),
                };
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
                        telemetry.received_block(b.slot)
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
                let captured = Captured {
                    update,
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
        });
        Ok(Self {
            rx,
            task: Some(task),
            scope: reader_scope,
        })
    }
    pub(super) fn clear_verified(&self) {
        self.scope.clear_verified();
    }
    pub(super) async fn next(&mut self) -> Option<Captured> {
        self.rx.recv().await
    }
    pub(super) async fn end(&mut self) -> Result<End> {
        Ok(self.task.take().expect("reader task").await?)
    }
}
impl Drop for Reader {
    fn drop(&mut self) {
        // Abort is scheduled; financial continuity must close synchronously.
        self.scope.interrupted();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
#[path = "../../source_tests/reader_lifetime_fixture.rs"]
mod lifetime_fixture;
