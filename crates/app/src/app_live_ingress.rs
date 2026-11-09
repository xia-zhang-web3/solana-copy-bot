//! One app dispatch path for legacy and protected-mode ordinary stream swaps.
use anyhow::Result;
use copybot_core_types::SwapEvent;
use tokio::sync::mpsc;

pub(crate) struct LiveEnvelope {
    pub swap: SwapEvent,
    pub _permit: tokio::sync::OwnedSemaphorePermit,
}
pub(crate) fn ordinary_enabled(association_mode: bool, protected_mode: bool) -> bool {
    !association_mode || protected_mode
}
pub(crate) async fn next_swap(
    legacy: impl std::future::Future<Output = Result<Option<SwapEvent>>>,
    protected: Option<&mut mpsc::Receiver<LiveEnvelope>>,
) -> Result<Option<SwapEvent>> {
    match protected {
        Some(receiver) => match receiver.recv().await {
            Some(envelope) => Ok(Some(envelope.swap)),
            // Actor termination is a fatal app-monitor result. It must not be
            // downgraded into the ordinary handler's outage/backoff branch.
            None => std::future::pending().await,
        },
        None => legacy.await,
    }
}
