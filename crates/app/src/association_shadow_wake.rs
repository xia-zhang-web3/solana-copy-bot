//! Only the stale-close producer owns this notification. SQLite owns the work;
//! coalescing or losing this hint cannot erase a committed pair cursor on restart.
use copybot_storage_core::association_inbox::InboxLimits;
use std::sync::Arc;
use tokio::sync::Notify;
#[derive(Clone)]
pub(crate) struct ShadowWake {
    pub(crate) limits: InboxLimits,
    notify: Arc<Notify>,
}
impl ShadowWake {
    pub(crate) fn new(limits: InboxLimits) -> Self {
        Self {
            limits,
            notify: Arc::new(Notify::new()),
        }
    }
    pub(crate) fn notify(&self) {
        self.notify.notify_one();
    }
    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}
