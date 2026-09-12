use anyhow::{Context, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[derive(Clone, Default)]
pub(super) struct Tasks {
    listener: Arc<AtomicUsize>,
    handlers: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

pub(super) struct Active {
    count: Arc<AtomicUsize>,
    completed: Option<Arc<AtomicUsize>>,
    changed: Arc<Notify>,
}
impl Tasks {
    pub fn listener(&self) -> Active {
        self.enter(self.listener.clone(), None)
    }
    pub fn handler(&self) -> Active {
        self.enter(self.handlers.clone(), Some(self.completed.clone()))
    }
    fn enter(&self, count: Arc<AtomicUsize>, completed: Option<Arc<AtomicUsize>>) -> Active {
        count.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        Active {
            count,
            completed,
            changed: self.changed.clone(),
        }
    }
    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.listener.load(Ordering::SeqCst),
            self.handlers.load(Ordering::SeqCst),
            self.completed.load(Ordering::SeqCst),
        )
    }
    pub async fn wait_accepted(&self, n: usize) -> Result<()> {
        self.wait(|(_, active, completed)| active + completed >= n)
            .await
    }
    pub async fn wait_handlers(&self, n: usize) -> Result<()> {
        self.wait(|(_, active, _)| active == n).await
    }
    pub async fn quiescent(&self) -> Result<()> {
        self.wait(|(listener, active, _)| listener == 0 && active == 0)
            .await
    }
    async fn wait(&self, ready: impl Fn((usize, usize, usize)) -> bool) -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let changed = self.changed.notified();
                if ready(self.counts()) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .context("receipt fixture task ownership wait")
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        if let Some(completed) = &self.completed {
            completed.fetch_add(1, Ordering::SeqCst);
        }
        self.count.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_one();
    }
}
