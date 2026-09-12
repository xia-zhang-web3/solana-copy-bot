//! Test-only regression seam: a bounded ready-event queue at the real next_swap boundary.
//! No alternate dispatch loop or replacement for the production handler.
use anyhow::{Context, Result};
use copybot_core_types::SwapEvent;
use copybot_ingestion::{IngestionRuntimeSnapshot, IngestionService};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::{mpsc, Notify};
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
pub(crate) async fn acquire() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}
static ACTIVE: Mutex<Option<Arc<Hooks>>> = Mutex::new(None);
pub(crate) struct Hooks {
    start: Instant,
    pub events: Mutex<Vec<Value>>,
    receiver: Mutex<Option<mpsc::Receiver<SwapEvent>>>,
    shadow_gate: (Mutex<bool>, std::sync::Condvar),
    stopped: AtomicBool,
    stop: Notify,
}
pub(crate) struct Installed(pub Arc<Hooks>);
impl Installed {
    pub fn new() -> (Self, mpsc::Sender<SwapEvent>) {
        let (tx, rx) = mpsc::channel(2);
        let hooks = Arc::new(Hooks {
            start: Instant::now(),
            events: Mutex::new(Vec::new()),
            receiver: Mutex::new(Some(rx)),
            shadow_gate: (Mutex::new(false), std::sync::Condvar::new()),
            stopped: AtomicBool::new(false),
            stop: Notify::new(),
        });
        assert!(ACTIVE.lock().unwrap().replace(hooks.clone()).is_none());
        (Self(hooks), tx)
    }
}
impl Drop for Installed {
    fn drop(&mut self) {
        self.0.hold_shadow(false);
        assert!(ACTIVE.lock().unwrap().take().is_some());
    }
}
impl Hooks {
    pub fn hold_shadow(&self, held: bool) {
        *self.shadow_gate.0.lock().unwrap() = held;
        self.shadow_gate.1.notify_all();
    }

    pub fn mark(&self, stage: &str, signature: &str) {
        if !signature.is_empty() && !signature.contains("b70-") {
            return;
        }
        let mut events = self.events.lock().unwrap();
        let seq = events.len();
        assert!(seq < 500, "bounded audit event budget");
        events.push(json!({"seq":seq,"stage":stage,"signature":signature,"monotonic_us":self.start.elapsed().as_micros() as u64,"utc":chrono::Utc::now().to_rfc3339()}));
    }
    pub fn count(&self, stage: &str, signature: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v["stage"] == stage && v["signature"] == signature)
            .count()
    }
    pub async fn wait(&self, stage: &str, signature: &str, count: usize) -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(4), async {
            while self.count(stage, signature) < count {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("missing audit stage {stage} for {signature}"))
    }
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.stop.notify_one();
    }
}
pub(crate) fn mark(stage: &str, signature: &str) {
    if let Some(h) = ACTIVE.lock().unwrap().as_ref() {
        h.mark(stage, signature);
    }
}
// Observe the real consumer's retained task at an existing ready loop branch.
// This seam never changes scheduling, completion, persistence or ACK behavior.
pub(crate) fn execution_tick(consumer: Option<&crate::association_consumer::AssociationConsumer>) {
    mark("execution_tick", "");
    if let Some(pending) = consumer.and_then(|c| c.pending.as_ref()) {
        if !pending.is_finished() {
            mark(&format!("association_pending:{:?}", pending.id()), "");
        }
    }
}
pub(crate) fn before_shadow(signature: &str) {
    if !signature.starts_with("b70-") {
        return;
    }
    let hooks = ACTIVE.lock().unwrap().clone();
    if let Some(h) = hooks {
        h.mark("shadow_worker_entered", signature);
        let (held, timeout) = h
            .shadow_gate
            .1
            .wait_timeout_while(
                h.shadow_gate.0.lock().unwrap(),
                std::time::Duration::from_secs(5),
                |held| *held,
            )
            .unwrap();
        assert!(
            !timeout.timed_out() || !*held,
            "checked shadow gate release missing"
        );
    }
}
pub(crate) struct Consumer<'a> {
    inner: &'a mut IngestionService,
    receiver: Option<mpsc::Receiver<SwapEvent>>,
}
impl std::ops::Deref for Consumer<'_> {
    type Target = IngestionService;
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}
pub(crate) fn consumer(inner: &mut IngestionService) -> Consumer<'_> {
    let receiver = ACTIVE
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|h| h.receiver.lock().unwrap().take());
    Consumer { inner, receiver }
}
impl Consumer<'_> {
    pub async fn next_swap(&mut self) -> Result<Option<SwapEvent>> {
        if let Some(rx) = &mut self.receiver {
            let swap = rx
                .recv()
                .await
                .context("audit source unexpectedly closed")?;
            mark("next_swap_consumed", &swap.signature);
            Ok(Some(swap))
        } else {
            self.inner.next_swap().await
        }
    }
    pub fn runtime_snapshot(&self) -> Option<IngestionRuntimeSnapshot> {
        self.inner.runtime_snapshot()
    }
}
pub(crate) async fn stop() -> std::io::Result<()> {
    let h = ACTIVE.lock().unwrap().clone();
    if let Some(h) = h {
        while !h.stopped.load(Ordering::SeqCst) {
            h.stop.notified().await;
        }
        Ok(())
    } else {
        tokio::signal::ctrl_c().await
    }
}
pub(crate) async fn checked_shutdown(
    s: &mut crate::shadow_scheduler::ShadowScheduler,
) -> Result<()> {
    // Only shutdown: await rather than drop any still-owned test tasks.
    while let Some(out) = s.shadow_workers.join_next().await {
        out?.outcome?;
    }
    if let Some(handle) = s.shadow_snapshot_handle.take() {
        handle.await??;
    }
    anyhow::ensure!(
        s.pending_shadow_task_count == 0,
        "unexpected pending shadow tasks at audit shutdown"
    );
    mark("checked_shutdown", "");
    Ok(())
}
