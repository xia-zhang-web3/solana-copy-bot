//! Causal polling, ordering, ahead-window and cancellation checks without HTTP.
use super::OrderedPipeline;
use futures_util::{stream, StreamExt};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::watch;

#[derive(Default)]
struct Counts {
    started: AtomicUsize,
    completed: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    cancelled: AtomicUsize,
}
struct Active(Arc<Counts>, bool);
impl Active {
    fn start(counts: Arc<Counts>) -> Self {
        counts.started.fetch_add(1, Ordering::SeqCst);
        let active = counts.active.fetch_add(1, Ordering::SeqCst) + 1;
        counts.peak.fetch_max(active, Ordering::SeqCst);
        Self(counts, false)
    }
    fn complete(&mut self) {
        self.1 = true;
        self.0.completed.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        if !self.1 {
            self.0.cancelled.fetch_add(1, Ordering::SeqCst);
        }
    }
}
async fn until(check: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn fetch(
    slot: u64,
    counts: Arc<Counts>,
    mut gate: watch::Receiver<Vec<bool>>,
) -> anyhow::Result<u64> {
    let mut active = Active::start(counts);
    gate.wait_for(|allowed| allowed[slot as usize]).await?;
    active.complete();
    Ok(slot)
}

#[tokio::test]
async fn busy_ordered_apply_keeps_fetches_polled_without_expanding_width4_window4() {
    // Accepted lazy buffered control: futures cease progressing while the
    // consumer applies the first item and does not poll next().
    let old = Arc::new(Counts::default());
    let (old_gate, old_rx) = watch::channel(vec![true, false, false, false]);
    let old_counts = old.clone();
    let lazy = stream::iter(0..4)
        .map(move |slot| fetch(slot, old_counts.clone(), old_rx.clone()))
        .buffered(4);
    tokio::pin!(lazy);
    assert_eq!(lazy.next().await.unwrap().unwrap(), 0);
    old_gate.send(vec![true; 4]).unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(old.completed.load(Ordering::SeqCst), 1);

    let counts = Arc::new(Counts::default());
    let (gate, rx) = watch::channel(vec![true, false, false, true, false, false]);
    let observed = counts.clone();
    let mut pipeline = OrderedPipeline::start((0..6).collect(), 4, 4, move |slot| {
        fetch(slot, observed.clone(), rx.clone())
    })
    .unwrap();
    let (first, permit) = pipeline.next().await.unwrap().unwrap().into_parts();
    assert_eq!(first, 0);
    until(|| counts.completed.load(Ordering::SeqCst) == 2).await;
    assert_eq!(counts.started.load(Ordering::SeqCst), 4);
    gate.send_modify(|allowed| {
        allowed[1] = true;
        allowed[2] = true;
    });
    // The consumer still owns the first raw/application permit and never calls
    // next(); all three other fetches finish, while no fifth request can start.
    until(|| counts.completed.load(Ordering::SeqCst) == 4).await;
    assert_eq!(counts.started.load(Ordering::SeqCst), 4);
    drop(permit);
    until(|| counts.started.load(Ordering::SeqCst) == 5).await;
    gate.send(vec![true; 6]).unwrap();
    let mut applied = vec![first];
    while let Some(item) = pipeline.next().await.unwrap() {
        let (slot, permit) = item.into_parts();
        applied.push(slot);
        drop(permit);
    }
    assert_eq!(applied, (0..6).collect::<Vec<_>>());
    assert!(counts.peak.load(Ordering::SeqCst) <= 4);
    assert_eq!(counts.started.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn owner_stop_drops_all_inflight_fetches_and_starts_no_followup() {
    for (width, window) in [(4, 4), (10, 32)] {
        let counts = Arc::new(Counts::default());
        let observed = counts.clone();
        let pipeline = OrderedPipeline::<u64>::start((0..40).collect(), width, window, move |_| {
            let observed = observed.clone();
            async move {
                let _active = Active::start(observed);
                std::future::pending::<anyhow::Result<u64>>().await
            }
        })
        .unwrap();
        until(|| counts.started.load(Ordering::SeqCst) == width).await;
        drop(pipeline);
        until(|| counts.cancelled.load(Ordering::SeqCst) == width).await;
        assert_eq!(counts.active.load(Ordering::SeqCst), 0);
        assert_eq!(counts.started.load(Ordering::SeqCst), width);
    }
}

#[tokio::test]
async fn ordered_failure_cancels_other_fetches_and_cannot_apply_a_later_slot() {
    for (width, window) in [(4, 4), (10, 32)] {
        let counts = Arc::new(Counts::default());
        let observed = counts.clone();
        let (refuse, gate) = watch::channel(false);
        let mut pipeline = OrderedPipeline::start((0..40).collect(), width, window, move |slot| {
            let observed = observed.clone();
            let mut gate = gate.clone();
            async move {
                let mut active = Active::start(observed);
                match slot {
                    0 => {
                        active.complete();
                        Ok(slot)
                    }
                    1 => {
                        gate.wait_for(|refuse| *refuse).await?;
                        anyhow::bail!("fixture_terminal_refusal")
                    }
                    _ => std::future::pending::<anyhow::Result<u64>>().await,
                }
            }
        })
        .unwrap();
        let (slot, permit) = pipeline.next().await.unwrap().unwrap().into_parts();
        assert_eq!(slot, 0);
        until(|| counts.started.load(Ordering::SeqCst) >= width).await;
        refuse.send(true).unwrap();
        drop(permit);
        let error = pipeline.next().await.err().unwrap();
        assert!(error.to_string().contains("fixture_terminal_refusal"));
        assert!(pipeline.next().await.unwrap().is_none());
        assert_eq!(counts.active.load(Ordering::SeqCst), 0);
        assert_eq!(counts.completed.load(Ordering::SeqCst), 1);
        // The released first permit may start one last bounded request before the
        // terminal reply becomes observable; no followup survives that refusal.
        assert!(counts.started.load(Ordering::SeqCst) <= width + 1);
    }
}
