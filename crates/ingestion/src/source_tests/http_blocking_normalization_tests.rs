//! CPU-worker progress, charged cancellation/restart, and ordered refusal.
use super::super::ordered_pipeline::OrderedPipeline;
use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use tokio::sync::oneshot;

async fn until(check: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_transform_keeps_single_thread_runtime_heartbeat_alive_and_result_charged() {
    let worker = BlockingNormalization::default();
    let (owner, window) = worker.begin(1).await.unwrap();
    let raw = Charged::from_parts(7u64, window.acquire().await.unwrap());
    let (started, started_rx) = oneshot::channel();
    let (release, blocked) = mpsc::channel();
    let job = tokio::spawn(async move {
        owner
            .run(raw, move |raw| {
                started.send(()).unwrap();
                let _ = blocked.recv();
                Ok(raw + 1)
            })
            .await
            .unwrap()
    });
    started_rx.await.unwrap();
    let (heartbeat, beat) = oneshot::channel();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        heartbeat.send(()).unwrap();
    });
    tokio::time::timeout(Duration::from_secs(1), beat)
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), window.acquire())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let (transformed, owner) = job.await.unwrap();
    let (transformed, permit) = transformed.into_parts();
    assert_eq!(transformed.result.unwrap(), 8);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), window.acquire())
            .await
            .is_err()
    );
    drop(permit);
    drop(window.acquire().await.unwrap());
    drop(owner);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_transform_retains_charge_and_gates_replacement_pipeline_in_same_window() {
    let worker = BlockingNormalization::default();
    let (owner, window) = worker.begin(32).await.unwrap();
    let old_started = Arc::new(AtomicUsize::new(0));
    let old_count = old_started.clone();
    let mut old = OrderedPipeline::start_in_window((0..64).collect(), 10, window, move |slot| {
        let n = old_count.clone();
        async move {
            n.fetch_add(1, Ordering::SeqCst);
            Ok(slot)
        }
    })
    .unwrap();
    let input = old.next().await.unwrap().unwrap();
    // Represents residual charged producer work awaiting scheduled cancellation.
    let residual = old.next().await.unwrap().unwrap();
    let (started, started_rx) = oneshot::channel();
    let (release, blocked) = mpsc::channel();
    let emitted = Arc::new(AtomicUsize::new(0));
    let old_emit = emitted.clone();
    let job = tokio::spawn(async move {
        let result = owner
            .run(input, move |raw| {
                started.send(()).unwrap();
                let _ = blocked.recv();
                Ok(raw)
            })
            .await;
        old_emit.fetch_add(1, Ordering::SeqCst);
        result
    });
    started_rx.await.unwrap();
    until(|| old_started.load(Ordering::SeqCst) == 32).await;
    job.abort();
    assert!(job.await.err().unwrap().is_cancelled());
    drop(old);
    let new_started = Arc::new(AtomicUsize::new(0));
    let new_count = new_started.clone();
    let replacement = tokio::spawn(async move {
        let (owner, window) = worker.begin(32).await.unwrap();
        let pipeline =
            OrderedPipeline::start_in_window((0..64).collect(), 10, window, move |slot| {
                let n = new_count.clone();
                async move {
                    n.fetch_add(1, Ordering::SeqCst);
                    Ok(slot)
                }
            })
            .unwrap();
        (owner, pipeline)
    });
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(new_started.load(Ordering::SeqCst), 0);
    assert_eq!(emitted.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    let (owner, replacement) = replacement.await.unwrap();
    until(|| new_started.load(Ordering::SeqCst) == 31).await;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    // Old residual1 + new31 is the SAME whole window, never a hidden33.
    assert_eq!(new_started.load(Ordering::SeqCst), 31);
    assert_eq!(emitted.load(Ordering::SeqCst), 0);
    drop(residual);
    until(|| new_started.load(Ordering::SeqCst) == 32).await;
    drop(replacement);
    drop(owner);
}

#[test]
fn cancelled_queued_transform_never_executes_and_releases_both_leases() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (release, blocked) = mpsc::channel();
        let (started, started_rx) = oneshot::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = blocked.recv();
        });
        started_rx.await.unwrap();
        let worker = BlockingNormalization::default();
        let (owner, window) = worker.begin(1).await.unwrap();
        let input = Charged::from_parts(0u64, window.acquire().await.unwrap());
        let executed = Arc::new(AtomicBool::new(false));
        let observed = executed.clone();
        let (submitted, submitted_rx) = oneshot::channel();
        let queued = tokio::spawn(async move {
            submitted.send(()).unwrap();
            owner
                .run(input, move |raw| {
                    observed.store(true, Ordering::SeqCst);
                    Ok(raw)
                })
                .await
        });
        submitted_rx.await.unwrap();
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        queued.abort();
        assert!(queued.await.err().unwrap().is_cancelled());
        release.send(()).unwrap();
        occupied.await.unwrap();
        drop(
            tokio::time::timeout(Duration::from_secs(1), window.acquire())
                .await
                .unwrap()
                .unwrap(),
        );
        let (owner, _) = tokio::time::timeout(Duration::from_secs(1), worker.begin(1))
            .await
            .unwrap()
            .unwrap();
        assert!(!executed.load(Ordering::SeqCst));
        drop(owner);
    });
}

#[derive(Default)]
struct ActiveCounts {
    active: AtomicUsize,
    peak: AtomicUsize,
    jobs: AtomicUsize,
}
struct ActiveJob(Arc<ActiveCounts>);
impl ActiveJob {
    fn new(counts: Arc<ActiveCounts>) -> Self {
        let n = counts.active.fetch_add(1, Ordering::SeqCst) + 1;
        counts.peak.fetch_max(n, Ordering::SeqCst);
        counts.jobs.fetch_add(1, Ordering::SeqCst);
        Self(counts)
    }
}
impl Drop for ActiveJob {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_transform_preserves_order_refusal_and_width10_window32() {
    let worker = BlockingNormalization::default();
    let (mut owner, window) = worker.begin(32).await.unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let starts = started.clone();
    let mut pipeline =
        OrderedPipeline::start_in_window((0..40).collect(), 10, window, move |slot| {
            let n = starts.clone();
            async move {
                if slot % 2 == 0 {
                    tokio::task::yield_now().await;
                }
                n.fetch_add(1, Ordering::SeqCst);
                Ok(slot)
            }
        })
        .unwrap();
    let input = pipeline.next().await.unwrap().unwrap();
    let (release, blocked) = mpsc::channel();
    let (began, began_rx) = oneshot::channel();
    let jobs = Arc::new(ActiveCounts::default());
    let counts = jobs.clone();
    let first = tokio::spawn(async move {
        owner
            .run(input, move |slot| {
                let _active = ActiveJob::new(counts);
                began.send(()).unwrap();
                let _ = blocked.recv();
                Ok(slot)
            })
            .await
            .unwrap()
    });
    began_rx.await.unwrap();
    until(|| started.load(Ordering::SeqCst) == 32).await;
    assert_eq!(jobs.jobs.load(Ordering::SeqCst), 1);
    release.send(()).unwrap();
    let (first, next_owner) = first.await.unwrap();
    owner = next_owner;
    let (first, permit) = first.into_parts();
    let mut applied = vec![first.result.unwrap()];
    assert_eq!(applied, [0]);
    assert_eq!(started.load(Ordering::SeqCst), 32);
    drop(permit);
    while let Some(input) = pipeline.next().await.unwrap() {
        let counts = jobs.clone();
        let (transformed, next_owner) = owner
            .run(input, move |slot| {
                let _active = ActiveJob::new(counts);
                if slot == 5 {
                    anyhow::bail!("ordered_normalization_refusal");
                }
                Ok(slot)
            })
            .await
            .unwrap();
        owner = next_owner;
        let (transformed, permit) = transformed.into_parts();
        match transformed.result {
            Ok(slot) => applied.push(slot),
            Err(error) => {
                assert!(error.to_string().contains("ordered_normalization_refusal"));
                drop(permit);
                break;
            }
        }
        drop(permit);
    }
    drop(pipeline);
    drop(owner);
    assert_eq!(applied, [0, 1, 2, 3, 4]);
    assert_eq!(jobs.jobs.load(Ordering::SeqCst), 6);
    assert_eq!(jobs.peak.load(Ordering::SeqCst), 1);
    assert_eq!(jobs.active.load(Ordering::SeqCst), 0);
}
