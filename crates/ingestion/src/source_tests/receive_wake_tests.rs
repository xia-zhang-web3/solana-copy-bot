use super::*;
use crate::source::receive_stage_ledger::{Stage, StageLedger};
use crate::source::receive_wake_metrics::WakeSnapshot;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
struct Consumer {
    calls: AtomicU64,
    reenter: Option<Arc<Shared>>,
}
impl Wake for Consumer {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = &self.reenter {
            assert!(
                shared.active.try_lock().is_ok(),
                "external wake held state lock"
            );
            assert!(shared.metrics.snapshot().is_some());
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
fn clock() -> (Arc<AtomicU64>, Clock) {
    let time = Arc::new(AtomicU64::new(0));
    let read = time.clone();
    (
        time,
        Arc::new(move || Duration::from_millis(read.load(Ordering::SeqCst))),
    )
}
fn count(s: &WakeSnapshot, c: Count) -> u64 {
    s.counts[c as usize]
}
fn consumer() -> (Arc<Consumer>, Waker) {
    let state = Arc::new(Consumer::default());
    let waker = Waker::from(state.clone());
    (state, waker)
}

fn causal_case(wake_at: u64) -> (u64, u64, WakeSnapshot) {
    let ledger = StageLedger::default();
    let (time, now) = clock();
    let mut observer = WakeObserver::with_clock(ledger.wake.clone(), now.clone());
    let (consumer, outer) = consumer();
    let saved = Arc::new(Mutex::new(None));
    let mut calls = 0;
    let mut future = observer.observe(|cx| {
        ledger.measure_poll_with(
            || now(),
            || {
                calls += 1;
                time.fetch_add(1, Ordering::SeqCst);
                if calls == 1 {
                    *saved.lock().unwrap() = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(Ok::<_, &'static str>(73))
                }
            },
        )
    });
    let mut cx = Context::from_waker(&outer);
    assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Pending);
    let first = saved.lock().unwrap().clone().unwrap();
    time.store(wake_at, Ordering::SeqCst);
    first.wake();
    time.store(100, Ordering::SeqCst);
    assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(Ok(73)));
    drop(future);
    assert_eq!(calls, 2);
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 1);
    ledger.record(Stage::NextNetworkDecodeSchedulingWait, now());
    let old = ledger.snapshot().unwrap();
    assert_eq!((old.poll_pending, old.poll_ready), (1, 1));
    let wake = ledger.wake.snapshot().unwrap();
    assert_eq!(count(&wake, Count::ValidPairs), 1);
    assert_eq!(
        (
            count(&wake, Count::Polls),
            count(&wake, Count::RepollsReady)
        ),
        (2, 1)
    );
    assert_eq!(wake.active_pending, 0);
    let report: serde_json::Value = serde_json::from_str(
        &crate::source::receive_stage_measurement::snapshot_json(&ledger),
    )
    .unwrap();
    assert_eq!(report["consumer_wake"]["counts"]["valid_pairs"], 1);
    (
        old.stages[Stage::NextNetworkDecodeSchedulingWait as usize].sum,
        old.stages[Stage::NextPollExecution as usize].sum,
        wake,
    )
}

#[test]
fn equal_old_aggregates_distinguish_late_notification_from_delayed_consumer() {
    let (late_total, late_poll, late) = causal_case(90);
    let (early_total, early_poll, early) = causal_case(10);
    assert_eq!((late_total, late_poll), (early_total, early_poll));
    assert_eq!((late_total, late_poll), (101_000_000, 2_000_000));
    assert_eq!(
        (
            late.pending_to_first_wake_ns.sum,
            late.first_wake_to_repoll_ns.sum
        ),
        (89_000_000, 10_000_000)
    );
    assert_eq!(
        (
            early.pending_to_first_wake_ns.sum,
            early.first_wake_to_repoll_ns.sum
        ),
        (9_000_000, 90_000_000)
    );
    println!(
        "causal proof: old next=101ms/poll=2ms equal; late pre/post=89/10ms; early pre/post=9/90ms"
    );
}

#[test]
fn immutable_old_registration_delegates_old_consumer_and_cannot_stamp_new_epoch() {
    let metrics = WakeMetrics::default();
    let (time, now) = clock();
    let mut observer = WakeObserver::with_clock(metrics.clone(), now);
    let a = Arc::new(Consumer {
        reenter: Some(observer.shared.clone()),
        ..Consumer::default()
    });
    let wa = Waker::from(a.clone());
    let (b, wb) = consumer();
    let mut calls = 0;
    let mut future = observer.observe(|_| {
        calls += 1;
        if calls < 3 {
            Poll::Pending
        } else {
            Poll::Ready(Err::<(), _>("unchanged"))
        }
    });
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&wa))
        .is_pending());
    let old = Waker::from(future.observer.slots[0].clone());
    time.store(5, Ordering::SeqCst);
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&wb))
        .is_pending());
    let current = Waker::from(future.observer.slots[1].clone());
    old.wake_by_ref();
    assert_eq!(
        (
            a.calls.load(Ordering::SeqCst),
            b.calls.load(Ordering::SeqCst)
        ),
        (1, 0)
    );
    time.store(10, Ordering::SeqCst);
    current.wake_by_ref();
    time.store(20, Ordering::SeqCst);
    current.clone().wake();
    time.store(30, Ordering::SeqCst);
    assert_eq!(
        Pin::new(&mut future).poll(&mut Context::from_waker(&wb)),
        Poll::Ready(Err("unchanged"))
    );
    drop(future);
    assert_eq!(calls, 3);
    let s = metrics.snapshot().unwrap();
    assert_eq!(b.calls.load(Ordering::SeqCst), 2);
    assert_eq!(count(&s, Count::SupersededNotifications), 1);
    assert_eq!(count(&s, Count::RepollsWithoutWake), 1);
    assert_eq!(count(&s, Count::RepeatedNotifications), 1);
    assert_eq!(
        (
            s.pending_to_first_wake_ns.sum,
            s.first_wake_to_repoll_ns.sum
        ),
        (5_000_000, 20_000_000)
    );
}

#[test]
fn wake_inside_poll_and_at_pending_boundary_are_not_fabricated_zero_latency() {
    for boundary in [false, true] {
        let metrics = WakeMetrics::default();
        let time = Arc::new(AtomicU64::new(0));
        let armed = Arc::new(AtomicBool::new(false));
        let saved: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let now: Clock = {
            let (time, armed, saved) = (time.clone(), armed.clone(), saved.clone());
            Arc::new(move || {
                if armed.swap(false, Ordering::SeqCst) {
                    let wake = saved.lock().unwrap().clone().unwrap();
                    wake.wake_by_ref();
                }
                Duration::from_millis(time.load(Ordering::SeqCst))
            })
        };
        let mut observer = WakeObserver::with_clock(metrics.clone(), now);
        let (consumer, outer) = consumer();
        let mut calls = 0;
        let mut future = observer.observe(|cx| {
            calls += 1;
            if calls == 1 {
                *saved.lock().unwrap() = Some(cx.waker().clone());
                if boundary {
                    armed.store(true, Ordering::SeqCst);
                } else {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            } else {
                cx.waker().wake_by_ref();
                Poll::Ready(8)
            }
        });
        assert!(Pin::new(&mut future)
            .poll(&mut Context::from_waker(&outer))
            .is_pending());
        time.store(10, Ordering::SeqCst);
        assert_eq!(
            Pin::new(&mut future).poll(&mut Context::from_waker(&outer)),
            Poll::Ready(8)
        );
        drop(future);
        let s = metrics.snapshot().unwrap();
        assert_eq!(consumer.calls.load(Ordering::SeqCst), 2);
        assert_eq!(count(&s, Count::PrePendingRepolls), 1);
        assert_eq!(count(&s, Count::ReadyPollNotified), 1);
        assert_eq!(s.pre_pending_wake_to_repoll_ns.sum, 10_000_000);
        assert_eq!(s.pending_to_first_wake_ns.count, 0);
        assert_eq!(s.first_wake_to_repoll_ns.count, 0);
    }
}

#[test]
fn repeated_spurious_notification_then_repoll_without_wake_remain_distinct() {
    let metrics = WakeMetrics::default();
    let (time, now) = clock();
    let mut observer = WakeObserver::with_clock(metrics.clone(), now);
    let (_, outer) = consumer();
    let mut calls = 0;
    let mut future = observer.observe(|_| {
        calls += 1;
        if calls < 3 {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    });
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&outer))
        .is_pending());
    let wake = Waker::from(future.observer.slots[0].clone());
    time.store(10, Ordering::SeqCst);
    wake.wake_by_ref();
    time.store(20, Ordering::SeqCst);
    wake.wake_by_ref();
    time.store(30, Ordering::SeqCst);
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&outer))
        .is_pending());
    time.store(40, Ordering::SeqCst);
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&outer))
        .is_ready());
    drop(future);
    let s = metrics.snapshot().unwrap();
    assert_eq!(
        (
            count(&s, Count::RepollsPending),
            count(&s, Count::RepollsReady)
        ),
        (1, 1)
    );
    assert_eq!(count(&s, Count::RepollsWithoutWake), 1);
    assert_eq!(
        (
            s.pending_to_first_wake_ns.sum,
            s.first_wake_to_repoll_ns.sum
        ),
        (10_000_000, 20_000_000)
    );
}

#[test]
fn cancelled_and_late_notifications_never_complete_or_cross_waits() {
    let metrics = WakeMetrics::default();
    let (_, now) = clock();
    let mut observer = WakeObserver::with_clock(metrics.clone(), now);
    let (consumer, outer) = consumer();
    let mut first = observer.observe(|_| Poll::<()>::Pending);
    assert!(Pin::new(&mut first)
        .poll(&mut Context::from_waker(&outer))
        .is_pending());
    let late = Waker::from(first.observer.slots[0].clone());
    drop(first);
    late.wake_by_ref();
    let mut next = observer.observe(|_| Poll::<()>::Pending);
    assert!(Pin::new(&mut next)
        .poll(&mut Context::from_waker(&outer))
        .is_pending());
    late.wake_by_ref();
    drop(next);
    let s = metrics.snapshot().unwrap();
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 2);
    assert_eq!(count(&s, Count::CancelledPending), 2);
    assert_eq!(
        (
            count(&s, Count::ClosedNotifications),
            count(&s, Count::SupersededNotifications)
        ),
        (1, 1)
    );
    assert_eq!((s.active_pending, count(&s, Count::ValidPairs)), (0, 0));
}

#[test]
fn fixed_registration_pool_falls_back_without_allocation_or_changed_poll_result() {
    let metrics = WakeMetrics::default();
    let (_, now) = clock();
    let mut observer = WakeObserver::with_clock(metrics.clone(), now);
    let addresses = observer.slots.each_ref().map(Arc::as_ptr);
    let (_, outer) = consumer();
    let mut held = Vec::new();
    let mut calls = 0;
    let mut future = observer.observe(|cx| {
        calls += 1;
        if calls <= SLOT_COUNT {
            held.push(cx.waker().clone());
            Poll::Pending
        } else {
            assert!(cx.waker().will_wake(&outer));
            Poll::Ready(99)
        }
    });
    for _ in 0..SLOT_COUNT {
        assert!(Pin::new(&mut future)
            .poll(&mut Context::from_waker(&outer))
            .is_pending());
    }
    assert_eq!(
        Pin::new(&mut future).poll(&mut Context::from_waker(&outer)),
        Poll::Ready(99)
    );
    drop(future);
    assert_eq!(calls, SLOT_COUNT + 1);
    for wake in &held {
        for _ in 0..100 {
            wake.wake_by_ref();
        }
    }
    assert_eq!(observer.slots.each_ref().map(Arc::as_ptr), addresses);
    assert_eq!(
        count(&metrics.snapshot().unwrap(), Count::UnobservedPolls),
        1
    );
    drop(held);
    let mut reused = observer.observe(|_| Poll::Ready(7));
    assert_eq!(
        Pin::new(&mut reused).poll(&mut Context::from_waker(&outer)),
        Poll::Ready(7)
    );
    drop(reused);
    assert_eq!(
        count(&metrics.snapshot().unwrap(), Count::UnobservedPolls),
        1
    );
}

#[test]
fn concurrent_notifications_keep_first_once_and_delegate_every_wake() {
    let metrics = WakeMetrics::default();
    let (time, now) = clock();
    let mut observer = WakeObserver::with_clock(metrics.clone(), now);
    let (consumer, outer) = consumer();
    let mut calls = 0;
    let mut future = observer.observe(|_| {
        calls += 1;
        if calls == 1 {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    });
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&outer))
        .is_pending());
    let wake = Waker::from(future.observer.slots[0].clone());
    time.store(50, Ordering::SeqCst);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let wake = wake.clone();
            scope.spawn(move || wake.wake());
        }
    });
    time.store(100, Ordering::SeqCst);
    assert!(Pin::new(&mut future)
        .poll(&mut Context::from_waker(&outer))
        .is_ready());
    drop(future);
    let s = metrics.snapshot().unwrap();
    assert_eq!(
        (
            count(&s, Count::FirstNotifications),
            count(&s, Count::RepeatedNotifications)
        ),
        (1, 7)
    );
    assert_eq!(consumer.calls.load(Ordering::SeqCst), 8);
    assert_eq!(
        (
            s.pending_to_first_wake_ns.sum,
            s.first_wake_to_repoll_ns.sum
        ),
        (50_000_000, 50_000_000)
    );
}

#[tokio::test]
async fn actual_timeout_drops_wrapper_and_cancels_pending_measurement() {
    let metrics = WakeMetrics::default();
    let mut observer = WakeObserver::new(metrics.clone());
    let result =
        tokio::time::timeout(Duration::ZERO, observer.observe(|_| Poll::<()>::Pending)).await;
    assert!(result.is_err());
    let s = metrics.snapshot().unwrap();
    assert_eq!(count(&s, Count::CancelledFutures), 1);
    assert_eq!(count(&s, Count::CancelledPending), 1);
    assert_eq!((s.active_pending, count(&s, Count::ValidPairs)), (0, 0));
}
