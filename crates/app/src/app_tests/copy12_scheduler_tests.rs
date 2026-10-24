//! Scheduling hints cannot bypass continuity and survive outer-select cancellation.
use super::Scheduler;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Notify;

fn scheduler(wake: Option<Arc<Notify>>, hold: Arc<AtomicBool>) -> Scheduler {
    Scheduler {
        interval: tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(15),
            Duration::from_secs(15),
        ),
        wake,
        hold: Some(hold),
        sell_pending: false,
    }
}

#[tokio::test]
async fn copy12_periodic_misses_short_window_but_commit_hint_wakes_once() {
    let hold = Arc::new(AtomicBool::new(false));
    let mut old = scheduler(None, hold.clone());
    assert!(tokio::time::timeout(Duration::from_millis(80), old.ready())
        .await
        .is_err());
    let wake = Arc::new(Notify::new());
    let mut fixed = scheduler(Some(wake.clone()), hold);
    wake.notify_one();
    wake.notify_one(); // Hints are coalesced; not a queue of trade permissions.
    tokio::time::timeout(Duration::from_millis(80), fixed.ready())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(80), fixed.ready())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn copy12_hint_waits_for_hold_and_survives_poll_cancellation() {
    let wake = Arc::new(Notify::new());
    let hold = Arc::new(AtomicBool::new(true));
    let mut fixed = scheduler(Some(wake.clone()), hold.clone());
    wake.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(80), fixed.ready())
            .await
            .is_err()
    );
    assert!(
        fixed.sell_pending,
        "a cancelled app select must retain the committed hint"
    );
    hold.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_millis(80), fixed.ready())
        .await
        .unwrap();
    assert!(!fixed.sell_pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), fixed.ready())
            .await
            .is_err()
    );
}
