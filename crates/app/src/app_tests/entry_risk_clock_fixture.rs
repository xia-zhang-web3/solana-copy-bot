use chrono::{DateTime, Utc};
use std::{cell::RefCell, collections::VecDeque, future::Future};

struct Script {
    times: VecDeque<DateTime<Utc>>,
    last: Option<DateTime<Utc>>,
}
tokio::task_local! { static CLOCK: RefCell<Script>; }

pub(crate) fn sample() -> Option<(DateTime<Utc>, Option<DateTime<Utc>>)> {
    CLOCK
        .try_with(|clock| {
            let mut clock = clock.borrow_mut();
            let current = *clock.times.front().expect("nonempty clock script");
            if clock.times.len() > 1 {
                clock.times.pop_front();
            }
            let previous = clock.last;
            clock.last = Some(previous.map_or(current, |old| old.max(current)));
            (current, previous)
        })
        .ok()
}

pub(super) fn at<F: Future>(time: DateTime<Utc>, future: F) -> impl Future<Output = F::Output> {
    sequence([time], future)
}

pub(super) fn advance_to(time: DateTime<Utc>) {
    CLOCK.with(|clock| clock.borrow_mut().times = VecDeque::from([time]));
}
pub(super) fn sequence<F: Future>(
    times: impl IntoIterator<Item = DateTime<Utc>>,
    future: F,
) -> impl Future<Output = F::Output> {
    let times: VecDeque<_> = times.into_iter().collect();
    assert!(!times.is_empty());
    // Box before constructing the task-local future: async fn would retain F
    // in its initial state and duplicate these large caller futures at each layer.
    CLOCK.scope(RefCell::new(Script { times, last: None }), Box::pin(future))
}

pub(super) fn or_at<F: Future>(time: DateTime<Utc>, future: F) -> impl Future<Output = F::Output> {
    let future = Box::pin(future);
    async move {
        if CLOCK.try_with(|_| ()).is_ok() {
            future.await
        } else {
            at(time, future).await
        }
    }
}
