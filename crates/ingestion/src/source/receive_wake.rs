//! One delegated poll, immutable wake registrations and a fixed four-slot pool.
use super::receive_wake_metrics::{Count, WakeMetrics};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

pub(crate) const SLOT_COUNT: usize = 4;
type Clock = Arc<dyn Fn() -> Duration + Send + Sync>;
#[derive(Clone, Copy)]
struct Notification {
    at: Duration,
    pre_pending: bool,
}
struct Epoch {
    id: u64,
    pending_at: Option<Duration>,
    first: Option<Notification>,
}
struct Shared {
    active: Mutex<Option<Epoch>>,
    metrics: WakeMetrics,
    clock: Clock,
}
impl Shared {
    fn notify(&self, id: u64) {
        let Ok(mut active) = self.active.lock() else {
            self.metrics.update(|s| s.bump(Count::StateUnavailable));
            return;
        };
        // Timestamp the first serialized observation, not a racing thread's
        // earlier entry. Clocks must be nonblocking and must not reenter us.
        let at = (self.clock)();
        self.metrics.update(|s| {
            s.bump(Count::Notifications);
            let Some(epoch) = active.as_mut() else {
                s.bump(Count::ClosedNotifications);
                return;
            };
            if epoch.id != id {
                s.bump(Count::SupersededNotifications);
                return;
            }
            if epoch.first.is_some() {
                s.bump(Count::RepeatedNotifications);
                return;
            }
            let pre_pending = epoch.pending_at.is_none_or(|pending| at < pending);
            if pre_pending {
                s.bump(Count::PrePendingNotifications);
            }
            s.bump(Count::FirstNotifications);
            epoch.first = Some(Notification { at, pre_pending });
        });
    }
    fn take(&self) -> Option<Epoch> {
        match self.active.lock() {
            Ok(mut active) => {
                let epoch = active.take();
                if epoch.as_ref().is_some_and(|e| e.pending_at.is_some()) {
                    self.metrics
                        .update(|s| s.active_pending = s.active_pending.saturating_sub(1));
                }
                epoch
            }
            Err(_) => {
                self.metrics.update(|s| s.bump(Count::StateUnavailable));
                None
            }
        }
    }
}
struct Registration {
    shared: Arc<Shared>,
    id: u64,
    consumer: Option<Waker>,
}
impl Wake for Registration {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.notify(self.id);
        // No internal lock is held across external wake code. Old registrations
        // retain their original consumer, even when a later poll changes tasks.
        if let Some(consumer) = &self.consumer {
            consumer.wake_by_ref();
        }
    }
}

pub(crate) struct WakeObserver {
    shared: Arc<Shared>,
    slots: [Arc<Registration>; SLOT_COUNT],
    sequence: u64,
}
impl WakeObserver {
    pub fn new(metrics: WakeMetrics) -> Self {
        let origin = Instant::now();
        Self::with_clock(metrics, Arc::new(move || origin.elapsed()))
    }
    pub fn with_clock(metrics: WakeMetrics, clock: Clock) -> Self {
        let shared = Arc::new(Shared {
            active: Mutex::new(None),
            metrics,
            clock,
        });
        let slots = std::array::from_fn(|_| {
            Arc::new(Registration {
                shared: shared.clone(),
                id: 0,
                consumer: None,
            })
        });
        Self {
            shared,
            slots,
            sequence: 0,
        }
    }
    pub fn observe<F, T>(&mut self, poll: F) -> Observed<'_, F>
    where
        F: FnMut(&mut Context<'_>) -> Poll<T> + Unpin,
    {
        Observed {
            observer: self,
            poll,
            complete: false,
        }
    }
    fn prepare(&mut self, consumer: &Waker) -> Option<(u64, Waker)> {
        let Some(id) = self.sequence.checked_add(1) else {
            self.shared
                .metrics
                .update(|s| s.bump(Count::GenerationExhausted));
            return None;
        };
        self.sequence = id;
        let Some(slot) = self
            .slots
            .iter_mut()
            .find(|slot| Arc::strong_count(slot) == 1)
        else {
            self.shared
                .metrics
                .update(|s| s.bump(Count::UnobservedPolls));
            return None;
        };
        // Only unique slots may be re-tagged. Retained upstream wakers keep their
        // immutable epoch/consumer; exhaustion falls back to the original waker.
        let Some(registration) = Arc::get_mut(slot) else {
            return None;
        };
        registration.id = id;
        registration.consumer = Some(consumer.clone());
        if let Ok(mut active) = self.shared.active.lock() {
            *active = Some(Epoch {
                id,
                pending_at: None,
                first: None,
            });
            Some((id, Waker::from(slot.clone())))
        } else {
            self.shared
                .metrics
                .update(|s| s.bump(Count::StateUnavailable));
            None
        }
    }
    fn finish(&self, id: Option<u64>, pending: bool) {
        let at = (self.shared.clock)();
        if let Ok(mut active) = self.shared.active.lock() {
            if let Some(epoch) = active.as_mut().filter(|e| Some(e.id) == id) {
                if pending {
                    epoch.pending_at = Some(at);
                    self.shared.metrics.update(|s| {
                        s.active_pending = s.active_pending.saturating_add(1);
                        s.bump(Count::PendingEpochs);
                    });
                } else {
                    if epoch.first.is_some() {
                        self.shared
                            .metrics
                            .update(|s| s.bump(Count::ReadyPollNotified));
                    }
                    *active = None;
                }
            }
        }
    }
    fn repoll(&self, previous: Option<Epoch>, at: Duration, pending: bool) {
        self.shared.metrics.update(|s| {
            s.bump(Count::Polls);
            s.bump(if pending {
                Count::Pending
            } else {
                Count::Ready
            });
            let Some(epoch) = previous.filter(|e| e.pending_at.is_some()) else {
                return;
            };
            s.bump(if pending {
                Count::RepollsPending
            } else {
                Count::RepollsReady
            });
            let Some(first) = epoch.first else {
                s.bump(Count::RepollsWithoutWake);
                return;
            };
            let Some(after) = at.checked_sub(first.at) else {
                s.bump(Count::InvalidOrdering);
                return;
            };
            if first.pre_pending {
                s.record_pre_pending(after);
                return;
            }
            match first.at.checked_sub(epoch.pending_at.unwrap()) {
                Some(before) => s.record_pair(before, after),
                None => s.bump(Count::InvalidOrdering),
            }
        });
    }
}
pub(crate) struct Observed<'a, F> {
    observer: &'a mut WakeObserver,
    poll: F,
    complete: bool,
}
impl<F, T> Future for Observed<'_, F>
where
    F: FnMut(&mut Context<'_>) -> Poll<T> + Unpin,
{
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        let at = (this.observer.shared.clock)();
        let previous = this.observer.shared.take();
        let prepared = this.observer.prepare(cx.waker());
        let result = match &prepared {
            Some((_, waker)) => (this.poll)(&mut Context::from_waker(waker)),
            None => (this.poll)(cx),
        };
        this.observer
            .finish(prepared.as_ref().map(|p| p.0), result.is_pending());
        this.observer.repoll(previous, at, result.is_pending());
        this.complete = result.is_ready();
        result
    }
}
impl<F> Drop for Observed<'_, F> {
    fn drop(&mut self) {
        if !self.complete {
            let active = self.observer.shared.take();
            self.observer.shared.metrics.update(|s| {
                s.bump(Count::CancelledFutures);
                if active.is_some_and(|e| e.pending_at.is_some()) {
                    s.bump(Count::CancelledPending);
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../source_tests/receive_wake_tests.rs"]
mod tests;
