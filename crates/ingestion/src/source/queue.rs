use std::collections::VecDeque;
use tokio::sync::{Mutex as AsyncMutex, Notify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueueOverflowPolicy {
    Block,
    DropOldest,
}

impl QueueOverflowPolicy {
    pub(super) fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "drop_oldest" | "drop-oldest" => Self::DropOldest,
            _ => Self::Block,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::DropOldest => "drop_oldest",
        }
    }
}

#[derive(Debug)]
pub(super) enum QueuePushResult {
    Enqueued { backpressured: bool },
    ReplacedOldest,
}

#[derive(Debug)]
struct OverflowQueueState<T> {
    deque: VecDeque<T>,
    closed: bool,
}

#[derive(Debug)]
pub(super) struct OverflowQueue<T> {
    state: AsyncMutex<OverflowQueueState<T>>,
    capacity: usize,
    not_empty: Notify,
    not_full: Notify,
}

impl<T> OverflowQueue<T> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            state: AsyncMutex::new(OverflowQueueState {
                deque: VecDeque::with_capacity(capacity),
                closed: false,
            }),
            capacity: capacity.max(1),
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    pub(super) async fn push(
        &self,
        item: T,
        policy: QueueOverflowPolicy,
    ) -> Option<QueuePushResult> {
        let mut pending = Some(item);
        let mut was_backpressured = false;
        loop {
            let mut guard = self.state.lock().await;
            if guard.closed {
                return None;
            }
            if guard.deque.len() < self.capacity {
                guard
                    .deque
                    .push_back(pending.take().expect("pending item exists before enqueue"));
                drop(guard);
                self.not_empty.notify_one();
                return Some(QueuePushResult::Enqueued {
                    backpressured: was_backpressured,
                });
            }

            if matches!(policy, QueueOverflowPolicy::DropOldest) {
                let _ = guard.deque.pop_front();
                guard.deque.push_back(
                    pending
                        .take()
                        .expect("pending item exists before replacement"),
                );
                drop(guard);
                self.not_empty.notify_one();
                self.not_full.notify_one();
                return Some(QueuePushResult::ReplacedOldest);
            }

            was_backpressured = true;
            drop(guard);
            self.not_full.notified().await;
        }
    }

    pub(super) async fn pop(&self) -> Option<T> {
        loop {
            let mut guard = self.state.lock().await;
            if let Some(item) = guard.deque.pop_front() {
                drop(guard);
                self.not_full.notify_one();
                return Some(item);
            }
            if guard.closed {
                return None;
            }
            drop(guard);
            self.not_empty.notified().await;
        }
    }

    pub(super) async fn close(&self) {
        let mut guard = self.state.lock().await;
        guard.closed = true;
        drop(guard);
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }
}
