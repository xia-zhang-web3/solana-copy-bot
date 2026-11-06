//! Fixed, cumulative measurement only. Nested spans are not additive wall budgets.
use std::sync::Mutex;
use std::task::Poll;
use std::time::{Duration, Instant};

pub(crate) const STAGE_COUNT: usize = 14;
pub(crate) const DURATION_BOUNDS_NS: [u64; 7] = [
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];
pub(crate) const AGE_BOUNDS_MS: [u64; 7] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub(crate) enum Stage {
    SerialLoopTurnTotal,
    RefreshTotal,
    NextNetworkDecodeSchedulingWait,
    NextPollExecution,
    ScopedProcessTotal,
    OutputPushWait,
    CaptureRefreshBlockingQueueWait,
    CaptureRefreshWork,
    CaptureProcessBlockingQueueWait,
    CaptureProcessLockWait,
    PayloadEncodeHash,
    ReceiveEligibilityStore,
    DomainParse,
    CaptureFinish,
}

impl Stage {
    pub const ALL: [Self; STAGE_COUNT] = [
        Self::SerialLoopTurnTotal,
        Self::RefreshTotal,
        Self::NextNetworkDecodeSchedulingWait,
        Self::NextPollExecution,
        Self::ScopedProcessTotal,
        Self::OutputPushWait,
        Self::CaptureRefreshBlockingQueueWait,
        Self::CaptureRefreshWork,
        Self::CaptureProcessBlockingQueueWait,
        Self::CaptureProcessLockWait,
        Self::PayloadEncodeHash,
        Self::ReceiveEligibilityStore,
        Self::DomainParse,
        Self::CaptureFinish,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::SerialLoopTurnTotal => "serial_loop_turn_total",
            Self::RefreshTotal => "refresh_total",
            Self::NextNetworkDecodeSchedulingWait => "next_network_decode_scheduling_wait_total",
            Self::NextPollExecution => "next_poll_execution_wall_not_cpu",
            Self::ScopedProcessTotal => "scoped_process_total",
            Self::OutputPushWait => "output_push_wait",
            Self::CaptureRefreshBlockingQueueWait => "capture_refresh_blocking_queue_wait",
            Self::CaptureRefreshWork => "capture_refresh_work_including_lock_and_store",
            Self::CaptureProcessBlockingQueueWait => "capture_process_blocking_queue_wait",
            Self::CaptureProcessLockWait => "capture_process_lock_wait",
            Self::PayloadEncodeHash => "payload_encode_hash",
            Self::ReceiveEligibilityStore => "receive_eligibility_store",
            Self::DomainParse => "domain_parse",
            Self::CaptureFinish => "capture_finish",
        }
    }
    pub fn parent(self) -> Option<Self> {
        match self {
            Self::SerialLoopTurnTotal => None,
            Self::NextPollExecution => Some(Self::NextNetworkDecodeSchedulingWait),
            Self::CaptureRefreshBlockingQueueWait | Self::CaptureRefreshWork => {
                Some(Self::RefreshTotal)
            }
            Self::CaptureProcessBlockingQueueWait
            | Self::CaptureProcessLockWait
            | Self::PayloadEncodeHash
            | Self::ReceiveEligibilityStore
            | Self::DomainParse
            | Self::CaptureFinish => Some(Self::ScopedProcessTotal),
            _ => Some(Self::SerialLoopTurnTotal),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Distribution {
    pub count: u64,
    pub sum: u64,
    pub max: u64,
    pub histogram: [u64; 8],
}
impl Distribution {
    fn record(&mut self, value: u64, bounds: &[u64; 7]) {
        self.count = self.count.saturating_add(1);
        self.sum = self.sum.saturating_add(value);
        self.max = self.max.max(value);
        let bucket = bounds.iter().position(|bound| value <= *bound).unwrap_or(7);
        self.histogram[bucket] = self.histogram[bucket].saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderAge {
    Absent,
    Invalid,
    Future(u64),
    Available(u64),
}
impl ProviderAge {
    pub fn at(stamp: Option<(i64, i32)>, now_unix_ns: i128) -> Self {
        let Some((seconds, nanos)) = stamp else {
            return Self::Absent;
        };
        // Protobuf Timestamp range; diagnostic classification never filters business data.
        if !(-62_135_596_800..=253_402_300_799).contains(&seconds)
            || !(0..1_000_000_000).contains(&nanos)
        {
            return Self::Invalid;
        }
        let created = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
        let milliseconds = now_unix_ns.abs_diff(created) / 1_000_000;
        let value = milliseconds.min(u128::from(u64::MAX)) as u64;
        if created > now_unix_ns {
            Self::Future(value)
        } else {
            Self::Available(value)
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AgeSummary {
    pub absent: u64,
    pub invalid: u64,
    pub future: u64,
    pub available_ms: Distribution,
    pub future_ahead_ms: Distribution,
}
impl AgeSummary {
    fn record(&mut self, age: ProviderAge) {
        match age {
            ProviderAge::Absent => self.absent = self.absent.saturating_add(1),
            ProviderAge::Invalid => self.invalid = self.invalid.saturating_add(1),
            ProviderAge::Future(value) => {
                self.future = self.future.saturating_add(1);
                self.future_ahead_ms.record(value, &AGE_BOUNDS_MS);
            }
            ProviderAge::Available(value) => self.available_ms.record(value, &AGE_BOUNDS_MS),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Snapshot {
    pub stages: [Distribution; STAGE_COUNT],
    pub active: [u64; STAGE_COUNT],
    pub poll_pending: u64,
    pub poll_ready: u64,
    pub decoded_updates_age: AgeSummary,
    pub decoded_transactions_age: AgeSummary,
    pub enqueued_pairs: u64,
    pub enqueued_before_process_age: AgeSummary,
    pub enqueued_after_process_age: AgeSummary,
}

#[derive(Debug, Default)]
pub(crate) struct StageLedger {
    state: Mutex<Snapshot>,
}
impl StageLedger {
    pub fn record(&self, stage: Stage, duration: Duration) {
        if let Ok(mut state) = self.state.lock() {
            state.stages[stage as usize].record(nanoseconds(duration), &DURATION_BOUNDS_NS);
        }
    }
    #[cfg(test)]
    pub fn record_span(&self, stage: Stage, started: Duration, ended: Duration) {
        self.record(stage, ended.saturating_sub(started));
    }
    pub fn timer(&self, stage: Stage) -> StageTimer<'_> {
        if let Ok(mut state) = self.state.lock() {
            state.active[stage as usize] = state.active[stage as usize].saturating_add(1);
        }
        StageTimer {
            ledger: self,
            stage,
            started: Instant::now(),
        }
    }
    pub fn measure_poll<T>(&self, poll: impl FnOnce() -> Poll<T>) -> Poll<T> {
        let origin = Instant::now();
        self.measure_poll_with(|| origin.elapsed(), poll)
    }
    pub fn measure_poll_with<T>(
        &self,
        mut clock: impl FnMut() -> Duration,
        poll: impl FnOnce() -> Poll<T>,
    ) -> Poll<T> {
        let started = clock();
        let result = poll();
        let duration = clock().saturating_sub(started);
        if let Ok(mut state) = self.state.lock() {
            state.stages[Stage::NextPollExecution as usize]
                .record(nanoseconds(duration), &DURATION_BOUNDS_NS);
            let count = if result.is_ready() {
                &mut state.poll_ready
            } else {
                &mut state.poll_pending
            };
            *count = count.saturating_add(1);
        }
        result
    }
    pub fn decoded_age(&self, transaction: bool, age: ProviderAge) {
        if let Ok(mut state) = self.state.lock() {
            state.decoded_updates_age.record(age);
            if transaction {
                state.decoded_transactions_age.record(age);
            }
        }
    }
    pub fn enqueued_age_pair(&self, before: ProviderAge, after: ProviderAge) {
        if let Ok(mut state) = self.state.lock() {
            state.enqueued_pairs = state.enqueued_pairs.saturating_add(1);
            state.enqueued_before_process_age.record(before);
            state.enqueued_after_process_age.record(after);
        }
    }
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.state.lock().ok().map(|state| *state)
    }
}

fn nanoseconds(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

pub(crate) struct StageTimer<'a> {
    ledger: &'a StageLedger,
    stage: Stage,
    started: Instant,
}
impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        let elapsed = nanoseconds(self.started.elapsed());
        if let Ok(mut state) = self.ledger.state.lock() {
            state.active[self.stage as usize] = state.active[self.stage as usize].saturating_sub(1);
            state.stages[self.stage as usize].record(elapsed, &DURATION_BOUNDS_NS);
        }
    }
}

#[cfg(test)]
#[path = "../source_tests/receive_stage_ledger_tests.rs"]
mod tests;
