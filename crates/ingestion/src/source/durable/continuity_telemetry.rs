//! Constant-space observations; none of these values authorize continuity.
use super::diagnostic_clock::ClockStamp;
use copybot_core_types::association_recovery::DurableCheckpoint;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub fn monotonic_ns() -> u64 {
    origin().0.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
pub(super) fn diagnostic_time() -> (u64, ClockStamp) {
    super::diagnostic_clock::observe(monotonic_ns)
}
pub(super) fn origin_unix_ms() -> u64 {
    origin().1
}
fn origin() -> &'static (Instant, u64) {
    static ORIGIN: OnceLock<(Instant, u64)> = OnceLock::new();
    ORIGIN.get_or_init(|| {
        (
            Instant::now(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        )
    })
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReaderIdentity {
    pub reader_id: u64,
    pub session: String,
    pub generation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CheckpointWitness {
    pub session: String,
    pub sequence: u64,
    pub slot: u64,
    pub observed_ns: u64,
    /// None means this ACK cannot be attributed to the active reader.
    pub reader_id: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FrontierWitness {
    pub reader: ReaderIdentity,
    pub slot: u64,
    pub observed_ns: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ContinuityContext {
    pub active_reader: Option<ReaderIdentity>,
    pub reader_clock: Option<ClockStamp>,
    pub checkpoint_clock: Option<ClockStamp>,
    pub frontier_clock: Option<ClockStamp>,
    pub checkpoint: Option<CheckpointWitness>,
    pub live_frontier: Option<FrontierWitness>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionKind {
    TaskGuardDrop,
    ReaderDrop,
    VerifiedClear,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviousRelation {
    First,
    Before,
    After,
    Overlap,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ContinuityTransition {
    pub kind: TransitionKind,
    pub reader: ReaderIdentity,
    /// Source-time bracket, independent of logging/consumer scheduling.
    pub began_ns: u64,
    pub ended_ns: u64,
    pub began_clock: ClockStamp,
    pub ended_clock: ClockStamp,
    pub hold_before: Option<bool>,
    pub hold_after: Option<bool>,
    pub interrupted_before: bool,
    pub interrupted_after: bool,
    pub context_before: ContinuityContext,
    /// Context sampled when recording; it may advance after ended_ns.
    pub context_at_record: ContinuityContext,
    pub recorded_ns: u64,
    pub recorded_clock: ClockStamp,
    pub record_sequence: u64,
    /// Relation of source brackets, never inferred from emission order.
    pub previous_relation: PreviousRelation,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ContinuitySnapshot {
    pub origin_unix_ms: u64,
    pub sampled_ns: u64,
    pub sampled_clock: ClockStamp,
    pub context: ContinuityContext,
    pub last_task_guard_drop: Option<ContinuityTransition>,
    pub last_reader_drop: Option<ContinuityTransition>,
    pub last_clear: Option<ContinuityTransition>,
    pub transition_count: u64,
    /// Latest-per-kind summaries are bounded, not a complete transition history.
    pub overwritten_observations: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ContinuityPrecheck {
    pub began_ns: u64,
    pub ended_ns: u64,
    pub began_clock: ClockStamp,
    pub ended_clock: ClockStamp,
    pub hold_before: Option<bool>,
    pub hold_after: Option<bool>,
    pub snapshot: ContinuitySnapshot,
}
#[derive(Default)]
struct State {
    next_reader: u64,
    context: ContinuityContext,
    last_task_guard_drop: Option<ContinuityTransition>,
    last_reader_drop: Option<ContinuityTransition>,
    last_clear: Option<ContinuityTransition>,
    previous: Option<(u64, u64)>,
    count: u64,
    overwritten: u64,
}
#[derive(Default)]
pub(crate) struct ContinuityTelemetry(Mutex<State>);
#[derive(Clone)]
pub(in crate::source) struct ReaderObservation {
    telemetry: Arc<ContinuityTelemetry>,
    identity: ReaderIdentity,
}
pub(in crate::source) struct TransitionStart {
    began_ns: u64,
    began_clock: ClockStamp,
    context: ContinuityContext,
    hold: Option<bool>,
    interrupted: bool,
}
impl ContinuityTelemetry {
    pub(in crate::source) fn reader(
        self: &Arc<Self>,
        session: String,
        generation: u64,
    ) -> ReaderObservation {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.next_reader = state.next_reader.saturating_add(1);
        let identity = ReaderIdentity {
            reader_id: state.next_reader,
            session,
            generation,
        };
        state.context.active_reader = Some(identity.clone());
        let (observed_ns, reader_clock) = diagnostic_time();
        state.context.reader_clock = Some(reader_clock.clone());
        let result = ReaderObservation {
            telemetry: self.clone(),
            identity,
        };
        tracing::info!(reader = ?result.identity, observed_ns, clock_stamp = ?reader_clock,
            origin_unix_ms = origin().1, "durable reader generation");
        result
    }
    pub(crate) fn checkpoint(head: &DurableCheckpoint) -> CheckpointWitness {
        CheckpointWitness {
            session: head.session.clone(),
            sequence: head.sequence,
            slot: head.block.observation.child.slot,
            observed_ns: monotonic_ns(),
            reader_id: None,
        }
    }
    pub(crate) fn acknowledged(&self, mut checkpoint: CheckpointWitness) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        checkpoint.reader_id = state
            .context
            .active_reader
            .as_ref()
            .filter(|r| r.session == checkpoint.session)
            .map(|r| r.reader_id);
        let (observed_ns, clock) = diagnostic_time();
        checkpoint.observed_ns = observed_ns;
        state.context.checkpoint_clock = Some(clock);
        state.context.checkpoint = Some(checkpoint);
    }
    pub(crate) fn snapshot(&self) -> ContinuitySnapshot {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let (sampled_ns, sampled_clock) = diagnostic_time();
        ContinuitySnapshot {
            origin_unix_ms: origin().1,
            sampled_ns,
            sampled_clock,
            context: state.context.clone(),
            last_task_guard_drop: state.last_task_guard_drop.clone(),
            last_reader_drop: state.last_reader_drop.clone(),
            last_clear: state.last_clear.clone(),
            transition_count: state.count,
            overwritten_observations: state.overwritten,
        }
    }
}
impl ReaderObservation {
    pub(super) fn identity(&self) -> ReaderIdentity {
        self.identity.clone()
    }
    pub(in crate::source) fn frontier(&self, slot: u64) {
        let mut state = self.telemetry.0.lock().unwrap_or_else(|e| e.into_inner());
        let (observed_ns, clock) = diagnostic_time();
        state.context.frontier_clock = Some(clock);
        state.context.live_frontier = Some(FrontierWitness {
            reader: self.identity.clone(),
            slot,
            observed_ns,
        });
    }
    pub(in crate::source) fn begin_observed(
        &self,
        began: (u64, ClockStamp),
        hold: Option<bool>,
        interrupted: bool,
    ) -> TransitionStart {
        let context = self
            .telemetry
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .context
            .clone();
        TransitionStart {
            began_ns: began.0,
            began_clock: began.1,
            context,
            hold,
            interrupted,
        }
    }
    pub(in crate::source) fn finish_observed(
        &self,
        kind: TransitionKind,
        start: TransitionStart,
        ended: (u64, ClockStamp),
        hold: Option<bool>,
        interrupted: bool,
    ) {
        let (ended_ns, ended_clock) = ended;
        let mut state = self.telemetry.0.lock().unwrap_or_else(|e| e.into_inner());
        state.count = state.count.saturating_add(1);
        let previous_relation = match state.previous {
            None => PreviousRelation::First,
            Some((_, end)) if end <= start.began_ns => PreviousRelation::After,
            Some((begin, _)) if ended_ns <= begin => PreviousRelation::Before,
            Some(_) => PreviousRelation::Overlap,
        };
        let (recorded_ns, recorded_clock) = diagnostic_time();
        let event = ContinuityTransition {
            kind,
            reader: self.identity.clone(),
            began_ns: start.began_ns,
            ended_ns,
            began_clock: start.began_clock,
            ended_clock,
            hold_before: start.hold,
            hold_after: hold,
            interrupted_before: start.interrupted,
            interrupted_after: interrupted,
            context_before: start.context,
            context_at_record: state.context.clone(),
            recorded_ns,
            recorded_clock,
            record_sequence: state.count,
            previous_relation,
        };
        state.previous = Some((event.began_ns, event.ended_ns));
        let target = match kind {
            TransitionKind::TaskGuardDrop => &mut state.last_task_guard_drop,
            TransitionKind::ReaderDrop => &mut state.last_reader_drop,
            TransitionKind::VerifiedClear => &mut state.last_clear,
        };
        let replaced = target.replace(event.clone()).is_some();
        state.overwritten = state.overwritten.saturating_add(u64::from(replaced));
        drop(state);
        if let Ok(encoded) = serde_json::to_string(&event) {
            tracing::info!(transition_json = %encoded, origin_unix_ms = origin().1,
                "durable continuity transition");
        }
    }
}

#[cfg(test)]
#[path = "../../source_tests/continuity_test_impl.rs"]
mod test_impl;
