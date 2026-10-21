//! Two source-time ACK observations per clear; diagnostic state is constant-space.
use super::*;
use crate::source::durable::hold_observation::{HoldStamp, HoldTracker};
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CommitIdentity {
    pub session: String,
    pub sequence: u64,
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub from_slot: u64,
}
impl From<&DurableCheckpoint> for CommitIdentity {
    fn from(h: &DurableCheckpoint) -> Self {
        Self {
            session: h.session.clone(),
            sequence: h.sequence,
            slot: h.block.observation.child.slot,
            blockhash: h.block.observation.child.hash.clone(),
            parent_slot: h.block.observation.parent.slot,
            parent_blockhash: h.block.observation.parent.hash.clone(),
            from_slot: h.from_slot,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AckSample {
    pub checkpoint: Option<CheckpointWitness>,
    pub commit_identity: CommitIdentity,
    pub reader: Option<ReaderIdentity>,
    pub began_ns: u64,
    pub ended_ns: u64,
    pub began_clock: ClockStamp,
    pub ended_clock: ClockStamp,
    pub hold_before: HoldStamp,
    pub hold_after: HoldStamp,
    pub context_before: ContinuityContext,
    pub context_after: ContinuityContext,
    pub valid: bool,
    pub invalid_reason: Option<&'static str>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AckObservation {
    pub schema: &'static str,
    pub origin_unix_ms: u64,
    pub eligible_clear: ContinuityTransition,
    pub clear_hold_before: HoldStamp,
    pub clear_hold_after: HoldStamp,
    pub samples: Vec<AckSample>,
    pub completed: bool,
    pub diagnostic_invalid: bool,
    pub invalid_reason: Option<&'static str>,
}
#[derive(Default)]
pub(super) struct AckState {
    pub current: Option<AckObservation>,
    pub completed: Option<AckObservation>,
}
pub(in crate::source) struct AckStart {
    began_ns: u64,
    began_clock: ClockStamp,
    hold: HoldStamp,
    context: ContinuityContext,
    commit: CommitIdentity,
}
fn exclusive(before: &HoldStamp, after: &HoldStamp) -> bool {
    before.coherent
        && after.coherent
        && before.epoch == after.epoch
        && before.revision.checked_add(2) == Some(after.revision)
}
fn clock_pair(before: &ClockStamp, after: &ClockStamp) -> bool {
    let qualified = |s: &ClockStamp| {
        s.schema == "linux_monotonic_elapsed_bracket_v1"
            && s.status == "PRESENT_NOT_ORDER_QUALIFIED"
            && s.clock_id == "CLOCK_MONOTONIC"
            && s.clock_code == 1
            && s.before_ns.zip(s.after_ns).is_some_and(|(a, b)| a <= b)
            && s.resolution_ns.is_some_and(|v| v > 0)
            && s.identity.boot_id.is_some()
            && s.identity.time_namespace.is_some()
            && s.identity.timens_offsets.is_some()
            && s.identity.pid_namespace.is_some()
            && s.identity.pid > 0
            && s.identity.process_start_ticks.is_some()
            && s.identity.process_origin_monotonic_ns.is_some()
            && s.identity.run_id.is_some()
    };
    qualified(before)
        && qualified(after)
        && before.identity == after.identity
        && before
            .after_ns
            .zip(after.before_ns)
            .is_some_and(|(a, b)| a <= b)
        && before.elapsed_ns <= after.elapsed_ns
}
impl ContinuityTelemetry {
    pub(in crate::source) fn hold_tracker(&self) -> Arc<HoldTracker> {
        self.holds.clone()
    }
    pub(in crate::source) fn association_session(&self, session: String) {
        let _phase = self.holds.phase();
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .context
            .association_session = Some(session);
    }
    pub(in crate::source) fn begin_ack(
        &self,
        head: &DurableCheckpoint,
        flag: Option<&Arc<AtomicBool>>,
    ) -> AckStart {
        let (began_ns, began_clock) = diagnostic_time();
        let hold = self.holds.sample(flag);
        let context = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .context
            .clone();
        AckStart {
            began_ns,
            began_clock,
            hold,
            context,
            commit: CommitIdentity::from(head),
        }
    }
    pub(in crate::source) fn finish_ack(&self, start: AckStart, flag: Option<&Arc<AtomicBool>>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let context = state.context.clone();
        #[cfg(test)]
        crate::source::durable::clock_qualification_tests::ack_hook::at_ack_tail();
        let (ended_ns, ended_clock) = diagnostic_time();
        // Last flag/counter read covers both sampled context and the entire
        // exported source-time bracket. A later writer is conservatively seen,
        // never hidden inside an already-labelled coherent ACK bracket.
        let hold = self.holds.sample(flag);
        let Some(current) = state.ack.current.as_mut().filter(|e| e.samples.len() < 2) else {
            return;
        };
        let checkpoint = context.checkpoint.clone();
        let reader = context.active_reader.clone();
        let cp_matches = checkpoint.as_ref().is_some_and(|c| {
            c.session == start.commit.session
                && c.sequence == start.commit.sequence
                && c.slot == start.commit.slot
        });
        let same_reader = reader.as_ref() == Some(&current.eligible_clear.reader)
            && start.context.active_reader == reader
            && start.context.association_session.as_ref() == Some(&start.commit.session)
            && context.association_session == start.context.association_session
            && current.eligible_clear.context_at_record.association_session
                == context.association_session;
        let coherent = exclusive(&start.hold, &hold)
            && start.hold.hold == Some(false)
            && hold.hold == Some(false)
            && hold.epoch == current.clear_hold_after.epoch
            && cp_matches
            && same_reader
            && clock_pair(&current.eligible_clear.ended_clock, &start.began_clock)
            && clock_pair(&start.began_clock, &ended_clock);
        if coherent
            && current
                .samples
                .last()
                .is_some_and(|s| s.commit_identity == start.commit)
        {
            // Exact duplicate ACKs are observable continuity updates, not the
            // next distinct checkpoint. Never substitute them for sample two.
            return;
        }
        let previous = current
            .samples
            .last()
            .and_then(|s| s.checkpoint.as_ref())
            .or(current.eligible_clear.context_at_record.checkpoint.as_ref());
        let forward = previous.is_some_and(|p| {
            p.session == start.commit.session
                && p.sequence < start.commit.sequence
                && p.slot < start.commit.slot
        });
        let reason = if !exclusive(&start.hold, &hold) {
            Some("ack_writer_overlap_or_incoherent")
        } else if start.hold.hold != Some(false)
            || hold.hold != Some(false)
            || hold.epoch != current.clear_hold_after.epoch
        {
            Some("hold_epoch_or_flag_changed")
        } else if !cp_matches {
            Some("checkpoint_continuity_mismatch")
        } else if !same_reader {
            Some("reader_or_association_mismatch")
        } else if !clock_pair(&current.eligible_clear.ended_clock, &start.began_clock)
            || !clock_pair(&start.began_clock, &ended_clock)
        {
            Some("ack_clock_unqualified")
        } else if !forward {
            Some("duplicate_or_nonforward_ack")
        } else if start.began_ns < current.eligible_clear.ended_ns || ended_ns < start.began_ns {
            Some("ack_clear_order")
        } else {
            None
        };
        current.samples.push(AckSample {
            checkpoint,
            commit_identity: start.commit,
            reader,
            began_ns: start.began_ns,
            ended_ns,
            began_clock: start.began_clock,
            ended_clock,
            hold_before: start.hold,
            hold_after: hold,
            context_before: start.context,
            context_after: context,
            valid: reason.is_none(),
            invalid_reason: reason,
        });
        if reason.is_some() {
            current.diagnostic_invalid = true;
            current.invalid_reason = reason;
        }
        current.completed = current.samples.len() == 2 && !current.diagnostic_invalid;
        let event = current.clone();
        if event.completed {
            state.ack.completed = Some(event.clone());
        }
        drop(state);
        if let Ok(encoded) = serde_json::to_string(&event) {
            // Export is diagnostic-only: a subscriber failure cannot change a
            // successfully committed cursor/continuity ACK or its business flag.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tracing::info!(ack_observation_json = %encoded, "durable ACK observation");
            }));
        }
    }
    pub(in crate::source) fn ack_snapshot(
        &self,
    ) -> (Option<AckObservation>, Option<AckObservation>) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        (state.ack.current.clone(), state.ack.completed.clone())
    }
    pub(in crate::source) fn failed_ack(&self) {
        self.invalidate_ack("cursor_ack_refused");
    }
    pub(in crate::source) fn invalidate_ack(&self, reason: &'static str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = state.ack.current.as_mut().filter(|e| !e.completed) {
            current.diagnostic_invalid = true;
            current.invalid_reason = Some(reason);
        }
    }
}
pub(super) fn clear(
    state: &mut State,
    event: &ContinuityTransition,
    before: HoldStamp,
    after: HoldStamp,
) {
    let context_stable = event.context_before.active_reader.as_ref() == Some(&event.reader)
        && event.context_at_record.active_reader == event.context_before.active_reader
        && event.context_before.association_session == event.context_at_record.association_session
        && event.context_before.checkpoint == event.context_at_record.checkpoint
        && event.context_before.checkpoint_clock == event.context_at_record.checkpoint_clock;
    let valid = exclusive(&before, &after)
        && before.hold == Some(true)
        && after.hold == Some(false)
        && !event.interrupted_before
        && !event.interrupted_after
        && context_stable
        && clock_pair(&event.began_clock, &event.ended_clock);
    state.ack.current = Some(AckObservation {
        schema: "durable_ack_observation_v1",
        origin_unix_ms: origin().1,
        eligible_clear: event.clone(),
        clear_hold_before: before,
        clear_hold_after: after,
        samples: Vec::with_capacity(2),
        completed: false,
        diagnostic_invalid: !valid,
        invalid_reason: (!valid).then_some("clear_overlap_or_ineligible"),
    });
}
