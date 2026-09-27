//! Bounded timing and coverage facts for the durable association consumer.
use copybot_core_types::association_delivery::{DeliveryEvent, SessionGap};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tracing::info;

const REPORT_EVERY: Duration = Duration::from_secs(30);
const SAMPLE_CAP: usize = 128;

pub(crate) struct AssociationIngressTelemetry {
    reported_at: Instant,
    persisted: u64,
    parents: u64,
    admissions: u64,
    session_starts: u64,
    session_gaps: u64,
    last_parent_slot: Option<u64>,
    last_fence_slot: Option<u64>,
    persist_ms: VecDeque<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AssociationIngressSnapshot {
    pub(crate) persisted: u64,
    pub(crate) parents: u64,
    pub(crate) admissions: u64,
    pub(crate) session_starts: u64,
    pub(crate) session_gaps: u64,
    pub(crate) last_parent_slot: Option<u64>,
    pub(crate) last_fence_slot: Option<u64>,
    pub(crate) processed_minus_parent_slots: Option<i64>,
    pub(crate) persist_ms_p95: u64,
}

impl Default for AssociationIngressTelemetry {
    fn default() -> Self {
        Self {
            reported_at: Instant::now(), persisted: 0, parents: 0,
            admissions: 0, session_starts: 0, session_gaps: 0,
            last_parent_slot: None, last_fence_slot: None,
            persist_ms: VecDeque::new(),
        }
    }
}

impl AssociationIngressTelemetry {
    pub(crate) fn note_persisted(
        &mut self, event: Option<&DeliveryEvent>, fence_slot: Option<u64>, elapsed_ms: u64,
    ) {
        if event.is_none() && fence_slot.is_none() {
            return;
        }
        self.persisted = self.persisted.saturating_add(1);
        if self.persist_ms.len() == SAMPLE_CAP {
            self.persist_ms.pop_front();
        }
        self.persist_ms.push_back(elapsed_ms);
        match event {
            Some(DeliveryEvent::Parent(parent)) => {
                self.parents = self.parents.saturating_add(1);
                self.last_parent_slot = Some(parent.child.slot);
            }
            Some(DeliveryEvent::Admission(_)) => {
                self.admissions = self.admissions.saturating_add(1);
            }
            Some(DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)) => {
                self.session_starts = self.session_starts.saturating_add(1);
                self.last_parent_slot = None;
                self.last_fence_slot = None;
            }
            Some(DeliveryEvent::Session(_)) => {
                self.session_gaps = self.session_gaps.saturating_add(1);
                self.last_parent_slot = None;
                self.last_fence_slot = None;
            }
            _ => {}
        }
        if let Some(slot) = fence_slot {
            self.last_fence_slot = Some(slot);
        }
        self.emit_if_due();
    }

    pub(crate) fn snapshot(&self) -> AssociationIngressSnapshot {
        let mut values = self.persist_ms.iter().copied().collect::<Vec<_>>();
        values.sort_unstable();
        let p95 = if values.is_empty() { 0 } else {
            values[((values.len() - 1) as f64 * 0.95).round() as usize]
        };
        AssociationIngressSnapshot {
            persisted: self.persisted, parents: self.parents,
            admissions: self.admissions, session_starts: self.session_starts,
            session_gaps: self.session_gaps,
            last_parent_slot: self.last_parent_slot,
            last_fence_slot: self.last_fence_slot,
            processed_minus_parent_slots: self.last_fence_slot.zip(self.last_parent_slot)
                .map(|(fence, parent)| ((fence as i128 - parent as i128)
                    .clamp(i128::from(i64::MIN), i128::from(i64::MAX))) as i64),
            persist_ms_p95: p95,
        }
    }

    fn emit_if_due(&mut self) {
        if self.reported_at.elapsed() < REPORT_EVERY {
            return;
        }
        let s = self.snapshot();
        info!(
            persisted = s.persisted, parents = s.parents, admissions = s.admissions,
            session_starts = s.session_starts, session_gaps = s.session_gaps,
            last_parent_slot = ?s.last_parent_slot, last_processed_fence_slot = ?s.last_fence_slot,
            processed_minus_parent_slots = ?s.processed_minus_parent_slots,
            persist_ms_p95 = s.persist_ms_p95,
            "durable association consumer telemetry"
        );
        self.reported_at = Instant::now();
        self.persisted = 0;
        self.parents = 0;
        self.admissions = 0;
        self.session_starts = 0;
        self.session_gaps = 0;
        self.persist_ms.clear();
    }
}
