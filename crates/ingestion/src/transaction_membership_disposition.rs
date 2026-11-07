//! Sparse counters per selected ID, never a delivery trace or last-writer status.
use super::{Capture, RecordStage, Row, MAX_ROWS_PER_RECEIPT};
use copybot_core_types::SwapEvent;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

macro_rules! dispositions {
    ($($variant:ident => $label:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug)]
        #[repr(u8)]
        pub enum Disposition { $($variant),+ }
        impl Disposition {
            pub fn code(self) -> u8 { self as u8 }
            pub fn legend() -> BTreeMap<u8, &'static str> {
                [$( (Self::$variant.code(), $label) ),+].into_iter().collect()
            }
        }
    };
}

dispositions! {
    ParseVote => "parse_vote",
    ParseFailed => "parse_failed_transaction",
    ParseProgram => "parse_uninterested_program",
    ParseNoSwap => "parse_no_attributed_swap",
    ParseAmount => "parse_invalid_amount",
    ParseUnsupportedPumpSwap => "parse_unsupported_pumpswap",
    ParseUnsupportedMessage => "parse_unsupported_message",
    ParseOther => "parse_refused_unclassified",
    ParseError => "parse_error",
    CreatedAtUnavailable => "created_at_unavailable",
    SourceDedup => "source_signature_dedup",
    OutputReplaced => "output_replaced_old_record",
    OutputClosed => "output_closed",
    SwapParserRefused => "swap_parser_refused",
    AppDedup => "app_signature_dedup",
    AppAdmissionRefused => "app_admission_refused",
    BestEffortBudget => "best_effort_budget_exhausted",
    BestEffortOutputPressure => "best_effort_output_pressure",
    BestEffortWriterPressure => "best_effort_writer_backpressure",
    BacklogFull => "app_backlog_full",
    CriticalBacklogFull => "discovery_critical_backlog_full",
    BestEffortBacklogPruned => "best_effort_backlog_pruned",
    AppUnknown => "app_result_unknown",
    RawInserted => "raw_committed_inserted",
    RawDuplicate => "raw_committed_duplicate",
    RawUnknown => "raw_result_unknown",
    ContextBestEffort => "context_noncritical_best_effort",
    ContextDiscoveryCritical => "context_discovery_critical",
    ContextRelevant => "context_relevant",
    ContextOwnedSell => "context_owned_sell",
    ContextProtectedCapture => "context_protected_capture_receipt",
    Buffered => "backpressure_buffered_not_terminal",
}

impl Disposition {
    pub(super) fn unknown(self) -> bool {
        matches!(self, Self::RawUnknown | Self::AppUnknown)
    }
}

impl Row {
    pub(super) fn resolve(&mut self) {
        let terminals: u64 = self
            .outcomes
            .iter()
            .filter(|(code, _)| **code <= Disposition::RawUnknown.code())
            .map(|(_, count)| *count)
            .sum();
        let unknown = [Disposition::RawUnknown, Disposition::AppUnknown]
            .iter()
            .any(|outcome| self.outcomes.contains_key(&outcome.code()));
        self.pre_raw_resolution = if unknown {
            3
        } else if self.decoded.count > 1 || terminals > 1 {
            2
        } else if terminals == 1 {
            1
        } else {
            0
        };
    }
}

impl Capture {
    pub fn observe_disposition(&self, signature: &str, slot: u64, outcome: Disposition) {
        let Some(_active) = self.enter() else { return };
        if outcome.unknown() {
            self.gap();
        }
        self.record(signature, slot, RecordStage::Disposition(outcome));
    }

    /// Only successful whole raw transactions supply individual bools. No SQL attempt hook.
    pub fn observe_raw(&self, swaps: &[SwapEvent], inserted: Option<&[bool]>) {
        let Some(_active) = self.enter() else { return };
        if swaps.len() > MAX_ROWS_PER_RECEIPT {
            self.overflow.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let inserted = inserted.filter(|values| values.len() == swaps.len());
        if inserted.is_none() {
            self.gap();
        }
        for (index, swap) in swaps.iter().enumerate() {
            let outcome = match inserted.map(|values| values[index]) {
                Some(true) => Disposition::RawInserted,
                Some(false) => Disposition::RawDuplicate,
                None => Disposition::RawUnknown,
            };
            self.record(
                &swap.signature,
                swap.slot,
                RecordStage::Disposition(outcome),
            );
        }
    }
}

pub fn disposition(signature: &str, slot: u64, outcome: Disposition) {
    if let Some(capture) = super::GLOBAL.get() {
        capture.observe_disposition(signature, slot, outcome);
    }
}

pub fn raw_receipt(swaps: &[SwapEvent], inserted: Option<&[bool]>) {
    if let Some(capture) = super::GLOBAL.get() {
        capture.observe_raw(swaps, inserted);
    }
}

pub fn enabled() -> bool {
    super::GLOBAL.get().is_some_and(|capture| capture.enabled)
}
