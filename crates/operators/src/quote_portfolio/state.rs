use std::collections::BTreeMap;

use super::{AdmissionReason, Components, CoverageIssue, Event, Knowledge, Mint, Order, Refusal};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mark {
    /// Retains quote/cost units, bindings, time and provenance verbatim.
    pub event: Event,
    pub raw: u64,
    pub net_lamports: i128,
    pub assumed_or_synthetic: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub id: String,
    pub mint: Mint,
    pub decimals: u8,
    pub entry_event_id: String,
    pub entry_raw: u64,
    pub remaining_raw: u64,
    pub entry: Components,
    pub allocated: Components,
    pub remainder: Components,
    pub locked_rent_lamports: u64,
    pub mark: Option<Mark>,
}

/// Lifetime accepted cash flows (all lamports), including closed positions.
/// cash = initial + sell_gross + rent_refunded - buy_principal - expenses
///        - rent_deposited. Mark estimates and entry allocation do not add flows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CashFlows {
    pub buy_principal: u64,
    pub sell_gross: u64,
    pub expenses: u64,
    pub rent_deposited: u64,
    pub rent_refunded: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Book {
    pub initial_cash_lamports: u64,
    pub cash_lamports: u64,
    pub max_open_positions: usize,
    pub open_slots: usize,
    pub locked_rent_lamports: u64,
    pub positions: BTreeMap<String, Position>,
    pub flows: CashFlows,
    pub assumed_or_synthetic: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EstimateBasis {
    CallerObservedOperands,
    AssumedOrSyntheticOperands,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValuationScope {
    IndependentExactQuotesAndRentBookValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Valuation {
    pub scope: ValuationScope,
    pub basis: EstimateBasis,
    pub cash_subtotal_lamports: u64,
    pub locked_rent_lamports: u64,
    pub known_net_marks_lamports: i128,
    pub full_equity_lamports: Knowledge<i128>,
    pub net_change_lamports: Knowledge<i128>,
    pub missing_marks: Vec<String>,
    pub unresolved: Vec<CoverageIssue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disposition {
    Applied,
    Skipped(Vec<AdmissionReason>),
    Refused(Refusal),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventState {
    pub cash_lamports: u64,
    pub locked_rent_lamports: u64,
    pub open_slots: usize,
    pub position: Option<Position>,
    pub flows: CashFlows,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Retains all input operands, including cost provenance, even on refusal.
    pub event: Event,
    pub disposition: Disposition,
    pub before: EventState,
    pub after: EventState,
    pub allocated_this_event: Components,
    pub valuation_after: Valuation,
}

impl Book {
    pub(super) fn event_state(&self, position_id: &str) -> EventState {
        EventState {
            cash_lamports: self.cash_lamports,
            locked_rent_lamports: self.locked_rent_lamports,
            open_slots: self.open_slots,
            position: self.positions.get(position_id).cloned(),
            flows: self.flows.clone(),
        }
    }
}

// The watermark is separate from the committed money/inventory book.
pub(super) fn follows(next: Order, previous: Option<Order>) -> bool {
    previous.is_none_or(|p| next.sequence > p.sequence && next.unix_ms >= p.unix_ms)
}
