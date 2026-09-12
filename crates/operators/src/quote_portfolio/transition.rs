use std::collections::BTreeMap;

use super::state::follows;
use super::{trades, valuation, Book, CashFlows, Components, CoverageIssue, Disposition};
use super::{Action, EstimateBasis, Provenance};
use super::{Event, InitialState, Knowledge, Order, Outcome, Refusal, Valuation};

/// One append-only in-memory session. No imports of partially known history.
#[derive(Clone, Debug)]
pub struct Portfolio {
    book: Book,
    seen: BTreeMap<String, (Event, Outcome)>,
    last_order: Option<Order>,
    unresolved: Vec<CoverageIssue>,
    unaccounted_failed_expense: Option<String>,
    refused_expense_assumed: bool,
}

impl Portfolio {
    pub fn new(initial: InitialState) -> Result<Self, Refusal> {
        if initial.inventory_raw != Knowledge::Known(0)
            || initial.external_transfers_lamports != Knowledge::Known(0)
        {
            return Err(Refusal::UnsupportedInitialHistory);
        }
        Ok(Self {
            book: Book {
                initial_cash_lamports: initial.cash_lamports,
                cash_lamports: initial.cash_lamports,
                max_open_positions: initial.max_open_positions,
                open_slots: 0,
                locked_rent_lamports: 0,
                positions: BTreeMap::new(),
                flows: CashFlows::default(),
                assumed_or_synthetic: false,
            },
            seen: BTreeMap::new(),
            last_order: None,
            unresolved: Vec::new(),
            unaccounted_failed_expense: None,
            refused_expense_assumed: false,
        })
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn valuation(&self) -> Valuation {
        let mut value = valuation::value(&self.book, &self.unresolved);
        if self.refused_expense_assumed {
            value.basis = EstimateBasis::AssumedOrSyntheticOperands;
        }
        value
    }

    pub fn last_order(&self) -> Option<Order> {
        self.last_order
    }

    /// Duplicate identity with identical operands returns its historical outcome.
    /// Conflicts and refusals change coverage only, never the committed book.
    pub fn apply(&mut self, event: Event) -> Outcome {
        if let Some((previous, outcome)) = self.seen.get(&event.id) {
            if *previous == event {
                return outcome.clone();
            }
            return self.refuse(event, Refusal::ConflictId, false);
        }
        if !valid_id(&event.id) || !valid_id(&event.position_id) {
            return self.refuse(event, Refusal::InvalidIdentity, true);
        }
        if !follows(event.order, self.last_order) {
            return self.refuse(event, Refusal::OutOfOrder, true);
        }
        self.last_order = Some(event.order);
        if matches!(event.action, Action::Buy { .. }) {
            if let Some(expense_event_id) = self.unaccounted_failed_expense.clone() {
                return self.refuse(
                    event,
                    Refusal::CashAvailabilityUnknown { expense_event_id },
                    true,
                );
            }
        }
        let before = self.book.event_state(&event.position_id);
        // Stage only the bounded book, not replay history. Any error drops all
        // intermediate arithmetic/quantity changes together.
        let mut staged = self.book.clone();
        let (disposition, allocated_this_event) = match trades::apply(&mut staged, &event) {
            Ok(result) => result,
            Err(reason) => return self.refuse(event, reason, true),
        };
        if disposition == Disposition::Applied {
            self.book = staged;
        } else if matches!(disposition, Disposition::Skipped(_)) {
            // Keep admission provenance without committing staged money/inventory.
            self.book.assumed_or_synthetic |= staged.assumed_or_synthetic;
        }
        let outcome = Outcome {
            before,
            after: self.book.event_state(&event.position_id),
            disposition,
            allocated_this_event,
            valuation_after: self.valuation(),
            event: event.clone(),
        };
        self.seen.insert(event.id.clone(), (event, outcome.clone()));
        outcome
    }

    fn refuse(&mut self, event: Event, refusal: Refusal, remember: bool) -> Outcome {
        // Only this new expense kind taints future cash admission. A conflict
        // may change the action itself, so inspect both sides of the identity.
        for relevant in
            std::iter::once(&event).chain(self.seen.get(&event.id).map(|(previous, _)| previous))
        {
            if let Action::FailedAttemptExpense { amount } = &relevant.action {
                self.unaccounted_failed_expense
                    .get_or_insert_with(|| event.id.clone());
                self.refused_expense_assumed |=
                    !matches!(amount.provenance, Provenance::Observed(_));
            }
        }
        let issue = CoverageIssue {
            event_id: event.id.clone(),
            refusal: refusal.clone(),
        };
        if !self.unresolved.contains(&issue) {
            self.unresolved.push(issue);
        }
        let state = self.book.event_state(&event.position_id);
        let outcome = Outcome {
            event: event.clone(),
            disposition: Disposition::Refused(refusal),
            before: state.clone(),
            after: state,
            allocated_this_event: Components::default(),
            valuation_after: self.valuation(),
        };
        if remember {
            self.seen.insert(event.id.clone(), (event, outcome.clone()));
        }
        outcome
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b))
}
