use super::{Direction, Knowledge, Lamports, Mint, Order, Provenance};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialState {
    pub cash_lamports: u64,
    pub max_open_positions: usize,
    pub inventory_raw: Knowledge<u64>,
    pub external_transfers_lamports: Knowledge<i128>,
}

/// Exact quote input/output units: BUY lamports→raw; SELL raw→lamports.
/// Quotes are bound to position, mint, decimals, direction and requested size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactQuote {
    pub position_id: String,
    pub mint: Mint,
    pub decimals: u8,
    pub direction: Direction,
    pub input: u64,
    pub output: Knowledge<u64>,
    pub provenance: Provenance,
}

/// Each bundle belongs exclusively to this event/position/direction.
/// BUY exit must be explicitly Known(0); SELL setup must be Known(0).
/// A zero component still needs explicit provenance. No inferred fee defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeCosts {
    pub event_id: String,
    pub position_id: String,
    pub direction: Direction,
    pub base: Lamports,
    pub priority: Lamports,
    pub setup: Lamports,
    pub exit: Lamports,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Buy {
        mint: Mint,
        decimals: u8,
        input_lamports: u64,
        quote: Knowledge<ExactQuote>,
        costs: TradeCosts,
        /// Bound to this BUY event; retained independently of expenses.
        rent_deposit: Lamports,
    },
    Sell {
        raw: u64,
        quote: Knowledge<ExactQuote>,
        costs: TradeCosts,
    },
    Mark {
        quote: Knowledge<ExactQuote>,
        /// Prospective SELL costs; never charged to the book by a mark.
        costs: TradeCosts,
    },
    RentRefund {
        deposit_event_id: String,
        amount: Lamports,
    },
    /// Separate caller-declared charge for a failed attempt, never inferred from
    /// a skipped BUY. position_id is a correlation label, not a position binding.
    /// Distinct event ids do not prove distinct actual payments.
    FailedAttemptExpense { amount: Lamports },
    /// Free-form expenses remain refused with incomplete coverage; never
    /// interpreted as zero or automatically promoted to a supported debit.
    UnsupportedExpense { kind: String, amount: Lamports },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub id: String,
    pub position_id: String,
    pub order: Order,
    pub action: Action,
}
