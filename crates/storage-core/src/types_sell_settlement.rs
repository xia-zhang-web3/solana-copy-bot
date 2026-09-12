use crate::{ExecutionCanaryReceiptFacts, ReceiptDecomposition};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};

/// A computed value only: Ready is neither an accounting marker nor apply permission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionCanarySellSettlement {
    Ready(ExecutionCanarySellSettlementPlan),
    Unsupported(SellSettlementUnsupported),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SellSettlementUnsupported {
    MissingReceiptFacts,
    MissingOrder,
    OrderNotPending,
    AlreadyAccounted,
    NotSell,
    UnresolvedTokenCoverage,
    NonNegativeTokenDelta,
    TokenQuantityOutOfRange,
    NoOwnedPosition,
    MultipleOwnedPositions,
    MissingPositionQuantity,
    PositionQuantityOutOfRange,
    MissingEntryBasis,
    MissingAccumulatedCashResult,
    DecimalsMismatch,
    Oversell,
    ArithmeticOverflow,
    SqliteIntegerOutOfRange,
    ReceiptAlreadyClaimed,
    UnprovenReceiptOwnership,
    DurableIdentity(crate::ReceiptFactsIdentityRejection),
}

impl std::fmt::Display for SellSettlementUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sell settlement unsupported: {self:?}")
    }
}
impl std::error::Error for SellSettlementUnsupported {}

/// All exact inputs of the position calculation, for future transactional revalidation.
/// The existing pnl_lamports value is read as a known INTEGER, without NULL/REAL fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SellSettlementExpectedPosition {
    pub position_id: String,
    pub token: String,
    pub accounting_bucket: String,
    pub state: String,
    pub opened_ts: String,
    pub closed_ts: Option<String>,
    pub quantity: TokenQuantity,
    pub entry_basis: Lamports,
    pub accumulated_cash_result: SignedLamports,
}

/// Explicit compatibility with the current SQLite INTEGER columns. OutOfRange is
/// still an exact i128 calculation; it must never be narrowed by a future writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementSqliteSignedValue {
    Fits(i64),
    OutOfRange,
}

impl SettlementSqliteSignedValue {
    pub(crate) fn from_exact(value: SignedLamports) -> Self {
        match i64::try_from(value.as_i128()) {
            Ok(value) => Self::Fits(value),
            Err(_) => Self::OutOfRange,
        }
    }
}

/// Reserved exact price representation. The receipt-only planner always returns None.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementSwapPrice {
    pub quote_lamports: Lamports,
    pub base_quantity: TokenQuantity,
}

/// Cash basis only, not economic/trading PnL. No fee adjustment is performed:
/// fees, rent, WSOL, tips and external transfers are not quantitatively decomposed.
///
/// A future apply MUST re-read/revalidate order/signal/proof/facts (including no fill)
/// and the expected position in the SAME write transaction as inventory, the unified
/// settlement marker and completion. This snapshot can become stale immediately.
/// Use apply_execution_canary_sell_settlement with fresh receipt operands; never
/// write this externally computed plan. The apply method replans inside its write tx.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCanarySellSettlementPlan {
    pub receipt: ExecutionCanaryReceiptFacts,
    pub expected_position: SellSettlementExpectedPosition,
    pub sold_quantity: TokenQuantity,
    pub remaining_quantity: TokenQuantity,
    /// OPEN for every nonzero raw remainder, including one raw unit.
    pub remaining_position_state: String,
    pub allocated_entry_basis: Lamports,
    pub remaining_entry_basis: Lamports,
    pub wallet_native_cash_delta: SignedLamports,
    pub cash_result_delta: SignedLamports,
    pub accumulated_cash_result: SignedLamports,
    pub native_delta_sqlite: SettlementSqliteSignedValue,
    pub cash_result_delta_sqlite: SettlementSqliteSignedValue,
    pub accumulated_cash_result_sqlite: SettlementSqliteSignedValue,
    pub swap_price: Option<SettlementSwapPrice>,
    pub decomposition: ReceiptDecomposition,
}
