use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};

/// An actual fill with receipt-native cash semantics, never an economic swap price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCanaryCashSettlement {
    pub order_id: String,
    pub position_id: String,
    pub token: String,
    pub sold_quantity: TokenQuantity,
    pub remaining_quantity: TokenQuantity,
    pub wallet_native_cash_delta: SignedLamports,
    pub allocated_entry_basis: Lamports,
    pub remaining_entry_basis: Lamports,
    pub cash_result_delta: SignedLamports,
    pub accumulated_cash_result: SignedLamports,
    pub swap_price: Option<crate::SettlementSwapPrice>,
    pub decomposition: crate::ReceiptDecomposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCanaryCashSettlementResult {
    pub already_accounted: bool,
    pub settlement: ExecutionCanaryCashSettlement,
}
