use crate::{
    execution_canary_fill_marker::fill_exists, receipt_facts_identity::validate_identity,
    receipt_facts_rows, sell_settlement_position, ExecutionCanarySellSettlement,
    ExecutionCanarySellSettlementPlan, ReceiptDecomposition, ReceiptFactsIdentityRejection,
    SellSettlementUnsupported as Unsupported, SettlementSqliteSignedValue, SqliteDiscoveryStore,
    EXECUTION_CANARY_POSITION_STATE_CLOSED, EXECUTION_CANARY_POSITION_STATE_OPEN,
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use anyhow::{ensure, Context, Result};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use rusqlite::{Connection, OptionalExtension};

impl SqliteDiscoveryStore {
    /// Computes cash-basis allocation in one DEFERRED read transaction. No schema
    /// creation, inventory writes, fill marker, completion or risk release occurs.
    /// Unsupported is a deliberate domain rejection; SQL/corrupt data remain Err.
    /// See ExecutionCanarySellSettlementPlan for the mandatory future apply contract.
    pub fn plan_execution_canary_sell_settlement(
        &self,
        order_id: &str,
    ) -> Result<ExecutionCanarySellSettlement> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("begin SELL settlement read snapshot")?;
        let result = match plan(&tx, order_id) {
            Ok(plan) => ExecutionCanarySellSettlement::Ready(plan),
            Err(error) => {
                if let Some(reason) = error.downcast_ref::<Unsupported>() {
                    ExecutionCanarySellSettlement::Unsupported(*reason)
                } else if let Some(reason) = error.downcast_ref::<ReceiptFactsIdentityRejection>() {
                    ExecutionCanarySellSettlement::Unsupported(Unsupported::DurableIdentity(
                        *reason,
                    ))
                } else {
                    return Err(error);
                }
            }
        };
        tx.commit()
            .context("finish SELL settlement read snapshot")?;
        Ok(result)
    }
}

pub(crate) fn plan(conn: &Connection, order_id: &str) -> Result<ExecutionCanarySellSettlementPlan> {
    let status = conn
        .query_row(
            "SELECT status FROM orders WHERE order_id = ?1",
            [order_id],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .ok_or(Unsupported::MissingOrder)?;
    ensure!(!fill_exists(conn, order_id)?, Unsupported::AlreadyAccounted);
    ensure!(
        status == EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
        Unsupported::OrderNotPending
    );
    let receipt =
        receipt_facts_rows::load(conn, order_id)?.ok_or(Unsupported::MissingReceiptFacts)?;
    validate_identity(conn, &receipt)?;
    ensure!(receipt.side == "sell", Unsupported::NotSell);
    let delta = receipt
        .token_delta
        .ok_or(Unsupported::UnresolvedTokenCoverage)?;
    ensure!(delta.raw < 0, Unsupported::NonNegativeTokenDelta);
    let sold = delta
        .raw
        .checked_neg()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or(Unsupported::TokenQuantityOutOfRange)?;
    let expected_position = sell_settlement_position::load(conn, &receipt.token)?;
    let old = expected_position.quantity;
    ensure!(
        old.decimals() == delta.decimals,
        Unsupported::DecimalsMismatch
    );
    ensure!(sold <= old.raw(), Unsupported::Oversell);
    let cost = expected_position.entry_basis.as_u64();
    let allocated = allocate(cost, sold, old.raw())?;
    let remaining = old
        .raw()
        .checked_sub(sold)
        .ok_or(Unsupported::ArithmeticOverflow)?;
    let remaining_basis = cost
        .checked_sub(allocated)
        .ok_or(Unsupported::ArithmeticOverflow)?;
    let wallet_native_cash_delta = receipt.wallet_native_delta;
    let cash_result_delta = wallet_native_cash_delta
        .checked_sub(SignedLamports::new(i128::from(allocated)))
        .ok_or(Unsupported::ArithmeticOverflow)?;
    let accumulated_cash_result = expected_position
        .accumulated_cash_result
        .checked_add(cash_result_delta)
        .ok_or(Unsupported::ArithmeticOverflow)?;
    Ok(ExecutionCanarySellSettlementPlan {
        receipt,
        expected_position,
        sold_quantity: TokenQuantity::new(sold, old.decimals()),
        remaining_quantity: TokenQuantity::new(remaining, old.decimals()),
        remaining_position_state: if remaining == 0 {
            EXECUTION_CANARY_POSITION_STATE_CLOSED
        } else {
            EXECUTION_CANARY_POSITION_STATE_OPEN
        }
        .into(),
        allocated_entry_basis: Lamports::new(allocated),
        remaining_entry_basis: Lamports::new(remaining_basis),
        wallet_native_cash_delta,
        cash_result_delta,
        accumulated_cash_result,
        native_delta_sqlite: SettlementSqliteSignedValue::from_exact(wallet_native_cash_delta),
        cash_result_delta_sqlite: SettlementSqliteSignedValue::from_exact(cash_result_delta),
        accumulated_cash_result_sqlite: SettlementSqliteSignedValue::from_exact(
            accumulated_cash_result,
        ),
        swap_price: None,
        decomposition: ReceiptDecomposition::Unresolved,
    })
}

fn allocate(cost: u64, sold: u64, old: u64) -> Result<u64> {
    if sold == old {
        return Ok(cost);
    }
    let product = i128::from(cost)
        .checked_mul(i128::from(sold))
        .ok_or(Unsupported::ArithmeticOverflow)?;
    let divisor = i128::from(old);
    let quotient = product
        .checked_div(divisor)
        .ok_or(Unsupported::ArithmeticOverflow)?;
    let remainder = product
        .checked_rem(divisor)
        .ok_or(Unsupported::ArithmeticOverflow)?;
    let allocated = quotient
        .checked_add(i128::from(remainder != 0))
        .ok_or(Unsupported::ArithmeticOverflow)?;
    Ok(u64::try_from(allocated).map_err(|_| Unsupported::ArithmeticOverflow)?)
}
