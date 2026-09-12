//! Ephemeral provenance of an insert performed by this ShadowService call.
//! No reconstruction from wallet/mint/time and no persistence or replay credit.
use anyhow::{ensure, Context, Result};
use copybot_core_types::{CopySignalRow, Lamports, SwapEvent, TokenQuantity};
use copybot_storage_core::{
    SqliteStore, POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER, SHADOW_RISK_CONTEXT_MARKET,
};

#[derive(Debug, Clone)]
pub struct RecordedBuyLot {
    signal: CopySignalRow,
    lot_id: i64,
    qty: f64,
    exact_qty: Option<TokenQuantity>,
    lot_cost: Lamports,
}
impl RecordedBuyLot {
    pub(crate) fn inserted(
        store: &SqliteStore,
        signal: &CopySignalRow,
        lot_id: i64,
        qty: f64,
        exact_qty: Option<TokenQuantity>,
    ) -> Result<Self> {
        let lot = store
            .list_shadow_lots(&signal.wallet_id, &signal.token)?
            .into_iter()
            .find(|lot| lot.id == lot_id)
            .context("inserted shadow BUY lot missing")?;
        let receipt = Self {
            signal: signal.clone(),
            lot_id,
            qty,
            exact_qty,
            lot_cost: lot
                .cost_lamports
                .context("inserted shadow BUY lot cost unknown")?,
        };
        receipt.verify_lot(store)?;
        Ok(receipt)
    }
    pub fn verify(&self, store: &SqliteStore, swap: &SwapEvent) -> Result<()> {
        let expected = &self.signal;
        ensure!(
            expected.signal_id
                == format!(
                    "shadow:{}:{}:buy:{}",
                    swap.signature, swap.wallet, swap.token_out
                )
                && expected.wallet_id == swap.wallet
                && expected.token == swap.token_out
                && expected.side == "buy"
                && expected.ts == swap.ts_utc,
            "shadow BUY receipt origin changed"
        );
        let signal = store
            .load_copy_signal_by_signal_id(&expected.signal_id)?
            .context("shadow BUY receipt signal missing")?;
        ensure!(
            signal.wallet_id == expected.wallet_id
                && signal.token == expected.token
                && signal.side == expected.side
                && signal.ts == expected.ts
                && signal.status == expected.status
                && signal.notional_origin == expected.notional_origin
                && signal.notional_lamports == expected.notional_lamports
                && signal.notional_sol.to_bits() == expected.notional_sol.to_bits(),
            "shadow BUY receipt signal changed"
        );
        ensure!(
            expected
                .notional_lamports
                .context("shadow BUY signal cost unknown")?
                .as_u64()
                > 0,
            "shadow BUY signal cost invalid"
        );
        self.verify_lot(store)
    }
    fn verify_lot(&self, store: &SqliteStore) -> Result<()> {
        let expected = &self.signal;
        let cost = self.lot_cost;
        let quantity = self
            .exact_qty
            .context("shadow BUY receipt quantity unknown")?;
        ensure!(
            cost.as_u64() > 0
                && quantity.raw() > 0
                && self.qty.is_finite()
                && self.qty > copybot_storage_core::SHADOW_LOT_OPEN_EPS,
            "shadow BUY receipt amounts invalid"
        );
        let lot = store
            .list_shadow_lots(&expected.wallet_id, &expected.token)?
            .into_iter()
            .find(|lot| lot.id == self.lot_id)
            .context("shadow BUY receipt lot missing")?;
        ensure!(
            lot.wallet_id == expected.wallet_id
                && lot.token == expected.token
                && lot.opened_ts == expected.ts
                && lot.qty_exact == Some(quantity)
                && lot.qty.to_bits() == self.qty.to_bits()
                && lot.cost_lamports == Some(cost)
                && lot.cost_sol.to_bits() == expected.notional_sol.to_bits()
                && lot.accounting_bucket == POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
                && lot.risk_context == SHADOW_RISK_CONTEXT_MARKET,
            "shadow BUY receipt lot changed or unknown"
        );
        Ok(())
    }
}
