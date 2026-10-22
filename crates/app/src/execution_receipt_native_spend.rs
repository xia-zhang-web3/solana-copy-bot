//! Controlled BUY bounded debit proof. An existing WSOL account is not our rent/output.
use super::ContextProof;
use crate::execution_native_rpc::rent_types::ClassicAtaFundingFacts;
use anyhow::{ensure, Result};
impl ContextProof {
    pub(crate) fn receipt_spend(
        &self,
        payload: &str,
        required: u128,
        facts: &ClassicAtaFundingFacts,
    ) -> Result<()> {
        ensure!(
            self.receipt_scoped()
                && required <= u128::from(copybot_storage_core::TINY_NATIVE_ALLOWANCE),
            "receipt_lot_native_spend_cap"
        );
        let mut bound = self.binding.request().clone();
        // BundleRequest deliberately omits wallet_id; its authenticated payer is retained.
        bound.wallet_id = bound.wallet_pubkey.clone();
        bound.metadata.protected_capital = Some(Box::new(self.clone()));
        let request = &bound;
        if request.signal_id.starts_with("owner-buy:") {
            crate::execution_owner_buy_wire::verify(request, payload)?;
        } else {
            crate::execution_owner_buy_wire::verify_cohort(request, payload)?;
        }
        crate::execution_wsol_lifecycle::check_initial(request, payload, facts)?;
        Ok(())
    }
}
