//! Explicit shared-wallet controlled scenario. No wallet inventory becomes a lot.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
pub const TRANSACTION_OWNED_WSOL_V1: &str = "transaction_owned_wsol_create_with_seed_v1";
pub const CONTROLLED_RECEIPT_LOT_V1: &str = "controlled_receipt_lot_custody_v1";
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlledReceiptLotConfig {
    pub policy: String,
    pub source_run_id: String,
    pub bot_run_id: String,
    pub source_wallet: String,
    pub bot_wallet: String,
    pub source_buy_intent_id: String,
    pub source_state_db_path: String,
    pub wsol_lifecycle: String,
}
pub fn validate_controlled_receipt_lot(e: &crate::ExecutionConfig) -> Result<()> {
    let Some(p) = &e.controlled_receipt_lot else {
        return Ok(());
    };
    ensure!(
        p.policy == CONTROLLED_RECEIPT_LOT_V1
            && p.wsol_lifecycle == TRANSACTION_OWNED_WSOL_V1
            && p.source_wallet != p.bot_wallet
            && p.source_run_id != p.bot_run_id,
        "receipt_lot_scope_identity"
    );
    for id in [&p.source_run_id, &p.bot_run_id, &p.source_buy_intent_id] {
        ensure!(
            (1..=110).contains(&id.len())
                && id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c)),
            "receipt_lot_scope_id"
        );
    }
    for wallet in [&p.source_wallet, &p.bot_wallet] {
        ensure!(
            (32..=44).contains(&wallet.len()) && wallet.bytes().all(|c| c.is_ascii_alphanumeric()),
            "receipt_lot_scope_wallet"
        );
    }
    ensure!(
        std::path::Path::new(&p.source_state_db_path).is_absolute()
            && !p.source_state_db_path.contains('\0'),
        "receipt_lot_source_path"
    );
    ensure!(
        !e.enabled
            && e.pretrade_min_sol_reserve == 0.0
            && e.tiny_experiment.policy_mode == crate::TinyPolicyMode::ProtectedNativeCapital
            && e.canary_buy_size_sol == 0.01
            && e.quote_canary_buy_size_sol == 0.01
            && e.max_submit_attempts == 1
            && e.canary_max_open_positions == 1
            && e.canary_max_daily_loss_sol <= 0.02
            && e.pretrade_max_priority_fee_lamports <= 50_000
            && e.quote_canary_buy_slippage_bps <= 50
            && e.quote_canary_sell_slippage_bps <= 50,
        "receipt_lot_financial_caps"
    );
    if e.canary_wallet_pubkey == p.source_wallet {
        let b = e
            .owner_technical_buy
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("receipt_lot_source_owner"))?;
        ensure!(
            b.policy == crate::OWNER_EXECUTION_ROUNDTRIP_V1
                && b.run_id == p.source_run_id
                && b.intent_id == p.source_buy_intent_id
                && b.min_reserve_lamports == 0
                && b.source_copy_exit_gate.as_ref().is_some_and(
                    |g| g.bot_run_id == p.bot_run_id && g.bot_wallet_pubkey == p.bot_wallet
                )
                && e.technical_cohort.is_none()
                && e.owned_sell_preparation.is_none(),
            "receipt_lot_source_authority"
        );
    } else {
        let s = e
            .owned_sell_preparation
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("receipt_lot_bot_sell"))?;
        ensure!(
            e.canary_wallet_pubkey == p.bot_wallet
                && e.owner_technical_buy.is_none()
                && e.technical_cohort.as_ref().is_some_and(
                    |c| c.run_id == p.bot_run_id && c.wallet_ids == [p.source_wallet.clone()]
                )
                && s.fractional_inventory.as_deref() == Some(CONTROLLED_RECEIPT_LOT_V1)
                && s.protected_bot_usdc_baseline.is_none(),
            "receipt_lot_bot_authority"
        );
    }
    Ok(())
}
