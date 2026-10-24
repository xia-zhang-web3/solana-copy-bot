//! Optional new-cycle BUY prerequisite; this grants no exit or BUY authority.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const PRIOR_BOT_RECEIPT_EXIT_V1: &str = "prior_bot_receipt_exit_closed_v1";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PriorBotReceiptExitConfig {
    pub policy: String,
    pub state_db_path: String,
    pub exit_intent_id: String,
    pub buy_order_id: String,
    pub buy_receipt_signature: String,
    pub position_id: String,
    pub wallet_pubkey: String,
    pub mint: String,
    pub amount_raw: u64,
    pub decimals: u8,
}

pub fn validate_prior_bot_receipt_exit(c: &crate::ExecutionConfig) -> Result<()> {
    let Some(p) = &c.prior_bot_receipt_exit else {
        return Ok(());
    };
    ensure!(
        p.policy == PRIOR_BOT_RECEIPT_EXIT_V1,
        "prior_bot_exit_policy"
    );
    ensure!(
        std::path::Path::new(&p.state_db_path).is_absolute()
            && p.state_db_path.trim() == p.state_db_path
            && !p.state_db_path.contains('\0'),
        "prior_bot_exit_path"
    );
    for value in [
        &p.exit_intent_id,
        &p.buy_order_id,
        &p.buy_receipt_signature,
        &p.position_id,
        &p.wallet_pubkey,
        &p.mint,
    ] {
        ensure!(
            (1..=512).contains(&value.len()) && value.trim() == value && !value.contains('\0'),
            "prior_bot_exit_identity"
        );
    }
    ensure!(p.exit_intent_id.len() <= 128, "prior_bot_exit_identity");
    ensure!(
        p.buy_order_id
            .strip_prefix("exec-canary:native-buy-v1:")
            .is_some_and(|signature| !signature.is_empty()
                && p.exit_intent_id == format!("bot-receipt-exit:{signature}"))
            && p.position_id == format!("exec-canary-pos:{}", p.buy_order_id),
        "prior_bot_exit_native_buy"
    );
    ensure!(
        p.mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            && p.decimals == 6
            && p.amount_raw > 0
            && i64::try_from(p.amount_raw).is_ok(),
        "prior_bot_exit_quantity"
    );
    Ok(())
}
