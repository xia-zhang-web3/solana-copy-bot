//! A separate one-shot SELL window never renews the native BOT BUY authority.
use crate::{ExecutionConfig, OwnerExitConfig};
use anyhow::{ensure, Context, Result};

pub const OWNER_BOT_RECEIPT_EXIT_V1: &str = "owner_bot_receipt_exit_v1";

pub(crate) fn validate(e: &ExecutionConfig, p: &OwnerExitConfig) -> Result<()> {
    let scope = e
        .controlled_receipt_lot
        .as_ref()
        .context("bot_receipt_exit_scope")?;
    let cohort = e
        .technical_cohort
        .as_ref()
        .context("bot_receipt_exit_cohort")?;
    let suffix = p
        .buy_order_id
        .strip_prefix("exec-canary:native-buy-v1:")
        .filter(|s| !s.is_empty())
        .context("bot_receipt_exit_native_origin")?;
    ensure!(
        p.intent_id == format!("bot-receipt-exit:{suffix}")
            && p.position_id == format!("exec-canary-pos:{}", p.buy_order_id)
            && p.run_id == scope.bot_run_id
            && p.run_id == cohort.run_id
            && p.wallet_pubkey == scope.bot_wallet
            && p.signer_pubkey == scope.bot_wallet
            && p.amount_raw > 0
            && p.decimals == 6,
        "bot_receipt_exit_parent_identity"
    );
    ensure!(
        !cohort.activate
            && !e.tiny_experiment.activate
            && e.tiny_experiment.id.as_deref() == Some(p.run_id.as_str())
            && e.owner_technical_buy.is_none()
            && e.native_fresh_buy.is_none()
            && !e.canary_entry_submit_enabled,
        "bot_receipt_exit_buy_disabled"
    );
    ensure!(!p.activated_at.is_empty() && !p.expires_at.is_empty()
        && !cohort.deadline.is_empty(), "bot_receipt_exit_separate_window");
    ensure!(
        p.min_reserve_lamports == 0
            && p.max_priority_fee_lamports <= 22_000
            && p.max_slippage_bps <= 50
            && p.max_daily_loss_lamports <= 20_000_000,
        "bot_receipt_exit_limits"
    );
    Ok(())
}
