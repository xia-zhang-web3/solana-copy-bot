//! Exact100 stays outside the canonical native BUY lot and its owner EXIT.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{OwnerExitIntent, OwnerNativeRecoveryOriginal, SqliteStore};

pub(crate) fn original(c: &ExecutionConfig) -> Result<Option<OwnerNativeRecoveryOriginal>> {
    let Some(p) = c.owner_exit.as_ref().and_then(|p| p.native_recovery.as_ref()) else {
        return Ok(None);
    };
    ensure!(copybot_config::owner_native_recovery_exit(c), "owner_native_recovery_policy_required");
    let exit = c.owner_exit.as_ref().expect("recovery EXIT");
    let b = &p.protected_bot_usdc_baseline;
    use crate::execution_pumpswap_accounts::{associated_token_address, parse_pubkey, token_program_id};
    let wallet = parse_pubkey(&exit.wallet_pubkey, "recovery_baseline_wallet")?;
    let mint = parse_pubkey(&exit.mint, "recovery_baseline_mint")?;
    ensure!(bs58::encode(associated_token_address(&wallet, &mint, &token_program_id())).into_string()
        == b.token_account, "owner_native_recovery_baseline_account");
    Ok(Some(OwnerNativeRecoveryOriginal {
        activated_at: DateTime::parse_from_rfc3339(&p.original_activated_at)?.with_timezone(&Utc),
        expires_at: DateTime::parse_from_rfc3339(&p.original_expires_at)?.with_timezone(&Utc),
        policy_identity: p.original_policy_identity.clone(),
    }))
}

pub(crate) fn wallet_amount(c: &ExecutionConfig, i: &OwnerExitIntent, held: u64) -> Result<()> {
    let baseline = if original(c)?.is_some() { 100 } else { 0 };
    ensure!(held == i.amount_raw.checked_add(baseline)
        .context("owner_native_recovery_wallet_overflow")?, "owner_exit_wallet_amount_changed");
    Ok(())
}

/// Called after simulation and before the existing signing/key-loader boundary.
/// The same fresh check is already reused by the ordinary pre-submit boundary.
pub(crate) async fn before_sign(
    c: &ExecutionConfig, store: &SqliteStore, i: &OwnerExitIntent, tick: DateTime<Utc>,
) -> Result<()> {
    if original(c)?.is_none() { return Ok(()); }
    let check = || crate::execution_owner_exit_authority::current(c, store, i,
        crate::execution_canary_safety::risk_clock::decision_time(tick)
            .context("owner_exit_clock")?);
    let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never()).build()?;
    verify_balance(&http, c, store, i, &check).await
}

/// Fresh protected balance must come from at least the canonical BUY receipt slot.
pub(crate) async fn verify_balance(
    http: &reqwest::Client, c: &ExecutionConfig, store: &SqliteStore,
    i: &OwnerExitIntent, check: &impl Fn() -> Result<()>,
) -> Result<()> {
    let slot=crate::execution_owner_exit_quote::verify_balance(http,c,i,check).await?;
    if original(c)?.is_some() {
        let buy=store.load_execution_canary_receipt_facts(&i.buy_order_id)?
            .context("owner_native_recovery_buy_facts_missing")?;
        ensure!(slot>=buy.slot,"owner_native_recovery_wallet_stale");
    }
    Ok(())
}
