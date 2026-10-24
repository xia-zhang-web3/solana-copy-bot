use super::OwnerBotReceiptExitProof;
use crate::{NativeObservation, ObservationCoverage, OwnerExitIntent};
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;

fn known(v: &NativeObservation) -> Result<&str> {
    ensure!(v.coverage == ObservationCoverage::Known, "bot_receipt_exit_observation_unknown");
    v.value.as_deref().context("bot_receipt_exit_observation_missing")
}
fn raw(v: &NativeObservation) -> Result<u64> {
    let text = known(v)?;
    let value = text.parse::<u64>()?;
    ensure!(value.to_string() == text, "bot_receipt_exit_raw_domain");
    Ok(value)
}

pub(super) fn proof(c: &Connection, i: &OwnerExitIntent, slot: u64, remaining: u64)
    -> Result<OwnerBotReceiptExitProof>
{
    let (observations, conflict) = crate::native_observations::storage::load(c, &i.buy_order_id)?
        .context("bot_receipt_exit_observations_missing")?;
    ensure!(conflict.is_none() && observations.order_id == i.buy_order_id
        && observations.tx_signature == i.buy_receipt_signature
        && observations.wallet_pubkey == i.wallet && observations.token == i.mint
        && observations.side == "buy" && observations.slot == slot.to_string(),
        "bot_receipt_exit_observations_identity");
    // Aggregate coverage includes unrelated accounts and instructions. This proof
    // requires the exact target's known endpoints, not complete cash decomposition.
    let mut target = None;
    for account in &observations.accounts {
        let endpoints = [&account.pre_token, &account.post_token];
        if !endpoints.iter().any(|e| e.mint.value.as_deref() == Some(i.mint.as_str())
            && e.token_owner.value.as_deref() == Some(i.wallet.as_str())) { continue; }
        ensure!(target.is_none(), "bot_receipt_exit_target_ambiguous");
        for e in endpoints {
            ensure!(known(&e.mint)? == i.mint && known(&e.token_owner)? == i.wallet
                && known(&e.token_program)? == crate::native_buy::SPL_TOKEN_PROGRAM
                && known(&e.decimals)? == "6", "bot_receipt_exit_target_identity");
        }
        let personal = raw(&account.pre_token.raw)?;
        let total = raw(&account.post_token.raw)?;
        ensure!(total.checked_sub(personal) == Some(i.amount_raw) && !account.pubkey.is_empty(),
            "bot_receipt_exit_target_amount");
        target = Some(OwnerBotReceiptExitProof { personal_raw: personal,
            expected_wallet_total_raw: total, native_remaining: remaining,
            token_account: account.pubkey.clone(), buy_slot: slot });
    }
    target.context("bot_receipt_exit_target_missing")
}
