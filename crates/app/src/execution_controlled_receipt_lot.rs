//! Typed controlled scenario glue; no operator command can become a native event.
use anyhow::{ensure, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{controlled_receipt_lot::Scope, SqliteStore};
pub(crate) fn scope(c: &ExecutionConfig) -> Result<Option<Scope>> {
    copybot_config::validate_controlled_receipt_lot(c)?;
    c.controlled_receipt_lot
        .as_ref()
        .map(|p| {
            Ok(Scope {
                policy: p.policy.clone(),
                source_run_id: p.source_run_id.clone(),
                bot_run_id: p.bot_run_id.clone(),
                source_wallet: p.source_wallet.clone(),
                bot_wallet: p.bot_wallet.clone(),
                source_buy_intent_id: p.source_buy_intent_id.clone(),
                source_state_db_path: p.source_state_db_path.clone(),
            })
        })
        .transpose()
}
pub(crate) fn register(c: &ExecutionConfig, store: &SqliteStore) -> Result<()> {
    if let Some(p) = scope(c)? {
        store.register_controlled_receipt_lot_scope(&p, &c.canary_wallet_pubkey)?;
    }
    Ok(())
}
pub(crate) fn reserve(c: &ExecutionConfig) -> Result<u64> {
    if scope(c)?.is_some() {
        Ok(0)
    } else {
        crate::execution_native_floor_policy::reserve_lamports(c.pretrade_min_sol_reserve)
    }
}
pub(crate) fn source_buy(
    c: &ExecutionConfig,
    signature: &str,
    slot: u64,
    wallet: &str,
    mint: &str,
) -> Result<()> {
    let Some(p) = scope(c)? else { return Ok(()) };
    let buy = SqliteStore::controlled_source_buy(&p)?
        .ok_or_else(|| anyhow::anyhow!("receipt_lot_source_buy_pending"))?;
    ensure!(
        signature == buy.signature
            && slot == buy.slot
            && wallet == p.source_wallet
            && mint == copybot_storage_core::OWNER_EXIT_MINT,
        "receipt_lot_source_buy_event"
    );
    Ok(())
}
pub(crate) fn canonical(c: &ExecutionConfig) -> Result<String> {
    use crate::execution_pumpswap_accounts::*;
    let wallet = parse_pubkey(&c.canary_wallet_pubkey, "receipt_lot_wallet")?;
    let mint = parse_pubkey(copybot_storage_core::OWNER_EXIT_MINT, "receipt_lot_mint")?;
    Ok(bs58::encode(associated_token_address(
        &wallet,
        &mint,
        &token_program_id(),
    ))
    .into_string())
}
pub(crate) async fn before_boundary(
    c: &ExecutionConfig,
    store: &SqliteStore,
    r: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    payload: &str,
    mut io: impl crate::execution_owned_sell_rpc::fractional::transport::Transport,
) -> Result<()> {
    use crate::execution_owned_sell_prepare::submit::guard;
    use anyhow::Context;
    use copybot_storage_core::ordered_sell_quote::fractional::controlled::available;
    use serde_json::json;
    let scope = scope(c)?.ok_or_else(|| anyhow::anyhow!("receipt_lot_scope_missing"))?;
    let p = guard::request(store, r)?;
    guard::config(c, &p)?;
    let b = &p.handoff.snapshot.quote;
    let f = b
        .fractional
        .as_ref()
        .context("receipt_lot_fraction_missing")?;
    let proof = f
        .controlled_receipt
        .as_ref()
        .context("receipt_lot_proof_missing")?;
    ensure!(
        proof.bot_wallet == scope.bot_wallet
            && proof.source_wallet == scope.source_wallet
            && r.wallet_pubkey == scope.bot_wallet
            && b.raw == f.selected_raw
            && b.raw > 0
            && b.raw <= f.owned_raw
            && proof.bot_token_account == canonical(c)?,
        "receipt_lot_boundary_binding"
    );
    crate::execution_usdc_sell_wire::verify(r, payload, b.raw, false)?;
    let mut check = || {
        guard::config(c, &p)?;
        guard::live(r)
    };
    let request = json!({"jsonrpc":"2.0","id":"controlled-receipt-lot-inventory","method":"getTokenAccountsByOwner","params":[c.canary_wallet_pubkey,{"mint":b.mint},{"encoding":"jsonParsed","commitment":"finalized","minContextSlot":p.handoff.snapshot.sell.facts.slot}]});
    let response = io.read(request.clone(), &mut check).await?;
    check()?;
    ensure!(
        guard::request(store, r)? == p,
        "receipt_lot_prepared_changed"
    );
    ensure!(
        response["id"] == request["id"]
            && response["jsonrpc"] == "2.0"
            && response.get("error").is_none(),
        "receipt_lot_rpc_binding"
    );
    available(
        &response["result"],
        &proof.bot_token_account,
        &scope.bot_wallet,
        f.owned_raw,
        p.handoff.snapshot.sell.facts.slot,
    )?;
    let wsol = bs58::encode(crate::execution_wsol_lifecycle::address(r)?).into_string();
    let request = json!({"jsonrpc":"2.0","id":"controlled-receipt-lot-wsol","method":"getAccountInfo","params":[wsol,{"encoding":"base64","commitment":"finalized","minContextSlot":p.handoff.snapshot.sell.facts.slot}]});
    let response = io.read(request.clone(), &mut check).await?;
    check()?;
    ensure!(
        guard::request(store, r)? == p,
        "receipt_lot_prepared_changed"
    );
    ensure!(
        response["id"] == request["id"]
            && response["jsonrpc"] == "2.0"
            && response.get("error").is_none()
            && response["result"]["context"]["slot"]
                .as_u64()
                .is_some_and(|v| v >= p.handoff.snapshot.sell.facts.slot),
        "receipt_lot_wsol_rpc"
    );
    ensure!(
        response["result"]["value"].is_null(),
        "owned_wsol_temporary_occupied"
    );
    check()
}

#[path = "execution_controlled_exit_wire.rs"]
mod exit_wire;
pub(crate) use exit_wire::{stamp as stamp_exit, ExitWire};
