//! Focused four-role custody; quote, confirmation and AMM execution are explicit LOCAL mocks.
use super::{
    protected_usdc_fixture as old, receipt_lot_custody_fixture as f, wsol_lifecycle_fixture as w,
};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
#[tokio::test]
async fn wsol_lifecycle_bot_native_buy_sell_actual_producer_receipt_and_atomicity() -> Result<()> {
    let mut c = f::Case::wsol().await?;
    let source = c.source_dispatch()?;
    c.native_sell().await?;
    c.source_confirm(&source).await?;
    let claim = c.bot.claim()?;
    let claim = c.bind(claim, 100_000_000).await?;
    // Quote and durable handoff use the existing actual SQLite path.
    let (p, request) = super::protected_usdc_prepare::prepare(&c.bot, &claim)?;
    let cfg = &c.bot.config;
    let wallet =
        crate::execution_pumpswap_accounts::parse_pubkey(&request.wallet_pubkey, "LOCALbot")?;
    let mut ix = super::owner_exit_test_fixture::instructions(wallet)?;
    ix.iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap()
        .data[16..24]
        .copy_from_slice(&old::H.to_le_bytes());
    let payload = w::build(cfg, &request, &ix)?;
    crate::execution_protected_token_baseline::wire(cfg, &request, &payload, old::H)?;
    let receipt = w::exercise("bot-sell", cfg, &request, &payload)?;
    let mut missing = request.clone();
    missing.metadata.rpc_owned_sell = None;
    missing.metadata.transaction_owned_wsol = None;
    assert!(
        crate::execution_protected_token_baseline::wire(cfg, &missing, &payload, old::H).is_err()
    );
    // A new Metis blockhash changes bytes, but never derives a fresh account after UNKNOWN.
    let before = crate::execution_wsol_lifecycle::address(&request)?;
    let mut changed = request.clone();
    changed.attempt = 2;
    assert_eq!(crate::execution_wsol_lifecycle::address(&changed)?, before);
    let mut cfg_missing = cfg.clone();
    cfg_missing
        .controlled_receipt_lot
        .as_mut()
        .unwrap()
        .wsol_lifecycle
        .clear();
    assert!(copybot_config::validate_controlled_receipt_lot(&cfg_missing).is_err());
    let (mut dispatch, budget) = super::protected_usdc_prepare::dispatch(&p);
    assert_eq!(payload, p.payload);
    let prepared = p;
    let mut budget = budget;
    dispatch.message_sha256 = prepared.message_sha256.clone();
    budget.message_sha256 = prepared.message_sha256.clone();
    w::snapshot("bot-sell", &c.bot.db.sql)?;
    assert_eq!(
        c.bot
            .db
            .store
            .claim_owned_sell_dispatch(&prepared, &dispatch, &budget, || Ok(Utc::now()))?,
        copybot_storage_core::ExecutionDispatchClaim::New
    );
    assert_eq!(
        c.bot
            .db
            .store
            .claim_owned_sell_dispatch(&prepared, &dispatch, &budget, || Ok(Utc::now()))?,
        copybot_storage_core::ExecutionDispatchClaim::Existing
    );
    w::receipt_checks(&c.bot.db.store, &request, &receipt)?;
    super::wsol_lifecycle_failed_fixture::exercise(&c.bot.db.sql, &request, &receipt).await?;
    assert_eq!(
        old::confirm(
            &c.bot.db.store,
            &dispatch.order_id,
            &request.wallet_pubkey,
            receipt.clone()
        )
        .await?
        .sell_closed,
        1
    );
    assert_eq!(
        old::confirm(
            &c.bot.db.store,
            &dispatch.order_id,
            &request.wallet_pubkey,
            receipt
        )
        .await?
        .reason
        .as_deref(),
        Some("receipt_already_accounted")
    );
    assert!(c
        .bot
        .db
        .store
        .load_execution_canary_open_position(old::USDC)?
        .is_none());
    if let Ok(root) = std::env::var("WSOL_LIFECYCLE_FIXTURE_DIR") {
        c.bot.db.sql.execute(
            "VACUUM INTO ?1",
            [std::path::Path::new(&root)
                .join("bot-closed.db")
                .to_string_lossy()
                .as_ref()],
        )?;
        std::fs::write(
            std::path::Path::new(&root).join("decision.json"),
            serde_json::to_vec_pretty(claim.binding.fractional.as_ref().unwrap())?,
        )?;
        std::fs::write(
            std::path::Path::new(&root).join("manifest.json"),
            serde_json::to_vec_pretty(
                &json!({"kind":"LOCAL_CHANGED_WS0L_LIFECYCLE_ONLY","external_requests":0,"signatures_created":0,"canonical_provider":"MOCK closed Jupiter/Raydium profile","artifact":"NONE","native_receipt_lot":"accepted unchanged receipts/event/N-D path reused","oracle":"interprets produced System/Token instructions, modeled verified ExactIn AMM; not full VM"}),
            )?,
        )?;
    }
    Ok(())
}
