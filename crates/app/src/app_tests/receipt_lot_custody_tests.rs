//! Affected custody path only; no live quote/provider/signing evidence.
use super::{
    association_parent_fixture as parent, protected_usdc_fixture as old,
    receipt_lot_custody_fixture as f,
};
use anyhow::Result;
use copybot_storage_core::{controlled_receipt_lot::*, SqliteStore};
use serde_json::json;
#[tokio::test]
async fn receipt_lot_custody_native_event_receipts_sqlite_and_quote() -> Result<()> {
    let mut c = f::Case::new().await?;
    let source = c.source_dispatch()?;
    c.native_sell().await?;
    let claim = c.bot.claim()?;
    // Genuine dispatch is already durable; receipt may arrive AFTER native admission.
    assert!(SqliteStore::controlled_source_receipts(&c.scope, f::SELL_SIG, 150)?.is_none());
    c.source_confirm(&source).await?;
    let claim = c.bind(claim, 100_000_000).await?;
    let d = claim.binding.fractional.as_ref().unwrap();
    assert_eq!(
        (d.inventory.numerator, d.inventory.denominator.as_str()),
        (10_000, "10000")
    );
    assert_eq!(
        (d.owned_raw, d.selected_raw, claim.binding.raw),
        (old::H, old::H, old::H)
    );
    assert!(d.protected_execution_baseline.is_none());
    assert_eq!(d.controlled_receipt.as_ref().unwrap().bot_owned_raw, old::H);
    assert!(c.bot.db.store.recheck_strict_sell_quote(
        &claim,
        parent::limits(),
        chrono::Utc::now()
    )?);
    c.export(&claim)?;
    // Mock quote endpoint reaches the real quote binder. There are no keys or send.
    let (prepared, request) = super::protected_usdc_prepare::prepare(&c.bot, &claim)?;
    assert_eq!(request.side, "sell");
    boundary(&c, &request, &prepared.payload, 100_000_000, false).await?;
    assert!(boundary(&c, &request, &prepared.payload, old::H - 1, false)
        .await
        .is_err());
    assert!(boundary(
        &c,
        &request,
        &super::protected_usdc_prepare::payload(&c.bot.config, old::H + 1)?,
        100_000_000,
        false
    )
    .await
    .is_err());
    assert!(boundary(&c, &request, &prepared.payload, 100_000_000, true)
        .await
        .unwrap_err()
        .to_string()
        .contains("owned_wsol_temporary_occupied"));
    let (dispatch, budget) = super::protected_usdc_prepare::dispatch(&prepared);
    assert_eq!(
        c.bot
            .db
            .store
            .claim_owned_sell_dispatch(&prepared, &dispatch, &budget, || Ok(chrono::Utc::now()))?,
        copybot_storage_core::ExecutionDispatchClaim::New
    );
    assert_eq!(
        c.bot
            .db
            .store
            .claim_owned_sell_dispatch(&prepared, &dispatch, &budget, || Ok(chrono::Utc::now()))?,
        copybot_storage_core::ExecutionDispatchClaim::Existing
    );
    let sparse_receipt = old::receipt(
        &request.wallet_pubkey,
        &dispatch.tx_signature,
        "sell",
        100_000_000,
        100_000_000 - old::H,
        151,
    );
    // A balance-only mock cannot establish the newly required immutable WSOL message.
    let pending = old::confirm(
        &c.bot.db.store,
        &dispatch.order_id,
        &request.wallet_pubkey,
        sparse_receipt,
    )
    .await?;
    assert_eq!(
        pending.reason.as_deref(),
        Some("owned_wsol_receipt_instructions")
    );
    assert_eq!(pending.pending, 1);
    assert!(!c
        .bot
        .db
        .store
        .execution_canary_fill_exists(&dispatch.order_id)?);
    let proof = c
        .bot
        .db
        .store
        .load_execution_canary_receipt_proof(&dispatch.order_id)?
        .unwrap();
    assert_eq!(proof.reason, "owned_wsol_receipt_instructions");
    assert_eq!(
        c.bot
            .db
            .store
            .load_execution_canary_open_position(old::USDC)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        old::H
    );
    if let Ok(root) = std::env::var("RECEIPT_LOT_CUSTODY_FIXTURE_DIR") {
        std::fs::write(
            std::path::Path::new(&root).join("sparse-receipt-refusal.json"),
            serde_json::to_vec_pretty(
                &json!({"kind":"LOCAL_EXPECTED_REFUSAL","reason":proof.reason,"pending":pending.pending,"order":dispatch.order_id,"fill":false,"lot_still_open_raw":old::H,"message":"balance-only prior fixture; no instructions/blockhash"}),
            )?,
        )?;
    }
    // Interpret the actual daemon-prepared message; only the LOCAL chain/AMM is modeled.
    let wallet =
        crate::execution_pumpswap_accounts::parse_pubkey(&request.wallet_pubkey, "LOCALreceipt")?;
    let mut bank = super::wsol_lifecycle_oracle::Bank::new(wallet)?;
    let mut receipt = bank.execute(&prepared.payload)?;
    receipt["result"]["transaction"]["signatures"] = json!([dispatch.tx_signature]);
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
    assert!(c
        .bot
        .db
        .store
        .load_execution_canary_open_position(old::USDC)?
        .is_none());
    let facts = c
        .bot
        .db
        .store
        .load_execution_canary_receipt_facts(&dispatch.order_id)?
        .unwrap();
    assert_eq!(facts.token_delta.unwrap().raw, -i128::from(old::H));
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
    c.export_closed(&dispatch.order_id)?;
    Ok(())
}
#[tokio::test]
async fn receipt_lot_custody_canonical_coverage_and_no_fixed_inventory() -> Result<()> {
    let c = f::Case::new().await?;
    let ata = crate::execution_controlled_receipt_lot::canonical(&c.bot.config)?;
    for held in [old::H, old::H + 100, 50_000_000] {
        copybot_storage_core::ordered_sell_quote::fractional::controlled::available(
            &c.accounts(held)?,
            &ata,
            &c.scope.bot_wallet,
            old::H,
            150,
        )?;
    }
    assert!(
        copybot_storage_core::ordered_sell_quote::fractional::controlled::available(
            &c.accounts(old::H - 1)?,
            &ata,
            &c.scope.bot_wallet,
            old::H,
            150
        )
        .is_err()
    );
    let mut foreign = c.accounts(50_000_000)?;
    foreign["value"][0]["pubkey"] = json!("noncanonical");
    assert!(
        copybot_storage_core::ordered_sell_quote::fractional::controlled::available(
            &foreign,
            &ata,
            &c.scope.bot_wallet,
            old::H,
            150
        )
        .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn receipt_lot_custody_marker_is_immutable_and_restart_exact() -> Result<()> {
    let c = f::Case::new().await?;
    let db = SqliteStore::open(&c.source_path)?;
    db.register_controlled_receipt_lot_scope(&c.scope, f::SOURCE)?;
    let before = db.controlled_receipt_lot_scope()?.unwrap();
    let mut changed = c.scope.clone();
    changed.bot_run_id.push_str("-foreign");
    assert!(db
        .register_controlled_receipt_lot_scope(&changed, f::SOURCE)
        .is_err());
    assert_eq!(db.controlled_receipt_lot_scope()?.unwrap(), before);
    assert_eq!(
        SqliteStore::controlled_source_buy(&c.scope)?
            .unwrap()
            .amount_raw,
        10_000
    );
    Ok(())
}

async fn boundary(
    c: &f::Case,
    r: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    payload: &str,
    held: u64,
    wsol: bool,
) -> Result<()> {
    use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
    let accounts = c.accounts(held)?;
    let rpc = Parsed(move |q: serde_json::Value| {
        std::future::ready(Ok(
            json!({"jsonrpc":"2.0","id":q["id"],"result":match q["method"].as_str().unwrap(){"getTokenAccountsByOwner"=>accounts.clone(),"getAccountInfo"=>json!({"context":{"slot":151},"value":if wsol {json!({"unrelated":"wsol must not be closed"})}else{json!(null)}}),_=>panic!("unscoped mock method")}}),
        ))
    });
    crate::execution_protected_token_baseline::before_financial_boundary(
        &c.bot.config,
        &c.bot.db.store,
        r,
        payload,
        rpc,
    )
    .await
}
#[tokio::test]
async fn receipt_lot_custody_partial_source_exit_refuses_before_quote() -> Result<()> {
    let c = f::Case::new().await?;
    let error = c.source_dispatch_amount(5_000).unwrap_err();
    assert!(format!("{error:#}").contains("owner_exit_buy_position_binding_changed"));
    let source_orders: i64 = rusqlite::Connection::open(&c.source_path)?.query_row(
        "SELECT count(*) FROM orders WHERE signal_id LIKE 'owner-exit:%'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(source_orders, 0);
    let decisions: i64 =
        c.bot
            .db
            .sql
            .query_row("SELECT count(*) FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
    assert_eq!(decisions, 0);
    assert_eq!(
        c.source
            .load_execution_canary_open_position(old::USDC)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        10_000
    );
    Ok(())
}
#[tokio::test]
async fn receipt_lot_custody_native_foreign_events_cannot_consume_budget_or_sell() -> Result<()> {
    let c = f::Case::new().await?;
    let foreign = bs58::encode([208; 64]).into_string();
    let identity: i64 = c.bot.db.sql.query_row(
        "SELECT count(*) FROM association_inbox_identities WHERE signature=?1",
        [&foreign],
        |r| r.get(0),
    )?;
    assert_eq!(identity, 1);
    let decisions: i64 = c.bot.db.sql.query_row(
        "SELECT count(*) FROM native_buy_cohort_decisions WHERE signature=?1",
        [&foreign],
        |r| r.get(0),
    )?;
    assert_eq!(decisions, 0);
    let valid: i64 = c.bot.db.sql.query_row(
        "SELECT count(*) FROM native_buy_cohort_decisions WHERE signature=?1",
        [f::BUY_SIG],
        |r| r.get(0),
    )?;
    assert_eq!(valid, 1);
    c.source_dispatch()?;
    c.native_sell().await?;
    let foreign_sell = bs58::encode([209; 64]).into_string();
    let foreign_sells: i64 = c.bot.db.sql.query_row(
        "SELECT count(*) FROM ordered_source_sell_intents WHERE signature=?1",
        [&foreign_sell],
        |r| r.get(0),
    )?;
    assert_eq!(foreign_sells, 0);
    Ok(())
}
#[tokio::test]
async fn receipt_lot_custody_corrupt_proof_rolls_back_and_cannot_requote() -> Result<()> {
    let mut c = f::Case::new().await?;
    let id = c.source_dispatch()?;
    c.native_sell().await?;
    c.source_confirm(&id).await?;
    let q = c.bot.claim()?;
    let q = c.bind(q, old::H).await?;
    let saved: String = c.bot.db.sql.query_row(
        "SELECT decision FROM fractional_sell_decisions WHERE intent_id=?1",
        [&q.intent_id],
        |r| r.get(0),
    )?;
    let mut bad: serde_json::Value = serde_json::from_str(&saved)?;
    bad["controlled_receipt"]["source_sold_raw"] = json!(10_001);
    c.bot.db.sql.execute(
        "UPDATE fractional_sell_decisions SET decision=?1 WHERE intent_id=?2",
        rusqlite::params![bad.to_string(), q.intent_id],
    )?;
    assert!(c
        .bot
        .db
        .store
        .recheck_strict_sell_quote(&q, parent::limits(), chrono::Utc::now())
        .is_err());
    c.bot.db.sql.execute(
        "UPDATE fractional_sell_decisions SET decision=?1 WHERE intent_id=?2",
        rusqlite::params![saved, q.intent_id],
    )?;
    assert!(c
        .bot
        .db
        .store
        .recheck_strict_sell_quote(&q, parent::limits(), chrono::Utc::now())?);
    assert!(c
        .bot
        .db
        .store
        .begin_fractional_sell(
            &q,
            parent::limits(),
            chrono::Utc::now(),
            &crate::execution_owned_sell_rpc::identity(&c.bot.config)?
        )
        .is_err());
    Ok(())
}
