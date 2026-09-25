use super::{
    fractional_financial_fixture as money, fractional_fixture::Fixture, fractional_tests as f,
};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::*;
#[tokio::test]
async fn fractional_native_unsigned_hold_dispatch_receipt_250_remaining_750() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let before =
        f.db.store
            .load_execution_canary_open_position(&claim.binding.mint)?
            .unwrap();
    let p = money::prepare(&f, &claim)?;
    assert_eq!(p.handoff.snapshot.quote.raw, 250);
    f.db.store.recheck_owned_sell_prepared(&p, Utc::now())?;
    let d = money::dispatch(&f, &p)?;
    let facts = money::receipt(&d, 250);
    f.db.store.mark_execution_canary_confirmed_unreconciled(
        &d.order_id,
        &ExecutionCanaryReceiptProof {
            tx_signature: d.tx_signature.clone(),
            wallet_pubkey: d.wallet.clone(),
            token: d.token.clone(),
            side: "sell".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(151),
            confirmed_at: Utc::now(),
            reason: "mocked canonical receipt".into(),
        },
        Utc::now(),
    )?;
    f.db.store
        .record_execution_canary_receipt_facts(&facts, Utc::now())?;
    f.db.store
        .apply_execution_canary_sell_settlement(&facts, Utc::now())?;
    let reopened = SqliteStore::open(&f.db.path)?;
    reopened.apply_execution_canary_sell_settlement(&facts, Utc::now())?;
    let after = reopened
        .load_execution_canary_open_position(&d.token)?
        .unwrap();
    assert_eq!(after.qty_exact.unwrap().raw(), 750);
    let basis = before.cost_lamports.unwrap().as_u64();
    let allocated = (basis * 250).div_ceil(1000);
    assert_eq!(after.cost_lamports.unwrap().as_u64(), basis - allocated);
    let cash = reopened
        .load_execution_canary_cash_settlement(&d.order_id)?
        .unwrap();
    assert_eq!(cash.sold_quantity.raw(), 250);
    assert_eq!(cash.remaining_quantity.raw(), 750);
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(cash.allocated_entry_basis.as_u64(), allocated);
    assert_eq!(cash.remaining_entry_basis.as_u64(), basis - allocated);
    let fills: i64 = f.db.sql.query_row(
        "SELECT count(*) FROM fills WHERE order_id=?1",
        [&d.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(fills, 1);
    let fee: Option<String> = f.db.sql.query_row(
        "SELECT transaction_fee FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&d.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(fee.as_deref(), Some("19000"));
    assert!(reopened
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    Ok(())
}
#[tokio::test]
async fn fractional_unknown_dispatch_restart_holds_no_resend() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    let d = money::dispatch(&f, &p)?;
    let before = f.db.store.load_tiny_experiment(Utc::now())?;
    let reopened = SqliteStore::open(&f.db.path)?;
    assert_eq!(
        reopened.owned_sell_dispatch_ids(4)?,
        vec![d.order_id.clone()]
    );
    assert_eq!(
        serde_json::to_value(before)?,
        serde_json::to_value(reopened.load_tiny_experiment(Utc::now())?)?
    );
    assert!(reopened
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    assert_eq!(
        reopened
            .load_execution_canary_open_position(&d.token)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        1000
    );
    assert!(reopened.has_owned_sell_handoff(&claim.intent_id)?);
    let count: i64 =
        f.db.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?;
    assert_eq!(count, 1);
    Ok(())
}
#[tokio::test]
async fn fractional_stale_decision_blocks_predispatch_without_release() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    f.db.sql.execute("UPDATE fractional_sell_decisions SET decision=json_set(decision,'$.decision_id','substituted')",[])?;
    assert!(f
        .db
        .store
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    let holds: i64 =
        f.db.sql
            .query_row("SELECT fee_reserve FROM rpc_owned_sell_handoffs", [], |r| {
                r.get(0)
            })?;
    assert_eq!(holds, 100000);
    assert_eq!(
        f.db.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get::<_, i64>(0)
            })?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn prepared_fast_guard_falls_back_after_foreign_decision_change() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    let version = f.db.store.sqlite_data_version()?;
    assert!(f
        .db
        .store
        .recheck_owned_sell_prepared_at_version(&p, version, Utc::now())?);
    f.db.sql.execute("UPDATE fractional_sell_decisions SET decision=json_set(decision,'$.decision_id','substituted')",[])?;
    assert!(!f
        .db
        .store
        .recheck_owned_sell_prepared_at_version(&p, version, Utc::now())?);
    assert!(f
        .db
        .store
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    Ok(())
}
