use super::{
    fractional_financial_fixture as money, fractional_fixture::Fixture, fractional_tests as f,
};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::*;
use serde_json::json;
#[tokio::test]
async fn fractional_reference_schema_rejections_before_quote() -> Result<()> {
    for fault in ["header", "prefix_signature", "prefix_index", "prefix_cpi"] {
        let mut f = Fixture::with_prefix(true).await?;
        let claim = f.claim()?;
        let mut e = super::fractional_synthetic_fixture::evidence(true)?;
        match fault {
            "header" => {
                e.block["transactions"][1]["transaction"]["message"]["header"]
                    ["numReadonlySignedAccounts"] = json!(2)
            }
            "prefix_signature" => {
                e.block["transactions"][0]["transaction"]["signatures"] = json!(["bad"])
            }
            "prefix_index" => {
                e.block["transactions"][0]["transaction"]["message"]["instructions"][0]
                    ["accounts"][0] = json!(99)
            }
            _ => {
                e.block["transactions"][0]["meta"]["innerInstructions"] = json!([{"index":0,"instructions":[{"programIdIndex":4,"accounts":[1,2,0],"data":"4"}]}])
            }
        }
        assert!(
            f::bind(&mut f, claim, e).await.is_err(),
            "native schema accepted {fault}"
        );
    }
    Ok(())
}
#[tokio::test]
async fn fractional_pending_barrier_and_transport_unknown_cannot_rearm() -> Result<()> {
    let mut f = Fixture::new().await?;
    let claim = f.claim()?;
    let _pending = super::b93_fixture::receipt(&f.db, &f.meta, "pending-earlier-sell", 1)?;
    assert!(f::bind(&mut f, claim, f::evidence()?).await.is_err());
    let mut f = Fixture::new().await?;
    let claim = f.claim()?;
    let c = f::config(&f)?;
    let mut calls = 0;
    assert!(crate::execution_owned_sell_rpc::fractional::bind(
        &mut f.db.store,
        &c,
        claim.clone(),
        super::association_parent_fixture::limits(),
        |_| {
            calls += 1;
            std::future::ready(Err(anyhow::anyhow!("unknown mocked HTTP outcome")))
        }
    )
    .await
    .is_err());
    assert_eq!(calls, 1);
    let mut reopened = SqliteStore::open(&f.db.path)?;
    assert!(crate::execution_owned_sell_rpc::fractional::bind(
        &mut reopened,
        &c,
        claim,
        super::association_parent_fixture::limits(),
        |_| {
            calls += 1;
            std::future::ready(Err(anyhow::anyhow!("must not resend")))
        }
    )
    .await
    .is_err());
    assert_eq!(calls, 1);
    Ok(())
}
#[tokio::test]
async fn fractional_full_prefix_inflow_is_in_denominator() -> Result<()> {
    let mut f = Fixture::with_prefix(true).await?;
    let claim = f.claim()?;
    let evidence = super::fractional_synthetic_fixture::evidence(true)?;
    let claim = f::bind(&mut f, claim, evidence).await?;
    let d = &claim.binding.fractional.as_ref().unwrap().inventory;
    // One earlier inflow increases D from 40,000 to 40,001.
    assert_eq!(d.denominator, "40001");
    assert_eq!(d.target_index, 1);
    assert_eq!(claim.binding.raw, 249);
    Ok(())
}
#[tokio::test]
async fn fractional_failed_receipt_fee_once_and_wrong_quantity_pending() -> Result<()> {
    for failed in [true, false] {
        let mut f = Fixture::new().await?;
        money::budget(&f)?;
        let old = f.claim()?;
        let claim = f::bind(&mut f, old, f::evidence()?).await?;
        let p = money::prepare(&f, &claim)?;
        let d = money::dispatch(&f, &p)?;
        if failed {
            let err = json!({"InstructionError":[0,{"Custom":7}]});
            f.db.store.detect_failed_expense(
                &d.order_id,
                &d.wallet,
                "signature_status",
                "confirmed",
                Some(151),
                &err,
                Utc::now(),
            )?;
            let facts = FailedTransactionFacts {
                tx_signature: d.tx_signature.clone(),
                wallet: d.wallet.clone(),
                slot: 151,
                commitment: "confirmed".into(),
                transaction_error: err,
                transaction_fee_lamports: Some("19000".into()),
                fee_coverage: FailedExpenseCoverage::Known,
                payer: Some(d.wallet.clone()),
                payer_coverage: FailedExpenseCoverage::Known,
                wallet_native_pre_lamports: Some("100000000".into()),
                wallet_native_post_lamports: Some("99981000".into()),
                native_coverage: FailedExpenseCoverage::Known,
            };
            f.db.store
                .apply_failed_expense(&d.order_id, &facts, Utc::now())?;
            let reopened = SqliteStore::open(&f.db.path)?;
            reopened.apply_failed_expense(&d.order_id, &facts, Utc::now())?;
            let (count,fee):(i64,String)=f.db.sql.query_row("SELECT count(*),wallet_fee_lamports FROM execution_failed_expense_ledger WHERE order_id=?1",[&d.order_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            assert_eq!((count, fee), (1, "19000".into()));
        } else {
            let facts = money::receipt(&d, 251);
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
                    reason: "synthetic mismatched delta".into(),
                },
                Utc::now(),
            )?;
            f.db.store
                .record_execution_canary_receipt_facts(&facts, Utc::now())?;
            assert!(f
                .db
                .store
                .apply_execution_canary_sell_settlement(&facts, Utc::now())
                .is_err());
            assert!(f
                .db
                .store
                .load_execution_canary_cash_settlement(&d.order_id)?
                .is_none());
        }
        assert_eq!(
            f.db.store
                .load_execution_canary_open_position(&d.token)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            1000
        );
    }
    Ok(())
}
#[tokio::test]
async fn fractional_missing_ownership_zero_and_stale_generation() -> Result<()> {
    // Canonical receipt partial update, not a quantity setter, produces H=1 -> zero.
    let mut f = Fixture::new().await?;
    let facts = super::b93_fixture::receipt(&f.db, &f.meta, "fractional-zero", 999)?;
    f.db.store
        .record_execution_canary_receipt_facts(&facts, Utc::now())?;
    f.db.store
        .apply_execution_canary_sell_settlement(&facts, Utc::now())?;
    let claim = f.claim()?;
    assert_eq!(claim.binding.raw, 1);
    let mut e = f::evidence()?;
    e.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
        ["amount"] = json!("1");
    assert!(f::bind(&mut f, claim, e)
        .await
        .unwrap_err()
        .to_string()
        .contains("fraction_zero_allocation"));
    let state: String =
        f.db.sql
            .query_row("SELECT state FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
    assert_eq!(state, "zero");
    let mut f = Fixture::new().await?;
    let claim = f.claim()?;
    f.db.store.begin_fractional_sell(
        &claim,
        super::association_parent_fixture::limits(),
        Utc::now(),
        &"a".repeat(64),
    )?;
    let partial = super::b93_fixture::receipt(&f.db, &f.meta, "fractional-stale", 1)?;
    f.db.store
        .record_execution_canary_receipt_facts(&partial, Utc::now())?;
    f.db.store
        .apply_execution_canary_sell_settlement(&partial, Utc::now())?;
    assert!(f
        .db
        .store
        .complete_fractional_sell(
            &claim,
            &f::evidence()?,
            super::association_parent_fixture::limits(),
            Utc::now()
        )
        .is_err());
    let f = Fixture::new().await?;
    let claim = f.claim()?;
    f.db.sql.execute(
        "DELETE FROM execution_canary_receipt_facts WHERE side='buy'",
        [],
    )?;
    assert!(f
        .db
        .store
        .begin_fractional_sell(
            &claim,
            super::association_parent_fixture::limits(),
            Utc::now(),
            &"a".repeat(64)
        )
        .is_err());
    Ok(())
}
#[tokio::test]
async fn fractional_producer_config_change_and_observation_only_remain_denied() -> Result<()> {
    let mut f = Fixture::new().await?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let mut c = f::config(&f)?;
    c.owned_sell_preparation.as_mut().unwrap().identity = "changed".into();
    let rejected = crate::execution_owned_sell_rpc::fractional::bind(
        &mut f.db.store,
        &c,
        claim,
        super::association_parent_fixture::limits(),
        |_| std::future::ready(Err(anyhow::anyhow!("must not read"))),
    )
    .await;
    assert!(rejected
        .unwrap_err()
        .to_string()
        .contains("fraction_producer_changed"));
    let mut app = super::association_fixture::config(&f.meta);
    app.execution = f::config(&f)?;
    copybot_config::validate_association_delivery(&app)?;
    app.ingestion.capture_scope_db = Some("never-open".into());
    app.ingestion.yellowstone_delivery_mode = "legacy".into();
    assert!(copybot_config::validate_association_delivery(&app).is_err());
    app.ingestion.capture_scope_db = None;
    app.ingestion.yellowstone_delivery_mode = "durable_association_v1".into();
    app.execution.enabled = true;
    assert!(copybot_config::validate_association_delivery(&app).is_err());
    Ok(())
}
