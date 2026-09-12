use super::{
    b93_fixture as f, b93_fixture::config, b93_http_fixture::Server,
    b93_submitted_fixture::submitted_receipt,
};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

async fn scenario(method: &'static str, stale: bool) -> Result<()> {
    let (db, m) = f::seeded(&format!("b93-existing-{method}")).await?;
    let signal = f::legacy(&db, &m)?;
    let facts = submitted_receipt(&db, &m, "prior-submitted", 3000, "prior-submitted-receipt")?;
    let prior = db
        .store
        .load_execution_canary_order(&facts.order_id)?
        .unwrap();
    assert_eq!(prior.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert!(!db
        .store
        .execution_canary_token_accounting_pending(f::token(&m))?);
    let now = f::at() + chrono::Duration::seconds(600);
    // Explicit synthetic clock: old submitted SELL is older than the existing
    // 300s same-route in-flight reservation window. No config/guard is changed.
    let quote = super::source_write_off_fixture::quote(&signal, now);
    db.store.record_execution_quote_canary_event(&quote)?;
    let mut rpc = Server::new(db.path.clone(), now, [94; 32]).await?;
    {
        let path = db.path.clone();
        let facts = facts.clone();
        let mut c = rpc.state.lock().unwrap();
        c.mutate_at = if stale { Some(method) } else { None };
        c.after_wallet_raw = Some(4000);
        c.mutation = Some(Box::new(move || {
            let db = f::open(&path)?;
            db.store.mark_execution_canary_confirmed_unreconciled(
                &facts.order_id,
                &ExecutionCanaryReceiptProof {
                    tx_signature: facts.tx_signature.clone(),
                    wallet_pubkey: facts.wallet_pubkey.clone(),
                    token: facts.token.clone(),
                    side: "sell".into(),
                    confirmation_status: "confirmed".into(),
                    slot: Some(facts.slot),
                    confirmed_at: now,
                    reason: "synthetic_delayed_confirmation".into(),
                },
                now,
            )?;
            db.store
                .record_execution_canary_receipt_facts(&facts, now)?;
            let plan = db
                .store
                .plan_execution_canary_sell_settlement(&facts.order_id)?;
            assert!(matches!(plan, ExecutionCanarySellSettlement::Ready(_)));
            let applied = db
                .store
                .apply_execution_canary_sell_settlement(&facts, now)?;
            assert_eq!(applied.settlement.remaining_quantity.raw(), 4000);
            f::write(
                &format!("prior-receipt-plan-{method}"),
                json!({"plan":format!("{plan:?}"),"applied":format!("{applied:?}"),"accounted_at":now}),
            )?;
            Ok(())
        }));
    }
    let out = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config(&rpc.url),
        &db.store,
        &quote.event_id,
        now,
    )
    .await?;
    rpc.finish().await?;
    let out = out.unwrap();
    let amounts: Vec<String> = db
        .sql
        .prepare("SELECT quote_in_amount_raw FROM execution_canary_build_plan_metadata")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    f::write(
        &format!("prior-submitted-{method}"),
        json!({"before_order":format!("{prior:?}"),"synthetic_now":now,"plan_amounts":amounts,"after":f::state(&db,&m)?,"summary":format!("{out:?}"),"calls":rpc.state.lock().unwrap().calls}),
    )?;
    assert_eq!(out.reserved, 1);
    assert_eq!(out.signing_envelope_built, 0);
    assert_eq!(rpc.count("sendTransaction"), 0);
    if stale {
        assert_eq!(f::raw(&db, &m)?, 4000);
        assert!(out.source_sell_refusals.count() > 0,
                "stale SELL must be refused before key loading: amount={amounts:?} current=4000 outcome={out:?}");
        assert!(!out
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("signer keypair")));
    } else {
        assert_eq!(f::raw(&db, &m)?, 7000);
        assert_eq!(amounts, vec!["7000"]);
        assert_eq!(out.source_sell_refusals.count(), 0);
        assert!(out
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("signer keypair")));
    }
    Ok(())
}
#[tokio::test]
async fn b93_healthy_control_reaches_key_boundary() -> Result<()> {
    scenario("healthy", false).await
}
#[tokio::test]
async fn b93_quote_partial_must_refuse_before_key_boundary() -> Result<()> {
    scenario("quote", true).await
}
#[tokio::test]
async fn b93_simulation_partial_must_refuse_before_key_boundary() -> Result<()> {
    scenario("simulateTransaction", true).await
}
