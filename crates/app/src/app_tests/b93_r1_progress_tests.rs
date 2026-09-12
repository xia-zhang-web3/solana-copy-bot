use super::{
    b93_fixture as f, b93_fixture::config, b93_http_fixture::Server,
    b93_submitted_fixture::submitted_receipt,
};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

async fn progress(method: &'static str, stale: bool) -> Result<()> {
    let (db, m) = f::seeded(&format!("b92-existing-{method}")).await?;
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
    if !stale {
        rpc.state.lock().unwrap().error_at = Some("simulateTransaction");
    }
    let mut config = config(&rpc.url);
    config.max_submit_attempts = 3;
    let out = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &db.store,
        &quote.event_id,
        now,
    )
    .await?;
    let first = out.unwrap();
    let first_order = db
        .store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .unwrap();
    if stale {
        assert_eq!(
            first.skipped_reason,
            Some("source_sell_amount_stale"),
            "{first:?}"
        );
        assert_eq!(f::raw(&db, &m)?, 4000);
    } else {
        assert_eq!(first_order.status, EXECUTION_STATUS_CANARY_FAILED);
        assert_eq!(
            first_order.err_code.as_deref(),
            Some(EXECUTION_ERROR_SIMULATION_FAILED)
        );
    }
    rpc.state.lock().unwrap().error_at = None;
    // Reopen the actual DB, without a manual failure/status writer. Sweep and route
    // must retain a way to rebuild this unsigned, otherwise valid SELL from current raw.
    let reopened = f::open(&db.path)?;
    let swept = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &reopened.store,
        now,
    )
    .await?;
    let fresh = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config,
        &reopened.store,
        &quote.event_id,
        now,
    )
    .await?
    .unwrap();
    let current_order = reopened
        .store
        .load_execution_canary_order_by_signal(&signal.signal_id)?
        .unwrap();
    let metadata = reopened
        .store
        .load_execution_canary_build_plan_metadata(&current_order.order_id)?;
    rpc.finish().await?;
    f::write(
        &format!("progress-{method}"),
        json!({"first":format!("{first:?}"),"first_order":format!("{first_order:?}"),"sweep":format!("{swept:?}"),"fresh":format!("{fresh:?}"),"current_order":format!("{current_order:?}"),"metadata":format!("{metadata:?}"),"calls":rpc.state.lock().unwrap().calls}),
    )?;
    assert_eq!(rpc.count("sendTransaction"), 0);
    assert_eq!(
        fresh.signing_envelope_built + swept.as_ref().map_or(0, |s| s.signing_envelope_built),
        0
    );
    let rebuilt = fresh.built + swept.as_ref().map_or(0, |s| s.built);
    assert_eq!(rebuilt, 1, "fresh SELL made no progress after actual refusal: first={first_order:?}, after={current_order:?}, fresh={fresh:?}");
    assert_eq!(
        metadata.unwrap().quote_in_amount_raw.as_deref(),
        Some(if stale { "4000" } else { "7000" })
    );
    assert!([
        fresh.last_error.as_deref(),
        swept.as_ref().and_then(|s| s.last_error.as_deref())
    ]
    .into_iter()
    .flatten()
    .any(|s| s.contains("signer keypair")));
    Ok(())
}
#[tokio::test]
async fn b93_r1_progress_actual_simulation_failure_control() -> Result<()> {
    progress("control", false).await
}
#[tokio::test]
async fn b93_r1_progress_after_wallet_refusal() -> Result<()> {
    progress("getTokenAccountsByOwner", true).await
}
#[tokio::test]
async fn b93_r1_progress_after_quote_refusal() -> Result<()> {
    progress("quote", true).await
}
#[tokio::test]
async fn b93_r1_progress_after_simulation_refusal() -> Result<()> {
    progress("simulateTransaction", true).await
}
