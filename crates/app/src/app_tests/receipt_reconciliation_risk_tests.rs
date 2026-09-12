use super::receipt_reconciliation_fixture::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::{
    EXECUTION_ACCOUNTING_PENDING_REASON, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use serde_json::json;

#[tokio::test]
async fn receipt_restart_timeout_sweep_holds_risk_and_only_reads_receipt() -> Result<()> {
    for side in ["buy", "sell"] {
        let mut f = Fixture::new(side)?;
        let rpc = Rpc::new(json!({"result":null})).await?;
        rpc.context(format!(
            "receipt_restart_timeout_sweep_holds_risk_and_only_reads_receipt side={side:?}"
        ));
        f.reconcile(&rpc, 10).await?;
        let proof = f
            .store
            .load_execution_canary_receipt_proof(&f.order_id)?
            .unwrap();
        f.conn()?
            .execute("DELETE FROM execution_canary_build_plan_metadata", [])?;
        f.reopen()?;
        *rpc.status.lock().unwrap() = json!({"result":{"value":[null]}});
        let now = f.now + Duration::seconds(1_000);
        let mut cfg = config(&rpc.url);
        cfg.canary_enabled = true;
        cfg.canary_dry_run = true;
        cfg.canary_tiny_submit_enabled = true;
        cfg.execution_signer_pubkey = WALLET.into();
        cfg.execution_signer_keypair_path = "/nonexistent/no-signing-allowed".into();
        cfg.swap_transaction_dry_run_enabled = true;
        cfg.max_confirm_seconds = 1;
        let swept = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &cfg, &f.store, now,
        )
        .await?
        .unwrap();
        assert_eq!(swept.existing, 1);
        assert_eq!(swept.submit_timeout_wait, 1);
        assert_eq!(swept.expired + swept.submit_timeout_retry, 0);
        let order = f.store.load_execution_canary_order(&f.order_id)?.unwrap();
        assert_eq!(order.status, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED);
        assert_eq!(order.attempt, 1);
        assert_eq!(order.tx_signature.as_deref(), Some(SIGNATURE));
        assert_eq!(
            f.store
                .load_execution_canary_receipt_proof(&f.order_id)?
                .unwrap(),
            proof
        );
        assert!(f
            .store
            .mark_execution_canary_expired(&f.order_id, now, "timeout")
            .is_err());
        assert!(f
            .store
            .mark_execution_canary_retry_after_submit_timeout(
                &f.order_id,
                now,
                Duration::seconds(1),
                "retry"
            )
            .is_err());
        let risk = f
            .store
            .execution_canary_submit_risk_summary(now, "retry", 3)?;
        assert_eq!(risk.active_orders, 1);
        assert_eq!(risk.retry_ready_orders, 0);
        assert_eq!(
            crate::execution_canary_safety::pre_submit_safety_snapshot(&cfg, &f.store, now)?
                .blocked_reason,
            Some(EXECUTION_ACCOUNTING_PENDING_REASON)
        );
        let buy = add_order(&f.store, "next-buy", "buy", "AnotherMint", now, false)?;
        let sell = add_order(&f.store, "next-sell", "sell", TOKEN, now, false)?;
        // Existing candidates also have to pass the final gate; no adapter or HTTP request occurs.
        let prior = rpc.calls.lock().unwrap().len();
        assert_eq!(
            try_submit(&f, &rpc, &buy).await?.reason.as_deref(),
            Some(if side == "buy" {
                "unresolved_buy_dispatch"
            } else {
                EXECUTION_ACCOUNTING_PENDING_REASON
            })
        );
        assert_eq!(
            try_submit(&f, &rpc, &sell).await?.reason.as_deref(),
            Some("sell_token_accounting_pending")
        );
        assert_eq!(
            try_submit(&f, &rpc, &f.order_id).await?.reason.as_deref(),
            Some("tiny_order_not_submit_eligible")
        );
        assert_eq!(rpc.calls.lock().unwrap().len(), prior);
        // The reservation boundary independently holds after 300 seconds and across routes.
        f.conn()?.execute("INSERT INTO copy_signals SELECT 'sell-reserve', wallet_id, side, token, notional_sol, ts, status, notional_lamports, notional_origin FROM copy_signals WHERE signal_id = 'next-sell'", [])?;
        let reserve = f
            .store
            .reserve_execution_canary_sell_order_unless_token_in_flight(
                "sell-reserve",
                "other-route",
                now,
            )?;
        assert!(reserve.blocked_by_in_flight_sell);
        assert_eq!(reserve.order.order_id, f.order_id);
        // SELL for another known position is not held by the accounting guard.
        let wallet = bs58::encode(super::tiny_submit_fixture::payer()).into_string();
        super::tiny_parent_fixture::seed(
            &f.store,
            &f.conn()?,
            "other-owned",
            "leader",
            "OtherMint",
            &wallet,
            copybot_core_types::TokenQuantity::new(2000, 3),
            2_000_000,
            now,
        )?;
        let other = add_order(
            &f.store,
            "shadow:other-sell:leader:sell:OtherMint",
            "sell",
            "OtherMint",
            now,
            false,
        )?;
        assert_eq!(
            f.store
                .execution_canary_receipt_submit_block_reason(&other, "OtherMint", "sell")?,
            None
        );
        let expected = super::tiny_submit_fixture::payload("sell")
            .tx_signature_hint
            .unwrap();
        rpc.set(json!({"result":expected}));
        let sent = super::entry_risk_clock_fixture::at(
            now + Duration::seconds(1),
            try_submit(&f, &rpc, &other),
        )
        .await?;
        assert_eq!(sent.submitted, 1, "{sent:?}");
        assert_eq!(sent.tx_signature.as_deref(), Some(expected.as_str()));
        assert_eq!(
            f.store
                .load_execution_canary_dispatch(&other)?
                .unwrap()
                .tx_signature,
            expected
        );
        assert_eq!(
            rpc.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|m| *m == "getFeeForMessage")
                .count(),
            1
        );
        assert!(!rpc.calls.lock().unwrap().iter().any(|m| matches!(
            m.as_str(),
            "getAccountInfo" | "getMinimumBalanceForRentExemption"
        )));
        assert_eq!(
            rpc.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|m| *m == "sendTransaction")
                .count(),
            1
        );
        // The original order gets exactly one real fill once its receipt becomes available.
        rpc.set(receipt(
            side,
            if side == "buy" {
                -900_000_000
            } else {
                1_200_000_000
            },
        ));
        assert_eq!(f.reconcile(&rpc, 2_000).await?.confirmation_confirmed, 1);
        assert_eq!(f.reconcile(&rpc, 3_000).await?.confirmation_pending, 0);
        assert_eq!(f.fills()?, 1);
        assert_eq!(
            rpc.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|m| *m == "sendTransaction")
                .count(),
            1
        );
        assert_eq!(
            rpc.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|m| *m == "getSignatureStatuses")
                .count(),
            1
        );
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_legacy_confirmed_is_in_sweep_and_blocks_before_first_receipt() -> Result<()> {
    let f = Fixture::new("sell")?;
    f.store
        .mark_execution_canary_confirmed(&f.order_id, f.now)?;
    f.conn()?.execute("UPDATE execution_canary_build_plan_metadata SET quote_response_json = '{', quote_price_sol = NULL", [])?;
    assert_eq!(
        f.store
            .list_reconcilable_execution_canary_orders_for_route(ROUTE, "retry", 10)?
            .len(),
        1
    );
    assert!(f.store.execution_canary_accounting_pending()?);
    let rpc = Rpc::new(json!({"result":null})).await?;
    rpc.context(format!(
        "receipt_legacy_confirmed_is_in_sweep_and_blocks_before_first_receipt"
    ));
    let next = add_order(
        &f.store,
        "legacy-next-buy",
        "buy",
        "OtherMint",
        f.now,
        false,
    )?;
    assert_eq!(
        try_submit(&f, &rpc, &next).await?.reason.as_deref(),
        Some(EXECUTION_ACCOUNTING_PENDING_REASON)
    );
    assert!(rpc.calls.lock().unwrap().is_empty());
    assert_eq!(f.reconcile(&rpc, 1_000).await?.confirmation_pending, 1);
    assert_eq!(*rpc.calls.lock().unwrap(), vec!["getTransaction"]);
    rpc.finish().await?;
    Ok(())
}

async fn try_submit(
    f: &Fixture,
    rpc: &Rpc,
    id: &str,
) -> Result<crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome> {
    let order = f.store.load_execution_canary_order(id)?.unwrap();
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&order.signal_id)?
        .unwrap();
    let mut request = ExecutionSubmitRequest {
        order_id: id.into(),
        signal_id: order.signal_id,
        client_order_id: order.client_order_id,
        attempt: order.attempt,
        route: order.route,
        wallet_id: signal.wallet_id,
        token: signal.token,
        side: signal.side,
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 10,
        wallet_pubkey: bs58::encode(super::tiny_submit_fixture::payer()).into_string(),
        entry_route_plan_json: None,
        metadata: crate::app_tests::priority_fee_fixture::metadata(),
    };
    let blocked = order.status != copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
        || f.store
            .execution_canary_receipt_submit_block_reason(id, &request.token, &request.side)?
            .is_some();
    if !blocked {
        request.slippage_tolerance_bps = 500;
        request.metadata.quote_in_amount_raw = Some("2000".into());
        request.metadata.quote_out_amount_raw = Some("10000000".into());
        request.metadata.quote_response_json = Some(json!({"inputMint":"OtherMint","outputMint":"So11111111111111111111111111111111111111112","inAmount":"2000","outAmount":"10000000","swapMode":"ExactIn","slippageBps":500}).to_string());
        super::owned_sell_fixture::bind(&f.store, &mut request, 2000, 3)?;
    }
    let envelope = if blocked {
        let plan = NoSubmitExecutionAdapter.build_transaction_plan(&request)?;
        crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
            &request,
            &plan,
            crate::execution_signing_envelope::ExecutionSignedTransactionPayload {
                signed_transaction_base64: "AQIDBA==".into(),
                tx_signature_hint: None,
            },
        )?
    } else {
        super::tiny_submit_fixture::envelope(&f.store, &request, f.now)?
    };
    crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
        &f.store,
        &Ready,
        &request,
        &envelope,
        &crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
            buy_safety_config: Some(super::tiny_submit_fixture::config(&request.wallet_pubkey)),
            allow_rpc_submit: true,
            pretrade_max_priority_fee_lamports: 500_000,
            pretrade_min_sol_reserve: 0.05,
            execution_wallet_pubkey: request.wallet_pubkey.clone(),
            submit_timeout_ms: 100,
        },
        &RpcExecutionSubmitTransport::new(rpc.url.clone()),
        f.now + Duration::seconds(1_000),
    )
    .await
}

struct Ready;
impl ExecutionSubmitAdapter for Ready {
    fn build_transaction_plan(
        &self,
        _: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        unreachable!()
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        _: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        unreachable!()
    }
    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::SubmitReady(ExecutionSubmitIntent {
            idempotency_key: execution_submit_idempotency_key(request),
            submit_route: "rpc_send_transaction".into(),
            signed_transaction_base64: super::tiny_submit_fixture::payload(&request.side)
                .signed_transaction_base64,
            tx_signature_hint: super::tiny_submit_fixture::payload(&request.side).tx_signature_hint,
        }))
    }
}
