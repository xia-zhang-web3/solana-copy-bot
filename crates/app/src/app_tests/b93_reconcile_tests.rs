use super::{b93_attempt_fixture::Attempt, b93_fixture as f};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::{json, Value};

fn receipt(a: &Attempt, signature: &str) -> Value {
    let wallet = &a.config.canary_wallet_pubkey;
    let token = f::token(&a.m);
    let row = |raw| json!({"accountIndex":1,"owner":wallet,"mint":token,"uiTokenAmount":{"amount":raw,"decimals":3}});
    json!({"slot":140,"blockTime":null,
        "transaction":{"signatures":[signature],"message":{"accountKeys":[
            {"pubkey":wallet,"signer":true,"writable":true},
            {"pubkey":"synthetic-token-account","signer":false,"writable":true}]}},
        "meta":{"err":null,"fee":10000,"preBalances":[1_000_000_000,2039280],"postBalances":[1_100_000_000,2039280],
            "preTokenBalances":[row("4000")],"postTokenBalances":[row("0")]}})
}

#[tokio::test]
async fn b93_known_signature_pending_then_receipt_bypasses_unsigned_amount_guard() -> Result<()> {
    for arm in ["sell-route", "sweep", "runner"] {
        let mut a = Attempt::new(&format!("b93-known-{arm}")).await?;
        let signature = "synthetic-known-submitted";
        a.db.store
            .mark_execution_canary_submitted(&a.request.order_id, f::at(), signature)?;
        let partial = a.partial("other-partial")?;
        assert!(
            a.db.store
                .apply_execution_canary_sell_settlement(&partial, f::at())?
                .already_accounted
        );
        a.reopen()?;
        let before = a.rpc.count("simulateTransaction");
        // The actual SELL route must leave the known signature pending, without rebuilding.
        let pending =
            crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
                &a.config,
                &a.db.store,
                &a.event_id,
                f::at(),
            )
            .await?
            .unwrap();
        assert_eq!(pending.source_sell_refusals.count(), 0, "{pending:?}");
        let order =
            a.db.store
                .load_execution_canary_order(&a.request.order_id)?
                .unwrap();
        assert_eq!(order.tx_signature.as_deref(), Some(signature));
        assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
        assert_eq!(f::raw(&a.db, &a.m)?, 4000);
        a.db.store.mark_execution_canary_confirmed_unreconciled(
            &order.order_id,
            &ExecutionCanaryReceiptProof {
                tx_signature: signature.into(),
                wallet_pubkey: a.config.canary_wallet_pubkey.clone(),
                token: a.signal.token.clone(),
                side: "sell".into(),
                confirmation_status: "confirmed".into(),
                slot: Some(140),
                confirmed_at: f::at(),
                reason: "synthetic-known-confirmation".into(),
            },
            f::at(),
        )?;
        assert!(a
            .db
            .store
            .execution_canary_token_accounting_pending(f::token(&a.m))?);
        a.rpc.state.lock().unwrap().receipt = Some(receipt(&a, signature));
        match arm {
            "sell-route" => {
                let out =
                    crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
                        &a.config,
                        &a.db.store,
                        &a.event_id,
                        f::at(),
                    )
                    .await?
                    .unwrap();
                assert_eq!(out.source_sell_refusals.count(), 0, "{out:?}");
                assert_eq!(out.sell_closed, 1, "{out:?}");
            }
            "sweep" => {
                let out = a.retry().await?;
                assert_eq!(out.sell_closed, 1, "{out:?}");
            }
            _ => {
                let out =
                    crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
                        &a.db.store,
                        &a.config,
                        &order.order_id,
                        &reqwest::Client::new(),
                        &a.rpc.url,
                        f::at(),
                        1000,
                    )
                    .await?;
                assert_eq!(out.sell_closed, 1, "{out:?}");
            }
        }
        let closed =
            a.db.store
                .load_execution_canary_order(&order.order_id)?
                .unwrap();
        assert_eq!(closed.status, EXECUTION_STATUS_CANARY_CONFIRMED);
        assert_eq!(closed.tx_signature.as_deref(), Some(signature));
        assert_eq!(closed.attempt, order.attempt);
        assert!(a
            .db
            .store
            .load_execution_canary_open_position(f::token(&a.m))?
            .is_none());
        assert!(!a
            .db
            .store
            .execution_canary_token_accounting_pending(f::token(&a.m))?);
        let cash =
            a.db.store
                .load_execution_canary_cash_settlement(&order.order_id)?;
        assert!(cash.is_some());
        let replay = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
            &a.db.store,
            &a.config,
            &order.order_id,
            &reqwest::Client::new(),
            &a.rpc.url,
            f::at(),
            1000,
        )
        .await?;
        assert_eq!(replay.confirmation_confirmed, 1);
        assert_eq!(replay.cash_settlement, cash);
        a.rpc.finish().await?;
        assert_eq!(a.rpc.count("simulateTransaction"), before);
        assert_eq!(a.rpc.count("sendTransaction"), 0);
        assert_eq!(a.rpc.count("quote"), 1);
        f::write(
            arm,
            json!({"order":format!("{closed:?}"),"cash":format!("{cash:?}"),"calls":a.rpc.state.lock().unwrap().calls}),
        )?;
    }
    Ok(())
}
