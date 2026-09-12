use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;
use serde_json::Value;

async fn hot(
    f: &Fixture,
) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&f.request.signal_id)?
        .unwrap();
    f.conn()?.execute(
        "DELETE FROM orders WHERE order_id=?1",
        [&f.request.order_id],
    )?;
    super::execution_state_machine_tiny_submit_route::record_tiny_route_quote(
        &f.store, &signal, f.now,
    )?;
    f.conn()?.execute(
        "UPDATE execution_quote_canary_events SET quote_price_sol=?1, quote_response_json=?2,
        quote_in_amount_raw=?3, quote_out_amount_raw=?4, route_plan_json=?5 WHERE signal_id=?6",
        rusqlite::params![
            f.request.metadata.quote_price_sol,
            f.request.metadata.quote_response_json,
            f.request.metadata.quote_in_amount_raw,
            f.request.metadata.quote_out_amount_raw,
            f.request.metadata.route_plan_json,
            f.request.signal_id
        ],
    )?;
    super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(2),
        crate::execution_canary_route::process_canary_state_machine_for_route(
            &f.config, &f.store, &signal, f.now,
        ),
    )
    .await
}

#[tokio::test]
async fn initial_sol_hot_and_retry_reject_insufficient_none_timeout_and_partial() -> Result<()> {
    for retry in [false, true] {
        for case in ["insufficient", "fee", "timeout", "extension"] {
            let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
            f.wire.lock().unwrap().extension = case == "extension";
            match case {
                "insufficient" => f.funding.lock().unwrap().balance = 0,
                "fee" => f.funding.lock().unwrap().fee = None,
                "timeout" => {
                    f.funding.lock().unwrap().delay_ms = 200;
                    f.config.submit_timeout_ms = 80;
                }
                _ => {}
            }
            let out = if retry {
                f.build().await?.envelope.unwrap();
                f.store.mark_execution_canary_retry_after_submit_not_sent(&f.request.order_id, f.now,
                    crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON)?;
                super::entry_risk_clock_fixture::at(
                    f.now + chrono::Duration::seconds(2),
                    crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                        &f.config, &f.store, f.now,
                    ),
                )
                .await?
                .unwrap()
            } else {
                hot(&f).await?
            };
            f.finish().await?;
            assert_eq!(out.signing_envelope_built, 1, "{case}/{retry}: {out:?}");
            assert_eq!(out.failed, 1, "{case}/{retry}: {out:?}");
            let reason = out.last_error.unwrap();
            let expected = match case {
                "insufficient" => "initial_sol_insufficient:",
                "fee" => "initial_sol_fee_unavailable",
                "timeout" => "initial_sol_observations_unavailable:",
                _ => "initial_sol_unsupported_setup",
            };
            assert!(reason.starts_with(expected), "{reason}");
            assert_eq!(f.sends(), 0);
            assert_eq!(
                f.conn()?.query_row(
                    "SELECT COUNT(*) FROM execution_failed_expense_ledger",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                0
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_rpc_rejection_preserves_original_message_without_new_collection() -> Result<()>
{
    let mut f = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
    f.wire.lock().unwrap().submit_error = true;
    let envelope = f.build().await?.envelope.unwrap();
    let first = f.submit(&envelope).await?;
    assert_eq!(first.submitted, 1);
    let original_signature = first.tx_signature.clone();
    f.wire.lock().unwrap().submit_error = false;
    f.wire.lock().unwrap().blockhash = 17;
    f.config.pretrade_min_sol_reserve = 0.075;
    // Fresh collector amount changes as well, ruling out reused prior observations.
    f.funding.lock().unwrap().fee = Some(20_000);
    let out = super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(2),
        crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &f.config, &f.store, f.now,
        ),
    )
    .await;
    f.finish().await?;
    let out = out?.unwrap();
    assert_eq!(out.failed, 0, "{out:?}");
    assert_eq!(
        f.store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap()
            .tx_signature
            .as_deref(),
        original_signature.as_deref()
    );
    let calls = f.calls.lock().unwrap();
    let requests: Vec<&Value> = calls
        .iter()
        .filter(|(_, r)| {
            r["id"]
                .as_str()
                .is_some_and(|s| s.starts_with("native-funding-"))
        })
        .map(|(_, r)| r)
        .collect();
    assert_eq!(requests.len(), 4);
    let fees: Vec<_> = requests
        .iter()
        .filter(|r| r["method"] == "getFeeForMessage")
        .collect();
    let sends: Vec<_> = calls
        .iter()
        .filter(|(_, r)| r["method"] == "sendTransaction")
        .collect();
    assert_eq!((fees.len(), sends.len()), (2, 1));
    for fee in &fees {
        let send = sends[0];
        use base64::{engine::general_purpose::STANDARD, Engine};
        let wire = crate::execution_transaction_wire::decode_message(
            send.1["params"][0].as_str().unwrap(),
            |_| Ok(()),
        )?;
        assert_eq!(
            fee["params"][0],
            STANDARD.encode(wire.binding.message_bytes)
        );
    }
    assert_eq!(
        f.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    eprintln!("B53_UNKNOWN collections=1 funding_requests=3 sends=1 rpc_rejection_unknown=true refreshed_message=false");
    Ok(())
}

#[tokio::test]
async fn initial_sol_shared_owner_reuses_pool_after_rejected_observations() -> Result<()> {
    use super::native_funding_fixture::{budget, payload, WALLET};
    let server = super::native_rpc_reuse_fixture::ReuseFixture::with_rent(true).await?;
    let p = payload(&budget())?;
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        outcomes.push(
            crate::execution_initial_sol::collect_and_check(&server.endpoint, 1000, &p, WALLET, 1)
                .await,
        );
    }
    let trace = server.finish().await?;
    for out in outcomes {
        assert_eq!(
            out.unwrap_err().to_string(),
            "initial_sol_payer_unavailable"
        );
    }
    assert_eq!(trace.requests.len(), 6);
    assert_eq!(
        trace.connections, 3,
        "the owner keeps the three parallel connections"
    );
    eprintln!("B26_POOL collections=2 requests=6 connections=3");
    Ok(())
}
