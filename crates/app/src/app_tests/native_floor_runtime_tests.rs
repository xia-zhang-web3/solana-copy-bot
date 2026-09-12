use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_native_floor::verify_final_native_floor as verify;
use crate::execution_native_floor_policy::reserve_lamports;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;

fn checked_simulation_and_send(
    f: &Fixture,
    expected_r: u64,
    extension: bool,
    stage: &str,
) -> Result<()> {
    let calls = f.calls.lock().unwrap();
    let simulated = calls
        .iter()
        .rev()
        .find(|(_, v)| v["method"] == "simulateTransaction")
        .unwrap();
    let sent = calls
        .iter()
        .rev()
        .find(|(_, v)| v["method"] == "sendTransaction")
        .unwrap();
    let payer =
        crate::execution_pumpswap_accounts::parse_pubkey(&f.config.canary_wallet_pubkey, "test")?;
    let before = verify(
        simulated.1["params"][0].as_str().unwrap(),
        payer,
        expected_r,
    )?;
    let after = verify(sent.1["params"][0].as_str().unwrap(), payer, expected_r)?;
    assert_eq!(
        before.binding().message_bytes,
        after.binding().message_bytes
    );
    assert_ne!(
        before.binding().transaction_sha256,
        after.binding().transaction_sha256
    );
    assert_eq!(
        after.final_instruction_index(),
        if extension { 9 } else { 8 }
    );
    let decoded = crate::execution_transaction_wire::decode_message(after.payload(), |_| Ok(()))?;
    assert_eq!(
        decoded.instructions[after.final_instruction_index() - 1].data,
        [9],
        "WSOL close precedes guard"
    );
    let fee = crate::execution_priority_fee_wire::decode_priority_fee(after.payload())?;
    assert_eq!(fee.limit.get(), 1_400_000);
    assert!(fee.total <= f.config.pretrade_max_priority_fee_lamports);
    let wire = STANDARD.decode(after.payload())?;
    ed25519_dalek::VerifyingKey::from_bytes(&payer)?.verify_strict(
        &wire[65..],
        &ed25519_dalek::Signature::from_slice(&wire[1..65])?,
    )?;
    eprintln!(
        "B25_RUNTIME {}",
        serde_json::json!({
            "stage": stage, "extension": extension, "reserve_lamports": expected_r.to_string(),
            "simulated_message_sha256": before.binding().message_sha256,
            "sent_message_sha256": after.binding().message_sha256,
            "simulated_transaction_sha256": before.binding().transaction_sha256,
            "sent_transaction_sha256": after.binding().transaction_sha256,
            "last_instruction_index": after.final_instruction_index(), "wire_bytes": wire.len(),
            "cu_limit": fee.limit.get(), "encoded_priority_fee_lamports": fee.total,
            "synthetic_signature_verified": true,
            "simulation_calls": calls.iter().filter(|(_,v)| v["method"] == "simulateTransaction").count(),
            "send_calls": calls.iter().filter(|(_,v)| v["method"] == "sendTransaction").count()
        })
    );
    Ok(())
}

#[tokio::test]
async fn native_floor_hot_direct_buy_simulates_signs_and_submits_exact_guard() -> Result<()> {
    for extension in [false, true] {
        let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
        f.wire.lock().unwrap().extension = extension;
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
        let out = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(2),
            crate::execution_canary_route::process_canary_state_machine_for_route(
                &f.config, &f.store, &signal, f.now,
            ),
        )
        .await;
        f.finish().await?;
        let out = out?;
        if extension {
            assert_eq!(out.failed, 1, "{out:?}");
            assert_eq!(
                out.last_error.as_deref(),
                Some("initial_sol_unsupported_setup")
            );
            assert_eq!(out.signing_envelope_built, 1);
            assert_eq!(f.sends(), 0);
            continue;
        }
        assert_eq!(out.failed, 0, "{out:?}");
        assert_eq!(out.signing_envelope_built, 1, "{out:?}");
        assert_eq!(f.sends(), 1);
        checked_simulation_and_send(&f, 50_000_001, extension, "hot")?;
    }
    Ok(())
}

#[tokio::test]
async fn native_floor_real_retry_rebuilds_with_current_r_and_blockhash() -> Result<()> {
    for extension in [false, true] {
        let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
        f.wire.lock().unwrap().extension = extension;
        let old = f.build().await?.envelope.unwrap();
        f.store.mark_execution_canary_retry_after_submit_not_sent(
            &f.request.order_id,
            f.now,
            crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON,
        )?;
        f.config.pretrade_min_sol_reserve = 0.075;
        f.wire.lock().unwrap().blockhash = 17;
        let out = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(2),
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &f.config, &f.store, f.now,
            ),
        )
        .await;
        f.finish().await?;
        let out = out?.unwrap();
        if extension {
            assert_eq!(out.failed, 1, "{out:?}");
            assert_eq!(
                out.last_error.as_deref(),
                Some("initial_sol_unsupported_setup")
            );
            assert_eq!(out.signing_envelope_built, 1);
            assert_eq!(f.sends(), 0);
            continue;
        }
        assert_eq!(out.failed, 0, "{out:?}");
        assert_eq!(out.signing_envelope_built, 1, "{out:?}");
        assert_eq!(f.sends(), 1);
        checked_simulation_and_send(&f, reserve_lamports(0.075)?, extension, "retry")?;
        let calls = f.calls.lock().unwrap();
        let sent = calls
            .iter()
            .find(|(_, v)| v["method"] == "sendTransaction")
            .unwrap();
        assert_ne!(
            old.signed_transaction_base64.as_deref().unwrap(),
            sent.1["params"][0].as_str().unwrap()
        );
        let metadata = f
            .store
            .load_execution_canary_build_plan_metadata(&f.request.order_id)?
            .unwrap();
        let durable: Value = serde_json::from_str(metadata.priority_fee_json.as_deref().unwrap())?;
        let proof = crate::execution_priority_fee_proof::prove(
            &crate::execution_submit_adapter::build_tiny_submit_reconciliation_request(
                &f.store,
                &f.config,
                &f.store
                    .load_execution_canary_order(&f.request.order_id)?
                    .unwrap(),
            )?,
            sent.1["params"][0].as_str().unwrap(),
            f.config.pretrade_max_priority_fee_lamports,
        )?;
        assert_eq!(durable["fee_proof"], serde_json::to_value(proof)?);
    }
    Ok(())
}

#[tokio::test]
async fn native_floor_unguarded_buy_and_real_fallback_stop_before_signer() -> Result<()> {
    for route in [
        Route::Metis,
        Route::Paid,
        Route::DirectFallback,
        Route::PaidFallback,
        Route::Direct,
    ] {
        let mut f = Fixture::new(route, 200_000, 200_000).await?;
        if route == Route::Direct {
            f.simulation_responses
                .lock()
                .unwrap()
                .push_back(serde_json::json!({}));
        }
        let built = f.build().await;
        f.finish().await?;
        let built = built?;
        assert_eq!(built.failed, 1, "{route:?}: {built:?}");
        assert_eq!(built.error.as_deref(), Some("native_floor_program"));
        assert_eq!((f.signatures(), f.sends()), (0, 0));
        let order = f
            .store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap();
        assert_eq!(order.err_code.as_deref(), Some("signing_envelope_failed"));
        assert!(order.tx_signature.is_none());
        if matches!(route, Route::DirectFallback | Route::Direct) {
            let calls = f.calls.lock().unwrap();
            assert!(calls
                .iter()
                .any(|(_, v)| v["method"] == "getMultipleAccounts"));
            assert!(calls.iter().any(|(p, _)| p.starts_with("POST /swap ")));
        }
        assert_eq!(
            f.conn()?.query_row(
                "SELECT COUNT(*) FROM execution_failed_expense_ledger",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
    }
    Ok(())
}
