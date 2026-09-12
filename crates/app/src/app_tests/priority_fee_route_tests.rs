use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn priority_fee_guarded_legacy_builders_and_fallbacks_sign_and_submit_below_cap() -> Result<()>
{
    for route in [
        Route::Metis,
        Route::MetisV0,
        Route::Direct,
        Route::Paid,
        Route::DirectFallback,
        Route::PaidFallback,
    ] {
        let mut f = Fixture::new(route, 10_000, 200_000).await?;
        f.wire.lock().unwrap().guard = Some(50_000_001);
        let outcome = f.build().await?;
        if route == Route::MetisV0 {
            assert_eq!(outcome.failed, 1);
            assert_eq!(
                outcome.error.as_deref(),
                Some("native_floor_unsupported_version")
            );
            assert_eq!((f.signatures(), f.sends()), (0, 0));
            f.finish().await?;
            continue;
        }
        assert_eq!(outcome.built, 1, "{route:?}: {:?}", outcome.error);
        let envelope = outcome.envelope.unwrap();
        assert_eq!(f.signatures(), 1);
        let stored = f
            .store
            .load_execution_canary_build_plan_metadata(&f.request.order_id)?
            .unwrap();
        let proof: Value = serde_json::from_str(&stored.priority_fee_json.unwrap())?;
        let expected = if route == Route::Direct {
            14_000
        } else {
            2_000
        };
        assert_eq!(proof["fee_proof"]["total_priority_fee_lamports"], expected);
        let submitted = f.submit(&envelope).await?;
        assert_eq!(submitted.submitted, 1, "{route:?}: {:?}", submitted.error);
        assert_eq!(f.sends(), 1);
        assert_eq!(
            f.store
                .load_execution_canary_order(&f.request.order_id)?
                .unwrap()
                .status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
        );
        // Replay never sends twice, even with the original valid proof/envelope.
        assert_eq!(f.submit(&envelope).await?.submitted, 0);
        assert_eq!(f.sends(), 1);
        let calls = f.calls.lock().unwrap();
        match route {
            Route::Direct => assert!(calls
                .iter()
                .any(|(_, b)| b["method"] == "getMultipleAccounts")),
            Route::Paid => assert!(calls
                .iter()
                .any(|(p, b)| p.contains("/pump-fun/swap ") && b["priorityFeeLevel"] == "high")),
            Route::DirectFallback => {
                assert!(calls
                    .iter()
                    .any(|(_, b)| b["method"] == "getMultipleAccounts"));
                assert!(calls.iter().any(|(p, _)| p.starts_with("POST /swap ")));
            }
            Route::PaidFallback => {
                assert!(calls.iter().any(|(p, _)| p.contains("/pump-fun/swap ")));
                assert!(calls.iter().any(|(p, _)| p.starts_with("POST /swap ")));
            }
            Route::Metis | Route::MetisV0 => {
                assert!(calls.iter().any(|(p, b)| p.starts_with("POST /swap ")
                    && b["prioritizationFeeLamports"] == 120000))
            }
        }
        drop(calls);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn priority_fee_all_builders_and_fallbacks_refuse_actual_over_cap_before_signer() -> Result<()>
{
    for route in [
        Route::Metis,
        Route::MetisV0,
        Route::Direct,
        Route::Paid,
        Route::DirectFallback,
        Route::PaidFallback,
    ] {
        let mut f = Fixture::new(route, 600_000, 1_400_000).await?;
        f.wire.lock().unwrap().guard = Some(50_000_001);
        let outcome = f.build().await?;
        assert_eq!(outcome.failed, 1, "{route:?}: {outcome:?}");
        assert!(outcome
            .error
            .unwrap()
            .contains("priority_fee_cap_exceeded: encoded=840000 cap=500000"));
        assert_eq!((f.signatures(), f.sends()), (0, 0));
        let order = f
            .store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap();
        assert_eq!(
            order.status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
        );
        assert!(order
            .simulation_error
            .unwrap()
            .contains("priority_fee_cap_exceeded"));
        assert!(order.tx_signature.is_none());
        assert_eq!(f.store.execution_canary_open_position_count()?, 0);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn priority_fee_metis_dynamic_request_preserves_cu_price_units() -> Result<()> {
    let mut f = Fixture::new(Route::Metis, 60_000, 200_000).await?;
    f.wire.lock().unwrap().guard = Some(50_000_001);
    f.request.metadata.priority_fee_lamports = None;
    f.request.metadata.priority_fee_json =
        Some(crate::execution_priority_fee::sample_quicknode_fee(&json!({"recommended":60000}))?.1);
    let envelope = f.build().await?.envelope.unwrap();
    assert_eq!(f.submit(&envelope).await?.submitted, 1);
    for (path, body) in f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.contains("/swap"))
    {
        assert_eq!(body["computeUnitPriceMicroLamports"], 60000, "{path}");
        assert!(body.get("prioritizationFeeLamports").is_none());
        assert_eq!(body["dynamicComputeUnitLimit"], true);
    }
    let proof = serde_json::to_value(envelope.priority_fee_proof.unwrap())?;
    assert_eq!(proof["total_priority_fee_lamports"], 12000);
    assert_eq!(proof["requested_compute_unit_limit"], 200000); // mock simulation consumed just 1 CU
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn priority_fee_submit_rejects_changed_message_missing_or_replayed_proof() -> Result<()> {
    for mutation in [
        "message",
        "current_cap",
        "signature",
        "missing_envelope",
        "missing_durable",
        "attempt",
        "different_valid_proof",
    ] {
        let mut f = Fixture::new(Route::Metis, 200_000, 200_000).await?;
        f.wire.lock().unwrap().guard = Some(50_000_001);
        let mut envelope = f.build().await?.envelope.unwrap();
        match mutation {
            "message" => {
                envelope.signed_transaction_base64 = Some(super::priority_fee_fixture::transaction(
                    [11; 32], 1_400_000, 600_000,
                ))
            }
            "current_cap" => f.config.pretrade_max_priority_fee_lamports = 1,
            "signature" => {
                use base64::{engine::general_purpose::STANDARD, Engine};
                let mut bytes =
                    STANDARD.decode(envelope.signed_transaction_base64.as_ref().unwrap())?;
                bytes[1] ^= 1;
                envelope.signed_transaction_base64 = Some(STANDARD.encode(bytes));
            }
            "missing_envelope" => envelope.priority_fee_proof = None,
            "missing_durable" => {
                let plan = NoSubmitExecutionAdapter.build_transaction_plan(&f.request)?;
                crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
                    &f.store, &plan, f.now,
                )?;
            }
            "attempt" => f.request.attempt += 1,
            "different_valid_proof" => {
                let bytes = super::priority_fee_fixture::transaction([11; 32], 200_000, 100_000);
                envelope.priority_fee_proof = Some(crate::execution_priority_fee_proof::prove(
                    &f.request, &bytes, 500_000,
                )?);
                envelope.signed_transaction_base64 = Some(bytes);
            }
            _ => unreachable!(),
        }
        let result = f.submit(&envelope).await;
        if mutation == "attempt" {
            let out = result?;
            assert_eq!(out.submit_ready_rejected, 1);
            assert_eq!(out.reason.as_deref(), Some("tiny_submit_identity_mismatch"));
            assert_eq!(out.failed, 0);
        } else {
            assert_eq!(result?.failed, 1, "{mutation}");
        }
        assert_eq!(f.sends(), 0, "{mutation}");
        assert!(f
            .store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap()
            .tx_signature
            .is_none());
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn priority_fee_production_retry_rebuilds_and_checks_new_bytes() -> Result<()> {
    for next_price in [10_000, 600_000] {
        let mut f = Fixture::new(Route::Metis, 10_000, 1_400_000).await?;
        f.wire.lock().unwrap().guard = Some(50_000_001);
        let old_envelope = f.build().await?.envelope.unwrap();
        assert_eq!(f.signatures(), 1);
        f.store.mark_execution_canary_retry_after_submit_not_sent(
            &f.request.order_id,
            f.now,
            crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON,
        )?;
        f.price.store(next_price, Ordering::SeqCst);
        if next_price == 600_000 {
            f.config.execution_signer_keypair_path = "/nonexistent/synthetic-retry-key.json".into();
        }
        // An actual daemon retry entry point reloads SQLite metadata and calls its own adapter.
        let outcome = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(2),
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &f.config,
                &f.store,
                f.now + chrono::Duration::seconds(1),
            ),
        )
        .await?
        .unwrap();
        let order = f
            .store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap();
        if next_price == 600_000 {
            assert_eq!(outcome.failed, 1, "{outcome:?}");
            assert_eq!(f.sends(), 0);
            assert!(order
                .simulation_error
                .unwrap()
                .contains("priority_fee_cap_exceeded"));
        } else {
            assert_eq!(f.sends(), 1, "{outcome:?} {order:?}");
            assert!(order.tx_signature.is_some());
            let stored = f
                .store
                .load_execution_canary_build_plan_metadata(&order.order_id)?
                .unwrap();
            let value: Value = serde_json::from_str(stored.priority_fee_json.as_deref().unwrap())?;
            assert!(value["fee_proof"].is_object());
        }
        assert_eq!(f.submit(&old_envelope).await?.submitted, 0);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn priority_fee_sell_builders_use_the_same_pre_sign_and_submit_gates() -> Result<()> {
    for route in [Route::Metis, Route::Paid, Route::Direct] {
        for price in [10_000, 600_000] {
            let mut f = Fixture::new(route, price, 1_400_000).await?;
            f.wire.lock().unwrap().guard = Some(50_000_001);
            f.make_sell()?;
            if route == Route::Metis && price == 600_000 {
                // Complete generic SELL proves the fee before its own simulation.
                assert!(f
                    .build()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("priority_fee_cap_exceeded"));
                assert_eq!((f.signatures(), f.sends()), (0, 0));
                assert!(!f
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(_, b)| b["method"] == "simulateTransaction"));
                f.finish().await?;
                continue;
            }
            let result = f.build().await?;
            if price == 600_000 {
                assert_eq!(result.failed, 1, "{route:?}: {result:?}");
                assert!(result.error.unwrap().contains("priority_fee_cap_exceeded"));
                assert_eq!((f.signatures(), f.sends()), (0, 0));
            } else {
                assert_eq!(f.submit(&result.envelope.unwrap()).await?.submitted, 1);
                assert_eq!((f.signatures(), f.sends()), (1, 1));
            }
            f.finish().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn priority_fee_ambiguous_old_sample_blocks_new_build_and_real_retry() -> Result<()> {
    let mut f = Fixture::new(Route::Metis, 200_000, 200_000).await?;
    f.wire.lock().unwrap().guard = Some(50_000_001);
    f.request.metadata.priority_fee_json = Some(r#"{"recommended":120000}"#.into());
    assert!(f
        .adapter
        .build_transaction_plan(&f.request)
        .unwrap_err()
        .to_string()
        .contains("priority_fee_units_unknown"));
    assert_eq!((f.signatures(), f.sends()), (0, 0));
    assert!(f.calls.lock().unwrap().is_empty());
    // A pre-upgrade retry row keeps its original untagged JSON and must not become a proof.
    let old_plan = NoSubmitExecutionAdapter.build_transaction_plan(&f.request)?;
    crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
        &f.store, &old_plan, f.now,
    )?;
    f.store
        .mark_execution_canary_built(&f.request.order_id, f.now)?;
    f.store.mark_execution_canary_simulated(
        &f.request.order_id,
        f.now,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    f.store.mark_execution_canary_retry_after_submit_not_sent(
        &f.request.order_id,
        f.now,
        crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON,
    )?;
    let outcome = super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(2),
        crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &f.config,
            &f.store,
            f.now + chrono::Duration::seconds(1),
        ),
    )
    .await?
    .unwrap();
    assert_eq!(outcome.failed, 1);
    assert_eq!(outcome.entry_gate_blocked, 1);
    assert_eq!((f.signatures(), f.sends()), (0, 0));
    let metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&f.request.order_id)?
        .unwrap();
    assert_eq!(
        metadata.priority_fee_json,
        old_plan.metadata.priority_fee_json
    );
    assert!(f
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(path, _)| path.starts_with("GET /quote?")));
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn priority_fee_incomplete_budget_is_durable_failure_even_after_rpc_simulation_pass(
) -> Result<()> {
    let mut f = Fixture::new(Route::Metis, 600_000, 0).await?;
    f.wire.lock().unwrap().guard = Some(50_000_001);
    let result = f.build().await?;
    assert_eq!(result.failed, 1);
    assert!(result
        .error
        .unwrap()
        .contains("priority_fee_invalid_cu_limit"));
    assert_eq!((f.signatures(), f.sends()), (0, 0));
    let order = f
        .store
        .load_execution_canary_order(&f.request.order_id)?
        .unwrap();
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert!(order
        .simulation_error
        .unwrap()
        .contains("priority_fee_invalid_cu_limit"));
    f.finish().await?;
    Ok(())
}
