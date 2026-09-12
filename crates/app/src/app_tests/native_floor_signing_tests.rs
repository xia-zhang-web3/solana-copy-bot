use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_signing_envelope::ExecutionSerializedTransactionPayload;
use crate::execution_submit_adapter::*;
use anyhow::Result;

#[tokio::test]
async fn native_floor_labels_cannot_bypass_presign_and_guarded_fallback_passes() -> Result<()> {
    for label in ["pumpswap_direct", "metis", "pump_fun_paid", "arbitrary"] {
        let mut f = Fixture::new(Route::Metis, 200_000, 200_000).await?;
        let plan = f.adapter.build_transaction_plan(&f.request)?;
        f.adapter.simulate_transaction_plan(&plan).await?;
        let slot = plan.serialized_transaction_payload_slot.as_ref().unwrap();
        let mut payload = slot.load()?.unwrap();
        payload.source = label.into();
        slot.store(payload)?;
        let error = f
            .adapter
            .build_signing_envelope(&f.request, &plan)
            .unwrap_err();
        f.finish().await?;
        assert_eq!(error.to_string(), "native_floor_program");
        assert_eq!((f.signatures(), f.sends()), (0, 0));
    }
    for route in [
        Route::Metis,
        Route::Paid,
        Route::DirectFallback,
        Route::PaidFallback,
        Route::Direct,
    ] {
        let mut f = Fixture::new(route, 10_000, 200_000).await?;
        // Synthetic provider supplies independently verifiable guarded legacy bytes.
        // This does not claim that the production external builders construct guards.
        f.wire.lock().unwrap().guard = Some(50_000_001);
        if route == Route::Direct {
            f.simulation_responses
                .lock()
                .unwrap()
                .push_back(serde_json::json!({}));
        }
        let out = f.build().await?;
        assert_eq!(out.built, 1, "{route:?}: {out:?}");
        let envelope = out.envelope.unwrap();
        assert_eq!(f.submit(&envelope).await?.submitted, 1);
        f.finish().await?;
        assert_eq!((f.signatures(), f.sends()), (1, 1));
    }
    Ok(())
}

#[tokio::test]
async fn native_floor_file_signer_rejects_invalid_policy_or_guard_before_key_read() -> Result<()> {
    let mut f = Fixture::new(Route::Metis, 200_000, 200_000).await?;
    let plan = f.adapter.build_transaction_plan(&f.request)?;
    let key = crate::execution_pumpswap_accounts::parse_pubkey(&f.request.wallet_pubkey, "test")?;
    let good = ExecutionSerializedTransactionPayload {
        source: "arbitrary".into(),
        serialized_transaction_base64: super::priority_fee_fixture::guarded_transaction(
            key, 200_000, 10_000,
        ),
    };
    let mut config = f.config.clone();
    config.execution_signer_keypair_path = "/nonexistent/b25-key-must-not-be-read.json".into();
    for invalid in [
        0.0,
        -0.0,
        -1.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
    ] {
        config.pretrade_min_sol_reserve = invalid;
        let error =
            sign_serialized_transaction_from_config(&config, &f.request, &plan, &good).unwrap_err();
        assert_eq!(error.to_string(), "native_floor_invalid_policy");
    }
    config.pretrade_min_sol_reserve = 0.05;
    let absent = ExecutionSerializedTransactionPayload {
        source: "pumpswap_direct".into(),
        serialized_transaction_base64: super::priority_fee_fixture::transaction(
            key, 200_000, 10_000,
        ),
    };
    assert_eq!(
        sign_serialized_transaction_from_config(&config, &f.request, &plan, &absent)
            .unwrap_err()
            .to_string(),
        "native_floor_program"
    );
    config.pretrade_min_sol_reserve = 0.075;
    assert_eq!(
        sign_serialized_transaction_from_config(&config, &f.request, &plan, &good)
            .unwrap_err()
            .to_string(),
        "native_floor_reserve_mismatch"
    );
    // Empty signer settings cannot sign, even with invalid BUY policy.
    config.execution_signer_keypair_path.clear();
    config.execution_signer_pubkey.clear();
    config.pretrade_min_sol_reserve = f64::NAN;
    assert!(sign_serialized_transaction_from_config(&config, &f.request, &plan, &good)?.is_none());
    // SELL remains signable using the real synthetic key file despite invalid BUY policy.
    config = f.config.clone();
    config.pretrade_min_sol_reserve = f64::NAN;
    let mut sell_request = f.request.clone();
    sell_request.side = "sell".into();
    let mut sell_plan = plan.clone();
    sell_plan.side = "sell".into();
    assert!(
        sign_serialized_transaction_from_config(&config, &sell_request, &sell_plan, &absent)?
            .is_some()
    );
    f.finish().await?;
    assert!(f.calls.lock().unwrap().is_empty());
    Ok(())
}
