use super::priority_fee_route_fixture::{Fixture, Route};
use super::rpc_simulation_http_fixture::valid;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use anyhow::Result;
use serde_json::json;

#[tokio::test]
async fn rpc_simulation_all_three_callers_report_skipped_without_simulation_rpc() -> Result<()> {
    for route in [Route::Metis, Route::Paid, Route::Direct] {
        let mut f = Fixture::new(route, 120_000, 1_000_000).await?;
        f.config.canary_tiny_submit_enabled = false;
        if route != Route::Direct {
            f.config.submit_adapter_http_url.clear();
        }
        let plan = f.adapter.build_transaction_plan(&f.request)?;
        let http = reqwest::Client::new();
        let result = match route {
            Route::Metis => crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(&http, &f.config, &plan).await,
            Route::Paid => crate::execution_pump_fun_swap_transaction_http::fetch_pump_fun_swap_transaction_dry_run(&http, &f.config, &plan).await,
            Route::Direct => crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run(&http, &f.config, &plan).await,
            _ => unreachable!(),
        };
        f.finish().await?;
        let result = result?.unwrap();
        assert!(
            result.summary.ends_with("rpc_simulation=skipped"),
            "{route:?}: {}",
            result.summary
        );
        assert!(!result.summary.contains("rpc_simulation=passed"));
        assert!(result.summary.chars().count() <= 500);
        assert!(f
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, r)| r["method"] != "simulateTransaction"));
        assert_eq!(f.signatures(), 0);
        assert_eq!(f.sends(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn rpc_simulation_all_three_callers_reject_malformed_final_payload() -> Result<()> {
    for route in [Route::Metis, Route::Paid, Route::Direct] {
        let mut f = Fixture::new(route, 120_000, 1_000_000).await?;
        f.simulation_responses.lock().unwrap().push_back(json!({}));
        let plan = f.adapter.build_transaction_plan(&f.request)?;
        let http = reqwest::Client::new();
        let result = match route {
            Route::Metis => crate::execution_swap_transaction_http::fetch_swap_transaction_dry_run(&http, &f.config, &plan).await,
            Route::Paid => crate::execution_pump_fun_swap_transaction_http::fetch_pump_fun_swap_transaction_dry_run(&http, &f.config, &plan).await,
            Route::Direct => crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run(&http, &f.config, &plan).await,
            _ => unreachable!(),
        };
        f.finish().await?;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("RPC simulation invalid"));
        assert!(plan
            .serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .load()?
            .is_none());
        assert_eq!(f.signatures(), 0);
        assert_eq!(f.sends(), 0);
    }
    Ok(())
}

async fn fallback(final_valid: bool) -> Result<()> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let mut f = Fixture::new(Route::Direct, 120_000, 1_000_000).await?;
    // Explicit guarded synthetic fallback preserves this simulation/signature control under B25.
    f.wire.lock().unwrap().guard = Some(50_000_001);
    // Direct PumpSwap and existing generic Metis fallback produce different valid bytes.
    f.simulation_responses
        .lock()
        .unwrap()
        .extend([json!({}), if final_valid { valid() } else { json!({}) }]);
    let build = f.build().await;
    let submit = if let Ok(outcome) = &build {
        if let Some(envelope) = outcome.envelope.as_ref() {
            Some(f.submit(envelope).await)
        } else {
            None
        }
    } else {
        None
    };
    f.finish().await?;
    let calls = f.calls.lock().unwrap();
    let simulations: Vec<_> = calls
        .iter()
        .enumerate()
        .filter(|(_, (_, r))| r["method"] == "simulateTransaction")
        .collect();
    assert_eq!(simulations.len(), 2, "{calls:?}");
    let first = &simulations[0].1 .1["params"][0];
    let second = &simulations[1].1 .1["params"][0];
    assert_ne!(first, second);
    for value in [first, second] {
        crate::execution_priority_fee_wire::decode_priority_fee(value.as_str().unwrap())?;
    }
    assert_eq!(f.signatures(), usize::from(final_valid));
    if final_valid {
        assert_eq!(build?.built, 1);
        assert_eq!(submit.unwrap()?.submitted, 1);
        let sends: Vec<_> = calls
            .iter()
            .enumerate()
            .filter(|(_, (_, r))| r["method"] == "sendTransaction")
            .collect();
        assert_eq!(sends.len(), 1);
        assert!(sends[0].0 > simulations[1].0);
        let sent = STANDARD.decode(sends[0].1 .1["params"][0].as_str().unwrap())?;
        let simulated = STANDARD.decode(second.as_str().unwrap())?;
        assert_eq!(
            &sent[65..],
            &simulated[65..],
            "only independently validated fallback message is signed"
        );
        let sig = ed25519_dalek::Signature::from_slice(&sent[1..65])?;
        let key: [u8; 32] = bs58::decode(&f.config.canary_wallet_pubkey)
            .into_vec()?
            .try_into()
            .unwrap();
        ed25519_dalek::VerifyingKey::from_bytes(&key)?.verify_strict(&sent[65..], &sig)?;
    } else {
        assert!(build
            .unwrap_err()
            .to_string()
            .contains("RPC simulation invalid"));
        assert!(submit.is_none());
        assert!(!calls.iter().any(|(_, r)| r["method"] == "sendTransaction"));
    }
    Ok(())
}

#[tokio::test]
async fn rpc_simulation_existing_fallback_signs_only_its_own_valid_payload() -> Result<()> {
    fallback(true).await
}
#[tokio::test]
async fn rpc_simulation_all_malformed_fallbacks_never_invoke_signer_or_send() -> Result<()> {
    fallback(false).await
}
