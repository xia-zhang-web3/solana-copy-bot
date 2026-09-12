use super::native_rpc_fixture::{success, Fixture as RpcFixture, Reply};
use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_native_funding::types::UnavailableNativeBudget;
use crate::execution_native_rpc::{types::AccountObservation, NativeFundingRpcClient};
use crate::execution_pumpswap_accounts::parse_pubkey;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use std::time::Duration;

#[tokio::test]
async fn native_rpc_actual_http_direct_builder_simulation_then_separate_collector() -> Result<()> {
    for sell in [false, true] {
        let mut builder = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
        if sell {
            builder.make_sell()?;
        }
        let plan = builder.adapter.build_transaction_plan(&builder.request)?;
        let result =
            crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run(
                &reqwest::Client::new(),
                &builder.config,
                &plan,
            )
            .await;
        builder.finish().await?;
        let built = result?.unwrap();
        assert!(built.summary.ends_with("rpc_simulation=passed"));
        let previous_calls = builder.calls.lock().unwrap().clone();
        let wallet = parse_pubkey(&builder.config.canary_wallet_pubkey, "fixture wallet")?;
        let rpc = RpcFixture::start(true, |r| Reply::json(success(r))).await?;
        let result = NativeFundingRpcClient::new()?
            .collect(
                &rpc.endpoint,
                Duration::from_secs(2),
                &built.serialized_transaction_base64,
                wallet,
                Some(69),
            )
            .await;
        let calls = rpc.finish().await?;
        let facts = result?;
        assert_eq!(calls.len(), 2);
        let fee = calls
            .iter()
            .find(|c| c.request["method"] == "getFeeForMessage")
            .unwrap();
        assert_eq!(
            STANDARD.decode(fee.request["params"][0].as_str().unwrap())?,
            facts.requirements().binding.message_bytes
        );
        assert_eq!(facts.requirements().expected_wallet, wallet);
        assert_eq!(
            facts
                .requirements()
                .nominal_wallet_source_transfer_operands_lamports,
            if sell { 0 } else { 10_000_000 + 50_000_001 }
        );
        assert_eq!(facts.fee().value, Some(19_000));
        assert_eq!(facts.requirements().encoded_priority_fee.total, 14_000);
        assert_eq!(
            facts.requirements().unavailable_budget,
            UnavailableNativeBudget::default()
        );
        assert_eq!(facts.accounts().value[0].pubkey, wallet);
        assert!(matches!(
            facts.accounts().value[1].account,
            AccountObservation::Absent
        ));
        assert_eq!(*builder.calls.lock().unwrap(), previous_calls);
        assert_eq!(builder.signatures(), 0);
        assert_eq!(builder.sends(), 0);
    }
    Ok(())
}
