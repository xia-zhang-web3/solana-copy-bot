use super::native_rpc_fixture::{success, Fixture as RpcFixture, Reply};
use super::native_setup_fixture::*;
use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_native_funding::{
    decode_native_funding_requirements, types::FundingOperation,
};
use crate::execution_native_rpc::NativeFundingRpcClient;
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn native_setup_actual_runtime_builder_loopback_collector_interpreter_no_extra_rpc(
) -> Result<()> {
    let client = NativeFundingRpcClient::new()?;
    for sell in [false, true] {
        let mut builder = Fixture::new(Route::Direct, 10_000, 1_400_000).await?;
        if sell {
            builder.make_sell()?;
        }
        let plan = builder.adapter.build_transaction_plan(&builder.request)?;
        let built =
            crate::execution_pumpswap_direct_builder::fetch_pumpswap_direct_transaction_dry_run(
                &reqwest::Client::new(),
                &builder.config,
                &plan,
            )
            .await;
        builder.finish().await?;
        let built = built?.context("direct builder result")?;
        let wallet = parse_pubkey(&builder.config.canary_wallet_pubkey, "fixture")?;
        let requirements =
            decode_native_funding_requirements(&built.serialized_transaction_base64, wallet)?;
        let keys: Vec<_> = requirements
            .binding
            .accounts
            .iter()
            .map(|a| bs58::encode(a.pubkey).into_string())
            .collect();
        let mut account_values = vec![Value::Null; keys.len()];
        for r in &requirements.requirements {
            if let FundingOperation::AssociatedTokenCreateIdempotent {
                associated_account,
                mint,
                owner,
                ..
            } = &r.operation
            {
                let i = keys
                    .iter()
                    .position(|k| *k == bs58::encode(associated_account).into_string())
                    .unwrap();
                let bytes = token_bytes(*mint, *owner, 7, (*mint == wsol_mint()).then_some(9));
                account_values[i] = json!({"lamports":(1_u64 << 53) + 1, "owner":bs58::encode(token_program_id()).into_string(), "executable":false, "data":[STANDARD.encode(&bytes),"base64"], "space":bytes.len()});
            }
        }
        let accounts_expected = account_values.clone();
        let rpc = RpcFixture::start(true, move |request| {
            let mut reply = success(request);
            if request["method"] == "getMultipleAccounts" {
                reply["result"]["value"] = json!(account_values);
            } else {
                reply["result"]["value"] = json!(if sell { 0 } else { u64::MAX });
            }
            Reply::json(reply)
        })
        .await?;
        let observed = client
            .collect(
                &rpc.endpoint,
                Duration::from_secs(2),
                &built.serialized_transaction_base64,
                wallet,
                Some(69),
            )
            .await;
        // Interpret while the listener is still alive so any accidental extra RPC is counted.
        let interpreted = observed
            .as_ref()
            .ok()
            .map(|facts| interpret(&built.serialized_transaction_base64, wallet, facts));
        let calls = rpc.finish().await?;
        let facts = observed.as_ref().map_err(|e| anyhow::anyhow!("{e}"))?;
        let result = interpreted.context("collected facts")??;
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|c| c.completed.is_some()));
        for method in ["getFeeForMessage", "getMultipleAccounts"] {
            assert_eq!(
                calls
                    .iter()
                    .filter(|c| c.request["method"] == method)
                    .count(),
                1
            );
        }
        let fee = &calls
            .iter()
            .find(|c| c.request["method"] == "getFeeForMessage")
            .unwrap()
            .request;
        let accounts = &calls
            .iter()
            .find(|c| c.request["method"] == "getMultipleAccounts")
            .unwrap()
            .request;
        assert_eq!(
            STANDARD.decode(fee["params"][0].as_str().unwrap())?,
            requirements.binding.message_bytes
        );
        assert_eq!(accounts["params"][0], json!(keys));
        for request in [fee, accounts] {
            assert_eq!(request["params"][1]["commitment"], "confirmed");
            assert_eq!(request["params"][1]["minContextSlot"], 69);
        }
        assert!(std::ptr::eq(result.facts, facts));
        assert_eq!(facts.requirements(), &requirements);
        assert_eq!((facts.fee().slot, facts.accounts().slot), (70, 72));
        assert_eq!(facts.fee().value, Some(if sell { 0 } else { u64::MAX }));
        for account in &result.initial_accounts {
            assert_eq!(account.observed_lamports, Some((1_u64 << 53) + 1));
            let AccountObservation::Present { data, .. } =
                &facts.accounts().value[account.observation_index].account
            else {
                panic!("present");
            };
            assert_eq!(
                STANDARD.encode(data),
                accounts_expected[account.observation_index]["data"][0]
            );
        }
        assert!(result.instructions.iter().any(|r| r.interpretation
            == SetupOperation::Associated(AssociatedInitialState::ExistingIdentityMatch)));
        assert_unknown(&result);
        assert_eq!(builder.signatures(), 0);
        assert_eq!(builder.sends(), 0);
    }
    Ok(())
}
