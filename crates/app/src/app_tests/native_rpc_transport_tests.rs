use super::native_funding_fixture::*;
use super::native_rpc_fixture::*;
use super::native_rpc_reuse_fixture::ReuseFixture;
use crate::execution_native_funding::types::UnavailableNativeBudget;
use crate::execution_native_rpc::NativeFundingRpcClient;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn native_rpc_factory_refuses_307_308_without_contacting_location() -> Result<()> {
    let client = NativeFundingRpcClient::new()?;
    for status in [307, 308] {
        let target = Fixture::start(false, |r| Reply::json(success(r))).await?;
        let location = target.endpoint.clone();
        let origin = Fixture::start(true, move |r| {
            let mut reply = Reply::json(success(r));
            reply.status = status;
            reply.location = Some(location.clone());
            reply
        })
        .await?;
        let result = client
            .collect(
                &origin.endpoint,
                Duration::from_secs(2),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let origin_done = origin.finish().await;
        let target_done = target.finish().await;
        let origin_calls = origin_done?;
        let target_calls = target_done?;
        assert_eq!(origin_calls.len(), 2);
        assert_eq!(
            origin_calls
                .iter()
                .filter(|c| c.request["method"] == "getFeeForMessage")
                .count(),
            1
        );
        assert_eq!(
            origin_calls
                .iter()
                .filter(|c| c.request["method"] == "getMultipleAccounts")
                .count(),
            1
        );
        assert!(target_calls.is_empty());
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("native_rpc_http_status"), "{error}");
        assert!(!error.contains("private-endpoint-secret"));
    }
    // The same wrapper remains usable after both refusals.
    let f = Fixture::start(true, |r| Reply::json(success(r))).await?;
    let result = client
        .collect(
            &f.endpoint,
            Duration::from_secs(2),
            &payload(&budget())?,
            WALLET,
            None,
        )
        .await;
    let calls = f.finish().await?;
    let facts = result?;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        (facts.fee().value, facts.accounts().slot),
        (Some(19_000), 72)
    );
    Ok(())
}

#[tokio::test]
async fn native_rpc_wrapper_and_clone_reuse_pool_with_separate_message_observations() -> Result<()>
{
    let client = NativeFundingRpcClient::new()?;
    let cloned = client.clone();
    let f = ReuseFixture::start().await?;
    let first = payload(&direct(true, false, 17)?)?;
    let mut signatures = STANDARD.decode(&first)?;
    signatures[1..65].fill(127);
    let inputs = [
        first,
        STANDARD.encode(signatures),
        payload(&direct(false, true, 18)?)?,
    ];
    let mut results = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        let owner = if index == 1 { &cloned } else { &client };
        results.push(
            owner
                .collect(&f.endpoint, Duration::from_secs(2), input, WALLET, Some(69))
                .await,
        );
    }
    let trace = f.finish().await?;
    let facts: Vec<_> = results.into_iter().collect::<Result<_>>()?;
    assert_eq!(
        trace.connections, 2,
        "six RPCs reuse the two parallel connections, including clone"
    );
    assert_eq!(trace.requests.len(), 6);
    for id in [1, 2] {
        assert_eq!(
            trace
                .requests
                .iter()
                .filter(|(socket, _)| *socket == id)
                .count(),
            3
        );
    }
    for (index, value) in facts.iter().enumerate() {
        let sequence = index as u64;
        assert_eq!(
            (value.fee().slot, value.accounts().slot, value.fee().value),
            (70 + sequence, 72 + sequence, Some(19_000 + sequence))
        );
        assert_eq!(value.observed_payer_lamports(), Some(u64::MAX - sequence));
        assert_eq!(
            value.requirements().unavailable_budget,
            UnavailableNativeBudget::default()
        );
        let requests = &trace.requests[index * 2..index * 2 + 2];
        let fee = &requests
            .iter()
            .find(|(_, r)| r["method"] == "getFeeForMessage")
            .unwrap()
            .1;
        let accounts = &requests
            .iter()
            .find(|(_, r)| r["method"] == "getMultipleAccounts")
            .unwrap()
            .1;
        assert_eq!(
            fee["params"][0],
            STANDARD.encode(&value.requirements().binding.message_bytes)
        );
        assert_eq!(
            accounts["params"][0],
            json!(value
                .requested_keys()
                .iter()
                .map(|key| bs58::encode(key).into_string())
                .collect::<Vec<_>>())
        );
        assert_eq!(value.requirements().expected_wallet, WALLET);
    }
    assert_eq!(
        facts[0].requirements().binding.message_sha256,
        facts[1].requirements().binding.message_sha256
    );
    assert_ne!(
        facts[0].requirements().binding.transaction_sha256,
        facts[1].requirements().binding.transaction_sha256
    );
    assert_ne!(
        facts[0].requirements().binding.message_sha256,
        facts[2].requirements().binding.message_sha256
    );
    Ok(())
}
