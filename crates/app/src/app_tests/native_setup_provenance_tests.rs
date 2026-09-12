use super::native_rpc_fixture::{success, Fixture, Reply};
use super::native_setup_fixture::{assert_unknown, direct, interpret, payload, WALLET};
use crate::execution_native_rpc::{types::AccountObservation, NativeFundingRpcClient};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn native_setup_closed_facts_sequential_same_keys_keep_separate_observations() -> Result<()> {
    same_keys(false).await
}

#[tokio::test]
async fn native_setup_closed_facts_concurrent_client_clone_keep_separate_observations() -> Result<()>
{
    same_keys(true).await
}

async fn same_keys(concurrent: bool) -> Result<()> {
    let a = payload(&direct(true, false, 7)?)?;
    let b = payload(&direct(true, false, 9)?)?;
    let client = NativeFundingRpcClient::new()?;
    let clone = client.clone();
    for (fee_a, fee_b) in [
        (Some(7), Some(9)),
        (None, Some(0)),
        (Some(0), Some(u64::MAX)),
    ] {
        let server_a = server(fee_a, 100, 70, 72, 3).await?;
        let server_b = server(fee_b, u64::MAX, 85, 83, 7).await?;
        let future_a = client.collect(
            &server_a.endpoint,
            Duration::from_secs(2),
            &a,
            WALLET,
            Some(69),
        );
        let future_b = clone.collect(
            &server_b.endpoint,
            Duration::from_secs(2),
            &b,
            WALLET,
            Some(81),
        );
        let (result_a, result_b) = if concurrent {
            tokio::join!(future_a, future_b)
        } else {
            (future_a.await, future_b.await)
        };
        let (calls_a, calls_b) = tokio::join!(server_a.finish(), server_b.finish());
        let (calls_a, calls_b) = (calls_a?, calls_b?);
        let (facts_a, facts_b) = (result_a?, result_b?);
        for (value, encoded, fee, balance, slots, floor, tag, calls) in [
            (&facts_a, &a, fee_a, 100, (70, 72), 69, 3, &calls_a),
            (&facts_b, &b, fee_b, u64::MAX, (85, 83), 81, 7, &calls_b),
        ] {
            assert_eq!(calls.len(), 2);
            for method in ["getFeeForMessage", "getMultipleAccounts"] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|c| c.request["method"] == method)
                        .count(),
                    1
                );
            }
            assert!(calls.iter().all(|c| c.completed.is_some()));
            let fee_request = &calls
                .iter()
                .find(|c| c.request["method"] == "getFeeForMessage")
                .unwrap()
                .request;
            let account_request = &calls
                .iter()
                .find(|c| c.request["method"] == "getMultipleAccounts")
                .unwrap()
                .request;
            assert_eq!(
                STANDARD.decode(fee_request["params"][0].as_str().unwrap())?,
                value.requirements().binding.message_bytes
            );
            assert_eq!(
                account_request["params"][0],
                json!(value
                    .requested_keys()
                    .iter()
                    .map(|k| bs58::encode(k).into_string())
                    .collect::<Vec<_>>())
            );
            for request in [fee_request, account_request] {
                assert_eq!(request["params"][1]["minContextSlot"], floor);
                assert_eq!(request["params"][1]["commitment"], "confirmed");
            }
            assert_eq!(value.fee().value, fee);
            assert_eq!(value.observed_payer_lamports(), Some(balance));
            assert_eq!((value.fee().slot, value.accounts().slot), slots);
            assert_eq!(value.min_context_slot(), Some(floor));
            assert_eq!(value.commitment(), "confirmed");
            let AccountObservation::Present { lamports, data, .. } =
                &value.accounts().value[0].account
            else {
                panic!("payer");
            };
            assert_eq!((*lamports, data.as_slice()), (balance, &[tag][..]));
            assert!(value.timing().elapsed >= value.fee().timing.elapsed);
            assert!(value.timing().elapsed >= value.accounts().timing.elapsed);
            let interpreted = interpret(encoded, WALLET, value)?;
            assert!(std::ptr::eq(interpreted.facts, value));
            assert_unknown(&interpreted);
            let copied = value.clone();
            assert_eq!(&copied, value);
            assert_unknown(&interpret(encoded, WALLET, &copied)?);
            // Public copies can be changed, but this cannot replace anything in facts.
            let mut detached = value.accounts().clone();
            detached.value.clear();
            assert_eq!(value.accounts().value.len(), value.requested_keys().len());
        }
        assert_eq!(facts_a.requested_keys(), facts_b.requested_keys());
        assert_ne!(
            facts_a.requirements().binding.message_bytes,
            facts_b.requirements().binding.message_bytes
        );
        assert_eq!(
            interpret(&a, WALLET, &facts_b).unwrap_err().to_string(),
            "native_setup_requirements_mismatch"
        );
        assert_eq!(
            interpret(&b, WALLET, &facts_a).unwrap_err().to_string(),
            "native_setup_requirements_mismatch"
        );
    }
    Ok(())
}

async fn server(
    fee: Option<u64>,
    balance: u64,
    fee_slot: u64,
    account_slot: u64,
    tag: u8,
) -> Result<Fixture> {
    Fixture::start(true, move |request| {
        let is_fee = request["method"] == "getFeeForMessage";
        let mut value = success(request);
        value["result"]["context"]["slot"] = json!(if is_fee { fee_slot } else { account_slot });
        if is_fee {
            value["result"]["value"] = json!(fee);
        } else {
            value["result"]["value"][0]["lamports"] = json!(balance);
            value["result"]["value"][0]["data"] = json!([STANDARD.encode([tag]), "base64"]);
        }
        let mut reply = Reply::json(value);
        reply.delay = Duration::from_millis(u64::from(tag));
        reply
    })
    .await
}
