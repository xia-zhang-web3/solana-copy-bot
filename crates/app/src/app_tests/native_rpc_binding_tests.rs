use super::native_funding_fixture::*;
use super::native_rpc_fixture::*;
use crate::execution_native_funding::{decode_native_funding_requirements, types::*};
use crate::execution_native_rpc::{types::*, NativeFundingRpcClient};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn native_rpc_actual_direct_buy_sell_extension_exact_message_keys_and_unknowns() -> Result<()>
{
    for buy in [false, true] {
        for extension in [false, true] {
            let payload = payload(&direct(buy, extension, 10_000_000)?)?;
            let decoded = decode_native_funding_requirements(&payload, WALLET)?;
            let f = Fixture::start(true, |request| Reply::json(success(request))).await?;
            let before = SystemTime::now();
            let result = NativeFundingRpcClient::new()?
                .collect(
                    &f.endpoint,
                    Duration::from_secs(2),
                    &payload,
                    WALLET,
                    Some(69),
                )
                .await;
            let calls = f.finish().await?;
            let value = result?;
            assert_eq!(calls.len(), 2);
            assert_eq!(value.requirements(), &decoded);
            assert_eq!(value.commitment(), "confirmed");
            assert_eq!(value.min_context_slot(), Some(69));
            assert_eq!((value.fee().slot, value.accounts().slot), (70, 72));
            assert_eq!(value.fee().value, Some(19_000));
            assert_eq!(value.requirements().encoded_priority_fee.total, 14_000);
            assert_eq!(
                value.requirements().unavailable_budget,
                UnavailableNativeBudget::default()
            );
            assert_eq!(
                value.requirements().coverage,
                FundingCoverage::PartialWithStateOrUnsupported
            );
            assert_eq!(value.observed_payer_lamports(), Some(u64::MAX));
            let keys: Vec<_> = decoded.binding.accounts.iter().map(|a| a.pubkey).collect();
            assert_eq!(value.requested_keys(), keys);
            assert_eq!(value.accounts().value.len(), keys.len());
            for (index, observation) in value.accounts().value.iter().enumerate() {
                assert_eq!(observation.pubkey, keys[index]);
                assert_eq!(
                    observation.account,
                    match index {
                        0 => AccountObservation::Present {
                            lamports: u64::MAX,
                            owner_program: [171; 32],
                            executable: false,
                            data: vec![0, 255, 17]
                        },
                        1 => AccountObservation::Absent,
                        _ => AccountObservation::Present {
                            lamports: 0,
                            owner_program: [171; 32],
                            executable: false,
                            data: vec![]
                        },
                    }
                );
            }
            for call in &calls {
                let request = &call.request;
                assert_eq!(request["jsonrpc"], "2.0");
                if request["method"] == "getFeeForMessage" {
                    assert_eq!(request["id"], FEE_ID);
                    assert_eq!(
                        request["params"],
                        json!([STANDARD.encode(&decoded.binding.message_bytes), {"commitment":"confirmed", "minContextSlot":69}])
                    );
                    assert_ne!(request["params"][0], payload);
                } else {
                    assert_eq!(request["method"], "getMultipleAccounts");
                    assert_eq!(request["id"], ACCOUNTS_ID);
                    assert_eq!(
                        request["params"],
                        json!([keys.iter().map(|key| bs58::encode(key).into_string()).collect::<Vec<_>>(), {"commitment":"confirmed", "minContextSlot":69, "encoding":"base64"}])
                    );
                }
            }
            for timing in [
                value.timing(),
                &value.fee().timing,
                &value.accounts().timing,
            ] {
                assert!(timing.started_at >= before && timing.completed_at >= timing.started_at);
                assert!(timing.elapsed > Duration::ZERO && timing.elapsed < Duration::from_secs(2));
            }
            assert!(value.timing().elapsed >= value.fee().timing.elapsed);
            assert!(value.timing().elapsed >= value.accounts().timing.elapsed);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_binding_signatures_blockhash_instruction_and_version() -> Result<()> {
    let base = payload(&direct(true, false, 17)?)?;
    let mut signatures = STANDARD.decode(&base)?;
    signatures[1..65].fill(127);
    let mut blockhash = STANDARD.decode(&base)?;
    let count = usize::from(blockhash[68]);
    blockhash[69 + count * 32] ^= 1;
    let changed_instruction = payload(&direct(true, false, 18)?)?;
    let variants = [
        base.clone(),
        STANDARD.encode(signatures),
        STANDARD.encode(blockhash),
        changed_instruction,
        version_zero(&base)?,
    ];
    let mut facts = Vec::new();
    let mut messages = Vec::new();
    for payload in variants {
        let f = Fixture::start(true, |request| Reply::json(success(request))).await?;
        let result = NativeFundingRpcClient::new()?
            .collect(&f.endpoint, Duration::from_secs(2), &payload, WALLET, None)
            .await;
        let calls = f.finish().await?;
        let value = result?;
        assert_eq!(calls.len(), 2);
        assert!(calls
            .iter()
            .all(|c| c.request["params"][1].get("minContextSlot").is_none()));
        let fee = calls
            .iter()
            .find(|c| c.request["method"] == "getFeeForMessage")
            .unwrap();
        assert_eq!(
            STANDARD.decode(fee.request["params"][0].as_str().unwrap())?,
            value.requirements().binding.message_bytes
        );
        messages.push(fee.request["params"][0].clone());
        facts.push(value);
    }
    assert_eq!(messages[0], messages[1]);
    assert_eq!(
        facts[0].requirements().requirements,
        facts[1].requirements().requirements
    );
    assert_eq!(
        facts[0].requirements().binding.message_sha256,
        facts[1].requirements().binding.message_sha256
    );
    assert_ne!(
        facts[0].requirements().binding.transaction_sha256,
        facts[1].requirements().binding.transaction_sha256
    );
    for index in 2..5 {
        assert_ne!(messages[0], messages[index]);
        assert_ne!(
            facts[0].requirements().binding.message_sha256,
            facts[index].requirements().binding.message_sha256
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_invalid_wallet_wire_and_local_bounds_send_no_requests() -> Result<()> {
    let valid = payload(&budget())?;
    let mut trailing = STANDARD.decode(&valid)?;
    trailing.push(0);
    for (payload, wallet, timeout, reason) in [
        (
            valid.clone(),
            PEER,
            Duration::from_secs(1),
            "native_rpc_invalid_requirements",
        ),
        (
            "not-base64".to_owned(),
            WALLET,
            Duration::from_secs(1),
            "native_rpc_invalid_requirements",
        ),
        (
            STANDARD.encode(trailing),
            WALLET,
            Duration::from_secs(1),
            "native_rpc_invalid_requirements",
        ),
        (
            "A".repeat(1645),
            WALLET,
            Duration::from_secs(1),
            "native_rpc_payload_too_large",
        ),
        (
            valid.clone(),
            WALLET,
            Duration::ZERO,
            "native_rpc_timeout_bounds",
        ),
        (
            valid,
            WALLET,
            Duration::from_secs(31),
            "native_rpc_timeout_bounds",
        ),
    ] {
        let f = Fixture::start(false, |r| Reply::json(success(r))).await?;
        let result = NativeFundingRpcClient::new()?
            .collect(&f.endpoint, timeout, &payload, wallet, None)
            .await;
        let calls = f.finish().await?;
        assert!(calls.is_empty());
        assert_eq!(result.unwrap_err().to_string(), reason);
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_two_inflight_reverse_completion_preserves_attribution() -> Result<()> {
    let f = Fixture::start(true, |r| {
        let mut reply = Reply::json(success(r));
        if r["method"] == "getFeeForMessage" {
            reply.delay = Duration::from_millis(40);
        }
        reply
    })
    .await?;
    let result = NativeFundingRpcClient::new()?
        .collect(
            &f.endpoint,
            Duration::from_secs(2),
            &payload(&budget())?,
            WALLET,
            None,
        )
        .await;
    let calls = f.finish().await?;
    let value = result?;
    assert_eq!(calls.len(), 2);
    let fee = calls.iter().find(|c| c.request["id"] == FEE_ID).unwrap();
    let accounts = calls
        .iter()
        .find(|c| c.request["id"] == ACCOUNTS_ID)
        .unwrap();
    assert!(
        fee.received < accounts.completed.unwrap() && accounts.received < fee.completed.unwrap()
    );
    assert!(accounts.completed < fee.completed);
    assert!(value.accounts().timing.completed_at < value.fee().timing.completed_at);
    assert_eq!(
        (value.fee().slot, value.fee().value, value.accounts().slot),
        (70, Some(19_000), 72)
    );
    assert_eq!(value.observed_payer_lamports(), Some(u64::MAX));
    Ok(())
}
