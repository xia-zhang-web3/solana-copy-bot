use super::native_ata_fixture::*;
use super::native_rpc_fixture::{Fixture, Reply};
use crate::execution_native_rpc::NativeFundingRpcClient;
use anyhow::Result;
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn native_ata_rpc_scalar_params_ids_exact_values_and_independent_timing() -> Result<()> {
    let client = NativeFundingRpcClient::new()?;
    for rent in [0, 9_007_199_254_740_993, u64::MAX] {
        let f = Fixture::start_with_in_flight(3, move |r| {
            let mut v = rpc_success(r);
            if r["method"] == RENT_METHOD {
                v["result"] = json!(rent);
            }
            if r["method"] == "getFeeForMessage" {
                v["result"]["value"] = Value::Null;
            }
            Reply::json(v)
        })
        .await?;
        let p = payload(&direct(true, false, 17)?)?;
        let result = client
            .collect_with_classic_ata_rent(
                &f.endpoint,
                Duration::from_secs(2),
                &p,
                WALLET,
                Some(69),
            )
            .await;
        let trace = f.finish().await?;
        exact_calls(&trace, 3)?;
        let facts = result?;
        let r = trace
            .iter()
            .find(|r| r.request["method"] == RENT_METHOD)
            .unwrap();
        assert_eq!(
            r.request,
            json!({"jsonrpc":"2.0","id":RENT_ID,"method":RENT_METHOD,"params":[165,{"commitment":"confirmed"}]})
        );
        assert_eq!(facts.rent().data_length(), 165);
        assert_eq!(facts.rent().commitment(), "confirmed");
        assert_eq!(facts.rent().lamports(), rent);
        assert_eq!(facts.native().fee().value, None);
        assert_eq!(
            (facts.native().fee().slot, facts.native().accounts().slot),
            (70, 72)
        );
        assert_eq!(facts.native().min_context_slot(), Some(69));
        assert!(facts.rent().timing().elapsed <= facts.native().timing().elapsed);
        // The scalar type exposes no slot. Timing is independent, with no age/atomicity claim.
        assert_eq!(facts.clone(), facts);
    }
    let f = Fixture::start(true, |r| Reply::json(rpc_success(r))).await?;
    let result = client
        .collect(
            &f.endpoint,
            Duration::from_secs(2),
            &payload(&budget())?,
            WALLET,
            None,
        )
        .await;
    let trace = f.finish().await?;
    exact_calls(&trace, 2)?;
    result?;
    assert!(!trace.iter().any(|r| r.request["method"] == RENT_METHOD));
    Ok(())
}

#[tokio::test]
async fn native_ata_rpc_malformed_scalar_and_envelope_reject_without_partial_bundle() -> Result<()>
{
    let mut cases = vec![
        json!(null),
        json!("17"),
        json!(-1),
        json!(1.5),
        json!(true),
        json!([]),
        json!({"context":{"slot":90},"value":17}),
    ];
    cases.push(serde_json::from_str("18446744073709551616")?);
    for (case, value) in cases.into_iter().enumerate() {
        let f = Fixture::start_with_in_flight(3, move |r| {
            let mut v = rpc_success(r);
            if r["method"] == RENT_METHOD {
                v["result"] = value.clone();
            }
            Reply::json(v)
        })
        .await?;
        let result = NativeFundingRpcClient::new()?
            .collect_with_classic_ata_rent(
                &f.endpoint,
                Duration::from_secs(2),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let trace = f.finish().await?;
        exact_calls(&trace, 3)?;
        assert!(
            format!("{:#}", result.unwrap_err()).contains("native_rpc_rent_value"),
            "scalar case={case}"
        );
    }
    for case in [
        "missing",
        "id",
        "version",
        "error_null",
        "error_object",
        "root_array",
    ] {
        let f = Fixture::start_with_in_flight(3, move |r| {
            let mut v = rpc_success(r);
            if r["method"] == RENT_METHOD {
                match case {
                    "missing" => {
                        v.as_object_mut().unwrap().remove("result");
                    }
                    "id" => v["id"] = json!("native-funding-fee"),
                    "version" => v["jsonrpc"] = json!("1.0"),
                    "error_null" => v["error"] = Value::Null,
                    "error_object" => v["error"] = json!({"message":"PRIVATE_BODY"}),
                    _ => v = json!([]),
                }
            }
            Reply::json(v)
        })
        .await?;
        let result = NativeFundingRpcClient::new()?
            .collect_with_classic_ata_rent(
                &f.endpoint,
                Duration::from_secs(2),
                &payload(&budget())?,
                WALLET,
                None,
            )
            .await;
        let trace = f.finish().await?;
        exact_calls(&trace, 3)?;
        let error = format!("{:#}", result.unwrap_err());
        let reason = match case {
            "missing" => "result",
            "id" => "id",
            "version" => "jsonrpc",
            "error_null" | "error_object" => "error_key",
            _ => "envelope",
        };
        assert!(
            error.contains(&format!("native_rpc_{reason}")),
            "{case}: {error}"
        );
        assert!(!error.contains("PRIVATE_BODY"));
    }
    Ok(())
}

#[tokio::test]
async fn native_ata_rpc_pre_io_validation_sends_no_requests() -> Result<()> {
    let client = NativeFundingRpcClient::new()?;
    let p = payload(&budget())?;
    for (input, wallet, timeout) in [
        ("???".into(), WALLET, Duration::from_secs(2)),
        ("x".repeat(1645), WALLET, Duration::from_secs(2)),
        (p.clone(), PEER, Duration::from_secs(2)),
        (p.clone(), WALLET, Duration::ZERO),
        (p.clone(), WALLET, Duration::from_secs(31)),
        (p.clone(), WALLET, Duration::from_nanos(1)),
    ] {
        let f = Fixture::start(false, |r| Reply::json(rpc_success(r))).await?;
        let result = client
            .collect_with_classic_ata_rent(&f.endpoint, timeout, &input, wallet, None)
            .await;
        let trace = f.finish().await?;
        assert!(trace.is_empty());
        assert!(result.is_err());
    }
    Ok(())
}
