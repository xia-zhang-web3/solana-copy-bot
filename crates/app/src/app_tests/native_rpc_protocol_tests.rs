use super::native_funding_fixture::*;
use super::native_rpc_fixture::*;
use crate::execution_native_funding::types::UnavailableNativeBudget;
use crate::execution_native_rpc::{types::*, NativeFundingRpcClient};
use anyhow::Result;
use serde_json::{json, Value};
use std::time::Duration;

fn original(fee: bool) -> Value {
    success(
        &json!({"method":if fee {"getFeeForMessage"} else {"getMultipleAccounts"}, "params":[["a","b"]]}),
    )
}

async fn run(
    fee: bool,
    replacement: Value,
    floor: Option<u64>,
) -> Result<Result<NativeFundingRpcFacts>> {
    let f = Fixture::start(true, move |r| {
        Reply::json(if (r["method"] == "getFeeForMessage") == fee {
            replacement.clone()
        } else {
            success(r)
        })
    })
    .await?;
    let result = NativeFundingRpcClient::new()?
        .collect(
            &f.endpoint,
            Duration::from_secs(2),
            &payload(&budget())?,
            WALLET,
            floor,
        )
        .await;
    let calls = f.finish().await?;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls
            .iter()
            .filter(|c| c.request["method"] == "getFeeForMessage")
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| c.request["method"] == "getMultipleAccounts")
            .count(),
        1
    );
    Ok(result)
}

#[tokio::test]
async fn native_rpc_fee_null_zero_exact_large_and_max_are_distinct() -> Result<()> {
    for fee in [None, Some(0), Some(9_007_199_254_740_993), Some(u64::MAX)] {
        let mut reply = original(true);
        reply["result"]["value"] = json!(fee);
        let facts = run(true, reply, None).await??;
        assert_eq!(facts.fee().value, fee);
        assert_eq!(facts.fee().slot, 70);
        assert_eq!(facts.requirements().encoded_priority_fee.total, 120_000);
        assert_eq!(
            facts.requirements().unavailable_budget,
            UnavailableNativeBudget::default()
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_strict_envelope_for_both_methods() -> Result<()> {
    for fee in [false, true] {
        let base = original(fee);
        let mut cases = vec![
            (json!(null), "envelope"),
            (json!([]), "envelope"),
            (json!("rpc-body-secret"), "envelope"),
        ];
        for (field, reason, values) in [
            (
                "jsonrpc",
                "jsonrpc",
                vec![json!(null), json!(2), json!("1.0")],
            ),
            (
                "id",
                "id",
                vec![
                    json!(null),
                    json!(1),
                    json!(if fee { ACCOUNTS_ID } else { FEE_ID }),
                ],
            ),
            ("result", "result", vec![json!(null), json!([]), json!(1)]),
        ] {
            let mut absent = base.clone();
            absent.as_object_mut().unwrap().remove(field);
            cases.push((absent, reason));
            for value in values {
                let mut r = base.clone();
                r[field] = value;
                cases.push((r, reason));
            }
        }
        for error in [
            json!(null),
            json!({"secret":"rpc-body-secret"}),
            json!(false),
        ] {
            let mut r = base.clone();
            r["error"] = error;
            cases.push((r, "error_key"));
        }
        let mut absent = base.clone();
        absent["result"].as_object_mut().unwrap().remove("context");
        cases.push((absent, "context"));
        for context in [json!(null), json!([]), json!(true)] {
            let mut r = base.clone();
            r["result"]["context"] = context;
            cases.push((r, "context"));
        }
        let mut absent = base.clone();
        absent["result"]["context"]
            .as_object_mut()
            .unwrap()
            .remove("slot");
        cases.push((absent, "slot"));
        for slot in [
            json!(null),
            json!("70"),
            json!(-1),
            json!(0.0),
            json!(1.5),
            serde_json::from_str("18446744073709551616")?,
        ] {
            let mut r = base.clone();
            r["result"]["context"]["slot"] = slot;
            cases.push((r, "slot"));
        }
        let mut absent = base.clone();
        absent["result"].as_object_mut().unwrap().remove("value");
        cases.push((absent, "missing_value"));
        for value in [
            json!(true),
            json!("70"),
            json!(-1),
            json!(0.0),
            json!(1.5),
            serde_json::from_str("18446744073709551616")?,
        ] {
            let mut r = base.clone();
            r["result"]["value"] = value;
            cases.push((r, if fee { "fee_value" } else { "accounts_value" }));
        }
        if fee {
            for value in [json!([]), json!({})] {
                let mut r = base.clone();
                r["result"]["value"] = value;
                cases.push((r, "fee_value"));
            }
        } else {
            for (value, reason) in [
                (json!(null), "accounts_value"),
                (json!({}), "accounts_value"),
                (json!([]), "accounts_length"),
                (json!([null]), "accounts_length"),
                (json!([null, null, null]), "accounts_length"),
            ] {
                let mut r = base.clone();
                r["result"]["value"] = value;
                cases.push((r, reason));
            }
        }
        for (reply, reason) in cases {
            let error = format!("{:#}", run(fee, reply, None).await?.unwrap_err());
            assert!(
                error.contains(&format!("native_rpc_{reason}")),
                "{error}, expected {reason}, fee={fee}"
            );
            assert!(
                !error.contains("rpc-body-secret") && !error.contains("private-endpoint-secret")
            );
            assert!(error.len() < 150);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_context_zero_max_floor_and_distinct_slots() -> Result<()> {
    for fee in [false, true] {
        for slot in [0, u64::MAX] {
            let mut r = original(fee);
            r["result"]["context"]["slot"] = json!(slot);
            let facts = run(fee, r, Some(0)).await??;
            assert_eq!(
                if fee {
                    facts.fee().slot
                } else {
                    facts.accounts().slot
                },
                slot
            );
        }
        let mut r = original(fee);
        r["result"]["context"]["slot"] = json!(68);
        assert!(format!("{:#}", run(fee, r, Some(69)).await?.unwrap_err())
            .contains("native_rpc_slot_below_floor"));
    }
    let f = Fixture::start(true, |r| {
        let mut value = success(r);
        value["result"]["context"]["slot"] = json!(u64::MAX);
        Reply::json(value)
    })
    .await?;
    let result = NativeFundingRpcClient::new()?
        .collect(
            &f.endpoint,
            Duration::from_secs(2),
            &payload(&budget())?,
            WALLET,
            Some(u64::MAX),
        )
        .await;
    let calls = f.finish().await?;
    let value = result?;
    assert_eq!(
        (value.fee().slot, value.accounts().slot),
        (u64::MAX, u64::MAX)
    );
    assert_eq!(calls.len(), 2);
    assert!(calls
        .iter()
        .all(|c| c.request["params"][1]["minContextSlot"] == json!(u64::MAX)));
    assert_eq!(
        value.requirements().unavailable_budget,
        UnavailableNativeBudget::default()
    );
    Ok(())
}

#[tokio::test]
async fn native_rpc_payer_absent_zero_present_unexpected_program_owner_and_full_data() -> Result<()>
{
    for (row, expected) in [
        (json!(null), AccountObservation::Absent),
        (
            account(0, &[]),
            AccountObservation::Present {
                lamports: 0,
                owner_program: [171; 32],
                executable: false,
                data: vec![],
            },
        ),
        (
            account(9_007_199_254_740_993, &[1, 0, 254]),
            AccountObservation::Present {
                lamports: 9_007_199_254_740_993,
                owner_program: [171; 32],
                executable: false,
                data: vec![1, 0, 254],
            },
        ),
        (
            {
                let mut r = account(0, &[7; 37]);
                r["executable"] = json!(true);
                r["space"] = json!(37);
                r
            },
            AccountObservation::Present {
                lamports: 0,
                owner_program: [171; 32],
                executable: true,
                data: vec![7; 37],
            },
        ),
    ] {
        let mut response = original(false);
        response["result"]["value"][0] = row;
        let facts = run(false, response, None).await??;
        assert_eq!(facts.accounts().value[0].account, expected);
        assert_eq!(facts.accounts().value[0].pubkey, WALLET);
        assert_eq!(
            facts.observed_payer_lamports(),
            match expected {
                AccountObservation::Absent => None,
                AccountObservation::Present { lamports, .. } => Some(lamports),
            }
        );
        assert_eq!(
            facts.requirements().unavailable_budget,
            UnavailableNativeBudget::default()
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_rpc_malformed_account_fields_are_errors_never_absent_or_zero() -> Result<()> {
    let base = account(17, &[0, 255, 17]);
    let mut cases = vec![
        (json!(true), "object"),
        (json!([]), "object"),
        (json!(0), "object"),
    ];
    for (field, reason, values) in [
        (
            "lamports",
            "lamports",
            vec![
                json!(null),
                json!("0"),
                json!(-1),
                json!(0.0),
                json!(1.5),
                serde_json::from_str("18446744073709551616")?,
            ],
        ),
        (
            "owner",
            "owner",
            vec![
                json!(null),
                json!(0),
                json!("0"),
                json!(""),
                json!("1".repeat(31)),
                json!("1".repeat(33)),
                json!(format!(" {}", bs58::encode([171; 32]).into_string())),
            ],
        ),
        (
            "executable",
            "executable",
            vec![json!(null), json!(0), json!("false")],
        ),
        (
            "data",
            "encoding",
            vec![
                json!(null),
                json!(""),
                json!([]),
                json!(["AA=="]),
                json!(["AA==", "base64", "extra"]),
                json!(["AA==", "base58"]),
                json!(["AA==", "base64+zstd"]),
                json!([1, "base64"]),
                json!(["AA==", null]),
            ],
        ),
    ] {
        let mut absent = base.clone();
        absent.as_object_mut().unwrap().remove(field);
        cases.push((absent, reason));
        for value in values {
            let mut r = base.clone();
            r[field] = value;
            cases.push((r, reason));
        }
    }
    for data in ["?", "AA=", "A===", "AA==\n"] {
        let mut r = base.clone();
        r["data"] = json!([data, "base64"]);
        cases.push((r, "base64"));
    }
    for space in [
        json!(null),
        json!("3"),
        json!(-1),
        json!(3.0),
        json!(2),
        json!(4),
        json!(u64::MAX),
    ] {
        let mut r = base.clone();
        r["space"] = space;
        cases.push((r, "space"));
    }
    for (row, reason) in cases {
        let mut r = original(false);
        r["result"]["value"][0] = row;
        let error = format!("{:#}", run(false, r, None).await?.unwrap_err());
        assert!(
            error.contains(&format!("native_rpc_account_{reason}")),
            "{error}, expected {reason}"
        );
    }
    Ok(())
}
