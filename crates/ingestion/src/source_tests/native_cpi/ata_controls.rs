use super::{
    ata_fixture::{self as f, as_parsed, built, replace},
    harness::{capture, capture_proto, known},
    proto,
};
use anyhow::Result;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::subscribe_update;

fn mutate(v: &mut Value, case: &str) {
    let r = v["roles"].clone();
    match case {
        "incomplete" => {
            v["result"]["meta"]["innerInstructions"]
                .as_array_mut()
                .unwrap()
                .remove(0);
        }
        "lower_bump" => {
            let (other, _) = f::address(r["user"].as_str().unwrap(), f::SOL, 1);
            replace(v, r["user_base"].as_str().unwrap(), &other);
        }
        "wrong_create_owner" => {
            let i = &mut v["result"]["meta"]["innerInstructions"][0]["instructions"][1];
            let mut d = bs58::decode(i["data"].as_str().unwrap())
                .into_vec()
                .unwrap();
            d[20..52].copy_from_slice(&bs58::decode(f::SYSTEM).into_vec().unwrap());
            i["data"] = json!(bs58::encode(d).into_string());
        }
        "wrong_mint" => {
            v["result"]["meta"]["innerInstructions"][0]["instructions"][3]["accounts"][1] =
                r["quote_mint"].clone()
        }
        "wrong_close_destination" => {
            let top = v["result"]["transaction"]["message"]["instructions"]
                .as_array_mut()
                .unwrap();
            top.last_mut().unwrap()["accounts"][1] = r["donor"].clone();
        }
        "group_index" => v["result"]["meta"]["innerInstructions"][0]["index"] = json!(1),
        "setup_order" => v["result"]["meta"]["innerInstructions"][0]["instructions"]
            .as_array_mut()
            .unwrap()
            .swap(2, 3),
        "extra_group" => {
            let ix = v["result"]["meta"]["innerInstructions"][1]["instructions"][0].clone();
            // Known relevant transfer under another top-level instruction, not setup.
            v["result"]["meta"]["innerInstructions"]
                .as_array_mut()
                .unwrap()
                .push(json!({"index":2,"instructions":[ix]}));
        }
        "partial_binding" => {
            v["result"]["meta"]["innerInstructions"][0]["instructions"][1]["accounts"] =
                json!([r["user"]])
        }
        "new_target" => {
            let index = v["result"]["transaction"]["message"]["accountKeys"]
                .as_array()
                .unwrap()
                .iter()
                .position(|k| k["pubkey"] == r["user_quote"])
                .unwrap();
            v["result"]["meta"]["preBalances"][index] = json!(0);
            v["result"]["meta"]["preTokenBalances"]
                .as_array_mut()
                .unwrap()
                .retain(|row| row["accountIndex"] != index);
        }
        _ => unreachable!(),
    }
}

#[test]
fn ata_creation_binding_and_lifecycle_refusals() -> Result<()> {
    for buy in [true, false] {
        let healthy = built("temporary", buy);
        for provider in ["rpc_backfill", "helius_fetch", "yellowstone"] {
            for repr in ["raw", "parsed"] {
                if provider == "yellowstone" && repr == "parsed" {
                    continue;
                }
                let convert = |v: &Value| {
                    if repr == "raw" {
                        v.clone()
                    } else {
                        as_parsed(v)
                    }
                };
                let prefix = format!("b59-control-{buy}-{repr}");
                let good = capture(&format!("{prefix}-healthy"), &convert(&healthy), provider)?;
                known(&good, buy);
                for case in [
                    "incomplete",
                    "lower_bump",
                    "wrong_create_owner",
                    "wrong_mint",
                    "wrong_close_destination",
                    "group_index",
                    "setup_order",
                    "extra_group",
                    "partial_binding",
                    "new_target",
                ] {
                    if !buy
                        && !["incomplete", "wrong_close_destination", "partial_binding"]
                            .contains(&case)
                    {
                        continue;
                    }
                    let mut bad = healthy.clone();
                    mutate(&mut bad, case);
                    let ev = capture(
                        &format!("b59-{case}-{}-{repr}", if buy { "buy" } else { "sell" }),
                        &convert(&bad),
                        provider,
                    )?;
                    assert!(ev.is_null(), "{case} {provider} {repr}: {ev}");
                }
                assert_eq!(
                    capture(&format!("{prefix}-restored"), &convert(&healthy), provider)?,
                    good
                );
            }
        }
    }
    Ok(())
}

fn parsed_ata(v: &mut Value) {
    for ix in v["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        if ix["programId"] != f::ATA {
            continue;
        }
        let a = &ix["accounts"];
        *ix = json!({"programId":f::ATA,"parsed":{"type":"createIdempotent","info":{"source":a[0],"account":a[1],"wallet":a[2],"mint":a[3],"systemProgram":a[4],"tokenProgram":a[5]}}});
    }
}

#[test]
fn actual_parsed_ata_and_compiled_invalid_index_require_complete_proof() -> Result<()> {
    let healthy = built("temporary", true);
    // Frozen reference vectors: the highest off-curve bump is 254, not 255.
    assert_eq!(
        healthy["roles"]["user_base"],
        "7i4VVk55NzhtekVjPg7EZzoSGznZYixPyd5cCeDxi7rW"
    );
    assert_eq!(healthy["audit"]["ata_bump"], 254);
    for provider in ["rpc_backfill", "helius_fetch"] {
        let mut h = as_parsed(&healthy);
        parsed_ata(&mut h);
        let good = capture("b59-fully-parsed-healthy", &h, provider)?;
        known(&good, true);
        for field in ["account", "wallet", "mint"] {
            let mut bad = h.clone();
            bad["result"]["transaction"]["message"]["instructions"][0]["parsed"]["info"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(
                capture(&format!("b59-fully-parsed-missing-{field}"), &bad, provider)?.is_null()
            );
        }
        let mut bad = h.clone();
        bad["result"]["meta"]["innerInstructions"][0]["instructions"][0]["parsed"]["info"]
            ["extensionTypes"] = json!(["transferFeeAmount"]);
        assert!(capture("b59-parsed-unknown-extension", &bad, provider)?.is_null());
        assert_eq!(capture("b59-fully-parsed-restored", &h, provider)?, good);
    }
    let mut update = proto::update(&healthy);
    if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof {
        tx.transaction
            .as_mut()
            .unwrap()
            .meta
            .as_mut()
            .unwrap()
            .inner_instructions[0]
            .instructions[1]
            .accounts[1] = 255;
    }
    assert!(capture_proto("b59-invalid-creation-proto-index", &healthy, update)?.is_null());
    known(
        &capture("b59-proto-restored", &healthy, "yellowstone")?,
        true,
    );
    Ok(())
}
