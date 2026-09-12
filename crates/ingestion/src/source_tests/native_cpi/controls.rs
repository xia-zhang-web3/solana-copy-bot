use super::{
    harness::{capture, capture_proto, fixture, known},
    parsed, proto,
};
use anyhow::Result;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{subscribe_update, SubscribeUpdate};

#[test]
fn raw_parsed_and_protobuf_same_twelve_inputs() -> Result<()> {
    for mechanism in [
        "persistent",
        "temporary",
        "fee_only",
        "payment",
        "rent",
        "incomplete",
    ] {
        for side in ["buy", "sell"] {
            let f = fixture(mechanism, side);
            let parsed = parsed::converted(&f);
            let label = format!("repr-{mechanism}-{side}");
            let expected = capture(&format!("{label}-raw"), &f, "yellowstone")?;
            for provider in ["rpc_backfill", "helius_fetch"] {
                let result = capture(&format!("{label}-parsed"), &parsed, provider)?;
                assert_eq!(result, expected, "{label} {provider}");
            }
        }
    }
    Ok(())
}

#[test]
fn buy_donation_flips_or_zeroes_native_cash() -> Result<()> {
    for donation in [1_000_010_000u64, 2_000_000_000] {
        let mut f = fixture("temporary", "buy");
        let r = f["roles"].clone();
        let keys = f["result"]["transaction"]["message"]["accountKeys"]
            .as_array()
            .unwrap();
        let donor = keys.iter().position(|k| k["pubkey"] == r["donor"]).unwrap();
        let mut bytes = 2u32.to_le_bytes().to_vec();
        bytes.extend(donation.to_le_bytes());
        f["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().push(json!({
            "programId":r["system_program"],"accounts":[r["donor"],r["user"]],"data":bs58::encode(bytes).into_string()}));
        let post = f["result"]["meta"]["postBalances"].as_array_mut().unwrap();
        post[0] = json!(post[0].as_u64().unwrap() + donation);
        post[donor] = json!(post[donor].as_u64().unwrap() - donation);
        let cash = i128::from(post[0].as_u64().unwrap())
            - i128::from(f["result"]["meta"]["preBalances"][0].as_u64().unwrap());
        assert!(cash >= 0);
        for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
            let label = format!("donation-{donation}");
            let result = capture(&label, &f, provider)?;
            known(&result, true);
            if provider != "yellowstone" {
                assert_eq!(
                    capture(&format!("{label}-parsed"), &parsed::converted(&f), provider)?,
                    result
                );
            }
        }
    }
    Ok(())
}

#[test]
fn sell_uses_actual_output_above_minimum() -> Result<()> {
    let mut f = fixture("temporary", "sell");
    let ix = &mut f["result"]["transaction"]["message"]["instructions"][2];
    let mut d = bs58::decode(ix["data"].as_str().unwrap())
        .into_vec()
        .unwrap();
    d[16..24].copy_from_slice(&900_000_000u64.to_le_bytes());
    ix["data"] = json!(bs58::encode(d).into_string());
    for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
        let result = capture("sell-above-minimum", &f, provider)?;
        known(&result, false);
        if provider != "yellowstone" {
            assert_eq!(
                capture(
                    "sell-above-minimum-parsed",
                    &parsed::converted(&f),
                    provider
                )?,
                result
            );
        }
    }
    Ok(())
}

fn tx(
    update: &mut SubscribeUpdate,
) -> &mut yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo {
    match update.update_oneof.as_mut().unwrap() {
        subscribe_update::UpdateOneof::Transaction(tx) => tx.transaction.as_mut().unwrap(),
        _ => panic!("transaction"),
    }
}

#[test]
fn protobuf_loaded_addresses_preserve_indices() -> Result<()> {
    let f = fixture("temporary", "buy");
    let mut update = proto::update(&f);
    let tx = tx(&mut update);
    let message = tx.transaction.as_mut().unwrap().message.as_mut().unwrap();
    let moved = message
        .account_keys
        .split_off(message.account_keys.len() - 2);
    message
        .header
        .as_mut()
        .unwrap()
        .num_readonly_unsigned_accounts -= 2;
    tx.meta.as_mut().unwrap().loaded_readonly_addresses = moved;
    let result = capture_proto("loaded-addresses", &f, update)?;
    known(&result, true);
    assert_eq!(result, capture("static-addresses", &f, "yellowstone")?);
    Ok(())
}

#[test]
fn protobuf_missing_none_flag_bad_key_and_parent_index_are_terminal() -> Result<()> {
    let f = fixture("temporary", "buy");
    for case in [
        "none_flag",
        "bad_key",
        "bad_account_index",
        "nested_depth",
        "missing_signer_header",
    ] {
        let healthy = capture("proto-metadata-healthy", &f, "yellowstone")?;
        let mut update = proto::update(&f);
        let tx = tx(&mut update);
        match case {
            "none_flag" => tx.meta.as_mut().unwrap().inner_instructions_none = true,
            "bad_key" => {
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .account_keys[2] = vec![3; 31]
            }
            "bad_account_index" => {
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .instructions[4]
                    .accounts[5] = 255
            }
            "nested_depth" => {
                tx.meta.as_mut().unwrap().inner_instructions[0].instructions[0].stack_height =
                    Some(3)
            }
            "missing_signer_header" => {
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .header = None
            }
            _ => unreachable!(),
        }
        let damaged = capture_proto(&format!("proto-{case}"), &f, update)?;
        let restored = capture(&format!("proto-{case}-restored"), &f, "yellowstone")?;
        known(&healthy, true);
        assert_eq!(restored, healthy);
        assert!(damaged.is_null(), "{case}");
    }
    Ok(())
}

#[test]
fn parsed_hybrid_and_missing_index_metadata_refused_with_restored_controls() -> Result<()> {
    let f = fixture("temporary", "buy");
    let healthy = parsed::converted(&f);
    for case in [
        "hybrid_data",
        "hybrid_accounts",
        "bad_key",
        "missing_info",
        "wrong_program",
        "wrong_amount_type",
    ] {
        let mut bad = healthy.clone();
        match case {
            "hybrid_data" => {
                bad["result"]["transaction"]["message"]["instructions"][0]["data"] =
                    f["result"]["transaction"]["message"]["instructions"][0]["data"].clone()
            }
            "hybrid_accounts" => {
                bad["result"]["transaction"]["message"]["instructions"][0]["accounts"] = json!([])
            }
            "bad_key" => bad["result"]["transaction"]["message"]["accountKeys"][2] = Value::Null,
            "missing_info" => bad["result"]["transaction"]["message"]["instructions"][0]["parsed"]
                ["info"]
                .as_object_mut()
                .unwrap()
                .remove("seed")
                .map(|_| ())
                .unwrap(),
            "wrong_program" => {
                bad["result"]["transaction"]["message"]["instructions"][0]["programId"] =
                    f["roles"]["fee_program"].clone()
            }
            "wrong_amount_type" => {
                bad["result"]["meta"]["innerInstructions"][0]["instructions"][0]["parsed"]["info"]
                    ["amount"] = json!(1_000_000_000)
            }
            _ => unreachable!(),
        }
        for provider in ["rpc_backfill", "helius_fetch"] {
            let before = capture(&format!("json-{case}-healthy"), &healthy, provider)?;
            let result = capture(&format!("json-{case}-damaged"), &bad, provider)?;
            let restored = capture(&format!("json-{case}-restored"), &healthy, provider)?;
            known(&before, true);
            assert_eq!(restored, before);
            assert!(result.is_null(), "{case}");
        }
    }
    Ok(())
}

#[test]
fn mixed_seed_and_ata_lifecycle_cannot_restore_fallback() -> Result<()> {
    let healthy = fixture("temporary", "buy");
    let mut bad = healthy.clone();
    let r = bad["roles"].clone();
    bad["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().insert(0,json!({
        "programId":r["associated_token_program"],"accounts":[r["user"],r["user_base"],r["user"],r["base_mint"],r["system_program"],r["token_program"]],"data":"2"}));
    bad["result"]["meta"]["innerInstructions"][0]["index"] = json!(5);
    for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
        for (repr, h, b) in [
            ("raw", healthy.clone(), bad.clone()),
            (
                "parsed",
                parsed::converted(&healthy),
                parsed::converted(&bad),
            ),
        ] {
            if repr == "parsed" && provider == "yellowstone" {
                continue;
            }
            let before = capture(&format!("mixed-ata-{repr}-healthy"), &h, provider)?;
            let damaged = capture(&format!("mixed-ata-{repr}-damaged"), &b, provider)?;
            let restored = capture(&format!("mixed-ata-{repr}-restored"), &h, provider)?;
            known(&before, true);
            assert_eq!(restored, before);
            assert!(damaged.is_null());
        }
    }
    Ok(())
}

#[test]
fn candidate_requires_unambiguous_success_metadata() -> Result<()> {
    let healthy = fixture("temporary", "buy");
    let mut failures = Vec::new();
    for provider in ["rpc_backfill", "helius_fetch"] {
        for repr in ["raw", "parsed"] {
            let h = if repr == "raw" {
                healthy.clone()
            } else {
                parsed::converted(&healthy)
            };
            let mut bad = h.clone();
            bad["result"]["meta"].as_object_mut().unwrap().remove("err");
            let before = capture(&format!("success-{repr}-healthy"), &h, provider)?;
            let damaged = capture(&format!("success-{repr}-missing"), &bad, provider)?;
            let restored = capture(&format!("success-{repr}-restored"), &h, provider)?;
            known(&before, true);
            assert_eq!(before, restored);
            if !damaged.is_null() {
                failures.push(format!("{provider}-{repr}"));
            }
        }
    }
    let before = capture("success-healthy", &healthy, "yellowstone")?;
    let mut update = proto::update(&healthy);
    tx(&mut update).meta.as_mut().unwrap().err = Some(Default::default());
    let damaged = capture_proto("success-ambiguous-error", &healthy, update)?;
    let restored = capture("success-restored", &healthy, "yellowstone")?;
    known(&before, true);
    assert_eq!(before, restored);
    if !damaged.is_null() {
        failures.push("yellowstone-empty-error".to_owned());
    }
    assert!(failures.is_empty(), "{failures:?}");
    Ok(())
}
