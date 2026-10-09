use super::super::{accounts, group, parent_index};
use super::*;

fn bytes(ix: &Value) -> Vec<u8> {
    bs58::decode(ix["data"].as_str().unwrap())
        .into_vec()
        .unwrap()
}
fn set(ix: &mut Value, data: &[u8]) {
    ix["data"] = bs58::encode(data).into_string().into();
}
fn parent(r: &mut Value) -> &mut Value {
    let i = parent_index(r);
    &mut r["transaction"]["message"]["instructions"][i]
}
fn event(r: &mut Value) -> &mut Value {
    group(r).last_mut().unwrap()
}
fn change_u64(ix: &mut Value, offset: usize, change: i64) {
    let mut data = bytes(ix);
    let raw = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
    data[offset..offset + 8]
        .copy_from_slice(&raw.checked_add_signed(change).unwrap().to_le_bytes());
    set(ix, &data);
}
fn refused(r: &Value) {
    assert!(decode(r)["exact_amounts"].is_null());
    for parsed in [false, true] {
        assert!(json_native(r, parsed)["exact_amounts"].is_null());
    }
}

#[test]
fn discriminator_length_optional_and_limits_are_not_amounts() {
    for label in ["05-03", "09-03", "12-03"] {
        let original = fixture(label);
        for mode in 0..5 {
            let mut r = original.clone();
            let mut data = bytes(parent(&mut r));
            match mode {
                0 => data[0] ^= 1,
                1 => {
                    data.truncate(23);
                }
                2 => data.extend([0, 0]),
                3 => {
                    data.resize(25, 0);
                    data[24] = 2;
                }
                _ => data[8] ^= 1,
            }
            set(parent(&mut r), &data);
            refused(&r);
        }
        let mut r = original.clone();
        let mut data = bytes(parent(&mut r));
        data[16..24].copy_from_slice(&1u64.to_le_bytes());
        set(parent(&mut r), &data);
        refused(&r);
        let mut r = original.clone();
        change_u64(parent(&mut r), 16, 1000);
        change_u64(event(&mut r), 32, 1000);
        assert_eq!(decode(&r)["exact_amounts"], expected(label));
    }
}

#[test]
fn event_and_instruction_semantics_must_agree() {
    for label in ["05-03", "09-03", "12-03"] {
        for offset in [
            0, 8, 24, 32, 40, 48, 56, 64, 88, 104, 112, 120, 160, 360, 401, 409, 424, 440,
        ] {
            let mut r = fixture(label);
            let mut data = bytes(event(&mut r));
            data[offset] ^= 1;
            set(event(&mut r), &data);
            refused(&r);
        }
        let mut r = fixture(label);
        let mut data = bytes(event(&mut r));
        data.pop();
        set(event(&mut r), &data);
        refused(&r);
    }
    let mut r = fixture("09-03");
    let mut data = bytes(event(&mut r));
    data[368] = 0;
    set(event(&mut r), &data);
    refused(&r);
}

#[test]
fn owner_mint_token_program_and_roles_are_bound() {
    for mode in 0..5 {
        let mut r = fixture("12-03");
        let a = accounts(&r);
        match mode {
            0 => parent(&mut r)["accounts"][1] = json!(a[0]),
            1 => parent(&mut r)["accounts"][3] = json!(a[4]),
            2 => parent(&mut r)["accounts"][25] = json!(a[10]),
            _ => {
                for field in ["preTokenBalances", "postTokenBalances"] {
                    let row = r["meta"][field]
                        .as_array_mut()
                        .unwrap()
                        .iter_mut()
                        .find(|x| x["accountIndex"] == a[5])
                        .unwrap();
                    if mode == 3 {
                        row["owner"] = json!(SOL);
                    } else {
                        row["programId"] = json!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
                    }
                }
            }
        }
        refused(&r);
    }
}

#[test]
fn executed_cpi_fee_and_ambiguity_fail_closed() {
    for mode in 0..7 {
        let mut r = fixture("09-03");
        let a = accounts(&r);
        match mode {
            0 => change_u64(&mut group(&mut r)[1], 1, 1),
            1 => change_u64(&mut group(&mut r)[3], 1, 1),
            2 => {
                let extra = group(&mut r)[1].clone();
                group(&mut r).insert(2, extra);
            }
            3 => group(&mut r)[2]["stackHeight"] = json!(3),
            4 => group(&mut r)[1]["accounts"][1] = json!(a[4]),
            5 => {
                let extra = parent(&mut r).clone();
                r["transaction"]["message"]["instructions"]
                    .as_array_mut()
                    .unwrap()
                    .push(extra);
            }
            _ => group(&mut r)[1]["accounts"][3] = json!(a[1]),
        }
        refused(&r);
    }
}

#[test]
fn temporary_account_creation_sync_and_close_are_proven() {
    for mode in 0..4 {
        let mut r = fixture("09-03");
        let a = accounts(&r);
        match mode {
            0 => r["transaction"]["message"]["instructions"][0]["accounts"][2] = json!(a[0]),
            1 => r["transaction"]["message"]["instructions"][3]["accounts"][3] = json!(a[4]),
            2 => {
                r["transaction"]["message"]["instructions"][2]["data"] =
                    json!(bs58::encode([9]).into_string())
            }
            _ => r["transaction"]["message"]["instructions"][5]["accounts"][1] = json!(a[0]),
        }
        refused(&r);
    }
}

#[test]
fn initial_wsol_balance_is_not_added_to_swap_amount() {
    let mut r = fixture("12-03");
    let a = accounts(&r);
    for field in ["preTokenBalances", "postTokenBalances"] {
        let row = r["meta"][field]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|x| x["accountIndex"] == a[6])
            .unwrap();
        let raw = row["uiTokenAmount"]["amount"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            + 1000;
        row["uiTokenAmount"]["amount"] = raw.to_string().into();
        row["uiTokenAmount"]["uiAmount"] = json!(raw as f64 / 1e9);
        row["uiTokenAmount"]["uiAmountString"] = format!("{:.9}", raw as f64 / 1e9).into();
    }
    for field in ["preBalances", "postBalances"] {
        r["meta"][field][a[6]] = json!(r["meta"][field][a[6]].as_u64().unwrap() + 1000);
    }
    change_u64(event(&mut r), 48, 1000);
    assert_eq!(decode(&r)["exact_amounts"], expected("12-03"));
    for parsed in [false, true] {
        assert_eq!(json_native(&r, parsed)["exact_amounts"], expected("12-03"));
    }
}

#[test]
fn nested_buy_is_outside_direct_profile() {
    let mut r = fixture("09-03");
    let a = accounts(&r);
    let mut nested = parent(&mut r).clone();
    nested["stackHeight"] = json!(2);
    parent(&mut r)["programIdIndex"] = json!(a[2]);
    let index = r["meta"]["innerInstructions"]
        .as_array()
        .unwrap()
        .iter()
        .position(|g| g["instructions"].as_array().is_some_and(|v| v.len() == 7))
        .unwrap();
    let children = r["meta"]["innerInstructions"][index]["instructions"]
        .as_array_mut()
        .unwrap();
    for ix in children.iter_mut() {
        ix["stackHeight"] = json!(3);
    }
    children.insert(0, nested);
    assert!(!json_presence(&r));
    refused(&r);
}
