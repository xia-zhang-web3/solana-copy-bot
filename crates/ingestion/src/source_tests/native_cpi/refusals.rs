use super::{
    harness::{capture, fixture, known},
    parsed,
};
use anyhow::Result;
use serde_json::{json, Value};

fn data(ix: &mut Value, change: impl FnOnce(&mut Vec<u8>)) {
    let mut d = bs58::decode(ix["data"].as_str().unwrap())
        .into_vec()
        .unwrap();
    change(&mut d);
    ix["data"] = json!(bs58::encode(d).into_string());
}

fn mutate(f: &mut Value, case: &str) {
    let r = f["roles"].clone();
    let parent = 4;
    match case {
        "wrong_parent_group" => f["result"]["meta"]["innerInstructions"][0]["index"] = json!(0),
        "missing_group" => {
            f["result"]["meta"]
                .as_object_mut()
                .unwrap()
                .remove("innerInstructions");
        }
        "duplicate_group" => {
            let g = f["result"]["meta"]["innerInstructions"][0].clone();
            f["result"]["meta"]["innerInstructions"]
                .as_array_mut()
                .unwrap()
                .push(g);
        }
        "repeated_parent" => {
            let ix = f["result"]["transaction"]["message"]["instructions"][parent].clone();
            f["result"]["transaction"]["message"]["instructions"]
                .as_array_mut()
                .unwrap()
                .push(ix);
        }
        "reordered_funding" => f["result"]["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .swap(2, 3),
        "wrong_seed_base" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][0],
            |d| {
                d[4..36].copy_from_slice(
                    &bs58::decode(r["donor"].as_str().unwrap())
                        .into_vec()
                        .unwrap(),
                )
            },
        ),
        "wrong_seed_owner" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][0],
            |d| {
                let n = d.len();
                d[n - 32..].copy_from_slice(
                    &bs58::decode(r["donor"].as_str().unwrap())
                        .into_vec()
                        .unwrap(),
                );
            },
        ),
        "wrong_program" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["programId"] =
                r["fee_program"].clone()
        }
        "missing_parent_accounts" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["accounts"] = json!([])
        }
        "wrong_parent_account" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["accounts"][6] =
                r["fee_ata"].clone()
        }
        "wrong_user_base" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["accounts"][5] =
                r["donor"].clone()
        }
        "wrong_trader" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["accounts"][1] =
                r["donor"].clone()
        }
        "wrong_token_program" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["accounts"][11] =
                r["system_program"].clone()
        }
        "wrong_target_mint" => {
            f["result"]["transaction"]["message"]["instructions"][parent]["accounts"][4] =
                r["donor"].clone()
        }
        "wrong_authority" => {
            f["result"]["meta"]["innerInstructions"][0]["instructions"][0]["accounts"][2] =
                r["donor"].clone()
        }
        "wrong_target_authority" => {
            f["result"]["meta"]["innerInstructions"][0]["instructions"][1]["accounts"][2] =
                r["donor"].clone()
        }
        "wrong_target_raw" => data(
            &mut f["result"]["meta"]["innerInstructions"][0]["instructions"][1],
            |d| d[1..9].copy_from_slice(&11_000_000u64.to_le_bytes()),
        ),
        "wrong_input_argument" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][parent],
            |d| d[8..16].copy_from_slice(&900_000_000u64.to_le_bytes()),
        ),
        "min_out_exceeds_actual" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][parent],
            |d| d[16..24].copy_from_slice(&11_000_000u64.to_le_bytes()),
        ),
        "extra_quote_fee" | "extra_sol_fee" => {
            let ix_index = if case == "extra_quote_fee" { 1 } else { 0 };
            let mut ix =
                f["result"]["meta"]["innerInstructions"][0]["instructions"][ix_index].clone();
            ix["accounts"][1] = r["fee_ata"].clone();
            data(&mut ix, |d| d[1..9].copy_from_slice(&1u64.to_le_bytes()));
            f["result"]["meta"]["innerInstructions"][0]["instructions"]
                .as_array_mut()
                .unwrap()
                .push(ix);
        }
        "outside_quote_transfer" => {
            let ix = f["result"]["meta"]["innerInstructions"][0]["instructions"][1].clone();
            f["result"]["transaction"]["message"]["instructions"]
                .as_array_mut()
                .unwrap()
                .push(ix);
        }
        "missing_lifecycle" => {
            let p = f["result"]["transaction"]["message"]["instructions"][parent].clone();
            f["result"]["transaction"]["message"]["instructions"] = json!([p]);
            f["result"]["meta"]["innerInstructions"][0]["index"] = json!(0);
        }
        "reordered_lifecycle" => f["result"]["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .swap(0, 1),
        "repeated_lifecycle" => {
            let ix = f["result"]["transaction"]["message"]["instructions"][5].clone();
            f["result"]["transaction"]["message"]["instructions"]
                .as_array_mut()
                .unwrap()
                .push(ix);
        }
        "wrong_close_destination" => {
            f["result"]["transaction"]["message"]["instructions"][5]["accounts"][1] =
                r["donor"].clone()
        }
        "wrong_close_authority" => {
            f["result"]["transaction"]["message"]["instructions"][5]["accounts"][2] =
                r["donor"].clone()
        }
        "wrong_init_mint" => {
            f["result"]["transaction"]["message"]["instructions"][1]["accounts"][1] =
                r["quote_mint"].clone()
        }
        "wrong_init_owner" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][1],
            |d| {
                d[1..33].copy_from_slice(
                    &bs58::decode(r["donor"].as_str().unwrap())
                        .into_vec()
                        .unwrap(),
                )
            },
        ),
        "wrong_seed" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][0],
            |d| d[44] = b'x',
        ),
        "wrong_space" => data(
            &mut f["result"]["transaction"]["message"]["instructions"][0],
            |d| {
                let n = u64::from_le_bytes(d[36..44].try_into().unwrap()) as usize;
                d[52 + n..60 + n].copy_from_slice(&166u64.to_le_bytes());
            },
        ),
        "missing_lamports" => f["result"]["meta"]["preBalances"] = json!([]),
        "missing_token_rows" => f["result"]["meta"]["postTokenBalances"] = json!([]),
        "wrong_row_owner" => {
            f["result"]["meta"]["postTokenBalances"][1]["owner"] = r["donor"].clone()
        }
        "wrong_row_mint" => {
            f["result"]["meta"]["postTokenBalances"][1]["mint"] = r["gift_mint"].clone()
        }
        "wrong_row_index" => {
            f["result"]["meta"]["postTokenBalances"][1]["accountIndex"] = json!(999)
        }
        "wrong_row_decimals" => {
            f["result"]["meta"]["postTokenBalances"][1]["uiTokenAmount"]["decimals"] = json!(7)
        }
        "malformed_owned_amount" => {
            f["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["uiAmountString"] =
                json!("broken")
        }
        "unknown_timestamp" => {
            f["result"]["blockTime"] = Value::Null;
            f["created_at"] = Value::Null;
        }
        _ => panic!("unknown case {case}"),
    }
}

fn refusal(case: &str) -> Result<()> {
    let healthy = fixture("temporary", "buy");
    let mut bad = healthy.clone();
    mutate(&mut bad, case);
    for (repr, h, b) in [
        ("raw", healthy.clone(), bad.clone()),
        (
            "parsed",
            parsed::converted(&healthy),
            parsed::converted(&bad),
        ),
    ] {
        for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
            if repr == "parsed" && provider == "yellowstone" {
                continue;
            }
            let before = capture(&format!("{case}-{repr}-healthy"), &h, provider)?;
            let damaged = capture(&format!("{case}-{repr}-damaged"), &b, provider)?;
            let restored = capture(&format!("{case}-{repr}-restored"), &h, provider)?;
            known(&before, true);
            assert_eq!(restored, before, "{case} {repr} {provider}");
            assert!(damaged.is_null(), "{case} {repr} {provider}: {damaged}");
        }
    }
    Ok(())
}
macro_rules! cases { ($($name:ident),+ $(,)?) => { $(#[test] fn $name() -> Result<()> {refusal(stringify!($name))})+ }; }
cases!(
    wrong_parent_group,
    missing_group,
    duplicate_group,
    repeated_parent,
    reordered_funding,
    wrong_seed_base,
    wrong_seed_owner,
    outside_quote_transfer,
    wrong_program,
    missing_parent_accounts,
    wrong_parent_account,
    wrong_user_base,
    wrong_trader,
    wrong_token_program,
    wrong_target_mint,
    wrong_authority,
    wrong_target_authority,
    wrong_target_raw,
    wrong_input_argument,
    min_out_exceeds_actual,
    extra_quote_fee,
    extra_sol_fee,
    missing_lifecycle,
    reordered_lifecycle,
    repeated_lifecycle,
    wrong_close_destination,
    wrong_close_authority,
    wrong_init_mint,
    wrong_init_owner,
    wrong_seed,
    wrong_space,
    missing_lamports,
    missing_token_rows,
    wrong_row_owner,
    wrong_row_mint,
    wrong_row_index,
    wrong_row_decimals,
    malformed_owned_amount,
    unknown_timestamp
);
