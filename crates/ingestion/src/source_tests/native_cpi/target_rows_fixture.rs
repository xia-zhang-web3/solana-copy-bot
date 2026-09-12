use super::harness::fixture;
use serde_json::{json, Value};

pub(super) fn healthy(buy: bool) -> Value {
    fixture("persistent", if buy { "buy" } else { "sell" })
}

pub(super) fn target(f: &Value, field: &str) -> usize {
    f["result"]["meta"][field]
        .as_array()
        .unwrap()
        .iter()
        .position(|r| r["owner"] == f["roles"]["user"] && r["mint"] == f["roles"]["quote_mint"])
        .unwrap()
}

pub(super) fn missing(f: &mut Value, buy: bool) {
    let field = if buy {
        "preTokenBalances"
    } else {
        "postTokenBalances"
    };
    let position = target(f, field);
    f["result"]["meta"][field]
        .as_array_mut()
        .unwrap()
        .remove(position);
}

pub(super) fn quantity(row: &mut Value, n: u64) {
    row["uiTokenAmount"] = json!({"amount":(n*1_000_000).to_string(),"decimals":6,
        "uiAmount":n as f64,"uiAmountString":n.to_string()});
}

pub(super) fn second_pair(f: &mut Value, foreign: bool) {
    let i = target(f, "preTokenBalances");
    let mut row = f["result"]["meta"]["preTokenBalances"][i].clone();
    let keys = f["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap();
    row["accountIndex"] = json!(keys.len());
    keys.push(
        json!({"pubkey":bs58::encode([41u8;32]).into_string(),"signer":false,"writable":true}),
    );
    quantity(&mut row, 3);
    if foreign {
        row["owner"] = f["roles"]["donor"].clone();
        // Foreign amount data is deliberately unusable and must remain irrelevant.
        row["uiTokenAmount"]["uiAmountString"] = json!("foreign-noise");
    }
    for field in ["preTokenBalances", "postTokenBalances"] {
        f["result"]["meta"][field]
            .as_array_mut()
            .unwrap()
            .push(row.clone());
    }
    for field in ["preBalances", "postBalances"] {
        f["result"]["meta"][field]
            .as_array_mut()
            .unwrap()
            .push(json!(5_000_000));
    }
}

pub(super) fn mutated(buy: bool, case: &str) -> Value {
    let mut f = healthy(buy);
    let pre = target(&f, "preTokenBalances");
    let post = target(&f, "postTokenBalances");
    let index = f["result"]["meta"]["preTokenBalances"][pre]["accountIndex"]
        .as_u64()
        .unwrap() as usize;
    match case {
        "reordered" => {
            for field in ["preTokenBalances", "postTokenBalances"] {
                f["result"]["meta"][field].as_array_mut().unwrap().reverse();
            }
        }
        "second_pair" | "second_pair_missing" => {
            second_pair(&mut f, false);
            if case.ends_with("missing") {
                missing(&mut f, buy);
            }
            f["result"]["meta"]["postTokenBalances"]
                .as_array_mut()
                .unwrap()
                .reverse();
        }
        "foreign_noise" => second_pair(&mut f, true),
        "duplicate" | "zero_duplicate" => {
            let row = f["result"]["meta"]["postTokenBalances"][post].clone();
            f["result"]["meta"]["postTokenBalances"]
                .as_array_mut()
                .unwrap()
                .push(row);
            if case == "zero_duplicate" {
                f["result"]["meta"]["preBalances"][index] = json!(0);
            }
        }
        "owner_conflict" => {
            f["result"]["meta"]["preTokenBalances"][pre]["owner"] = f["roles"]["donor"].clone()
        }
        "mint_conflict" => {
            f["result"]["meta"]["preTokenBalances"][pre]["mint"] = f["roles"]["gift_mint"].clone()
        }
        "empty_owner" => f["result"]["meta"]["preTokenBalances"][pre]["owner"] = json!(""),
        "empty_mint" => f["result"]["meta"]["preTokenBalances"][pre]["mint"] = json!(""),
        "index_conflict" => {
            f["result"]["meta"]["preTokenBalances"][pre]["accountIndex"] = json!(999)
        }
        "paired_zero" => {
            quantity(
                &mut f["result"]["meta"]["preTokenBalances"][pre],
                if buy { 0 } else { 10 },
            );
            quantity(
                &mut f["result"]["meta"]["postTokenBalances"][post],
                if buy { 10 } else { 0 },
            );
        }
        "native_zero_pre" | "native_zero_post" => {
            // Preserve the old branch; this is not a lifecycle or correct-volume proof.
            missing(&mut f, buy);
            let field = if case.ends_with("pre") {
                "preBalances"
            } else {
                "postBalances"
            };
            f["result"]["meta"][field][index] = json!(0);
        }
        _ => unreachable!("{case}"),
    }
    f
}
