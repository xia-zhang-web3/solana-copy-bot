use super::*;

fn key(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}
fn index(f: &Value, key: &str) -> usize {
    f["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .position(|k| k["pubkey"] == key)
        .unwrap()
}
fn amount(raw: u64, decimals: u8) -> Value {
    let ui = raw as f64 / 10_f64.powi(i32::from(decimals));
    json!({"amount":raw.to_string(),"decimals":decimals,"uiAmount":ui,"uiAmountString":ui.to_string()})
}
fn data_transfer(raw: u64) -> String {
    let mut bytes = vec![3];
    bytes.extend_from_slice(&raw.to_le_bytes());
    bs58::encode(bytes).into_string()
}

pub(super) fn rename(f: &mut Value, label: &str) {
    assert!(label.len() < 63);
    f["case"] = json!(label);
    let mut signature = [0; 64];
    signature[0] = 45;
    signature[1..=label.len()].copy_from_slice(label.as_bytes());
    let signature = bs58::encode(signature).into_string();
    f["signature"] = json!(signature);
    f["result"]["transaction"]["signatures"][0] = f["signature"].clone();
}

fn insert_account(f: &mut Value, pubkey: &str, writable: bool) -> usize {
    let keys = f["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap();
    let at = if writable {
        keys.iter()
            .position(|k| k["signer"] == false && k["writable"] == false)
            .unwrap()
    } else {
        keys.len()
    };
    keys.insert(
        at,
        json!({"pubkey":pubkey,"signer":false,"writable":writable}),
    );
    for field in ["preBalances", "postBalances"] {
        f["result"]["meta"][field]
            .as_array_mut()
            .unwrap()
            .insert(at, json!(2_039_280));
    }
    for field in ["preTokenBalances", "postTokenBalances"] {
        for row in f["result"]["meta"][field].as_array_mut().unwrap() {
            let old = row["accountIndex"].as_u64().unwrap();
            if old >= at as u64 {
                row["accountIndex"] = json!(old + 1);
            }
        }
    }
    at
}

fn add_transfer(f: &mut Value, mint: &str, raw: u64, decimals: u8, from: &str, to: &str, seed: u8) {
    let source = key(seed);
    let target = key(seed + 1);
    let a = insert_account(f, &source, true);
    let b = insert_account(f, &target, true);
    let token = f["roles"]["token_program"].clone();
    for (field, source_amount, target_amount) in
        [("preTokenBalances", raw, 0), ("postTokenBalances", 0, raw)]
    {
        let rows = f["result"]["meta"][field].as_array_mut().unwrap();
        rows.push(json!({"accountIndex":a,"mint":mint,"owner":from,"programId":token,"uiTokenAmount":amount(source_amount,decimals)}));
        rows.push(json!({"accountIndex":b,"mint":mint,"owner":to,"programId":token,"uiTokenAmount":amount(target_amount,decimals)}));
    }
    f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .push(json!({"programId":token,"accounts":[source,target,from],"data":data_transfer(raw)}));
}

pub(super) fn gift(f: &mut Value, sell: bool, raw: u64, decimals: u8) {
    let user = f["roles"]["user"].as_str().unwrap().to_string();
    let donor = f["roles"]["donor"].as_str().unwrap().to_string();
    let (from, to) = if sell {
        (&user, &donor)
    } else {
        (&donor, &user)
    };
    add_transfer(f, &key(4), raw, decimals, from, to, 140);
}

pub(super) fn base(sell: bool, use_native: bool) -> Value {
    let mut f = super::super::cases::swap_fixture(sell);
    // The older control mirrors token rows only; mirror WSOL backing too.
    if sell {
        for role in ["user_base", "pool_base"] {
            let i = index(&f, f["roles"][role].as_str().unwrap());
            let pre = f["result"]["meta"]["preBalances"][i].clone();
            f["result"]["meta"]["preBalances"][i] = f["result"]["meta"]["postBalances"][i].clone();
            f["result"]["meta"]["postBalances"][i] = pre;
        }
    }
    if use_native {
        native(&mut f, sell);
    }
    f
}

fn native(f: &mut Value, sell: bool) {
    let user = f["roles"]["user"].as_str().unwrap().to_string();
    let wsol = f["roles"]["user_base"].as_str().unwrap().to_string();
    let wsol_index = index(f, &wsol);
    let user_index = index(f, &user);
    for field in ["preTokenBalances", "postTokenBalances"] {
        f["result"]["meta"][field]
            .as_array_mut()
            .unwrap()
            .retain(|r| !(r["owner"] == user && r["mint"] == SOL));
    }
    for field in ["preBalances", "postBalances"] {
        f["result"]["meta"][field][wsol_index] = json!(0);
    }
    let pre = f["result"]["meta"]["preBalances"][user_index]
        .as_u64()
        .unwrap();
    let fee = f["result"]["meta"]["fee"].as_u64().unwrap();
    f["result"]["meta"]["postBalances"][user_index] = json!(if sell {
        pre + 1_000_000_000 - fee
    } else {
        pre - 1_000_000_000 - fee
    });
    let token = f["roles"]["token_program"].clone();
    let system = f["roles"]["system_program"].clone();
    let mut prefix = vec![
        json!({"programId":f["roles"]["associated_token_program"],"accounts":[user,wsol,user,SOL,system,token],"data":bs58::encode([1]).into_string()}),
    ];
    if !sell {
        let mut bytes = 2_u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&1_000_000_000_u64.to_le_bytes());
        prefix.push(json!({"programId":system,"accounts":[user,wsol],"data":bs58::encode(bytes).into_string()}));
        prefix.push(
            json!({"programId":token,"accounts":[wsol],"data":bs58::encode([17]).into_string()}),
        );
    }
    for group in f["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
    {
        group["index"] = json!(group["index"].as_u64().unwrap() + prefix.len() as u64);
    }
    let top = f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap();
    prefix.append(top);
    *top = prefix;
    top.push(json!({"programId":token,"accounts":[wsol,user,user],"data":bs58::encode([9]).into_string()}));
    // These legacy aggregation/amount/time controls intentionally exercise the
    // native quote-WSOL class. Their synthetic ATA has no creation CPI proof;
    // temporary base-WSOL controls now live in native_cpi/ata_* with a full proof.
    // Swap base/quote roles coherently, retaining the same transfers and cash.
    let parent = top.iter_mut().find(|ix| ix["programId"] == PUMP).unwrap();
    let accounts = parent["accounts"].as_array_mut().unwrap();
    for (a, b) in [(3, 4), (5, 6), (7, 8)] {
        accounts.swap(a, b);
    }
    let mut data = bs58::decode(super::super::cases::data(!sell))
        .into_vec()
        .unwrap();
    let (input, minimum): (u64, u64) = if sell {
        (10_000_000, 1_000_000_000)
    } else {
        (1_000_000_000, 10_000_000)
    };
    data[8..16].copy_from_slice(&input.to_le_bytes());
    data[16..24].copy_from_slice(&minimum.to_le_bytes());
    parent["data"] = json!(bs58::encode(data).into_string());
    parent["audit_label"] =
        json!("legacy quote-WSOL class; account aggregation/time boundary control");
}

pub(super) fn target_decimals(f: &mut Value, decimals: u8) {
    let mint = f["roles"]["quote_mint"].clone();
    for field in ["preTokenBalances", "postTokenBalances"] {
        for row in f["result"]["meta"][field].as_array_mut().unwrap() {
            if row["mint"] == mint {
                let raw = row["uiTokenAmount"]["amount"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                row["uiTokenAmount"] = amount(raw, decimals);
            }
        }
    }
}

pub(super) fn reverse_rows(f: &mut Value) {
    for field in ["preTokenBalances", "postTokenBalances"] {
        f["result"]["meta"][field].as_array_mut().unwrap().reverse();
    }
}

pub(super) fn raydium(f: &mut Value) {
    insert_account(f, RAY, false);
    for ix in f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        if ix["programId"] == PUMP {
            ix["programId"] = json!(RAY);
        }
    }
    f["result"]["meta"]["logMessages"] = json!([]);
}

pub(super) fn zero_net(f: &mut Value) {
    let user = f["roles"]["user"].as_str().unwrap().to_string();
    add_transfer(f, &key(4), 1_000_000_000, 6, &user, &user, 140);
}

pub(super) fn split_target(f: &mut Value, sell: bool) {
    let user = f["roles"]["user"].clone();
    let mint = f["roles"]["quote_mint"].clone();
    let token = f["roles"]["token_program"].clone();
    let other = key(160);
    let other_index = insert_account(f, &other, true);
    for field in ["preTokenBalances", "postTokenBalances"] {
        let rows = f["result"]["meta"][field].as_array_mut().unwrap();
        let funded = (field == "preTokenBalances") == sell;
        let row = rows
            .iter_mut()
            .find(|r| r["owner"] == user && r["mint"] == mint)
            .unwrap();
        row["uiTokenAmount"] = amount(if funded { 6_000_000 } else { 0 }, 6);
        rows.push(json!({"accountIndex":other_index,"owner":user,"mint":mint,"programId":token,"uiTokenAmount":amount(if funded {4_000_000}else{0},6)}));
    }
    // Split the same quote transfer, preserving its total and authority.
    let quote = f["roles"]["user_quote"].clone();
    for group in f["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
    {
        let ixs = group["instructions"].as_array_mut().unwrap();
        if let Some(ix) = ixs.iter_mut().find(|ix| {
            ix["accounts"]
                .as_array()
                .is_some_and(|a| a.contains(&quote))
        }) {
            ix["data"] = json!(data_transfer(6_000_000));
            let mut second = ix.clone();
            second["data"] = json!(data_transfer(4_000_000));
            for account in second["accounts"].as_array_mut().unwrap() {
                if *account == quote {
                    *account = json!(other);
                }
            }
            ixs.push(second);
            break;
        }
    }
}

fn payment(f: &mut Value, from_user: bool, lamports: u64) {
    let user = f["roles"]["user"].as_str().unwrap().to_string();
    let donor = f["roles"]["donor"].as_str().unwrap().to_string();
    let (from, to) = if from_user {
        (&user, &donor)
    } else {
        (&donor, &user)
    };
    let a = index(f, from);
    let b = index(f, to);
    let before = f["result"]["meta"]["preBalances"][a].as_u64().unwrap();
    let after = f["result"]["meta"]["postBalances"][a].as_u64().unwrap();
    let cushion = 10_000_000_000_u64.saturating_sub(before);
    f["result"]["meta"]["preBalances"][a] = json!(before + cushion);
    f["result"]["meta"]["postBalances"][a] = json!(after + cushion - lamports);
    let after = f["result"]["meta"]["postBalances"][b].as_u64().unwrap();
    f["result"]["meta"]["postBalances"][b] = json!(after + lamports);
    let mut data = 2_u32.to_le_bytes().to_vec();
    data.extend_from_slice(&lamports.to_le_bytes());
    let system = f["roles"]["system_program"].clone();
    f["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().push(json!({"programId":system,"accounts":[from,to],"data":bs58::encode(data).into_string()}));
}

pub(super) fn fallback_trap(sell: bool) -> Value {
    let mut f = base(sell, false);
    gift(&mut f, sell, 10_000_000, 6);
    let user = f["roles"]["user"].as_str().unwrap().to_string();
    let donor = f["roles"]["donor"].as_str().unwrap().to_string();
    let (from, to) = if sell {
        (&donor, &user)
    } else {
        (&user, &donor)
    };
    add_transfer(&mut f, &key(91), 5_000_000, 6, from, to, 150);
    payment(&mut f, sell, 2_000_000_000);
    f
}

pub(super) fn token_token() -> Value {
    let mut f = base(false, false);
    let mint = key(91);
    for key in f["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
    {
        if key["pubkey"] == SOL {
            key["pubkey"] = json!(mint);
        }
    }
    for field in ["preTokenBalances", "postTokenBalances"] {
        for row in f["result"]["meta"][field].as_array_mut().unwrap() {
            if row["mint"] == SOL {
                row["mint"] = json!(mint);
            }
        }
    }
    for ix in f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        for account in ix["accounts"].as_array_mut().unwrap() {
            if *account == SOL {
                *account = json!(mint);
            }
        }
    }
    for role in ["user_base", "pool_base"] {
        let i = index(&f, f["roles"][role].as_str().unwrap());
        for field in ["preBalances", "postBalances"] {
            f["result"]["meta"][field][i] = json!(2_039_280);
        }
    }
    let fee = f["result"]["meta"]["fee"].as_u64().unwrap();
    payment(&mut f, false, fee);
    f["roles"]["base_mint"] = json!(mint);
    f
}
