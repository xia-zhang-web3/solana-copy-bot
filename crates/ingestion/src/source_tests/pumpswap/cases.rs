use super::*;

pub(super) fn data(buy: bool) -> String {
    let mut bytes = if buy {
        vec![198, 46, 21, 82, 180, 217, 232, 112]
    } else {
        vec![51, 230, 133, 164, 1, 127, 131, 173]
    };
    let (amount_in, minimum_out): (u64, u64) = if buy {
        (10_000_000, 1_000_000_000)
    } else {
        (1_000_000_000, 10_000_000)
    };
    bytes.extend_from_slice(&amount_in.to_le_bytes());
    bytes.extend_from_slice(&minimum_out.to_le_bytes());
    if buy {
        bytes.push(0);
    }
    bs58::encode(bytes).into_string()
}

pub(super) fn swap_fixture(buy: bool) -> Value {
    let mut f = fixture("g1_swap");
    if buy {
        let pre = f["result"]["meta"]["preTokenBalances"].clone();
        f["result"]["meta"]["preTokenBalances"] = f["result"]["meta"]["postTokenBalances"].clone();
        f["result"]["meta"]["postTokenBalances"] = pre;
        let user = f["roles"]["user"].clone();
        let pool = f["roles"]["pool"].clone();
        for (index, transfer) in f["result"]["meta"]["innerInstructions"][0]["instructions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            transfer["accounts"].as_array_mut().unwrap().swap(0, 1);
            transfer["accounts"][2] = if index == 0 {
                pool.clone()
            } else {
                user.clone()
            };
        }
        // Match the builder's buy account order: common accounts, two volume
        // accumulators, then fee config/program and remaining accounts.
        let global_volume = bs58::encode([89_u8; 32]).into_string();
        let user_volume = bs58::encode([90_u8; 32]).into_string();
        let keys = f["result"]["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap();
        keys.insert(
            10,
            json!({"pubkey":user_volume,"signer":false,"writable":true}),
        );
        keys.push(json!({"pubkey":global_volume,"signer":false,"writable":false}));
        for field in ["preBalances", "postBalances"] {
            let balances = f["result"]["meta"][field].as_array_mut().unwrap();
            balances.insert(10, json!(1_000_000));
            balances.push(json!(1_000_000));
        }
        let accounts = ix(&mut f)["accounts"].as_array_mut().unwrap();
        accounts.insert(19, json!(global_volume));
        accounts.insert(20, json!(user_volume));
        ix(&mut f)["audit_label"] = json!("PumpSwap.buy_exact_quote_in(base=WSOL, quote=token)");
    }
    ix(&mut f)["data"] = json!(data(buy));
    f
}

fn all(f: &Value, expected: bool) -> Result<()> {
    for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
        assert_case(f, provider, expected)?;
    }
    Ok(())
}

fn rename(f: &mut Value, label: &str) {
    f["case"] = json!(label);
}
fn ix(f: &mut Value) -> &mut Value {
    &mut f["result"]["transaction"]["message"]["instructions"][0]
}
fn token_program(f: &Value) -> String {
    f["result"]["meta"]["innerInstructions"][0]["instructions"][0]["programId"]
        .as_str()
        .unwrap()
        .into()
}
fn add_ray(f: &mut Value) -> Value {
    f["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .push(json!({"pubkey":RAY,"signer":false,"writable":false}));
    for name in ["preBalances", "postBalances"] {
        f["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .push(json!(0));
    }
    json!({"programId":RAY,"accounts":[f["roles"]["user"]],"data":""})
}

#[test]
fn both_supported_formats_preserve_controls_at_top_and_inner() -> Result<()> {
    for buy in [false, true] {
        for inner in [false, true] {
            for flag in 0..=u8::from(buy) {
                let mut f = swap_fixture(buy);
                let mut bytes = bs58::decode(data(buy)).into_vec()?;
                if buy {
                    *bytes.last_mut().unwrap() = flag;
                }
                ix(&mut f)["data"] = json!(bs58::encode(bytes).into_string());
                rename(&mut f, &format!("valid-{buy}-{inner}-{flag}"));
                if inner {
                    let ray = add_ray(&mut f);
                    let pump = ix(&mut f).clone();
                    *ix(&mut f) = ray;
                    f["result"]["meta"]["innerInstructions"][0]["instructions"]
                        .as_array_mut()
                        .unwrap()
                        .insert(0, pump);
                }
                all(&f, true)?;
                for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
                    let raw = parse(&f, provider)?.unwrap();
                    let base = parse(&swap_fixture(buy), provider)?.unwrap();
                    assert_eq!(
                        (raw.amount_in, raw.amount_out),
                        if buy { (10.0, 1.0) } else { (1.0, 10.0) }
                    );
                    assert_eq!(
                        (raw.signer, raw.signature, raw.slot, raw.ts_utc),
                        (base.signer, base.signature, base.slot, base.ts_utc)
                    );
                    assert_eq!(
                        (
                            raw.token_in,
                            raw.amount_in,
                            raw.token_out,
                            raw.amount_out,
                            raw.exact_amounts
                        ),
                        (
                            base.token_in,
                            base.amount_in,
                            base.token_out,
                            base.amount_out,
                            base.exact_amounts
                        )
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn unsupported_data_never_supplies_instruction_presence() -> Result<()> {
    let good = bs58::decode(data(false)).into_vec()?;
    let mut values = vec![
        vec![],
        good[..7].to_vec(),
        good[..23].to_vec(),
        [good.as_slice(), &[0]].concat(),
        vec![9; 24],
    ];
    let buy = bs58::decode(data(true)).into_vec()?;
    values.push(buy[..24].to_vec());
    values.push([buy.as_slice(), &[0]].concat());
    let mut invalid_bool = buy;
    invalid_bool[24] = 2;
    values.push(invalid_bool);
    let mut wrong = good;
    wrong[0] ^= 1;
    values.push(wrong);
    for (n, bytes) in values.into_iter().enumerate() {
        let mut f = fixture("g1_swap");
        rename(&mut f, &format!("invalid-bytes-{n}"));
        ix(&mut f)["data"] = json!(bs58::encode(bytes).into_string());
        all(&f, false)?;
    }
    Ok(())
}

#[test]
fn identity_accounts_hint_and_config_do_not_replace_instruction() -> Result<()> {
    for variant in 0..6 {
        let mut f = fixture("g1_swap");
        rename(&mut f, &format!("no-proof-{variant}"));
        match variant {
            0 => {
                let id = token_program(&f);
                ix(&mut f)["programId"] = json!(id);
            }
            1 => {
                f["result"]["transaction"]["message"]["instructions"] = json!([]);
                f["result"]["meta"]["innerInstructions"] = json!([]);
            }
            2 => {
                f["result"]["transaction"]["message"]["instructions"] = json!([]);
                f["result"]["meta"]["innerInstructions"] = json!([]);
                f["result"]["meta"]["logMessages"] = json!([]);
            }
            3 => {
                let id = token_program(&f);
                ix(&mut f)["programId"] = json!(id);
                f["result"]["meta"]["logMessages"] = json!(["Instruction: PumpSwap sell"]);
            }
            4 => {
                ix(&mut f)["accounts"] = json!([]);
            }
            5 => {
                ix(&mut f)["programId"] = json!(bs58::encode([98u8; 32]).into_string());
            }
            _ => unreachable!(),
        }
        all(&f, false)?;
    }
    Ok(())
}

#[test]
fn mixed_ids_cannot_bypass_refusal_and_pure_raydium_is_unchanged() -> Result<()> {
    for first in [false, true] {
        let mut f = fixture("g4_extend_gift_buy");
        rename(&mut f, &format!("mixed-no-proof-{first}"));
        let ray = add_ray(&mut f);
        let instructions = f["result"]["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap();
        let position = if first { 0 } else { instructions.len() };
        instructions.insert(position, ray);
        all(&f, false)?;
    }
    let mut f = fixture("g1_swap");
    rename(&mut f, "pure-raydium");
    let ray = add_ray(&mut f);
    *ix(&mut f) = ray;
    f["result"]["meta"]["logMessages"] = json!([]);
    all(&f, true)?;
    Ok(())
}

#[test]
fn parser_captures_for_unchanged_v2_bridge() -> Result<()> {
    for name in [
        "g1_swap",
        "g1_chain_swap",
        "g1_support_before",
        "g1_support_after",
        "g8_old_block",
    ] {
        all(&fixture(name), true)?;
    }
    Ok(())
}

#[test]
fn pump_hint_requires_instruction_even_without_pump_in_normalized_ids() -> Result<()> {
    let mut f = fixture("g1_swap");
    rename(&mut f, "hint-without-pump-ids");
    let token = token_program(&f);
    ix(&mut f)["programId"] = json!(token);
    f["result"]["meta"]["logMessages"] = json!(["Instruction: PumpSwap sell"]);
    let mut c = config();
    c.yellowstone_program_ids.push(token.clone());
    c.subscribe_program_ids.push(token);
    f["test_config_interest"] = json!(c.yellowstone_program_ids);
    // The unrelated actual program is interested, so normalization cannot reject
    // this case early. Its used PumpSwap hint must trigger the necessary gate.
    let ids =
        crate::source::HeliusWsSource::extract_program_ids(&f["result"], &f["result"]["meta"], &[]);
    assert!(!ids.contains(PUMP));
    for provider in ["yellowstone", "rpc_backfill", "helius_fetch"] {
        assert_config_case(&f, provider, false, &c)?;
    }
    Ok(())
}
