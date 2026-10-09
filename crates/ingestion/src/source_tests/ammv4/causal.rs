use super::*;

#[test]
fn missing_malformed_repeated_or_nested_cpi_is_terminal() {
    for w in TARGETS {
        for mode in 0..8 {
            let mut r = fixture(w);
            let g = group(&mut r);
            match mode {
                0 => {
                    g.pop();
                }
                1 => {
                    g.push(g[0].clone());
                }
                2 => {
                    g[0]["stackHeight"] = Value::Null;
                }
                3 => {
                    g[1]["stackHeight"] = json!(3);
                }
                4 => {
                    let mut d = raw(&g[1]);
                    d.pop();
                    set_raw(&mut g[1], &d);
                }
                5 => {
                    g[1]["accounts"][2] = g[0]["accounts"][2].clone();
                }
                6 => {
                    g[0]["accounts"][1] = g[1]["accounts"][1].clone();
                }
                7 => {
                    let mut d = raw(&g[1]);
                    d[1] ^= 1;
                    set_raw(&mut g[1], &d);
                }
                _ => unreachable!(),
            }
            assert_terminal(&r);
        }
    }
}
#[test]
fn owner_mint_program_rows_and_unique_bindings_are_required() {
    for w in TARGETS {
        for mode in 0..9 {
            let mut r = fixture(w);
            let a = accounts(&r);
            let target = if w == 6 { a[16] } else { a[15] };
            let sol_pool = if w == 2 { a[5] } else { a[5] };
            let index = if matches!(mode, 1 | 2) {
                sol_pool
            } else {
                target
            };
            let row = r["meta"]["preTokenBalances"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|row| row["accountIndex"] == index)
                .unwrap();
            match mode {
                0 => row["owner"] = json!(keys(&fixture(w))[a[2]]),
                1 => row["owner"] = json!(keys(&fixture(w))[0]),
                2 => row["mint"] = json!(keys(&fixture(w))[0]),
                3 => row["programId"] = json!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"),
                4 => {
                    row.as_object_mut().unwrap().remove("owner");
                }
                5 => {
                    let duplicate = row.clone();
                    r["meta"]["preTokenBalances"]
                        .as_array_mut()
                        .unwrap()
                        .push(duplicate);
                }
                6 => {
                    let dup = r["meta"]["innerInstructions"][0].clone();
                    r["meta"]["innerInstructions"]
                        .as_array_mut()
                        .unwrap()
                        .push(dup);
                }
                7 => {
                    let p = parent(&r);
                    r["transaction"]["message"]["instructions"][p]["accounts"][16] = json!(a[15]);
                }
                8 => {
                    r["transaction"]["message"]["accountKeys"][1] =
                        r["transaction"]["message"]["accountKeys"][0].clone();
                }
                _ => unreachable!(),
            }
            assert_terminal(&r);
        }
    }
}
#[test]
fn recognized_parent_and_pool_mint_damage_cannot_become_native_cash() {
    for w in TARGETS {
        for mode in 0..5 {
            let mut r = fixture(w);
            let p = parent(&r);
            let ix = &mut r["transaction"]["message"]["instructions"][p];
            match mode {
                0 => {
                    let mut d = raw(ix);
                    d.pop();
                    set_raw(ix, &d);
                }
                1 => {
                    ix["accounts"].as_array_mut().unwrap().pop();
                    ix["accounts"].as_array_mut().unwrap().pop();
                }
                2 => {
                    set_raw(ix, &[]);
                }
                3 => {
                    let (_, _, out) = expected(w);
                    let mut d = raw(ix);
                    d[9..17].copy_from_slice(&(out + 1).to_le_bytes());
                    set_raw(ix, &d);
                }
                4 => {
                    let a = accounts(&r);
                    for field in ["preTokenBalances", "postTokenBalances"] {
                        for row in r["meta"][field].as_array_mut().unwrap() {
                            if row["accountIndex"] == a[5] {
                                row["mint"] = json!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            assert_terminal(&r);
        }
    }
}
#[test]
fn close_alone_does_not_prove_mint_owner_or_setup() {
    for w in TARGETS {
        for mode in 0..4 {
            let mut r = fixture(w);
            let p = parent(&r);
            let k = keys(&r);
            if mode == 0 {
                r["meta"]["innerInstructions"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|g| g["index"] == p);
            }
            if mode == 0 && w == 6 {
                set_raw(&mut r["transaction"]["message"]["instructions"][3], &[]);
            }
            if mode == 1 {
                let top = r["transaction"]["message"]["instructions"]
                    .as_array_mut()
                    .unwrap();
                let last = top.last_mut().unwrap();
                last["accounts"][2] = json!(accounts(&fixture(w))[2]);
            }
            if mode == 2 {
                for g in r["meta"]["innerInstructions"].as_array_mut().unwrap() {
                    if g["index"] != p {
                        for ix in g["instructions"].as_array_mut().unwrap() {
                            let mut d = raw(ix);
                            if d.first() == Some(&18) {
                                d[1..33].copy_from_slice(&bs58::decode(&k[1]).into_vec().unwrap());
                                set_raw(ix, &d);
                            }
                        }
                    }
                }
                if w == 6 {
                    r["transaction"]["message"]["instructions"][3]["accounts"][2] = json!(2);
                }
            }
            if mode == 3 {
                r["transaction"]["message"]["instructions"]
                    .as_array_mut()
                    .unwrap()
                    .pop();
            }
            assert_terminal(&r);
        }
    }
}
#[test]
fn executed_cpi_direction_and_checked_legs_not_discriminator_or_limits() {
    for w in TARGETS {
        let mut r = fixture(w);
        let p = parent(&r);
        r["transaction"]["message"]["instructions"][p]["accounts"]
            .as_array_mut()
            .unwrap()
            .swap(5, 6);
        assert_exact(&r, w); // base/quote mint arrangement is not BUY/SELL.
        let mut r = fixture(w);
        let a = accounts(&r);
        let (buy, _, _) = expected(w);
        let target_mint = r["meta"]["preTokenBalances"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["accountIndex"] == if buy { a[16] } else { a[15] })
            .unwrap()["mint"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(r["meta"]["loadedAddresses"]["writable"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(r["meta"]["loadedAddresses"]["readonly"]
            .as_array()
            .unwrap()
            .is_empty());
        let target_index = keys(&r).len();
        r["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap()
            .push(json!(target_mint));
        r["transaction"]["message"]["header"]["numReadonlyUnsignedAccounts"] = json!(
            r["transaction"]["message"]["header"]["numReadonlyUnsignedAccounts"]
                .as_u64()
                .unwrap()
                + 1
        );
        r["meta"]["preBalances"]
            .as_array_mut()
            .unwrap()
            .push(json!(0));
        r["meta"]["postBalances"]
            .as_array_mut()
            .unwrap()
            .push(json!(0));
        let sol_index = keys(&r).iter().position(|key| key == SOL_MINT).unwrap();
        for (n, ix) in group(&mut r).iter_mut().enumerate() {
            let is_sol = (n == 0) == buy;
            let mut d = raw(ix);
            d[0] = 12;
            d.push(if is_sol { 9 } else { 6 });
            set_raw(ix, &d);
            ix["accounts"]
                .as_array_mut()
                .unwrap()
                .insert(1, json!(if is_sol { sol_index } else { target_index }));
        }
        assert_exact(&r, w); // synthetic checked SPL variant, both executed legs.
        let mut damaged = r.clone();
        group(&mut damaged)[0]["accounts"][1] = json!(0);
        assert_terminal(&damaged);
        let mut damaged = r.clone();
        let mut d = raw(&group(&mut damaged)[1]);
        d[9] ^= 1;
        set_raw(&mut group(&mut damaged)[1], &d);
        assert_terminal(&damaged);
        let mut r = fixture(w);
        let p = parent(&r);
        let (_, input, output) = expected(w);
        let mut d = vec![11];
        d.extend((input + 1).to_le_bytes());
        d.extend(output.to_le_bytes());
        set_raw(&mut r["transaction"]["message"]["instructions"][p], &d);
        assert_exact(&r, w); // synthetic exact-out constraint; amount still from CPI.
        let mut r = fixture(w);
        let p = parent(&r);
        r["transaction"]["message"]["instructions"][p]["accounts"]
            .as_array_mut()
            .unwrap()
            .remove(4);
        assert_exact(&r, w); // synthetic optional target-orders omission, not real17 proof.
    }
}
#[test]
fn fees_tips_creation_surplus_and_close_refund_never_change_swap_raw() {
    for w in TARGETS {
        let mut r = fixture(w);
        r["meta"]["fee"] = json!(r["meta"]["fee"].as_u64().unwrap() + 17_000);
        let balance = r["meta"]["postBalances"][0].as_u64().unwrap();
        r["meta"]["postBalances"][0] = json!(balance - 17_000);
        assert_exact(&r, w);
        let mut r = fixture(w);
        let k = keys(&r);
        let a = accounts(&r);
        let sys = k
            .iter()
            .position(|k| k == native_attribution::SYSTEM)
            .unwrap();
        let destination = a[1];
        let mut d = vec![2, 0, 0, 0];
        d.extend(4_321u64.to_le_bytes());
        r["transaction"]["message"]["instructions"].as_array_mut().unwrap().push(json!({"programIdIndex":sys,"accounts":[0,destination],"data":bs58::encode(d).into_string()}));
        r["meta"]["postBalances"][0] =
            json!(r["meta"]["postBalances"][0].as_u64().unwrap() - 4_321);
        r["meta"]["postBalances"][destination] =
            json!(r["meta"]["postBalances"][destination].as_u64().unwrap() + 4_321);
        assert_exact(&r, w);
    }
    let mut r = fixture(6);
    let mut d = raw(&r["transaction"]["message"]["instructions"][1]);
    let creation = u64::from_le_bytes(d[4..12].try_into().unwrap());
    d[4..12].copy_from_slice(&(creation + 1_000_000).to_le_bytes());
    set_raw(&mut r["transaction"]["message"]["instructions"][1], &d);
    assert_exact(&r, 6);
}
#[test]
fn existing_owned_wsol_initial_stock_is_not_sale_proceeds() {
    let mut r = fixture(2);
    let a = accounts(&r);
    let quote = a[16];
    let p = parent(&r);
    let k = keys(&r);
    let swap = r["transaction"]["message"]["instructions"][p].clone();
    let close = r["transaction"]["message"]["instructions"][p + 1].clone();
    r["transaction"]["message"]["instructions"] = json!([swap, close]);
    let mut g = r["meta"]["innerInstructions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["index"] == p)
        .unwrap()
        .clone();
    g["index"] = json!(0);
    r["meta"]["innerInstructions"] = json!([g]);
    r["meta"]["preBalances"][quote] = json!(2_000_000_000u64 + 2_039_280);
    r["meta"]["preTokenBalances"].as_array_mut().unwrap().push(json!({"accountIndex":quote,"mint":SOL_MINT,"owner":k[0],"programId":TOKEN,"uiTokenAmount":{"amount":"2000000000","decimals":9,"uiAmountString":"2","uiAmount":2.0}}));
    assert_exact(&r, 2);
    let mut damaged = r.clone();
    damaged["meta"]["preTokenBalances"]
        .as_array_mut()
        .unwrap()
        .retain(|row| row["accountIndex"] != quote);
    assert_terminal(&damaged);
}
#[test]
fn unrelated_token_movement_and_second_parent_remain_ambiguous() {
    for w in TARGETS {
        let mut r = fixture(w);
        let extra = group(&mut r)[0].clone();
        r["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .push(extra);
        assert_terminal(&r);
        let mut r = fixture(w);
        let p = parent(&r);
        let extra = r["transaction"]["message"]["instructions"][p].clone();
        r["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .push(extra);
        assert_terminal(&r);
    }
}
