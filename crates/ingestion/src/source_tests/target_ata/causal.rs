use super::*;

#[test]
fn target_ata_owner_mint_pda_classic_and_complete_metadata_are_required() {
    for b in bindings() {
        for mode in 0..12 {
            let mut r = saved(b["label"].as_str().unwrap());
            let t = accounts(&r)[16];
            let k = keys(&r);
            let ata = target_ata(&r);
            match mode {
                0 | 1 | 2 | 3 | 4 | 5 => {
                    let field = if mode % 2 == 0 {
                        "preTokenBalances"
                    } else {
                        "postTokenBalances"
                    };
                    let row = r["meta"][field]
                        .as_array_mut()
                        .unwrap()
                        .iter_mut()
                        .find(|row| row["accountIndex"] == t)
                        .unwrap();
                    match mode / 2 {
                        0 => row["owner"] = json!(k[1]),
                        1 => row["mint"] = json!(SOL_MINT),
                        2 => {
                            row["programId"] = json!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
                        }
                        _ => unreachable!(),
                    }
                }
                6 | 7 => {
                    let field = if mode == 6 {
                        "preTokenBalances"
                    } else {
                        "postTokenBalances"
                    };
                    r["meta"][field]
                        .as_array_mut()
                        .unwrap()
                        .retain(|row| row["accountIndex"] != t);
                }
                8 => {
                    // Coherent replacement target key violates canonical PDA only.
                    r["transaction"]["message"]["accountKeys"][t] =
                        json!(bs58::encode([42u8; 32]).into_string());
                }
                9 => r["transaction"]["message"]["instructions"][ata]["accounts"][2] = json!(1),
                10 => r["transaction"]["message"]["instructions"][ata]["accounts"][3] = json!(0),
                11 => r["transaction"]["message"]["instructions"][ata]["accounts"][5] = json!(0),
                _ => unreachable!(),
            }
            assert_terminal(&r);
        }
    }
}
#[test]
fn target_ata_must_be_pre_swap_idempotent_and_have_no_cpi_side_effects() {
    for b in bindings() {
        for mode in 0..6 {
            let mut r = saved(b["label"].as_str().unwrap());
            let ata = target_ata(&r);
            let p = parent(&r);
            match mode {
                0 => set_raw(&mut r["transaction"]["message"]["instructions"][ata], &[]),
                1 => set_raw(&mut r["transaction"]["message"]["instructions"][ata], &[0]),
                2 => set_raw(
                    &mut r["transaction"]["message"]["instructions"][ata],
                    &[1, 0],
                ),
                3 => {
                    let side_effect = group(&mut r)[0].clone();
                    r["meta"]["innerInstructions"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"index":ata,"instructions":[side_effect]}));
                }
                4 => {
                    let top = r["transaction"]["message"]["instructions"]
                        .as_array_mut()
                        .unwrap();
                    let last = top.len() - 1;
                    top.swap(ata, last); // Group indices/parent remain untouched, ATA after swap.
                }
                5 => {
                    let a = r["transaction"]["message"]["instructions"][ata]["accounts"]
                        .as_array_mut()
                        .unwrap();
                    a.extend([json!(0), json!(0)]);
                }
                _ => unreachable!(),
            }
            assert!(ata < p);
            assert!(
                proto(&r).get("token_in").is_none(),
                "label={} mode={mode}",
                b["label"]
            );
            assert_terminal(&r);
        }
    }
}
#[test]
fn executed_transfers_pool_delta_and_wsol_lifecycle_remain_required() {
    for b in bindings() {
        for mode in 0..7 {
            let mut r = saved(b["label"].as_str().unwrap());
            let a = accounts(&r);
            let quote = a[15];
            match mode {
                0 | 1 => {
                    let g = group(&mut r);
                    let mut d = raw(&g[mode]);
                    d[1] ^= 1;
                    set_raw(&mut g[mode], &d);
                }
                2 => {
                    group(&mut r).pop();
                }
                3 => group(&mut r)[0]["accounts"][2] = json!(1),
                4 => {
                    let k = keys(&r);
                    // Removing SyncNative breaks the proven funding path.
                    let top = r["transaction"]["message"]["instructions"]
                        .as_array_mut()
                        .unwrap();
                    let ix = top
                        .iter_mut()
                        .find(|i| {
                            k[i["programIdIndex"].as_u64().unwrap() as usize] == TOKEN
                                && raw(i) == [17]
                        })
                        .unwrap();
                    set_raw(ix, &[17, 0]);
                }
                5 => {
                    let close = r["transaction"]["message"]["instructions"]
                        .as_array_mut()
                        .unwrap()
                        .last_mut()
                        .unwrap();
                    assert_eq!(raw(close), [9]);
                    close["accounts"][1] = json!(1);
                }
                6 => {
                    r["meta"]["postTokenBalances"].as_array_mut().unwrap().push(json!({
                        "accountIndex":quote,"mint":SOL_MINT,"owner":keys(&saved(b["label"].as_str().unwrap()))[0],
                        "programId":TOKEN,"uiTokenAmount":{"amount":"0","decimals":9,"uiAmount":0.0,"uiAmountString":"0"}}));
                }
                _ => unreachable!(),
            }
            assert_terminal(&r);
        }
    }
}
#[test]
fn fees_tips_funding_surplus_and_initial_wsol_do_not_supply_swap_amounts() {
    for b in bindings() {
        let original = saved(b["label"].as_str().unwrap());
        let mut r = original.clone();
        r["meta"]["fee"] = json!(r["meta"]["fee"].as_u64().unwrap() + 17_000);
        r["meta"]["postBalances"][0] =
            json!(r["meta"]["postBalances"][0].as_u64().unwrap() - 17_000);
        assert_buy(&r, &b);
        let mut r = original.clone();
        let k = keys(&r);
        let quote = accounts(&r)[15];
        let top = r["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap();
        let fund = top
            .iter_mut()
            .find(|i| {
                k[i["programIdIndex"].as_u64().unwrap() as usize] == native_attribution::SYSTEM
                    && i["accounts"][1] == quote
                    && raw(i)[..4] == [2, 0, 0, 0]
            })
            .unwrap();
        let mut d = raw(fund);
        let n = u64::from_le_bytes(d[4..12].try_into().unwrap());
        d[4..12].copy_from_slice(&(n + 1_000_000).to_le_bytes());
        set_raw(fund, &d);
        assert_buy(&r, &b); // Excess funding/close refund cannot enter either leg.
        let mut r = original.clone();
        let sys = k
            .iter()
            .position(|s| s == native_attribution::SYSTEM)
            .unwrap();
        let mut d = vec![2, 0, 0, 0];
        d.extend(4_321u64.to_le_bytes());
        r["transaction"]["message"]["instructions"].as_array_mut().unwrap().push(json!({
            "programIdIndex":sys,"accounts":[0,accounts(&original)[1]],"data":bs58::encode(d).into_string()}));
        r["meta"]["postBalances"][0] =
            json!(r["meta"]["postBalances"][0].as_u64().unwrap() - 4_321);
        assert_buy(&r, &b);
        // Existing WSOL with an initial stock, idempotent noop and later funding.
        let mut r = original.clone();
        let quote_ata = r["transaction"]["message"]["instructions"]
            .as_array()
            .unwrap()
            .iter()
            .position(|i| {
                k[i["programIdIndex"].as_u64().unwrap() as usize] == ATA
                    && i["accounts"][1] == quote
            })
            .unwrap();
        set_raw(
            &mut r["transaction"]["message"]["instructions"][quote_ata],
            &[1],
        );
        r["meta"]["innerInstructions"]
            .as_array_mut()
            .unwrap()
            .retain(|g| g["index"] != quote_ata);
        r["meta"]["preBalances"][quote] = json!(2_039_280 + 2_000_000_000u64);
        r["meta"]["preTokenBalances"]
            .as_array_mut()
            .unwrap()
            .push(json!({"accountIndex":quote,
            "mint":SOL_MINT,"owner":k[0],"programId":TOKEN,"uiTokenAmount":{
                "amount":"2000000000","decimals":9,"uiAmount":2.0,"uiAmountString":"2"}}));
        assert_buy(&r, &b);
        let mut damaged = r.clone();
        damaged["meta"]["preTokenBalances"]
            .as_array_mut()
            .unwrap()
            .retain(|row| row["accountIndex"] != quote);
        assert_terminal(&damaged);
    }
}
