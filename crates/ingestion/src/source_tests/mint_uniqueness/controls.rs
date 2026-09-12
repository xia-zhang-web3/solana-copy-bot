use super::*;

pub(super) fn ledger(f: &Value) {
    let meta = &f["result"]["meta"];
    let total = |field: &str| -> u64 {
        meta[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .sum()
    };
    assert_eq!(
        total("preBalances") - total("postBalances"),
        meta["fee"].as_u64().unwrap()
    );
    let mut deltas = std::collections::BTreeMap::<String, i128>::new();
    for (field, sign) in [("preTokenBalances", -1), ("postTokenBalances", 1)] {
        for row in meta[field].as_array().unwrap() {
            let mint = row["mint"].as_str().unwrap();
            let amount = &row["uiTokenAmount"];
            let raw = amount["amount"].as_str().unwrap().parse::<i128>().unwrap();
            assert_eq!(
                amount["uiAmount"].as_f64().unwrap(),
                raw as f64 / 10_f64.powi(amount["decimals"].as_i64().unwrap() as i32)
            );
            if mint != SOL {
                *deltas.entry(mint.into()).or_default() += sign * raw;
            }
        }
    }
    assert!(deltas.values().all(|&v| v == 0), "{deltas:?}");
}

fn expected(f: &Value, sell: bool, native: bool, ray: bool) -> Value {
    let quote = f["roles"]["quote_mint"].clone();
    let base = f["roles"]["base_mint"].clone();
    let (token_in, token_out) = if sell { (quote, base) } else { (base, quote) };
    let fee = f["result"]["meta"]["fee"].as_u64().unwrap() as f64 / 1e9;
    let sol_amount = if !native {
        1.0
    } else if sell {
        1.0 - fee
    } else {
        1.0 + fee
    };
    let (amount_in, amount_out) = if sell {
        (10.0, sol_amount)
    } else {
        (sol_amount, 10.0)
    };
    let (raw_in, dec_in, raw_out, dec_out) = if sell {
        ("10000000", 6, "1000000000", 9)
    } else {
        ("1000000000", 9, "10000000", 6)
    };
    let exact = if native {
        Value::Null
    } else {
        json!({"amount_in_raw":raw_in,"amount_in_decimals":dec_in,"amount_out_raw":raw_out,"amount_out_decimals":dec_out})
    };
    let ts =
        chrono::DateTime::from_timestamp(f["result"]["blockTime"].as_i64().unwrap(), 0).unwrap();
    json!({"signature":f["signature"],"wallet":f["roles"]["user"],
        "slot":f["result"]["slot"],"ts_utc":ts,"dex":if ray {"raydium"}else{"pumpswap"},
        "token_in":token_in,"token_out":token_out,"amount_in":amount_in,"amount_out":amount_out,"exact_amounts":exact})
}

#[test]
fn unique_mint_preserves_fields_after_account_aggregation() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for ray in [false, true] {
                for mode in 0..5 {
                    let mut f = fixtures::base(sell, native);
                    match mode {
                        0 => {}
                        1 => fixtures::split_target(&mut f, sell),
                        2 => fixtures::zero_net(&mut f),
                        3 => fixtures::gift(&mut f, sell, 1, 12),
                        4 => fixtures::gift(&mut f, sell, 1, 13),
                        _ => unreachable!(),
                    }
                    if ray {
                        fixtures::raydium(&mut f);
                    }
                    fixtures::rename(&mut f, &format!("control-{sell}-{native}-{ray}-{mode}"));
                    ledger(&f);
                    for provider in PROVIDERS {
                        let record = check(&f, provider, true)?;
                        assert_eq!(
                            record["event"],
                            expected(&f, sell, native, ray),
                            "{} {provider}",
                            f["case"]
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn one_token_in_one_token_out_preserves_exact_fields() -> Result<()> {
    for ray in [false, true] {
        let mut f = fixtures::token_token();
        if ray {
            fixtures::raydium(&mut f);
        }
        fixtures::rename(&mut f, &format!("control-token-token-{ray}"));
        ledger(&f);
        for provider in PROVIDERS {
            assert_eq!(
                check(&f, provider, true)?["event"],
                expected(&f, false, false, ray)
            );
        }
    }
    Ok(())
}
