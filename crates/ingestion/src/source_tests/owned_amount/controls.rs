use super::*;

fn expected(f: &Value, sell: bool, native: bool, ray: bool) -> Value {
    let token = f["roles"]["quote_mint"].clone();
    let (token_in, token_out) = if sell {
        (token, json!(SOL))
    } else {
        (json!(SOL), token)
    };
    let fee = f["result"]["meta"]["fee"].as_u64().unwrap() as f64 / 1e9;
    let sol = if !native {
        1.0
    } else if sell {
        1.0 - fee
    } else {
        1.0 + fee
    };
    let (amount_in, amount_out) = if sell { (10.0, sol) } else { (sol, 10.0) };
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
    json!({"signature":f["signature"],"wallet":f["roles"]["user"],"slot":f["result"]["slot"],"ts_utc":ts,
        "dex":if ray {"raydium"}else{"pumpswap"},"token_in":token_in,"token_out":token_out,"amount_in":amount_in,"amount_out":amount_out,"exact_amounts":exact})
}

#[test]
fn healthy_inventory_and_multiple_accounts_preserve_all_fields() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for split in [false, true] {
                for ray in [false, true] {
                    let mut f = fixtures::healthy(sell, native, split, ray);
                    fixtures::rename(
                        &mut f,
                        &format!("b48-healthy-{sell}-{native}-{split}-{ray}"),
                    );
                    for provider in PROVIDERS {
                        assert_eq!(
                            provider::check(&f, provider, None, true)?["event"],
                            expected(&f, sell, native, ray)
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn invalid_foreign_owner_is_ignored_before_and_after() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for post in [false, true] {
                for damage in fixtures::DAMAGES {
                    let mut f = fixtures::healthy(sell, native, true, false);
                    fixtures::rename(
                        &mut f,
                        &format!("b48-foreign-{sell}-{native}-{post}-{}", damage.label()),
                    );
                    let op = Operand {
                        post,
                        foreign: true,
                        damage,
                    };
                    for provider in PROVIDERS {
                        assert_eq!(
                            provider::check(&f, provider, Some(op), true)?["event"],
                            expected(&f, sell, native, false)
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
