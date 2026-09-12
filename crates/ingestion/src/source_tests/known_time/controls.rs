use super::*;

fn expected(f: &Value, sell: bool, native: bool, provider: &str) -> Value {
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
    let ts = fixtures::converted(f, provider).unwrap();
    json!({"signature":f["signature"],"wallet":f["roles"]["user"],"slot":f["result"]["slot"],"ts_utc":ts,
        "dex":"pumpswap","token_in":token_in,"token_out":token_out,"amount_in":amount_in,"amount_out":amount_out,"exact_amounts":exact})
}

pub(super) fn assert_fields(f: &Value, event: &Value, sell: bool, native: bool, provider: &str) {
    assert_eq!(*event, expected(f, sell, native, provider));
}

#[test]
fn valid_time_matrix_preserves_complete_events() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for kind in 0..5 {
                let mut f =
                    fixtures::base(sell, native, &format!("b51-valid-{sell}-{native}-{kind}"));
                fixtures::valid(&mut f, kind);
                for provider in PROVIDERS {
                    let capture = provider::capture(&f, &f, provider, "control")?;
                    assert_fields(&f, &capture["event"], sell, native, provider);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn original_g8_old_block_preserves_provider_time_origin() -> Result<()> {
    let f = super::super::super::super::fixture("g8_old_block");
    let mut events = Vec::new();
    for provider in PROVIDERS {
        let capture = provider::capture(&f, &f, provider, "control")?;
        assert_fields(&f, &capture["event"], false, false, provider);
        events.push(capture["event"].clone());
    }
    assert_ne!(events[0]["ts_utc"], events[1]["ts_utc"]);
    assert_eq!(events[1], events[2]);
    events[0]["ts_utc"] = events[1]["ts_utc"].clone();
    assert_eq!(events[0], events[1]);
    Ok(())
}
