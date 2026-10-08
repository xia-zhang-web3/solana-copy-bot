use super::*;

const OTHER_QUOTE: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[test]
fn unrelated_non_sol_quote_retains_original_not_applicable_boundary() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let a = accounts(&r);
        r["transaction"]["message"]["accountKeys"][a[4]] = json!(OTHER_QUOTE);
        for field in ["preTokenBalances", "postTokenBalances"] {
            for row in r["meta"][field].as_array_mut().unwrap() {
                if row["mint"] == SOL {
                    row["mint"] = json!(OTHER_QUOTE);
                }
            }
        }
        assert_eq!(json_native(&r, false)["refused"], "not_applicable");
    }
}

#[test]
fn corrupted_quote_mint_cannot_escape_via_persistent_or_native_fallback() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let a = accounts(&r);
        r["transaction"]["message"]["accountKeys"][a[4]] = json!(OTHER_QUOTE);
        assert_eq!(json_native(&r, false)["refused"], "unknown");
        assert_refused(&r);
    }
}

#[test]
fn corrupted_base_mint_and_fresh_quote_owner_remain_terminal() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let a = accounts(&r);
        let p = parent_index(&r);
        r["transaction"]["message"]["instructions"][p]["accounts"][3] = json!(a[4]);
        assert_eq!(json_native(&r, false)["refused"], "unknown");
        assert_refused(&r);
    }
    for wallet in [11, 12] {
        let mut r = fixture(wallet);
        let a = accounts(&r);
        let setup = r["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|ix| {
                ix["accounts"]
                    .as_array()
                    .is_some_and(|keys| keys.len() == 6 && keys[1] == a[6])
            })
            .unwrap();
        setup["accounts"][2] = json!(a[0]);
        assert_refused(&r);
    }
}

#[test]
fn truncated_checked_profile_is_unknown_but_legacy_unchecked_layout_stays_nonexact() {
    let mut r = fixture(12);
    let parent = parent_index(&r);
    r["transaction"]["message"]["instructions"][parent]["accounts"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert_eq!(json_native(&r, false)["refused"], "unknown");
    assert_refused(&r);
    for ix in group(&mut r) {
        let bytes = bs58::decode(ix["data"].as_str().unwrap())
            .into_vec()
            .unwrap();
        if bytes.len() == 10 && bytes[0] == 12 {
            let mut unchecked = vec![3];
            unchecked.extend_from_slice(&bytes[1..9]);
            ix["data"] = bs58::encode(unchecked).into_string().into();
            ix["accounts"].as_array_mut().unwrap().remove(1);
        }
    }
    assert_eq!(json_native(&r, false)["refused"], "not_applicable");
    let facts = decode(&r);
    assert!(!facts.is_null(), "retain legacy nonexact observation");
    assert!(facts["exact_amounts"].is_null());
    r["transaction"]["message"]["instructions"][parent]["accounts"]
        .as_array_mut()
        .unwrap()
        .push(json!(0));
    assert_eq!(json_native(&r, false)["refused"], "unknown");
    assert_refused(&r);
}
