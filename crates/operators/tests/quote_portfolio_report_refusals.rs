mod quote_portfolio_report_support;
use quote_portfolio_report_support::*;
use serde_json::json;

#[test]
fn source_raw_identity_response_and_decimals_cannot_be_overridden() {
    let f = Fixture::new("bindings");
    for case in [
        "raw",
        "mint",
        "side",
        "status",
        "wallet",
        "time",
        "response_raw",
        "response_mint",
        "decimals",
        "missing_decimals",
        "missing_response",
        "ambiguous",
        "missing_raw",
        "absent",
    ] {
        let mut e = buy("small", "A", 1, 1, 100, 100);
        e["action"]["quote"]["event_id"] = json!(format!("bad-{case}"));
        let v = f.pair(case, Some(&scenario(200, 1, vec![e])));
        assert_eq!(
            v["events"][0]["source_binding"]["state"], "unavailable",
            "{case}: {v}"
        );
        assert_eq!(v["events"][0]["outcome"]["disposition"]["state"], "refused");
        assert_eq!(v["book"]["cash_lamports"], "200");
        assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
    }
}

#[test]
fn known_zero_is_distinct_from_missing_fee_quote_and_bound_zero_output() {
    let f = Fixture::new("known-zero");
    for missing in [
        "none",
        "quote",
        "base",
        "priority",
        "setup",
        "exit",
        "rent_deposit",
    ] {
        let mut e = buy("small", "A", 1, 1, 100, 100);
        if ["base", "priority", "setup", "exit"].contains(&missing) {
            e["action"]["costs"]
                .as_object_mut()
                .unwrap()
                .remove(missing);
        } else if missing != "none" {
            e["action"].as_object_mut().unwrap().remove(missing);
        }
        let v = f.pair(missing, Some(&scenario(100, 1, vec![e])));
        assert_eq!(
            v["events"][0]["outcome"]["disposition"]["state"],
            if missing == "none" {
                "applied"
            } else {
                "refused"
            }
        );
        assert_eq!(
            v["book"]["cash_lamports"],
            if missing == "none" { "0" } else { "100" }
        );
    }
    let i = scenario(
        100,
        1,
        vec![
            buy("small", "A", 1, 1, 100, 100),
            sell("zero-sell", "A", 2, 100, 0),
        ],
    );
    let v = f.pair("bound-output-zero", Some(&i));
    assert_eq!(v["events"][1]["outcome"]["disposition"]["state"], "applied");
    assert_eq!(v["valuation"]["full_equity_lamports"]["known"], "0");
}

#[test]
fn caller_order_conflict_and_exact_replay_retain_kernel_contract() {
    let f = Fixture::new("ordering");
    let a = buy("small", "A", 1, 1, 100, 100);
    let b = buy("next", "B", 2, 2, 1, 1);
    let v = f.pair(
        "replay",
        Some(&scenario(200, 2, vec![a.clone(), b.clone(), a.clone()])),
    );
    assert_eq!(v["book"]["cash_lamports"], "99");
    assert_eq!(v["events"][0]["outcome"], v["events"][2]["outcome"]);
    let mut conflict = a.clone();
    conflict["action"]["costs"]["base"] = amount(1);
    for (name, events, code) in [
        ("conflict", vec![a.clone(), conflict], "conflict_id"),
        ("out-of-order", vec![b, a], "out_of_order"),
    ] {
        let v = f.pair(name, Some(&scenario(200, 2, events)));
        assert_eq!(
            v["events"][1]["outcome"]["disposition"]["refusal"]["code"],
            code
        );
        assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
        assert_eq!(
            v["events"][1]["outcome"]["before"],
            v["events"][1]["outcome"]["after"]
        );
    }
}

#[test]
fn isolated_assumed_skip_survives_later_observed_event_and_replay() {
    let f = Fixture::new("skip75r1");
    for field in ["base", "priority", "setup", "rent_deposit"] {
        let mut a = buy("small", "A", 1, 1, 100, 100);
        observed(&mut a);
        let cost = if field == "rent_deposit" {
            &mut a["action"][field]
        } else {
            &mut a["action"]["costs"][field]
        };
        *cost = amount(1);
        cost["provenance"] = json!({"assumed":"isolated admission cost"});
        let mut b = buy("next", "B", 2, 2, 1, 1);
        observed(&mut b);
        let mut i = scenario(100, 2, vec![]);
        observed(&mut i);
        i["events"] = json!([a.clone(), b, a]);
        let v = f.pair(field, Some(&i));
        assert_eq!(v["book"]["cash_lamports"], "99");
        assert!(v["book"]["positions"]["A"].is_null());
        assert_eq!(v["events"][0]["outcome"]["disposition"]["state"], "skipped");
        assert_eq!(
            v["events"][0]["outcome"]["valuation_after"]["full_equity_lamports"]["known"],
            "100"
        );
        assert_eq!(
            v["events"][0]["outcome"]["valuation_after"]["basis"],
            "assumed_or_synthetic_operands"
        );
        assert_eq!(v["events"][0]["outcome"], v["events"][2]["outcome"]);
        assert_eq!(v["book"]["kernel_assumed_or_synthetic"], true);
    }
}

#[test]
fn costs_and_virtual_position_are_explicit_bindings_not_db_proof() {
    let f = Fixture::new("caller-binding");
    for field in [
        "cost_event",
        "cost_position",
        "cost_side",
        "quote_position",
        "quote_side",
        "quote_size",
    ] {
        let mut e = buy("small", "A", 1, 1, 100, 100);
        match field {
            "cost_event" => e["action"]["costs"]["event_id"] = json!("other"),
            "cost_position" => e["action"]["costs"]["position_id"] = json!("other"),
            "cost_side" => e["action"]["costs"]["side"] = json!("sell"),
            "quote_position" => e["action"]["quote"]["position_id"] = json!("other"),
            "quote_side" => e["action"]["quote"]["side"] = json!("sell"),
            "quote_size" => e["action"]["input_lamports"] = json!("101"),
            _ => unreachable!(),
        }
        let v = f.pair(field, Some(&scenario(200, 1, vec![e])));
        assert_eq!(v["events"][0]["outcome"]["disposition"]["state"], "refused");
    }
}

#[test]
fn changed_source_or_caller_identity_cannot_hide_as_identical_kernel_replay() {
    let f = Fixture::new("external-conflict");
    for case in ["reference", "provenance"] {
        let a = buy("small", "A", 1, 1, 100, 100);
        let mut b = a.clone();
        if case == "reference" {
            b["action"]["quote"]["event_id"] = json!("small-alt");
        } else {
            b["identity_provenance"] = json!({"synthetic":"different caller identity evidence"});
        }
        let v = f.pair(case, Some(&scenario(200, 1, vec![a, b])));
        assert_eq!(v["status"], "unavailable");
        assert!(v["reason"]
            .as_str()
            .unwrap()
            .contains("caller event identity changed"));
    }
}
