mod quote_portfolio_report_support;
use quote_portfolio_report_support::*;
use serde_json::{json, Value};

#[test]
fn actual_two_clis_match_the_four_cash_cap_oracles_from_one_immutable_db() {
    let f = Fixture::new("oracle");
    for (cash, cap, expected, reasons) in [
        (1_000_000_000, 1, 710_429_710, 2),
        (1_000_000_000, 2, 710_429_710, 1),
        (2_000_000_000, 1, 1_710_429_710, 1),
        (2_000_000_000, 2, 1_108_368_427, 0),
    ] {
        let v = f.pair(&format!("cash-{cash}-cap-{cap}"), Some(&oracle(cash, cap)));
        assert_eq!(v["status"], "replayed");
        assert_eq!(v["book"]["cash_lamports"], expected.to_string());
        let a = &v["book"]["positions"]["A"];
        assert_eq!(a["remaining_raw"], "60");
        assert_eq!(a["remainder"]["priority"], "4201");
        assert_eq!(
            v["events"][2]["outcome"]["allocated_this_event"]["priority"],
            "2802"
        );
        let b = &v["events"][1]["outcome"];
        if reasons == 0 {
            assert_eq!(b["disposition"]["state"], "applied");
            assert_eq!(v["book"]["positions"]["B"]["remaining_raw"], "100");
        } else {
            assert_eq!(b["disposition"]["state"], "skipped");
            assert_eq!(
                b["disposition"]["reasons"].as_array().unwrap().len(),
                reasons
            );
            assert_eq!(b["before"], b["after"]);
        }
        assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
        assert!(v["dataset_coverage"]["unknown"].is_string());
        assert_eq!(v["production_green"], false);
        for e in v["events"].as_array().unwrap() {
            assert_eq!(e["source_binding"]["state"], "verified_db_fields");
        }
    }
}

#[test]
fn full_residual_mark_is_required_and_input_completeness_is_separate_from_dataset() {
    let f = Fixture::new("marks");
    for (id, raw) in [("mark-a40", 40), ("mark-a60", 60)] {
        let mut input = oracle(1_000_000_000, 1);
        let mark = event(
            id,
            "A",
            4,
            json!({"kind":"mark","quote":reference(id,"A",1,"sell",raw,if raw==60 {450_000_000} else {312_500_000}),"costs":costs(id,"A","sell")}),
        );
        input["events"].as_array_mut().unwrap().push(mark);
        let v = f.pair(id, Some(&input));
        if raw == 60 {
            assert_eq!(
                v["valuation"]["full_equity_lamports"]["known"],
                "1162468990"
            );
            assert_eq!(v["book"]["positions"]["A"]["mark"]["raw"], "60");
        } else {
            assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
            assert_eq!(v["events"][3]["outcome"]["disposition"]["state"], "refused");
        }
        assert!(v["dataset_full_equity_lamports"]["unknown"].is_string());
        input["window"]["input_complete"] = json!(false);
        let v = f.pair(&format!("{id}-incomplete"), Some(&input));
        assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
        assert_eq!(v["input_coverage"]["caller_declared_complete"], false);
    }
}

#[test]
fn initial_assumptions_tag_even_empty_or_fully_observed_history() {
    let f = Fixture::new("initial");
    for field in [
        "cash_lamports",
        "max_open_positions",
        "inventory_raw",
        "external_transfers_lamports",
    ] {
        let mut i = scenario(100, 1, vec![]);
        observed(&mut i);
        i["initial"][field]["provenance"] = json!({"assumed":"initial scenario"});
        let v = f.pair(field, Some(&i));
        assert_eq!(v["valuation"]["full_equity_lamports"]["known"], "100");
        assert_eq!(v["valuation"]["basis"], "assumed_or_synthetic_operands");
        assert_eq!(v["assumed_or_synthetic"], true);
    }
    let mut i = scenario(100, 1, vec![]);
    observed(&mut i);
    let v = f.pair("observed", Some(&i));
    assert_eq!(v["valuation"]["basis"], "caller_observed_operands");
    i["scenario_provenance"] = json!({"assumed":"empty scenario"});
    assert_eq!(
        f.pair("scenario", Some(&i))["valuation"]["basis"],
        "assumed_or_synthetic_operands"
    );
}

#[test]
fn no_input_matches_accepted_cli_fields_and_gates_except_run_timestamps() {
    let f = Fixture::new("no-input");
    let v = f.pair("absent", None);
    assert_eq!(v["status"], "unavailable");
    assert_eq!(v["reason"], "portfolio_input_not_supplied");
    if let Some(baseline) = std::env::var_os("BATCH76_BASELINE_BIN_DIR") {
        for bin in BINS {
            let (mut old, old_code) =
                run(&std::path::PathBuf::from(&baseline).join(bin), &f.db, None);
            let (mut new, new_code) = run(&binary(bin), &f.db, None);
            assert_eq!(old_code, new_code);
            std::fs::write(
                f.dir.join(format!("baseline.{bin}.json")),
                serde_json::to_vec_pretty(&old).unwrap(),
            )
            .unwrap();
            std::fs::write(
                f.dir.join(format!("current.{bin}.json")),
                serde_json::to_vec_pretty(&new).unwrap(),
            )
            .unwrap();
            new.as_object_mut().unwrap().remove("portfolio_replay");
            strip_as_of(&mut old);
            strip_as_of(&mut new);
            assert_eq!(old, new, "legacy financial fields or gates changed");
        }
    }
}
fn strip_as_of(v: &mut Value) {
    match v {
        Value::Object(o) => {
            o.remove("as_of");
            for v in o.values_mut() {
                strip_as_of(v);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(strip_as_of),
        _ => {}
    }
}
