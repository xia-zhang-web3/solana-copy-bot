mod quote_portfolio_report_decimals_support;
use quote_portfolio_report_decimals_support::*;
use serde_json::{json, Value};

#[test]
fn generic_bytes_require_explicit_bound_source_for_buy_partial_sell_and_mark() {
    let f = Fixture::new("decimals-causal", |_, _| {});
    let before = f.pair("strict", &lifecycle(false));
    assert_eq!(
        before["events"][0]["source_binding"]["state"],
        "unavailable"
    );
    assert_eq!(before["book"]["cash_lamports"], "1000000000");
    let i = lifecycle(true);
    let after = f.pair("bound", &i);
    assert_eq!(after["status"], "replayed", "{after}");
    for (index, side) in ["buy", "sell", "sell"].iter().enumerate() {
        let e = &after["events"][index];
        assert_eq!(e["outcome"]["disposition"]["state"], "applied", "{after}");
        let d = &e["source_binding"]["decimals_evidence"];
        assert_eq!(d["source"], "observed_signature");
        assert_eq!(d["signature"], signature(71));
        assert_eq!(d["field"], "observed_swaps.qty_out_decimals");
        assert_eq!(d["signal_id"], signal(71, "buy"));
        assert_eq!(d["metadata_source_side"], "buy");
        assert_eq!(d["quote_side"], *side);
        assert_eq!(d["token_decimals"], "3");
        assert_eq!(d["sol_decimals"]["source"], "protocol_constant");
        assert_eq!(e["input"]["action"]["quote"]["provenance"], origin());
    }
    assert_eq!(after["book"]["cash_lamports"], "991054710");
    let a = &after["book"]["positions"]["A"];
    assert_eq!(a["remaining_raw"], "6000");
    assert_eq!(a["remainder"]["priority"], "4201");
    assert_eq!(
        after["events"][1]["outcome"]["allocated_this_event"]["priority"],
        "2802"
    );
    assert_eq!(a["mark"]["raw"], "6000");
    assert_eq!(
        after["valuation"]["full_equity_lamports"]["known"],
        "997593990"
    );
    assert!(after["dataset_full_equity_lamports"]["unknown"].is_string());
    assert!(after["dataset_net_change_lamports"]["unknown"].is_string());
    assert_eq!(after["assumed_or_synthetic"], true);
    assert_eq!(after["production_green"], false);
    let again = f.pair("same-input-repeated", &i);
    assert_eq!(after, again);
}

#[test]
fn metadata_references_cannot_hide_changes_as_an_identical_replay() {
    let f = Fixture::new("decimals-replay", |_, _| {});
    let a = buy();
    let v = f.pair(
        "exact-replay",
        &scenario(1_000_000_000, 1, vec![a.clone(), a.clone()]),
    );
    assert_eq!(v["events"][0]["outcome"], v["events"][1]["outcome"]);
    assert_eq!(v["book"]["cash_lamports"], "990000000");
    let mut b = a.clone();
    b["action"]["quote"]["decimals_source"] = source(72);
    let v = f.pair(
        "signature-only",
        &scenario(1_000_000_000, 1, vec![a.clone(), b.clone()]),
    );
    assert_eq!(v["events"][1]["source_binding"]["state"], "unavailable");
    assert_eq!(
        v["events"][1]["outcome"]["disposition"]["refusal"]["code"],
        "conflict_id"
    );
    assert_eq!(
        v["events"][1]["outcome"]["before"],
        v["events"][1]["outcome"]["after"]
    );
    b["action"]["quote"]["event_id"] = json!("alternate");
    b["action"]["quote"]["signal_id"] = json!(signal(72, "buy"));
    let v = f.pair(
        "different-valid-source",
        &scenario(1_000_000_000, 1, vec![a, b]),
    );
    assert_eq!(v["status"], "unavailable");
    assert!(v["reason"]
        .as_str()
        .unwrap()
        .contains("caller event identity changed"));
    // Change only the new optional source; both references independently verify
    // against the same row and produce an identical kernel quote.
    let f = Fixture::new("decimals-source-presence", |_, c| {
        let mut response: Value =
            serde_json::from_str(&payload("buy", 10_000_000, 10_000)).unwrap();
        response["inputDecimals"] = json!(9);
        response["outputDecimals"] = json!(3);
        c.execute(
            "UPDATE execution_quote_canary_events SET quote_response_json=?1 WHERE event_id='buy'",
            [response.to_string()],
        )
        .unwrap();
    });
    let bound = buy();
    let mut strict = bound.clone();
    strict["action"]["quote"]
        .as_object_mut()
        .unwrap()
        .remove("decimals_source");
    for (name, first, second) in [
        ("add", strict.clone(), bound.clone()),
        ("remove", bound, strict),
    ] {
        let v = f.pair(name, &scenario(1_000_000_000, 1, vec![first, second]));
        assert_eq!(v["status"], "unavailable");
        assert!(v["reason"]
            .as_str()
            .unwrap()
            .contains("caller event identity changed"));
    }
}

#[test]
fn no_input_is_identical_to_accepted76_including_portfolio_unavailable() {
    let f = Fixture::new("decimals-no-input", |_, _| {});
    f.pair_path("no-input", None);
    let baseline = std::env::var_os("BATCH77_BASELINE_BIN_DIR").unwrap();
    for bin in BINS {
        let path = f.dir.join(format!("no-input.{bin}.json"));
        let mut current: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let old_path = std::path::PathBuf::from(&baseline).join(bin);
        let mut command = std::process::Command::new(old_path);
        command
            .args(["--json", "--db-path"])
            .arg(&f.db)
            .args(["--since", TS]);
        if bin == BINS[1] {
            command.arg("--no-live-wallet");
        }
        let out = command.output().unwrap();
        assert!(out.status.success());
        assert!(out.stderr.is_empty());
        let mut old: Value = serde_json::from_slice(&out.stdout).unwrap();
        std::fs::write(f.dir.join(format!("baseline.{bin}.json")), &out.stdout).unwrap();
        strip_time(&mut current);
        strip_time(&mut old);
        assert_eq!(current, old);
    }
}
fn strip_time(v: &mut Value) {
    match v {
        Value::Object(o) => {
            o.remove("as_of");
            for v in o.values_mut() {
                strip_time(v)
            }
        }
        Value::Array(a) => a.iter_mut().for_each(strip_time),
        _ => {}
    }
}
