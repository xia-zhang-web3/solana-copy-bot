mod quote_portfolio_report_decimals_support;
use quote_portfolio_report_decimals_support::*;
use serde_json::{json, Value};

const FIELDS: [&str; 10] = [
    "inputDecimals",
    "outputDecimals",
    "inDecimals",
    "outDecimals",
    "meta.inDecimals",
    "meta.outDecimals",
    "meta.inputDecimals",
    "meta.outputDecimals",
    "inputToken.decimals",
    "outputToken.decimals",
];
fn expected(field: &str) -> u8 {
    if field.contains("out") {
        3
    } else {
        9
    }
}
fn set(v: &mut Value, field: &str, n: Value) {
    if let Some((container, key)) = field.split_once('.') {
        if v[container].is_null() {
            v[container] = json!({});
        }
        v[container][key] = n;
    } else {
        v[field] = n;
    }
}
fn fixture(name: &str, text: &str) -> Fixture {
    Fixture::new(name, |_, c| {
        c.execute(
            "UPDATE execution_quote_canary_events SET quote_response_json=?1 WHERE event_id='buy'",
            [text],
        )
        .unwrap();
    })
}

#[test]
fn every_present_supported_assertion_must_agree_and_be_exact() {
    for field in FIELDS {
        for (case, n, reason) in [
            (
                "conflict",
                json!(expected(field) - 1),
                "response decimals conflict",
            ),
            ("null", json!(null), "malformed decimals assertion"),
            ("fraction", json!(3.5), "malformed decimals assertion"),
            ("string", json!("03"), "malformed decimals assertion"),
        ] {
            let mut v: Value = serde_json::from_str(&payload("buy", 10_000_000, 10_000)).unwrap();
            set(&mut v, field, n);
            let name = format!("decimals-assert-{field}-{case}");
            let f = fixture(&name, &v.to_string());
            assert_unknown(
                &f.pair("refused", &scenario(1_000_000_000, 1, vec![buy()])),
                reason,
            );
        }
    }
}

#[test]
fn duplicate_assertions_and_containers_cannot_be_silently_overwritten() {
    for field in FIELDS
        .into_iter()
        .chain(["meta", "inputToken", "outputToken"])
    {
        let mut v: Value = serde_json::from_str(&payload("buy", 10_000_000, 10_000)).unwrap();
        let container = ["meta", "inputToken", "outputToken"].contains(&field);
        if container {
            v[field] = json!({});
        } else {
            set(&mut v, field, json!(expected(field)));
        }
        let key = field.rsplit('.').next().unwrap();
        let old = format!(
            "\"{key}\":{}",
            if container {
                "{}".into()
            } else {
                expected(field).to_string()
            }
        );
        let duplicate = format!("{old},{old}");
        let text = v.to_string().replacen(&old, &duplicate, 1);
        assert_ne!(text, v.to_string());
        let f = fixture(&format!("decimals-duplicate-{field}"), &text);
        assert_unknown(
            &f.pair("duplicate", &scenario(1_000_000_000, 1, vec![buy()])),
            "duplicate field",
        );
    }
}

#[test]
fn absent_assertions_or_all_agreeing_aliases_work_only_with_explicit_metadata() {
    for case in [
        "absent",
        "all-numeric",
        "all-strings",
        "strict-numeric",
        "strict-strings",
    ] {
        let mut v: Value = serde_json::from_str(&payload("buy", 10_000_000, 10_000)).unwrap();
        v.as_object_mut().unwrap().remove("meta");
        if case != "absent" {
            for field in FIELDS {
                set(
                    &mut v,
                    field,
                    if case.ends_with("strings") {
                        json!(expected(field).to_string())
                    } else {
                        json!(expected(field))
                    },
                );
            }
        }
        let f = fixture(&format!("decimals-supported-{case}"), &v.to_string());
        let mut b = buy();
        if case.starts_with("strict") {
            b["action"]["quote"]
                .as_object_mut()
                .unwrap()
                .remove("decimals_source");
        }
        let report = f.pair(case, &scenario(1_000_000_000, 1, vec![b]));
        if case == "strict-strings" {
            assert_unknown(&report, "strict root response decimals missing");
        } else {
            assert_eq!(
                report["events"][0]["outcome"]["disposition"]["state"], "applied",
                "{report}"
            );
            assert_eq!(report["book"]["cash_lamports"], "990000000");
        }
    }
}

#[test]
fn malformed_containers_pump_shape_and_inexact_response_operands_stay_unknown() {
    for case in [
        "meta-null",
        "inputToken-null",
        "outputToken-array",
        "inputToken-array",
        "meta-array",
        "root-array",
        "negative",
        "overflow",
        "boolean",
        "pump",
        "missing-mint",
        "missing-amount",
        "wrong-mint",
        "wrong-amount",
        "duplicate-mint",
        "duplicate-amount",
    ] {
        let mut v: Value = serde_json::from_str(&payload("buy", 10_000_000, 10_000)).unwrap();
        match case {
            "meta-null" => v["meta"] = json!(null),
            "inputToken-null" => v["inputToken"] = json!(null),
            "outputToken-array" => v["outputToken"] = json!([]),
            "inputToken-array" => v["inputToken"] = json!([9]),
            "meta-array" => v["meta"] = json!([]),
            "root-array" => v = json!([SOL, mint(1), "10000000", "10000"]),
            "negative" => v["outputDecimals"] = json!(-1),
            "overflow" => v["outputDecimals"] = json!(256),
            "boolean" => v["outputDecimals"] = json!(true),
            "pump" => v = json!({"quote":v}),
            "missing-mint" => {
                v.as_object_mut().unwrap().remove("inputMint");
            }
            "missing-amount" => {
                v.as_object_mut().unwrap().remove("inAmount");
            }
            "wrong-mint" => v["inputMint"] = json!(mint(2)),
            "wrong-amount" => v["outAmount"] = json!("999999999"),
            _ => {}
        }
        let mut text = v.to_string();
        if case == "duplicate-mint" {
            text = text.replacen('{', &format!("{{\"inputMint\":\"{SOL}\","), 1);
        }
        if case == "duplicate-amount" {
            text = text.replacen('{', "{\"inAmount\":\"10000000\",", 1);
        }
        let f = fixture(&format!("decimals-malformed-{case}"), &text);
        assert_unknown(
            &f.pair(case, &scenario(1_000_000_000, 1, vec![buy()])),
            "quote response binding",
        );
    }
}

#[test]
fn b64_generic_shape_without_mints_does_not_acquire_them_from_observed_metadata() {
    // app_tests/b64_http_fixture.rs generic branch: unchanged shape, concrete amount query.
    let v = json!({"inAmount":"10000000","outAmount":"1000000",
        "routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]});
    let f = Fixture::new("decimals-b64-no-mints", |_, c| {
        c.execute("UPDATE execution_quote_canary_events SET quote_response_json=?1,quote_out_amount_raw='1000000' WHERE event_id='buy'",[v.to_string()]).unwrap();
    });
    let mut b = buy();
    b["action"]["quote"]["output_raw"] = json!("1000000");
    assert_unknown(
        &f.pair("b64", &scenario(1_000_000_000, 1, vec![b])),
        "missing field `inputMint`",
    );
}
