mod quote_portfolio_report_time_support;
use quote_portfolio_report_time_support::*;
use serde_json::json;

#[test]
fn actual_two_clis_gate_buy_sell_mark_at_nanosecond_boundary_and_continue_b() {
    let mut captures = Vec::new();
    for kind in ["buy", "sell", "mark"] {
        let (input, row, index, boundary) = case(kind);
        for (control, start) in [
            ("healthy", started(MS, 0)),
            ("boundary", started(boundary, 0)),
            ("plus-one-ns", started(boundary, 1)),
            ("after-window", started(MS + 1001, 0)),
        ] {
            let f = fixture(&format!("time-{kind}-{control}"), row, Some(&start));
            let v = f.pair("input", Some(&input));
            captures.push((kind, control, start, index, v));
        }
    }
    // Capture every actual pair before assertions, preserving the full RED matrix.
    for (kind, control, _, index, v) in &captures {
        let expected = if ["plus-one-ns", "after-window"].contains(control) {
            "refused"
        } else {
            "applied"
        };
        assert_eq!(
            v["events"][*index]["outcome"]["disposition"]["state"], expected,
            "{kind}/{control}: {v}"
        );
    }
    for (kind, control, start, index, v) in captures {
        let blocked = ["plus-one-ns", "after-window"].contains(&control);
        let e = &v["events"][index];
        assert_eq!(
            e["outcome"]["disposition"]["state"],
            if blocked { "refused" } else { "applied" },
            "{kind}/{control}: {v}"
        );
        if blocked {
            refused(&v, index, "quote HTTP start after declared transition");
        } else {
            verified(&v, index, &start);
        }
        let last = v["events"].as_array().unwrap().last().unwrap();
        assert_eq!(last["outcome"]["disposition"]["state"], "applied");
        assert_eq!(v["book"]["positions"]["B"]["remaining_raw"], "1");
        let (cash, slots) = match (kind, blocked) {
            ("buy", true) => (999, 1),
            ("sell", false) => (986, 1),
            _ => (885, 2),
        };
        assert_eq!(v["book"]["cash_lamports"], cash.to_string());
        assert_eq!(v["book"]["open_slots"], slots.to_string());
        if blocked && kind == "buy" {
            assert!(v["book"]["positions"]["A"].is_null());
            assert_eq!(
                v["book"]["flows"],
                json!({"buy_principal":"1","sell_gross":"0",
                "expenses":"0","rent_deposited":"0","rent_refunded":"0"})
            );
        } else if blocked {
            assert_eq!(v["book"]["positions"]["A"]["remaining_raw"], "100");
            assert_eq!(
                v["book"]["flows"],
                json!({"buy_principal":"101","sell_gross":"0",
                "expenses":"9","rent_deposited":"5","rent_refunded":"0"})
            );
            assert!(v["book"]["positions"]["A"]["mark"].is_null());
        }
        assert_eq!(v["assumed_or_synthetic"], true);
        assert_eq!(v["production_green"], false);
        assert!(v["dataset_full_equity_lamports"]["unknown"].is_string());
    }
}

#[test]
fn missing_http_start_refuses_each_action_without_request_time_fallback() {
    for kind in ["buy", "sell", "mark"] {
        let (input, row, index, _) = case(kind);
        let f = fixture(&format!("time-missing-{kind}"), row, None);
        let v = f.pair("input", Some(&input));
        refused(&v, index, "actual quote HTTP start missing");
        assert_eq!(
            v["events"].as_array().unwrap().last().unwrap()["outcome"]["disposition"]["state"],
            "applied"
        );
        assert_eq!(
            v["events"][index]["input"]["action"]["quote"]["request_ts"],
            TS
        );
        assert_eq!(v["assumed_or_synthetic"], true);
    }
}

#[test]
fn future_mark_cannot_replace_invalidated_old_mark_after_partial_sell() {
    let input = scenario(
        1000,
        1,
        vec![
            charged_buy(),
            mark("old-mark", 2, "refund-sell", 100, 110),
            sell("sell-a40", "A", 3, 40, 312_500_000),
            mark("new-mark", 4, "mark-a60", 60, 450_000_000),
        ],
    );
    let f = fixture("time-stale-mark", "mark-a60", Some(&started(MS + 4, 1)));
    let v = f.pair("input", Some(&input));
    assert!(
        v["events"][1]["outcome"]["valuation_after"]["full_equity_lamports"]["known"].is_string()
    );
    assert!(
        v["events"][2]["outcome"]["valuation_after"]["full_equity_lamports"]["unknown"].is_string()
    );
    refused(&v, 3, "quote HTTP start after declared transition");
    assert!(v["book"]["positions"]["A"]["mark"].is_null());
    assert_eq!(v["book"]["positions"]["A"]["remaining_raw"], "60");
    assert_eq!(v["book"]["positions"]["A"]["allocated"]["priority"], "2");
    assert_eq!(v["book"]["positions"]["A"]["remainder"]["priority"], "1");
    assert_eq!(v["valuation"]["known_net_marks_lamports"], "0");
    assert_eq!(v["valuation"]["missing_marks"], json!(["A"]));
}

#[test]
fn known_zero_sell_and_mark_remain_distinct_from_time_unknown() {
    for kind in ["sell", "mark"] {
        for (label, start) in [("known", Some(started(MS + 2, 0))), ("unknown", None)] {
            let action = if kind == "sell" {
                sell("zero-sell", "A", 2, 100, 0)
            } else {
                mark("zero-mark", 2, "zero-sell", 100, 0)
            };
            let input = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100), action]);
            let f = fixture(
                &format!("time-zero-{kind}-{label}"),
                "zero-sell",
                start.as_deref(),
            );
            let v = f.pair("input", Some(&input));
            if label == "known" {
                assert_eq!(v["valuation"]["full_equity_lamports"]["known"], "0");
                verified(&v, 1, start.as_ref().unwrap());
            } else {
                refused(&v, 1, "actual quote HTTP start missing");
                assert_eq!(v["book"]["positions"]["A"]["remaining_raw"], "100");
            }
        }
    }
}
