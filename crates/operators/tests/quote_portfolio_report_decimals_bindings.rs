mod quote_portfolio_report_decimals_support;
use quote_portfolio_report_decimals_support::*;
use serde_json::json;

#[test]
fn observed_zero_and_nineteen_are_exact_and_twenty_is_out_of_scope() {
    for decimals in [0, 19, 20] {
        let f = Fixture::new(&format!("decimals-boundary-{decimals}"), |_, c| {
            c.execute("UPDATE observed_swaps SET qty_out_decimals=?1", [decimals])
                .unwrap();
            let mut v: serde_json::Value =
                serde_json::from_str(&payload("buy", 10_000_000, 10_000)).unwrap();
            v.as_object_mut().unwrap().remove("meta");
            c.execute("UPDATE execution_quote_canary_events SET quote_response_json=?1 WHERE event_id='buy'",[v.to_string()]).unwrap();
        });
        let mut b = buy();
        b["action"]["decimals"] = json!(decimals);
        b["action"]["quote"]["decimals"] = json!(decimals);
        let v = f.pair("boundary", &scenario(1_000_000_000, 1, vec![b]));
        if decimals == 20 {
            assert_unknown(&v, "invalid token mint/decimals");
        } else {
            assert_eq!(v["book"]["cash_lamports"], "990000000");
            assert_eq!(
                v["events"][0]["source_binding"]["decimals_evidence"]["token_decimals"],
                decimals.to_string()
            );
        }
    }
}

#[test]
fn metadata_source_schema_does_not_accept_numeric_overrides_or_duplicate_signatures() {
    let f = Fixture::new("decimals-source-input", |_, _| {});
    for (case, source) in [
        ("numeric", json!(3)),
        ("kind", json!({"kind":"override","decimals":3})),
        (
            "extra",
            json!({"kind":"observed_signature","signature":signature(71),"decimals":3}),
        ),
        ("missing-signature", json!({"kind":"observed_signature"})),
    ] {
        let mut b = buy();
        b["action"]["quote"]["decimals_source"] = source;
        let v = f.pair(case, &scenario(1_000_000_000, 1, vec![b]));
        assert_eq!(v["status"], "unavailable");
        assert!(v["reason"]
            .as_str()
            .unwrap()
            .contains("invalid portfolio input"));
    }
    let i = scenario(1_000_000_000, 1, vec![buy()]);
    let text = i.to_string().replace(
        "\"signature\":",
        &format!("\"signature\":\"{}\",\"signature\":", signature(71)),
    );
    let path = f.dir.join("duplicate.input.json");
    std::fs::write(&path, &text).unwrap();
    std::fs::write(f.dir.join("duplicate.input.before"), &text).unwrap();
    let v = f.pair_path("duplicate", Some(&path));
    assert_eq!(v["status"], "unavailable");
    assert!(v["reason"].as_str().unwrap().contains("duplicate field"));
}

#[test]
fn missing_exact_and_mismatched_observed_or_signal_fields_cannot_supply_decimals() {
    for (case, sql, reason) in [
        (
            "no-exact",
            "UPDATE observed_swaps SET qty_out_decimals=NULL",
            "exact observed token_decimals missing",
        ),
        (
            "wrong-exact",
            "UPDATE observed_swaps SET qty_out_decimals=4",
            "differs from caller expectation",
        ),
        (
            "negative-exact",
            "UPDATE observed_swaps SET qty_out_decimals=-1",
            "observed lookup/schema unavailable",
        ),
        (
            "observed-wallet",
            "UPDATE observed_swaps SET wallet_id='other'",
            "observed wallet mismatch",
        ),
        (
            "observed-mint",
            "UPDATE observed_swaps SET token_out='other'",
            "observed mint mismatch",
        ),
        (
            "observed-side",
            "UPDATE observed_swaps SET token_in=token_out,token_out=token_in",
            "observed/signal side mismatch",
        ),
        (
            "signal-wallet",
            "UPDATE copy_signals SET wallet_id='other'",
            "signal wallet mismatch",
        ),
        (
            "signal-mint",
            "UPDATE copy_signals SET token='other'",
            "signal mint mismatch",
        ),
        (
            "signal-side",
            "UPDATE copy_signals SET side='sell'",
            "BUY quote requires a BUY metadata signal",
        ),
        (
            "signal-side-unknown",
            "UPDATE copy_signals SET side='unsupported'",
            "signal side unsupported",
        ),
        (
            "missing-signal",
            "DELETE FROM copy_signals",
            "signal row missing",
        ),
        (
            "missing-observed",
            "DELETE FROM observed_swaps",
            "observed leg missing",
        ),
        (
            "missing-observed-schema",
            "ALTER TABLE observed_swaps RENAME COLUMN qty_out_decimals TO hidden_decimals",
            "observed lookup/schema unavailable",
        ),
    ] {
        let f = Fixture::new(&format!("decimals-{case}"), |_, c| {
            c.execute_batch(sql).unwrap();
        });
        let v = f.pair(case, &scenario(1_000_000_000, 1, vec![buy()]));
        assert_unknown(&v, reason);
    }
}

#[test]
fn full_canonical_signal_and_real_row_are_required_even_for_the_same_mint() {
    for case in [
        "unrelated",
        "suffix",
        "prefix",
        "no-signal-reference",
        "quote-signal",
        "unknown-signature",
    ] {
        let mut b = buy();
        let mut id = signal(71, "buy");
        match case {
            "unrelated" => b["action"]["quote"]["decimals_source"] = source(72),
            "suffix" => id.push_str(":suffix"),
            "prefix" => id = format!("other:{id}"),
            "quote-signal" => b["action"]["quote"]["signal_id"] = json!(signal(72, "buy")),
            "unknown-signature" => {
                b["action"]["quote"]["decimals_source"] = source(73);
                id = signal(73, "buy");
            }
            _ => {}
        }
        let f = Fixture::new(&format!("decimals-canonical-{case}"), |_, c| {
            if ["suffix", "prefix", "unknown-signature"].contains(&case) {
                c.execute(
                    "UPDATE copy_signals SET signal_id=?1 WHERE signal_id=?2",
                    [&id, &signal(71, "buy")],
                )
                .unwrap();
                c.execute(
                    "UPDATE execution_quote_canary_events SET signal_id=?1 WHERE signal_id=?2",
                    [&id, &signal(71, "buy")],
                )
                .unwrap();
            }
            if case == "no-signal-reference" {
                c.execute(
                    "UPDATE execution_quote_canary_events SET signal_id=NULL",
                    [],
                )
                .unwrap();
            }
        });
        if ["suffix", "prefix", "unknown-signature"].contains(&case) {
            b["action"]["quote"]["signal_id"] = json!(id);
        }
        if case == "no-signal-reference" {
            b["action"]["quote"]["signal_id"] = json!(null);
        }
        let v = f.pair(case, &scenario(1_000_000_000, 1, vec![b]));
        assert_unknown(
            &v,
            match case {
                "quote-signal" => "quote wallet/signal/closed-trade identity mismatch",
                "no-signal-reference" => "quote signal reference missing",
                "unknown-signature" => "observed leg missing",
                _ => "canonical signal/signature mismatch",
            },
        );
    }
}

#[test]
fn exact_metadata_does_not_copy_ui_or_leader_sizes_and_can_use_a_bound_sell_leg() {
    let f = Fixture::new("decimals-ignored-ui", |_, c| {
        c.execute_batch(
            "UPDATE observed_swaps SET qty_in=9876.5,qty_out=0.000000001,qty_out_raw='7';
            UPDATE copy_signals SET notional_sol=9999.5;
            UPDATE execution_quote_canary_events SET leader_notional_sol=1234.5;",
        )
        .unwrap();
    });
    let v = f.pair("ignored-ui", &lifecycle(true));
    assert_eq!(v["book"]["cash_lamports"], "991054710");
    assert_eq!(v["book"]["positions"]["A"]["remaining_raw"], "6000");
    let f = Fixture::new("decimals-sell-metadata", |store, c| {
        insert_observed(store, 73, "sell");
        let mut sell = store
            .load_copy_signal_by_signal_id(&signal(71, "buy"))
            .unwrap()
            .unwrap();
        sell.signal_id = signal(73, "sell");
        sell.side = "sell".into();
        store.insert_copy_signal(&sell).unwrap();
        c.execute(
            "UPDATE execution_quote_canary_events SET signal_id=?1 WHERE event_id IN ('sell','mark','alternate')",
            [signal(73, "sell")],
        )
        .unwrap();
    });
    let mut i = lifecycle(true);
    for e in i["events"].as_array_mut().unwrap().iter_mut().skip(1) {
        e["action"]["quote"]["signal_id"] = json!(signal(73, "sell"));
        e["action"]["quote"]["decimals_source"] = source(73);
    }
    let v = f.pair("sell-source", &i);
    assert_eq!(v["book"]["cash_lamports"], "991054710");
    for e in v["events"].as_array().unwrap().iter().skip(1) {
        assert_eq!(
            e["source_binding"]["decimals_evidence"]["metadata_source_side"],
            "sell"
        );
        assert_eq!(
            e["source_binding"]["decimals_evidence"]["field"],
            "observed_swaps.qty_in_decimals"
        );
    }
    let mut b = buy();
    b["action"]["quote"]["event_id"] = json!("alternate");
    b["action"]["quote"]["signal_id"] = json!(signal(73, "sell"));
    b["action"]["quote"]["decimals_source"] = source(73);
    assert_unknown(
        &f.pair(
            "buy-from-sell-refused",
            &scenario(1_000_000_000, 1, vec![b]),
        ),
        "BUY quote requires a BUY metadata signal",
    );
}
