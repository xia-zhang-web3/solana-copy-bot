mod quote_portfolio_report_support;
use copybot_operators::quote_portfolio_report;
use quote_portfolio_report_support::*;
use serde_json::json;
use std::fs;

#[test]
fn bounded_invalid_inputs_fail_explicitly_without_truncation() {
    let f = Fixture::new("limits");
    for case in [
        "version",
        "events",
        "window",
        "outside",
        "numeric",
        "overflow",
        "initial",
        "unknown_initial",
        "provenance",
    ] {
        let mut i = scenario(100, 1, vec![]);
        match case {
            "version" => i["version"] = json!(2),
            "events" => i["events"] = json!(vec![buy("small", "A", 1, 1, 100, 100); 257]),
            "window" => i["window"]["start_unix_ms"] = json!((MS + 2000).to_string()),
            "outside" => {
                i["events"] = json!([buy("small", "A", 1, 1, 100, 100)]);
                i["events"][0]["unix_ms"] = json!("0");
            }
            "numeric" => i["initial"]["cash_lamports"]["value"]["known"] = json!(100),
            "overflow" => {
                i["initial"]["cash_lamports"]["value"]["known"] = json!("18446744073709551616")
            }
            "initial" => i["initial"]["inventory_raw"] = amount(1),
            "unknown_initial" => {
                i["initial"]["cash_lamports"]["value"] = json!({"unknown":"not observed"})
            }
            "provenance" => i["scenario_provenance"] = json!({"assumed":""}),
            _ => unreachable!(),
        }
        let v = f.pair(case, Some(&i));
        assert_eq!(v["status"], "unavailable", "{case}: {v}");
    }
    for (name, bytes) in [
        ("bytes", vec![b' '; 1_048_577]),
        ("duplicate", b"{\"version\":1,\"version\":1}".to_vec()),
    ] {
        let p = f.dir.join(format!("{name}.input.json"));
        fs::write(&p, bytes).unwrap();
        assert_eq!(f.pair_path(name, Some(&p))["status"], "unavailable");
    }
}

#[test]
fn read_only_schema_errors_remain_unavailable_without_creating_tables() {
    let f = Fixture::new("schema");
    let db = f.dir.join("empty.sqlite");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch("CREATE TABLE sentinel (id INTEGER);")
        .unwrap();
    drop(c);
    let i = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100)]);
    let p = f.dir.join("schema.input.json");
    fs::write(&p, i.to_string()).unwrap();
    let before = fs::read(&db).unwrap();
    fs::write(f.dir.join("empty.before.sqlite"), &before).unwrap();
    fs::copy(&p, f.dir.join("schema.input.before")).unwrap();
    let value = quote_portfolio_report::build(Some(&p), Some(&db));
    assert_eq!(value["events"][0]["source_binding"]["state"], "unavailable");
    assert!(value["valuation"]["full_equity_lamports"]["unknown"].is_string());
    for bin in BINS {
        let (v, code) = run(&binary(bin), &db, Some(&p));
        assert_eq!(code, 1);
        assert_eq!(v["portfolio_replay"], value);
        fs::write(
            f.dir.join(format!("schema.{bin}.json")),
            serde_json::to_vec_pretty(&v).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(before, fs::read(&db).unwrap());
    integrity(&db);
    let c = rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    let missing = f.dir.join("missing.sqlite");
    assert_eq!(
        quote_portfolio_report::build(Some(&p), Some(&missing))["status"],
        "unavailable"
    );
    assert!(!missing.exists());
}

#[test]
fn exact_large_integers_refunds_and_unsupported_expenses_are_visible() {
    let f = Fixture::new("lifecycle");
    let large = 9_007_199_254_740_993;
    let v = f.pair(
        "large",
        Some(&scenario(
            large + 7,
            1,
            vec![buy("large", "A", 1, 1, large, 100)],
        )),
    );
    assert_eq!(v["book"]["cash_lamports"], "7");
    assert_eq!(v["book"]["flows"]["buy_principal"], large.to_string());
    let mut a = buy("small", "A", 1, 1, 100, 100);
    a["action"]["rent_deposit"] = amount(2);
    let refund = event(
        "refund",
        "A",
        3,
        json!({"kind":"rent_refund","deposit_event_id":"small","amount":amount(2)}),
    );
    let i = scenario(
        200,
        1,
        vec![a, sell("refund-sell", "A", 2, 100, 110), refund],
    );
    let v = f.pair("refund", Some(&i));
    assert_eq!(v["book"]["cash_lamports"], "210");
    assert_eq!(v["valuation"]["net_change_lamports"]["known"], "10");
    let expense = event(
        "expense",
        "A",
        1,
        json!({"kind":"unsupported_expense","description":"failed fee","amount":amount(1)}),
    );
    let v = f.pair("unsupported", Some(&scenario(100, 1, vec![expense])));
    assert_eq!(
        v["events"][0]["outcome"]["disposition"]["refusal"]["code"],
        "unsupported_expense"
    );
    assert_eq!(v["book"]["cash_lamports"], "100");
    assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
}

#[test]
fn exact_limits_preserve_all_caller_events_and_cli_flag_requires_a_value() {
    let f = Fixture::new("exact-limits");
    let i = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100); 256]);
    let v = f.pair("events-256", Some(&i));
    assert_eq!(v["events"].as_array().unwrap().len(), 256);
    assert_eq!(v["book"]["cash_lamports"], "0");
    let mut bytes = scenario(100, 1, vec![]).to_string().into_bytes();
    bytes.resize(1_048_576, b' ');
    let p = f.dir.join("bytes-limit.input.json");
    fs::write(&p, bytes).unwrap();
    assert_eq!(f.pair_path("bytes-limit", Some(&p))["status"], "replayed");
    for args in [
        vec!["--db-path", "fixture", "--json", "--portfolio-input"],
        vec![
            "--db-path",
            "fixture",
            "--json",
            "--portfolio-input",
            "input.json",
        ],
    ] {
        let a = copybot_operators::execution_canary_quote_pnl::parse_args_from(args.clone());
        let b = copybot_operators::execution_tiny_economics::parse_args_from(args.clone());
        assert_eq!(a.is_ok(), args.len() == 5);
        assert_eq!(b.is_ok(), args.len() == 5);
        if let (Ok(a), Ok(b)) = (a, b) {
            assert_eq!(a.portfolio_input, b.portfolio_input);
        }
    }
}
