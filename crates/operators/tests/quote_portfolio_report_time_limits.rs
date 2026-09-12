mod quote_portfolio_report_time_support;
use quote_portfolio_report_time_support::*;
use serde_json::{json, Value};
use std::fs;

#[test]
fn invalid_or_out_of_range_event_and_window_times_are_explicit_errors() {
    let f = fixture("time-invalid-boundaries", "small", Some(TS));
    let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_millis() as u64;
    for field in ["start_unix_ms", "end_unix_ms", "unix_ms"] {
        for (n, value) in [
            "-1".to_owned(),
            "not-time".to_owned(),
            "1.1".to_owned(),
            "18446744073709551616".to_owned(),
            u64::MAX.to_string(),
            i64::MAX.to_string(),
            (max + 1).to_string(),
        ]
        .iter()
        .enumerate()
        {
            let mut input = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100)]);
            if field == "unix_ms" {
                input["events"][0][field] = json!(value);
            } else {
                input["window"][field] = json!(value);
            }
            let v = f.pair(&format!("{field}-{n}"), Some(&input));
            assert_eq!(v["status"], "unavailable", "{v}");
            assert!(v["reason"].as_str().is_some_and(|s| !s.is_empty()));
        }
    }
    // The last representable millisecond is supported; no timestamp narrowing.
    let mut input = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100)]);
    for key in ["start_unix_ms", "end_unix_ms"] {
        input["window"][key] = json!(max.to_string());
    }
    input["events"][0]["unix_ms"] = json!(max.to_string());
    let v = f.pair("largest-supported-ms", Some(&input));
    verified(&v, 0, &started(MS, 0));
    assert_eq!(v["book"]["cash_lamports"], "0");
}

fn reader_pair(f: &Fixture, name: &str, expected_code: i32) -> Value {
    let input = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100)]);
    let path = f.dir.join(format!("{name}.input.json"));
    fs::write(&path, serde_json::to_vec_pretty(&input).unwrap()).unwrap();
    let before = fs::read(&f.db).unwrap();
    let original_input = fs::read(&path).unwrap();
    let value = copybot_operators::quote_portfolio_report::build(Some(&path), Some(&f.db));
    for bin in BINS {
        let (v, code) = run(&binary(bin), &f.db, Some(&path));
        fs::write(
            f.dir.join(format!("{name}.{bin}.json")),
            serde_json::to_vec_pretty(&v).unwrap(),
        )
        .unwrap();
        fs::write(f.dir.join(format!("{name}.{bin}.exit")), code.to_string()).unwrap();
        assert_eq!(code, expected_code);
        assert_eq!(v["portfolio_replay"], value);
    }
    assert_eq!(before, fs::read(&f.db).unwrap());
    assert_eq!(original_input, fs::read(&path).unwrap());
    integrity(&f.db);
    value
}

#[test]
fn malformed_actual_http_time_is_unknown_with_read_only_db_preservation() {
    for (n, start) in [
        "invalid+00:00",
        "2026-02-30T12:00:00+00:00",
        "+999999-01-01T00:00:00+00:00",
        "2026-06-02T12:00:00Z",
    ]
    .iter()
    .enumerate()
    {
        let f = fixture(&format!("time-invalid-http-{n}"), "small", Some(start));
        let v = reader_pair(&f, "input", 0);
        refused(&v, 0, "http_request_started_ts");
        assert_eq!(v["book"]["cash_lamports"], "100");
    }
}

#[test]
fn legacy_missing_timing_column_stays_unknown_without_synthetic_fallback() {
    let f = fixture("time-pre0066", "small", Some(TS));
    writable(&f.db, true);
    let c = rusqlite::Connection::open(&f.db).unwrap();
    c.execute_batch(
        "DELETE FROM schema_migrations WHERE version='0066_quote_http_timing.sql';
        ALTER TABLE execution_quote_canary_events DROP COLUMN http_request_started_ts;
        ALTER TABLE execution_quote_canary_provider_samples DROP COLUMN http_request_started_ts;
        ALTER TABLE execution_canary_build_plan_metadata DROP COLUMN http_request_started_ts;",
    )
    .unwrap();
    drop(c);
    writable(&f.db, false);
    fs::copy(&f.db, f.dir.join("schema.before.sqlite")).unwrap();
    let v = reader_pair(&f, "input", 0);
    refused(&v, 0, "actual quote HTTP start missing");
    assert_eq!(v["book"]["cash_lamports"], "100");
}

#[test]
fn nonfuture_start_has_no_ttl_or_completion_claim_even_with_long_latency() {
    for (name, start) in [
        ("old", "2000-01-01T00:00:00+00:00".to_owned()),
        ("boundary", started(MS + 1, 0)),
    ] {
        let f = fixture(&format!("time-no-ttl-{name}"), "small", Some(&start));
        writable(&f.db, true);
        let c = rusqlite::Connection::open(&f.db).unwrap();
        c.execute("UPDATE execution_quote_canary_events SET quote_latency_ms=10000 WHERE event_id='small'", []).unwrap();
        drop(c);
        writable(&f.db, false);
        fs::copy(&f.db, f.dir.join("latency.before.sqlite")).unwrap();
        let input = scenario(100, 1, vec![buy("small", "A", 1, 1, 100, 100)]);
        let v = f.pair("input", Some(&input));
        let expected = chrono::DateTime::parse_from_rfc3339(&start)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, false);
        verified(&v, 0, &expected);
        assert!(v["limitations"].as_array().unwrap().iter().any(|v| v
            .as_str()
            .unwrap()
            .contains("response availability and freshness unproved")));
    }
}

#[test]
fn time_refusal_retains_assumed_provenance_and_replay_without_stopping_b() {
    let f = fixture("time-assumed-replay", "small", Some(&started(MS + 1, 1)));
    let mut a = buy("small", "A", 1, 1, 100, 100);
    observed(&mut a);
    a["action"]["quote"]["provenance"] = json!({"assumed":"explicit HTTP timing scenario"});
    let mut b = buy("next", "B", 2, 2, 1, 1);
    observed(&mut b);
    let mut input = scenario(100, 2, vec![]);
    observed(&mut input);
    input["events"] = json!([a.clone(), b, a]);
    let v = f.pair("input", Some(&input));
    refused(&v, 0, "quote HTTP start after declared transition");
    assert_eq!(v["events"][0]["outcome"], v["events"][2]["outcome"]);
    assert_eq!(v["book"]["cash_lamports"], "99");
    assert_eq!(v["book"]["open_slots"], "1");
    assert_eq!(v["assumed_or_synthetic"], true);
    assert_eq!(v["valuation"]["basis"], "assumed_or_synthetic_operands");
}
