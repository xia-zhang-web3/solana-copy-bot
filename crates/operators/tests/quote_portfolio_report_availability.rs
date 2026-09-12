mod quote_portfolio_report_time_support;
use quote_portfolio_report_time_support::*;
use serde_json::Value;

fn set_available(f: &Fixture, target: &str, value: Option<&str>) {
    writable(&f.db, true);
    let c = rusqlite::Connection::open(&f.db).unwrap();
    c.execute(
        "UPDATE execution_quote_canary_events SET quote_response_available_ts=?1 WHERE event_id=?2",
        rusqlite::params![value, target],
    )
    .unwrap();
    drop(c);
    writable(&f.db, false);
}
#[test]
fn both_clis_buy_sell_mark_require_full_available_instant_before_kernel() {
    for kind in ["buy", "sell", "mark"] {
        let (input, row, index, boundary) = case(kind);
        for (label, value, reason) in [
            ("equal", Some(started(boundary, 0)), None),
            (
                "one-ns-future",
                Some(started(boundary, 1)),
                Some("quote response availability after declared transition"),
            ),
            (
                "before-start",
                Some(started(MS, -1)),
                Some("quote response availability before HTTP start"),
            ),
            ("missing", None, Some("quote response availability missing")),
            (
                "invalid",
                Some("not-a-time".into()),
                Some("quote_response_available_ts"),
            ),
        ] {
            let f = fixture(&format!("available-{kind}-{label}"), row, Some(TS));
            set_available(&f, row, value.as_deref());
            let v = f.pair("availability", Some(&input));
            if let Some(reason) = reason {
                refused(&v, index, reason);
            } else {
                assert_eq!(
                    v["events"][index]["outcome"]["disposition"]["state"],
                    "applied"
                );
                let binding = &v["events"][index]["source_binding"]["time_binding"];
                assert_eq!(
                    binding["response_availability"]["quote_response_available_ts"],
                    value.unwrap()
                );
                assert_eq!(binding["state"], "response_available_not_after_transition");
            }
            assert_eq!(
                v["events"].as_array().unwrap().last().unwrap()["outcome"]["disposition"]["state"],
                "applied"
            );
            assert_eq!(v["production_green"], false);
            assert!(v["dataset_coverage"]["unknown"].is_string());
        }
    }
}
#[test]
fn both_clis_legacy_availability_never_falls_back_to_start_latency_or_migration() {
    let f = fixture("available-legacy", "small", Some(TS));
    writable(&f.db, true);
    let c = rusqlite::Connection::open(&f.db).unwrap();
    c.execute_batch(
        "DELETE FROM schema_migrations WHERE version='0076_quote_response_availability.sql';
        ALTER TABLE execution_quote_canary_events DROP COLUMN quote_response_available_ts;
        ALTER TABLE execution_quote_canary_provider_samples DROP COLUMN quote_response_available_ts;
        ALTER TABLE execution_canary_build_plan_metadata DROP COLUMN quote_response_available_ts;
        UPDATE execution_quote_canary_events SET quote_latency_ms=0;",
    )
    .unwrap();
    drop(c);
    writable(&f.db, false);
    let (input, _, _, _) = case("buy");
    let v = f.pair("legacy", Some(&input));
    for event in v["events"].as_array().unwrap() {
        assert_eq!(event["outcome"]["disposition"]["state"], "refused");
        assert!(event["source_binding"]["reason"]
            .as_str()
            .unwrap()
            .contains("quote response availability missing"));
        assert_eq!(event["outcome"]["before"], event["outcome"]["after"]);
    }
    assert_eq!(v["book"]["cash_lamports"], "1000");
    assert_eq!(v["book"]["positions"], serde_json::json!({}));
    assert_eq!(v["production_green"], Value::Bool(false));
}
