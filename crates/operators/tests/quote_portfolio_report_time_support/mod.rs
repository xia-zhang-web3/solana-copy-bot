#![allow(dead_code)]
#[path = "../quote_portfolio_report_support/mod.rs"]
mod prior;
pub use prior::*;
use serde_json::{json, Value};
use std::{fs, path::Path};

// Explicit synthetic start/availability operands (equal instants), never HTTP evidence.
// Keep prior source.before.sqlite and save the fully prepared DB separately.
pub fn fixture(name: &str, target: &str, start: Option<&str>) -> Fixture {
    let f = Fixture::new(name);
    writable(&f.db, true);
    let c = rusqlite::Connection::open(&f.db).unwrap();
    c.execute(
        "UPDATE execution_quote_canary_events SET http_request_started_ts=?1, quote_response_available_ts=?1",
        [TS],
    )
    .unwrap();
    c.execute(
        "UPDATE execution_quote_canary_events SET http_request_started_ts=?1, quote_response_available_ts=?1 WHERE event_id=?2",
        rusqlite::params![start, target],
    )
    .unwrap();
    drop(c);
    writable(&f.db, false);
    fs::copy(&f.db, f.dir.join("prepared.before.sqlite")).unwrap();
    f
}
pub fn writable(p: &Path, yes: bool) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            p,
            fs::Permissions::from_mode(if yes { 0o644 } else { 0o444 }),
        )
        .unwrap();
    }
}
pub fn started(ms: u64, ns: i64) -> String {
    (chrono::DateTime::from_timestamp_millis(i64::try_from(ms).unwrap()).unwrap()
        + chrono::Duration::nanoseconds(ns))
    .to_rfc3339_opts(chrono::SecondsFormat::Nanos, false)
}
pub fn charged_buy() -> Value {
    let mut e = buy("small", "A", 1, 1, 100, 100);
    e["action"]["costs"]["base"] = amount(2);
    e["action"]["costs"]["priority"] = amount(3);
    e["action"]["costs"]["setup"] = amount(4);
    e["action"]["rent_deposit"] = amount(5);
    e
}
pub fn mark(id: &str, seq: u64, row: &str, raw: u64, output: u64) -> Value {
    event(
        id,
        "A",
        seq,
        json!({"kind":"mark",
        "quote":reference(row,"A",1,"sell",raw,output),"costs":costs(id,"A","sell")}),
    )
}
pub fn case(kind: &str) -> (Value, &'static str, usize, u64) {
    let mut events = vec![charged_buy()];
    let (target, index, seq) = match kind {
        "buy" => ("small", 0, 1),
        "sell" => {
            let mut s = sell("refund-sell", "A", 2, 100, 110);
            s["action"]["costs"]["base"] = amount(2);
            s["action"]["costs"]["priority"] = amount(3);
            s["action"]["costs"]["exit"] = amount(4);
            events.push(s);
            ("refund-sell", 1, 2)
        }
        "mark" => {
            events.push(mark("mark", 2, "refund-sell", 100, 110));
            ("refund-sell", 1, 2)
        }
        _ => panic!("unknown fixture kind"),
    };
    events.push(buy("next", "B", 5, 2, 1, 1));
    (scenario(1000, 2, events), target, index, MS + seq)
}
pub fn refused(v: &Value, index: usize, reason: &str) {
    let e = &v["events"][index];
    assert_eq!(e["source_binding"]["state"], "unavailable", "{v}");
    assert!(
        e["source_binding"]["reason"]
            .as_str()
            .unwrap()
            .contains(reason),
        "{v}"
    );
    assert_eq!(e["outcome"]["disposition"]["state"], "refused");
    assert_eq!(
        e["outcome"]["disposition"]["refusal"]["code"],
        "missing_operand"
    );
    assert_eq!(e["outcome"]["before"], e["outcome"]["after"]);
    assert_eq!(
        e["outcome"]["allocated_this_event"],
        json!({"principal":"0","base":"0","priority":"0","setup":"0","exit":"0"})
    );
    assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
    assert_eq!(v["valuation"]["unresolved"].as_array().unwrap().len(), 1);
    assert_eq!(
        v["valuation"]["unresolved"][0]["event_id"],
        e["input"]["id"]
    );
}
pub fn verified(v: &Value, index: usize, expected_start: &str) {
    let e = &v["events"][index];
    assert_eq!(e["source_binding"]["state"], "verified_db_fields");
    assert_eq!(e["outcome"]["disposition"]["state"], "applied");
    let t = &e["source_binding"]["time_binding"];
    assert_eq!(t["state"], "response_available_not_after_transition");
    assert_eq!(t["http_request_started_ts"], expected_start);
    assert_eq!(t["declared_transition_unix_ms"], e["input"]["unix_ms"]);
    assert!(t["response_availability"]["quote_response_available_ts"].is_string());
}
