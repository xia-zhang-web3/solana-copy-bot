//! Local successful response availability is a causal lower bound, not freshness/TTL proof.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};

pub fn instant(unix_ms: u64) -> Result<DateTime<Utc>> {
    let ms = i64::try_from(unix_ms).context("declared time outside supported UTC range")?;
    DateTime::from_timestamp_millis(ms).context("declared time outside supported UTC range")
}

pub fn check(
    start: Option<DateTime<Utc>>,
    available: Option<DateTime<Utc>>,
    transition_unix_ms: u64,
) -> Result<Value> {
    let transition = instant(transition_unix_ms)?;
    let start = start.context("actual quote HTTP start missing; time binding unknown")?;
    // Compare instants directly: timestamp_millis() would erase a future +1ns.
    ensure!(
        start <= transition,
        "quote HTTP start after declared transition"
    );
    let available =
        available.context("actual quote response availability missing; time binding unknown")?;
    ensure!(
        start <= available,
        "quote response availability before HTTP start"
    );
    ensure!(
        available <= transition,
        "quote response availability after declared transition"
    );
    Ok(json!({
        "state":"response_available_not_after_transition",
        "http_request_started_ts":start.to_rfc3339_opts(SecondsFormat::Nanos, false),
        "declared_transition_unix_ms":transition_unix_ms.to_string(),
        "response_availability":{"quote_response_available_ts":available.to_rfc3339_opts(SecondsFormat::Nanos, false), "provenance":"local successful body/decode completion; freshness/TTL/coverage unknown"}
    }))
}
