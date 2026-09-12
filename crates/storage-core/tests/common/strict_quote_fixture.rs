#![allow(dead_code)]
#[path = "b96_ordered.rs"]
mod ordered;
use anyhow::Result;
use chrono::{DateTime, Utc};
pub use copybot_storage_core::ordered_sell_quote::*;
pub use ordered::*;
pub const ENDPOINT: &str = "http://127.0.0.1:1/";
pub const ID: &str = "source-sell:sell";
pub fn fixture() -> Result<F> {
    let mut f = within()?;
    inserted(&mut f)?;
    Ok(f)
}
pub fn claim(f: &F, now: DateTime<Utc>) -> Result<QuoteClaim> {
    let step =
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, || now)?;
    let QuoteClaimStep::Claimed(c) = step else {
        panic!("{step:?}");
    };
    Ok(c)
}
pub fn observation(c: &QuoteClaim, now: DateTime<Utc>) -> QuoteObservation {
    QuoteObservation {
        version: 1,
        binding: Some(c.binding.clone()),
        outcome: QuoteOutcome::Current,
        reason: None,
        http_started: Some(now),
        http_response: Some(now),
        quote_response_available_ts: None,
        http_ended: now,
        response_in_raw: Some(c.binding.raw.to_string()),
        response_out_raw: Some("1000".into()),
        response_sha256: Some("a".repeat(64)),
        event_time: None,
        event_delay_ns: None,
    }
}
