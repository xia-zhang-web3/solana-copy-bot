use crate::fixture::Fixture;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

/// Thirty executed entries and one skipped entry, all with actual HTTP timing
/// and known zero fees. The full cohort clears the existing numeric gates.
pub fn ready_mixed(opened: DateTime<Utc>) -> Result<Fixture> {
    let mut f = Fixture::new()?;
    for n in 1..=31 {
        f.opened = opened + Duration::seconds(n);
        let mut buy = f.event(true, 0, "100", Some(0));
        let signal = format!("buy-{n}");
        buy.event_id = format!("quote:entry:{signal}");
        buy.signal_id = Some(signal.clone());
        buy.http_request_started_ts = Some(buy.request_ts);
        if n == 31 {
            buy.decision_status = Some("would_skip".into());
        }
        f.store.record_execution_quote_canary_event(&buy)?;
        f.store.record_execution_quote_canary_shadow_gate_event(
            &signal,
            "wallet",
            "Token",
            "buy",
            "shadow_recorded",
            None,
            f.opened + Duration::milliseconds(15),
        )?;
        f.exit(n, "100", Some(0))?;
    }
    f.sql("UPDATE execution_quote_canary_events SET http_request_started_ts=request_ts WHERE side='sell'")?;
    f.opened = opened;
    Ok(f)
}
