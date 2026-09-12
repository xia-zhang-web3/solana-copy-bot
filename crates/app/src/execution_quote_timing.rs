//! Timing starts only after a Request is built, immediately before client execution.
use crate::execution_quote_canary_helpers::QuoteSample;
use chrono::{DateTime, Utc};
use std::{fmt, time::Instant};

#[derive(Debug, Clone, Copy)]
pub(crate) struct QuoteHttpTiming {
    pub started_ts: DateTime<Utc>,
    pub elapsed_ms: u64,
}

pub(crate) struct QuoteAttemptClock {
    started_ts: DateTime<Utc>,
    started: Instant,
}
impl QuoteAttemptClock {
    pub fn start() -> Self {
        Self {
            started_ts: Utc::now(),
            started: Instant::now(),
        }
    }
    fn finish(self) -> QuoteHttpTiming {
        QuoteHttpTiming {
            started_ts: self.started_ts,
            elapsed_ms: self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        }
    }
}

#[derive(Debug)]
pub(crate) struct QuoteAttemptError {
    pub timing: Option<QuoteHttpTiming>,
    error: anyhow::Error,
}
impl fmt::Display for QuoteAttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}
impl std::error::Error for QuoteAttemptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}
impl std::ops::Deref for QuoteAttemptError {
    type Target = anyhow::Error;
    fn deref(&self) -> &Self::Target {
        &self.error
    }
}

pub(crate) type QuoteAttemptResult = Result<QuoteSample, QuoteAttemptError>;
pub(crate) fn complete_attempt(
    result: anyhow::Result<QuoteSample>,
    clock: Option<QuoteAttemptClock>,
) -> QuoteAttemptResult {
    let timing = clock.map(QuoteAttemptClock::finish);
    match result {
        Ok(mut quote) => {
            quote.http_request_started_ts = timing.map(|t| t.started_ts);
            quote.latency_ms = timing.map(|t| t.elapsed_ms).unwrap_or(0);
            Ok(quote)
        }
        Err(error) => Err(QuoteAttemptError { error, timing }),
    }
}

/// The full successful body has been decoded and required boundary checks passed.
/// This is local availability, independent of provider/chain clock and elapsed flooring.
pub(crate) fn response_available(mut quote: QuoteSample) -> QuoteSample {
    quote.quote_response_available_ts = Some(Utc::now());
    quote
}

pub(crate) fn apply_error_timing(
    event: &mut copybot_storage_core::ExecutionQuoteCanaryEventInsert,
    error: &QuoteAttemptError,
) {
    event.quote_response_available_ts = None;
    event.http_request_started_ts = error.timing.map(|t| t.started_ts);
    event.quote_latency_ms = error.timing.map(|t| t.elapsed_ms);
    event.decision_delay_ms = actual_delay(event.signal_ts, event.http_request_started_ts);
}
pub(crate) fn actual_delay(
    source: Option<DateTime<Utc>>,
    started: Option<DateTime<Utc>>,
) -> Option<u64> {
    let (source, started) = (source?, started?);
    if started < source {
        return None;
    }
    crate::execution_quote_canary_helpers::duration_ms_between(source, started)
}

pub(crate) fn apply_anyhow_error_timing(
    event: &mut copybot_storage_core::ExecutionQuoteCanaryEventInsert,
    error: &anyhow::Error,
) {
    if let Some(attempt) = error.downcast_ref::<QuoteAttemptError>() {
        apply_error_timing(event, attempt);
    }
}
