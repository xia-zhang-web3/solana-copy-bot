use chrono::{DateTime, Utc};
use std::sync::Mutex;

// Serialize wall-clock sampling, not database reads. A backwards wall adjustment
// must catch up to the previous decision before new BUY risk can be evaluated.
static LAST_SYSTEM_SAMPLE: Mutex<Option<DateTime<Utc>>> = Mutex::new(None);

pub(crate) fn decision_time(tick_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let (current, previous) = sample()?;
    // Equal time is insufficient for the half-open fee window when a prior order
    // in this tick already used tick_at. Do not manufacture a later timestamp.
    (current > tick_at && previous.is_none_or(|last| current >= last)).then_some(current)
}

fn sample() -> Option<(DateTime<Utc>, Option<DateTime<Utc>>)> {
    #[cfg(test)]
    if let Some(sample) = crate::app_tests::entry_risk_clock_fixture::sample() {
        return Some(sample);
    }
    let mut last = LAST_SYSTEM_SAMPLE.lock().ok()?;
    let current = Utc::now();
    let previous = *last;
    *last = Some(previous.map_or(current, |old| old.max(current)));
    Some((current, previous))
}
