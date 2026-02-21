use anyhow::{anyhow, Result};
use reqwest::header::RETRY_AFTER;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::time;

use super::{IngestionTelemetry, SeenSignatureEntry};

pub(super) fn is_seen_signature(
    seen_signatures_map: &HashMap<String, Instant>,
    signature: &str,
    ttl: Duration,
    now: Instant,
) -> bool {
    seen_signatures_map
        .get(signature)
        .map(|seen_at| now.duration_since(*seen_at) < ttl)
        .unwrap_or(false)
}

pub(super) fn normalize_program_ids_or_fallback(
    mut program_ids: HashSet<String>,
    interested_program_ids: &HashSet<String>,
    telemetry: &IngestionTelemetry,
    missing_program_ids_error: &str,
) -> Result<Option<HashSet<String>>> {
    if program_ids.is_empty() {
        if interested_program_ids.is_empty() {
            return Err(anyhow!("{}", missing_program_ids_error));
        }
        telemetry.note_parse_fallback("missing_program_ids_fallback");
        program_ids.extend(interested_program_ids.iter().cloned());
        return Ok(Some(program_ids));
    }
    if !program_ids
        .iter()
        .any(|program| interested_program_ids.contains(program))
    {
        return Ok(None);
    }
    Ok(Some(program_ids))
}

pub(super) fn mark_seen_signature(
    seen_signatures_map: &mut HashMap<String, Instant>,
    seen_signatures_queue: &mut VecDeque<SeenSignatureEntry>,
    seen_signatures_limit: usize,
    seen_signatures_ttl: Duration,
    signature: String,
    now: Instant,
) {
    seen_signatures_map.insert(signature.clone(), now);
    seen_signatures_queue.push_back(SeenSignatureEntry {
        signature,
        seen_at: now,
    });
    prune_seen_signatures(
        seen_signatures_map,
        seen_signatures_queue,
        seen_signatures_limit,
        seen_signatures_ttl,
        now,
    );
}

pub(super) fn prune_seen_signatures(
    seen_signatures_map: &mut HashMap<String, Instant>,
    seen_signatures_queue: &mut VecDeque<SeenSignatureEntry>,
    seen_signatures_limit: usize,
    seen_signatures_ttl: Duration,
    now: Instant,
) {
    while let Some(front) = seen_signatures_queue.front() {
        let expired = now.duration_since(front.seen_at) >= seen_signatures_ttl;
        let over_capacity = seen_signatures_queue.len() > seen_signatures_limit;
        if !expired && !over_capacity {
            break;
        }
        if let Some(removed) = seen_signatures_queue.pop_front() {
            if seen_signatures_map
                .get(&removed.signature)
                .is_some_and(|seen_at| *seen_at == removed.seen_at)
            {
                seen_signatures_map.remove(&removed.signature);
            }
        }
    }
}

pub(super) fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

pub(super) fn compute_retry_delay(
    base_ms: u64,
    max_ms: u64,
    jitter_ms: u64,
    attempt: u32,
    signature: &str,
    retry_after: Option<Duration>,
) -> Duration {
    let base_ms = base_ms.max(1);
    let mut cap_ms = max_ms.max(base_ms);
    if let Some(retry_after) = retry_after {
        let retry_after_ms = retry_after.as_millis().min(u128::from(u64::MAX)) as u64;
        cap_ms = cap_ms.max(retry_after_ms);
    }
    let exp_factor = 1u64 << attempt.min(10);
    let mut delay_ms = base_ms.saturating_mul(exp_factor).min(cap_ms);
    if let Some(retry_after) = retry_after {
        let retry_after_ms = retry_after.as_millis().min(u128::from(u64::MAX)) as u64;
        delay_ms = delay_ms.max(retry_after_ms.min(cap_ms));
    }
    let jitter = retry_jitter_ms(signature, attempt, jitter_ms);
    Duration::from_millis(
        delay_ms
            .saturating_add(jitter)
            .min(cap_ms.saturating_add(jitter_ms)),
    )
}

pub(super) fn retry_jitter_ms(signature: &str, attempt: u32, max_jitter_ms: u64) -> u64 {
    if max_jitter_ms == 0 {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    signature.hash(&mut hasher);
    attempt.hash(&mut hasher);
    hasher.finish() % (max_jitter_ms + 1)
}

pub(super) fn effective_per_endpoint_rps_limit(
    configured: u64,
    global: u64,
    endpoint_count: usize,
) -> u64 {
    if endpoint_count == 1 && global > 0 {
        if configured == 0 || configured < global {
            return global;
        }
    }
    configured
}

pub(super) async fn sleep_with_backoff(next_backoff_ms: &mut u64, initial_ms: u64, max_ms: u64) {
    let delay = (*next_backoff_ms).clamp(initial_ms, max_ms);
    time::sleep(Duration::from_millis(delay)).await;
    *next_backoff_ms = delay.saturating_mul(2).min(max_ms);
}

pub(super) fn push_sample(samples: &mut VecDeque<u64>, value: u64, cap: usize) {
    if samples.len() >= cap {
        let _ = samples.pop_front();
    }
    samples.push_back(value);
}

pub(super) fn percentile(values: &[u64], q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

pub(super) fn increment_atomic_usize(counter: &AtomicUsize) {
    counter.fetch_add(1, Ordering::Relaxed);
}

pub(super) fn decrement_atomic_usize(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}
