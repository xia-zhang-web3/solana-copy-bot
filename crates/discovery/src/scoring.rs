pub(super) fn tanh01(value: f64) -> f64 {
    ((value.tanh() + 1.0) * 0.5).clamp(0.0, 1.0)
}

pub(super) fn hold_time_quality_score(median_seconds: i64) -> f64 {
    if median_seconds <= 0 {
        0.0
    } else if median_seconds < 45 {
        0.2
    } else if median_seconds < 120 {
        0.5
    } else if median_seconds <= 6 * 60 * 60 {
        1.0
    } else if median_seconds <= 24 * 60 * 60 {
        0.75
    } else {
        0.4
    }
}

pub(super) fn median_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some((sorted[mid - 1] + sorted[mid]) / 2)
    }
}
