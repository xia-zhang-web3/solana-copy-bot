use crate::track_b_entry_quote_report_types::NumericStats;

pub(crate) fn numeric_stats(mut values: Vec<f64>) -> NumericStats {
    values.retain(|value| value.is_finite());
    values.sort_by(|left, right| left.total_cmp(right));
    let count = values.len() as u64;
    if values.is_empty() {
        return NumericStats::default();
    }
    let avg = values.iter().sum::<f64>() / values.len() as f64;
    NumericStats {
        count,
        avg: Some(avg),
        p50: percentile(&values, 0.50),
        p90: percentile(&values, 0.90),
        p95: percentile(&values, 0.95),
        max: values.last().copied(),
    }
}

fn percentile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let pos = ((values.len() - 1) as f64 * quantile).round() as usize;
    values.get(pos).copied()
}
