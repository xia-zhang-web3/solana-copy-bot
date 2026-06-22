use crate::exit_policy_shadow_quote_report_db::DiagnosticEvent;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct CountSummary {
    pub total_events: u64,
    pub ok_events: u64,
    pub error_events: u64,
    pub steady_events: u64,
    pub steady_ok_events: u64,
    pub steady_error_events: u64,
    pub future_close_matched_events: u64,
    pub open_or_unmatched_events: u64,
    pub multi_close_match_events: u64,
    pub possible_attribution_ambiguity_events: u64,
}

#[derive(Debug, Serialize)]
pub struct BenefitReport {
    pub event_count: u64,
    pub gross_benefit_sol: f64,
    pub estimated_exit_cost_sol: f64,
    pub net_benefit_after_exit_cost_sol: f64,
    pub avg_gross_benefit_sol: Option<f64>,
    pub median_gross_benefit_sol: Option<f64>,
    pub positive_events: u64,
    pub negative_events: u64,
    pub zero_events: u64,
}

#[derive(Debug, Serialize)]
pub struct BenefitSummary {
    pub bucket: String,
    #[serde(flatten)]
    pub benefit: BenefitReport,
}

#[derive(Debug, Serialize)]
pub struct ErrorClassCount {
    pub error_class: String,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct ConditionalSweepReport {
    pub max_quote_to_entry_ratio: f64,
    pub all: BenefitReport,
    pub steady: BenefitReport,
}

pub(crate) struct FullSummary {
    pub(crate) counts: CountSummary,
    pub(crate) benefit: BenefitReport,
    pub(crate) steady_benefit: BenefitReport,
    pub(crate) by_close_context: Vec<BenefitSummary>,
    pub(crate) by_comparability_bucket: Vec<BenefitSummary>,
    pub(crate) by_error_class: Vec<ErrorClassCount>,
    pub(crate) conditional_sweeps: Vec<ConditionalSweepReport>,
}

#[derive(Debug, Clone)]
struct EventBenefit {
    benefit_sol: f64,
    quote_to_entry_ratio: Option<f64>,
    close_context: String,
    comparability_bucket: String,
    steady: bool,
}

pub(crate) fn summarize_exit_policy_shadow_quotes(
    events: Vec<DiagnosticEvent>,
    thresholds: &[f64],
    exit_cost_sol: f64,
    steady_max_delay_ms: u64,
) -> FullSummary {
    let mut benefits = Vec::new();
    let mut error_classes = BTreeMap::<String, u64>::new();
    let mut counts = CountSummary {
        total_events: events.len() as u64,
        ok_events: 0,
        error_events: 0,
        steady_events: 0,
        steady_ok_events: 0,
        steady_error_events: 0,
        future_close_matched_events: 0,
        open_or_unmatched_events: 0,
        multi_close_match_events: 0,
        possible_attribution_ambiguity_events: 0,
    };

    for event in &events {
        let steady = event
            .decision_delay_ms
            .map(|delay| delay <= steady_max_delay_ms)
            .unwrap_or(false);
        if steady {
            counts.steady_events += 1;
        }
        if event.quote_status == "ok" {
            counts.ok_events += 1;
            if steady {
                counts.steady_ok_events += 1;
            }
        } else {
            counts.error_events += 1;
            if steady {
                counts.steady_error_events += 1;
            }
            *error_classes.entry(classify_error(event)).or_default() += 1;
            continue;
        }

        if event.close_matches.is_empty() {
            counts.open_or_unmatched_events += 1;
            continue;
        }
        counts.future_close_matched_events += 1;
        if event.close_matches.len() > 1 {
            counts.multi_close_match_events += 1;
            counts.possible_attribution_ambiguity_events += 1;
        }
        if let Some(benefit) = event_benefit(event, steady) {
            benefits.push(benefit);
        }
    }

    let steady_benefits = benefits
        .iter()
        .filter(|event| event.steady)
        .cloned()
        .collect::<Vec<_>>();
    let by_close_context = summarize_by_context(&benefits, exit_cost_sol);
    let by_comparability_bucket = summarize_by_comparability(&benefits, exit_cost_sol);
    let conditional_sweeps = thresholds
        .iter()
        .copied()
        .map(|threshold| conditional_sweep(threshold, &benefits, exit_cost_sol))
        .collect();

    FullSummary {
        counts,
        benefit: summarize_benefits(&benefits, exit_cost_sol),
        steady_benefit: summarize_benefits(&steady_benefits, exit_cost_sol),
        by_close_context,
        by_comparability_bucket,
        by_error_class: error_classes
            .into_iter()
            .map(|(error_class, count)| ErrorClassCount { error_class, count })
            .collect(),
        conditional_sweeps,
    }
}

fn event_benefit(event: &DiagnosticEvent, steady: bool) -> Option<EventBenefit> {
    let quote_price = event.quote_price_sol?;
    let shadow_price = event.shadow_price_sol?;
    if !quote_price.is_finite() || !shadow_price.is_finite() || quote_price < 0.0 {
        return None;
    }
    let future_qty = event
        .close_matches
        .iter()
        .map(|close| close.qty.max(0.0))
        .sum::<f64>();
    let future_exit_value_sol = event
        .close_matches
        .iter()
        .map(|close| close.exit_value_sol)
        .sum::<f64>();
    if future_qty <= 0.0 {
        return None;
    }
    let close_context = close_context_bucket(event);
    let comparability_bucket = comparability_bucket(&close_context);
    let quote_to_entry_ratio = (shadow_price > 0.0).then_some(quote_price / shadow_price);
    Some(EventBenefit {
        benefit_sol: quote_price * future_qty - future_exit_value_sol,
        quote_to_entry_ratio,
        close_context,
        comparability_bucket,
        steady,
    })
}

fn close_context_bucket(event: &DiagnosticEvent) -> String {
    let Some(first) = event.close_matches.first() else {
        return "unmatched".to_string();
    };
    if event
        .close_matches
        .iter()
        .all(|close| close.close_context == first.close_context)
    {
        return first.close_context.clone();
    }
    "mixed".to_string()
}

fn comparability_bucket(close_context: &str) -> String {
    match close_context {
        "stale_quote_price" => "executable_vs_executable",
        "stale_terminal_zero_price" | "recovery_terminal_zero_price" => "executable_vs_zero",
        "market" | "stale_market_price" | "quarantined_legacy" => "executable_vs_paper",
        "mixed" => "mixed",
        _ => "other",
    }
    .to_string()
}

fn conditional_sweep(
    threshold: f64,
    benefits: &[EventBenefit],
    exit_cost_sol: f64,
) -> ConditionalSweepReport {
    let firing = benefits
        .iter()
        .filter(|event| {
            event
                .quote_to_entry_ratio
                .map(|ratio| ratio <= threshold)
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();
    let steady = firing
        .iter()
        .filter(|event| event.steady)
        .cloned()
        .collect::<Vec<_>>();
    ConditionalSweepReport {
        max_quote_to_entry_ratio: threshold,
        all: summarize_benefits(&firing, exit_cost_sol),
        steady: summarize_benefits(&steady, exit_cost_sol),
    }
}

fn summarize_by_context(benefits: &[EventBenefit], exit_cost_sol: f64) -> Vec<BenefitSummary> {
    let mut buckets = BTreeMap::<String, Vec<EventBenefit>>::new();
    for event in benefits {
        buckets
            .entry(event.close_context.clone())
            .or_default()
            .push(event.clone());
    }
    buckets
        .into_iter()
        .map(|(bucket, values)| BenefitSummary {
            bucket,
            benefit: summarize_benefits(&values, exit_cost_sol),
        })
        .collect()
}

fn summarize_by_comparability(
    benefits: &[EventBenefit],
    exit_cost_sol: f64,
) -> Vec<BenefitSummary> {
    let mut buckets = BTreeMap::<String, Vec<EventBenefit>>::new();
    for event in benefits {
        buckets
            .entry(event.comparability_bucket.clone())
            .or_default()
            .push(event.clone());
    }
    buckets
        .into_iter()
        .map(|(bucket, values)| BenefitSummary {
            bucket,
            benefit: summarize_benefits(&values, exit_cost_sol),
        })
        .collect()
}

fn summarize_benefits(events: &[EventBenefit], exit_cost_sol: f64) -> BenefitReport {
    let gross = events.iter().map(|event| event.benefit_sol).sum::<f64>();
    let values = events
        .iter()
        .map(|event| event.benefit_sol)
        .collect::<Vec<_>>();
    let event_count = events.len() as u64;
    let estimated_exit_cost_sol = exit_cost_sol * event_count as f64;
    BenefitReport {
        event_count,
        gross_benefit_sol: gross,
        estimated_exit_cost_sol,
        net_benefit_after_exit_cost_sol: gross - estimated_exit_cost_sol,
        avg_gross_benefit_sol: (event_count > 0).then_some(gross / event_count as f64),
        median_gross_benefit_sol: median(values),
        positive_events: events
            .iter()
            .filter(|event| event.benefit_sol > 0.0)
            .count() as u64,
        negative_events: events
            .iter()
            .filter(|event| event.benefit_sol < 0.0)
            .count() as u64,
        zero_events: events
            .iter()
            .filter(|event| event.benefit_sol == 0.0)
            .count() as u64,
    }
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

fn classify_error(event: &DiagnosticEvent) -> String {
    let raw = event.error.as_deref().unwrap_or(&event.quote_status);
    let lower = raw.to_ascii_lowercase();
    if lower.contains("no route") || lower.contains("no_routes") {
        "no_route".to_string()
    } else if lower.contains("dust") || lower.contains("threshold") {
        "dust_or_threshold".to_string()
    } else if lower.contains("request failed") || lower.contains("timeout") {
        "request_failed".to_string()
    } else {
        "quote_error".to_string()
    }
}
