use crate::track_b_entry_quote_report_caveats::track_b_caveats;
use crate::track_b_entry_quote_report_db::{CloseOutcome, EntryQuoteOutcome};
use crate::track_b_entry_quote_report_executable::fully_executable_pnl;
use crate::track_b_entry_quote_report_segments::{
    summarize_hold_time_buckets, summarize_rank_cohorts, summarize_source_cohorts, HoldTimeBucket,
    RankCohort, SourceCohort,
};
use crate::track_b_entry_quote_report_stats::numeric_stats;
use crate::track_b_entry_quote_report_sweep::{sweep_price_impact, sweep_ratio};
use crate::track_b_entry_quote_report_types::{
    BucketSummary, SummaryCounts, TrackBEntryQuoteSummary,
};

const QUOTE_OK: &str = "ok";
const RATIO_MIN: f64 = 0.1;
const RATIO_MAX: f64 = 10.0;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseBucket {
    Open,
    Market,
    StaleQuote,
    StaleMarket,
    Terminal,
    Mixed,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitExecutability {
    FullyExecutable,
    HybridPaperExit,
    MixedAmbiguous,
}

#[derive(Debug, Clone)]
pub(crate) struct CleanEvent {
    pub(crate) cohort: RankCohort,
    pub(crate) source_cohort: SourceCohort,
    pub(crate) bucket: CloseBucket,
    exit_executability: ExitExecutability,
    pub(crate) hold_time_bucket: HoldTimeBucket,
    pub(crate) shadow_pnl_sol: f64,
    pub(crate) entry_adjusted_pnl_sol: f64,
    pub(crate) fully_executable_pnl_sol: Option<f64>,
    pub(crate) ratio: f64,
    pub(crate) price_impact_pct: Option<f64>,
    market_exit_quote_events: u64,
    market_exit_error_events: u64,
    market_exit_dead_error_events: u64,
    market_exit_transient_error_events: u64,
    market_exit_missing_quote_events: u64,
    market_exit_ratio_outlier_events: u64,
    market_exit_zero_exit_events: u64,
    market_exit_quote_shadow_ratios: Vec<f64>,
    market_exit_decision_delay_ms: Vec<f64>,
}

pub(crate) fn summarize_track_b(
    outcomes: Vec<EntryQuoteOutcome>,
    close_match_limit: u32,
    max_market_exit_delay_ms: Option<i64>,
) -> TrackBEntryQuoteSummary {
    let mut counts = SummaryCounts {
        total_events: outcomes.len() as u64,
        first_event_ts: outcomes
            .first()
            .map(|row| row.event.request_ts.to_rfc3339()),
        last_event_ts: outcomes.last().map(|row| row.event.request_ts.to_rfc3339()),
        ..SummaryCounts::default()
    };
    let mut clean = Vec::new();
    let mut ratios = Vec::new();
    let mut impacts = Vec::new();
    let mut market_exit_ratios = Vec::new();
    let mut market_exit_delays = Vec::new();
    for outcome in &outcomes {
        if outcome.event.quote_status == QUOTE_OK {
            counts.ok_events += 1;
        } else {
            counts.error_events += 1;
        }
        if outcome.closes.is_empty() {
            counts.open_or_unmatched_events += 1;
        } else {
            counts.closed_events += 1;
        }
        if outcome.closes.len() > 1 {
            counts.multi_close_match_events += 1;
        }
        if outcome.closes.len() >= close_match_limit as usize {
            counts.truncated_at_close_match_limit_events += 1;
        }
        if outcome.event.quote_status == QUOTE_OK && outcome.event.quote_price_sol.is_none() {
            counts.ok_null_quote_price_events += 1;
        }
        let Some(event) = clean_event(outcome, max_market_exit_delay_ms) else {
            if is_contaminated(outcome).unwrap_or(false) {
                counts.contaminated_ratio_events += 1;
            }
            continue;
        };
        if event.bucket == CloseBucket::Mixed {
            counts.mixed_close_context_events += 1;
        }
        counts.market_exit_quote_events += event.market_exit_quote_events;
        counts.market_exit_error_events += event.market_exit_error_events;
        counts.market_exit_dead_error_events += event.market_exit_dead_error_events;
        counts.market_exit_transient_error_events += event.market_exit_transient_error_events;
        counts.market_exit_missing_quote_events += event.market_exit_missing_quote_events;
        counts.market_exit_ratio_outlier_events += event.market_exit_ratio_outlier_events;
        counts.market_exit_zero_exit_events += event.market_exit_zero_exit_events;
        ratios.push(event.ratio);
        market_exit_ratios.extend(event.market_exit_quote_shadow_ratios.iter().copied());
        market_exit_delays.extend(event.market_exit_decision_delay_ms.iter().copied());
        if let Some(impact) = event.price_impact_pct.filter(|value| value.is_finite()) {
            impacts.push(impact);
        }
        clean.push(event);
    }
    counts.clean_closed_usable_events = clean.len() as u64;
    TrackBEntryQuoteSummary {
        metric_basis: "entry_quote_with_exit_executability_split".to_string(),
        caveats: track_b_caveats(),
        counts,
        price_ratio_stats: numeric_stats(ratios),
        price_impact_stats: numeric_stats(impacts),
        market_exit_quote_ratio_stats: numeric_stats(market_exit_ratios),
        market_exit_decision_delay_ms_stats: numeric_stats(market_exit_delays),
        by_close_bucket: summarize_close_buckets(&clean),
        by_exit_executability: summarize_exit_executability(&clean),
        by_hold_time_bucket: summarize_hold_time_buckets(&clean),
        by_rank_cohort: summarize_rank_cohorts(&clean),
        by_source_cohort: summarize_source_cohorts(&clean),
        price_impact_sweep: sweep_price_impact(&clean),
        quote_shadow_ratio_sweep: sweep_ratio(&clean),
    }
}

fn clean_event(
    outcome: &EntryQuoteOutcome,
    max_market_exit_delay_ms: Option<i64>,
) -> Option<CleanEvent> {
    if outcome.closes.is_empty() || outcome.event.quote_status != QUOTE_OK {
        return None;
    }
    let quote_price = positive(outcome.event.quote_price_sol?)?;
    let shadow_price = positive(outcome.event.shadow_price_sol?)?;
    let ratio = quote_price / shadow_price;
    if is_contaminated(outcome)? {
        return None;
    }
    let shadow_pnl_sol = outcome.closes.iter().map(|row| row.pnl_sol).sum::<f64>();
    let entry_cost_sol = outcome
        .closes
        .iter()
        .map(|row| row.entry_cost_sol)
        .sum::<f64>();
    let exit_value_sol = outcome
        .closes
        .iter()
        .map(|row| row.exit_value_sol)
        .sum::<f64>();
    let entry_qty_factor = shadow_price / quote_price;
    let adjusted_exit = exit_value_sol * entry_qty_factor;
    let bucket = close_bucket(&outcome.closes);
    let hold_time_bucket = HoldTimeBucket::from_seconds(max_hold_seconds(&outcome.closes));
    let executable = fully_executable_pnl(
        outcome,
        entry_qty_factor,
        entry_cost_sol,
        max_market_exit_delay_ms,
    );
    Some(CleanEvent {
        cohort: RankCohort::from_rank(outcome.event.discovery_rank),
        source_cohort: SourceCohort::from_source(outcome.event.source_cohort.as_deref()),
        bucket,
        exit_executability: exit_executability(bucket, executable.pnl_sol),
        hold_time_bucket,
        shadow_pnl_sol,
        entry_adjusted_pnl_sol: adjusted_exit - entry_cost_sol,
        fully_executable_pnl_sol: executable.pnl_sol,
        ratio,
        price_impact_pct: outcome.event.price_impact_pct,
        market_exit_quote_events: executable.market_quote_events,
        market_exit_error_events: executable.market_error_events,
        market_exit_dead_error_events: executable.market_dead_error_events,
        market_exit_transient_error_events: executable.market_transient_error_events,
        market_exit_missing_quote_events: executable.market_missing_events,
        market_exit_ratio_outlier_events: executable.market_ratio_outlier_events,
        market_exit_zero_exit_events: executable.market_zero_exit_events,
        market_exit_quote_shadow_ratios: executable.market_quote_shadow_ratios,
        market_exit_decision_delay_ms: executable.market_decision_delay_ms,
    })
}

fn max_hold_seconds(closes: &[CloseOutcome]) -> i64 {
    closes
        .iter()
        .map(|close| {
            close
                .closed_ts
                .signed_duration_since(close.opened_ts)
                .num_seconds()
                .max(0)
        })
        .max()
        .unwrap_or(0)
}

fn is_contaminated(outcome: &EntryQuoteOutcome) -> Option<bool> {
    let quote_price = positive(outcome.event.quote_price_sol?)?;
    let shadow_price = positive(outcome.event.shadow_price_sol?)?;
    let ratio = quote_price / shadow_price;
    Some(!(RATIO_MIN..=RATIO_MAX).contains(&ratio))
}

fn positive(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}

fn close_bucket(closes: &[CloseOutcome]) -> CloseBucket {
    let mut bucket = None;
    for close in closes {
        let next = match close.close_context.as_str() {
            "market" => CloseBucket::Market,
            "stale_quote_price" => CloseBucket::StaleQuote,
            "stale_market_price" => CloseBucket::StaleMarket,
            "stale_terminal_zero_price" | "recovery_terminal_zero_price" => CloseBucket::Terminal,
            _ => CloseBucket::Other,
        };
        if bucket.is_some_and(|current| current != next) {
            return CloseBucket::Mixed;
        }
        bucket = Some(next);
    }
    bucket.unwrap_or(CloseBucket::Open)
}

fn exit_executability(
    bucket: CloseBucket,
    fully_executable_pnl_sol: Option<f64>,
) -> ExitExecutability {
    if bucket == CloseBucket::Mixed {
        return ExitExecutability::MixedAmbiguous;
    }
    if fully_executable_pnl_sol.is_some() {
        return ExitExecutability::FullyExecutable;
    }
    match bucket {
        CloseBucket::Market
        | CloseBucket::StaleMarket
        | CloseBucket::Other
        | CloseBucket::StaleQuote
        | CloseBucket::Terminal => ExitExecutability::HybridPaperExit,
        CloseBucket::Mixed | CloseBucket::Open => ExitExecutability::MixedAmbiguous,
    }
}

pub(crate) fn summarize_close_buckets(events: &[CleanEvent]) -> Vec<BucketSummary> {
    [
        (CloseBucket::Market, "market"),
        (CloseBucket::StaleQuote, "stale_quote_price"),
        (CloseBucket::StaleMarket, "stale_market_price"),
        (CloseBucket::Terminal, "terminal"),
        (CloseBucket::Mixed, "mixed"),
        (CloseBucket::Other, "other"),
    ]
    .into_iter()
    .map(|(bucket, label)| {
        summarize_bucket(label, events.iter().filter(|event| event.bucket == bucket))
    })
    .collect()
}

pub(crate) fn summarize_exit_executability(events: &[CleanEvent]) -> Vec<BucketSummary> {
    [
        (ExitExecutability::FullyExecutable, "fully_executable"),
        (ExitExecutability::HybridPaperExit, "hybrid_paper_exit"),
        (ExitExecutability::MixedAmbiguous, "mixed_ambiguous"),
    ]
    .into_iter()
    .map(|(bucket, label)| {
        summarize_bucket(
            label,
            events
                .iter()
                .filter(|event| event.exit_executability == bucket),
        )
    })
    .collect()
}

pub(crate) fn summarize_bucket<'a>(
    label: &str,
    events: impl Iterator<Item = &'a CleanEvent>,
) -> BucketSummary {
    let mut out = BucketSummary {
        bucket: label.to_string(),
        ..BucketSummary::default()
    };
    let mut ratio_sum = 0.0;
    let mut impact_sum = 0.0;
    let mut impact_count = 0_u64;
    let mut fully_executable_pnl_sol = 0.0;
    let mut fully_executable_shadow_pnl_sol = 0.0;
    let mut fully_executable_events = 0_u64;
    let mut fully_executable_delta_values = Vec::new();
    let mut market_exit_ratio_sum = 0.0;
    let mut market_exit_ratio_count = 0_u64;
    for event in events {
        out.events += 1;
        out.shadow_pnl_sol += event.shadow_pnl_sol;
        out.entry_adjusted_pnl_sol += event.entry_adjusted_pnl_sol;
        if let Some(pnl) = event.fully_executable_pnl_sol {
            fully_executable_events += 1;
            fully_executable_pnl_sol += pnl;
            fully_executable_shadow_pnl_sol += event.shadow_pnl_sol;
            fully_executable_delta_values.push(pnl - event.shadow_pnl_sol);
        }
        out.market_exit_quote_events += event.market_exit_quote_events;
        out.market_exit_error_events += event.market_exit_error_events;
        out.market_exit_dead_error_events += event.market_exit_dead_error_events;
        out.market_exit_transient_error_events += event.market_exit_transient_error_events;
        out.market_exit_missing_quote_events += event.market_exit_missing_quote_events;
        out.market_exit_ratio_outlier_events += event.market_exit_ratio_outlier_events;
        out.market_exit_zero_exit_events += event.market_exit_zero_exit_events;
        ratio_sum += event.ratio;
        for ratio in &event.market_exit_quote_shadow_ratios {
            market_exit_ratio_sum += ratio;
            market_exit_ratio_count += 1;
        }
        if let Some(impact) = event.price_impact_pct.filter(|value| value.is_finite()) {
            impact_sum += impact;
            impact_count += 1;
        }
    }
    out.entry_adjusted_delta_sol = out.entry_adjusted_pnl_sol - out.shadow_pnl_sol;
    out.fully_executable_events = fully_executable_events;
    if fully_executable_events > 0 {
        out.fully_executable_pnl_sol = Some(fully_executable_pnl_sol);
        out.fully_executable_shadow_pnl_sol = Some(fully_executable_shadow_pnl_sol);
        out.fully_executable_delta_sol =
            Some(fully_executable_pnl_sol - fully_executable_shadow_pnl_sol);
        out.fully_executable_delta_stats = numeric_stats(fully_executable_delta_values);
    }
    if out.events > 0 {
        out.avg_quote_shadow_ratio = Some(ratio_sum / out.events as f64);
        out.fully_executable_coverage = Some(fully_executable_events as f64 / out.events as f64);
    }
    if impact_count > 0 {
        out.avg_price_impact_pct = Some(impact_sum / impact_count as f64);
    }
    if market_exit_ratio_count > 0 {
        out.avg_market_exit_quote_shadow_ratio =
            Some(market_exit_ratio_sum / market_exit_ratio_count as f64);
    }
    out
}
