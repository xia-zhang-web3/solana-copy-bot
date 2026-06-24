use crate::track_b_entry_quote_report_db::{CloseOutcome, EntryQuoteOutcome};
use crate::track_b_entry_quote_report_executable::fully_executable_pnl;
use crate::track_b_entry_quote_report_stats::numeric_stats;
use crate::track_b_entry_quote_report_types::{
    BucketSummary, SummaryCounts, SweepRow, TrackBEntryQuoteSummary,
};

const QUOTE_OK: &str = "ok";
const CONTAMINATION_IMPACT_MAX: f64 = 0.01;
const RATIO_MIN: f64 = 0.1;
const RATIO_MAX: f64 = 10.0;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseBucket {
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
struct CleanEvent {
    bucket: CloseBucket,
    exit_executability: ExitExecutability,
    shadow_pnl_sol: f64,
    entry_adjusted_pnl_sol: f64,
    fully_executable_pnl_sol: Option<f64>,
    ratio: f64,
    price_impact_pct: Option<f64>,
    market_exit_quote_events: u64,
    market_exit_missing_quote_events: u64,
    market_exit_quote_shadow_ratios: Vec<f64>,
    market_exit_decision_delay_ms: Vec<f64>,
}

pub(crate) fn summarize_track_b(
    outcomes: Vec<EntryQuoteOutcome>,
    close_match_limit: u32,
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
        let Some(event) = clean_event(outcome) else {
            if is_contaminated(outcome).unwrap_or(false) {
                counts.contaminated_ratio_events += 1;
            }
            continue;
        };
        if event.bucket == CloseBucket::Mixed {
            counts.mixed_close_context_events += 1;
        }
        counts.market_exit_quote_events += event.market_exit_quote_events;
        counts.market_exit_missing_quote_events += event.market_exit_missing_quote_events;
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
        caveats: caveats(),
        counts,
        price_ratio_stats: numeric_stats(ratios),
        price_impact_stats: numeric_stats(impacts),
        market_exit_quote_ratio_stats: numeric_stats(market_exit_ratios),
        market_exit_decision_delay_ms_stats: numeric_stats(market_exit_delays),
        by_close_bucket: summarize_close_buckets(&clean),
        by_exit_executability: summarize_exit_executability(&clean),
        price_impact_sweep: sweep_price_impact(&clean),
        quote_shadow_ratio_sweep: sweep_ratio(&clean),
    }
}

fn clean_event(outcome: &EntryQuoteOutcome) -> Option<CleanEvent> {
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
    let executable = fully_executable_pnl(outcome, entry_qty_factor, entry_cost_sol);
    Some(CleanEvent {
        bucket,
        exit_executability: exit_executability(bucket, executable.pnl_sol),
        shadow_pnl_sol,
        entry_adjusted_pnl_sol: adjusted_exit - entry_cost_sol,
        fully_executable_pnl_sol: executable.pnl_sol,
        ratio,
        price_impact_pct: outcome.event.price_impact_pct,
        market_exit_quote_events: executable.market_quote_events,
        market_exit_missing_quote_events: executable.market_missing_events,
        market_exit_quote_shadow_ratios: executable.market_quote_shadow_ratios,
        market_exit_decision_delay_ms: executable.market_decision_delay_ms,
    })
}

fn is_contaminated(outcome: &EntryQuoteOutcome) -> Option<bool> {
    let quote_price = positive(outcome.event.quote_price_sol?)?;
    let shadow_price = positive(outcome.event.shadow_price_sol?)?;
    let impact = outcome.event.price_impact_pct?;
    let ratio = quote_price / shadow_price;
    Some(impact <= CONTAMINATION_IMPACT_MAX && !(RATIO_MIN..=RATIO_MAX).contains(&ratio))
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

fn summarize_close_buckets(events: &[CleanEvent]) -> Vec<BucketSummary> {
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

fn summarize_exit_executability(events: &[CleanEvent]) -> Vec<BucketSummary> {
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

fn summarize_bucket<'a>(
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
    let mut fully_executable_events = 0_u64;
    let mut market_exit_ratio_sum = 0.0;
    let mut market_exit_ratio_count = 0_u64;
    for event in events {
        out.events += 1;
        out.shadow_pnl_sol += event.shadow_pnl_sol;
        out.entry_adjusted_pnl_sol += event.entry_adjusted_pnl_sol;
        if let Some(pnl) = event.fully_executable_pnl_sol {
            fully_executable_events += 1;
            fully_executable_pnl_sol += pnl;
        }
        out.market_exit_quote_events += event.market_exit_quote_events;
        out.market_exit_missing_quote_events += event.market_exit_missing_quote_events;
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
        out.fully_executable_delta_sol = Some(fully_executable_pnl_sol - out.shadow_pnl_sol);
    }
    if out.events > 0 {
        out.avg_quote_shadow_ratio = Some(ratio_sum / out.events as f64);
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

fn sweep_price_impact(events: &[CleanEvent]) -> Vec<SweepRow> {
    [0.01, 0.05, 0.10, 0.20, 0.50]
        .into_iter()
        .map(|threshold| {
            sweep(
                "price_impact_pct",
                threshold,
                events.iter().filter(|event| {
                    event
                        .price_impact_pct
                        .map(|impact| impact > threshold)
                        .unwrap_or(false)
                }),
            )
        })
        .collect()
}

fn sweep_ratio(events: &[CleanEvent]) -> Vec<SweepRow> {
    [1.01, 1.05, 1.10, 1.20, 1.50, 2.0]
        .into_iter()
        .map(|threshold| {
            sweep(
                "quote_shadow_ratio",
                threshold,
                events.iter().filter(|event| event.ratio > threshold),
            )
        })
        .collect()
}

fn sweep<'a>(
    metric: &str,
    threshold: f64,
    events: impl Iterator<Item = &'a CleanEvent>,
) -> SweepRow {
    let mut row = SweepRow {
        metric: metric.to_string(),
        threshold_gt: threshold,
        rejected_events: 0,
        rejected_market_events: 0,
        rejected_stale_quote_events: 0,
        rejected_stale_market_events: 0,
        rejected_terminal_events: 0,
        rejected_mixed_events: 0,
        rejected_shadow_pnl_sol: 0.0,
        rejected_entry_adjusted_pnl_sol: 0.0,
        rejected_fully_executable_pnl_sol: None,
        delta_if_rejected_entry_adjusted_sol: 0.0,
        delta_if_rejected_fully_executable_sol: None,
        warning:
            "Rows with fully_executable_pnl_sol=null still lack executable market-exit data or use paper close marks."
                .to_string(),
    };
    let mut rejected_full = 0.0;
    let mut rejected_full_count = 0_u64;
    for event in events {
        row.rejected_events += 1;
        match event.bucket {
            CloseBucket::Market => row.rejected_market_events += 1,
            CloseBucket::StaleQuote => row.rejected_stale_quote_events += 1,
            CloseBucket::StaleMarket => row.rejected_stale_market_events += 1,
            CloseBucket::Terminal => row.rejected_terminal_events += 1,
            CloseBucket::Mixed => row.rejected_mixed_events += 1,
            CloseBucket::Open | CloseBucket::Other => {}
        }
        row.rejected_shadow_pnl_sol += event.shadow_pnl_sol;
        row.rejected_entry_adjusted_pnl_sol += event.entry_adjusted_pnl_sol;
        if let Some(pnl) = event.fully_executable_pnl_sol {
            rejected_full_count += 1;
            rejected_full += pnl;
        }
    }
    row.delta_if_rejected_entry_adjusted_sol = -row.rejected_entry_adjusted_pnl_sol;
    if rejected_full_count > 0 {
        row.rejected_fully_executable_pnl_sol = Some(rejected_full);
        row.delta_if_rejected_fully_executable_sol = Some(-rejected_full);
    }
    row
}

fn caveats() -> Vec<String> {
    vec![
        "Entry is executable Track-B quote; market exits become fully executable only when a matching market-exit diagnostic quote is present."
            .to_string(),
        "Fully executable calls should use rows with fully_executable_pnl_sol present, not aggregate paper buckets."
            .to_string(),
        "Outcome join is wallet_id + token + opened_ts(signal_ts), not sell-side signal_id."
            .to_string(),
        "Mixed close-context events are reported separately to avoid fanout hiding.".to_string(),
        "Market-exit executable quotes are delayed diagnostics; inspect market_exit_decision_delay_ms_stats before treating them as close-time exits."
            .to_string(),
    ]
}
