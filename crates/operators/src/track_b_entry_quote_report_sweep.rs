use crate::track_b_entry_quote_report_summary::{CleanEvent, CloseBucket};
use crate::track_b_entry_quote_report_types::SweepRow;

pub(crate) fn sweep_price_impact(events: &[CleanEvent]) -> Vec<SweepRow> {
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

pub(crate) fn sweep_ratio(events: &[CleanEvent]) -> Vec<SweepRow> {
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
