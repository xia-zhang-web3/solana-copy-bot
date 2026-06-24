pub(crate) fn track_b_caveats() -> Vec<String> {
    [
        "Entry is executable Track-B quote; market exits become fully executable only when a matching market-exit diagnostic quote is present.",
        "Failed market-exit quotes are booked as zero-exit; missing rows remain no-data.",
        "Fully executable calls should use rows with fully_executable_pnl_sol present, not aggregate paper buckets.",
        "Outcome join is wallet_id + token + opened_ts(signal_ts), not sell-side signal_id.",
        "Mixed close-context events are reported separately to avoid fanout hiding.",
        "Market-exit executable quotes are delayed diagnostics; inspect market_exit_decision_delay_ms_stats before treating them as close-time exits.",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}
