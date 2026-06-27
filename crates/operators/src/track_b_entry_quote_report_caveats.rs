pub(crate) fn track_b_caveats() -> Vec<String> {
    [
        "Entry is executable Track-B quote; market exits become fully executable only when a matching market-exit diagnostic quote is present.",
        "Only terminal market-exit errors (not-tradable/no-route) are booked as zero-exit; transient errors and missing rows remain no-data.",
        "Entry and market-exit quote rows with quote/shadow price ratios outside 0.1x..10x are excluded as quote-unit outliers.",
        "Fully executable calls should use rows with fully_executable_pnl_sol present, not aggregate paper buckets.",
        "Outcome join is wallet_id + token + opened_ts(signal_ts), not sell-side signal_id.",
        "Cohort split prefers discovery_rank stamped on the entry diagnostic event; legacy rows fall back to the latest available wallet_metrics rank at or before request_ts.",
        "Source-cohort split uses source_cohort stamped on the entry diagnostic event; legacy rows before the stamp are reported as unknown.",
        "Mixed close-context events are reported separately to avoid fanout hiding.",
        "Market-exit executable quotes are delayed diagnostics; inspect market_exit_decision_delay_ms_stats before treating them as close-time exits.",
        "Hold-time buckets use max(closed_ts-opened_ts) across matched partial closes for the entry event.",
        "Hold-time buckets are realized outcome-dependent; smaller gaps in slower buckets can reflect survivorship/collider bias. Treat this slice as a kill-test: weak or negative slow-bucket evidence is informative, while positive evidence needs an entry-time token-speed proxy before green-lighting a universe pivot.",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}
