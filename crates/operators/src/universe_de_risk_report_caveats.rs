pub(crate) fn caveats() -> Vec<String> {
    vec![
        "Leader-side de-risk only: this report does not measure follower-gap.".to_string(),
        "Scope: only WSOL-paired swaps are scanned; USDC-paired and token-token established markets are structurally excluded, so a thin/no-go result is not proof that #3 has no slow population.".to_string(),
        "Survival/decay metrics are observed-swaps proxies, not shadow stale/rug outcomes.".to_string(),
        "Token age and verification are not inferred from observed_swaps retention.".to_string(),
        "Left-truncation: buys before --since are not loaded; in-window sells without an in-window buy are dropped from FIFO PnL and exposed as unmatched_sell_events.".to_string(),
        "DEX summaries group token-level PnL by dominant observed DEX; use them as directional segmentation, not exact per-DEX execution accounting.".to_string(),
        "Read any positive verdict with concentration fields; sum-only leader PnL can be dominated by a few wallets or tokens.".to_string(),
        "Global copyable_wallets are wallet-level; by_dex copyable_wallets are per dominant DEX segment and are not expected to sum to the global total.".to_string(),
        "safety.rows_seen is raw fetched rows; summary.totals.rows_seen is rows retained by the report's positive-SOL-notional filter.".to_string(),
        "Use this report to choose candidate pools; Track-B on a separate instance is needed for executable edge.".to_string(),
    ]
}
