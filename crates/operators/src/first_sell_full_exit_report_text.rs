pub(crate) fn caveats() -> Vec<String> {
    vec![
        "Historical SELL quotes were usually sized to partial shadow closes; amount_scaled_estimate linearly rescales those quotes and is not a historical full-position quote.".to_string(),
        "PnL summaries include valued exits only; full coverage requires a daemon-buildable quote for every entry, so no-trigger, unknown, and assumed terminal-zero outcomes keep the verdict fail-closed.".to_string(),
        "strict_unscaled includes exact raw-amount quotes and terminal-zero outcomes, but excludes scaled partial quotes.".to_string(),
        "daemon_token_first_sell mirrors token-level sell and stale triggers; origin_wallet_first_sell is a counterfactual policy.".to_string(),
        "The first trigger is never replaced by a later successful quote when the first quote is missing or errors.".to_string(),
        "Planned net uses configured fee assumptions, not charged transaction metadata or realized fills.".to_string(),
        "New-ATA cash cost may later be recoverable; it is shown separately from the existing-ATA scenario.".to_string(),
        "Each entry is replayed independently; portfolio position caps, overlapping same-token entries, landing, and scheduler races are not reconstructed.".to_string(),
        "The entry gate uses the canonical quote event; alternate provider selection and later risk, submit, confirmation, and landing gates are not reconstructed.".to_string(),
        "A no-route or not-tradable quote leaves the virtual position open and is reported as unknown, not as a realized zero exit.".to_string(),
        "A shadow stale/recovery terminal-zero close without a quote is a conservative zero-valuation assumption, not proof that the follower transaction landed.".to_string(),
    ]
}

pub(crate) fn prospective_requirements() -> Vec<String> {
    vec![
        "Persist entry quotes for the same signal at 0.01, 0.02, 0.05, 0.075, and 0.10 SOL.".to_string(),
        "Persist exactly one full-raw-amount SELL quote at the first eligible token-level trigger for every virtual size.".to_string(),
        "Persist route, raw amounts, errors, fee samples, config identity, and trigger/origin wallets.".to_string(),
        "Keep actual charged network fees, ATA rent, failed-submit cost, and realized fills as separate proof.".to_string(),
    ]
}
