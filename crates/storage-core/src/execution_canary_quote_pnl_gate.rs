use crate::{
    ExecutionCanaryQuotePnlSummary, ExecutionCanaryQuoteReadinessCheck,
    ExecutionCanaryQuoteReadinessGate,
};

const MIN_MARKET_CLOSED_TRADES: u64 = 30;
const MAX_SKIP_RATE_PCT: f64 = 20.0;
const WARN_SKIP_RATE_PCT: f64 = 15.0;
const MAX_NON_OK_PRIORITY_FEE_RATE_PCT: f64 = 10.0;
const WARN_NON_OK_PRIORITY_FEE_RATE_PCT: f64 = 0.0;
const MAX_ENTRY_QUOTE_LATENCY_MS: f64 = 500.0;
const WARN_ENTRY_QUOTE_LATENCY_MS: f64 = 250.0;
const MAX_ENTRY_DECISION_DELAY_MS: f64 = 4_000.0;
const WARN_ENTRY_DECISION_DELAY_MS: f64 = 2_500.0;

const STATUS_READY: &str = "ready";
const STATUS_READY_WITH_WARNINGS: &str = "ready_with_warnings";
const STATUS_BLOCKED: &str = "blocked";
const CHECK_PASS: &str = "pass";
const CHECK_WARN: &str = "warn";
const CHECK_BLOCK: &str = "block";

pub(crate) fn build_quote_readiness_gate(
    summary: &ExecutionCanaryQuotePnlSummary,
    open_position_count: u64,
) -> ExecutionCanaryQuoteReadinessGate {
    let market_closed_trades = summary.shadow_close_breakdown.market_closed_trades;
    let sampled_market_trades = summary.total_closed_trades;
    let skip_rate_pct = percent(summary.skipped_trades, sampled_market_trades);
    let unknown_rate_pct = percent(summary.unknown_trades, sampled_market_trades);
    let quote_win_rate_pct = percent(
        summary.quote_win_count,
        summary.quote_win_count + summary.quote_loss_count,
    );
    let non_ok_priority_fee_rate_pct = non_ok_priority_fee_rate_pct(summary);
    let avg_entry_quote_latency_ms = summary.quote_diagnostics.entry_all.quote_latency_ms_avg;
    let avg_entry_decision_delay_ms = summary.quote_diagnostics.entry_all.decision_delay_ms_avg;

    let mut checks = Vec::new();
    checks.push(min_market_trades_check(market_closed_trades));
    checks.push(sample_size_check(sampled_market_trades));
    checks.push(open_position_check(open_position_count));
    checks.push(unknown_quote_check(summary.unknown_trades));
    checks.push(quote_pnl_check(
        summary.quote_adjusted_pnl_after_priority_fee_sol,
    ));
    checks.push(skip_rate_check(skip_rate_pct));
    checks.push(priority_fee_check(
        priority_fee_sample_count(summary),
        non_ok_priority_fee_rate_pct,
    ));
    checks.push(entry_quote_latency_check(avg_entry_quote_latency_ms));
    checks.push(entry_decision_delay_check(avg_entry_decision_delay_ms));
    checks.push(stale_rug_like_check(
        summary.shadow_close_breakdown.stale_rug_like_closed_trades,
    ));
    checks.push(force_exit_check(
        summary.force_exit_counted_trades + summary.force_exit_skipped_entry_trades,
    ));

    let blocker_count = checks
        .iter()
        .filter(|check| check.status == CHECK_BLOCK)
        .count() as u64;
    let warning_count = checks
        .iter()
        .filter(|check| check.status == CHECK_WARN)
        .count() as u64;
    let status = if blocker_count > 0 {
        STATUS_BLOCKED
    } else if warning_count > 0 {
        STATUS_READY_WITH_WARNINGS
    } else {
        STATUS_READY
    };

    ExecutionCanaryQuoteReadinessGate {
        status: status.to_string(),
        can_start_tiny_execution: blocker_count == 0,
        blocker_count,
        warning_count,
        min_market_closed_trades: MIN_MARKET_CLOSED_TRADES,
        market_closed_trades,
        sampled_market_trades,
        open_position_count,
        quote_after_fee_pnl_sol: summary.quote_adjusted_pnl_after_priority_fee_sol,
        quote_win_rate_pct,
        skip_rate_pct,
        unknown_rate_pct,
        non_ok_priority_fee_rate_pct,
        avg_entry_quote_latency_ms,
        avg_entry_decision_delay_ms,
        checks,
    }
}

fn min_market_trades_check(market_closed_trades: u64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "market_closed_trades",
        if market_closed_trades >= MIN_MARKET_CLOSED_TRADES {
            CHECK_PASS
        } else {
            CHECK_BLOCK
        },
        market_closed_trades.to_string(),
        format!(">= {MIN_MARKET_CLOSED_TRADES}"),
        "need enough closed market trades in the fresh window",
    )
}

fn sample_size_check(sampled_market_trades: u64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "quote_sample_size",
        if sampled_market_trades >= MIN_MARKET_CLOSED_TRADES {
            CHECK_PASS
        } else {
            CHECK_BLOCK
        },
        sampled_market_trades.to_string(),
        format!(">= {MIN_MARKET_CLOSED_TRADES}"),
        "quote-adjusted report needs enough sampled market closes",
    )
}

fn open_position_check(open_position_count: u64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "canary_open_positions",
        if open_position_count == 0 {
            CHECK_PASS
        } else {
            CHECK_BLOCK
        },
        open_position_count.to_string(),
        "0",
        "tiny test should start from a clean canary-owned position state",
    )
}

fn unknown_quote_check(unknown_trades: u64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "unknown_or_missing_quotes",
        if unknown_trades == 0 {
            CHECK_PASS
        } else {
            CHECK_BLOCK
        },
        unknown_trades.to_string(),
        "0",
        "entry and exit quote accounting must be complete",
    )
}

fn quote_pnl_check(quote_after_fee_pnl_sol: f64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "quote_pnl_after_fee",
        if quote_after_fee_pnl_sol > 0.0 {
            CHECK_PASS
        } else {
            CHECK_BLOCK
        },
        sol_value(quote_after_fee_pnl_sol),
        "> 0 SOL",
        "quote-adjusted PnL must stay positive after priority fees",
    )
}

fn skip_rate_check(skip_rate_pct: f64) -> ExecutionCanaryQuoteReadinessCheck {
    let status = if skip_rate_pct > MAX_SKIP_RATE_PCT {
        CHECK_BLOCK
    } else if skip_rate_pct > WARN_SKIP_RATE_PCT {
        CHECK_WARN
    } else {
        CHECK_PASS
    };
    check(
        "buy_skip_rate",
        status,
        pct_value(skip_rate_pct),
        format!("<= {MAX_SKIP_RATE_PCT:.1}%"),
        "too many entries are outside the active buy slippage gate",
    )
}

fn priority_fee_check(
    priority_fee_samples: u64,
    non_ok_priority_fee_rate_pct: f64,
) -> ExecutionCanaryQuoteReadinessCheck {
    let status = if priority_fee_samples == 0
        || non_ok_priority_fee_rate_pct > MAX_NON_OK_PRIORITY_FEE_RATE_PCT
    {
        CHECK_BLOCK
    } else if non_ok_priority_fee_rate_pct > WARN_NON_OK_PRIORITY_FEE_RATE_PCT {
        CHECK_WARN
    } else {
        CHECK_PASS
    };
    check(
        "priority_fee_health",
        status,
        format!(
            "samples={} non_ok={}",
            priority_fee_samples,
            pct_value(non_ok_priority_fee_rate_pct)
        ),
        format!("samples > 0 and non_ok <= {MAX_NON_OK_PRIORITY_FEE_RATE_PCT:.1}%"),
        "priority fee data must be available and stable",
    )
}

fn entry_quote_latency_check(
    avg_entry_quote_latency_ms: f64,
) -> ExecutionCanaryQuoteReadinessCheck {
    let status = if avg_entry_quote_latency_ms > MAX_ENTRY_QUOTE_LATENCY_MS {
        CHECK_BLOCK
    } else if avg_entry_quote_latency_ms > WARN_ENTRY_QUOTE_LATENCY_MS {
        CHECK_WARN
    } else {
        CHECK_PASS
    };
    check(
        "entry_quote_latency",
        status,
        ms_value(avg_entry_quote_latency_ms),
        format!("<= {MAX_ENTRY_QUOTE_LATENCY_MS:.0}ms"),
        "quote provider must answer fast enough for tiny entry tests",
    )
}

fn entry_decision_delay_check(
    avg_entry_decision_delay_ms: f64,
) -> ExecutionCanaryQuoteReadinessCheck {
    let status = if avg_entry_decision_delay_ms > MAX_ENTRY_DECISION_DELAY_MS {
        CHECK_BLOCK
    } else if avg_entry_decision_delay_ms > WARN_ENTRY_DECISION_DELAY_MS {
        CHECK_WARN
    } else {
        CHECK_PASS
    };
    check(
        "entry_decision_delay",
        status,
        ms_value(avg_entry_decision_delay_ms),
        format!("<= {MAX_ENTRY_DECISION_DELAY_MS:.0}ms"),
        "decision path must stay close to observed entry events",
    )
}

fn stale_rug_like_check(stale_rug_like_closed_trades: u64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "stale_rug_like_closes",
        if stale_rug_like_closed_trades == 0 {
            CHECK_PASS
        } else {
            CHECK_WARN
        },
        stale_rug_like_closed_trades.to_string(),
        "0 preferred",
        "rug-like stale closes are token risk, not quote path failure",
    )
}

fn force_exit_check(force_exit_trades: u64) -> ExecutionCanaryQuoteReadinessCheck {
    check(
        "force_exit_closes",
        if force_exit_trades == 0 {
            CHECK_PASS
        } else {
            CHECK_WARN
        },
        force_exit_trades.to_string(),
        "0 preferred",
        "force exits should be reviewed before increasing size",
    )
}

fn priority_fee_sample_count(summary: &ExecutionCanaryQuotePnlSummary) -> u64 {
    summary
        .priority_fee_status_counts
        .iter()
        .filter(|count| count.status != "skipped")
        .map(|count| count.events)
        .sum()
}

fn non_ok_priority_fee_rate_pct(summary: &ExecutionCanaryQuotePnlSummary) -> f64 {
    let total = priority_fee_sample_count(summary);
    let non_ok = summary
        .priority_fee_status_counts
        .iter()
        .filter(|count| count.status != "ok" && count.status != "skipped")
        .map(|count| count.events)
        .sum();
    percent(non_ok, total)
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

fn check(
    name: &str,
    status: &str,
    value: impl Into<String>,
    threshold: impl Into<String>,
    reason: &str,
) -> ExecutionCanaryQuoteReadinessCheck {
    ExecutionCanaryQuoteReadinessCheck {
        name: name.to_string(),
        status: status.to_string(),
        value: value.into(),
        threshold: threshold.into(),
        reason: reason.to_string(),
    }
}

fn pct_value(value: f64) -> String {
    format!("{value:.2}%")
}

fn ms_value(value: f64) -> String {
    format!("{value:.0}ms")
}

fn sol_value(value: f64) -> String {
    format!("{value:.6} SOL")
}
