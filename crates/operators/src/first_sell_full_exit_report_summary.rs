use crate::first_sell_full_exit_report_compute::{
    entry_amounts, estimate_replay_row, rank_cohort, AmountEvidence, EntryGateAssumptions,
    EntryValidationError, EstimateOutcome, FeeAssumptions, TradeEstimate,
};
use crate::first_sell_full_exit_report_db::AuditEvidence;
use crate::first_sell_full_exit_report_replay::{replay_policy, ReplayPolicy};
use crate::first_sell_full_exit_report_text::{caveats, prospective_requirements};
use crate::first_sell_full_exit_report_types::{
    CohortSummary, EntryGateModelSummary, FeeModelSummary, FirstSellAuditSummary, ObservedNotional,
    PnlStats, PolicySummary, ReasonCount,
};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};

const SOL_LAMPORTS: f64 = 1_000_000_000.0;
const MIN_EXACT_EVENTS: u64 = 30;

pub(crate) fn summarize_audit(
    mut evidence: AuditEvidence,
    gate: EntryGateAssumptions,
    fees: FeeAssumptions,
    outcome_until: DateTime<Utc>,
) -> FirstSellAuditSummary {
    let mut notionals = BTreeMap::new();
    let mut latest_entry_ready = None;
    for entry in &evidence.entries {
        match entry_amounts(entry, gate) {
            Ok(amounts) => {
                evidence.coverage.eligible_entry_events += 1;
                *notionals.entry(amounts.in_lamports).or_insert(0_u64) += 1;
                latest_entry_ready = latest_entry_ready.max(entry.modeled_entry_ready_ts);
            }
            Err(EntryValidationError::QuoteSkipped) => evidence.coverage.skipped_entry_events += 1,
            Err(EntryValidationError::ShadowDropped) => {
                evidence.coverage.shadow_dropped_entry_events += 1
            }
            Err(EntryValidationError::ShadowPending) => {
                evidence.coverage.shadow_pending_entry_events += 1
            }
            Err(EntryValidationError::InvalidAmount) => {
                evidence.coverage.invalid_entry_amount_events += 1
            }
            Err(EntryValidationError::GateRejected) => {
                evidence.coverage.entry_gate_rejected_events += 1
            }
            Err(EntryValidationError::TimingUnknown) => {
                evidence.coverage.entry_timing_unknown_events += 1
            }
        }
    }
    let policies = [
        ReplayPolicy::DaemonTokenFirstSell,
        ReplayPolicy::OriginWalletFirstSell,
    ]
    .into_iter()
    .map(|policy| summarize_policy(&evidence, policy, gate, fees))
    .collect::<Vec<_>>();
    let limit_hit = evidence.coverage.entry_limit_hit
        || evidence.coverage.sell_limit_hit
        || evidence.coverage.quote_limit_hit
        || evidence.coverage.close_limit_hit;
    let daemon = policies
        .iter()
        .find(|summary| summary.policy == ReplayPolicy::DaemonTokenFirstSell.label())
        .expect("daemon policy summary must exist");
    let minimum_eligible_entry_followup_seconds =
        latest_entry_ready.map(|ready: DateTime<Utc>| (outcome_until - ready).num_seconds());
    let incomplete_entry_evidence = evidence.coverage.shadow_pending_entry_events > 0
        || evidence.coverage.invalid_entry_amount_events > 0
        || evidence.coverage.entry_timing_unknown_events > 0;
    let verdict = if limit_hit {
        "fail_closed_truncated_evidence"
    } else if incomplete_entry_evidence {
        "fail_closed_incomplete_entry_coverage"
    } else if evidence.coverage.eligible_entry_events == 0 {
        "fail_closed_no_eligible_entries"
    } else if minimum_eligible_entry_followup_seconds.is_none_or(|seconds| seconds <= 0) {
        "fail_closed_no_entry_followup_horizon"
    } else if !daemon.complete_quote_backed_exit_coverage {
        "fail_closed_incomplete_exit_coverage"
    } else if daemon.amount_scaled_estimate.planned_net_events < daemon.valued_exit_events {
        "fail_closed_incomplete_planned_fee_coverage"
    } else if daemon.exact_amount.gross_events < MIN_EXACT_EVENTS {
        "fail_closed_insufficient_exact_full_amount_quotes"
    } else {
        "observation_only_exact_quote_sample_not_real_fills"
    };
    let observed_entry_notionals = notionals
        .into_iter()
        .map(|(entry_in_lamports, events)| ObservedNotional {
            entry_in_lamports,
            entry_notional_sol: entry_in_lamports as f64 / SOL_LAMPORTS,
            events,
        })
        .collect::<Vec<_>>();
    let notional_frontier_status = if observed_entry_notionals.len() <= 1 {
        "unavailable_single_historical_entry_size"
    } else {
        "observed_sizes_only_no_historical_same_signal_matrix"
    };
    FirstSellAuditSummary {
        metric_basis: "valued_exit_only_first_trigger_full_entry_amount_views".to_string(),
        entry_population: "shadow_recorded_canonical_quote_entry_gate".to_string(),
        minimum_eligible_entry_followup_seconds,
        verdict: verdict.to_string(),
        caveats: caveats(),
        coverage: evidence.coverage,
        fee_model: FeeModelSummary {
            priority_fee_cap_lamports_per_leg: fees.priority_fee_cap_lamports,
            base_fee_lamports_per_leg: fees.base_fee_lamports_per_leg,
            new_ata_cash_lamports_per_entry: fees.new_ata_cash_lamports,
            legs_assumed_per_round_trip: 2,
            missing_priority_sample_policy: if fees.priority_fee_cap_lamports > 0 {
                "use_configured_cap_as_conservative_upper_bound"
            } else {
                "missing_sample_keeps_that_trade_planned_net_unknown"
            }
            .to_string(),
            actual_network_fees_available: false,
        },
        entry_gate_model: EntryGateModelSummary {
            expected_in_lamports: gate.expected_in_lamports,
            max_slippage_bps: gate.max_slippage_bps,
            earliest_open_basis: "historical model: max(correlation_request_ts + stored_latency_ms, shadow_gate_recorded_ts); actual HTTP coverage is separate"
                .to_string(),
        },
        observed_entry_notionals,
        notional_frontier_status: notional_frontier_status.to_string(),
        policies,
        prospective_requirements: prospective_requirements(),
    }
}

fn summarize_policy(
    evidence: &AuditEvidence,
    policy: ReplayPolicy,
    gate: EntryGateAssumptions,
    fees: FeeAssumptions,
) -> PolicySummary {
    let rows = replay_policy(evidence, policy);
    let mut trades = Vec::new();
    let mut exact = Vec::new();
    let mut strict_unscaled = Vec::new();
    let mut reasons = BTreeMap::new();
    let mut cohort_entries = BTreeMap::new();
    let mut rank_entries = BTreeMap::new();
    let mut triggers = 0_u64;
    let mut cross_wallet_triggers = 0_u64;
    let mut no_trigger_events = 0_u64;
    let mut exact_amount_events = 0_u64;
    let mut scaled_up_events = 0_u64;
    let mut scaled_down_events = 0_u64;
    let mut terminal_zero_events = 0_u64;
    let mut unknown_exit_events = 0_u64;
    let mut missing_entry_priority_samples = 0_u64;
    let mut missing_exit_priority_samples = 0_u64;
    for row in rows {
        if entry_amounts(&row.entry, gate).is_err() {
            continue;
        }
        let cohort = row
            .entry
            .source_cohort
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        *cohort_entries.entry(cohort).or_insert(0_u64) += 1;
        let rank = rank_cohort(row.entry.discovery_rank);
        *rank_entries.entry(rank).or_insert(0_u64) += 1;
        if row.trigger.is_some() {
            triggers += 1;
        }
        match estimate_replay_row(&row, gate, fees) {
            EstimateOutcome::NoTrigger => {
                no_trigger_events += 1;
                *reasons
                    .entry("no_exit_trigger".to_string())
                    .or_insert(0_u64) += 1;
                if row.entry.priority_fee_lamports.is_none() {
                    missing_entry_priority_samples += 1;
                }
            }
            EstimateOutcome::NoData {
                reason,
                cross_wallet_trigger,
                missing_entry_priority_sample,
                missing_exit_priority_sample,
            } => {
                unknown_exit_events += 1;
                cross_wallet_triggers += u64::from(cross_wallet_trigger);
                missing_entry_priority_samples += u64::from(missing_entry_priority_sample);
                missing_exit_priority_samples += u64::from(missing_exit_priority_sample);
                *reasons.entry(reason).or_insert(0_u64) += 1;
            }
            EstimateOutcome::Trade(trade) => {
                cross_wallet_triggers += u64::from(trade.cross_wallet_trigger);
                missing_entry_priority_samples += u64::from(trade.missing_entry_priority_sample);
                missing_exit_priority_samples += u64::from(trade.missing_exit_priority_sample);
                match trade.amount_evidence {
                    AmountEvidence::Exact => {
                        exact_amount_events += 1;
                        exact.push(trade.clone());
                        strict_unscaled.push(trade.clone());
                    }
                    AmountEvidence::ScaledUp => scaled_up_events += 1,
                    AmountEvidence::ScaledDown => scaled_down_events += 1,
                    AmountEvidence::TerminalZero => {
                        terminal_zero_events += 1;
                        strict_unscaled.push(trade.clone());
                    }
                }
                trades.push(trade);
            }
            EstimateOutcome::SkippedEntry | EstimateOutcome::InvalidEntryAmount => {}
        }
    }
    let by_source_cohort = cohort_entries
        .into_iter()
        .map(|(cohort, entries)| {
            let cohort_trades = trades
                .iter()
                .filter(|trade| trade.source_cohort == cohort)
                .cloned()
                .collect::<Vec<_>>();
            CohortSummary {
                cohort,
                entries,
                exact_amount_events: cohort_trades
                    .iter()
                    .filter(|trade| trade.amount_evidence == AmountEvidence::Exact)
                    .count() as u64,
                amount_scaled_estimate: pnl_stats(&cohort_trades),
            }
        })
        .collect();
    let by_rank_cohort = rank_entries
        .into_iter()
        .map(|(cohort, entries)| {
            let cohort_trades = trades
                .iter()
                .filter(|trade| trade.rank_cohort == cohort)
                .cloned()
                .collect::<Vec<_>>();
            CohortSummary {
                cohort,
                entries,
                exact_amount_events: cohort_trades
                    .iter()
                    .filter(|trade| trade.amount_evidence == AmountEvidence::Exact)
                    .count() as u64,
                amount_scaled_estimate: pnl_stats(&cohort_trades),
            }
        })
        .collect();
    let valued_exit_events = trades.len() as u64;
    let quote_backed_exit_events = trades
        .iter()
        .filter(|trade| trade.amount_evidence != AmountEvidence::TerminalZero)
        .count() as u64;
    let entries = evidence.coverage.eligible_entry_events;
    PolicySummary {
        policy: policy.label().to_string(),
        pnl_scope: "valued_exit_rows_only_including_explicit_terminal_zero_assumptions".to_string(),
        entries,
        triggers,
        valued_exit_events,
        quote_backed_exit_events,
        quote_backed_exit_coverage_pct: percent(quote_backed_exit_events, entries),
        complete_quote_backed_exit_coverage: quote_backed_exit_events == entries,
        cross_wallet_triggers,
        no_trigger_events,
        exact_amount_events,
        scaled_up_events,
        scaled_down_events,
        terminal_zero_events,
        unknown_exit_events,
        missing_entry_priority_samples,
        missing_exit_priority_samples,
        exact_amount: pnl_stats(&exact),
        strict_unscaled: pnl_stats(&strict_unscaled),
        amount_scaled_estimate: pnl_stats(&trades),
        by_source_cohort,
        by_rank_cohort,
        no_data_reasons: reasons
            .into_iter()
            .map(|(reason, events)| ReasonCount { reason, events })
            .collect(),
    }
}

fn percent(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 * 100.0 / denominator as f64)
}

fn pnl_stats(trades: &[TradeEstimate]) -> PnlStats {
    if trades.is_empty() {
        return PnlStats::default();
    }
    let gross = trades.iter().fold(0_i128, |sum, trade| {
        sum.saturating_add(trade.gross_pnl_lamports)
    });
    let net_trades = trades
        .iter()
        .filter_map(|trade| {
            trade
                .planned_net_existing_ata_lamports
                .map(|net| (trade, net))
        })
        .collect::<Vec<_>>();
    let net_values = net_trades.iter().map(|(_, net)| *net).collect::<Vec<_>>();
    let new_ata_values = trades
        .iter()
        .filter_map(|trade| trade.planned_net_new_ata_cash_lamports)
        .collect::<Vec<_>>();
    PnlStats {
        gross_events: trades.len() as u64,
        planned_net_events: net_values.len() as u64,
        wins: net_values.iter().filter(|value| **value > 0).count() as u64,
        losses_or_zero: net_values.iter().filter(|value| **value <= 0).count() as u64,
        gross_pnl_sol: Some(to_sol(gross)),
        planned_net_existing_ata_sol: sum_values(&net_values).map(to_sol),
        planned_net_new_ata_cash_sol: sum_values(&new_ata_values).map(to_sol),
        planned_net_median_sol: median(&net_values).map(to_sol),
        planned_net_ex_top3_sol: ex_top3(&net_values).map(to_sol),
        planned_net_ex_top_wallet_sol: ex_top_wallet(&net_trades).map(to_sol),
        planned_net_max_drawdown_sol: max_drawdown(&net_trades).map(to_sol),
    }
}

fn sum_values(values: &[i128]) -> Option<i128> {
    (!values.is_empty()).then(|| {
        values
            .iter()
            .fold(0_i128, |sum, value| sum.saturating_add(*value))
    })
}

fn median(values: &[i128]) -> Option<i128> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[middle])
    } else {
        Some((sorted[middle - 1] + sorted[middle]) / 2)
    }
}

fn ex_top3(values: &[i128]) -> Option<i128> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| right.cmp(left));
    Some(
        sorted
            .iter()
            .skip(3)
            .fold(0_i128, |sum, value| sum.saturating_add(*value)),
    )
}

fn ex_top_wallet(trades: &[(&TradeEstimate, i128)]) -> Option<i128> {
    if trades.is_empty() {
        return None;
    }
    let mut by_wallet: HashMap<&str, i128> = HashMap::new();
    for (trade, net) in trades {
        let current = by_wallet.entry(&trade.wallet_id).or_default();
        *current = current.saturating_add(*net);
    }
    let total = by_wallet
        .values()
        .fold(0_i128, |sum, value| sum.saturating_add(*value));
    let top = by_wallet.values().copied().max().unwrap_or(0);
    Some(total - top)
}

fn max_drawdown(trades: &[(&TradeEstimate, i128)]) -> Option<i128> {
    if trades.is_empty() {
        return None;
    }
    let mut ordered = trades.to_vec();
    ordered.sort_by_key(|(trade, _)| trade.trigger_ts);
    let mut cumulative = 0_i128;
    let mut peak = 0_i128;
    let mut drawdown = 0_i128;
    for (_, net) in ordered {
        cumulative = cumulative.saturating_add(net);
        peak = peak.max(cumulative);
        drawdown = drawdown.min(cumulative - peak);
    }
    Some(drawdown)
}

fn to_sol(lamports: i128) -> f64 {
    lamports as f64 / SOL_LAMPORTS
}
