use crate::first_sell_full_exit_report_db::EntryEvidence;
use crate::first_sell_full_exit_report_replay::{ExitTrigger, ReplayRow};
use chrono::{DateTime, Utc};

const QUOTE_OK: &str = "ok";
const WOULD_EXECUTE: &str = "would_execute";
const WOULD_FORCE_EXIT: &str = "would_force_exit";
const SHADOW_RECORDED: &str = "shadow_recorded";
const SHADOW_DROPPED: &str = "shadow_dropped";

#[derive(Debug, Clone, Copy)]
pub(crate) struct FeeAssumptions {
    pub(crate) priority_fee_cap_lamports: u64,
    pub(crate) base_fee_lamports_per_leg: u64,
    pub(crate) new_ata_cash_lamports: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EntryGateAssumptions {
    pub(crate) expected_in_lamports: u64,
    pub(crate) max_slippage_bps: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AmountEvidence {
    Exact,
    ScaledUp,
    ScaledDown,
    TerminalZero,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryValidationError {
    QuoteSkipped,
    ShadowDropped,
    ShadowPending,
    InvalidAmount,
    GateRejected,
    TimingUnknown,
}

#[derive(Debug, Clone)]
pub(crate) struct TradeEstimate {
    pub(crate) wallet_id: String,
    pub(crate) source_cohort: String,
    pub(crate) rank_cohort: String,
    pub(crate) trigger_ts: DateTime<Utc>,
    pub(crate) cross_wallet_trigger: bool,
    pub(crate) amount_evidence: AmountEvidence,
    pub(crate) gross_pnl_lamports: i128,
    pub(crate) planned_net_existing_ata_lamports: Option<i128>,
    pub(crate) planned_net_new_ata_cash_lamports: Option<i128>,
    pub(crate) missing_entry_priority_sample: bool,
    pub(crate) missing_exit_priority_sample: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum EstimateOutcome {
    SkippedEntry,
    InvalidEntryAmount,
    NoTrigger,
    NoData {
        reason: String,
        cross_wallet_trigger: bool,
        missing_entry_priority_sample: bool,
        missing_exit_priority_sample: bool,
    },
    Trade(TradeEstimate),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EntryAmounts {
    pub(crate) in_lamports: u64,
    pub(crate) out_raw: u128,
}

pub(crate) fn entry_amounts(
    entry: &EntryEvidence,
    gate: EntryGateAssumptions,
) -> Result<EntryAmounts, EntryValidationError> {
    if entry.quote_status != QUOTE_OK || entry.decision_status.as_deref() != Some(WOULD_EXECUTE) {
        return Err(EntryValidationError::QuoteSkipped);
    }
    match entry.shadow_gate_status.as_deref() {
        Some(SHADOW_RECORDED) => {}
        Some(SHADOW_DROPPED) => return Err(EntryValidationError::ShadowDropped),
        _ => return Err(EntryValidationError::ShadowPending),
    }
    if entry.copy_signal_status.as_deref() != Some(SHADOW_RECORDED) {
        return Err(EntryValidationError::ShadowPending);
    }
    let in_lamports = parse_u64(entry.quote_in_amount_raw.as_deref())
        .filter(|value| *value > 0)
        .ok_or(EntryValidationError::InvalidAmount)?;
    if in_lamports != gate.expected_in_lamports
        || entry
            .quote_price_sol
            .is_none_or(|price| !price.is_finite() || price <= 0.0)
        || entry
            .route_plan_json
            .as_deref()
            .is_none_or(|route| route.trim().is_empty())
        || quote_response_requires_fee_account(entry.quote_response_json.as_deref())
        || entry
            .slippage_bps
            .is_none_or(|slippage| !slippage.is_finite() || slippage > gate.max_slippage_bps as f64)
        || entry.priority_fee_status.as_deref() != Some(QUOTE_OK)
        || entry.priority_fee_lamports.is_none()
    {
        return Err(EntryValidationError::GateRejected);
    }
    if entry.entry_ready_ts.is_none() {
        return Err(EntryValidationError::TimingUnknown);
    }
    let out_raw = parse_u128(entry.quote_out_amount_raw.as_deref())
        .filter(|value| *value > 0)
        .ok_or(EntryValidationError::InvalidAmount)?;
    Ok(EntryAmounts {
        in_lamports,
        out_raw,
    })
}

pub(crate) fn estimate_replay_row(
    row: &ReplayRow,
    gate: EntryGateAssumptions,
    fees: FeeAssumptions,
) -> EstimateOutcome {
    let amounts = match entry_amounts(&row.entry, gate) {
        Ok(amounts) => amounts,
        Err(
            EntryValidationError::QuoteSkipped
            | EntryValidationError::ShadowDropped
            | EntryValidationError::ShadowPending
            | EntryValidationError::GateRejected
            | EntryValidationError::TimingUnknown,
        ) => return EstimateOutcome::SkippedEntry,
        Err(EntryValidationError::InvalidAmount) => return EstimateOutcome::InvalidEntryAmount,
    };
    let Some(trigger) = row.trigger.as_ref() else {
        return EstimateOutcome::NoTrigger;
    };
    let cross_wallet_trigger = trigger.wallet_id() != row.entry.wallet_id;
    let missing_entry_priority_sample = row.entry.priority_fee_lamports.is_none();
    let missing_exit_priority_sample = trigger
        .quote()
        .and_then(|quote| quote.priority_fee_lamports)
        .is_none();
    let (exit_lamports, amount_evidence) = match exit_value(trigger, amounts.out_raw) {
        Ok(value) => value,
        Err(reason) => {
            return EstimateOutcome::NoData {
                reason,
                cross_wallet_trigger,
                missing_entry_priority_sample,
                missing_exit_priority_sample,
            }
        }
    };
    let Some(exit_lamports) = i128::try_from(exit_lamports).ok() else {
        return EstimateOutcome::NoData {
            reason: "scaled_exit_overflow".to_string(),
            cross_wallet_trigger,
            missing_entry_priority_sample,
            missing_exit_priority_sample,
        };
    };
    let gross_pnl_lamports = exit_lamports - i128::from(amounts.in_lamports);
    let entry_priority = modeled_priority_fee(
        row.entry.priority_fee_lamports,
        fees.priority_fee_cap_lamports,
    );
    let exit_priority = modeled_priority_fee(
        trigger
            .quote()
            .and_then(|quote| quote.priority_fee_lamports),
        fees.priority_fee_cap_lamports,
    );
    let planned_net_existing_ata_lamports = modeled_net(
        gross_pnl_lamports,
        entry_priority,
        exit_priority,
        fees.base_fee_lamports_per_leg,
        0,
    );
    let planned_net_new_ata_cash_lamports = modeled_net(
        gross_pnl_lamports,
        entry_priority,
        exit_priority,
        fees.base_fee_lamports_per_leg,
        fees.new_ata_cash_lamports,
    );
    EstimateOutcome::Trade(TradeEstimate {
        wallet_id: row.entry.wallet_id.clone(),
        source_cohort: row
            .entry
            .source_cohort
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        rank_cohort: rank_cohort(row.entry.discovery_rank),
        trigger_ts: trigger.ts(),
        cross_wallet_trigger,
        amount_evidence,
        gross_pnl_lamports,
        planned_net_existing_ata_lamports,
        planned_net_new_ata_cash_lamports,
        missing_entry_priority_sample,
        missing_exit_priority_sample,
    })
}

fn exit_value(
    trigger: &ExitTrigger,
    entry_out_raw: u128,
) -> Result<(u128, AmountEvidence), String> {
    if let Some(quote) = trigger.quote() {
        if quote.quote_status != QUOTE_OK {
            return Err(if non_landing_exit_error(quote.error.as_deref()) {
                "first_exit_unlanded_position_remains_open".to_string()
            } else {
                "first_exit_quote_error".to_string()
            });
        }
        if quote
            .quote_price_sol
            .is_none_or(|price| !price.is_finite() || price <= 0.0)
            || quote
                .route_plan_json
                .as_deref()
                .is_none_or(|route| route.trim().is_empty())
            || quote.priority_fee_status.as_deref() != Some(QUOTE_OK)
            || quote.priority_fee_lamports.is_none()
            || !matches!(
                quote.decision_status.as_deref(),
                Some(WOULD_EXECUTE | WOULD_FORCE_EXIT)
            )
        {
            return Err("first_exit_quote_not_daemon_buildable".to_string());
        }
        let sell_in = parse_u128(quote.quote_in_amount_raw.as_deref())
            .filter(|value| *value > 0)
            .ok_or_else(|| "missing_exit_quote_in_amount".to_string())?;
        let sell_out = parse_u128(quote.quote_out_amount_raw.as_deref())
            .ok_or_else(|| "missing_exit_quote_out_amount".to_string())?;
        return scaled_exit(sell_in, sell_out, entry_out_raw);
    }
    match trigger {
        ExitTrigger::ShadowClose { close, .. }
            if matches!(
                close.close_context.as_str(),
                "stale_terminal_zero_price" | "recovery_terminal_zero_price"
            ) =>
        {
            Ok((0, AmountEvidence::TerminalZero))
        }
        ExitTrigger::ShadowClose { close, .. } if close.close_context == "stale_quote_price" => {
            Err("missing_daemon_buildable_stale_exit_quote".to_string())
        }
        ExitTrigger::ShadowClose { close, .. } => {
            Err(format!("unsupported_close_context:{}", close.close_context))
        }
        ExitTrigger::SellSignal { .. } => Err("missing_first_sell_quote".to_string()),
    }
}

fn scaled_exit(
    quoted_in_raw: u128,
    quoted_out_lamports: u128,
    entry_out_raw: u128,
) -> Result<(u128, AmountEvidence), String> {
    let amount_evidence = match entry_out_raw.cmp(&quoted_in_raw) {
        std::cmp::Ordering::Equal => AmountEvidence::Exact,
        std::cmp::Ordering::Greater => AmountEvidence::ScaledUp,
        std::cmp::Ordering::Less => AmountEvidence::ScaledDown,
    };
    let exit = quoted_out_lamports
        .checked_mul(entry_out_raw)
        .ok_or_else(|| "scaled_exit_multiply_overflow".to_string())?
        / quoted_in_raw;
    Ok((exit, amount_evidence))
}

fn modeled_priority_fee(sample: Option<u64>, cap: u64) -> Option<u64> {
    match (sample, cap) {
        (Some(sample), 0) => Some(sample),
        (Some(sample), cap) => Some(sample.min(cap)),
        (None, 0) => None,
        (None, cap) => Some(cap),
    }
}

fn modeled_net(
    gross: i128,
    entry_priority: Option<u64>,
    exit_priority: Option<u64>,
    base_fee_per_leg: u64,
    ata_cash: u64,
) -> Option<i128> {
    let fees = entry_priority?
        .checked_add(exit_priority?)?
        .checked_add(base_fee_per_leg.checked_mul(2)?)?
        .checked_add(ata_cash)?;
    Some(gross - i128::from(fees))
}

fn parse_u64(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse().ok()
}

fn parse_u128(raw: Option<&str>) -> Option<u128> {
    raw?.trim().parse().ok()
}

fn non_landing_exit_error(error: Option<&str>) -> bool {
    let Some(error) = error else {
        return false;
    };
    let lower = error.to_ascii_lowercase();
    lower.contains("token_not_tradable")
        || lower.contains("not tradable")
        || lower.contains("no_routes")
        || lower.contains("no route")
        || lower.contains("no_route")
}

fn quote_response_requires_fee_account(raw: Option<&str>) -> bool {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| value.get("platformFee").cloned())
        .is_some_and(|fee| !fee.is_null())
}

pub(crate) fn rank_cohort(rank: Option<u64>) -> String {
    match rank {
        Some(1..=15) => "rank_1_15",
        Some(16..=30) => "rank_16_30",
        Some(_) => "rank_gt_30",
        None => "unranked",
    }
    .to_string()
}
