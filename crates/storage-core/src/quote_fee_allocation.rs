use crate::execution_canary_quote_pnl_rows::QuotePnlRow;

pub(crate) fn raw(value: Option<&str>) -> Option<u128> {
    value
        .filter(|v| !v.is_empty() && v.bytes().all(|c| c.is_ascii_digit()))?
        .parse()
        .ok()
}

/// Each group is one uniquely inferred quoted BUY event. Retained history is
/// processed in (closed instant, persisted close ID) order, before output filters.
/// Increment = ceil(F * cumulative_q / Q) - prior_ceiling. Full exit takes F's
/// remainder. Every operation is checked; an uncertain quantity poisons the suffix.
pub(crate) fn allocate(rows: &mut [QuotePnlRow]) {
    let mut consumed = 0u128;
    let mut charged = 0u64;
    let mut gap = None;
    for row in rows {
        let a = &mut row.allocation;
        a.entry_attribution = row
            .buy
            .event_id
            .as_ref()
            .filter(|_| row.entry_attributed)
            .map(|_| "unique_entry_timestamp_inference".into());
        a.observed_buy_fee_lamports = row.buy.priority_fee_lamports.map(|v| v.to_string());
        a.observed_sell_fee_lamports = row.sell.priority_fee_lamports.map(|v| v.to_string());
        let entry = raw(row.buy.quote_out_amount_raw.as_deref()).filter(|v| *v > 0);
        let exit = raw(row.sell.quote_in_amount_raw.as_deref()).filter(|v| *v > 0);
        a.entry_quantity_raw = entry.map(|v| v.to_string());
        a.exit_quantity_raw = exit.map(|v| v.to_string());
        let current_error = row.binding_error.or_else(|| {
            if row.buy.quote_status.as_deref() != Some("ok")
                || row.sell.quote_status.as_deref() != Some("ok")
            {
                Some("quote_quantity_unproven")
            } else if !matches!(
                row.buy.decision_status.as_deref(),
                Some("would_execute" | "would_skip")
            ) || !matches!(
                row.sell.decision_status.as_deref(),
                Some("would_execute" | "would_force_exit")
            ) {
                Some("exit_quantity_decision_unproven")
            } else {
                None
            }
        });
        gap = gap.or(current_error);
        if let Some(reason) = gap {
            a.reason = reason.into();
            continue;
        }
        let (Some(entry), Some(exit)) = (entry, exit) else {
            gap = Some("invalid_quote_quantity");
            a.reason = gap.unwrap().into();
            continue;
        };
        let Some(next) = consumed.checked_add(exit).filter(|v| *v <= entry) else {
            gap = Some("quote_quantity_over_close");
            a.reason = gap.unwrap().into();
            continue;
        };
        // SOL raw amounts are required for this quantity basis too. They are not
        // reconstructed from shadow quantity, UI price or floating point values.
        if raw(row.buy.quote_in_amount_raw.as_deref())
            .filter(|v| *v > 0)
            .is_none()
            || raw(row.sell.quote_out_amount_raw.as_deref()).is_none()
        {
            gap = Some("invalid_quote_amount");
            a.reason = gap.unwrap().into();
            continue;
        }
        consumed = next;
        a.cumulative_exit_quantity_raw = Some(consumed.to_string());
        let Some(fee) = row.buy.priority_fee_lamports else {
            a.reason = "priority_fee_total_unknown".into();
            continue;
        };
        let ceiling = if consumed == entry {
            Some(fee)
        } else {
            u128::from(fee)
                .checked_mul(consumed)
                .and_then(|product| (product / entry).checked_add(u128::from(product % entry != 0)))
                .and_then(|v| u64::try_from(v).ok())
        };
        let Some(total) = ceiling.filter(|v| *v >= charged && *v <= fee) else {
            gap = Some("quote_fee_allocation_overflow");
            a.reason = gap.unwrap().into();
            continue;
        };
        a.buy_fee_allocated_lamports = Some((total - charged).to_string());
        a.buy_fee_remaining_lamports = Some((fee - total).to_string());
        a.reason = if row
            .sell
            .priority_fee_lamports
            .and_then(|sell| (total - charged).checked_add(sell))
            .is_some()
        {
            "known_retained_history"
        } else {
            "priority_fee_total_unknown"
        }
        .into();
        charged = total;
    }
}
