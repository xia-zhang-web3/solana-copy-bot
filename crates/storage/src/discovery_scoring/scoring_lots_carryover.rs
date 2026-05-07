use super::*;

pub(super) fn load_wallet_scoring_carryover_lot_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<Option<CarryoverLotRow>> {
    let row: Option<(f64, f64, String)> = conn
        .query_row(
            "SELECT qty, cost_sol, oldest_opened_ts
             FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![wallet_id, token],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed querying wallet_scoring_carryover_lots")?;
    row.map(|(qty, cost_sol, oldest_opened_ts_raw)| {
        Ok(CarryoverLotRow {
            qty,
            cost_sol,
            oldest_opened_ts: parse_ts(
                &oldest_opened_ts_raw,
                "wallet_scoring_carryover_lots.oldest_opened_ts",
            )?,
        })
    })
    .transpose()
}

pub(super) fn apply_wallet_scoring_carryover_sell_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
    segment_index: i64,
) -> Result<i64> {
    let token = swap.token_in.as_str();
    let Some(carryover) = load_wallet_scoring_carryover_lot_on_conn(conn, &swap.wallet, token)?
    else {
        return Ok(0);
    };
    let qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || carryover.qty <= 1e-12 {
        return Ok(0);
    }

    let take_qty = qty_remaining.min(carryover.qty);
    let lot_fraction = take_qty / carryover.qty;
    let cost_part = carryover.cost_sol * lot_fraction;
    let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
    let pnl_sol = proceeds_part - cost_part;
    let remaining_qty = (carryover.qty - take_qty).max(0.0);
    let remaining_cost = (carryover.cost_sol - cost_part).max(0.0);

    if remaining_qty <= 1e-12 {
        conn.execute(
            "DELETE FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token],
        )
        .context("failed deleting consumed wallet_scoring_carryover_lot")?;
    } else {
        conn.execute(
            "UPDATE wallet_scoring_carryover_lots
             SET qty = ?3, cost_sol = ?4
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token, remaining_qty, remaining_cost],
        )
        .context("failed updating partially consumed wallet_scoring_carryover_lot")?;
    }

    let hold_seconds = (swap.ts_utc - carryover.oldest_opened_ts)
        .num_seconds()
        .max(0);
    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_close_facts(
            sell_signature,
            segment_index,
            wallet_id,
            token,
            closed_ts,
            activity_day,
            pnl_sol,
            hold_seconds,
            win
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            &swap.signature,
            segment_index,
            &swap.wallet,
            token,
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            pnl_sol,
            hold_seconds,
            if pnl_sol > 0.0 { 1 } else { 0 },
        ],
    )
    .context("failed inserting wallet_scoring_close_facts row for carryover lot")?;

    Ok(1)
}

pub(super) fn apply_wallet_scoring_carryover_sell_lot_only_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
) -> Result<()> {
    let token = swap.token_in.as_str();
    let Some(carryover) = load_wallet_scoring_carryover_lot_on_conn(conn, &swap.wallet, token)?
    else {
        return Ok(());
    };
    let qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || carryover.qty <= 1e-12 {
        return Ok(());
    }

    let take_qty = qty_remaining.min(carryover.qty);
    let lot_fraction = take_qty / carryover.qty;
    let cost_part = carryover.cost_sol * lot_fraction;
    let remaining_qty = (carryover.qty - take_qty).max(0.0);
    let remaining_cost = (carryover.cost_sol - cost_part).max(0.0);

    if remaining_qty <= 1e-12 {
        conn.execute(
            "DELETE FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token],
        )
        .context("failed deleting consumed wallet_scoring_carryover_lot")?;
    } else {
        conn.execute(
            "UPDATE wallet_scoring_carryover_lots
             SET qty = ?3, cost_sol = ?4
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token, remaining_qty, remaining_cost],
        )
        .context("failed updating partially consumed wallet_scoring_carryover_lot")?;
    }

    Ok(())
}
