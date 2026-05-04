fn apply_wallet_scoring_sell_lot_only_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let token = swap.token_in.as_str();
    let lots = load_wallet_scoring_open_lots_on_conn(conn, &swap.wallet, token)?;
    if lots.is_empty() {
        apply_wallet_scoring_carryover_sell_lot_only_on_conn(conn, swap)?;
        return Ok(());
    }

    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        return Ok(());
    }

    for lot in lots {
        if qty_remaining <= 1e-12 {
            break;
        }
        if lot.qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting empty wallet_scoring_open_lot")?;
            continue;
        }

        let take_qty = qty_remaining.min(lot.qty);
        let lot_fraction = take_qty / lot.qty;
        let cost_part = lot.cost_sol * lot_fraction;
        let remaining_qty = (lot.qty - take_qty).max(0.0);
        let remaining_cost = (lot.cost_sol - cost_part).max(0.0);

        if remaining_qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting consumed wallet_scoring_open_lot")?;
        } else {
            conn.execute(
                "UPDATE wallet_scoring_open_lots
                 SET qty = ?2, cost_sol = ?3
                 WHERE buy_signature = ?1",
                params![&lot.buy_signature, remaining_qty, remaining_cost],
            )
            .context("failed updating partially consumed wallet_scoring_open_lot")?;
        }

        qty_remaining -= take_qty;
    }

    if qty_remaining > 1e-12 {
        let carryover_sell = SwapEvent {
            amount_in: qty_remaining,
            ..swap.clone()
        };
        apply_wallet_scoring_carryover_sell_lot_only_on_conn(conn, &carryover_sell)?;
    }

    Ok(())
}
