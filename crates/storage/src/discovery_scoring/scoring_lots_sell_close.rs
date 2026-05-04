fn load_wallet_scoring_open_lots_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<Vec<OpenLotRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             WHERE wallet_id = ?1
               AND token = ?2
             ORDER BY opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots query")?;
    let mut rows = stmt
        .query(params![wallet_id, token])
        .context("failed querying wallet_scoring_open_lots")?;
    let mut lots = Vec::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots rows")?
    {
        let opened_ts_raw: String = row
            .get(3)
            .context("failed reading wallet_scoring_open_lots.opened_ts")?;
        lots.push(OpenLotRow {
            buy_signature: row
                .get(0)
                .context("failed reading wallet_scoring_open_lots.buy_signature")?,
            qty: row
                .get(1)
                .context("failed reading wallet_scoring_open_lots.qty")?,
            cost_sol: row
                .get(2)
                .context("failed reading wallet_scoring_open_lots.cost_sol")?,
            opened_ts: parse_ts(&opened_ts_raw, "wallet_scoring_open_lots.opened_ts")?,
        });
    }
    Ok(lots)
}

fn apply_wallet_scoring_sell_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let token = swap.token_in.as_str();
    let lots = load_wallet_scoring_open_lots_on_conn(conn, &swap.wallet, token)?;
    if lots.is_empty() {
        let _ = apply_wallet_scoring_carryover_sell_on_conn(conn, swap, 0)?;
        return Ok(());
    }

    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        return Ok(());
    }

    let mut segment_index = 0i64;
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
        let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
        let pnl_sol = proceeds_part - cost_part;
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

        let hold_seconds = (swap.ts_utc - lot.opened_ts).num_seconds().max(0);
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
        .context("failed inserting wallet_scoring_close_facts row")?;

        qty_remaining -= take_qty;
        segment_index += 1;
    }

    if qty_remaining > 1e-12 {
        let carryover_sell = SwapEvent {
            amount_in: qty_remaining,
            ..swap.clone()
        };
        let _ = apply_wallet_scoring_carryover_sell_on_conn(conn, &carryover_sell, segment_index)?;
    }

    Ok(())
}
