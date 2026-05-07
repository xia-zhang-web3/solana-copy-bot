use super::*;

impl SqliteStore {
    pub(crate) fn close_shadow_lots_fifo_atomic_once(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> rusqlite::Result<ShadowCloseOutcome> {
        const EPS: f64 = SHADOW_LOT_OPEN_EPS;

        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let close_result = (|| -> rusqlite::Result<ShadowCloseOutcome> {
            let mut qty_remaining = target_qty;
            let mut qty_remaining_exact = target_qty_exact;
            let mut closed_qty = 0.0;
            let mut realized_pnl_sol = 0.0;

            let mut stmt = self.conn.prepare(
                "SELECT id, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts, accounting_bucket, risk_context
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )?;
            let mut rows = stmt.query(params![wallet_id, token])?;
            let mut lots: Vec<(
                i64,
                f64,
                Option<String>,
                Option<i64>,
                f64,
                Option<i64>,
                String,
                String,
                String,
            )> = Vec::new();
            while let Some(row) = rows.next()? {
                lots.push((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ));
            }
            drop(rows);
            drop(stmt);

            for (
                lot_id,
                lot_qty,
                lot_qty_raw,
                lot_qty_decimals,
                lot_cost_sol,
                lot_cost_lamports_raw,
                lot_opened_ts,
                lot_accounting_bucket,
                lot_risk_context,
            ) in lots
            {
                if qty_remaining <= EPS {
                    break;
                }

                if lot_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                    continue;
                }

                let take_qty = qty_remaining.min(lot_qty);
                let final_segment = (qty_remaining - take_qty) <= EPS;
                let current_qty_exact = token_quantity_from_sql(
                    lot_qty_raw,
                    lot_qty_decimals,
                    "shadow fifo close lot qty",
                )
                .map_err(to_sql_conversion_error)?;
                let (segment_qty_exact, next_remaining_qty_exact) = match qty_remaining_exact {
                    Some(total_exact) => split_token_quantity_pro_rata(
                        total_exact,
                        take_qty,
                        qty_remaining,
                        final_segment,
                        "shadow fifo close qty_exact",
                    )
                    .map_err(to_sql_conversion_error)?,
                    None => (None, None),
                };
                let segment_qty_exact =
                    reject_zero_raw_exact_qty(segment_qty_exact, "shadow fifo close segment qty")
                        .map_err(to_sql_conversion_error)?;
                let closed_qty_exact = match (current_qty_exact, segment_qty_exact) {
                    (Some(current), Some(segment)) if current.decimals() == segment.decimals() => {
                        let raw = current.raw().min(segment.raw());
                        if raw == 0 {
                            None
                        } else {
                            Some(TokenQuantity::new(raw, current.decimals()))
                        }
                    }
                    _ => None,
                };
                let entry_cost_sol = lot_cost_sol * (take_qty / lot_qty);
                let remaining_qty = (lot_qty - take_qty).max(0.0);
                let remaining_cost = (lot_cost_sol - entry_cost_sol).max(0.0);
                let lot_cost_lamports = shadow_lot_cost_lamports(
                    lot_cost_sol,
                    lot_cost_lamports_raw,
                    "shadow fifo close lot",
                )
                .map_err(to_sql_conversion_error)?;
                let entry_cost_lamports = if remaining_qty <= EPS {
                    lot_cost_lamports
                } else {
                    let estimated = sol_to_lamports_ceil_storage(
                        entry_cost_sol,
                        "shadow closed trade entry_cost_sol",
                    )
                    .map_err(to_sql_conversion_error)?;
                    if estimated > lot_cost_lamports {
                        lot_cost_lamports
                    } else {
                        estimated
                    }
                };
                let remaining_cost_lamports = lot_cost_lamports
                    .checked_sub(entry_cost_lamports)
                    .ok_or_else(|| {
                        to_sql_conversion_error(anyhow!(
                            "shadow lot lamport cost underflow after close"
                        ))
                    })?;
                let remaining_qty_exact = merge_position_qty_exact_on_sell(
                    current_qty_exact,
                    closed_qty_exact,
                    remaining_qty <= EPS,
                )
                .map_err(to_sql_conversion_error)?;

                if remaining_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                } else {
                    let remaining_qty_exact = reject_zero_raw_exact_qty(
                        remaining_qty_exact,
                        "shadow fifo close remaining qty",
                    )
                    .map_err(to_sql_conversion_error)?;
                    self.conn.execute(
                        "UPDATE shadow_lots SET qty = ?1, qty_raw = ?2, qty_decimals = ?3, cost_sol = ?4, cost_lamports = ?5 WHERE id = ?6",
                        params![
                            remaining_qty,
                            remaining_qty_exact
                                .as_ref()
                                .map(|value| value.raw().to_string()),
                            remaining_qty_exact
                                .as_ref()
                                .map(|value| i64::from(value.decimals())),
                            remaining_cost,
                            u64_to_sql_i64(
                                "shadow_lots.cost_lamports",
                                remaining_cost_lamports.as_u64()
                            )
                            .map_err(to_sql_conversion_error)?,
                            lot_id
                        ],
                    )?;
                }

                let exit_value_sol = take_qty * exit_price_sol;
                let exit_value_lamports = sol_to_lamports_floor_storage(
                    exit_value_sol,
                    "shadow closed trade exit_value_sol",
                )
                .map_err(to_sql_conversion_error)?;
                let effective_close_context = if close_context == SHADOW_CLOSE_CONTEXT_MARKET
                    && lot_risk_context == SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY
                {
                    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY
                } else {
                    close_context
                };
                let pnl_lamports = crate::SignedLamports::new(
                    i128::from(exit_value_lamports.as_u64())
                        - i128::from(entry_cost_lamports.as_u64()),
                );
                let pnl_sol = exit_value_sol - entry_cost_sol;
                self.conn.execute(
                    "INSERT INTO shadow_closed_trades(
                        signal_id,
                        wallet_id,
                        token,
                        close_context,
                        accounting_bucket,
                        qty,
                        qty_raw,
                        qty_decimals,
                        entry_cost_sol,
                        entry_cost_lamports,
                        exit_value_sol,
                        exit_value_lamports,
                        pnl_sol,
                        pnl_lamports,
                        opened_ts,
                        closed_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                    params![
                        signal_id,
                        wallet_id,
                        token,
                        effective_close_context,
                        lot_accounting_bucket,
                        take_qty,
                        closed_qty_exact
                            .as_ref()
                            .map(|value| value.raw().to_string()),
                        closed_qty_exact
                            .as_ref()
                            .map(|value| i64::from(value.decimals())),
                        entry_cost_sol,
                        u64_to_sql_i64(
                            "shadow_closed_trades.entry_cost_lamports",
                            entry_cost_lamports.as_u64()
                        )
                        .map_err(to_sql_conversion_error)?,
                        exit_value_sol,
                        u64_to_sql_i64(
                            "shadow_closed_trades.exit_value_lamports",
                            exit_value_lamports.as_u64()
                        )
                        .map_err(to_sql_conversion_error)?,
                        pnl_sol,
                        signed_lamports_to_sql_i64(
                            "shadow_closed_trades.pnl_lamports",
                            pnl_lamports
                        )
                        .map_err(to_sql_conversion_error)?,
                        lot_opened_ts,
                        closed_ts.to_rfc3339(),
                    ],
                )?;

                qty_remaining -= take_qty;
                qty_remaining_exact = next_remaining_qty_exact;
                closed_qty += take_qty;
                realized_pnl_sol += signed_lamports_to_sol(pnl_lamports);
            }

            let remaining_lots: i64 = self.conn.query_row(
                "SELECT COUNT(*)
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2 AND qty > ?3",
                params![wallet_id, token, SHADOW_LOT_OPEN_EPS],
                |row| row.get(0),
            )?;

            Ok(ShadowCloseOutcome {
                closed_qty,
                realized_pnl_sol,
                has_open_lots_after: remaining_lots > 0,
            })
        })();

        match close_result {
            Ok(outcome) => match self.conn.execute_batch("COMMIT") {
                Ok(()) => Ok(outcome),
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    Err(error)
                }
            },
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}
