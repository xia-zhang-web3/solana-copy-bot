use super::*;

impl SqliteStore {
    pub fn update_shadow_lot(&self, id: i64, qty: f64, cost_sol: f64) -> Result<()> {
        self.update_shadow_lot_exact(id, qty, None, cost_sol)
    }

    pub fn update_shadow_lot_exact(
        &self,
        id: i64,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
    ) -> Result<()> {
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "update shadow lot")?;
        self.execute_with_retry(|conn| {
            let cost_lamports = sol_to_lamports_ceil_storage(cost_sol, "shadow lot cost_sol")
                .map_err(to_sql_conversion_error)?;
            conn.execute(
                "UPDATE shadow_lots SET qty = ?1, qty_raw = ?2, qty_decimals = ?3, cost_sol = ?4, cost_lamports = ?5 WHERE id = ?6",
                params![
                    qty,
                    qty_exact.as_ref().map(|value| value.raw().to_string()),
                    qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                    cost_sol,
                    u64_to_sql_i64("shadow_lots.cost_lamports", cost_lamports.as_u64())
                        .map_err(to_sql_conversion_error)?,
                    id
                ],
            )
        })
        .context("failed to update shadow lot")?;
        Ok(())
    }

    pub fn delete_shadow_lot(&self, id: i64) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute("DELETE FROM shadow_lots WHERE id = ?1", params![id])
        })
        .context("failed to delete shadow lot")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_closed_trade(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        self.insert_shadow_closed_trade_exact(
            signal_id,
            wallet_id,
            token,
            qty,
            None,
            entry_cost_sol,
            exit_value_sol,
            pnl_sol,
            opened_ts,
            closed_ts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_closed_trade_exact(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        self.insert_shadow_closed_trade_exact_with_context(
            signal_id,
            wallet_id,
            token,
            qty,
            qty_exact,
            entry_cost_sol,
            exit_value_sol,
            pnl_sol,
            SHADOW_CLOSE_CONTEXT_MARKET,
            opened_ts,
            closed_ts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_closed_trade_exact_with_context(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        close_context: &str,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "insert shadow closed trade")?;
        self.execute_with_retry(|conn| {
            let entry_cost_lamports =
                sol_to_lamports_ceil_storage(entry_cost_sol, "shadow closed trade entry_cost_sol")
                    .map_err(to_sql_conversion_error)?;
            let exit_value_lamports =
                sol_to_lamports_floor_storage(exit_value_sol, "shadow closed trade exit_value_sol")
                    .map_err(to_sql_conversion_error)?;
            let pnl_lamports = crate::SignedLamports::new(
                i128::from(exit_value_lamports.as_u64()) - i128::from(entry_cost_lamports.as_u64()),
            );
            let accounting_bucket = shadow_accounting_bucket_for_qty_exact(qty_exact);
            conn.execute(
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
                    close_context,
                    accounting_bucket,
                    qty,
                    qty_exact.as_ref().map(|value| value.raw().to_string()),
                    qty_exact.as_ref().map(|value| i64::from(value.decimals())),
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
                    signed_lamports_to_sql_i64("shadow_closed_trades.pnl_lamports", pnl_lamports)
                        .map_err(to_sql_conversion_error)?,
                    opened_ts.to_rfc3339(),
                    closed_ts.to_rfc3339(),
                ],
            )
        })
        .context("failed to insert shadow closed trade")?;
        Ok(())
    }

    pub fn shadow_closed_trade_qty_exact(&self, signal_id: &str) -> Result<Option<TokenQuantity>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT qty_raw, qty_decimals
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1",
            )
            .context("failed to prepare shadow closed trade exact qty query")?;
        let mut rows = stmt
            .query(params![signal_id])
            .context("failed to query shadow closed trade exact qty")?;
        let Some(row) = rows
            .next()
            .context("failed to read shadow closed trade exact qty row")?
        else {
            return Ok(None);
        };

        token_quantity_from_sql(
            row.get(0)
                .context("failed reading shadow_closed_trades.qty_raw")?,
            row.get(1)
                .context("failed reading shadow_closed_trades.qty_decimals")?,
            "shadow_closed_trades",
        )
    }

    pub fn shadow_closed_trade_accounting_bucket(&self, signal_id: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT accounting_bucket
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                 LIMIT 1",
                params![signal_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed reading shadow closed trade accounting bucket")
    }

    pub fn shadow_closed_trade_close_context(&self, signal_id: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT close_context
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                 LIMIT 1",
                params![signal_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed reading shadow closed trade close context")
    }
}
