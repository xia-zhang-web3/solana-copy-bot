use super::*;

impl SqliteStore {
    pub fn close_shadow_lots_fifo_atomic(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        self.close_shadow_lots_fifo_atomic_with_context(
            signal_id,
            wallet_id,
            token,
            target_qty,
            exit_price_sol,
            SHADOW_CLOSE_CONTEXT_MARKET,
            closed_ts,
        )
    }

    pub fn close_shadow_lots_fifo_atomic_with_context(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        self.close_shadow_lots_fifo_atomic_exact_with_context(
            signal_id,
            wallet_id,
            token,
            target_qty,
            None,
            exit_price_sol,
            close_context,
            closed_ts,
        )
    }

    pub fn close_shadow_lots_fifo_atomic_exact(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        self.close_shadow_lots_fifo_atomic_exact_with_context(
            signal_id,
            wallet_id,
            token,
            target_qty,
            target_qty_exact,
            exit_price_sol,
            SHADOW_CLOSE_CONTEXT_MARKET,
            closed_ts,
        )
    }

    pub fn close_shadow_lots_fifo_atomic_exact_with_context(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        const EPS: f64 = SHADOW_LOT_OPEN_EPS;
        let target_qty_exact =
            reject_zero_raw_exact_qty(target_qty_exact, "shadow fifo close target qty")?;

        if target_qty <= EPS {
            return Ok(ShadowCloseOutcome {
                has_open_lots_after: self.has_shadow_lots(wallet_id, token)?,
                ..ShadowCloseOutcome::default()
            });
        }

        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            match self.close_shadow_lots_fifo_atomic_once(
                signal_id,
                wallet_id,
                token,
                target_qty,
                target_qty_exact,
                exit_price_sol,
                close_context,
                closed_ts,
            ) {
                Ok(outcome) => return Ok(outcome),
                Err(error) => {
                    let retryable = is_retryable_sqlite_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        note_sqlite_write_retry();
                        std::thread::sleep(StdDuration::from_millis(
                            SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                        ));
                        continue;
                    }
                    return Err(error).context("failed to close shadow fifo lots atomically");
                }
            }
        }

        unreachable!("retry loop must return on success or terminal error");
    }
}
