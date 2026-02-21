use crate::intent::{ExecutionIntent, ExecutionSide};
use crate::ExecutionRuntime;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage::SqliteStore;
use tracing::debug;

impl ExecutionRuntime {
    pub(crate) fn should_pause_buy_submission(
        &self,
        intent: &ExecutionIntent,
        lifecycle_status: &str,
        buy_submit_pause_reason: Option<&str>,
    ) -> bool {
        if !matches!(intent.side, ExecutionSide::Buy) {
            return false;
        }
        if !self.is_pre_submit_status(lifecycle_status) {
            return false;
        }
        if let Some(reason) = buy_submit_pause_reason {
            debug!(
                signal_id = %intent.signal_id,
                token = %intent.token,
                reason,
                "buy execution paused by runtime gate"
            );
            return true;
        }
        false
    }

    pub(crate) fn is_pre_submit_status(&self, status: &str) -> bool {
        matches!(
            status,
            "shadow_recorded" | "execution_pending" | "execution_simulated"
        )
    }

    pub(crate) fn execution_risk_block_reason(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        now: DateTime<Utc>,
    ) -> Result<Option<String>> {
        match intent.side {
            ExecutionSide::Sell => {
                if !store.live_has_open_position(&intent.token)? {
                    return Ok(Some(format!(
                        "sell_without_open_position token={}",
                        intent.token
                    )));
                }
                Ok(None)
            }
            ExecutionSide::Buy => {
                if intent.notional_sol > self.risk.max_position_sol {
                    return Ok(Some(format!(
                        "max_position_sol_exceeded notional_sol={:.6} max_position_sol={:.6}",
                        intent.notional_sol, self.risk.max_position_sol
                    )));
                }

                let current_exposure = store.live_open_exposure_sol()?;
                if current_exposure + intent.notional_sol > self.risk.max_total_exposure_sol {
                    return Ok(Some(format!(
                        "max_total_exposure_exceeded current={:.6} notional={:.6} max_total={:.6}",
                        current_exposure, intent.notional_sol, self.risk.max_total_exposure_sol
                    )));
                }

                let current_token_exposure =
                    store.live_open_exposure_sol_for_token(&intent.token)?;
                if current_token_exposure + intent.notional_sol
                    > self.risk.max_exposure_per_token_sol
                {
                    return Ok(Some(format!(
                        "max_exposure_per_token_exceeded token={} current={:.6} notional={:.6} max_token={:.6}",
                        intent.token,
                        current_token_exposure,
                        intent.notional_sol,
                        self.risk.max_exposure_per_token_sol
                    )));
                }

                let has_open = store.live_has_open_position(&intent.token)?;
                let open_count = store.live_open_positions_count()?;
                if !has_open && open_count >= self.risk.max_concurrent_positions as u64 {
                    return Ok(Some(format!(
                        "max_concurrent_positions_exceeded open_count={} max={}",
                        open_count, self.risk.max_concurrent_positions
                    )));
                }

                let daily_loss_limit_sol = self.daily_loss_limit_sol();
                let max_drawdown_limit_sol = self.max_drawdown_limit_sol();
                let (unrealized_pnl_sol, unrealized_missing_price_count) =
                    if daily_loss_limit_sol.is_some() || max_drawdown_limit_sol.is_some() {
                        store.live_unrealized_pnl_sol(now)?
                    } else {
                        (0.0, 0)
                    };
                if unrealized_missing_price_count > 0 {
                    return Ok(Some(format!(
                        "unrealized_price_unavailable unrealized_missing_price_count={} as_of={}",
                        unrealized_missing_price_count,
                        now.to_rfc3339()
                    )));
                }

                if let Some(daily_loss_limit_sol) = daily_loss_limit_sol {
                    let loss_window_start = now - Duration::hours(24);
                    let (_, realized_pnl_24h) = store.live_realized_pnl_since(loss_window_start)?;
                    let pnl_24h = realized_pnl_24h + unrealized_pnl_sol;
                    if pnl_24h <= -daily_loss_limit_sol {
                        return Ok(Some(format!(
                            "daily_loss_limit_exceeded pnl_24h={:.6} realized_pnl_24h={:.6} unrealized_pnl={:.6} unrealized_missing_price_count={} loss_limit_sol={:.6} loss_limit_pct={:.4} window_start={}",
                            pnl_24h,
                            realized_pnl_24h,
                            unrealized_pnl_sol,
                            unrealized_missing_price_count,
                            daily_loss_limit_sol,
                            self.risk.daily_loss_limit_pct,
                            loss_window_start.to_rfc3339()
                        )));
                    }
                }

                if let Some(max_drawdown_limit_sol) = max_drawdown_limit_sol {
                    let drawdown_window_start = now - Duration::hours(24);
                    let max_drawdown_sol = store.live_max_drawdown_with_unrealized_since(
                        drawdown_window_start,
                        unrealized_pnl_sol,
                    )?;
                    if max_drawdown_sol >= max_drawdown_limit_sol {
                        return Ok(Some(format!(
                            "max_drawdown_exceeded max_drawdown_sol={:.6} unrealized_missing_price_count={} drawdown_limit_sol={:.6} drawdown_limit_pct={:.4} window_start={}",
                            max_drawdown_sol,
                            unrealized_missing_price_count,
                            max_drawdown_limit_sol,
                            self.risk.max_drawdown_pct,
                            drawdown_window_start.to_rfc3339()
                        )));
                    }
                }

                Ok(None)
            }
        }
    }

    pub(crate) fn daily_loss_limit_sol(&self) -> Option<f64> {
        if !self.risk.daily_loss_limit_pct.is_finite() || self.risk.daily_loss_limit_pct <= 0.0 {
            return None;
        }
        Some(self.risk.max_total_exposure_sol * (self.risk.daily_loss_limit_pct / 100.0))
    }

    pub(crate) fn max_drawdown_limit_sol(&self) -> Option<f64> {
        if !self.risk.max_drawdown_pct.is_finite() || self.risk.max_drawdown_pct <= 0.0 {
            return None;
        }
        Some(self.risk.max_total_exposure_sol * (self.risk.max_drawdown_pct / 100.0))
    }
}
