use crate::intent::{ExecutionIntent, ExecutionSide};
use crate::money::{checked_add_lamports, lamports_to_sol, sol_to_lamports_floor};
use crate::ExecutionRuntime;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage::{ExecutionConfirmStateSnapshot, SqliteStore};
use tracing::debug;

fn signed_lamports_to_sol(value: SignedLamports) -> f64 {
    value.as_i128() as f64 / 1_000_000_000.0
}

#[derive(Debug, Clone)]
pub(crate) struct ConfirmedBuyRiskBreach {
    pub total_exposure_sol: f64,
    pub token_exposure_sol: f64,
    pub open_positions: u64,
    pub reasons: Vec<String>,
}

impl ExecutionRuntime {
    fn max_position_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(self.risk.max_position_sol, "risk.max_position_sol")
    }

    fn max_total_exposure_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.risk.max_total_exposure_sol,
            "risk.max_total_exposure_sol",
        )
    }

    fn max_exposure_per_token_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.risk.max_exposure_per_token_sol,
            "risk.max_exposure_per_token_sol",
        )
    }

    fn current_total_exposure_lamports(&self, store: &SqliteStore) -> Result<Lamports> {
        store.live_open_exposure_lamports()
    }

    fn current_token_exposure_lamports(
        &self,
        store: &SqliteStore,
        token: &str,
    ) -> Result<Lamports> {
        store.live_open_exposure_lamports_for_token(token)
    }

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

    fn loss_limit_lamports_from_pct(&self, pct: f64, label: &str) -> Result<Option<Lamports>> {
        if !pct.is_finite() || pct <= 0.0 {
            return Ok(None);
        }
        let scaled = (self.max_total_exposure_lamports()?.as_u64() as f64) * (pct / 100.0);
        if !scaled.is_finite() || scaled <= 0.0 || scaled > u64::MAX as f64 {
            return Err(anyhow!(
                "{} produced invalid lamport threshold from pct={}",
                label,
                pct
            ));
        }
        Ok(Some(Lamports::new(scaled.floor() as u64)))
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
                let notional_lamports = intent.notional_lamports()?;
                let max_position_lamports = self.max_position_lamports()?;
                if notional_lamports > max_position_lamports {
                    return Ok(Some(format!(
                        "max_position_sol_exceeded notional_sol={:.6} max_position_sol={:.6}",
                        lamports_to_sol(notional_lamports),
                        lamports_to_sol(max_position_lamports)
                    )));
                }

                let current_exposure_lamports = self.current_total_exposure_lamports(store)?;
                let max_total_exposure_lamports = self.max_total_exposure_lamports()?;
                let projected_total_exposure_lamports = checked_add_lamports(
                    current_exposure_lamports,
                    notional_lamports,
                    "projected total exposure lamports",
                )?;
                if projected_total_exposure_lamports > max_total_exposure_lamports {
                    return Ok(Some(format!(
                        "max_total_exposure_exceeded current={:.6} notional={:.6} max_total={:.6}",
                        lamports_to_sol(current_exposure_lamports),
                        lamports_to_sol(notional_lamports),
                        lamports_to_sol(max_total_exposure_lamports)
                    )));
                }

                let current_token_exposure_lamports =
                    self.current_token_exposure_lamports(store, &intent.token)?;
                let max_exposure_per_token_lamports = self.max_exposure_per_token_lamports()?;
                let projected_token_exposure_lamports = checked_add_lamports(
                    current_token_exposure_lamports,
                    notional_lamports,
                    "projected token exposure lamports",
                )?;
                if projected_token_exposure_lamports > max_exposure_per_token_lamports {
                    return Ok(Some(format!(
                        "max_exposure_per_token_exceeded token={} current={:.6} notional={:.6} max_token={:.6}",
                        intent.token,
                        lamports_to_sol(current_token_exposure_lamports),
                        lamports_to_sol(notional_lamports),
                        lamports_to_sol(max_exposure_per_token_lamports)
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

                let daily_loss_limit_lamports = self.loss_limit_lamports_from_pct(
                    self.risk.daily_loss_limit_pct,
                    "risk.daily_loss_limit_pct",
                )?;
                let max_drawdown_limit_lamports = self.loss_limit_lamports_from_pct(
                    self.risk.max_drawdown_pct,
                    "risk.max_drawdown_pct",
                )?;
                let need_unrealized =
                    daily_loss_limit_lamports.is_some() || max_drawdown_limit_lamports.is_some();
                let use_exact_unrealized =
                    need_unrealized && store.live_open_positions_are_exact_ready()?;
                if use_exact_unrealized {
                    let (unrealized_pnl_lamports, unrealized_missing_price_count) =
                        store.live_unrealized_pnl_lamports(now)?;
                    if unrealized_missing_price_count > 0 {
                        return Ok(Some(format!(
                            "unrealized_price_unavailable unrealized_missing_price_count={} as_of={}",
                            unrealized_missing_price_count,
                            now.to_rfc3339()
                        )));
                    }

                    if let Some(daily_loss_limit_lamports) = daily_loss_limit_lamports {
                        let loss_window_start = now - Duration::hours(24);
                        let (_, realized_pnl_24h_lamports) =
                            store.live_realized_pnl_lamports_since(loss_window_start)?;
                        let pnl_24h_lamports = realized_pnl_24h_lamports
                            .checked_add(unrealized_pnl_lamports)
                            .ok_or_else(|| anyhow!("daily loss pnl lamports overflow"))?;
                        let daily_loss_floor =
                            SignedLamports::new(-i128::from(daily_loss_limit_lamports.as_u64()));
                        if pnl_24h_lamports <= daily_loss_floor {
                            return Ok(Some(format!(
                                "daily_loss_limit_exceeded pnl_24h={:.6} realized_pnl_24h={:.6} unrealized_pnl={:.6} unrealized_missing_price_count={} loss_limit_sol={:.6} loss_limit_pct={:.4} window_start={}",
                                signed_lamports_to_sol(pnl_24h_lamports),
                                signed_lamports_to_sol(realized_pnl_24h_lamports),
                                signed_lamports_to_sol(unrealized_pnl_lamports),
                                unrealized_missing_price_count,
                                lamports_to_sol(daily_loss_limit_lamports),
                                self.risk.daily_loss_limit_pct,
                                loss_window_start.to_rfc3339()
                            )));
                        }
                    }

                    if let Some(max_drawdown_limit_lamports) = max_drawdown_limit_lamports {
                        let drawdown_window_start = now - Duration::hours(24);
                        let max_drawdown_lamports = store
                            .live_max_drawdown_with_unrealized_lamports_since(
                                drawdown_window_start,
                                unrealized_pnl_lamports,
                            )?;
                        if max_drawdown_lamports >= max_drawdown_limit_lamports {
                            return Ok(Some(format!(
                                "max_drawdown_exceeded max_drawdown_sol={:.6} unrealized_missing_price_count={} drawdown_limit_sol={:.6} drawdown_limit_pct={:.4} window_start={}",
                                lamports_to_sol(max_drawdown_lamports),
                                unrealized_missing_price_count,
                                lamports_to_sol(max_drawdown_limit_lamports),
                                self.risk.max_drawdown_pct,
                                drawdown_window_start.to_rfc3339()
                            )));
                        }
                    }
                } else if need_unrealized {
                    let (unrealized_pnl_sol, unrealized_missing_price_count) =
                        store.live_unrealized_pnl_sol(now)?;
                    if unrealized_missing_price_count > 0 {
                        return Ok(Some(format!(
                            "unrealized_price_unavailable unrealized_missing_price_count={} as_of={}",
                            unrealized_missing_price_count,
                            now.to_rfc3339()
                        )));
                    }

                    if let Some(daily_loss_limit_lamports) = daily_loss_limit_lamports {
                        let loss_window_start = now - Duration::hours(24);
                        let (_, realized_pnl_24h) =
                            store.live_realized_pnl_since(loss_window_start)?;
                        let pnl_24h = realized_pnl_24h + unrealized_pnl_sol;
                        let daily_loss_limit_sol = lamports_to_sol(daily_loss_limit_lamports);
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

                    if let Some(max_drawdown_limit_lamports) = max_drawdown_limit_lamports {
                        let drawdown_window_start = now - Duration::hours(24);
                        let max_drawdown_sol = store.live_max_drawdown_with_unrealized_since(
                            drawdown_window_start,
                            unrealized_pnl_sol,
                        )?;
                        let max_drawdown_limit_sol = lamports_to_sol(max_drawdown_limit_lamports);
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
                }

                Ok(None)
            }
        }
    }

    pub(crate) fn confirmed_buy_risk_breach(
        &self,
        intent: &ExecutionIntent,
        snapshot: ExecutionConfirmStateSnapshot,
    ) -> Result<Option<ConfirmedBuyRiskBreach>> {
        if !matches!(intent.side, ExecutionSide::Buy) {
            return Ok(None);
        }

        let mut reasons = Vec::new();
        let total_exposure_lamports = snapshot.total_exposure_lamports;
        let token_exposure_lamports = snapshot.token_exposure_lamports;
        let max_total_exposure_lamports = self.max_total_exposure_lamports()?;
        let max_exposure_per_token_lamports = self.max_exposure_per_token_lamports()?;

        if total_exposure_lamports > max_total_exposure_lamports {
            reasons.push(format!(
                "max_total_exposure_exceeded current={:.6} max_total={:.6}",
                lamports_to_sol(total_exposure_lamports),
                lamports_to_sol(max_total_exposure_lamports)
            ));
        }

        if token_exposure_lamports > max_exposure_per_token_lamports {
            reasons.push(format!(
                "max_exposure_per_token_exceeded token={} current={:.6} max_token={:.6}",
                intent.token,
                lamports_to_sol(token_exposure_lamports),
                lamports_to_sol(max_exposure_per_token_lamports)
            ));
        }

        if snapshot.open_positions > self.risk.max_concurrent_positions as u64 {
            reasons.push(format!(
                "max_concurrent_positions_exceeded open_count={} max={}",
                snapshot.open_positions, self.risk.max_concurrent_positions
            ));
        }

        if reasons.is_empty() {
            return Ok(None);
        }

        Ok(Some(ConfirmedBuyRiskBreach {
            total_exposure_sol: snapshot.total_exposure_sol,
            token_exposure_sol: snapshot.token_exposure_sol,
            open_positions: snapshot.open_positions,
            reasons,
        }))
    }
}
