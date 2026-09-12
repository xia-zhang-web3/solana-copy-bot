use super::*;

impl ShadowRiskGuard {
    pub(crate) fn can_complete_buy_quote(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        pause_on_outage: bool,
        swap: &copybot_core_types::SwapEvent,
        receipt: Option<&copybot_shadow::RecordedBuyLot>,
    ) -> BuyRiskDecision {
        let proof = (|| -> Result<()> {
            if let Some(receipt) = receipt {
                receipt.verify(store, swap)?;
            } else {
                let id = format!(
                    "shadow:{}:{}:buy:{}",
                    swap.signature, swap.wallet, swap.token_out
                );
                anyhow::ensure!(
                    store.load_copy_signal_by_signal_id(&id)?.is_none(),
                    "recorded BUY has no insert provenance"
                );
            }
            Ok(())
        })();
        if let Err(error) = proof {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail: format!("hot_quote_own_lot_unproven: {error:#}"),
            };
        }
        let decision = self.check_buy(
            store,
            now,
            pause_on_outage,
            Some(&swap.wallet),
            Some(&swap.token_out),
            receipt.is_some(),
        );
        if !matches!(decision, BuyRiskDecision::Allow) || !self.config.shadow_killswitch_enabled {
            return decision;
        }
        // Fresh reads close the HTTP wait window without advancing the guard's
        // throttled stop-recovery hysteresis or changing its persistent state.
        match self.current_completion_stop(store, now) {
            Ok(None) => decision,
            Ok(Some(reason)) => BuyRiskDecision::Blocked {
                reason,
                detail: "current risk constraint changed during quote".into(),
            },
            Err(error) => BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail: format!("hot_quote_risk_refresh_error: {error:#}"),
            },
        }
    }

    fn current_completion_stop(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<Option<BuyRiskBlockReason>> {
        for (hours, threshold, name, reason) in [
            (
                24,
                self.config.shadow_drawdown_24h_stop_sol,
                "risk.shadow_drawdown_24h_stop_sol",
                BuyRiskBlockReason::HardStop,
            ),
            (
                6,
                self.config.shadow_drawdown_6h_stop_sol,
                "risk.shadow_drawdown_6h_stop_sol",
                BuyRiskBlockReason::TimedPause,
            ),
            (
                1,
                self.config.shadow_drawdown_1h_stop_sol,
                "risk.shadow_drawdown_1h_stop_sol",
                BuyRiskBlockReason::TimedPause,
            ),
        ] {
            let (_, pnl) = store
                .shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(hours))?;
            if pnl <= self.shadow_drawdown_stop_lamports(threshold, name)? {
                return Ok(Some(reason));
            }
        }
        let exposure = store.shadow_risk_open_notional_lamports()?;
        if exposure >= self.shadow_hard_exposure_cap_lamports()? {
            return Ok(Some(BuyRiskBlockReason::ExposureCap));
        }
        if exposure >= self.shadow_soft_exposure_cap_lamports()? {
            return Ok(Some(BuyRiskBlockReason::TimedPause));
        }
        let since = now
            - chrono::Duration::minutes(self.config.shadow_rug_loss_window_minutes.max(1) as i64);
        let count = store
            .shadow_rug_loss_count_since(since, self.config.shadow_rug_loss_return_threshold)?;
        let floor = self.config.shadow_rug_loss_rate_sample_size.max(1);
        let (_, sampled, rate) = store.shadow_rug_loss_rate_recent(
            since,
            floor,
            self.config.shadow_rug_loss_return_threshold,
        )?;
        Ok((count >= self.config.shadow_rug_loss_count_threshold
            || (sampled >= floor && rate > self.config.shadow_rug_loss_rate_threshold))
            .then_some(BuyRiskBlockReason::HardStop))
    }
}
