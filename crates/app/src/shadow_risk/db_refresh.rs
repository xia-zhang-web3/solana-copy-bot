use super::*;

impl ShadowRiskGuard {
    pub(crate) fn maybe_refresh_db_state(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            self.last_db_refresh_error = None;
            return Ok(());
        }

        if let Some(last_refresh) = self.last_db_refresh_at {
            if now - last_refresh < chrono::Duration::seconds(RISK_DB_REFRESH_MIN_SECONDS.max(1)) {
                if let Some(cached_error) = self.last_db_refresh_error.as_deref() {
                    return Err(anyhow::anyhow!(
                        "risk refresh cached error (within throttle window): {}",
                        cached_error
                    ));
                }
                return Ok(());
            }
        }
        self.last_db_refresh_at = Some(now);

        let refresh_result = (|| -> Result<()> {
            let (_, pnl_1h_lamports) =
                store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(1))?;
            let (_, pnl_6h_lamports) =
                store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(6))?;
            let (_, pnl_24h_lamports) =
                store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::hours(24))?;
            let drawdown_1h_stop_lamports = self.shadow_drawdown_stop_lamports(
                self.config.shadow_drawdown_1h_stop_sol,
                "risk.shadow_drawdown_1h_stop_sol",
            )?;
            let drawdown_6h_stop_lamports = self.shadow_drawdown_stop_lamports(
                self.config.shadow_drawdown_6h_stop_sol,
                "risk.shadow_drawdown_6h_stop_sol",
            )?;
            let drawdown_24h_stop_lamports = self.shadow_drawdown_stop_lamports(
                self.config.shadow_drawdown_24h_stop_sol,
                "risk.shadow_drawdown_24h_stop_sol",
            )?;
            let shadow_soft_exposure_cap_lamports = self.shadow_soft_exposure_cap_lamports()?;
            let shadow_soft_exposure_resume_below_lamports =
                self.shadow_soft_exposure_resume_below_lamports()?;
            let shadow_hard_exposure_cap_lamports = self.shadow_hard_exposure_cap_lamports()?;

            let rug_window_start = now
                - chrono::Duration::minutes(
                    self.config.shadow_rug_loss_window_minutes.max(1) as i64
                );
            let rug_count_since = store.shadow_rug_loss_count_since(
                rug_window_start,
                self.config.shadow_rug_loss_return_threshold,
            )?;
            let (_, rug_sample_total, rug_rate_recent) = store.shadow_rug_loss_rate_recent(
                rug_window_start,
                self.config.shadow_rug_loss_rate_sample_size.max(1),
                self.config.shadow_rug_loss_return_threshold,
            )?;
            let rug_rate_sample_floor = self
                .config
                .shadow_rug_loss_rate_sample_size
                .min(self.config.shadow_rug_loss_count_threshold)
                .max(1);
            let rug_rate_breach = rug_sample_total >= rug_rate_sample_floor
                && rug_rate_recent > self.config.shadow_rug_loss_rate_threshold;

            let hard_stop_breach = if pnl_24h_lamports <= drawdown_24h_stop_lamports {
                Some((
                    "drawdown_24h",
                    format!(
                        "pnl_24h={:.6} <= stop={:.6}",
                        signed_lamports_to_sol(pnl_24h_lamports),
                        signed_lamports_to_sol(drawdown_24h_stop_lamports)
                    ),
                ))
            } else if rug_count_since >= self.config.shadow_rug_loss_count_threshold
                || rug_rate_breach
            {
                Some((
                    "rug_loss",
                    format!(
                        "rug_count_since={} sample_total={} rug_rate_sample_floor={} rug_rate_recent={:.4}",
                        rug_count_since,
                        rug_sample_total,
                        rug_rate_sample_floor,
                        rug_rate_recent
                    ),
                ))
            } else {
                None
            };

            if let Some((stop_type, detail)) = hard_stop_breach {
                self.hard_stop_clear_healthy_streak = 0;
                self.activate_hard_stop(store, now, stop_type, detail)?;
                return Ok(());
            }

            if self.hard_stop_reason.is_some() {
                self.hard_stop_clear_healthy_streak = self
                    .hard_stop_clear_healthy_streak
                    .saturating_add(1)
                    .min(HARD_STOP_CLEAR_HEALTHY_REFRESHES);
                if self.hard_stop_clear_healthy_streak < HARD_STOP_CLEAR_HEALTHY_REFRESHES {
                    return Ok(());
                }
                self.clear_hard_stop(store, now)?;
            } else {
                self.hard_stop_clear_healthy_streak = 0;
            }

            let exposure_lamports = store.shadow_risk_open_notional_lamports()?;
            let exposure_blocked_now = exposure_lamports >= shadow_hard_exposure_cap_lamports;
            let exposure_detail = format!(
                "risk_open_notional_sol={:.6} hard_cap={:.6}",
                lamports_to_sol(exposure_lamports),
                lamports_to_sol(shadow_hard_exposure_cap_lamports)
            );
            if exposure_blocked_now != self.exposure_hard_blocked {
                if exposure_blocked_now {
                    self.exposure_hard_blocked = true;
                    self.exposure_hard_detail = Some(exposure_detail.clone());
                    warn!(
                        detail = %exposure_detail,
                        "shadow risk exposure hard cap active"
                    );
                    let details_json = format!(
                        "{{\"state\":\"active\",\"detail\":\"{}\"}}",
                        sanitize_json_value(&exposure_detail)
                    );
                    record_shadow_risk_state_event_or_warn(
                        store,
                        "shadow_risk_exposure_hard_cap",
                        "warn",
                        now,
                        &details_json,
                        "failed to persist shadow risk exposure hard cap event with fatal sqlite I/O",
                    )?;
                } else {
                    info!("shadow risk exposure hard cap cleared");
                    record_shadow_risk_state_event_or_warn(
                        store,
                        "shadow_risk_exposure_hard_cap_cleared",
                        "info",
                        now,
                        "{\"state\":\"cleared\"}",
                        "failed to persist shadow risk exposure hard cap clear event with fatal sqlite I/O",
                    )?;
                    self.exposure_hard_blocked = false;
                    self.exposure_hard_detail = None;
                }
            } else if exposure_blocked_now {
                self.exposure_hard_detail = Some(exposure_detail);
            }

            if exposure_lamports >= shadow_hard_exposure_cap_lamports {
                return Ok(());
            }
            if self.soft_exposure_pause_latched
                && self.pause_until.is_none()
                && exposure_lamports < shadow_soft_exposure_resume_below_lamports
            {
                self.clear_soft_exposure_pause(store, now)?;
            } else if !self.soft_exposure_pause_latched
                && self.pause_until.is_none()
                && exposure_lamports >= shadow_soft_exposure_cap_lamports
            {
                self.activate_soft_exposure_pause(
                    store,
                    now,
                    chrono::Duration::minutes(self.config.shadow_soft_pause_minutes.max(1) as i64),
                    exposure_lamports,
                    shadow_soft_exposure_cap_lamports,
                    shadow_soft_exposure_resume_below_lamports,
                )?;
            }
            if pnl_6h_lamports <= drawdown_6h_stop_lamports {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(
                        self.config.shadow_drawdown_6h_pause_minutes.max(1) as i64
                    ),
                    "drawdown_6h",
                    format!(
                        "pnl_6h={:.6} <= stop={:.6}",
                        signed_lamports_to_sol(pnl_6h_lamports),
                        signed_lamports_to_sol(drawdown_6h_stop_lamports)
                    ),
                )?;
            }
            if pnl_1h_lamports <= drawdown_1h_stop_lamports {
                self.activate_pause(
                    store,
                    now,
                    chrono::Duration::minutes(
                        self.config.shadow_drawdown_1h_pause_minutes.max(1) as i64
                    ),
                    "drawdown_1h",
                    format!(
                        "pnl_1h={:.6} <= stop={:.6}",
                        signed_lamports_to_sol(pnl_1h_lamports),
                        signed_lamports_to_sol(drawdown_1h_stop_lamports)
                    ),
                )?;
            }

            Ok(())
        })();

        match refresh_result {
            Ok(()) => {
                self.last_db_refresh_error = None;
                Ok(())
            }
            Err(error) => {
                self.last_db_refresh_error = Some(error.to_string());
                Err(error)
            }
        }
    }
}
