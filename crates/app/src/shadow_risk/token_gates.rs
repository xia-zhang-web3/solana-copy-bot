use super::*;

impl ShadowRiskGuard {
    pub(crate) fn token_recent_close_cooldown(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        token: &str,
    ) -> Result<Option<String>> {
        if !self.config.shadow_token_recent_close_cooldown_enabled {
            return Ok(None);
        }
        let close_minutes = self
            .config
            .shadow_token_recent_close_cooldown_minutes
            .max(1);
        let loss_minutes = self.config.shadow_token_recent_loss_cooldown_minutes.max(1);
        let since = now - chrono::Duration::minutes(close_minutes.max(loss_minutes) as i64);
        let Some(recent) = store.shadow_token_recent_close_since(token, since)? else {
            return Ok(None);
        };
        let age_seconds = (now - recent.closed_ts).num_seconds();
        let close_active = age_seconds < (close_minutes as i64 * 60);
        let loss_active = recent
            .roi
            .is_some_and(|roi| roi <= self.config.shadow_token_recent_loss_cooldown_max_roi)
            && age_seconds < (loss_minutes as i64 * 60);
        if !close_active && !loss_active {
            return Ok(None);
        }
        let reason = if loss_active {
            "recent_loss"
        } else {
            "recent_close"
        };
        Ok(Some(format!(
            "token={} reason={} closed_ts={} age_seconds={} close_cooldown_minutes={} loss_cooldown_minutes={} pnl={:.6} entry={:.6} roi={:.4}",
            recent.token,
            reason,
            recent.closed_ts.to_rfc3339(),
            age_seconds,
            close_minutes,
            loss_minutes,
            recent.pnl_sol,
            recent.entry_cost_sol,
            recent.roi.unwrap_or(0.0)
        )))
    }

    pub(crate) fn wallet_token_fast_loss_cooldown(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        wallet_id: &str,
        token: &str,
    ) -> Result<Option<String>> {
        if !self.config.shadow_wallet_token_fast_loss_cooldown_enabled {
            return Ok(None);
        }
        let window_minutes = self
            .config
            .shadow_wallet_token_fast_loss_cooldown_window_minutes
            .max(1);
        let since = now - chrono::Duration::minutes(window_minutes as i64);
        let Some(cooldown) = store.shadow_wallet_token_fast_loss_since(
            wallet_id,
            token,
            since,
            self.config
                .shadow_wallet_token_fast_loss_cooldown_min_entry_sol,
            self.config
                .shadow_wallet_token_fast_loss_cooldown_max_hold_seconds
                .max(1),
            self.config.shadow_wallet_token_fast_loss_cooldown_max_roi,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(format!(
            "wallet={} token={} window_minutes={} closed_ts={} hold_seconds={} pnl={:.6} entry={:.6} roi={:.4}",
            cooldown.wallet_id,
            cooldown.token,
            window_minutes,
            cooldown.closed_ts.to_rfc3339(),
            cooldown.hold_seconds,
            cooldown.pnl_sol,
            cooldown.entry_cost_sol,
            cooldown.roi
        )))
    }

    pub(crate) fn token_open_notional_cap(
        &self,
        store: &SqliteStore,
        token: &str,
    ) -> Result<Option<String>> {
        let max_open_lots = self.config.shadow_max_open_lots_per_token.max(1);
        let open_lots = store.shadow_risk_open_lot_count_for_token(token)?;
        if open_lots >= max_open_lots {
            return Ok(Some(format!(
                "token={} risk_open_lots={} per_token_lot_cap={}",
                token, open_lots, max_open_lots
            )));
        }
        let cap = self.shadow_max_open_notional_per_token_lamports()?;
        let open_notional = store.shadow_risk_open_notional_lamports_for_token(token)?;
        if open_notional < cap {
            return Ok(None);
        }
        Ok(Some(format!(
            "token={} risk_open_notional_sol={:.6} per_token_cap={:.6}",
            token,
            lamports_to_sol(open_notional),
            lamports_to_sol(cap)
        )))
    }
}
