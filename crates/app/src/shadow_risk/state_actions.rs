use super::*;

impl ShadowRiskGuard {
    pub(crate) fn activate_hard_stop(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        stop_type: &str,
        detail: String,
    ) -> Result<()> {
        self.hard_stop_clear_healthy_streak = 0;
        if self.hard_stop_reason.is_some() {
            return Ok(());
        }
        let reason = format!("{stop_type}: {detail}");
        self.hard_stop_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk hard stop activated");
        let details_json = format!(
            "{{\"stop_type\":\"{}\",\"detail\":\"{}\"}}",
            sanitize_json_value(stop_type),
            sanitize_json_value(&detail)
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_hard_stop",
            "error",
            now,
            &details_json,
            "failed to persist shadow risk hard stop event with fatal sqlite I/O",
        )
    }

    pub(crate) fn clear_hard_stop(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let Some(previous_reason) = self.hard_stop_reason.clone() else {
            return Ok(());
        };
        let details_json = format!(
            "{{\"state\":\"cleared\",\"previous_reason\":\"{}\"}}",
            sanitize_json_value(&previous_reason)
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_hard_stop_cleared",
            "info",
            now,
            &details_json,
            "failed to persist shadow risk hard stop clear event with fatal sqlite I/O",
        )?;
        self.hard_stop_clear_healthy_streak = 0;
        self.hard_stop_reason = None;
        info!(
            previous_reason = %previous_reason,
            "shadow risk hard stop cleared"
        );
        Ok(())
    }

    pub(crate) fn activate_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        duration: chrono::Duration,
        pause_type: &str,
        detail: String,
    ) -> Result<()> {
        let duration = if duration <= chrono::Duration::zero() {
            chrono::Duration::minutes(1)
        } else {
            duration
        };
        let until = now + duration;
        let should_update = self.pause_until.map(|value| until > value).unwrap_or(true);
        if !should_update {
            return Ok(());
        }
        self.pause_until = Some(until);
        let reason = format!("{pause_type}: {detail}; until={}", until.to_rfc3339());
        self.pause_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk timed pause activated");
        let details_json = format!(
            "{{\"pause_type\":\"{}\",\"detail\":\"{}\",\"until\":\"{}\"}}",
            sanitize_json_value(pause_type),
            sanitize_json_value(&detail),
            until.to_rfc3339()
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause",
            "warn",
            now,
            &details_json,
            "failed to persist shadow risk timed pause event with fatal sqlite I/O",
        )
    }

    pub(crate) fn activate_soft_exposure_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        duration: chrono::Duration,
        exposure_lamports: Lamports,
        soft_cap_lamports: Lamports,
        resume_below_lamports: Lamports,
    ) -> Result<()> {
        let duration = if duration <= chrono::Duration::zero() {
            chrono::Duration::minutes(1)
        } else {
            duration
        };
        let until = now + duration;
        self.soft_exposure_pause_latched = true;
        self.soft_exposure_pause_until = Some(until);
        let detail = format!(
            "risk_open_notional_sol={:.6} >= soft_cap={:.6}; resume_below={:.6}",
            lamports_to_sol(exposure_lamports),
            lamports_to_sol(soft_cap_lamports),
            lamports_to_sol(resume_below_lamports)
        );
        let reason = format!("exposure_soft_cap: {detail}; until={}", until.to_rfc3339());
        self.soft_exposure_pause_reason = Some(reason.clone());
        warn!(reason = %reason, "shadow risk soft exposure pause activated");
        let details_json = format!(
            "{{\"pause_type\":\"exposure_soft_cap\",\"detail\":\"{}\",\"until\":\"{}\",\"resume_below_sol\":{:.9}}}",
            sanitize_json_value(&detail),
            until.to_rfc3339(),
            lamports_to_sol(resume_below_lamports)
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause",
            "warn",
            now,
            &details_json,
            "failed to persist shadow risk soft exposure pause event with fatal sqlite I/O",
        )
    }

    pub(crate) fn clear_pause(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let Some(previous_until) = self.pause_until else {
            self.pause_reason = None;
            return Ok(());
        };
        let previous_reason = self
            .pause_reason
            .clone()
            .unwrap_or_else(|| format!("paused_until={}", previous_until.to_rfc3339()));
        let cleared_pause_type = previous_reason
            .split_once(':')
            .map(|(pause_type, _)| pause_type.trim())
            .filter(|pause_type| !pause_type.is_empty())
            .unwrap_or("timed_pause");
        let details_json = format!(
            "{{\"state\":\"cleared\",\"pause_type\":\"{}\",\"previous_reason\":\"{}\",\"previous_until\":\"{}\"}}",
            sanitize_json_value(cleared_pause_type),
            sanitize_json_value(&previous_reason),
            previous_until.to_rfc3339()
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause_cleared",
            "info",
            now,
            &details_json,
            "failed to persist shadow risk timed pause clear event with fatal sqlite I/O",
        )?;
        self.pause_until = None;
        self.pause_reason = None;
        info!(
            previous_reason = %previous_reason,
            previous_until = %previous_until.to_rfc3339(),
            "shadow risk timed pause cleared"
        );
        Ok(())
    }

    pub(crate) fn clear_soft_exposure_pause(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let Some(previous_until) = self.soft_exposure_pause_until else {
            self.soft_exposure_pause_latched = false;
            self.soft_exposure_pause_reason = None;
            return Ok(());
        };
        let previous_reason = self.soft_exposure_pause_reason.clone().unwrap_or_else(|| {
            format!(
                "exposure_soft_cap: paused_until={}",
                previous_until.to_rfc3339()
            )
        });
        let details_json = format!(
            "{{\"state\":\"cleared\",\"pause_type\":\"exposure_soft_cap\",\"previous_reason\":\"{}\",\"previous_until\":\"{}\"}}",
            sanitize_json_value(&previous_reason),
            previous_until.to_rfc3339()
        );
        record_shadow_risk_state_event_or_warn(
            store,
            "shadow_risk_pause_cleared",
            "info",
            now,
            &details_json,
            "failed to persist shadow risk soft exposure pause clear event with fatal sqlite I/O",
        )?;
        self.soft_exposure_pause_latched = false;
        self.soft_exposure_pause_until = None;
        self.soft_exposure_pause_reason = None;
        info!(
            previous_reason = %previous_reason,
            previous_until = %previous_until.to_rfc3339(),
            "shadow risk soft exposure pause cleared"
        );
        Ok(())
    }

    pub(crate) fn should_emit_infra_event(&mut self, now: DateTime<Utc>) -> bool {
        let allow = self
            .infra_last_event_at
            .map(|last| {
                now - last >= chrono::Duration::seconds(RISK_INFRA_EVENT_THROTTLE_SECONDS.max(1))
            })
            .unwrap_or(true);
        if allow {
            self.infra_last_event_at = Some(now);
        }
        allow
    }
}
