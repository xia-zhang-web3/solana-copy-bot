use super::*;

impl ShadowRiskGuard {
    pub(crate) fn observe_ingestion_snapshot(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        snapshot: Option<IngestionRuntimeSnapshot>,
    ) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            return Ok(());
        }
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        let sample_ts = snapshot.ts_utc;
        let min_interval = chrono::Duration::seconds(RISK_INFRA_SAMPLE_MIN_SECONDS.max(1));
        let should_push = self
            .infra_samples
            .back()
            .map(|last| sample_ts - last.ts_utc >= min_interval)
            .unwrap_or(true);
        if should_push {
            self.infra_samples.push_back(snapshot);
        } else if let Some(last) = self.infra_samples.back_mut() {
            *last = snapshot;
        }

        let retention_minutes = self
            .config
            .shadow_infra_window_minutes
            .max(self.config.shadow_infra_lag_breach_minutes)
            .max(20)
            .saturating_mul(2);
        let cutoff = sample_ts - chrono::Duration::minutes(retention_minutes as i64);
        while self
            .infra_samples
            .front()
            .map(|sample| sample.ts_utc < cutoff)
            .unwrap_or(false)
        {
            self.infra_samples.pop_front();
        }

        if snapshot.ingestion_lag_ms_p95 > self.config.shadow_infra_lag_p95_threshold_ms {
            if self.lag_breach_since.is_none() {
                self.lag_breach_since = Some(sample_ts);
            }
        } else {
            self.lag_breach_since = None;
        }

        let new_signal = self.compute_infra_block_signal(sample_ts);
        match new_signal {
            Some(signal) => {
                self.infra_healthy_streak = 0;
                if self.infra_candidate_key == Some(signal.key) {
                    self.infra_candidate_streak = self.infra_candidate_streak.saturating_add(1);
                } else {
                    self.infra_candidate_key = Some(signal.key);
                    self.infra_candidate_streak = 1;
                }

                if self.infra_block_key == Some(signal.key) {
                    self.infra_block_reason = Some(signal.reason);
                    return Ok(());
                }

                if self.infra_block_key.is_some() {
                    if self.infra_candidate_streak >= RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
                        self.infra_block_key = Some(signal.key);
                        self.infra_block_reason = Some(signal.reason);
                    }
                    return Ok(());
                }

                if self.infra_candidate_streak >= RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
                    let reason = signal.reason;
                    warn!(reason = %reason, "shadow risk infra stop activated");
                    let details_json = self.build_infra_stop_details_json(&reason);
                    if self.should_emit_infra_event(now) {
                        record_shadow_risk_state_event_or_warn(
                            store,
                            "shadow_risk_infra_stop",
                            "warn",
                            now,
                            &details_json,
                            "failed to persist shadow risk infra stop event with fatal sqlite I/O",
                        )?;
                    }
                    self.infra_block_key = Some(signal.key);
                    self.infra_block_reason = Some(reason);
                }
            }
            None => {
                self.infra_candidate_key = None;
                self.infra_candidate_streak = 0;
                if self.infra_block_key.is_none() {
                    return Ok(());
                }
                self.infra_healthy_streak = self.infra_healthy_streak.saturating_add(1);
                if self.infra_healthy_streak < RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
                    return Ok(());
                }
                self.infra_healthy_streak = 0;
                info!("shadow risk infra stop cleared");
                if self.should_emit_infra_event(now) {
                    record_shadow_risk_state_event_or_warn(
                        store,
                        "shadow_risk_infra_cleared",
                        "info",
                        now,
                        "{\"state\":\"cleared\"}",
                        "failed to persist shadow risk infra clear event with fatal sqlite I/O",
                    )?;
                }
                self.infra_block_key = None;
                self.infra_block_reason = None;
            }
        }
        Ok(())
    }
}
