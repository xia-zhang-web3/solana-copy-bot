use super::*;

impl ShadowRiskGuard {
    #[cfg(test)]
    pub(crate) fn new(config: RiskConfig) -> Self {
        Self::new_with_ingestion_source(config, "mock")
    }

    pub(crate) fn new_with_ingestion_source(
        config: RiskConfig,
        ingestion_source: impl Into<String>,
    ) -> Self {
        Self {
            config,
            ingestion_source: ingestion_source.into(),
            ..Self::default()
        }
    }

    pub(crate) fn restore_pause_from_store(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if !self.config.shadow_killswitch_enabled {
            return Ok(());
        }
        let restore_result = (|| -> Result<Vec<(String, DateTime<Utc>, String)>> {
            let pause_events = store.list_risk_events_by_type_desc("shadow_risk_pause")?;
            if pause_events.is_empty() {
                return Ok(Vec::new());
            }
            let cleared_events =
                store.list_risk_events_by_type_desc("shadow_risk_pause_cleared")?;

            let mut latest_clear_by_type: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            let mut wildcard_clear_rowid: Option<i64> = None;
            for cleared_event in cleared_events {
                let cleared_pause_type = cleared_event
                    .details_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                    .and_then(|value| {
                        value
                            .get("pause_type")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    });
                match cleared_pause_type {
                    Some(pause_type) => {
                        latest_clear_by_type
                            .entry(pause_type)
                            .or_insert(cleared_event.rowid);
                    }
                    None => {
                        wildcard_clear_rowid.get_or_insert(cleared_event.rowid);
                    }
                }
            }

            let mut restored_pause_types = std::collections::HashSet::new();
            let mut restored_pauses = Vec::new();
            for pause_event in pause_events {
                let pause_details_json = pause_event
                    .details_json
                    .as_deref()
                    .ok_or_else(|| anyhow!("shadow_risk_pause missing details_json"))?;
                let pause_details: serde_json::Value = serde_json::from_str(pause_details_json)
                    .context("failed to parse shadow_risk_pause details_json")?;
                let pause_type = pause_details
                    .get("pause_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("timed_pause")
                    .to_string();
                if !restored_pause_types.insert(pause_type.clone()) {
                    continue;
                }
                if wildcard_clear_rowid.is_some_and(|rowid| rowid > pause_event.rowid) {
                    continue;
                }
                if latest_clear_by_type
                    .get(pause_type.as_str())
                    .copied()
                    .is_some_and(|rowid| rowid > pause_event.rowid)
                {
                    continue;
                }
                let until_raw = pause_details
                    .get("until")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| anyhow!("shadow_risk_pause missing until"))?;
                let until = DateTime::parse_from_rfc3339(until_raw)
                    .context("failed to parse shadow_risk_pause until")?
                    .with_timezone(&Utc);
                if pause_type != "exposure_soft_cap" && until <= now {
                    continue;
                }
                let detail = pause_details
                    .get("detail")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        format!("restored_from_risk_event_rowid={}", pause_event.rowid)
                    });
                restored_pauses.push((
                    pause_type.clone(),
                    until,
                    format!("{pause_type}: {detail}; until={}", until.to_rfc3339()),
                ));
            }

            Ok(restored_pauses)
        })();
        match restore_result {
            Ok(restored_pauses) => {
                for (pause_type, until, reason) in restored_pauses {
                    if pause_type == "exposure_soft_cap" {
                        self.soft_exposure_pause_latched = true;
                        self.soft_exposure_pause_until = Some(until);
                        self.soft_exposure_pause_reason = Some(reason.clone());
                    } else if self.pause_until.map(|value| until > value).unwrap_or(true) {
                        self.pause_until = Some(until);
                        self.pause_reason = Some(reason.clone());
                    }
                    info!(
                        reason = %reason,
                        until = %until.to_rfc3339(),
                        "restored shadow risk timed pause from durable state"
                    );
                }
            }
            Err(error) => {
                if shadow_risk_pause_restore_error_requires_restart(&error) {
                    return Err(error).context(
                        "failed to restore shadow risk timed pause with fatal sqlite I/O",
                    );
                }
                warn!(error = %error, "failed to restore shadow risk timed pause");
            }
        }
        Ok(())
    }

    pub(crate) fn shadow_soft_exposure_cap_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.config.shadow_soft_exposure_cap_sol,
            "risk.shadow_soft_exposure_cap_sol",
        )
    }

    pub(crate) fn shadow_soft_exposure_resume_below_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.config.shadow_soft_exposure_resume_below_sol,
            "risk.shadow_soft_exposure_resume_below_sol",
        )
    }

    pub(crate) fn shadow_hard_exposure_cap_lamports(&self) -> Result<Lamports> {
        sol_to_lamports_floor(
            self.config.shadow_hard_exposure_cap_sol,
            "risk.shadow_hard_exposure_cap_sol",
        )
    }

    pub(crate) fn shadow_drawdown_stop_lamports(
        &self,
        stop_sol: f64,
        label: &str,
    ) -> Result<SignedLamports> {
        sol_to_signed_lamports_conservative(stop_sol, label)
    }
}
