use super::*;

impl WalletFreshnessCaptureSnapshot {
    pub fn to_storage_write(&self) -> Result<DiscoveryWalletFreshnessCaptureWrite> {
        Ok(DiscoveryWalletFreshnessCaptureWrite {
            captured_at: self.captured_at,
            recent_cycles: self.recent_cycles,
            verdict: self.audit.verdict.as_str().to_string(),
            reason: self.audit.reason.clone(),
            publication_age_seconds: self.audit.publication_age_seconds,
            raw_truth_sufficient: self.audit.raw_truth.sufficient,
            raw_truth_reason: self.audit.raw_truth.reason.clone(),
            shadow_signal_verdict: self.shadow_signal.verdict.as_str().to_string(),
            shadow_signal_reason: self.shadow_signal.reason.clone(),
            published_wallet_ids: self.audit.published_wallet_ids.clone(),
            active_follow_wallet_ids: self.audit.active_follow_wallet_ids.clone(),
            current_raw_top_wallet_ids: self.audit.current_raw_top_wallet_ids.clone(),
            audit_json: serde_json::to_string(&self.audit)
                .context("failed serializing wallet freshness audit report")?,
            shadow_signal_json: serde_json::to_string(&self.shadow_signal)
                .context("failed serializing wallet freshness shadow evidence")?,
        })
    }
}

pub fn wallet_freshness_capture_from_row(
    row: DiscoveryWalletFreshnessCaptureRow,
) -> Result<WalletFreshnessCaptureSnapshot> {
    let audit: WalletFreshnessAuditReport = serde_json::from_str(&row.audit_json)
        .context("failed deserializing persisted wallet freshness audit json")?;
    let shadow_signal: WalletFreshnessShadowSignalEvidence =
        serde_json::from_str(&row.shadow_signal_json)
            .context("failed deserializing persisted wallet freshness shadow evidence json")?;
    Ok(WalletFreshnessCaptureSnapshot {
        capture_id: Some(row.capture_id),
        captured_at: row.captured_at,
        capture_age_seconds: None,
        within_recent_horizon: None,
        recent_cycles: row.recent_cycles,
        audit,
        shadow_signal,
    })
}

fn capture_has_rotation_evidence(capture: &WalletFreshnessCaptureSnapshot) -> bool {
    capture.audit.rotation.signal_available
        && (!capture
            .audit
            .rotation
            .entered_since_previous_cycle
            .is_empty()
            || !capture.audit.rotation.left_since_previous_cycle.is_empty()
            || capture.audit.rotation.unique_wallet_count_across_cycles
                > capture.audit.current_raw_top_wallet_ids.len())
}

fn shadow_signal_present(capture: &WalletFreshnessCaptureSnapshot) -> bool {
    matches!(
        capture.shadow_signal.verdict,
        WalletShadowSignalVerdict::ShadowSignalsPresentButConcentrated
            | WalletShadowSignalVerdict::ShadowSignalsPresentAndDistributed
    )
}

pub(super) fn summarize_wallet_freshness_history(
    generated_at: DateTime<Utc>,
    captures_requested: usize,
    recent_horizon_seconds: u64,
    captures: Vec<WalletFreshnessCaptureSnapshot>,
) -> WalletFreshnessHistoryReport {
    let captures_loaded = captures.len();
    let latest_capture_age_seconds = captures.first().map(|capture| {
        generated_at
            .signed_duration_since(capture.captured_at)
            .num_seconds()
            .max(0) as u64
    });
    let annotated_captures = captures
        .into_iter()
        .map(|mut capture| {
            let capture_age_seconds = generated_at
                .signed_duration_since(capture.captured_at)
                .num_seconds()
                .max(0) as u64;
            let within_recent_horizon = capture_age_seconds <= recent_horizon_seconds;
            capture.capture_age_seconds = Some(capture_age_seconds);
            capture.within_recent_horizon = Some(within_recent_horizon);
            capture
        })
        .collect::<Vec<_>>();
    let captures = annotated_captures
        .iter()
        .filter(|capture| capture.within_recent_horizon == Some(true))
        .cloned()
        .collect::<Vec<_>>();
    let captures_within_recent_horizon = captures.len();
    let stale_captures_excluded_count =
        captures_loaded.saturating_sub(captures_within_recent_horizon);

    let mut fresh_capture_count = 0usize;
    let mut drifting_capture_count = 0usize;
    let mut stale_capture_count = 0usize;
    let mut insufficient_raw_capture_count = 0usize;
    let mut no_publication_truth_capture_count = 0usize;
    let mut exact_published_current_match_count = 0usize;
    let mut exact_active_current_match_count = 0usize;
    let mut rotation_evidence_capture_count = 0usize;
    let mut shadow_signal_present_capture_count = 0usize;
    let mut broad_shadow_signal_capture_count = 0usize;
    let mut active_follow_change_count = 0usize;
    let mut current_raw_change_count = 0usize;

    for (index, capture) in captures.iter().enumerate() {
        match capture.audit.verdict {
            WalletFreshnessVerdict::FreshCurrent => fresh_capture_count += 1,
            WalletFreshnessVerdict::DriftingButAcceptable => drifting_capture_count += 1,
            WalletFreshnessVerdict::StalePublicationTruth => stale_capture_count += 1,
            WalletFreshnessVerdict::InsufficientRawTruth => insufficient_raw_capture_count += 1,
            WalletFreshnessVerdict::FailClosedNoPublicationTruth => {
                no_publication_truth_capture_count += 1;
            }
        }
        if capture.audit.published_vs_current_raw.exact_match {
            exact_published_current_match_count += 1;
        }
        if capture.audit.active_follow_vs_current_raw.exact_match {
            exact_active_current_match_count += 1;
        }
        if capture_has_rotation_evidence(capture) {
            rotation_evidence_capture_count += 1;
        }
        if shadow_signal_present(capture) {
            shadow_signal_present_capture_count += 1;
        }
        if capture.shadow_signal.shadow_signal_broadly_distributed {
            broad_shadow_signal_capture_count += 1;
        }

        if index > 0 {
            let previous = &captures[index - 1];
            if !compare_wallet_universes(
                &capture.audit.active_follow_wallet_ids,
                &previous.audit.active_follow_wallet_ids,
            )
            .exact_match
            {
                active_follow_change_count += 1;
            }
            if !compare_wallet_universes(
                &capture.audit.current_raw_top_wallet_ids,
                &previous.audit.current_raw_top_wallet_ids,
            )
            .exact_match
            {
                current_raw_change_count += 1;
            }
        }
    }

    let captures_considered = captures.len();
    let (verdict, reason) = if captures.is_empty() {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            if captures_loaded == 0 {
                "no_persisted_wallet_freshness_captures".to_string()
            } else {
                "no_recent_wallet_freshness_captures_within_horizon".to_string()
            },
        )
    } else if insufficient_raw_capture_count > 0 {
        (
            WalletFreshnessHistoryVerdict::RawTruthInsufficient,
            "recent_captures_include_insufficient_raw_truth".to_string(),
        )
    } else if no_publication_truth_capture_count > 0 {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            "recent_captures_include_missing_publication_truth".to_string(),
        )
    } else if stale_capture_count > 0 || drifting_capture_count > 0 {
        (
            WalletFreshnessHistoryVerdict::PublicationDrifting,
            "recent_captures_show_publication_drift_from_current_raw_truth".to_string(),
        )
    } else if captures_considered < 3 {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            "fewer_than_three_recent_wallet_freshness_captures".to_string(),
        )
    } else if shadow_signal_present_capture_count == 0 {
        (
            WalletFreshnessHistoryVerdict::InsufficientEvidence,
            "no_recent_shadow_signal_evidence_from_selected_wallets".to_string(),
        )
    } else if active_follow_change_count == 0
        && current_raw_change_count == 0
        && rotation_evidence_capture_count == 0
    {
        (
            WalletFreshnessHistoryVerdict::PartiallyValidatedButLowRotation,
            "recent_captures_remain_fresh_but_show_low_rotation".to_string(),
        )
    } else {
        (
            WalletFreshnessHistoryVerdict::ValidatedCurrent,
            "recent_captures_show_current_wallet_selection_and_shadow_signal_evidence".to_string(),
        )
    };

    WalletFreshnessHistoryReport {
        generated_at,
        captures_requested,
        captures_loaded,
        captures_considered,
        captures_within_recent_horizon,
        recent_horizon_seconds,
        latest_capture_age_seconds,
        stale_captures_excluded_from_verdict: stale_captures_excluded_count > 0,
        stale_captures_excluded_count,
        verdict,
        reason,
        fresh_capture_count,
        drifting_capture_count,
        stale_capture_count,
        insufficient_raw_capture_count,
        no_publication_truth_capture_count,
        exact_published_current_match_count,
        exact_active_current_match_count,
        active_follow_change_count,
        current_raw_change_count,
        rotation_evidence_capture_count,
        shadow_signal_present_capture_count,
        broad_shadow_signal_capture_count,
        captures: annotated_captures,
    }
}
