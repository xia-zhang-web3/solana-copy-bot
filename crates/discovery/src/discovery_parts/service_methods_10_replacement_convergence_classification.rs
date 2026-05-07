use super::*;

impl DiscoveryService {
    pub(super) fn classify_recent_raw_replacement_convergence(
        replacement_candidate_exists: bool,
        source_state_available: bool,
        candidate_complete_against_current_source: Option<bool>,
        candidate_promotable_now: Option<bool>,
        attempt_telemetry_parseable: bool,
        latest_attempt_advanced: Option<bool>,
        latest_attempt_stalled: Option<bool>,
        latest_attempt_reset_or_recreated: Option<bool>,
        rows_remaining_to_current_source: Option<u64>,
        latest_attempt_row_count_delta: Option<i64>,
        estimated_attempts_to_current_source: Option<u64>,
    ) -> (RecentRawReplacementConvergenceReasonClass, bool, String) {
        if candidate_promotable_now == Some(true) {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceReadyToPromote,
                true,
                "recent_raw fixed staged replacement candidate is complete against the current source cached row_count/frontier and is promotable now under the existing replacement-promotion contract".to_string(),
            );
        }

        if !replacement_candidate_exists || !source_state_available {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw replacement convergence is unproven because the bounded current evidence is incomplete. replacement_candidate_exists={}, source_state_available={}",
                    replacement_candidate_exists,
                    source_state_available,
                ),
            );
        }

        if candidate_complete_against_current_source.is_none() || candidate_promotable_now.is_none()
        {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw replacement convergence is unproven because the existing replacement-promotion contract could not prove candidate completeness or promotability. candidate_complete_against_current_source={}, candidate_promotable_now={}",
                    candidate_complete_against_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    candidate_promotable_now
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        if !attempt_telemetry_parseable {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceMissingOrUnparseableAttemptTelemetry,
                false,
                "recent_raw replacement convergence is unproven because the latest exact-path attempt telemetry is missing or unparseable; bounded mode does not fall back to broad artifact scans".to_string(),
            );
        }

        if latest_attempt_reset_or_recreated == Some(true) {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceResetOrRecreated,
                true,
                "recent_raw latest attempt telemetry proves the fixed replacement path reset/recreated from a latest-surface seed, so convergence cannot be classified as continuous forward progress from that attempt".to_string(),
            );
        }

        if candidate_complete_against_current_source == Some(false)
            && latest_attempt_advanced == Some(true)
        {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceAdvancingButIncomplete,
                true,
                format!(
                    "recent_raw latest attempt telemetry proves forward progress but the fixed staged replacement candidate remains incomplete against current source. rows_remaining_to_current_source={}, latest_attempt_row_count_delta={}, estimated_attempts_to_current_source={}",
                    rows_remaining_to_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    latest_attempt_row_count_delta
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    estimated_attempts_to_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        if candidate_complete_against_current_source == Some(false)
            && latest_attempt_stalled == Some(true)
        {
            return (
                RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceStalledOnLatestAttempt,
                true,
                "recent_raw latest attempt telemetry proves the fixed replacement path resumed/preserved retry state but did not advance row_count or covered_through on the latest attempt".to_string(),
            );
        }

        (
            RecentRawReplacementConvergenceReasonClass::RecentRawReplacementConvergenceUnprovenDueToMissingEvidence,
            false,
            format!(
                "recent_raw replacement convergence remains unproven from bounded current evidence. candidate_complete_against_current_source={}, candidate_promotable_now={}, attempt_telemetry_parseable={}, latest_attempt_advanced={}, latest_attempt_stalled={}, latest_attempt_reset_or_recreated={}",
                candidate_complete_against_current_source
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                candidate_promotable_now
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                attempt_telemetry_parseable,
                latest_attempt_advanced
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                latest_attempt_stalled
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                latest_attempt_reset_or_recreated
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
            ),
        )
    }
}
