use super::*;

impl DiscoveryService {
    pub(crate) fn classify_recent_raw_catch_up_status(
        state: &RecentRawDiagnosticState,
    ) -> (RecentRawCatchUpReasonClass, bool, bool, bool, bool, String) {
        if state.promoted.manifest_error.is_some() || state.staged.manifest_error.is_some() {
            return (
                RecentRawCatchUpReasonClass::RecentRawCatchUpUnprovenDueToMissingEvidence,
                false,
                false,
                false,
                true,
                format!(
                    "recent_raw catch-up status is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                    state.promoted.manifest_error.as_deref().unwrap_or("null"),
                    state.staged.manifest_error.as_deref().unwrap_or("null")
                ),
            );
        }

        if !state.source_state_available {
            return (
                RecentRawCatchUpReasonClass::RecentRawCatchUpUnprovenDueToMissingEvidence,
                false,
                false,
                false,
                true,
                "recent_raw catch-up status is unproven because the current source recent_raw journal state could not be proven read-only from the runtime db".to_string(),
            );
        }

        if state.staged_exists {
            match state.source_outruns_staged {
                Some(false) => {
                    return (
                        RecentRawCatchUpReasonClass::RecentRawCatchUpCaughtUp,
                        true,
                        false,
                        false,
                        false,
                        "recent_raw staged surface currently covers the same source journal frontier, so accumulation is caught up under current on-disk evidence".to_string(),
                    )
                }
                Some(true) => match state.staged_vs_promoted_relation {
                    Some(RecentRawManifestProgressRelation::CandidateAhead) => {
                        return (
                            RecentRawCatchUpReasonClass::RecentRawCatchUpProgressingButNotCaughtUp,
                            true,
                            true,
                            false,
                            false,
                            "recent_raw staged surface is ahead of the promoted latest surface but still behind the current source journal frontier, so catch-up is progressing but not yet caught up".to_string(),
                        )
                    }
                    Some(RecentRawManifestProgressRelation::Equivalent) => {
                        return (
                            RecentRawCatchUpReasonClass::RecentRawCatchUpStalled,
                            true,
                            false,
                            false,
                            false,
                            "recent_raw staged surface is not ahead of the promoted latest surface while the source journal is still ahead of both, so current catch-up evidence is stalled".to_string(),
                        )
                    }
                    Some(RecentRawManifestProgressRelation::ReferenceAhead) => {
                        return (
                            RecentRawCatchUpReasonClass::RecentRawCatchUpLosingToSource,
                            true,
                            false,
                            true,
                            false,
                            "recent_raw staged surface is still behind the already promoted latest surface while the source journal is ahead of both, so the current accumulation path is losing ground under observed state".to_string(),
                        )
                    }
                    None => {
                        return (
                            RecentRawCatchUpReasonClass::RecentRawCatchUpUnprovenDueToMissingEvidence,
                            false,
                            false,
                            false,
                            true,
                            "recent_raw staged surface is behind the current source journal frontier, but its progression relative to the promoted latest surface could not be proven from current artifacts".to_string(),
                        )
                    }
                },
                None => {}
            }
        } else if state.source_outruns_promoted == Some(false) {
            return (
                RecentRawCatchUpReasonClass::RecentRawCatchUpCaughtUp,
                true,
                false,
                false,
                false,
                "recent_raw promoted latest surface already matches the current source journal frontier, so no staged catch-up is required on this run".to_string(),
            );
        } else if state.source_outruns_promoted == Some(true) {
            return (
                RecentRawCatchUpReasonClass::RecentRawCatchUpStalled,
                true,
                false,
                false,
                false,
                "recent_raw promoted latest surface is behind the current source journal frontier and no staged surface is available to show forward accumulation, so catch-up is currently stalled".to_string(),
            );
        }

        (
            RecentRawCatchUpReasonClass::RecentRawCatchUpUnprovenDueToMissingEvidence,
            false,
            false,
            false,
            true,
            "recent_raw catch-up status could not be proven from the currently available promoted, staged, and source artifacts".to_string(),
        )
    }
}
