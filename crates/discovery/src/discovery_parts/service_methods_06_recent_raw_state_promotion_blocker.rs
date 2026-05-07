use crate::*;

impl DiscoveryService {
    pub(crate) fn classify_recent_raw_promotion_blocker(
        promoted: &RecentRawSurfaceRead,
        staged: &RecentRawSurfaceRead,
        promoted_exists: bool,
        staged_exists: bool,
        source_state_available: bool,
        source_outruns_promoted: Option<bool>,
        source_outruns_staged: Option<bool>,
        staged_newer_than_promoted: Option<bool>,
    ) -> (RecentRawPromotionBlockerReasonClass, bool, bool, String) {
        if promoted.manifest_error.is_some() || staged.manifest_error.is_some() {
            return (
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByUnprovenState,
                true,
                false,
                format!(
                    "recent_raw promotion state is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                    promoted
                        .manifest_error
                        .as_deref()
                        .unwrap_or("null"),
                    staged.manifest_error.as_deref().unwrap_or("null")
                ),
            );
        }

        if !promoted_exists && !staged_exists {
            return (
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingPromotedLatest,
                true,
                false,
                "recent_raw promotion is blocked because neither a promoted latest surface nor a staged recent_raw snapshot is currently present".to_string(),
            );
        }

        if staged_exists {
            if !source_state_available {
                return (
                    RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByUnprovenState,
                    true,
                    false,
                    "recent_raw staged snapshot exists, but current source recent_raw journal state could not be proven read-only from the runtime db".to_string(),
                );
            }
            if source_outruns_staged == Some(true) {
                return (
                    RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByIncompleteStagedCoverage,
                    true,
                    false,
                    "recent_raw staged snapshot is still behind the current source recent_raw journal coverage, so promotion is not ready yet".to_string(),
                );
            }
            if !promoted_exists {
                return (
                    RecentRawPromotionBlockerReasonClass::RecentRawPromotionReadyNow,
                    false,
                    true,
                    "recent_raw staged snapshot is complete against the current source state and no promoted latest surface exists, so promotion is ready now".to_string(),
                );
            }
            if staged_newer_than_promoted == Some(false) {
                return (
                    RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByStagedNotNewerThanPromoted,
                    true,
                    false,
                    "recent_raw staged snapshot is not newer than the currently promoted latest surface, so promotion is not actionable on this run".to_string(),
                );
            }
            if staged_newer_than_promoted == Some(true) || source_outruns_promoted == Some(true) {
                return (
                    RecentRawPromotionBlockerReasonClass::RecentRawPromotionReadyNow,
                    false,
                    true,
                    "recent_raw staged snapshot is complete and ahead of the currently promoted truth, so promotion is ready now".to_string(),
                );
            }
            return (
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByUnprovenState,
                true,
                false,
                "recent_raw staged snapshot exists, but its readiness relative to the promoted latest surface could not be proven from current on-disk state".to_string(),
            );
        }

        if !promoted_exists {
            return (
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingPromotedLatest,
                true,
                false,
                "recent_raw promotion is blocked because the promoted latest surface is missing and no staged snapshot is available to promote".to_string(),
            );
        }

        if !source_state_available {
            return (
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByUnprovenState,
                true,
                false,
                "recent_raw promoted latest surface exists, but the current source recent_raw journal state could not be proven read-only from the runtime db".to_string(),
            );
        }

        if source_outruns_promoted == Some(true) {
            return (
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingStagedSnapshot,
                true,
                false,
                "recent_raw promoted latest surface is behind the current source journal coverage and no staged snapshot is available to promote".to_string(),
            );
        }

        (
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionNotNeededPromotedAlreadyCurrent,
            false,
            false,
            "recent_raw promoted latest surface already matches current source coverage closely enough that no staged promotion is needed on this run".to_string(),
        )
    }
}
