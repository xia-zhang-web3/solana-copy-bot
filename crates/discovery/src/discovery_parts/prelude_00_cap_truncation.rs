fn maybe_arm_cap_truncation_deactivation_guard(
    state: &mut DiscoveryWindowState,
    now: DateTime<Utc>,
    reason: CapTruncationDeactivationGuardReason,
) {
    if !state.arm_cap_truncation_deactivation_guard(
        now,
        reason,
        CAP_TRUNCATION_FOLLOWLIST_DEACTIVATION_GUARD_CYCLES,
    ) {
        return;
    }
    let Some(floor) = state.cap_truncation_floor.as_ref() else {
        return;
    };
    warn!(
        followlist_deactivation_suppression_reason = reason.as_str(),
        followlist_deactivation_suppression_started_at = %now,
        cap_truncation_floor_ts = %floor.ts_utc,
        cap_truncation_floor_signature = floor.signature.as_str(),
        cap_truncation_deactivation_guard_cycles =
            state.cap_truncation_deactivation_guard_cycles_remaining,
        "discovery followlist deactivations temporarily suppressed while raw window is cap-truncated"
    );
}

fn maybe_warn_on_cap_truncation_deactivation_guard_expiry(
    state: &DiscoveryWindowState,
    followlist_deactivations_suppressed: bool,
) {
    let Some(floor) = state.cap_truncation_floor.as_ref() else {
        return;
    };
    let reason = state
        .cap_truncation_deactivation_guard_reason
        .map(CapTruncationDeactivationGuardReason::as_str)
        .unwrap_or("cap_truncation");
    if followlist_deactivations_suppressed {
        warn!(
            followlist_deactivation_suppression_reason = reason,
            followlist_deactivation_suppression_started_at = ?state
                .cap_truncation_deactivation_guard_started_at,
            cap_truncation_floor_ts = %floor.ts_utc,
            cap_truncation_floor_signature = floor.signature.as_str(),
            "discovery cap-truncation guard countdown expired, but raw-window followlist mutations remain suppressed until truncation state clears"
        );
    }
}

#[derive(Debug, Clone, Default)]
struct CapTruncationTelemetrySnapshot {
    raw_window_cap_truncated: bool,
    cap_truncation_deactivation_guard_active: bool,
    cap_truncation_deactivation_guard_reason: Option<&'static str>,
    cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    cap_truncation_floor_signature: Option<String>,
}

fn snapshot_cap_truncation_telemetry(
    state: &DiscoveryWindowState,
    followlist_deactivations_suppressed: bool,
) -> CapTruncationTelemetrySnapshot {
    CapTruncationTelemetrySnapshot {
        raw_window_cap_truncated: state.cap_truncation_floor.is_some(),
        cap_truncation_deactivation_guard_active: followlist_deactivations_suppressed,
        cap_truncation_deactivation_guard_reason: state
            .cap_truncation_deactivation_guard_reason
            .map(CapTruncationDeactivationGuardReason::as_str),
        cap_truncation_deactivation_guard_started_at: state
            .cap_truncation_deactivation_guard_started_at,
        cap_truncation_floor_ts_utc: state
            .cap_truncation_floor
            .as_ref()
            .map(|floor| floor.ts_utc),
        cap_truncation_floor_signature: state
            .cap_truncation_floor
            .as_ref()
            .map(|floor| floor.signature.clone()),
    }
}

fn raw_window_history_incomplete_for_followlist_or_metrics(state: &DiscoveryWindowState) -> bool {
    state.cap_truncation_floor.is_some()
}
