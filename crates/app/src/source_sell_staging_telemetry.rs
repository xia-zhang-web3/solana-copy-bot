use copybot_storage_core::ExecutionSourceSellReject as Reject;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageNotice {
    Scheduled,
    WorkerCapacity,
    InFlight,
    IdentityConflict,
    GenerationUnknown,
    HintFailed,
    PersistenceFailed,
    Staged,
    Existing,
    Rejected(Reject),
    WorkerFailed,
    WorkerPanic,
    WorkerJoinFailed,
    RetryableSqlite,
    FatalSqlite,
}

impl StageNotice {
    fn reason(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::WorkerCapacity => "worker_capacity",
            Self::InFlight => "already_in_flight",
            Self::IdentityConflict => "event_identity_conflict",
            Self::GenerationUnknown => "original_generation_unknown",
            Self::HintFailed => "position_hint_failed",
            Self::PersistenceFailed => "observed_ack_failed",
            Self::Staged => "staged",
            Self::Existing => "existing",
            Self::WorkerFailed => "staging_failed",
            Self::WorkerPanic => "worker_panicked",
            Self::WorkerJoinFailed => "worker_join_failed",
            Self::RetryableSqlite => "retryable_sqlite",
            Self::FatalSqlite => "fatal_sqlite",
            Self::Rejected(reason) => match reason {
                Reject::InvalidSell => "invalid_sell",
                Reject::ObservedEventMismatch => "observed_event_mismatch",
                Reject::NoOwnedPosition => "no_owned_position",
                Reject::GenerationMismatch => "generation_mismatch",
                Reject::SellBeforePosition => "sell_before_position",
                Reject::SellBeforeLatestBuy => "sell_before_latest_buy",
                Reject::ShadowRiskPresent => "shadow_risk_present",
                Reject::SignalAlreadyExists => "signal_already_exists",
                Reject::SourceNotProven => "source_not_proven",
                Reject::StagedEventConflict => "staged_event_conflict",
                Reject::WitnessNoLongerProven => "witness_no_longer_proven",
            },
        }
    }
}

pub(crate) fn record(signature: &str, notice: StageNotice, error: Option<&anyhow::Error>) {
    // One event per outcome. A successful B never overwrites the identity of A.
    // Bounded reason vocabulary and bounded error text; no SQL/report side effects.
    let detail: String = error
        .map(|e| format!("{e:#}").chars().take(512).collect())
        .unwrap_or_default();
    if matches!(
        notice,
        StageNotice::Scheduled | StageNotice::Staged | StageNotice::Existing
    ) {
        tracing::debug!(target: "copybot_app::source_sell_staging", signature, reason = notice.reason(), "source SELL staging delivery");
    } else {
        tracing::warn!(target: "copybot_app::source_sell_staging", signature, reason = notice.reason(), detail, "source SELL staging delivery");
    }
}
