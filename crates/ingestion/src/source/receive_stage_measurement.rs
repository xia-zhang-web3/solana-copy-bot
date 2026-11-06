//! Typed rendering/clock adapter. The ledger itself has no external dependencies.
use super::{
    receive_stage_ledger::{
        AgeSummary, Distribution, ProviderAge, Stage, StageLedger, AGE_BOUNDS_MS,
        DURATION_BOUNDS_NS,
    },
    telemetry::IngestionTelemetry,
};
use chrono::Utc;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

fn now_unix_ns() -> i128 {
    let now = Utc::now();
    i128::from(now.timestamp()) * 1_000_000_000 + i128::from(now.timestamp_subsec_nanos())
}

pub(super) struct FrameAge {
    stamp: Option<(i64, i32)>,
    pub before_process: ProviderAge,
}
impl FrameAge {
    pub fn after_process(&self) -> ProviderAge {
        ProviderAge::at(self.stamp, now_unix_ns())
    }
}

pub(super) fn decoded_frame_age(
    update: &SubscribeUpdate,
    telemetry: &IngestionTelemetry,
) -> FrameAge {
    let stamp = update
        .created_at
        .as_ref()
        .map(|value| (value.seconds, value.nanos));
    let before_process = ProviderAge::at(stamp, now_unix_ns());
    telemetry.receive_stages.decoded_age(
        matches!(
            update.update_oneof.as_ref(),
            Some(UpdateOneof::Transaction(_))
        ),
        before_process,
    );
    FrameAge {
        stamp,
        before_process,
    }
}

fn distribution(value: Distribution) -> Value {
    json!({"count":value.count,"sum":value.sum,"max":value.max,"histogram":value.histogram})
}
fn age(value: AgeSummary) -> Value {
    json!({"absent":value.absent,"invalid":value.invalid,"future":value.future,
        "available_ms":distribution(value.available_ms),"future_ahead_ms":distribution(value.future_ahead_ms)})
}

fn wake(ledger: &StageLedger) -> Value {
    let Some(s) = ledger.wake.snapshot() else {
        return json!({"available":false,"reason":"poisoned_wake_metrics"});
    };
    let counts: serde_json::Map<String, Value> = super::receive_wake_metrics::COUNTER_NAMES
        .iter()
        .zip(s.counts)
        .map(|(name, value)| ((*name).into(), json!(value)))
        .collect();
    json!({"available":true,"schema":1,"basis":"consumer notification, not socket/message readiness or provider send",
        "registration_pool_size":super::receive_wake::SLOT_COUNT,
        "counts":counts,"active_pending":s.active_pending,"snapshot_atomic_with_stage_rows":false,
        "first_wake_basis":"first notification serialized by observer state lock; immutable registration epoch",
        "pending_to_first_wake_ns":distribution(s.pending_to_first_wake_ns),
        "first_wake_to_repoll_ns":distribution(s.first_wake_to_repoll_ns),
        "pre_pending_wake_to_repoll_ns":distribution(s.pre_pending_wake_to_repoll_ns),
        "missing_or_ambiguous_intervals":"excluded from valid pairs; see counters; pre-Pending separate",
        "repoll_boundary":"next external poll entry; result counted Pending/Ready; no cause attribution"})
}

pub(super) fn snapshot_json(ledger: &StageLedger) -> String {
    let Some(snapshot) = ledger.snapshot() else {
        return json!({"schema":1,"available":false,"reason":"poisoned_ledger_mutex"}).to_string();
    };
    let stages = Stage::ALL.map(|stage| json!({"stage":stage.name(),
        "parent":stage.parent().map(Stage::name),"closed_ns":distribution(snapshot.stages[stage as usize]),
        "active":snapshot.active[stage as usize]}));
    json!({"schema":1,"available":true,"scope":"legacy serial receive loop; cumulative since source creation",
        "nested_spans_additive":false,"closed_spans_only":true,
        "closed_includes_error_or_cancellation_drop":true,
        "snapshot_can_cut_active_parent_spans":true,
        "duration_histogram_inclusive_upper_bounds_ns":DURATION_BOUNDS_NS,
        "age_histogram_inclusive_upper_bounds_ms":AGE_BOUNDS_MS,"last_bucket":"above final bound",
        "distribution_kind":"fixed buckets, approximate distribution; no exact p95",
        "next_total_basis":"combined network/decode/scheduling wait",
        "next_poll_execution_basis":"one delegated poll invocation wall execution including preemption, not CPU/protobuf only",
        "stage_rows":stages,"poll_pending":snapshot.poll_pending,"poll_ready":snapshot.poll_ready,
        "consumer_wake":wake(ledger),
        "provider_timestamp_basis":"SubscribeUpdate.created_at metadata, not chain blocktime",
        "age_units":"ms","decoded_updates_age":age(snapshot.decoded_updates_age),
        "decoded_transactions_age":age(snapshot.decoded_transactions_age),
        "enqueued_process_age_cohort":"same frames whose parsed observations were added/replaced in output queue",
        "enqueued_pairs":snapshot.enqueued_pairs,
        "enqueued_before_process_age":age(snapshot.enqueued_before_process_age),
        "enqueued_after_process_age":age(snapshot.enqueued_after_process_age),
        "quantile_subtraction_valid":false,"tokio_worker_count":"UNKNOWN"}).to_string()
}
