use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use chrono::Utc;

pub(crate) fn quote_age_ms_at_build(metadata: &ExecutionBuildPlanMetadata) -> Option<i64> {
    let request_ts = metadata.http_request_started_ts?;
    let now = Utc::now();
    (request_ts <= now).then(|| (now - request_ts).num_milliseconds())
}

pub(crate) fn quote_age_ms_summary_field(metadata: &ExecutionBuildPlanMetadata) -> String {
    quote_age_ms_at_build(metadata)
        .map(|age| format!(" quote_age_ms_at_build={age}"))
        .unwrap_or_else(|| " quote_age_ms_at_build=unknown".to_string())
}
