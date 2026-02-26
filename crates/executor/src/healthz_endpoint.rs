use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use tracing::warn;

use crate::healthz_payload::{build_healthz_payload, HealthzPayloadInputs};
use crate::AppState;

pub(crate) async fn healthz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let idempotency_store_status = match state.idempotency.probe() {
        Ok(()) => "ok",
        Err(error) => {
            warn!(
                error = %error,
                "idempotency store probe failed in healthz"
            );
            "degraded"
        }
    };
    Json(build_healthz_payload(HealthzPayloadInputs {
        contract_version: state.config.contract_version.as_str(),
        enabled_routes: &state.config.route_allowlist,
        signer_source: state.config.signer_source.as_str(),
        signer_kms_key_id_configured: state.config.signer_kms_key_id.is_some(),
        signer_keypair_file_configured: state.config.signer_keypair_file.is_some(),
        submit_fastlane_enabled: state.config.submit_fastlane_enabled,
        idempotency_store_status,
        signer_pubkey: state.config.signer_pubkey.as_str(),
    }))
}
