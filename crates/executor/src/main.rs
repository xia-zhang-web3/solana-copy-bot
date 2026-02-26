use anyhow::{anyhow, Context, Result};
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
#[cfg(test)]
use axum::http::{HeaderMap, StatusCode};
use reqwest::Client;
#[cfg(test)]
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    env,
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod auth_crypto;
mod auth_mode;
mod auth_verifier;
mod common_contract;
mod contract_version;
mod executor_config_env;
mod env_parsing;
mod fee_hints;
mod healthz_endpoint;
mod healthz_payload;
mod http_utils;
mod idempotency;
mod idempotency_cleanup_worker;
mod key_validation;
mod request_ingress;
mod request_endpoints;
mod reject;
mod reject_mapping;
mod request_validation;
mod request_types;
mod response_envelope;
mod rfc3339_time;
mod route_allowlist;
mod route_adapters;
mod route_backend;
mod route_executor;
mod route_normalization;
mod route_policy;
mod secret_source;
mod secret_value;
mod send_rpc;
mod signer_source;
mod simulate_handler;
mod simulate_response;
mod submit_budget;
mod submit_claim_guard;
mod submit_deadline;
mod submit_handler;
mod submit_payload;
mod submit_response;
mod submit_transport;
mod submit_verify;
mod submit_verify_config;
mod submit_verify_payload;
mod tx_build;
mod upstream_forward;
mod upstream_outcome;

use crate::auth_verifier::AuthVerifier;
#[cfg(test)]
use crate::common_contract::{validate_common_contract_inputs, CommonContractInputs};
use crate::env_parsing::parse_bool_env;
use crate::healthz_endpoint::healthz;
use crate::idempotency::SubmitIdempotencyStore;
use crate::idempotency_cleanup_worker::spawn_response_cleanup_worker;
#[cfg(test)]
use crate::idempotency::{
    DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE, DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
};
#[cfg(test)]
use crate::idempotency_cleanup_worker::response_cleanup_worker_tick_sec;
#[cfg(test)]
use crate::key_validation::{validate_pubkey_like, validate_signature_like};
use crate::request_endpoints::simulate;
use crate::request_endpoints::submit;
pub(crate) use crate::reject::Reject;
#[cfg(test)]
use crate::reject_mapping::map_common_contract_validation_error_to_reject;
#[cfg(test)]
use crate::reject_mapping::simulate_http_status_for_reject;
#[cfg(test)]
use crate::request_types::{ComputeBudgetRequest, SimulateRequest, SubmitRequest};
use crate::route_backend::RouteBackend;
#[cfg(test)]
use crate::route_backend::UpstreamAction;
#[cfg(test)]
use crate::route_adapters::{
    clear_submit_instruction_plan_presence_for_test,
    take_submit_instruction_plan_presence_for_test,
};
#[cfg(test)]
use crate::route_executor::{
    execute_route_action, RouteActionPayloadExpectations, RouteSubmitExecutionContext,
};
#[cfg(test)]
use crate::route_policy::apply_submit_tip_policy;
#[cfg(test)]
use crate::secret_source::resolve_secret_source;
#[cfg(test)]
use crate::secret_source::secret_file_has_restrictive_permissions;
use crate::secret_value::SecretValue;
#[cfg(test)]
use crate::send_rpc::send_signed_transaction_via_rpc;
#[cfg(test)]
use crate::signer_source::resolve_signer_source_config;
use crate::signer_source::SignerSource;
#[cfg(test)]
use crate::simulate_handler::handle_simulate;
#[cfg(test)]
use crate::submit_budget::default_submit_total_budget_ms;
#[cfg(test)]
use crate::submit_handler::handle_submit;
#[cfg(test)]
use crate::submit_verify::SubmitSignatureVerification;
#[cfg(test)]
use crate::submit_verify::verify_submitted_signature_visibility;
#[cfg(test)]
use crate::submit_verify_config::build_submit_signature_verify_config;
use crate::submit_verify_config::SubmitSignatureVerifyConfig;
#[cfg(test)]
use crate::upstream_forward::forward_to_upstream;
#[cfg(test)]
use crate::upstream_outcome::{parse_upstream_outcome, UpstreamOutcome};
#[cfg(test)]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};

const TIP_MAX_LAMPORTS: u64 = 100_000_000;
const CU_LIMIT_MIN: u32 = 1;
const CU_LIMIT_MAX: u32 = 1_400_000;
const CU_PRICE_MIN: u64 = 1;
const CU_PRICE_MAX: u64 = 10_000_000;
const POLICY_FLOAT_EPSILON: f64 = 1e-6;
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8090";
const DEFAULT_TIMEOUT_MS: u64 = 8_000;
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_NOTIONAL_SOL: f64 = 10.0;
const DEFAULT_BASE_FEE_LAMPORTS: u64 = 5_000;
const DEFAULT_SUBMIT_VERIFY_ATTEMPTS: u64 = 3;
const DEFAULT_SUBMIT_VERIFY_INTERVAL_MS: u64 = 250;
const DEFAULT_IDEMPOTENCY_CLAIM_TTL_SEC: u64 = 60;
const DEFAULT_IDEMPOTENCY_RESPONSE_RETENTION_SEC: u64 = 7 * 24 * 60 * 60;
const DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES: u64 = 100_000;
const DEFAULT_LOG_FILTER: &str = "info,reqwest=warn,hyper=warn,h2=warn";

#[derive(Clone)]
struct AppState {
    config: ExecutorConfig,
    http: Client,
    auth: Arc<AuthVerifier>,
    idempotency: Arc<SubmitIdempotencyStore>,
}

#[derive(Clone)]
struct ExecutorConfig {
    bind_addr: SocketAddr,
    contract_version: String,
    signer_pubkey: String,
    signer_source: SignerSource,
    signer_keypair_file: Option<String>,
    signer_kms_key_id: Option<String>,
    submit_fastlane_enabled: bool,
    route_allowlist: HashSet<String>,
    route_backends: HashMap<String, RouteBackend>,
    bearer_token: Option<SecretValue>,
    hmac_key_id: Option<String>,
    hmac_secret: Option<SecretValue>,
    hmac_ttl_sec: u64,
    hmac_nonce_cache_max_entries: u64,
    request_timeout_ms: u64,
    submit_total_budget_ms: u64,
    idempotency_db_path: String,
    idempotency_claim_ttl_sec: u64,
    idempotency_response_retention_sec: u64,
    idempotency_response_cleanup_batch_size: u64,
    idempotency_response_cleanup_max_batches_per_run: u64,
    idempotency_response_cleanup_worker_tick_sec: u64,
    max_notional_sol: f64,
    allow_nonzero_tip: bool,
    submit_signature_verify: Option<SubmitSignatureVerifyConfig>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_json = parse_bool_env("COPYBOT_EXECUTOR_LOG_JSON", true)?;
    let env_filter = parse_executor_log_env_filter()?;
    if log_json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }

    let mut config = ExecutorConfig::from_env()?;
    let http = Client::builder()
        .timeout(Duration::from_millis(config.request_timeout_ms.max(500)))
        .build()
        .context("failed to build reqwest client")?;
    let auth = Arc::new(AuthVerifier::new(
        config.bearer_token.take(),
        config.hmac_key_id.clone(),
        config.hmac_secret.take(),
        config.hmac_ttl_sec,
        config.hmac_nonce_cache_max_entries,
    ));
    let idempotency = Arc::new(
        SubmitIdempotencyStore::open(config.idempotency_db_path.as_str())
            .context("failed to open idempotency store")?,
    );

    let state = Arc::new(AppState {
        config,
        http,
        auth,
        idempotency,
    });

    let router = build_router(state.clone());

    info!(
        bind_addr = %state.config.bind_addr,
        signer_pubkey = %state.config.signer_pubkey,
        signer_source = %state.config.signer_source.as_str(),
        signer_kms_key_id_configured = state.config.signer_kms_key_id.is_some(),
        signer_keypair_file_configured = state.config.signer_keypair_file.is_some(),
        contract_version = %state.config.contract_version,
        routes = ?state.config.route_allowlist,
        submit_fastlane_enabled = state.config.submit_fastlane_enabled,
        hmac_nonce_cache_max_entries = state.config.hmac_nonce_cache_max_entries,
        idempotency_db_path = %state.config.idempotency_db_path,
        idempotency_claim_ttl_sec = state.config.idempotency_claim_ttl_sec,
        idempotency_response_retention_sec = state.config.idempotency_response_retention_sec,
        idempotency_response_cleanup_batch_size = state.config.idempotency_response_cleanup_batch_size,
        idempotency_response_cleanup_max_batches_per_run = state.config.idempotency_response_cleanup_max_batches_per_run,
        idempotency_response_cleanup_worker_tick_sec = state.config.idempotency_response_cleanup_worker_tick_sec,
        submit_total_budget_ms = state.config.submit_total_budget_ms,
        submit_signature_verify_enabled = state.config.submit_signature_verify.is_some(),
        submit_signature_verify_strict = state
            .config
            .submit_signature_verify
            .as_ref()
            .map(|value| value.strict)
            .unwrap_or(false),
        "copybot executor started"
    );
    let listener = tokio::net::TcpListener::bind(state.config.bind_addr)
        .await
        .context("failed binding executor listener")?;
    let response_cleanup_worker = spawn_response_cleanup_worker(state.clone());
    let server_result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    response_cleanup_worker.abort();
    if let Err(error) = response_cleanup_worker.await {
        if !error.is_cancelled() {
            warn!(
                error = %error,
                "idempotency response cleanup worker terminated unexpectedly"
            );
        }
    }
    server_result.context("executor server crashed")
}

fn parse_executor_log_env_filter() -> Result<EnvFilter> {
    let raw = match env::var("COPYBOT_EXECUTOR_LOG_FILTER") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => DEFAULT_LOG_FILTER.to_string(),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(anyhow!("COPYBOT_EXECUTOR_LOG_FILTER must be valid UTF-8"));
        }
    };
    EnvFilter::try_new(raw.as_str())
        .map_err(|error| anyhow!("COPYBOT_EXECUTOR_LOG_FILTER is invalid: {}", error))
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/simulate", post(simulate))
        .route("/submit", post(submit))
        .layer(DefaultBodyLimit::max(DEFAULT_MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(
                error = %error,
                "failed to install CTRL+C handler; forcing shutdown path"
            );
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "failed to install SIGTERM handler; falling back to CTRL+C only"
                );
                await_shutdown_signal_ctrl_c_only(ctrl_c).await;
                info!("shutdown signal received");
                return;
            }
        };
        await_shutdown_signal_unix(ctrl_c, async move {
            let _ = terminate.recv().await;
        })
        .await;
    }

    #[cfg(not(unix))]
    {
        await_shutdown_signal_ctrl_c_only(ctrl_c).await;
    }

    info!("shutdown signal received");
}

async fn await_shutdown_signal_ctrl_c_only<C>(ctrl_c: C)
where
    C: Future<Output = ()>,
{
    ctrl_c.await;
}

#[cfg(unix)]
async fn await_shutdown_signal_unix<C, T>(ctrl_c: C, sigterm: T)
where
    C: Future<Output = ()>,
    T: Future<Output = ()>,
{
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{HeaderValue, Request},
    };
    use std::{
        ffi::OsString,
        fs as stdfs,
        io::{Read, Write},
        net::TcpListener,
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering},
            Mutex,
        },
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tower::ServiceExt;

    static TEMP_SECRET_COUNTER: AtomicU64 = AtomicU64::new(0);
    static LOG_FILTER_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_log_filter_env<T>(value: Option<OsString>, run: impl FnOnce() -> T) -> T {
        let _guard = LOG_FILTER_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved = env::var_os("COPYBOT_EXECUTOR_LOG_FILTER");
        env::remove_var("COPYBOT_EXECUTOR_LOG_FILTER");
        if let Some(value) = value {
            env::set_var("COPYBOT_EXECUTOR_LOG_FILTER", value);
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        env::remove_var("COPYBOT_EXECUTOR_LOG_FILTER");
        if let Some(saved) = saved {
            env::set_var("COPYBOT_EXECUTOR_LOG_FILTER", saved);
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    #[test]
    fn parse_executor_log_env_filter_uses_default_when_missing() {
        with_log_filter_env(None, || {
            parse_executor_log_env_filter().expect("missing env must use default log filter");
        });
    }

    #[test]
    fn parse_executor_log_env_filter_rejects_invalid_syntax() {
        with_log_filter_env(Some(OsString::from("[")), || {
            let error = parse_executor_log_env_filter().expect_err("invalid filter must reject");
            assert!(
                error.to_string().contains("COPYBOT_EXECUTOR_LOG_FILTER"),
                "unexpected error: {}",
                error
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn parse_executor_log_env_filter_rejects_non_utf8() {
        use std::os::unix::ffi::OsStringExt;

        with_log_filter_env(Some(OsString::from_vec(vec![0xff])), || {
            let error = parse_executor_log_env_filter().expect_err("non-UTF8 filter must reject");
            assert!(
                error.to_string().contains("COPYBOT_EXECUTOR_LOG_FILTER"),
                "unexpected error: {}",
                error
            );
            assert!(
                error.to_string().contains("UTF-8"),
                "unexpected error: {}",
                error
            );
        });
    }

    #[tokio::test]
    async fn shutdown_signal_ctrl_c_helper_completes_when_ctrl_c_source_resolves() {
        let (tx, rx) = oneshot::channel::<()>();
        let waiter = tokio::spawn(async move {
            await_shutdown_signal_ctrl_c_only(async {
                let _ = rx.await;
            })
            .await;
        });
        tx.send(()).expect("send ctrl_c source");
        timeout(Duration::from_millis(200), waiter)
            .await
            .expect("ctrl_c helper should complete quickly")
            .expect("task join must succeed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_signal_unix_helper_completes_when_sigterm_source_resolves() {
        let (tx, rx) = oneshot::channel::<()>();
        let waiter = tokio::spawn(async move {
            await_shutdown_signal_unix(std::future::pending::<()>(), async {
                let _ = rx.await;
            })
            .await;
        });
        tx.send(()).expect("send sigterm source");
        timeout(Duration::from_millis(200), waiter)
            .await
            .expect("unix helper should complete quickly")
            .expect("task join must succeed");
    }

    #[tokio::test]
    async fn router_rejects_oversized_request_body_before_handler() {
        let app = build_router(Arc::new(test_state("http://127.0.0.1:1/upstream")));
        let oversized_payload = format!(
            r#"{{"padding":"{}"}}"#,
            "x".repeat(DEFAULT_MAX_REQUEST_BODY_BYTES + 1024)
        );
        let request = Request::builder()
            .method("POST")
            .uri("/simulate")
            .header("content-type", "application/json")
            .body(Body::from(oversized_payload))
            .expect("request");
        let response = app
            .oneshot(request)
            .await
            .expect("router should produce response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn validate_common_contract_rejects_fastlane_when_feature_disabled() {
        let mut state = test_state("http://127.0.0.1:1/upstream");
        state.config.route_allowlist.insert("fastlane".to_string());
        let reject = validate_common_contract_inputs(CommonContractInputs {
            request_contract_version: Some("v1"),
            expected_contract_version: state.config.contract_version.as_str(),
            route: "fastlane",
            route_allowlist: &state.config.route_allowlist,
            submit_fastlane_enabled: state.config.submit_fastlane_enabled,
            side: "buy",
            token: "11111111111111111111111111111111",
            notional_sol: 1.0,
            max_notional_sol: state.config.max_notional_sol,
        })
        .map_err(map_common_contract_validation_error_to_reject)
        .expect_err("fastlane must be rejected when feature flag is disabled");
        assert_eq!(reject.code, "fastlane_not_enabled");
    }

    #[test]
    fn apply_submit_tip_policy_forces_rpc_tip_to_zero() {
        let (effective, policy_code) = apply_submit_tip_policy("rpc", 12_345);
        assert_eq!(effective, 0);
        assert_eq!(policy_code, Some("rpc_tip_forced_zero"));

        let (effective, policy_code) = apply_submit_tip_policy("jito", 12_345);
        assert_eq!(effective, 12_345);
        assert_eq!(policy_code, None);
    }

    #[test]
    fn parse_upstream_outcome_rejects_unknown_status() {
        let payload = json!({
            "status": "pending",
            "ok": true
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => assert_eq!(reject.code, "upstream_invalid_status"),
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn parse_upstream_outcome_rejects_explicit_reject() {
        let payload = json!({
            "status": "reject",
            "retryable": true,
            "code": "busy",
            "detail": "backpressure"
        });
        match parse_upstream_outcome(&payload, "default") {
            UpstreamOutcome::Reject(reject) => {
                assert!(reject.retryable);
                assert_eq!(reject.code, "busy");
            }
            UpstreamOutcome::Success => panic!("expected reject"),
        }
    }

    #[test]
    fn validate_pubkey_like_requires_32_bytes() {
        let ok = "11111111111111111111111111111111";
        assert!(validate_pubkey_like(ok).is_ok());
        assert!(validate_pubkey_like("not-base58").is_err());
    }

    #[test]
    fn validate_signature_like_requires_64_bytes() {
        let valid = bs58::encode([7u8; 64]).into_string();
        assert!(validate_signature_like(valid.as_str()).is_ok());
        assert!(validate_signature_like("not-base58").is_err());
        let short = bs58::encode([7u8; 32]).into_string();
        assert!(validate_signature_like(short.as_str()).is_err());
    }

    #[test]
    fn resolve_signer_source_config_requires_file_source_materials() {
        let error = resolve_signer_source_config(
            Some("file"),
            None,
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("file source requires keypair file");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be set"));
    }

    #[test]
    fn resolve_signer_source_config_rejects_kms_key_for_file_source() {
        let path = write_temp_signer_keypair_file([0u8; 32]);
        let error = resolve_signer_source_config(
            Some("file"),
            Some(path.to_str().expect("utf8 path")),
            Some("kms-key-id"),
            "11111111111111111111111111111111",
        )
        .expect_err("kms key id must be empty for file source");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be empty"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_signer_source_config_accepts_kms_source() {
        let source = resolve_signer_source_config(
            Some("kms"),
            None,
            Some("projects/p/keys/k"),
            "11111111111111111111111111111111",
        )
        .expect("kms source should validate");
        assert_eq!(source, SignerSource::Kms);
    }

    #[test]
    fn resolve_signer_source_config_rejects_kms_without_key_id() {
        let error = resolve_signer_source_config(
            Some("kms"),
            None,
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("kms source requires key id");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be set"));
    }

    #[test]
    fn resolve_signer_source_config_rejects_unknown_source() {
        let error = resolve_signer_source_config(
            Some("vault"),
            None,
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("unknown source must fail");
        assert!(error
            .to_string()
            .contains("COPYBOT_EXECUTOR_SIGNER_SOURCE must be one of: file,kms"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_signer_source_config_rejects_non_restrictive_keypair_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let path = write_temp_signer_keypair_file([0u8; 32]);
        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o644);
        stdfs::set_permissions(&path, perms).expect("set relaxed mode");
        let error = resolve_signer_source_config(
            Some("file"),
            Some(path.to_str().expect("utf8 path")),
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("broad keypair file permissions must fail");
        assert!(error
            .to_string()
            .contains("must use owner-only permissions"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_signer_source_config_rejects_keypair_pubkey_mismatch() {
        let path = write_temp_signer_keypair_file([1u8; 32]);
        let error = resolve_signer_source_config(
            Some("file"),
            Some(path.to_str().expect("utf8 path")),
            None,
            "11111111111111111111111111111111",
        )
        .expect_err("keypair pubkey mismatch must fail");
        assert!(error.to_string().contains("pubkey mismatch"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_fallback_without_primary() {
        let error = build_submit_signature_verify_config(
            None,
            Some("https://rpc-fallback.example.com".to_string()),
            3,
            250,
            false,
        )
        .expect_err("fallback without primary must fail");
        assert!(error
            .to_string()
            .contains("requires COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL"));
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_same_endpoint_identity() {
        let error = build_submit_signature_verify_config(
            Some("https://RPC.example.com".to_string()),
            Some("https://rpc.example.com:443/".to_string()),
            3,
            250,
            false,
        )
        .expect_err("same primary/fallback identity must fail");
        assert!(error
            .to_string()
            .contains("must resolve to distinct endpoint"));
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_attempts_out_of_range() {
        let error = build_submit_signature_verify_config(
            Some("https://rpc.example.com".to_string()),
            None,
            0,
            250,
            false,
        )
        .expect_err("attempts=0 must fail");
        assert!(error.to_string().contains("ATTEMPTS must be in 1..=20"));
    }

    #[test]
    fn build_submit_signature_verify_config_rejects_interval_out_of_range() {
        let error = build_submit_signature_verify_config(
            Some("https://rpc.example.com".to_string()),
            None,
            3,
            0,
            false,
        )
        .expect_err("interval=0 must fail");
        assert!(error
            .to_string()
            .contains("INTERVAL_MS must be in 1..=60000"));
    }

    #[test]
    fn resolve_secret_source_rejects_inline_and_file_conflict() {
        let error = resolve_secret_source(
            "COPYBOT_EXECUTOR_BEARER_TOKEN",
            Some("inline"),
            "COPYBOT_EXECUTOR_BEARER_TOKEN_FILE",
            Some("/tmp/adapter-bearer.secret"),
        )
        .expect_err("inline + file must fail-closed");
        let message = error.to_string();
        assert!(
            message.contains("COPYBOT_EXECUTOR_BEARER_TOKEN")
                && message.contains("COPYBOT_EXECUTOR_BEARER_TOKEN_FILE")
        );
    }

    #[test]
    fn resolve_secret_source_reads_trimmed_file() {
        let path = write_temp_secret_file(" \nsecret-value\n");
        let resolved = resolve_secret_source(
            "COPYBOT_EXECUTOR_BEARER_TOKEN",
            None,
            "COPYBOT_EXECUTOR_BEARER_TOKEN_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect("file-backed secret must resolve");
        assert_eq!(resolved.as_ref().map(SecretValue::as_str), Some("secret-value"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_secret_source_rejects_empty_file() {
        let path = write_temp_secret_file(" \n\t ");
        let error = resolve_secret_source(
            "COPYBOT_EXECUTOR_HMAC_SECRET",
            None,
            "COPYBOT_EXECUTOR_HMAC_SECRET_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("empty secret file must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_EXECUTOR_HMAC_SECRET_FILE"));
        assert!(message.contains("secret file is empty"));
        cleanup_temp_secret_file(path);
    }

    #[test]
    fn resolve_secret_source_rejects_missing_file() {
        let path = temp_secret_path("missing");
        let error = resolve_secret_source(
            "COPYBOT_EXECUTOR_HMAC_SECRET",
            None,
            "COPYBOT_EXECUTOR_HMAC_SECRET_FILE",
            Some(path.to_str().expect("utf8 path")),
        )
        .expect_err("missing secret file must fail");
        let message = format!("{:#}", error);
        assert!(message.contains("COPYBOT_EXECUTOR_HMAC_SECRET_FILE"));
        assert!(message.contains("secret file not found/readable"));
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_permissions_check_detects_relaxed_mode() {
        use std::os::unix::fs::PermissionsExt;

        let path = write_temp_secret_file("secret");
        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o644);
        stdfs::set_permissions(&path, perms).expect("set relaxed mode");
        assert!(
            !secret_file_has_restrictive_permissions(path.to_str().expect("utf8 path"))
                .expect("permission check"),
            "0644 should be flagged as broad permissions"
        );

        let mut perms = stdfs::metadata(&path)
            .expect("stat temp secret")
            .permissions();
        perms.set_mode(0o600);
        stdfs::set_permissions(&path, perms).expect("set strict mode");
        assert!(
            secret_file_has_restrictive_permissions(path.to_str().expect("utf8 path"))
                .expect("permission check"),
            "0600 should pass restrictive permission check"
        );

        cleanup_temp_secret_file(path);
    }

    #[tokio::test]
    async fn auth_verifier_rejects_wrong_bearer_token() {
        let verifier = AuthVerifier::new(
            Some("correct-token".to_string().into()),
            None,
            None,
            30,
            DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer wrong-token"),
        );
        let reject = verifier
            .verify(&headers, b"{}")
            .await
            .expect_err("wrong bearer token must fail");
        assert_eq!(reject.code, "auth_invalid");
    }

    #[tokio::test]
    async fn auth_verifier_accepts_correct_bearer_token() {
        let verifier = AuthVerifier::new(
            Some("correct-token".to_string().into()),
            None,
            None,
            30,
            DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer correct-token"),
        );
        verifier
            .verify(&headers, b"{\"ping\":true}")
            .await
            .expect("correct bearer token should pass");
    }

    #[tokio::test]
    async fn handle_simulate_rejects_empty_request_id() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "   ".to_string(),
            signal_id: "signal-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let reject = handle_simulate(&state, &request, b"{}")
            .await
            .expect_err("empty request_id must fail");
        assert_eq!(reject.code, "invalid_request_id");
    }

    #[tokio::test]
    async fn handle_simulate_rejects_empty_signal_id() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-1".to_string(),
            signal_id: " ".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let reject = handle_simulate(&state, &request, b"{}")
            .await
            .expect_err("empty signal_id must fail");
        assert_eq!(reject.code, "invalid_signal_id");
    }

    #[tokio::test]
    async fn handle_simulate_rejects_route_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-route-payload-mismatch-1".to_string(),
            signal_id: "signal-sim-route-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-route-payload-mismatch-1","signal_id":"signal-sim-route-payload-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"jito","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate route payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_dry_run_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-dry-run-payload-mismatch-1".to_string(),
            signal_id: "signal-sim-dry-run-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-dry-run-payload-mismatch-1","signal_id":"signal-sim-dry-run-payload-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":false}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate dry_run payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("dry_run mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_action_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-action-payload-mismatch-1".to_string(),
            signal_id: "signal-sim-action-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"submit","contract_version":"v1","request_id":"request-sim-action-payload-mismatch-1","signal_id":"signal-sim-action-payload-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate action payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_contract_version_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-contract-payload-mismatch-1".to_string(),
            signal_id: "signal-sim-contract-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v2","request_id":"request-sim-contract-payload-mismatch-1","signal_id":"signal-sim-contract-payload-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate contract_version payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_non_string_contract_version_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-contract-type-mismatch-1".to_string(),
            signal_id: "signal-sim-contract-type-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":123,"request_id":"request-sim-contract-type-mismatch-1","signal_id":"signal-sim-contract-type-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate non-string contract_version payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be string"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_non_string_request_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-id-type-mismatch-1".to_string(),
            signal_id: "signal-sim-id-type-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":123,"signal_id":"signal-sim-id-type-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate non-string request_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id must be string"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_empty_request_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-id-empty-1".to_string(),
            signal_id: "signal-sim-id-empty-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":" ","signal_id":"signal-sim-id-empty-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate empty request_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id must be non-empty"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_signal_id_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-signal-mismatch-1".to_string(),
            signal_id: "signal-sim-expected-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-signal-mismatch-1","signal_id":"signal-sim-other-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate signal_id payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_missing_signal_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-signal-missing-1".to_string(),
            signal_id: "signal-sim-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-signal-missing-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate missing signal_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing signal_id"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_request_id_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-expected-1".to_string(),
            signal_id: "signal-sim-request-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-other-1","signal_id":"signal-sim-request-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate request_id payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_missing_request_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-id-missing-1".to_string(),
            signal_id: "signal-sim-id-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","signal_id":"signal-sim-id-missing-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate missing request_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing request_id"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_side_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-side-mismatch-1".to_string(),
            signal_id: "signal-sim-side-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-side-mismatch-1","signal_id":"signal-sim-side-mismatch-1","side":"sell","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate side payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("side mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_missing_side_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-side-missing-1".to_string(),
            signal_id: "signal-sim-side-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-side-missing-1","signal_id":"signal-sim-side-missing-1","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate missing side payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing side"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_token_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-token-mismatch-1".to_string(),
            signal_id: "signal-sim-token-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-token-mismatch-1","signal_id":"signal-sim-token-mismatch-1","side":"buy","token":"22222222222222222222222222222222","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate token payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("token mismatch"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_missing_token_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-sim-token-missing-1".to_string(),
            signal_id: "signal-sim-token-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-sim-token-missing-1","signal_id":"signal-sim-token-missing-1","side":"buy","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("simulate missing token payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing token"));
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_route_mismatch() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"route":"jito"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-route-mismatch-1".to_string(),
            signal_id: "signal-route-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-route-mismatch-1","signal_id":"signal-route-mismatch-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("upstream route mismatch must reject");
        assert_eq!(reject.code, "simulation_route_mismatch");
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_contract_version_type_invalid() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"contract_version":123}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-sim-response-contract-type-1".to_string(),
            signal_id: "signal-invalid-sim-response-contract-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-sim-response-contract-type-1","signal_id":"signal-invalid-sim-response-contract-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-string upstream contract_version must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "simulation_invalid_response");
        assert!(
            reject
                .detail
                .contains("contract_version must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_route_null() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"route":null}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-sim-response-route-null-1".to_string(),
            signal_id: "signal-invalid-sim-response-route-null-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-sim-response-route-null-1","signal_id":"signal-invalid-sim-response-route-null-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("null upstream route must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "simulation_invalid_response");
        assert!(
            reject
                .detail
                .contains("route must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_detail_type_invalid() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"detail":123}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-sim-response-detail-type-1".to_string(),
            signal_id: "signal-invalid-sim-response-detail-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-sim-response-detail-type-1","signal_id":"signal-invalid-sim-response-detail-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-string upstream detail must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "simulation_invalid_response");
        assert!(
            reject
                .detail
                .contains("detail must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_reject_code_type_invalid() {
        let upstream_body =
            r#"{"status":"reject","ok":false,"accepted":false,"retryable":true,"code":123,"detail":"upstream busy"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-reject-code-type-1".to_string(),
            signal_id: "signal-invalid-upstream-reject-code-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-reject-code-type-1","signal_id":"signal-invalid-upstream-reject-code-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-string upstream reject code must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream reject code must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_retryable_type_invalid() {
        let upstream_body =
            r#"{"status":"reject","ok":false,"accepted":false,"retryable":"true","code":"upstream_busy","detail":"upstream busy"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-reject-retryable-type-1".to_string(),
            signal_id: "signal-invalid-upstream-reject-retryable-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-reject-retryable-type-1","signal_id":"signal-invalid-upstream-reject-retryable-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-bool upstream reject retryable must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream reject retryable must be boolean when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_retryable_null() {
        let upstream_body =
            r#"{"status":"reject","ok":false,"accepted":false,"retryable":null,"code":"upstream_busy","detail":"upstream busy"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-reject-retryable-null-1".to_string(),
            signal_id: "signal-invalid-upstream-reject-retryable-null-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-reject-retryable-null-1","signal_id":"signal-invalid-upstream-reject-retryable-null-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("null upstream reject retryable must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream reject retryable must be boolean when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_status_type_invalid() {
        let upstream_body = r#"{"status":123,"ok":true,"accepted":true}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-status-type-1".to_string(),
            signal_id: "signal-invalid-upstream-status-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-status-type-1","signal_id":"signal-invalid-upstream-status-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-string upstream status must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream status must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_ok_type_invalid() {
        let upstream_body = r#"{"status":"ok","ok":"true","accepted":true}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-ok-type-1".to_string(),
            signal_id: "signal-invalid-upstream-ok-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-ok-type-1","signal_id":"signal-invalid-upstream-ok-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-bool upstream ok must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream ok must be boolean when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_accepted_type_invalid() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":"true"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-accepted-type-1".to_string(),
            signal_id: "signal-invalid-upstream-accepted-type-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-accepted-type-1","signal_id":"signal-invalid-upstream-accepted-type-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("non-bool upstream accepted must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream accepted must be boolean when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_conflicting_status_flags() {
        let upstream_body = r#"{"status":"ok","ok":false,"accepted":true}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-status-flags-conflict-1".to_string(),
            signal_id: "signal-invalid-upstream-status-flags-conflict-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-status-flags-conflict-1","signal_id":"signal-invalid-upstream-status-flags-conflict-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("conflicting upstream status flags must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream status=ok conflicts with reject flags"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_conflicting_reject_status_flags() {
        let upstream_body = r#"{"status":"reject","ok":true,"accepted":false}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-reject-status-flags-conflict-1".to_string(),
            signal_id: "signal-invalid-upstream-reject-status-flags-conflict-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-reject-status-flags-conflict-1","signal_id":"signal-invalid-upstream-reject-status-flags-conflict-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("conflicting reject-status upstream flags must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream status=reject conflicts with success flags"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_upstream_conflicting_ok_accepted_without_status() {
        let upstream_body = r#"{"ok":true,"accepted":false}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-missing-status-flags-conflict-1".to_string(),
            signal_id: "signal-invalid-upstream-missing-status-flags-conflict-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-missing-status-flags-conflict-1","signal_id":"signal-invalid-upstream-missing-status-flags-conflict-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("conflicting upstream flags without status must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_response");
        assert!(
            reject
                .detail
                .contains("upstream ok/accepted flags conflict when status is missing"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_unknown_upstream_status_even_with_reject_flags() {
        let upstream_body =
            r#"{"status":"pending","ok":false,"accepted":false,"code":"busy","detail":"backpressure"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-unknown-status-reject-flags-1".to_string(),
            signal_id: "signal-invalid-upstream-unknown-status-reject-flags-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-unknown-status-reject-flags-1","signal_id":"signal-invalid-upstream-unknown-status-reject-flags-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("unknown status must reject even with reject flags");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_unknown_upstream_status_even_with_invalid_ok_type() {
        let upstream_body = r#"{"status":"pending","ok":"true","accepted":true}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-unknown-status-invalid-ok-1".to_string(),
            signal_id: "signal-invalid-upstream-unknown-status-invalid-ok-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-unknown-status-invalid-ok-1","signal_id":"signal-invalid-upstream-unknown-status-invalid-ok-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("unknown status must win over malformed ok flag");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_unknown_upstream_status_even_with_invalid_accepted_type() {
        let upstream_body = r#"{"status":"pending","ok":true,"accepted":"true"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-unknown-status-invalid-accepted-1".to_string(),
            signal_id: "signal-invalid-upstream-unknown-status-invalid-accepted-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-unknown-status-invalid-accepted-1","signal_id":"signal-invalid-upstream-unknown-status-invalid-accepted-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("unknown status must win over malformed accepted flag");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_unknown_upstream_status_before_retryable_type_validation() {
        let upstream_body = r#"{"status":"pending","ok":false,"accepted":false,"retryable":"true"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-unknown-status-invalid-retryable-1".to_string(),
            signal_id: "signal-invalid-upstream-unknown-status-invalid-retryable-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-unknown-status-invalid-retryable-1","signal_id":"signal-invalid-upstream-unknown-status-invalid-retryable-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("unknown status must win before retryable type validation");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_unknown_upstream_status_before_reject_code_type_validation() {
        let upstream_body =
            r#"{"status":"pending","ok":false,"accepted":false,"retryable":false,"code":123,"detail":"busy"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-unknown-status-invalid-reject-code-1".to_string(),
            signal_id: "signal-invalid-upstream-unknown-status-invalid-reject-code-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-unknown-status-invalid-reject-code-1","signal_id":"signal-invalid-upstream-unknown-status-invalid-reject-code-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("unknown status must win before reject code type validation");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_simulate_rejects_unknown_upstream_status_before_reject_detail_type_validation() {
        let upstream_body =
            r#"{"status":"pending","ok":false,"accepted":false,"retryable":false,"code":"busy","detail":null}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };
        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let request = SimulateRequest {
            action: Some("simulate".to_string()),
            contract_version: Some("v1".to_string()),
            request_id: "request-invalid-upstream-unknown-status-invalid-reject-detail-1".to_string(),
            signal_id: "signal-invalid-upstream-unknown-status-invalid-reject-detail-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            dry_run: Some(true),
        };
        let raw_body = br#"{"action":"simulate","contract_version":"v1","request_id":"request-invalid-upstream-unknown-status-invalid-reject-detail-1","signal_id":"signal-invalid-upstream-unknown-status-invalid-reject-detail-1","side":"buy","token":"11111111111111111111111111111111","notional_sol":1.0,"signal_ts":"2026-02-24T12:00:00Z","route":"rpc","dry_run":true}"#;
        let reject = handle_simulate(&state, &request, raw_body.as_slice())
            .await
            .expect_err("unknown status must win before reject detail type validation");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_empty_signal_id() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            signal_id: " ".to_string(),
            client_order_id: "client-1".to_string(),
            request_id: "request-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 1.0,
            signal_ts: "2026-02-24T12:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 50.0,
            route_slippage_cap_bps: 50.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let reject = handle_submit(&state, &request, b"{}")
            .await
            .expect_err("empty signal_id must fail");
        assert_eq!(reject.code, "invalid_signal_id");
    }

    #[test]
    fn simulate_reject_status_is_http_200_for_retryable_and_terminal() {
        let reject = Reject::retryable("busy", "upstream temporary issue");
        assert_eq!(simulate_http_status_for_reject(&reject), StatusCode::OK);
        let reject = Reject::terminal("invalid", "bad request");
        assert_eq!(simulate_http_status_for_reject(&reject), StatusCode::OK);
    }

    #[tokio::test]
    async fn forward_to_upstream_treats_plaintext_503_as_retryable() {
        let Some((url, handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary upstream outage")
        else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("503 upstream should be retryable");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_http_unavailable");
        assert!(
            reject.detail.contains("temporary upstream outage"),
            "detail={}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_truncates_large_http_error_body_detail() {
        let tail_marker = "TAIL_MARKER_MUST_NOT_LEAK";
        let long_body = format!(
            "{}{}",
            "x".repeat(crate::http_utils::MAX_HTTP_ERROR_BODY_READ_BYTES + 512),
            tail_marker
        );
        let Some((url, handle)) = spawn_one_shot_upstream_raw(503, "text/plain", long_body.as_str())
        else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("503 upstream should be retryable");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_http_unavailable");
        assert!(
            reject.detail.contains("...[truncated]"),
            "detail should mark truncation: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked body tail marker: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_oversized_json_response_body() {
        let tail_marker = "UPSTREAM_JSON_TAIL_MARKER_MUST_NOT_LEAK";
        let large_padding = "u".repeat(crate::http_utils::MAX_HTTP_JSON_BODY_READ_BYTES + 1024);
        let upstream_body = format!(
            r#"{{"status":"ok","accepted":true,"padding":"{}{}"}}"#,
            large_padding, tail_marker
        );
        let Some((url, handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("oversized upstream JSON should fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_response_too_large");
        assert!(
            reject.detail.contains("exceeded max bytes"),
            "detail should report response-too-large: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked upstream json tail marker: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_keeps_invalid_json_classification_with_marker_suffix() {
        let Some((url, handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            r#"{"status":"ok"}...[truncated]"#,
        ) else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("invalid JSON should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_json");
        assert!(
            !reject.detail.contains("exceeded max bytes"),
            "detail should not classify as oversized: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_invalid_utf8_json_response_body() {
        let upstream_body = b"{\"status\":\"ok\",\"accepted\":true,\"detail\":\"\xFF\"}";
        let Some((url, handle)) =
            spawn_one_shot_upstream_raw_bytes(200, "application/json", upstream_body)
        else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("invalid UTF-8 JSON should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_json");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_classifies_incomplete_json_body_as_response_read_failed() {
        let Some((url, handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            b"{\"status\":\"ok\"",
            64,
        ) else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("incomplete JSON body must classify as response read failure");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_unavailable");
        assert!(
            reject.detail.contains("response read failed"),
            "detail={}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_missing_route_backend_with_specific_code() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let reject = forward_to_upstream(&state, "jito", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("missing route backend should fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "route_backend_not_configured");
        assert!(reject.detail.contains("not configured"));
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_partial_valid_json_body_as_response_read_failed() {
        let partial_valid_json = br#"{"status":"ok","accepted":true}"#;
        let Some((url, handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            partial_valid_json,
            partial_valid_json.len() + 64,
        ) else {
            return;
        };
        let state = test_state(url.as_str());
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect_err("transport-incomplete body must reject even if partial bytes are valid JSON");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_unavailable");
        assert!(
            reject.detail.contains("response read failed"),
            "detail={}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_after_primary_response_read_failure() {
        let partial_valid_json = br#"{"status":"ok","accepted":true}"#;
        let Some((primary_url, primary_handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            partial_valid_json,
            partial_valid_json.len() + 64,
        ) else {
            return;
        };
        let Some((fallback_url, fallback_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            "{\"status\":\"ok\",\"accepted\":true}",
        ) else {
            return;
        };

        let state = test_state_with_backends(
            primary_url.as_str(),
            Some(fallback_url.as_str()),
            primary_url.as_str(),
            Some(fallback_url.as_str()),
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let body = forward_to_upstream(
            &state,
            "rpc",
            UpstreamAction::Submit,
            b"{}",
            Some(&submit_deadline),
        )
        .await
        .expect("fallback should succeed after primary response-read failure");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_submit_without_deadline_before_request() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let reject = forward_to_upstream(&state, "rpc", UpstreamAction::Submit, b"{}", None)
            .await
            .expect_err("submit without deadline must fail closed before request");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn forward_to_upstream_rejects_simulate_with_deadline_before_request() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = forward_to_upstream(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            b"{}",
            Some(&submit_deadline),
        )
        .await
        .expect_err("simulate with submit deadline must fail closed before request");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("must not include submit deadline"));
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_after_primary_send_error() {
        let Some((fallback_url, handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            "{\"status\":\"ok\",\"accepted\":true}",
        ) else {
            return;
        };
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            Some(fallback_url.as_str()),
            "http://127.0.0.1:1/upstream",
            Some(fallback_url.as_str()),
        );
        let body = forward_to_upstream(&state, "rpc", UpstreamAction::Simulate, b"{}", None)
            .await
            .expect("fallback should succeed after primary send error");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_after_primary_retryable_status() {
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary outage")
        else {
            return;
        };
        let Some((fallback_url, fallback_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            "{\"status\":\"ok\",\"accepted\":true}",
        ) else {
            return;
        };

        let state = test_state_with_backends(
            primary_url.as_str(),
            Some(fallback_url.as_str()),
            primary_url.as_str(),
            Some(fallback_url.as_str()),
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let body = forward_to_upstream(
            &state,
            "rpc",
            UpstreamAction::Submit,
            b"{}",
            Some(&submit_deadline),
        )
            .await
            .expect("fallback should succeed after retryable status");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_does_not_fallback_after_primary_terminal_status() {
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(401, "text/plain", "unauthorized")
        else {
            return;
        };

        let state = test_state_with_backends(
            primary_url.as_str(),
            Some("http://127.0.0.1:1/upstream"),
            primary_url.as_str(),
            Some("http://127.0.0.1:1/upstream"),
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = forward_to_upstream(
            &state,
            "rpc",
            UpstreamAction::Submit,
            b"{}",
            Some(&submit_deadline),
        )
        .await
        .expect_err("terminal status should short-circuit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_http_rejected");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn forward_to_upstream_uses_fallback_auth_token_when_retrying() {
        let fallback_token = "FallBack-Token-123";
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "temporary outage")
        else {
            return;
        };
        let Some((fallback_url, fallback_handle)) =
            spawn_one_shot_upstream_expect_bearer(fallback_token)
        else {
            return;
        };

        let mut state = test_state_with_backends(
            primary_url.as_str(),
            Some(fallback_url.as_str()),
            primary_url.as_str(),
            Some(fallback_url.as_str()),
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.primary_auth_token = Some("primary-token".to_string().into());
            backend.fallback_auth_token = Some(fallback_token.to_string().into());
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let body = forward_to_upstream(
            &state,
            "rpc",
            UpstreamAction::Submit,
            b"{}",
            Some(&submit_deadline),
        )
            .await
            .expect("fallback with dedicated token should succeed");
        assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_missing_deadline_before_request() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([42u8; 64]);
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );

        let reject = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect_err("send RPC without deadline must fail closed before request");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_missing_route_backend_with_specific_code() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([44u8; 64]);
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "jito",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("missing route backend should fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "route_backend_not_configured");
        assert!(reject.detail.contains("not configured"));
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_missing_deadline_before_topology_check() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([43u8; 64]);
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = None;
            backend.send_rpc_fallback_url =
                Some("http://127.0.0.1:1/send-rpc-fallback".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let reject = send_signed_transaction_via_rpc(&state, "rpc", signed_tx_base64.as_str(), None)
            .await
            .expect_err("deadline guard must reject before topology checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_truncates_large_http_error_body_detail() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([52u8; 64]);
        let tail_marker = "SEND_RPC_HTTP_TAIL_MARKER_MUST_NOT_LEAK";
        let long_body = format!(
            "{}{}",
            "h".repeat(crate::http_utils::MAX_HTTP_ERROR_BODY_READ_BYTES + 512),
            tail_marker
        );
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", long_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("large send-rpc HTTP body should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "send_rpc_http_unavailable");
        assert!(
            reject.detail.contains("...[truncated]"),
            "detail should mark truncation: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked send-rpc HTTP tail marker: {}",
            reject.detail
        );
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_truncates_large_error_payload_detail() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([53u8; 64]);
        let tail_marker = "SEND_RPC_PAYLOAD_TAIL_MARKER_MUST_NOT_LEAK";
        let long_message = format!(
            "{}{}",
            "p".repeat(crate::http_utils::MAX_HTTP_ERROR_BODY_READ_BYTES + 512),
            tail_marker
        );
        let rpc_body = format!(
            r#"{{"jsonrpc":"2.0","error":{{"code":-32002,"message":"{}"}}}}"#,
            long_message
        );
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("large send-rpc error payload should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_error_payload_terminal");
        assert!(
            reject.detail.contains("...[truncated]"),
            "detail should mark truncation: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked send-rpc payload tail marker: {}",
            reject.detail
        );
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_oversized_json_response_body() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([56u8; 64]);
        let tail_marker = "SEND_RPC_JSON_TAIL_MARKER_MUST_NOT_LEAK";
        let large_padding = "j".repeat(crate::http_utils::MAX_HTTP_JSON_BODY_READ_BYTES + 1024);
        let rpc_body = format!(
            r#"{{"jsonrpc":"2.0","result":"{}","padding":"{}{}"}}"#,
            bs58::encode([57u8; 64]).into_string(),
            large_padding,
            tail_marker
        );
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("oversized JSON response should fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_response_too_large");
        assert!(
            reject.detail.contains("exceeded max bytes"),
            "detail should report response-too-large: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked send-rpc json tail marker: {}",
            reject.detail
        );
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_keeps_invalid_json_classification_with_marker_suffix() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([58u8; 64]);
        let rpc_body = r#"{"jsonrpc":"2.0","result":"abc"}...[truncated]"#;
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body)
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("invalid JSON should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_invalid_json");
        assert!(
            !reject.detail.contains("exceeded max bytes"),
            "detail should not classify as oversized: {}",
            reject.detail
        );
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_invalid_utf8_json_response_body() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([59u8; 64]);
        let rpc_body = b"{\"jsonrpc\":\"2.0\",\"result\":\"\xFF\"}";
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw_bytes(200, "application/json", rpc_body)
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("invalid UTF-8 JSON should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_invalid_json");
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_classifies_incomplete_json_body_as_response_read_failed(
    ) {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([60u8; 64]);
        let Some((send_rpc_url, send_rpc_handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            b"{\"jsonrpc\":\"2.0\",\"result\":\"abc\"",
            128,
        ) else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("incomplete JSON body must classify as response read failure");
        assert!(reject.retryable);
        assert_eq!(reject.code, "send_rpc_unavailable");
        assert!(
            reject.detail.contains("response read failed"),
            "detail={}",
            reject.detail
        );
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_partial_valid_json_body_as_response_read_failed(
    ) {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([61u8; 64]);
        let partial_valid_json = format!(
            r#"{{"jsonrpc":"2.0","result":"{}"}}"#,
            expected_signature
        );
        let Some((send_rpc_url, send_rpc_handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            partial_valid_json.as_bytes(),
            partial_valid_json.len() + 64,
        ) else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("transport-incomplete body must reject even if partial bytes are valid JSON");
        assert!(reject.retryable);
        assert_eq!(reject.code, "send_rpc_unavailable");
        assert!(
            reject.detail.contains("response read failed"),
            "detail={}",
            reject.detail
        );
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_uses_fallback_after_primary_response_read_failure() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([62u8; 64]);
        let fallback_token = "Send-Rpc-Fallback-Token-Read-Failure";
        let partial_valid_json = format!(
            r#"{{"jsonrpc":"2.0","result":"{}"}}"#,
            expected_signature
        );
        let Some((primary_url, primary_handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            partial_valid_json.as_bytes(),
            partial_valid_json.len() + 64,
        ) else {
            return;
        };
        let Some((fallback_url, fallback_handle)) =
            spawn_one_shot_send_rpc_expect_bearer(fallback_token, expected_signature.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
            backend.send_rpc_fallback_url = Some(fallback_url);
            backend.send_rpc_fallback_auth_token = Some(fallback_token.to_string().into());
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let signature = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect("fallback should succeed after primary response-read failure");
        assert_eq!(signature, expected_signature);
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_returns_signature_result() {
        let (signed_tx_base64, rpc_signature) = test_signed_tx_base64_with_signature([13u8; 64]);
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url.clone());
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let signature =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
                .await
                .expect("send RPC should return tx signature");
        assert_eq!(signature, rpc_signature);
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_signature_mismatch() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([31u8; 64]);
        let rpc_signature = bs58::encode([32u8; 64]).into_string();
        assert_ne!(expected_signature, rpc_signature);
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
            .await
            .expect_err("mismatched send RPC signature must fail");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_signature_mismatch");
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_uses_fallback_auth_token_when_retrying() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([33u8; 64]);
        let fallback_token = "Send-Rpc-Fallback-Token";
        let Some((primary_url, primary_handle)) =
            spawn_one_shot_upstream_raw(503, "text/plain", "send rpc primary unavailable")
        else {
            return;
        };
        let Some((fallback_url, fallback_handle)) =
            spawn_one_shot_send_rpc_expect_bearer(fallback_token, expected_signature.as_str())
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
            backend.send_rpc_fallback_url = Some(fallback_url);
            backend.send_rpc_primary_auth_token =
                Some("send-rpc-primary-token".to_string().into());
            backend.send_rpc_fallback_auth_token = Some(fallback_token.to_string().into());
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let signature =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
            .await
            .expect("fallback send RPC with dedicated auth token should succeed");
        assert_eq!(signature, expected_signature);
        let _ = primary_handle.join();
        let _ = fallback_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_rejects_fallback_without_primary_url() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([36u8; 64]);

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = None;
            backend.send_rpc_fallback_url =
                Some("http://127.0.0.1:1/send-rpc-fallback".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
            .await
            .expect_err("fallback-only send RPC topology must fail closed");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "adapter_send_rpc_not_configured");
        assert!(
            reject
                .detail
                .contains("fallback URL but missing primary send RPC URL"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_accepts_already_processed_error() {
        let (signed_tx_base64, expected_signature) =
            test_signed_tx_base64_with_signature([34u8; 64]);
        let rpc_body = r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"Transaction already processed"}}"#;
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body)
        else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let signature =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
            .await
            .expect("already processed error should resolve to expected signature");
        assert_eq!(signature, expected_signature);
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_treats_blockhash_expired_payload_as_terminal() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([35u8; 64]);
        let Some((primary_url, primary_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"Blockhash not found"}}"#,
        ) else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
            backend.send_rpc_fallback_url =
                Some("http://127.0.0.1:1/send-rpc-fallback".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
            .await
            .expect_err("blockhash-expired send RPC payload should be terminal");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "executor_blockhash_expired");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_treats_generic_recent_blockhash_text_as_terminal() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([40u8; 64]);
        let Some((primary_url, primary_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"recent blockhash cache warming up"}}"#,
        ) else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = send_signed_transaction_via_rpc(
            &state,
            "rpc",
            signed_tx_base64.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("generic recent-blockhash text should remain terminal send-rpc error");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_error_payload_terminal");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn send_signed_transaction_via_rpc_treats_unknown_error_payload_as_terminal() {
        let (signed_tx_base64, _expected_signature) =
            test_signed_tx_base64_with_signature([41u8; 64]);
        let Some((primary_url, primary_handle)) = spawn_one_shot_upstream_raw(
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"mystery failure class"}}"#,
        ) else {
            return;
        };

        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(primary_url);
        } else {
            panic!("rpc backend must exist");
        }

        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject =
            send_signed_transaction_via_rpc(
                &state,
                "rpc",
                signed_tx_base64.as_str(),
                Some(&submit_deadline),
            )
            .await
            .expect_err("unknown send RPC error payload should be terminal");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "send_rpc_error_payload_terminal");
        let _ = primary_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_uses_send_rpc_when_upstream_returns_signed_tx_base64() {
        let (signed_tx_base64, rpc_signature) = test_signed_tx_base64_with_signature([14u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-1",
            "client_order_id": "client-order-1",
            "request_id": "request-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should succeed via send RPC");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(rpc_signature.as_str())
        );
        assert_eq!(
            response.get("submit_transport").and_then(Value::as_str),
            Some("adapter_send_rpc")
        );
        let _ = upstream_handle.join();
        let _ = send_rpc_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_uses_send_rpc_and_records_seen_signature_verification_when_enabled() {
        let (signed_tx_base64, rpc_signature) = test_signed_tx_base64_with_signature([63u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };
        let verify_body = r#"{
            "jsonrpc":"2.0",
            "result":{
                "value":[
                    {
                        "slot":123,
                        "confirmations":null,
                        "err":null,
                        "confirmationStatus":"confirmed"
                    }
                ]
            }
        }"#;
        let Some((verify_url, verify_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", verify_body)
        else {
            return;
        };

        let mut state = test_state_with_backends_and_verify(
            upstream_url.as_str(),
            None,
            upstream_url.as_str(),
            None,
            vec![verify_url.as_str()],
            true,
        );
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-send-rpc-verify-seen-1",
            "client_order_id": "client-order-send-rpc-verify-seen-1",
            "request_id": "request-send-rpc-verify-seen-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should succeed via send RPC with verification seen");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(rpc_signature.as_str())
        );
        assert_eq!(
            response.get("submit_transport").and_then(Value::as_str),
            Some("adapter_send_rpc")
        );
        assert_eq!(
            response
                .get("submit_signature_verify")
                .and_then(|value| value.get("enabled"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            response
                .get("submit_signature_verify")
                .and_then(|value| value.get("seen"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            response
                .get("submit_signature_verify")
                .and_then(|value| value.get("confirmation_status"))
                .and_then(Value::as_str),
            Some("confirmed")
        );
        let _ = upstream_handle.join();
        let _ = send_rpc_handle.join();
        let _ = verify_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_after_send_rpc_when_signature_verify_strict_unseen() {
        let (signed_tx_base64, rpc_signature) = test_signed_tx_base64_with_signature([64u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };
        let rpc_body = format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, rpc_signature);
        let Some((send_rpc_url, send_rpc_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", rpc_body.as_str())
        else {
            return;
        };
        let verify_body = r#"{"jsonrpc":"2.0","result":{"value":[null]}}"#;
        let Some((verify_url, verify_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", verify_body)
        else {
            return;
        };

        let mut state = test_state_with_backends_and_verify(
            upstream_url.as_str(),
            None,
            upstream_url.as_str(),
            None,
            vec![verify_url.as_str()],
            true,
        );
        if let Some(config) = state.config.submit_signature_verify.as_mut() {
            config.attempts = 1;
            config.interval_ms = 1;
        }
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some(send_rpc_url);
        } else {
            panic!("rpc backend must exist");
        }

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-send-rpc-verify-unseen-1",
            "client_order_id": "client-order-send-rpc-verify-unseen-1",
            "request_id": "request-send-rpc-verify-unseen-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("strict verify unseen after send-rpc should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        assert!(
            reject.detail.contains("reason=signature status pending"),
            "detail={}",
            reject.detail
        );
        let _ = upstream_handle.join();
        let _ = send_rpc_handle.join();
        let _ = verify_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_request_id_mismatch_before_send_rpc() {
        let (signed_tx_base64, _) = test_signed_tx_base64_with_signature([35u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":"{}","request_id":"request-mismatch-1"}}"#,
            signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        if let Some(backend) = state.config.route_backends.get_mut("rpc") {
            backend.send_rpc_url = Some("http://127.0.0.1:1/send-rpc".to_string());
        } else {
            panic!("rpc backend must exist");
        }

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-request-id-mismatch-1",
            "client_order_id": "client-order-request-id-mismatch-1",
            "request_id": "request-expected-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("request_id mismatch must reject before send RPC");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_request_id_mismatch");
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_route_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-route-payload-mismatch-1".to_string(),
            signal_id: "signal-route-payload-mismatch-1".to_string(),
            client_order_id: "client-order-route-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-route-payload-mismatch-1",
            "client_order_id": "client-order-route-payload-mismatch-1",
            "request_id": "request-route-payload-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "jito",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("route payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_slippage_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-slippage-payload-mismatch-1".to_string(),
            signal_id: "signal-slippage-payload-mismatch-1".to_string(),
            client_order_id: "client-order-slippage-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 12.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-slippage-payload-mismatch-1",
            "client_order_id": "client-order-slippage-payload-mismatch-1",
            "request_id": "request-slippage-payload-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("slippage payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("slippage_bps mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_route_slippage_cap_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-route-slippage-cap-payload-mismatch-1".to_string(),
            signal_id: "signal-route-slippage-cap-payload-mismatch-1".to_string(),
            client_order_id: "client-order-route-slippage-cap-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-route-slippage-cap-payload-mismatch-1",
            "client_order_id": "client-order-route-slippage-cap-payload-mismatch-1",
            "request_id": "request-route-slippage-cap-payload-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 25.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("route_slippage_cap payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route_slippage_cap_bps mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_fastlane_when_feature_disabled_before_forward() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-fastlane-disabled-1",
            "signal_id": "signal-fastlane-disabled-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-fastlane-disabled-1"),
                signal_id: Some("signal-fastlane-disabled-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("fastlane must reject before forward when feature disabled");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "fastlane_not_enabled");
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_not_in_allowlist_before_forward() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_backends.insert(
            "jito".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-jito-not-allowlisted-1",
            "signal_id": "signal-jito-not-allowlisted-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-jito-not-allowlisted-1"),
                signal_id: Some("signal-jito-not-allowlisted-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("non-allowlisted route must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "route_not_allowed");
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_route_not_in_allowlist_before_forward() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_backends.insert(
            "jito".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-jito-submit-not-allowlisted-1",
            "signal_id": "signal-jito-submit-not-allowlisted-1",
            "client_order_id": "client-order-jito-submit-not-allowlisted-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-jito-submit-not-allowlisted-1"),
                signal_id: Some("signal-jito-submit-not-allowlisted-1"),
                client_order_id: Some("client-order-jito-submit-not-allowlisted-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("non-allowlisted submit route must reject before action/context checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "route_not_allowed");
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_route_payload_hint_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-route-hint-mismatch-1",
            "signal_id": "signal-submit-route-hint-mismatch-1",
            "client_order_id": "client-order-submit-route-hint-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-submit-route-hint-mismatch-1"),
                signal_id: Some("signal-submit-route-hint-mismatch-1"),
                client_order_id: Some("client-order-submit-route-hint-mismatch-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("submit route hint mismatch must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_route_payload_hint_missing_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-route-hint-missing-1",
            "signal_id": "signal-submit-route-hint-missing-1",
            "client_order_id": "client-order-submit-route-hint-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-submit-route-hint-missing-1"),
                signal_id: Some("signal-submit-route-hint-missing-1"),
                client_order_id: Some("client-order-submit-route-hint-missing-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("submit missing route hint must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_route_payload_hint_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-route-hint-mismatch-1",
            "signal_id": "signal-simulate-route-hint-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-simulate-route-hint-mismatch-1"),
                signal_id: Some("signal-simulate-route-hint-mismatch-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("simulate route hint mismatch must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_route_payload_hint_missing_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-route-hint-missing-1",
            "signal_id": "signal-simulate-route-hint-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-simulate-route-hint-missing-1"),
                signal_id: Some("signal-simulate-route-hint-missing-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("simulate missing route hint must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_mismatch_before_allowlist_check() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-hint-priority-allowlist-1",
            "signal_id": "signal-route-hint-priority-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-route-hint-priority-allowlist-1"),
                signal_id: Some("signal-route-hint-priority-allowlist-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("route hint mismatch must reject before allowlist checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_missing_before_backend_check() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state
            .config
            .route_backends
            .remove("rpc")
            .expect("rpc backend should exist in test setup");
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-hint-priority-backend-1",
            "signal_id": "signal-route-hint-priority-backend-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-route-hint-priority-backend-1"),
                signal_id: Some("signal-route-hint-priority-backend-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("missing route hint must reject before backend checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_route_hint_mismatch_before_allowlist_check() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_backends.insert(
            "jito".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-route-hint-priority-allowlist-1",
            "signal_id": "signal-submit-route-hint-priority-allowlist-1",
            "client_order_id": "client-order-submit-route-hint-priority-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-route-hint-priority-allowlist-1"),
                signal_id: Some("signal-submit-route-hint-priority-allowlist-1"),
                client_order_id: Some("client-order-submit-route-hint-priority-allowlist-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("submit route hint mismatch must reject before allowlist checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_route_hint_missing_before_backend_check() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state
            .config
            .route_backends
            .remove("rpc")
            .expect("rpc backend should exist in test setup");
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-route-hint-priority-backend-1",
            "signal_id": "signal-submit-route-hint-priority-backend-1",
            "client_order_id": "client-order-submit-route-hint-priority-backend-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-submit-route-hint-priority-backend-1"),
                signal_id: Some("signal-submit-route-hint-priority-backend-1"),
                client_order_id: Some("client-order-submit-route-hint-priority-backend-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("submit route hint missing must reject before backend checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_payload_shape_on_submit() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-route-hint-before-shape-submit-1",
            "signal_id": "signal-route-hint-before-shape-submit-1",
            "client_order_id": "client-order-route-hint-before-shape-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-route-hint-before-shape-submit-1"),
                signal_id: Some("signal-route-hint-before-shape-submit-1"),
                client_order_id: Some("client-order-route-hint-before-shape-submit-1"),
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("route hint must reject before payload shape checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_deadline_context_on_submit() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-route-hint-before-deadline-submit-1",
            "signal_id": "signal-route-hint-before-deadline-submit-1",
            "client_order_id": "client-order-route-hint-before-deadline-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-route-hint-before-deadline-submit-1"),
                signal_id: Some("signal-route-hint-before-deadline-submit-1"),
                client_order_id: Some("client-order-route-hint-before-deadline-submit-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("route hint must reject before deadline context checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_action_context_on_submit() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-route-hint-before-action-context-submit-1",
            "signal_id": "signal-route-hint-before-action-context-submit-1",
            "client_order_id": "client-order-route-hint-before-action-context-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: None,
                request_id: Some("request-route-hint-before-action-context-submit-1"),
                signal_id: Some("signal-route-hint-before-action-context-submit-1"),
                client_order_id: Some("client-order-route-hint-before-action-context-submit-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("route hint must reject before action context checks on submit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route payload hint missing at route-executor boundary"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_payload_shape_on_simulate() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-hint-before-shape-simulate-1",
            "signal_id": "signal-route-hint-before-shape-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-route-hint-before-shape-simulate-1"),
                signal_id: Some("signal-route-hint-before-shape-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("route hint must reject before payload shape checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_deadline_context_on_simulate() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-hint-before-deadline-simulate-1",
            "signal_id": "signal-route-hint-before-deadline-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-route-hint-before-deadline-simulate-1"),
                signal_id: Some("signal-route-hint-before-deadline-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("route hint must reject before deadline context checks on simulate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_fastlane_feature_gate_on_submit() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-route-hint-before-fastlane-submit-1",
            "signal_id": "signal-route-hint-before-fastlane-submit-1",
            "client_order_id": "client-order-route-hint-before-fastlane-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-route-hint-before-fastlane-submit-1"),
                signal_id: Some("signal-route-hint-before-fastlane-submit-1"),
                client_order_id: Some("client-order-route-hint-before-fastlane-submit-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("route hint must reject before fastlane feature gate on submit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_fastlane_feature_gate_on_simulate() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-hint-before-fastlane-simulate-1",
            "signal_id": "signal-route-hint-before-fastlane-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-route-hint-before-fastlane-simulate-1"),
                signal_id: Some("signal-route-hint-before-fastlane-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("route hint must reject before fastlane feature gate on simulate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_route_hint_before_action_context_on_simulate() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-hint-before-action-context-simulate-1",
            "signal_id": "signal-route-hint-before-action-context-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-route-hint-before-action-context-simulate-1"),
                signal_id: Some("signal-route-hint-before-action-context-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("route hint must reject before action context checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("route payload mismatch"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_payload_shape_before_allowlist_check() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-shape-before-allowlist-1",
            "signal_id": "signal-shape-before-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-shape-before-allowlist-1"),
                signal_id: Some("signal-shape-before-allowlist-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("payload shape must reject before allowlist checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_payload_shape_before_allowlist_check() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-shape-before-allowlist-1",
            "signal_id": "signal-submit-shape-before-allowlist-1",
            "client_order_id": "client-order-submit-shape-before-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-submit-shape-before-allowlist-1"),
                signal_id: Some("signal-submit-shape-before-allowlist-1"),
                client_order_id: Some("client-order-submit-shape-before-allowlist-1"),
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("payload shape must reject before allowlist checks on submit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_payload_shape_before_fastlane_feature_gate() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-shape-before-fastlane-gate-1",
            "signal_id": "signal-shape-before-fastlane-gate-1",
            "client_order_id": "client-order-shape-before-fastlane-gate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-shape-before-fastlane-gate-1"),
                signal_id: Some("signal-shape-before-fastlane-gate-1"),
                client_order_id: Some("client-order-shape-before-fastlane-gate-1"),
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("payload shape must reject before fastlane feature gate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_payload_shape_before_fastlane_feature_gate() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-shape-before-fastlane-gate-1",
            "signal_id": "signal-simulate-shape-before-fastlane-gate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-simulate-shape-before-fastlane-gate-1"),
                signal_id: Some("signal-simulate-shape-before-fastlane-gate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("payload shape must reject before fastlane feature gate on simulate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_payload_shape_before_deadline_context_on_submit() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-shape-before-deadline-submit-1",
            "signal_id": "signal-shape-before-deadline-submit-1",
            "client_order_id": "client-order-shape-before-deadline-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-shape-before-deadline-submit-1"),
                signal_id: Some("signal-shape-before-deadline-submit-1"),
                client_order_id: Some("client-order-shape-before-deadline-submit-1"),
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("payload shape must reject before deadline context on submit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_payload_shape_before_action_context_on_submit() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-shape-before-action-context-submit-1",
            "signal_id": "signal-shape-before-action-context-submit-1",
            "client_order_id": "client-order-shape-before-action-context-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-shape-before-action-context-submit-1"),
                signal_id: Some("signal-shape-before-action-context-submit-1"),
                client_order_id: Some("client-order-shape-before-action-context-submit-1"),
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("payload shape must reject before action context on submit");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_payload_shape_before_deadline_context_on_simulate() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-shape-before-deadline-simulate-1",
            "signal_id": "signal-shape-before-deadline-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-shape-before-deadline-simulate-1"),
                signal_id: Some("signal-shape-before-deadline-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("payload shape must reject before deadline context on simulate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_payload_shape_before_action_context_on_simulate() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-shape-before-action-context-simulate-1",
            "signal_id": "signal-shape-before-action-context-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-shape-before-action-context-simulate-1"),
                signal_id: Some("signal-shape-before-action-context-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("payload shape must reject before action context on simulate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_deadline_context_before_allowlist_check() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-deadline-before-allowlist-1",
            "signal_id": "signal-deadline-before-allowlist-1",
            "client_order_id": "client-order-deadline-before-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-deadline-before-allowlist-1"),
                signal_id: Some("signal-deadline-before-allowlist-1"),
                client_order_id: Some("client-order-deadline-before-allowlist-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("deadline context must reject before allowlist checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_deadline_context_before_fastlane_feature_gate() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-deadline-before-fastlane-gate-1",
            "signal_id": "signal-deadline-before-fastlane-gate-1",
            "client_order_id": "client-order-deadline-before-fastlane-gate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-deadline-before-fastlane-gate-1"),
                signal_id: Some("signal-deadline-before-fastlane-gate-1"),
                client_order_id: Some("client-order-deadline-before-fastlane-gate-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("deadline context must reject before fastlane feature gate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_deadline_context_before_action_context_on_submit() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-deadline-before-action-context-submit-1",
            "signal_id": "signal-deadline-before-action-context-submit-1",
            "client_order_id": "client-order-deadline-before-action-context-submit-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-deadline-before-action-context-submit-1"),
                signal_id: Some("signal-deadline-before-action-context-submit-1"),
                client_order_id: Some("client-order-deadline-before-action-context-submit-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: None,
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("deadline context must reject before submit action-context checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_deadline_context_before_action_context_on_simulate() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-deadline-before-action-context-simulate-1",
            "signal_id": "signal-deadline-before-action-context-simulate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-deadline-before-action-context-simulate-1"),
                signal_id: Some("signal-deadline-before-action-context-simulate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("deadline context must reject before simulate action-context checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("must not include submit deadline"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_action_context_before_allowlist_check() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-action-context-before-allowlist-1",
            "signal_id": "signal-action-context-before-allowlist-1",
            "client_order_id": "client-order-action-context-before-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-action-context-before-allowlist-1"),
                signal_id: Some("signal-action-context-before-allowlist-1"),
                client_order_id: Some("client-order-action-context-before-allowlist-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("action context must reject before allowlist checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_action_context_before_fastlane_feature_gate() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-action-context-before-fastlane-gate-1",
            "signal_id": "signal-action-context-before-fastlane-gate-1",
            "client_order_id": "client-order-action-context-before-fastlane-gate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-action-context-before-fastlane-gate-1"),
                signal_id: Some("signal-action-context-before-fastlane-gate-1"),
                client_order_id: Some("client-order-action-context-before-fastlane-gate-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("action context must reject before fastlane feature gate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_action_context_before_backend_check() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state
            .config
            .route_backends
            .remove("rpc")
            .expect("rpc backend should exist in test setup");
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-action-context-before-backend-1",
            "signal_id": "signal-action-context-before-backend-1",
            "client_order_id": "client-order-action-context-before-backend-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-action-context-before-backend-1"),
                signal_id: Some("signal-action-context-before-backend-1"),
                client_order_id: Some("client-order-action-context-before-backend-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("action context must reject before backend checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_action_context_before_allowlist_check() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_backends.insert(
            "jito".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-sim-action-context-before-allowlist-1",
            "signal_id": "signal-sim-action-context-before-allowlist-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "jito"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "jito",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("jito"),
                request_id: Some("request-sim-action-context-before-allowlist-1"),
                signal_id: Some("signal-sim-action-context-before-allowlist-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("simulate action context must reject before allowlist checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include submit instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_action_context_before_backend_check() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state
            .config
            .route_backends
            .remove("rpc")
            .expect("rpc backend should exist in test setup");
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-sim-action-context-before-backend-1",
            "signal_id": "signal-sim-action-context-before-backend-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-sim-action-context-before-backend-1"),
                signal_id: Some("signal-sim-action-context-before-backend-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("simulate action context must reject before backend checks");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include submit instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_action_context_before_fastlane_feature_gate() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-sim-action-context-before-fastlane-gate-1",
            "signal_id": "signal-sim-action-context-before-fastlane-gate-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-sim-action-context-before-fastlane-gate-1"),
                signal_id: Some("signal-sim-action-context-before-fastlane-gate-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("simulate action context must reject before fastlane feature gate");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include submit instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_with_plan_without_deadline_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-without-deadline-1",
            "signal_id": "signal-submit-without-deadline-1",
            "client_order_id": "client-order-submit-without-deadline-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-without-deadline-1"),
                signal_id: Some("signal-submit-without-deadline-1"),
                client_order_id: Some("client-order-submit-without-deadline-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("submit with plan without deadline must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_with_deadline_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-with-deadline-1",
            "signal_id": "signal-simulate-with-deadline-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-simulate-with-deadline-1"),
                signal_id: Some("signal-simulate-with-deadline-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("simulate with submit deadline must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("must not include submit deadline"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_missing_client_order_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-missing-client-order-expectation-1",
            "signal_id": "signal-submit-missing-client-order-expectation-1",
            "client_order_id": "client-order-submit-missing-client-order-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-missing-client-order-expectation-1"),
                signal_id: Some("signal-submit-missing-client-order-expectation-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("submit missing client_order_id expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing client_order_id expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_with_client_order_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-client-order-expectation-1",
            "signal_id": "signal-simulate-client-order-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-simulate-client-order-expectation-1"),
                signal_id: Some("signal-simulate-client-order-expectation-1"),
                client_order_id: Some("client-order-not-allowed"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("simulate with client_order_id expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include client_order_id expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_with_empty_token_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-empty-token-expectation-1",
            "signal_id": "signal-submit-empty-token-expectation-1",
            "client_order_id": "client-order-submit-empty-token-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-empty-token-expectation-1"),
                signal_id: Some("signal-submit-empty-token-expectation-1"),
                client_order_id: Some("client-order-submit-empty-token-expectation-1"),
                side: Some("buy"),
                token: Some(" "),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("submit with empty token expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty token expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_with_empty_request_id_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-empty-request-expectation-1",
            "signal_id": "signal-simulate-empty-request-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some(""),
                signal_id: Some("signal-simulate-empty-request-expectation-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("simulate with empty request_id expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("empty request_id expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_allowlisted_route_without_backend_before_forward() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state
            .config
            .route_backends
            .remove("rpc")
            .expect("rpc backend should exist in test setup");
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-route-backend-missing-1",
            "signal_id": "signal-route-backend-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-route-backend-missing-1"),
                signal_id: Some("signal-route-backend-missing-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("allowlisted route without backend must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "route_backend_not_configured");
        assert!(reject.detail.contains("not configured"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_allowlisted_route_without_backend_before_forward() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state
            .config
            .route_backends
            .remove("rpc")
            .expect("rpc backend should exist in test setup");
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-submit-route-backend-missing-1",
            "signal_id": "signal-submit-route-backend-missing-1",
            "client_order_id": "client-order-submit-route-backend-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-route-backend-missing-1"),
                signal_id: Some("signal-submit-route-backend-missing-1"),
                client_order_id: Some("client-order-submit-route-backend-missing-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("submit missing backend must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "route_backend_not_configured");
        assert!(reject.detail.contains("not configured"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_fastlane_submit_when_feature_disabled_before_forward() {
        let mut state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        state.config.route_allowlist.insert("fastlane".to_string());
        state.config.route_backends.insert(
            "fastlane".to_string(),
            RouteBackend {
                submit_url: "http://127.0.0.1:1/upstream".to_string(),
                submit_fallback_url: None,
                simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                simulate_fallback_url: None,
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "request_id": "request-fastlane-submit-disabled-1",
            "signal_id": "signal-fastlane-submit-disabled-1",
            "client_order_id": "client-order-fastlane-submit-disabled-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "fastlane",
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "fastlane",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("fastlane"),
                request_id: Some("request-fastlane-submit-disabled-1"),
                signal_id: Some("signal-fastlane-submit-disabled-1"),
                client_order_id: Some("client-order-fastlane-submit-disabled-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("fastlane submit must reject before forward when feature disabled");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "fastlane_not_enabled");
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_with_submit_instruction_plan_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-with-plan-1",
            "signal_id": "signal-simulate-with-plan-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-simulate-with-plan-1"),
                signal_id: Some("signal-simulate-with-plan-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("simulate with submit instruction plan must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include submit instruction plan"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_missing_slippage_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "signal_id": "signal-submit-missing-slippage-expectation-1",
            "client_order_id": "client-order-submit-missing-slippage-expectation-1",
            "request_id": "request-submit-missing-slippage-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-missing-slippage-expectation-1"),
                signal_id: Some("signal-submit-missing-slippage-expectation-1"),
                client_order_id: Some("client-order-submit-missing-slippage-expectation-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: None,
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("submit missing slippage expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing slippage_bps expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_missing_route_slippage_cap_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "signal_id": "signal-submit-missing-route-slippage-cap-expectation-1",
            "client_order_id": "client-order-submit-missing-route-slippage-cap-expectation-1",
            "request_id": "request-submit-missing-route-slippage-cap-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-missing-route-slippage-cap-expectation-1"),
                signal_id: Some("signal-submit-missing-route-slippage-cap-expectation-1"),
                client_order_id: Some("client-order-submit-missing-route-slippage-cap-expectation-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: None,
            },
        )
        .await
        .expect_err(
            "submit missing route_slippage_cap expectation must reject before forward",
        );
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("missing route_slippage_cap_bps expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_non_finite_slippage_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "signal_id": "signal-submit-non-finite-slippage-expectation-1",
            "client_order_id": "client-order-submit-non-finite-slippage-expectation-1",
            "request_id": "request-submit-non-finite-slippage-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-non-finite-slippage-expectation-1"),
                signal_id: Some("signal-submit-non-finite-slippage-expectation-1"),
                client_order_id: Some("client-order-submit-non-finite-slippage-expectation-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(f64::NAN),
                expected_route_slippage_cap_bps: Some(20.0),
            },
        )
        .await
        .expect_err("submit non-finite slippage expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("slippage_bps expectation must be finite"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_non_finite_route_slippage_cap_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "submit",
            "signal_id": "signal-submit-non-finite-route-slippage-cap-expectation-1",
            "client_order_id": "client-order-submit-non-finite-route-slippage-cap-expectation-1",
            "request_id": "request-submit-non-finite-route-slippage-cap-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            Some(&submit_deadline),
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-non-finite-route-slippage-cap-expectation-1"),
                signal_id: Some("signal-submit-non-finite-route-slippage-cap-expectation-1"),
                client_order_id: Some("client-order-submit-non-finite-route-slippage-cap-expectation-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                instruction_plan: Some(crate::tx_build::SubmitInstructionPlan {
                    compute_budget_cu_limit: 300_000,
                    compute_budget_cu_price_micro_lamports: 1_000,
                    tip_instruction_lamports: None,
                }),
                expected_slippage_bps: Some(10.0),
                expected_route_slippage_cap_bps: Some(f64::INFINITY),
            },
        )
        .await
        .expect_err(
            "submit non-finite route slippage cap expectation must reject before forward",
        );
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("route_slippage_cap_bps expectation must be finite"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_with_slippage_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-with-slippage-expectation-1",
            "signal_id": "signal-simulate-with-slippage-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-simulate-with-slippage-expectation-1"),
                signal_id: Some("signal-simulate-with-slippage-expectation-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                expected_slippage_bps: Some(10.0),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("simulate with slippage expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include slippage_bps expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_simulate_with_route_slippage_cap_expectation_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "action": "simulate",
            "request_id": "request-simulate-with-route-slippage-cap-expectation-1",
            "signal_id": "signal-simulate-with-route-slippage-cap-expectation-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "route": "rpc"
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize simulate request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Simulate,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-simulate-with-route-slippage-cap-expectation-1"),
                signal_id: Some("signal-simulate-with-route-slippage-cap-expectation-1"),
                client_order_id: None,
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext {
                expected_route_slippage_cap_bps: Some(20.0),
                ..RouteSubmitExecutionContext::default()
            },
        )
        .await
        .expect_err("simulate with route slippage cap expectation must reject before forward");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("must not include route_slippage_cap_bps expectation"));
    }

    #[tokio::test]
    async fn execute_route_action_rejects_submit_without_instruction_plan_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-no-plan-1",
            "client_order_id": "client-order-submit-no-plan-1",
            "request_id": "request-submit-no-plan-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");

        let reject = execute_route_action(
            &state,
            "rpc",
            UpstreamAction::Submit,
            raw_body_bytes.as_slice(),
            None,
            RouteActionPayloadExpectations {
                route_hint: Some("rpc"),
                request_id: Some("request-submit-no-plan-1"),
                signal_id: Some("signal-submit-no-plan-1"),
                client_order_id: Some("client-order-submit-no-plan-1"),
                side: Some("buy"),
                token: Some("11111111111111111111111111111111"),
            },
            RouteSubmitExecutionContext::default(),
        )
        .await
        .expect_err("submit without instruction plan must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing instruction plan"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_instruction_plan_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-plan-mismatch-1",
            "client_order_id": "client-order-submit-plan-mismatch-1",
            "request_id": "request-submit-plan-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let mut request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");
        request.compute_budget.cu_limit = 350_000;

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("instruction-plan mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("compute_budget.cu_limit mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_instruction_plan_cu_price_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-plan-cu-price-mismatch-1",
            "client_order_id": "client-order-submit-plan-cu-price-mismatch-1",
            "request_id": "request-submit-plan-cu-price-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let mut request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");
        request.compute_budget.cu_price_micro_lamports = 1_500;

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("instruction-plan cu_price mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject
            .detail
            .contains("compute_budget.cu_price_micro_lamports mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_action_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-action-payload-mismatch-1".to_string(),
            signal_id: "signal-submit-action-payload-mismatch-1".to_string(),
            client_order_id: "client-order-submit-action-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-action-payload-mismatch-1",
            "client_order_id": "client-order-submit-action-payload-mismatch-1",
            "request_id": "request-submit-action-payload-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "action": "simulate",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit action payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_non_string_action_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-action-type-mismatch-1".to_string(),
            signal_id: "signal-submit-action-type-mismatch-1".to_string(),
            client_order_id: "client-order-submit-action-type-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-action-type-mismatch-1",
            "client_order_id": "client-order-submit-action-type-mismatch-1",
            "request_id": "request-submit-action-type-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "action": 123,
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit non-string action payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("action must be string"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_contract_version_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-contract-payload-mismatch-1".to_string(),
            signal_id: "signal-submit-contract-payload-mismatch-1".to_string(),
            client_order_id: "client-order-submit-contract-payload-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v2",
            "signal_id": "signal-submit-contract-payload-mismatch-1",
            "client_order_id": "client-order-submit-contract-payload-mismatch-1",
            "request_id": "request-submit-contract-payload-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit contract_version payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_empty_contract_version_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-contract-empty-1".to_string(),
            signal_id: "signal-submit-contract-empty-1".to_string(),
            client_order_id: "client-order-submit-contract-empty-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": " ",
            "signal_id": "signal-submit-contract-empty-1",
            "client_order_id": "client-order-submit-contract-empty-1",
            "request_id": "request-submit-contract-empty-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit empty contract_version payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("contract_version must be non-empty"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_empty_client_order_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-client-order-empty-1".to_string(),
            signal_id: "signal-submit-client-order-empty-1".to_string(),
            client_order_id: "client-order-submit-client-order-empty-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-client-order-empty-1",
            "client_order_id": " ",
            "request_id": "request-submit-client-order-empty-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit empty client_order_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("client_order_id must be non-empty"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_empty_signal_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-signal-empty-1".to_string(),
            signal_id: "signal-submit-signal-empty-1".to_string(),
            client_order_id: "client-order-submit-signal-empty-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": " ",
            "client_order_id": "client-order-submit-signal-empty-1",
            "request_id": "request-submit-signal-empty-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit empty signal_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id must be non-empty"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_request_id_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-expected-1".to_string(),
            signal_id: "signal-submit-request-mismatch-1".to_string(),
            client_order_id: "client-order-submit-request-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-request-mismatch-1",
            "client_order_id": "client-order-submit-request-mismatch-1",
            "request_id": "request-submit-other-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit request_id payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("request_id mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_missing_request_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-id-missing-1".to_string(),
            signal_id: "signal-submit-id-missing-1".to_string(),
            client_order_id: "client-order-submit-id-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-id-missing-1",
            "client_order_id": "client-order-submit-id-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit missing request_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing request_id"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_signal_id_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-signal-mismatch-1".to_string(),
            signal_id: "signal-submit-expected-1".to_string(),
            client_order_id: "client-order-submit-signal-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-other-1",
            "client_order_id": "client-order-submit-signal-mismatch-1",
            "request_id": "request-submit-signal-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit signal_id payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("signal_id mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_missing_signal_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-signal-missing-1".to_string(),
            signal_id: "signal-submit-missing-1".to_string(),
            client_order_id: "client-order-submit-signal-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "client_order_id": "client-order-submit-signal-missing-1",
            "request_id": "request-submit-signal-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit missing signal_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing signal_id"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_client_order_id_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-client-order-mismatch-1".to_string(),
            signal_id: "signal-submit-client-order-mismatch-1".to_string(),
            client_order_id: "client-order-submit-expected-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-client-order-mismatch-1",
            "client_order_id": "client-order-submit-other-1",
            "request_id": "request-submit-client-order-mismatch-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit client_order_id payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("client_order_id mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_missing_client_order_id_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-client-order-missing-1".to_string(),
            signal_id: "signal-submit-client-order-missing-1".to_string(),
            client_order_id: "client-order-submit-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-client-order-missing-1",
            "request_id": "request-submit-client-order-missing-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit missing client_order_id payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing client_order_id"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_side_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-side-mismatch-1".to_string(),
            signal_id: "signal-submit-side-mismatch-1".to_string(),
            client_order_id: "client-order-submit-side-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-side-mismatch-1",
            "client_order_id": "client-order-submit-side-mismatch-1",
            "request_id": "request-submit-side-mismatch-1",
            "side": "sell",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit side payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("side mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_missing_side_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-side-missing-1".to_string(),
            signal_id: "signal-submit-side-missing-1".to_string(),
            client_order_id: "client-order-submit-side-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-side-missing-1",
            "client_order_id": "client-order-submit-side-missing-1",
            "request_id": "request-submit-side-missing-1",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit missing side payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing side"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_token_payload_mismatch_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-token-mismatch-1".to_string(),
            signal_id: "signal-submit-token-mismatch-1".to_string(),
            client_order_id: "client-order-submit-token-mismatch-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-token-mismatch-1",
            "client_order_id": "client-order-submit-token-mismatch-1",
            "request_id": "request-submit-token-mismatch-1",
            "side": "buy",
            "token": "22222222222222222222222222222222",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit token payload mismatch must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("token mismatch"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_missing_token_payload_before_forward() {
        let state = test_state_with_backends(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
        );
        let request = SubmitRequest {
            contract_version: Some("v1".to_string()),
            request_id: "request-submit-token-missing-1".to_string(),
            signal_id: "signal-submit-token-missing-1".to_string(),
            client_order_id: "client-order-submit-token-missing-1".to_string(),
            side: "buy".to_string(),
            token: "11111111111111111111111111111111".to_string(),
            notional_sol: 0.1,
            signal_ts: "2026-02-20T00:00:00Z".to_string(),
            route: "rpc".to_string(),
            slippage_bps: 10.0,
            route_slippage_cap_bps: 20.0,
            tip_lamports: 0,
            compute_budget: ComputeBudgetRequest {
                cu_limit: 300_000,
                cu_price_micro_lamports: 1_000,
            },
        };
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-token-missing-1",
            "client_order_id": "client-order-submit-token-missing-1",
            "request_id": "request-submit-token-missing-1",
            "side": "buy",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit missing token payload must reject before forwarding");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing token"));
    }

    #[tokio::test]
    async fn handle_submit_rejects_unknown_upstream_status_even_with_invalid_ok_type() {
        let upstream_body = r#"{"status":"pending","ok":"true","accepted":true}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-unknown-status-invalid-ok-1",
            "client_order_id": "client-order-submit-unknown-status-invalid-ok-1",
            "request_id": "request-submit-unknown-status-invalid-ok-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("unknown status must win over malformed ok flag");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_unknown_upstream_status_even_with_invalid_accepted_type() {
        let upstream_body = r#"{"status":"pending","ok":true,"accepted":"true"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-unknown-status-invalid-accepted-1",
            "client_order_id": "client-order-submit-unknown-status-invalid-accepted-1",
            "request_id": "request-submit-unknown-status-invalid-accepted-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("unknown status must win over malformed accepted flag");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_unknown_upstream_status_before_retryable_type_validation() {
        let upstream_body = r#"{"status":"pending","ok":false,"accepted":false,"retryable":"true"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-unknown-status-invalid-retryable-1",
            "client_order_id": "client-order-submit-unknown-status-invalid-retryable-1",
            "request_id": "request-submit-unknown-status-invalid-retryable-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("unknown status must win before retryable type validation");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_unknown_upstream_status_before_reject_code_type_validation() {
        let upstream_body =
            r#"{"status":"pending","ok":false,"accepted":false,"retryable":false,"code":123,"detail":"busy"}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-unknown-status-invalid-reject-code-1",
            "client_order_id": "client-order-submit-unknown-status-invalid-reject-code-1",
            "request_id": "request-submit-unknown-status-invalid-reject-code-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("unknown status must win before reject code type validation");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_unknown_upstream_status_before_reject_detail_type_validation() {
        let upstream_body =
            r#"{"status":"pending","ok":false,"accepted":false,"retryable":false,"code":"busy","detail":null}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-unknown-status-invalid-reject-detail-1",
            "client_order_id": "client-order-submit-unknown-status-invalid-reject-detail-1",
            "request_id": "request-submit-unknown-status-invalid-reject-detail-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("unknown status must win before reject detail type validation");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_invalid_status");
        assert!(
            reject.detail.contains("unknown upstream status=pending"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_missing_transport_artifacts() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-missing-transport-1",
            "client_order_id": "client-order-missing-transport-1",
            "request_id": "request-missing-transport-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("missing submit transport artifacts should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("missing tx_signature and signed_tx_base64"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_returns_conflicting_transport_artifacts() {
        let tx_signature = bs58::encode([19u8; 64]).into_string();
        let (signed_tx_base64, _) = test_signed_tx_base64_with_signature([20u8; 64]);
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","signed_tx_base64":"{}"}}"#,
            tx_signature, signed_tx_base64
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-conflicting-transport-1",
            "client_order_id": "client-order-conflicting-transport-1",
            "request_id": "request-conflicting-transport-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("conflicting submit transport artifacts should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("exactly one of tx_signature or signed_tx_base64"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_tx_signature_type_is_invalid() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"tx_signature":123}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-tx-signature-type-1",
            "client_order_id": "client-order-invalid-tx-signature-type-1",
            "request_id": "request-invalid-tx-signature-type-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-string upstream tx_signature should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("tx_signature must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_signed_tx_base64_type_is_invalid() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":123}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-signed-tx-type-1",
            "client_order_id": "client-order-invalid-signed-tx-type-1",
            "request_id": "request-invalid-signed-tx-type-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-string upstream signed_tx_base64 should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("signed_tx_base64 must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_tx_signature_is_null() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"tx_signature":null}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-null-tx-signature-1",
            "client_order_id": "client-order-null-tx-signature-1",
            "request_id": "request-null-tx-signature-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("null upstream tx_signature should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("tx_signature must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_signed_tx_base64_is_empty() {
        let upstream_body = r#"{"status":"ok","ok":true,"accepted":true,"signed_tx_base64":" "}"#;
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body)
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-empty-signed-tx-1",
            "client_order_id": "client-order-empty-signed-tx-1",
            "request_id": "request-empty-signed-tx-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("empty upstream signed_tx_base64 should reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("signed_tx_base64 must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_request_id_type_is_invalid() {
        let signature = bs58::encode([21u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","request_id":123}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-response-request-id-type-1",
            "client_order_id": "client-order-invalid-response-request-id-type-1",
            "request_id": "request-invalid-response-request-id-type-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-string upstream request_id should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("request_id must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_request_id_is_null() {
        let signature = bs58::encode([24u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","request_id":null}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-null-response-request-id-1",
            "client_order_id": "client-order-null-response-request-id-1",
            "request_id": "request-null-response-request-id-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("null upstream request_id should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("request_id must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_upstream_client_order_id_is_empty() {
        let signature = bs58::encode([23u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","client_order_id":" "}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-empty-response-client-order-id-1",
            "client_order_id": "client-order-empty-response-client-order-id-1",
            "request_id": "request-empty-response-client-order-id-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("empty upstream client_order_id should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("client_order_id must be non-empty string when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_invalid_submitted_at_in_upstream_response() {
        let signature = bs58::encode([18u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","submitted_at":"not-rfc3339"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-submitted-at-1",
            "client_order_id": "client-order-invalid-submitted-at-1",
            "request_id": "request-invalid-submitted-at-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("invalid submitted_at should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject.detail.contains("submitted_at is not valid RFC3339"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_invalid_fee_hint_field_type_from_upstream_response() {
        let signature = bs58::encode([24u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","network_fee_lamports":"5300"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-fee-field-1",
            "client_order_id": "client-order-invalid-fee-field-1",
            "request_id": "request-invalid-fee-field-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("invalid fee hint field type should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("network_fee_lamports must be non-negative integer when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_null_fee_hint_field_from_upstream_response() {
        let signature = bs58::encode([25u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}","network_fee_lamports":null}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-null-fee-field-1",
            "client_order_id": "client-order-null-fee-field-1",
            "request_id": "request-null-fee-field-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.1,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("null fee hint field should reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "submit_adapter_invalid_response");
        assert!(
            reject
                .detail
                .contains("network_fee_lamports must be non-negative integer when present"),
            "unexpected detail: {}",
            reject.detail
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_returns_cached_response_for_duplicate_client_order_id() {
        let signature = bs58::encode([17u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-idempotent-1",
            "client_order_id": "client-order-idempotent-1",
            "request_id": "request-idempotent-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.3,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let first = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("first submit should succeed");
        let first_submitted_at = first
            .get("submitted_at")
            .and_then(Value::as_str)
            .expect("submitted_at");

        let second = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("second submit should return cached response");
        assert_eq!(
            second.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(
            second.get("submitted_at").and_then(Value::as_str),
            Some(first_submitted_at)
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_parallel_duplicate_client_order_id_in_flight() {
        let signature = bs58::encode([19u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) = spawn_one_shot_upstream_raw_with_delay(
            200,
            "application/json",
            upstream_body.as_str(),
            200,
        ) else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-idempotent-inflight-1",
            "client_order_id": "client-order-idempotent-inflight-1",
            "request_id": "request-idempotent-inflight-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let first_submit =
            async { handle_submit(&state, &request, raw_body_bytes.as_slice()).await };
        let second_submit = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            handle_submit(&state, &request, raw_body_bytes.as_slice()).await
        };

        let (first_result, second_result) = tokio::join!(first_submit, second_submit);
        let first_response = first_result.expect("first submit should succeed");
        assert_eq!(
            first_response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        let second_reject =
            second_result.expect_err("second submit should reject while first in flight");
        assert!(second_reject.retryable);
        assert_eq!(second_reject.code, "submit_in_flight");
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_returns_canonical_cached_response_when_store_conflicts() {
        let upstream_signature = bs58::encode([21u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            upstream_signature
        );
        let Some((upstream_url, upstream_handle)) = spawn_one_shot_upstream_raw_with_delay(
            200,
            "application/json",
            upstream_body.as_str(),
            250,
        ) else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let state_inject = state.clone();
        let client_order_id = "client-order-idempotent-overlap-1".to_string();
        let canonical = json!({
            "status": "ok",
            "ok": true,
            "accepted": true,
            "route": "rpc",
            "contract_version": "v1",
            "client_order_id": client_order_id,
            "request_id": "request-canonical-1",
            "tx_signature": bs58::encode([22u8; 64]).into_string(),
            "submit_transport": "upstream_signature",
            "submitted_at": "2026-02-24T00:00:00Z",
            "network_fee_lamports": 5300,
            "base_fee_lamports": 5000,
            "priority_fee_lamports": 300,
            "ata_create_rent_lamports": 0
        });

        let inject_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(60));
            state_inject
                .idempotency
                .store_submit_response(
                    "client-order-idempotent-overlap-1",
                    "request-canonical-1",
                    &canonical,
                )
                .expect("inject canonical response");
        });

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-idempotent-overlap-1",
            "client_order_id": "client-order-idempotent-overlap-1",
            "request_id": "request-idempotent-overlap-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should return canonical cached response");
        assert_eq!(
            response.get("request_id").and_then(Value::as_str),
            Some("request-canonical-1")
        );
        let _ = inject_handle.join();
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_when_submit_deadline_budget_exhausted() {
        let signature = bs58::encode([23u8; 64]).into_string();
        let upstream_body = format!(
            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
            signature
        );
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", upstream_body.as_str())
        else {
            return;
        };
        let Some((verify_url, verify_handle)) = spawn_one_shot_upstream_raw_with_delay(
            200,
            "application/json",
            r#"{"jsonrpc":"2.0","result":{"value":[null]}}"#,
            100,
        ) else {
            return;
        };

        let mut state = test_state_with_backends_and_verify(
            upstream_url.as_str(),
            None,
            upstream_url.as_str(),
            None,
            vec![verify_url.as_str()],
            true,
        );
        state.config.submit_total_budget_ms = 20;
        if let Some(config) = state.config.submit_signature_verify.as_mut() {
            config.attempts = 2;
            config.interval_ms = 20;
        }

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-budget-exhausted-1",
            "client_order_id": "client-order-budget-exhausted-1",
            "request_id": "request-budget-exhausted-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 10.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1000
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("submit deadline exhaustion must be retryable reject");
        assert!(reject.retryable);
        assert_eq!(reject.code, "executor_submit_timeout_budget_exceeded");
        let _ = upstream_handle.join();
        let _ = verify_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_forces_rpc_tip_to_zero_and_emits_trace() {
        let signature = bs58::encode([15u8; 64]).into_string();
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_tip_lamports(0, signature.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-1",
            "client_order_id": "client-order-rpc-tip",
            "request_id": "request-rpc-tip",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 2500,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should coerce rpc tip to zero and succeed");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(
            response.get("tip_lamports").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("policy_code"))
                .and_then(Value::as_str),
            Some("rpc_tip_forced_zero")
        );
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("requested_tip_lamports"))
                .and_then(Value::as_u64),
            Some(2500)
        );
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("effective_tip_lamports"))
                .and_then(Value::as_u64),
            Some(0)
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_wires_instruction_plan_presence_into_route_adapter_context() {
        let signature = bs58::encode([24u8; 64]).into_string();
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_tip_lamports(0, signature.as_str())
        else {
            return;
        };

        let state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        let client_order_id = "client-order-submit-context-plan-1";
        clear_submit_instruction_plan_presence_for_test(client_order_id);
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-submit-context-plan-1",
            "client_order_id": client_order_id,
            "request_id": "request-submit-context-plan-1",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 2500,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("submit should succeed and wire instruction context");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(
            take_submit_instruction_plan_presence_for_test(client_order_id),
            Some(true)
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_allows_rpc_tip_when_nonzero_tip_disabled() {
        let signature = bs58::encode([16u8; 64]).into_string();
        let Some((upstream_url, upstream_handle)) =
            spawn_one_shot_upstream_expect_tip_lamports(0, signature.as_str())
        else {
            return;
        };

        let mut state =
            test_state_with_backends(upstream_url.as_str(), None, upstream_url.as_str(), None);
        state.config.allow_nonzero_tip = false;

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-2",
            "client_order_id": "client-order-rpc-tip-disabled",
            "request_id": "request-rpc-tip-disabled",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 7000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let response = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect("rpc route should coerce tip and bypass nonzero-tip reject");
        assert_eq!(
            response.get("tx_signature").and_then(Value::as_str),
            Some(signature.as_str())
        );
        assert_eq!(
            response.get("tip_lamports").and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            response
                .get("tip_policy")
                .and_then(|value| value.get("policy_code"))
                .and_then(Value::as_str),
            Some("rpc_tip_forced_zero")
        );
        let _ = upstream_handle.join();
    }

    #[tokio::test]
    async fn handle_submit_rejects_nonzero_tip_for_jito_when_disabled() {
        let state = {
            let mut state = test_state("http://127.0.0.1:1/upstream");
            state.config.allow_nonzero_tip = false;
            state.config.route_allowlist.insert("jito".to_string());
            state.config.route_backends.insert(
                "jito".to_string(),
                RouteBackend {
                    submit_url: "http://127.0.0.1:1/upstream".to_string(),
                    submit_fallback_url: None,
                    simulate_url: "http://127.0.0.1:1/upstream".to_string(),
                    simulate_fallback_url: None,
                    primary_auth_token: None,
                    fallback_auth_token: None,
                    send_rpc_url: None,
                    send_rpc_fallback_url: None,
                    send_rpc_primary_auth_token: None,
                    send_rpc_fallback_auth_token: None,
                },
            );
            state
        };

        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-jito-1",
            "client_order_id": "client-order-jito-tip",
            "request_id": "request-jito-tip",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "jito",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 1000,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("non-rpc route must still reject non-zero tip when disabled");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "tip_not_supported");
    }

    #[tokio::test]
    async fn handle_submit_rejects_compute_budget_limit_out_of_range() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-cu-limit",
            "client_order_id": "client-order-invalid-cu-limit",
            "request_id": "request-invalid-cu-limit",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 0,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("invalid cu_limit must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_compute_budget");
        assert_eq!(
            reject.detail,
            "compute_budget.cu_limit must be in 1..=1400000"
        );
    }

    #[tokio::test]
    async fn handle_submit_rejects_compute_budget_price_out_of_range() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-cu-price",
            "client_order_id": "client-order-invalid-cu-price",
            "request_id": "request-invalid-cu-price",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 15.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 0
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("invalid cu_price must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_compute_budget");
        assert_eq!(
            reject.detail,
            "compute_budget.cu_price_micro_lamports must be in 1..=10000000"
        );
    }

    #[tokio::test]
    async fn handle_submit_rejects_slippage_bps_out_of_range() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-invalid-slippage",
            "client_order_id": "client-order-invalid-slippage",
            "request_id": "request-invalid-slippage",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 0.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("invalid slippage must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_slippage_bps");
        assert_eq!(reject.detail, "slippage_bps must be finite and > 0");
    }

    #[tokio::test]
    async fn handle_submit_rejects_slippage_exceeding_route_cap() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let raw_body = json!({
            "contract_version": "v1",
            "signal_id": "signal-slippage-cap",
            "client_order_id": "client-order-slippage-cap",
            "request_id": "request-slippage-cap",
            "side": "buy",
            "token": "11111111111111111111111111111111",
            "notional_sol": 0.2,
            "signal_ts": "2026-02-20T00:00:00Z",
            "route": "rpc",
            "slippage_bps": 25.0,
            "route_slippage_cap_bps": 20.0,
            "tip_lamports": 0,
            "compute_budget": {
                "cu_limit": 300000,
                "cu_price_micro_lamports": 1500
            }
        });
        let raw_body_bytes = serde_json::to_vec(&raw_body).expect("serialize submit request");
        let request: SubmitRequest =
            serde_json::from_slice(&raw_body_bytes).expect("deserialize submit request");

        let reject = handle_submit(&state, &request, raw_body_bytes.as_slice())
            .await
            .expect_err("slippage above cap must reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "slippage_exceeds_route_cap");
        assert!(reject
            .detail
            .contains("slippage_bps=25 exceeds route_slippage_cap_bps=20"));
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_missing_deadline_before_config_check() {
        let signature = bs58::encode([8u8; 64]).into_string();
        let state = test_state("http://127.0.0.1:1/upstream");

        let reject = verify_submitted_signature_visibility(&state, "rpc", signature.as_str(), None)
            .await
            .expect_err("submit verify without deadline must fail closed before config check");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_missing_deadline_before_request() {
        let signature = bs58::encode([7u8; 64]).into_string();
        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec!["http://127.0.0.1:1/verify"],
            true,
        );

        let reject = verify_submitted_signature_visibility(&state, "rpc", signature.as_str(), None)
            .await
            .expect_err("submit verify without deadline must fail closed before request");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("missing deadline"));
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_invalid_signature_before_config_check() {
        let state = test_state("http://127.0.0.1:1/upstream");
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            "not-base58",
            Some(&submit_deadline),
        )
        .await
        .expect_err("invalid submit signature must fail closed before config check");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("invalid tx_signature"));
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_invalid_signature_before_request() {
        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec!["http://127.0.0.1:1/verify"],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);

        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            "not-base58",
            Some(&submit_deadline),
        )
        .await
        .expect_err("invalid submit signature must fail closed before request");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "invalid_request_body");
        assert!(reject.detail.contains("invalid tx_signature"));
    }

    #[tokio::test]
    async fn verify_submit_signature_seen_when_rpc_reports_confirmation() {
        let signature = bs58::encode([9u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[{"err":null,"confirmationStatus":"confirmed"}]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let result = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await;
        let Ok(SubmitSignatureVerification::Seen {
            confirmation_status,
        }) = result
        else {
            panic!("expected seen verification result");
        };
        assert_eq!(confirmation_status, "confirmed");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_returns_unseen_when_not_strict() {
        let signature = bs58::encode([10u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[null]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            false,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let result = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await;
        let Ok(SubmitSignatureVerification::Unseen { reason }) = result else {
            panic!("expected unseen verification result");
        };
        assert!(
            reason.contains("pending") || reason.contains("missing"),
            "reason={}",
            reason
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_when_pending_and_strict() {
        let signature = bs58::encode([11u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[null]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("strict mode must reject unseen signature");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_when_onchain_error_seen() {
        let signature = bs58::encode([12u8; 64]).into_string();
        let body =
            r#"{"jsonrpc":"2.0","result":{"value":[{"err":{"InstructionError":[0,"Custom"]}}]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            false,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("on-chain err must be terminal reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_submit_failed_onchain");
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_truncates_large_onchain_error_detail() {
        let signature = bs58::encode([54u8; 64]).into_string();
        let tail_marker = "SUBMIT_VERIFY_TAIL_MARKER_MUST_NOT_LEAK";
        let long_message = format!(
            "{}{}",
            "e".repeat(crate::http_utils::MAX_HTTP_ERROR_BODY_DETAIL_CHARS + 128),
            tail_marker
        );
        let body = format!(
            r#"{{"jsonrpc":"2.0","result":{{"value":[{{"err":{{"InstructionError":[0,"{}"]}}}}]}}}}"#,
            long_message
        );
        let Some((verify_url, handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", body.as_str())
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            false,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("on-chain err must be terminal reject");
        assert!(!reject.retryable);
        assert_eq!(reject.code, "upstream_submit_failed_onchain");
        assert!(
            reject.detail.contains("...[truncated]"),
            "detail should mark truncation: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked submit-verify tail marker: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_rejects_oversized_json_response_body() {
        let signature = bs58::encode([55u8; 64]).into_string();
        let tail_marker = "SUBMIT_VERIFY_JSON_TAIL_MARKER_MUST_NOT_LEAK";
        let large_padding = "q".repeat(crate::http_utils::MAX_HTTP_JSON_BODY_READ_BYTES + 1024);
        let body = format!(
            r#"{{"jsonrpc":"2.0","result":{{"value":[null]}},"padding":"{}{}"}}"#,
            large_padding, tail_marker
        );
        let Some((verify_url, handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", body.as_str())
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("oversized submit-verify JSON response should fail closed");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        assert!(
            reject.detail.contains("response_too_large"),
            "detail should classify oversized response: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains(tail_marker),
            "detail leaked submit-verify json tail marker: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_keeps_invalid_json_classification_with_marker_suffix() {
        let signature = bs58::encode([56u8; 64]).into_string();
        let body = r#"{"jsonrpc":"2.0","result":{"value":[null]}}...[truncated]"#;
        let Some((verify_url, handle)) =
            spawn_one_shot_upstream_raw(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("invalid JSON should reject as invalid_json classification");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        assert!(
            reject.detail.contains("invalid_json"),
            "detail should keep invalid_json classification: {}",
            reject.detail
        );
        assert!(
            !reject.detail.contains("response_too_large"),
            "detail should not classify as oversized: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_keeps_invalid_utf8_json_classification() {
        let signature = bs58::encode([57u8; 64]).into_string();
        let body = b"{\"jsonrpc\":\"2.0\",\"result\":{\"value\":[{\"err\":\"\xFF\"}]}}";
        let Some((verify_url, handle)) =
            spawn_one_shot_upstream_raw_bytes(200, "application/json", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("invalid UTF-8 JSON should reject as invalid_json classification");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        assert!(
            reject.detail.contains("invalid_json"),
            "detail should keep invalid_json classification: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_includes_non_success_http_body_in_reason() {
        let signature = bs58::encode([58u8; 64]).into_string();
        let body = "submit-verify-upstream-unavailable";
        let Some((verify_url, handle)) = spawn_one_shot_upstream_raw(503, "text/plain", body)
        else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("strict mode should reject after non-success upstream verify response");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        assert!(
            reject.detail.contains(body),
            "detail should include bounded upstream body: {}",
            reject.detail
        );
        let _ = handle.join();
    }

    #[tokio::test]
    async fn verify_submit_signature_treats_partial_valid_json_body_as_response_read_failed() {
        let signature = bs58::encode([63u8; 64]).into_string();
        let partial_valid_json = br#"{"jsonrpc":"2.0","result":{"value":[null]}}"#;
        let Some((verify_url, handle)) = spawn_one_shot_upstream_incomplete_body(
            200,
            "application/json",
            partial_valid_json,
            partial_valid_json.len() + 64,
        ) else {
            return;
        };

        let state = test_state_with_backends_and_verify(
            "http://127.0.0.1:1/upstream",
            None,
            "http://127.0.0.1:1/upstream",
            None,
            vec![verify_url.as_str()],
            true,
        );
        let submit_deadline = crate::submit_deadline::SubmitDeadline::new(1_000);
        let reject = verify_submitted_signature_visibility(
            &state,
            "rpc",
            signature.as_str(),
            Some(&submit_deadline),
        )
        .await
        .expect_err("transport-incomplete verify body must classify as response_read_failed");
        assert!(reject.retryable);
        assert_eq!(reject.code, "upstream_submit_signature_unseen");
        assert!(
            reject.detail.contains("response_read_failed"),
            "detail={}",
            reject.detail
        );
        let _ = handle.join();
    }

    fn test_state(endpoint: &str) -> AppState {
        test_state_with_backends(endpoint, None, endpoint, None)
    }

    fn test_state_with_backends_and_verify(
        submit_primary: &str,
        submit_fallback: Option<&str>,
        simulate_primary: &str,
        simulate_fallback: Option<&str>,
        verify_endpoints: Vec<&str>,
        verify_strict: bool,
    ) -> AppState {
        let mut state = test_state_with_backends(
            submit_primary,
            submit_fallback,
            simulate_primary,
            simulate_fallback,
        );
        if !verify_endpoints.is_empty() {
            state.config.submit_signature_verify = Some(SubmitSignatureVerifyConfig {
                endpoints: verify_endpoints
                    .into_iter()
                    .map(|value| value.to_string())
                    .collect(),
                attempts: 1,
                interval_ms: 1,
                strict: verify_strict,
            });
        }
        state
    }

    fn test_state_with_backends(
        submit_primary: &str,
        submit_fallback: Option<&str>,
        simulate_primary: &str,
        simulate_fallback: Option<&str>,
    ) -> AppState {
        let mut route_allowlist = HashSet::new();
        route_allowlist.insert("rpc".to_string());
        let mut route_backends = HashMap::new();
        route_backends.insert(
            "rpc".to_string(),
            RouteBackend {
                submit_url: submit_primary.to_string(),
                submit_fallback_url: submit_fallback.map(|value| value.to_string()),
                simulate_url: simulate_primary.to_string(),
                simulate_fallback_url: simulate_fallback.map(|value| value.to_string()),
                primary_auth_token: None,
                fallback_auth_token: None,
                send_rpc_url: None,
                send_rpc_fallback_url: None,
                send_rpc_primary_auth_token: None,
                send_rpc_fallback_auth_token: None,
            },
        );
        let config = ExecutorConfig {
            bind_addr: "127.0.0.1:8080".parse().expect("valid bind"),
            contract_version: "v1".to_string(),
            signer_pubkey: "11111111111111111111111111111111".to_string(),
            signer_source: SignerSource::File,
            signer_keypair_file: Some("/tmp/copybot-executor-test-keypair.json".to_string()),
            signer_kms_key_id: None,
            submit_fastlane_enabled: false,
            route_allowlist,
            route_backends,
            bearer_token: None,
            hmac_key_id: None,
            hmac_secret: None,
            hmac_ttl_sec: 30,
            hmac_nonce_cache_max_entries: DEFAULT_HMAC_NONCE_CACHE_MAX_ENTRIES,
            request_timeout_ms: 2_000,
            submit_total_budget_ms: default_submit_total_budget_ms(2_000),
            idempotency_db_path: ":memory:".to_string(),
            idempotency_claim_ttl_sec: DEFAULT_IDEMPOTENCY_CLAIM_TTL_SEC,
            idempotency_response_retention_sec: DEFAULT_IDEMPOTENCY_RESPONSE_RETENTION_SEC,
            idempotency_response_cleanup_batch_size: DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
            idempotency_response_cleanup_max_batches_per_run:
                DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
            idempotency_response_cleanup_worker_tick_sec: response_cleanup_worker_tick_sec(
                DEFAULT_IDEMPOTENCY_RESPONSE_RETENTION_SEC,
            ),
            max_notional_sol: 10.0,
            allow_nonzero_tip: true,
            submit_signature_verify: None,
        };
        let auth = Arc::new(AuthVerifier::new(
            config.bearer_token.clone(),
            config.hmac_key_id.clone(),
            config.hmac_secret.clone(),
            config.hmac_ttl_sec,
            config.hmac_nonce_cache_max_entries,
        ));
        let idempotency = Arc::new(
            SubmitIdempotencyStore::open(config.idempotency_db_path.as_str())
                .expect("idempotency store"),
        );
        let http = Client::builder()
            .timeout(Duration::from_millis(2_000))
            .build()
            .expect("http client");
        AppState {
            config,
            http,
            auth,
            idempotency,
        }
    }

    fn spawn_one_shot_upstream_raw(
        status: u16,
        content_type: &str,
        body: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        spawn_one_shot_upstream_raw_bytes(status, content_type, body.as_bytes())
    }

    fn spawn_one_shot_upstream_raw_bytes(
        status: u16,
        content_type: &str,
        body: &[u8],
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let response_body = body.to_vec();
        let content_type = content_type.to_string();
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
        .to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let _ = stream.read(&mut request_buf);
                let headers = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status,
                    reason,
                    content_type,
                    response_body.len()
                );
                let _ = stream.write_all(headers.as_bytes());
                let _ = stream.write_all(response_body.as_slice());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_incomplete_body(
        status: u16,
        content_type: &str,
        partial_body: &[u8],
        declared_content_length: usize,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        if declared_content_length <= partial_body.len() {
            return None;
        }
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let response_body = partial_body.to_vec();
        let content_type = content_type.to_string();
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
        .to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let _ = stream.read(&mut request_buf);
                let headers = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status, reason, content_type, declared_content_length
                );
                let _ = stream.write_all(headers.as_bytes());
                let _ = stream.write_all(response_body.as_slice());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_raw_with_delay(
        status: u16,
        content_type: &str,
        body: &str,
        delay_ms: u64,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let response_body = body.to_string();
        let content_type = content_type.to_string();
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
        .to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let _ = stream.read(&mut request_buf);
                thread::sleep(Duration::from_millis(delay_ms));
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    content_type,
                    response_body.len(),
                    response_body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_expect_bearer(
        expected_token: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let expected_token = expected_token.to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_raw = String::from_utf8_lossy(&request_buf[..bytes_read]);
                let authorized = request_raw.lines().any(|line| {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        return false;
                    }
                    let Some((name, value)) = trimmed.split_once(':') else {
                        return false;
                    };
                    if !name.trim().eq_ignore_ascii_case("authorization") {
                        return false;
                    }
                    let value = value.trim();
                    if value.len() < "bearer ".len()
                        || !value[.."bearer ".len()].eq_ignore_ascii_case("bearer ")
                    {
                        return false;
                    }
                    let provided_token = &value["bearer ".len()..];
                    provided_token == expected_token
                });

                let (status, reason, body) = if authorized {
                    (
                        200u16,
                        "OK",
                        "{\"status\":\"ok\",\"ok\":true,\"accepted\":true}",
                    )
                } else {
                    (401u16, "Unauthorized", "missing or invalid bearer token")
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 { "application/json" } else { "text/plain" },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_upstream_expect_tip_lamports(
        expected_tip_lamports: u64,
        signature: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let signature = signature.to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_raw = String::from_utf8_lossy(&request_buf[..bytes_read]);
                let request_body = request_raw
                    .split_once("\r\n\r\n")
                    .map(|(_, body)| body)
                    .unwrap_or_default();
                let tip_matches = serde_json::from_str::<Value>(request_body)
                    .ok()
                    .and_then(|value| value.get("tip_lamports").and_then(Value::as_u64))
                    .map(|value| value == expected_tip_lamports)
                    .unwrap_or(false);

                let (status, reason, body) = if tip_matches {
                    (
                        200u16,
                        "OK",
                        format!(
                            r#"{{"status":"ok","ok":true,"accepted":true,"tx_signature":"{}"}}"#,
                            signature
                        ),
                    )
                } else {
                    (
                        400u16,
                        "Bad Request",
                        "tip_lamports mismatch for rpc route".to_string(),
                    )
                };

                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 { "application/json" } else { "text/plain" },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn spawn_one_shot_send_rpc_expect_bearer(
        expected_token: &str,
        signature: &str,
    ) -> Option<(String, thread::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let expected_token = expected_token.to_string();
        let signature = signature.to_string();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request_buf = [0u8; 8192];
                let bytes_read = stream.read(&mut request_buf).unwrap_or(0);
                let request_raw = String::from_utf8_lossy(&request_buf[..bytes_read]);
                let authorized = request_raw.lines().any(|line| {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        return false;
                    }
                    let Some((name, value)) = trimmed.split_once(':') else {
                        return false;
                    };
                    if !name.trim().eq_ignore_ascii_case("authorization") {
                        return false;
                    }
                    let value = value.trim();
                    if value.len() < "bearer ".len()
                        || !value[.."bearer ".len()].eq_ignore_ascii_case("bearer ")
                    {
                        return false;
                    }
                    let provided_token = &value["bearer ".len()..];
                    provided_token == expected_token
                });

                let (status, reason, body) = if authorized {
                    (
                        200u16,
                        "OK",
                        format!(r#"{{"jsonrpc":"2.0","result":"{}"}}"#, signature),
                    )
                } else {
                    (
                        401u16,
                        "Unauthorized",
                        "missing or invalid bearer token".to_string(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    reason,
                    if status == 200 { "application/json" } else { "text/plain" },
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });
        Some((format!("http://{}/upstream", addr), handle))
    }

    fn test_signed_tx_base64_with_signature(signature: [u8; 64]) -> (String, String) {
        let mut tx_bytes = Vec::with_capacity(1 + signature.len() + 1);
        tx_bytes.push(1u8);
        tx_bytes.extend_from_slice(&signature);
        tx_bytes.push(0u8);
        (
            BASE64_STANDARD.encode(tx_bytes),
            bs58::encode(signature).into_string(),
        )
    }

    fn temp_secret_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic time")
            .as_nanos();
        let seq = TEMP_SECRET_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "copybot_adapter_secret_{}_{}_{}_{}.tmp",
            prefix,
            std::process::id(),
            nanos,
            seq
        ))
    }

    fn write_temp_secret_file(contents: &str) -> PathBuf {
        let path = temp_secret_path("value");
        stdfs::write(&path, contents).expect("write temp secret");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = stdfs::metadata(&path)
                .expect("stat temp secret")
                .permissions();
            perms.set_mode(0o600);
            stdfs::set_permissions(&path, perms).expect("set temp secret perms");
        }
        path
    }

    fn write_temp_signer_keypair_file(pubkey_bytes: [u8; 32]) -> PathBuf {
        let mut bytes = vec![0u8; 64];
        bytes[32..64].copy_from_slice(&pubkey_bytes);
        let json = serde_json::to_string(&bytes).expect("serialize keypair json");
        write_temp_secret_file(json.as_str())
    }

    fn cleanup_temp_secret_file(path: PathBuf) {
        let _ = stdfs::remove_file(path);
    }
}
