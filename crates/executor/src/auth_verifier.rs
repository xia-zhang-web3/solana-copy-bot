use std::{collections::HashMap, sync::Arc};

use axum::http::HeaderMap;
use chrono::Utc;
use tokio::sync::Mutex;

use crate::{
    auth_crypto::{compute_hmac_signature_hex, constant_time_eq},
    idempotency::{HmacNonceClaimOutcome, SubmitIdempotencyStore},
    secret_value::SecretValue,
    Reject,
};

#[derive(Clone)]
pub(crate) struct AuthVerifier {
    bearer_token: Option<SecretValue>,
    hmac: Option<HmacConfig>,
    persistent_nonce_store: Option<Arc<SubmitIdempotencyStore>>,
    nonce_seen_until_epoch: Arc<Mutex<HashMap<String, i64>>>,
    nonce_cache_max_entries: usize,
}

#[derive(Clone)]
struct HmacConfig {
    key_id: String,
    secret: SecretValue,
    ttl_sec: u64,
}

impl AuthVerifier {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(
        bearer_token: Option<SecretValue>,
        hmac_key_id: Option<String>,
        hmac_secret: Option<SecretValue>,
        hmac_ttl_sec: u64,
        hmac_nonce_cache_max_entries: u64,
    ) -> Self {
        Self::build(
            bearer_token,
            hmac_key_id,
            hmac_secret,
            hmac_ttl_sec,
            hmac_nonce_cache_max_entries,
            None,
        )
    }

    pub(crate) fn new_with_nonce_store(
        bearer_token: Option<SecretValue>,
        hmac_key_id: Option<String>,
        hmac_secret: Option<SecretValue>,
        hmac_ttl_sec: u64,
        hmac_nonce_cache_max_entries: u64,
        persistent_nonce_store: Arc<SubmitIdempotencyStore>,
    ) -> Self {
        Self::build(
            bearer_token,
            hmac_key_id,
            hmac_secret,
            hmac_ttl_sec,
            hmac_nonce_cache_max_entries,
            Some(persistent_nonce_store),
        )
    }

    fn build(
        bearer_token: Option<SecretValue>,
        hmac_key_id: Option<String>,
        hmac_secret: Option<SecretValue>,
        hmac_ttl_sec: u64,
        hmac_nonce_cache_max_entries: u64,
        persistent_nonce_store: Option<Arc<SubmitIdempotencyStore>>,
    ) -> Self {
        let hmac = match (hmac_key_id, hmac_secret) {
            (Some(key_id), Some(secret)) => Some(HmacConfig {
                key_id,
                secret,
                ttl_sec: hmac_ttl_sec,
            }),
            _ => None,
        };
        Self {
            bearer_token,
            hmac,
            persistent_nonce_store,
            nonce_seen_until_epoch: Arc::new(Mutex::new(HashMap::new())),
            nonce_cache_max_entries: usize::try_from(hmac_nonce_cache_max_entries)
                .unwrap_or(usize::MAX)
                .max(1),
        }
    }

    pub(crate) async fn verify(
        &self,
        headers: &HeaderMap,
        raw_body: &[u8],
    ) -> std::result::Result<(), Reject> {
        self.verify_with_now_epoch(headers, raw_body, Utc::now().timestamp())
            .await
    }

    async fn verify_with_now_epoch(
        &self,
        headers: &HeaderMap,
        raw_body: &[u8],
        now_epoch: i64,
    ) -> std::result::Result<(), Reject> {
        if let Some(expected) = self.bearer_token.as_ref() {
            let auth_header = headers
                .get("authorization")
                .ok_or_else(|| Reject::terminal("auth_missing", "missing Authorization header"))?
                .to_str()
                .map_err(|_| {
                    Reject::terminal("auth_invalid", "Authorization header must be valid ASCII")
                })?
                .trim();
            let provided = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
                Reject::terminal("auth_invalid", "Authorization header must use Bearer token")
            })?;
            if !constant_time_eq(provided.trim().as_bytes(), expected.as_bytes()) {
                return Err(Reject::terminal("auth_invalid", "Bearer token mismatch"));
            }
        }

        if let Some(hmac) = self.hmac.as_ref() {
            let provided_key_id =
                get_required_header(headers, "x-copybot-key-id", "hmac_missing", "hmac_invalid")?;
            if !constant_time_eq(provided_key_id.as_bytes(), hmac.key_id.as_bytes()) {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-key-id mismatch",
                ));
            }
            let alg = get_required_header(
                headers,
                "x-copybot-signature-alg",
                "hmac_missing",
                "hmac_invalid",
            )?;
            if alg != "hmac-sha256-v1" {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-signature-alg must be hmac-sha256-v1",
                ));
            }
            let timestamp_text = get_required_header(
                headers,
                "x-copybot-timestamp",
                "hmac_missing",
                "hmac_invalid",
            )?;
            let timestamp = timestamp_text.parse::<i64>().map_err(|_| {
                Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-timestamp must be integer seconds",
                )
            })?;
            let ttl_text = get_required_header(
                headers,
                "x-copybot-auth-ttl-sec",
                "hmac_missing",
                "hmac_invalid",
            )?;
            let ttl = ttl_text.parse::<u64>().map_err(|_| {
                Reject::terminal("hmac_invalid", "x-copybot-auth-ttl-sec must be integer")
            })?;
            if ttl != hmac.ttl_sec {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-auth-ttl-sec mismatch",
                ));
            }
            let nonce =
                get_required_header(headers, "x-copybot-nonce", "hmac_missing", "hmac_invalid")?;
            if nonce.is_empty() || nonce.len() > 128 {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-nonce must be 1..=128 chars",
                ));
            }
            let signature = get_required_header(
                headers,
                "x-copybot-signature",
                "hmac_missing",
                "hmac_invalid",
            )?;
            let max_skew = hmac.ttl_sec as i64;
            if (now_epoch - timestamp).abs() > max_skew {
                return Err(Reject::terminal(
                    "hmac_expired",
                    "HMAC timestamp outside TTL window",
                ));
            }

            let payload = build_hmac_payload_bytes(timestamp_text, ttl_text, nonce, raw_body);
            let expected_signature =
                compute_hmac_signature_hex(hmac.secret.as_bytes(), payload.as_slice()).map_err(
                    |_| Reject::terminal("hmac_invalid", "failed computing HMAC signature"),
                )?;
            if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
                return Err(Reject::terminal("hmac_invalid", "HMAC signature mismatch"));
            }

            let nonce_key = format!("{}:{}", provided_key_id, nonce);
            let expires_at = timestamp.saturating_add(max_skew);
            if let Some(store) = self.persistent_nonce_store.as_ref() {
                match store.claim_hmac_nonce(
                    nonce_key.as_str(),
                    now_epoch,
                    expires_at,
                    self.nonce_cache_max_entries,
                ) {
                    Ok(HmacNonceClaimOutcome::Claimed) => {}
                    Ok(HmacNonceClaimOutcome::Replay) => {
                        return Err(Reject::terminal(
                            "hmac_replay",
                            "HMAC nonce replay detected",
                        ));
                    }
                    Ok(HmacNonceClaimOutcome::CapacityOverflow) => {
                        return Err(Reject::retryable(
                            "hmac_replay_cache_overflow",
                            format!(
                                "HMAC nonce replay cache capacity reached (max_entries={})",
                                self.nonce_cache_max_entries
                            ),
                        ));
                    }
                    Err(error) => {
                        return Err(Reject::retryable(
                            "hmac_replay_store_unavailable",
                            format!("persistent HMAC nonce claim failed: {}", error),
                        ));
                    }
                }
            } else {
                let mut seen = self.nonce_seen_until_epoch.lock().await;
                seen.retain(|_, expires_at| *expires_at >= now_epoch);
                if seen.contains_key(&nonce_key) {
                    return Err(Reject::terminal(
                        "hmac_replay",
                        "HMAC nonce replay detected",
                    ));
                }
                if seen.len() >= self.nonce_cache_max_entries {
                    return Err(Reject::retryable(
                        "hmac_replay_cache_overflow",
                        format!(
                            "HMAC nonce replay cache capacity reached (max_entries={})",
                            self.nonce_cache_max_entries
                        ),
                    ));
                }
                seen.insert(nonce_key, expires_at);
            }
        }

        Ok(())
    }
}

fn build_hmac_payload_bytes(timestamp: &str, ttl: &str, nonce: &str, raw_body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        timestamp
            .len()
            .saturating_add(ttl.len())
            .saturating_add(nonce.len())
            .saturating_add(raw_body.len())
            .saturating_add(3),
    );
    payload.extend_from_slice(timestamp.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(ttl.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(nonce.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(raw_body);
    payload
}

fn get_required_header<'a>(
    headers: &'a HeaderMap,
    key: &str,
    missing_code: &str,
    invalid_code: &str,
) -> std::result::Result<&'a str, Reject> {
    let value = headers
        .get(key)
        .ok_or_else(|| Reject::terminal(missing_code, format!("missing header {}", key)))?;
    let text = value.to_str().map_err(|_| {
        Reject::terminal(
            invalid_code,
            format!("invalid header {}: must be valid ASCII", key),
        )
    })?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(Reject::terminal(
            missing_code,
            format!("missing header {}", key),
        ));
    }
    Ok(trimmed)
}

#[cfg(test)]
mod tests {
    use super::{build_hmac_payload_bytes, AuthVerifier};
    use crate::auth_crypto::compute_hmac_signature_hex;
    use crate::idempotency::SubmitIdempotencyStore;
    use axum::http::{HeaderMap, HeaderValue};
    use chrono::Utc;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        sync::Arc,
    };

    fn build_hmac_headers(
        key_id: &str,
        ttl_sec: u64,
        nonce: &str,
        timestamp: i64,
        signature: &str,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-copybot-key-id", HeaderValue::from_str(key_id).unwrap());
        headers.insert(
            "x-copybot-signature-alg",
            HeaderValue::from_static("hmac-sha256-v1"),
        );
        headers.insert(
            "x-copybot-timestamp",
            HeaderValue::from_str(timestamp.to_string().as_str()).unwrap(),
        );
        headers.insert(
            "x-copybot-auth-ttl-sec",
            HeaderValue::from_str(ttl_sec.to_string().as_str()).unwrap(),
        );
        headers.insert("x-copybot-nonce", HeaderValue::from_str(nonce).unwrap());
        headers.insert(
            "x-copybot-signature",
            HeaderValue::from_str(signature).unwrap(),
        );
        headers
    }

    fn temp_db_path() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let seq = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "copybot_executor_hmac_replay_{}_{}_{}.sqlite3",
            std::process::id(),
            nanos,
            seq
        ))
    }

    #[tokio::test]
    async fn auth_verifier_hmac_accepts_valid_signature_and_detects_replay() {
        let verifier = AuthVerifier::new(
            None,
            Some("kid-1".to_string()),
            Some("secret-1".to_string().into()),
            30,
            100_000,
        );
        let body = br#"{"status":"ok"}"#;
        let timestamp = Utc::now().timestamp();
        let payload =
            build_hmac_payload_bytes(timestamp.to_string().as_str(), "30", "nonce-1", body);
        let signature = compute_hmac_signature_hex(b"secret-1", payload.as_slice()).unwrap();
        let headers = build_hmac_headers("kid-1", 30, "nonce-1", timestamp, signature.as_str());

        verifier
            .verify(&headers, body)
            .await
            .expect("first verify must pass");
        let reject = verifier
            .verify(&headers, body)
            .await
            .expect_err("replay must be rejected");
        assert_eq!(reject.code, "hmac_replay");
    }

    #[tokio::test]
    async fn auth_verifier_rejects_non_ascii_authorization_header() {
        let verifier =
            AuthVerifier::new(Some("token-1".to_string().into()), None, None, 30, 100_000);
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_bytes(&[0xff]).expect("non-ascii header value fixture"),
        );

        let reject = verifier
            .verify(&headers, br#"{"status":"ok"}"#)
            .await
            .expect_err("non-ascii Authorization header must reject");
        assert_eq!(reject.code, "auth_invalid");
        assert!(
            reject
                .detail
                .contains("Authorization header must be valid ASCII"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn auth_verifier_hmac_invalid_signature_does_not_burn_nonce() {
        let verifier = AuthVerifier::new(
            None,
            Some("kid-2".to_string()),
            Some("secret-2".to_string().into()),
            30,
            100_000,
        );
        let body = br#"{"side":"buy"}"#;
        let timestamp = Utc::now().timestamp();

        let bad_headers = build_hmac_headers("kid-2", 30, "nonce-2", timestamp, "deadbeef");
        let reject = verifier
            .verify(&bad_headers, body)
            .await
            .expect_err("bad signature must reject");
        assert_eq!(reject.code, "hmac_invalid");

        let payload =
            build_hmac_payload_bytes(timestamp.to_string().as_str(), "30", "nonce-2", body);
        let signature = compute_hmac_signature_hex(b"secret-2", payload.as_slice()).unwrap();
        let good_headers =
            build_hmac_headers("kid-2", 30, "nonce-2", timestamp, signature.as_str());
        verifier
            .verify(&good_headers, body)
            .await
            .expect("nonce must be available after invalid signature");
    }

    #[tokio::test]
    async fn auth_verifier_hmac_rejects_key_id_mismatch() {
        let verifier = AuthVerifier::new(
            None,
            Some("kid-expected".to_string()),
            Some("secret-key-id".to_string().into()),
            30,
            100_000,
        );
        let body = br#"{"status":"ok"}"#;
        let timestamp = Utc::now().timestamp();
        let payload =
            build_hmac_payload_bytes(timestamp.to_string().as_str(), "30", "nonce-key-id", body);
        let signature = compute_hmac_signature_hex(b"secret-key-id", payload.as_slice()).unwrap();
        let headers = build_hmac_headers(
            "kid-provided",
            30,
            "nonce-key-id",
            timestamp,
            signature.as_str(),
        );

        let reject = verifier
            .verify(&headers, body)
            .await
            .expect_err("mismatched key-id must reject");
        assert_eq!(reject.code, "hmac_invalid");
        assert!(reject.detail.contains("x-copybot-key-id mismatch"));
    }

    #[tokio::test]
    async fn auth_verifier_hmac_rejects_non_ascii_required_header() {
        let verifier = AuthVerifier::new(
            None,
            Some("kid-ascii".to_string()),
            Some("secret-ascii".to_string().into()),
            30,
            100_000,
        );
        let body = br#"{"status":"ok"}"#;
        let timestamp = Utc::now().timestamp();
        let payload =
            build_hmac_payload_bytes(timestamp.to_string().as_str(), "30", "nonce-ascii", body);
        let signature = compute_hmac_signature_hex(b"secret-ascii", payload.as_slice()).unwrap();
        let mut headers = build_hmac_headers(
            "kid-ascii",
            30,
            "nonce-ascii",
            timestamp,
            signature.as_str(),
        );
        headers.insert(
            "x-copybot-key-id",
            HeaderValue::from_bytes(&[0xff]).expect("non-ascii header value fixture"),
        );

        let reject = verifier
            .verify(&headers, body)
            .await
            .expect_err("non-ascii required hmac header must reject");
        assert_eq!(reject.code, "hmac_invalid");
        assert!(
            reject
                .detail
                .contains("invalid header x-copybot-key-id: must be valid ASCII"),
            "detail={}",
            reject.detail
        );
    }

    #[tokio::test]
    async fn auth_verifier_hmac_keeps_nonce_through_forward_skew_window() {
        let ttl_sec = 1;
        let verifier = AuthVerifier::new(
            None,
            Some("kid-3".to_string()),
            Some("secret-3".to_string().into()),
            ttl_sec,
            100_000,
        );
        let body = br#"{"action":"simulate"}"#;
        let now_epoch = Utc::now().timestamp();
        let timestamp = now_epoch.saturating_add(ttl_sec as i64);
        let payload = build_hmac_payload_bytes(
            timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-3",
            body,
        );
        let signature = compute_hmac_signature_hex(b"secret-3", payload.as_slice()).unwrap();
        let headers =
            build_hmac_headers("kid-3", ttl_sec, "nonce-3", timestamp, signature.as_str());

        verifier
            .verify_with_now_epoch(&headers, body, now_epoch)
            .await
            .expect("first verify must pass");

        let reject = verifier
            .verify_with_now_epoch(&headers, body, now_epoch.saturating_add(ttl_sec as i64))
            .await
            .expect_err("replay within accepted skew window must reject");
        assert_eq!(reject.code, "hmac_replay");
    }

    #[tokio::test]
    async fn auth_verifier_hmac_rejects_when_nonce_cache_capacity_reached() {
        let ttl_sec = 30;
        let verifier = AuthVerifier::new(
            None,
            Some("kid-cap".to_string()),
            Some("secret-cap".to_string().into()),
            ttl_sec,
            1,
        );
        let body = br#"{"action":"submit"}"#;
        let timestamp = Utc::now().timestamp();

        let payload_1 = build_hmac_payload_bytes(
            timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-cap-1",
            body,
        );
        let signature_1 = compute_hmac_signature_hex(b"secret-cap", payload_1.as_slice()).unwrap();
        let headers_1 = build_hmac_headers(
            "kid-cap",
            ttl_sec,
            "nonce-cap-1",
            timestamp,
            signature_1.as_str(),
        );
        verifier
            .verify(&headers_1, body)
            .await
            .expect("first nonce should pass");

        let payload_2 = build_hmac_payload_bytes(
            timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-cap-2",
            body,
        );
        let signature_2 = compute_hmac_signature_hex(b"secret-cap", payload_2.as_slice()).unwrap();
        let headers_2 = build_hmac_headers(
            "kid-cap",
            ttl_sec,
            "nonce-cap-2",
            timestamp,
            signature_2.as_str(),
        );
        let reject = verifier
            .verify(&headers_2, body)
            .await
            .expect_err("nonce cache overflow should reject");
        assert_eq!(reject.code, "hmac_replay_cache_overflow");
        assert!(reject.retryable);
    }

    #[tokio::test]
    async fn auth_verifier_hmac_evicts_expired_nonce_before_capacity_check() {
        let ttl_sec = 1;
        let verifier = AuthVerifier::new(
            None,
            Some("kid-evict".to_string()),
            Some("secret-evict".to_string().into()),
            ttl_sec,
            1,
        );
        let body = br#"{"action":"simulate"}"#;
        {
            let mut seen = verifier.nonce_seen_until_epoch.lock().await;
            seen.insert(
                "kid-evict:nonce-expired".to_string(),
                Utc::now().timestamp().saturating_sub(1),
            );
        }

        let timestamp_2 = Utc::now().timestamp();
        let payload_2 = build_hmac_payload_bytes(
            timestamp_2.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-evict-2",
            body,
        );
        let signature_2 =
            compute_hmac_signature_hex(b"secret-evict", payload_2.as_slice()).unwrap();
        let headers_2 = build_hmac_headers(
            "kid-evict",
            ttl_sec,
            "nonce-evict-2",
            timestamp_2,
            signature_2.as_str(),
        );
        verifier
            .verify(&headers_2, body)
            .await
            .expect("expired nonce entry must be evicted before capacity check");
    }

    #[tokio::test]
    async fn auth_verifier_hmac_persistent_nonce_rejects_replay_after_restart() {
        let ttl_sec = 30;
        let db_path = temp_db_path();
        let db_path_str = db_path.to_string_lossy().to_string();
        let body = br#"{"status":"ok"}"#;
        let timestamp = 1_000_i64;
        let payload = build_hmac_payload_bytes(
            timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-restart",
            body,
        );
        let signature = compute_hmac_signature_hex(b"secret-restart", payload.as_slice()).unwrap();
        let headers = build_hmac_headers(
            "kid-restart",
            ttl_sec,
            "nonce-restart",
            timestamp,
            signature.as_str(),
        );

        {
            let store = Arc::new(
                SubmitIdempotencyStore::open(db_path_str.as_str())
                    .expect("persistent replay store"),
            );
            let verifier = AuthVerifier::new_with_nonce_store(
                None,
                Some("kid-restart".to_string()),
                Some("secret-restart".to_string().into()),
                ttl_sec,
                100_000,
                store,
            );
            verifier
                .verify_with_now_epoch(&headers, body, timestamp)
                .await
                .expect("first verify must pass");
        }

        let reopened_store = Arc::new(
            SubmitIdempotencyStore::open(db_path_str.as_str())
                .expect("reopened persistent replay store"),
        );
        let reopened = AuthVerifier::new_with_nonce_store(
            None,
            Some("kid-restart".to_string()),
            Some("secret-restart".to_string().into()),
            ttl_sec,
            100_000,
            reopened_store,
        );
        let reject = reopened
            .verify_with_now_epoch(&headers, body, timestamp)
            .await
            .expect_err("replay after restart must reject");
        assert_eq!(reject.code, "hmac_replay");

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn auth_verifier_hmac_persistent_nonce_evicts_expired_entry_after_restart() {
        let ttl_sec = 1;
        let db_path = temp_db_path();
        let db_path_str = db_path.to_string_lossy().to_string();
        let body = br#"{"status":"ok"}"#;
        let first_timestamp = 1_000_i64;
        let first_payload = build_hmac_payload_bytes(
            first_timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-expire-restart",
            body,
        );
        let first_signature =
            compute_hmac_signature_hex(b"secret-expire-restart", first_payload.as_slice()).unwrap();
        let first_headers = build_hmac_headers(
            "kid-expire-restart",
            ttl_sec,
            "nonce-expire-restart",
            first_timestamp,
            first_signature.as_str(),
        );

        {
            let store = Arc::new(
                SubmitIdempotencyStore::open(db_path_str.as_str())
                    .expect("persistent replay store"),
            );
            let verifier = AuthVerifier::new_with_nonce_store(
                None,
                Some("kid-expire-restart".to_string()),
                Some("secret-expire-restart".to_string().into()),
                ttl_sec,
                100_000,
                store,
            );
            verifier
                .verify_with_now_epoch(&first_headers, body, first_timestamp)
                .await
                .expect("first verify must pass");
        }

        let second_timestamp = first_timestamp.saturating_add(ttl_sec as i64 + 1);
        let second_payload = build_hmac_payload_bytes(
            second_timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-expire-restart",
            body,
        );
        let second_signature =
            compute_hmac_signature_hex(b"secret-expire-restart", second_payload.as_slice())
                .unwrap();
        let second_headers = build_hmac_headers(
            "kid-expire-restart",
            ttl_sec,
            "nonce-expire-restart",
            second_timestamp,
            second_signature.as_str(),
        );

        let reopened_store = Arc::new(
            SubmitIdempotencyStore::open(db_path_str.as_str())
                .expect("reopened persistent replay store"),
        );
        let reopened = AuthVerifier::new_with_nonce_store(
            None,
            Some("kid-expire-restart".to_string()),
            Some("secret-expire-restart".to_string().into()),
            ttl_sec,
            100_000,
            reopened_store,
        );
        reopened
            .verify_with_now_epoch(&second_headers, body, second_timestamp)
            .await
            .expect("expired persistent nonce should be evicted after restart");

        let _ = std::fs::remove_file(db_path);
    }
}
