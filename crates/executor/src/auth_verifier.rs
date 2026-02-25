use std::{collections::HashMap, sync::Arc};

use axum::http::HeaderMap;
use chrono::Utc;
use tokio::sync::Mutex;

use crate::{
    auth_crypto::{compute_hmac_signature_hex, constant_time_eq},
    Reject,
};

#[derive(Clone)]
pub(crate) struct AuthVerifier {
    bearer_token: Option<String>,
    hmac: Option<HmacConfig>,
    nonce_seen_until_epoch: Arc<Mutex<HashMap<String, i64>>>,
    nonce_cache_max_entries: usize,
}

#[derive(Clone)]
struct HmacConfig {
    key_id: String,
    secret: String,
    ttl_sec: u64,
}

impl AuthVerifier {
    pub(crate) fn new(
        bearer_token: Option<String>,
        hmac_key_id: Option<String>,
        hmac_secret: Option<String>,
        hmac_ttl_sec: u64,
        hmac_nonce_cache_max_entries: u64,
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
        if let Some(expected) = self.bearer_token.as_deref() {
            let auth_header = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .ok_or_else(|| Reject::terminal("auth_missing", "missing Authorization header"))?;
            let provided = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
                Reject::terminal("auth_invalid", "Authorization header must use Bearer token")
            })?;
            if !constant_time_eq(provided.trim().as_bytes(), expected.as_bytes()) {
                return Err(Reject::terminal("auth_invalid", "Bearer token mismatch"));
            }
        }

        if let Some(hmac) = self.hmac.as_ref() {
            let key_id = get_required_header(headers, "x-copybot-key-id", "hmac_missing")?;
            if key_id != hmac.key_id {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-key-id mismatch",
                ));
            }
            let alg = get_required_header(headers, "x-copybot-signature-alg", "hmac_missing")?;
            if alg != "hmac-sha256-v1" {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-signature-alg must be hmac-sha256-v1",
                ));
            }
            let timestamp_raw =
                get_required_header(headers, "x-copybot-timestamp", "hmac_missing")?;
            let timestamp = timestamp_raw.parse::<i64>().map_err(|_| {
                Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-timestamp must be integer seconds",
                )
            })?;
            let ttl_raw = get_required_header(headers, "x-copybot-auth-ttl-sec", "hmac_missing")?;
            let ttl = ttl_raw.parse::<u64>().map_err(|_| {
                Reject::terminal("hmac_invalid", "x-copybot-auth-ttl-sec must be integer")
            })?;
            if ttl != hmac.ttl_sec {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-auth-ttl-sec mismatch",
                ));
            }
            let nonce = get_required_header(headers, "x-copybot-nonce", "hmac_missing")?;
            if nonce.is_empty() || nonce.len() > 128 {
                return Err(Reject::terminal(
                    "hmac_invalid",
                    "x-copybot-nonce must be 1..=128 chars",
                ));
            }
            let signature = get_required_header(headers, "x-copybot-signature", "hmac_missing")?;
            let now = Utc::now().timestamp();
            let max_skew = hmac.ttl_sec as i64;
            if (now - timestamp).abs() > max_skew {
                return Err(Reject::terminal(
                    "hmac_expired",
                    "HMAC timestamp outside TTL window",
                ));
            }

            let payload = build_hmac_payload_bytes(timestamp_raw, ttl_raw, nonce, raw_body);
            let expected_signature =
                compute_hmac_signature_hex(hmac.secret.as_bytes(), payload.as_slice()).map_err(
                    |_| Reject::terminal("hmac_invalid", "failed computing HMAC signature"),
                )?;
            if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
                return Err(Reject::terminal("hmac_invalid", "HMAC signature mismatch"));
            }

            {
                let now = Utc::now().timestamp();
                let mut seen = self.nonce_seen_until_epoch.lock().await;
                seen.retain(|_, expires_at| *expires_at >= now);
                let nonce_key = format!("{}:{}", key_id, nonce);
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
                let expires_at = timestamp.saturating_add(max_skew);
                seen.insert(nonce_key, expires_at);
            }
        }

        Ok(())
    }
}

fn build_hmac_payload_bytes(timestamp: &str, ttl: &str, nonce: &str, raw_body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        timestamp.len()
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
    err_code: &str,
) -> std::result::Result<&'a str, Reject> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Reject::terminal(err_code, format!("missing header {}", key)))
}

#[cfg(test)]
mod tests {
    use super::{build_hmac_payload_bytes, AuthVerifier};
    use crate::auth_crypto::compute_hmac_signature_hex;
    use axum::http::{HeaderMap, HeaderValue};
    use chrono::Utc;
    use tokio::time::{sleep, Duration};

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

    #[tokio::test]
    async fn auth_verifier_hmac_accepts_valid_signature_and_detects_replay() {
        let verifier = AuthVerifier::new(
            None,
            Some("kid-1".to_string()),
            Some("secret-1".to_string()),
            30,
            100_000,
        );
        let body = br#"{"status":"ok"}"#;
        let timestamp = Utc::now().timestamp();
        let payload = build_hmac_payload_bytes(timestamp.to_string().as_str(), "30", "nonce-1", body);
        let signature = compute_hmac_signature_hex(b"secret-1", payload.as_slice()).unwrap();
        let headers = build_hmac_headers("kid-1", 30, "nonce-1", timestamp, signature.as_str());

        verifier.verify(&headers, body).await.expect("first verify must pass");
        let reject = verifier
            .verify(&headers, body)
            .await
            .expect_err("replay must be rejected");
        assert_eq!(reject.code, "hmac_replay");
    }

    #[tokio::test]
    async fn auth_verifier_hmac_invalid_signature_does_not_burn_nonce() {
        let verifier = AuthVerifier::new(
            None,
            Some("kid-2".to_string()),
            Some("secret-2".to_string()),
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

        let payload = build_hmac_payload_bytes(timestamp.to_string().as_str(), "30", "nonce-2", body);
        let signature = compute_hmac_signature_hex(b"secret-2", payload.as_slice()).unwrap();
        let good_headers = build_hmac_headers("kid-2", 30, "nonce-2", timestamp, signature.as_str());
        verifier
            .verify(&good_headers, body)
            .await
            .expect("nonce must be available after invalid signature");
    }

    #[tokio::test]
    async fn auth_verifier_hmac_keeps_nonce_through_forward_skew_window() {
        let ttl_sec = 2;
        let verifier = AuthVerifier::new(
            None,
            Some("kid-3".to_string()),
            Some("secret-3".to_string()),
            ttl_sec,
            100_000,
        );
        let body = br#"{"action":"simulate"}"#;
        let timestamp = Utc::now().timestamp().saturating_add(ttl_sec as i64);
        let payload = build_hmac_payload_bytes(
            timestamp.to_string().as_str(),
            ttl_sec.to_string().as_str(),
            "nonce-3",
            body,
        );
        let signature = compute_hmac_signature_hex(b"secret-3", payload.as_slice()).unwrap();
        let headers = build_hmac_headers("kid-3", ttl_sec, "nonce-3", timestamp, signature.as_str());

        verifier.verify(&headers, body).await.expect("first verify must pass");
        sleep(Duration::from_millis(3_100)).await;

        let reject = verifier
            .verify(&headers, body)
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
            Some("secret-cap".to_string()),
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
        let headers_1 =
            build_hmac_headers("kid-cap", ttl_sec, "nonce-cap-1", timestamp, signature_1.as_str());
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
        let headers_2 =
            build_hmac_headers("kid-cap", ttl_sec, "nonce-cap-2", timestamp, signature_2.as_str());
        let reject = verifier
            .verify(&headers_2, body)
            .await
            .expect_err("nonce cache overflow should reject");
        assert_eq!(reject.code, "hmac_replay_cache_overflow");
        assert!(reject.retryable);
    }
}
