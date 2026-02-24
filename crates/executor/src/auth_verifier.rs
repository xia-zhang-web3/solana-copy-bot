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

            {
                let mut seen = self.nonce_seen_until_epoch.lock().await;
                seen.retain(|_, expires_at| *expires_at >= now);
                let nonce_key = format!("{}:{}", key_id, nonce);
                if seen.contains_key(&nonce_key) {
                    return Err(Reject::terminal(
                        "hmac_replay",
                        "HMAC nonce replay detected",
                    ));
                }
                seen.insert(nonce_key, now + max_skew);
            }

            let payload = format!(
                "{}\n{}\n{}\n{}",
                timestamp,
                ttl,
                nonce,
                String::from_utf8_lossy(raw_body)
            );
            let expected_signature =
                compute_hmac_signature_hex(hmac.secret.as_bytes(), payload.as_bytes()).map_err(
                    |_| Reject::terminal("hmac_invalid", "failed computing HMAC signature"),
                )?;
            if !constant_time_eq(signature.as_bytes(), expected_signature.as_bytes()) {
                return Err(Reject::terminal("hmac_invalid", "HMAC signature mismatch"));
            }
        }

        Ok(())
    }
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
