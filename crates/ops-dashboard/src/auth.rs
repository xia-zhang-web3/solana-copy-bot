use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    http::{
        header::{COOKIE, SET_COOKIE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};
use rand_core::OsRng;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const SESSION_COOKIE: &str = "grindscout_session";

pub struct AuthStore {
    conn: Mutex<Connection>,
    limiter: Mutex<LoginLimiter>,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct SessionResponse {
    pub authenticated: bool,
    pub username: Option<String>,
}

struct LoginLimiter {
    failures: HashMap<String, Vec<i64>>,
}

impl AuthStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create auth db dir {}", parent.display()))?;
        }
        let conn =
            Connection::open(path).with_context(|| format!("open auth db {}", path.display()))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
            limiter: Mutex::new(LoginLimiter {
                failures: HashMap::new(),
            }),
        })
    }

    pub fn verify_password(hash: &str, password: &str) -> bool {
        PasswordHash::new(hash)
            .ok()
            .and_then(|parsed| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &parsed)
                    .ok()
            })
            .is_some()
    }

    pub fn create_session(&self, username: &str, ttl: Duration) -> Result<String> {
        let now = now_ts();
        let session_id = Uuid::new_v4().to_string();
        self.conn.lock().unwrap().execute(
            "INSERT INTO sessions (id, username, created_at, expires_at, last_seen_at)
             VALUES (?1, ?2, ?3, ?4, ?3)",
            params![session_id, username, now, now + ttl.as_secs() as i64],
        )?;
        Ok(session_id)
    }

    pub fn validate_session(
        &self,
        session_id: &str,
        idle_timeout: Duration,
    ) -> Result<Option<String>> {
        let now = now_ts();
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT username, expires_at, last_seen_at FROM sessions WHERE id = ?1")?;
        let mut rows = stmt.query(params![session_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let username: String = row.get(0)?;
        let expires_at: i64 = row.get(1)?;
        let last_seen_at: i64 = row.get(2)?;
        if expires_at <= now || last_seen_at + idle_timeout.as_secs() as i64 <= now {
            drop(rows);
            drop(stmt);
            conn.execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
            return Ok(None);
        }
        drop(rows);
        drop(stmt);
        conn.execute(
            "UPDATE sessions SET last_seen_at = ?2 WHERE id = ?1",
            params![session_id, now],
        )?;
        Ok(Some(username))
    }

    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
        Ok(())
    }

    pub fn login_limited(&self, key: &str) -> bool {
        self.limiter.lock().unwrap().is_limited(key.to_string())
    }

    pub fn record_failure(&self, key: &str) {
        self.limiter.lock().unwrap().record(key.to_string());
    }
}

impl LoginLimiter {
    fn is_limited(&mut self, key: String) -> bool {
        let now = now_ts();
        let window_start = now - 600;
        let failures = self.failures.entry(key).or_default();
        failures.retain(|ts| *ts >= window_start);
        failures.len() >= 5
    }

    fn record(&mut self, key: String) {
        self.failures.entry(key).or_default().push(now_ts());
    }
}

pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|err| anyhow::anyhow!("failed to hash password: {err}"))?
        .to_string())
}

pub fn session_from_headers(headers: &HeaderMap) -> Option<String> {
    let header = headers.get(COOKIE)?.to_str().ok()?;
    header.split(';').find_map(|piece| {
        let (name, value) = piece.trim().split_once('=')?;
        (name == SESSION_COOKIE).then(|| value.to_string())
    })
}

pub fn rate_limit_key(
    headers: &HeaderMap,
    remote: SocketAddr,
    trust_proxy_headers: bool,
) -> String {
    if trust_proxy_headers {
        if let Some(ip) = forwarded_ip(headers) {
            return ip.to_string();
        }
    }
    remote.ip().to_string()
}

fn forwarded_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| raw.split(',').next())
        .and_then(|raw| raw.trim().parse::<IpAddr>().ok())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .and_then(|raw| raw.trim().parse::<IpAddr>().ok())
        })
}

pub fn session_cookie(session_id: &str, max_age: Duration, secure: bool) -> HeaderValue {
    let secure_part = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={session_id}; Path=/; HttpOnly; SameSite=Strict{secure_part}; Max-Age={}",
        max_age.as_secs()
    ))
    .expect("session cookie header")
}

pub fn expired_cookie(secure: bool) -> HeaderValue {
    let secure_part = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}=deleted; Path=/; HttpOnly; SameSite=Strict{secure_part}; Max-Age=0"
    ))
    .expect("expired cookie header")
}

pub fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(SessionResponse {
            authenticated: false,
            username: None,
        }),
    )
        .into_response()
}

pub fn with_cookie<T: Serialize>(cookie: HeaderValue, body: T) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(SET_COOKIE, cookie);
    (headers, Json(body)).into_response()
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
