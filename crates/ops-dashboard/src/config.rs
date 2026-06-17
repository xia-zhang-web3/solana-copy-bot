use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

#[derive(Clone)]
pub struct DashboardConfig {
    pub bind_addr: SocketAddr,
    pub static_dir: PathBuf,
    pub auth_db_path: PathBuf,
    pub report_dir: PathBuf,
    pub username: String,
    pub password_hash: String,
    pub cookie_secure: bool,
    pub trust_proxy_headers: bool,
    pub session_ttl: Duration,
    pub idle_timeout: Duration,
    pub snapshot_max_age: Duration,
}

impl DashboardConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("COPYBOT_OPS_DASHBOARD_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8787".to_string())
            .parse()
            .context("invalid COPYBOT_OPS_DASHBOARD_BIND")?;
        let static_dir = env::var("COPYBOT_OPS_DASHBOARD_STATIC_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("ui/ops-dashboard/dist"));
        let auth_db_path = env::var("COPYBOT_OPS_DASHBOARD_AUTH_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".tmp/ops-dashboard-auth.db"));
        let report_dir = env::var("COPYBOT_OPS_DASHBOARD_REPORT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".tmp/ops-dashboard-reports"));
        let username =
            env::var("COPYBOT_OPS_DASHBOARD_USERNAME").unwrap_or_else(|_| "operator".to_string());
        let password_hash = env::var("COPYBOT_OPS_DASHBOARD_PASSWORD_HASH")
            .context("COPYBOT_OPS_DASHBOARD_PASSWORD_HASH is required")?;
        let cookie_secure = env_bool("COPYBOT_OPS_DASHBOARD_COOKIE_SECURE", true)?;
        let trust_proxy_headers = env_bool("COPYBOT_OPS_DASHBOARD_TRUST_PROXY_HEADERS", false)?;
        let session_ttl = Duration::from_secs(env_u64(
            "COPYBOT_OPS_DASHBOARD_SESSION_TTL_SECS",
            12 * 3600,
        )?);
        let idle_timeout = Duration::from_secs(env_u64(
            "COPYBOT_OPS_DASHBOARD_IDLE_TIMEOUT_SECS",
            2 * 3600,
        )?);
        let snapshot_max_age = Duration::from_secs(env_u64(
            "COPYBOT_OPS_DASHBOARD_SNAPSHOT_MAX_AGE_SECS",
            5 * 60,
        )?);

        Ok(Self {
            bind_addr,
            static_dir,
            auth_db_path,
            report_dir,
            username,
            password_hash,
            cookie_secure,
            trust_proxy_headers,
            session_ttl,
            idle_timeout,
            snapshot_max_age,
        })
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => value
            .parse::<bool>()
            .with_context(|| format!("invalid {name} boolean")),
        Err(_) => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("invalid {name} integer")),
        Err(_) => Ok(default),
    }
}
