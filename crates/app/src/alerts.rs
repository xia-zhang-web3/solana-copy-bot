use anyhow::{anyhow, Context, Result};
use copybot_storage::{RiskEventRow, SqliteStore};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use std::env;
use std::time::Duration;

pub const APP_ALERT_WEBHOOK_URL_ENV: &str = "COPYBOT_APP_ALERT_WEBHOOK_URL";
pub const APP_ALERT_TIMEOUT_MS_ENV: &str = "COPYBOT_APP_ALERT_TIMEOUT_MS";
pub const APP_ALERT_TEST_ON_STARTUP_ENV: &str = "COPYBOT_APP_ALERT_TEST_ON_STARTUP";
const DEFAULT_ALERT_TIMEOUT_MS: u64 = 3_000;
const ALERT_DELIVERY_CHANNEL: &str = "webhook";
const ALERT_DELIVERY_BATCH_LIMIT: u32 = 50;

#[derive(Debug, Clone)]
pub(crate) struct AlertDispatcher {
    client: Client,
    webhook_url: Url,
    test_on_startup: bool,
}

impl AlertDispatcher {
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let Some(raw_webhook_url) = env::var_os(APP_ALERT_WEBHOOK_URL_ENV) else {
            return Ok(None);
        };
        let raw_webhook_url = raw_webhook_url
            .into_string()
            .map_err(|_| anyhow!("{APP_ALERT_WEBHOOK_URL_ENV} must be valid UTF-8"))?;
        let webhook_url = raw_webhook_url.trim();
        if webhook_url.is_empty() {
            return Err(anyhow!(
                "{APP_ALERT_WEBHOOK_URL_ENV} must not be empty when set"
            ));
        }
        let timeout_ms = match env::var(APP_ALERT_TIMEOUT_MS_ENV) {
            Ok(raw) => raw.trim().parse::<u64>().map_err(|error| {
                anyhow!(
                    "{APP_ALERT_TIMEOUT_MS_ENV} must be a valid u64, got {:?}: {}",
                    raw.trim(),
                    error
                )
            })?,
            Err(env::VarError::NotPresent) => DEFAULT_ALERT_TIMEOUT_MS,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(anyhow!("{APP_ALERT_TIMEOUT_MS_ENV} must be valid UTF-8"));
            }
        };
        let test_on_startup = match env::var(APP_ALERT_TEST_ON_STARTUP_ENV) {
            Ok(raw) => parse_bool(raw.trim()).ok_or_else(|| {
                anyhow!(
                    "{APP_ALERT_TEST_ON_STARTUP_ENV} must be a valid bool (1/0/true/false/yes/no/on/off), got {:?}",
                    raw.trim()
                )
            })?,
            Err(env::VarError::NotPresent) => false,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(anyhow!(
                    "{APP_ALERT_TEST_ON_STARTUP_ENV} must be valid UTF-8"
                ));
            }
        };
        let webhook_url = Url::parse(webhook_url).with_context(|| {
            format!(
                "{APP_ALERT_WEBHOOK_URL_ENV} must be a valid absolute URL, got {:?}",
                webhook_url
            )
        })?;
        match webhook_url.scheme() {
            "https" => {}
            "http" if endpoint_host_is_local(&webhook_url) => {}
            other => {
                return Err(anyhow!(
                    "{APP_ALERT_WEBHOOK_URL_ENV} must use https or loopback http, got scheme={other:?}"
                ));
            }
        }
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms.max(1)))
            .build()
            .context("failed building alert webhook client")?;
        Ok(Some(Self {
            client,
            webhook_url,
            test_on_startup,
        }))
    }

    pub(crate) fn test_on_startup(&self) -> bool {
        self.test_on_startup
    }

    pub(crate) async fn send_startup_test(&self, env_name: &str) -> Result<()> {
        let payload = json!({
            "kind": "alert_delivery_test",
            "env": env_name,
            "ts": chrono::Utc::now().to_rfc3339(),
        });
        self.post_payload(&payload).await
    }

    pub(crate) async fn deliver_pending(&self, store: &SqliteStore) -> Result<usize> {
        let cursor = store
            .load_alert_delivery_cursor(ALERT_DELIVERY_CHANNEL)
            .context("failed reading alert delivery cursor")?;
        let events = store
            .list_risk_events_after_cursor(cursor, ALERT_DELIVERY_BATCH_LIMIT)
            .context("failed reading pending risk events for alert delivery")?;
        let mut delivered = 0usize;
        for event in events {
            self.post_payload(&risk_event_payload(&event))
                .await
                .with_context(|| {
                    format!(
                        "failed delivering alert webhook for event_type={} event_id={}",
                        event.event_type, event.event_id
                    )
                })?;
            store
                .upsert_alert_delivery_cursor(ALERT_DELIVERY_CHANNEL, event.rowid)
                .context("failed to advance alert delivery cursor")?;
            delivered += 1;
        }
        Ok(delivered)
    }

    async fn post_payload(&self, payload: &Value) -> Result<()> {
        self.client
            .post(self.webhook_url.clone())
            .json(payload)
            .send()
            .await
            .context("alert webhook request failed")?
            .error_for_status()
            .context("alert webhook responded with error status")?;
        Ok(())
    }
}

fn risk_event_payload(event: &RiskEventRow) -> Value {
    let details = event
        .details_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or_else(|| match &event.details_json {
            Some(raw) => Value::String(raw.clone()),
            None => Value::Null,
        });
    json!({
        "kind": "risk_event",
        "event_id": event.event_id,
        "event_type": event.event_type,
        "severity": event.severity,
        "ts": event.ts,
        "details": details,
    })
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn endpoint_host_is_local(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host
        .trim()
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use chrono::Utc;
    use copybot_storage::SqliteStore;
    use rusqlite::params;
    use std::ffi::OsString;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as AsyncMutex;

    static ALERT_ENV_LOCK: Mutex<()> = Mutex::new(());

    async fn spawn_capture_server(
        max_requests: usize,
    ) -> Result<(
        String,
        Arc<AsyncMutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    )> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let captured = Arc::new(AsyncMutex::new(Vec::new()));
        let captured_clone = Arc::clone(&captured);
        let handle = tokio::spawn(async move {
            for _ in 0..max_requests {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let Ok(body) = read_http_request_body(&mut socket).await else {
                    return;
                };
                captured_clone.lock().await.push(body);
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            }
        });
        Ok((format!("http://{}", addr), captured, handle))
    }

    async fn read_http_request_body(socket: &mut tokio::net::TcpStream) -> Result<String> {
        let mut buffer = Vec::new();
        let mut header_end = None;
        let mut chunk = [0u8; 4096];
        loop {
            let read = socket.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(position) = buffer.windows(4).position(|part| part == b"\r\n\r\n") {
                header_end = Some(position + 4);
                break;
            }
        }
        let header_end = header_end.context("missing HTTP header terminator")?;
        let headers = String::from_utf8(buffer[..header_end].to_vec())
            .context("request headers must be UTF-8 in test server")?;
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        while buffer.len() < header_end + content_length {
            let read = socket.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        let body = &buffer[header_end..header_end + content_length];
        String::from_utf8(body.to_vec()).context("request body must be UTF-8 in test server")
    }

    fn make_test_store(name: &str) -> Result<(SqliteStore, std::path::PathBuf)> {
        let unique = format!(
            "{}-{}-{}",
            name,
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("copybot-alerts-{unique}.db"));
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((store, db_path))
    }

    fn with_alert_env<T>(
        webhook_url: Option<OsString>,
        timeout_ms: Option<OsString>,
        test_on_startup: Option<OsString>,
        run: impl FnOnce() -> T,
    ) -> T {
        let _guard = ALERT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved_webhook = env::var_os(APP_ALERT_WEBHOOK_URL_ENV);
        let saved_timeout = env::var_os(APP_ALERT_TIMEOUT_MS_ENV);
        let saved_test = env::var_os(APP_ALERT_TEST_ON_STARTUP_ENV);
        env::remove_var(APP_ALERT_WEBHOOK_URL_ENV);
        env::remove_var(APP_ALERT_TIMEOUT_MS_ENV);
        env::remove_var(APP_ALERT_TEST_ON_STARTUP_ENV);
        if let Some(value) = webhook_url {
            env::set_var(APP_ALERT_WEBHOOK_URL_ENV, value);
        }
        if let Some(value) = timeout_ms {
            env::set_var(APP_ALERT_TIMEOUT_MS_ENV, value);
        }
        if let Some(value) = test_on_startup {
            env::set_var(APP_ALERT_TEST_ON_STARTUP_ENV, value);
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        env::remove_var(APP_ALERT_WEBHOOK_URL_ENV);
        env::remove_var(APP_ALERT_TIMEOUT_MS_ENV);
        env::remove_var(APP_ALERT_TEST_ON_STARTUP_ENV);
        if let Some(value) = saved_webhook {
            env::set_var(APP_ALERT_WEBHOOK_URL_ENV, value);
        }
        if let Some(value) = saved_timeout {
            env::set_var(APP_ALERT_TIMEOUT_MS_ENV, value);
        }
        if let Some(value) = saved_test {
            env::set_var(APP_ALERT_TEST_ON_STARTUP_ENV, value);
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    #[tokio::test]
    async fn deliver_pending_sends_warn_events_and_advances_cursor() -> Result<()> {
        let (server_url, captured, handle) = spawn_capture_server(1).await?;
        let dispatcher = AlertDispatcher {
            client: Client::builder()
                .timeout(Duration::from_millis(1_000))
                .build()?,
            webhook_url: Url::parse(&server_url)?,
            test_on_startup: false,
        };
        let (store, db_path) = make_test_store("alert-delivery")?;
        let now = Utc::now();
        store.insert_risk_event("info_event", "info", now, Some("{\"k\":\"v\"}"))?;
        store.insert_risk_event("warn_event", "warn", now, Some("{\"severity\":\"warn\"}"))?;

        let delivered = dispatcher.deliver_pending(&store).await?;
        assert_eq!(delivered, 1);
        let body = captured
            .lock()
            .await
            .pop()
            .context("captured body missing")?;
        assert!(body.contains("\"kind\":\"risk_event\""));
        assert!(body.contains("\"event_type\":\"warn_event\""));
        let cursor = store
            .load_alert_delivery_cursor(ALERT_DELIVERY_CHANNEL)?
            .context("cursor must be stored after delivery")?;
        assert!(cursor > 0);
        assert_eq!(dispatcher.deliver_pending(&store).await?, 0);

        handle.abort();
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[tokio::test]
    async fn send_startup_test_posts_webhook_payload() -> Result<()> {
        let (server_url, captured, handle) = spawn_capture_server(1).await?;
        let dispatcher = AlertDispatcher {
            client: Client::builder()
                .timeout(Duration::from_millis(1_000))
                .build()?,
            webhook_url: Url::parse(&server_url)?,
            test_on_startup: true,
        };
        dispatcher.send_startup_test("prod").await?;
        let body = captured
            .lock()
            .await
            .pop()
            .context("captured body missing")?;
        assert!(body.contains("\"kind\":\"alert_delivery_test\""));
        assert!(body.contains("\"env\":\"prod\""));
        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn late_same_ts_event_with_lower_lexical_id_is_still_delivered() -> Result<()> {
        let (server_url, captured, handle) = spawn_capture_server(2).await?;
        let dispatcher = AlertDispatcher {
            client: Client::builder()
                .timeout(Duration::from_millis(1_000))
                .build()?,
            webhook_url: Url::parse(&server_url)?,
            test_on_startup: false,
        };
        let (store, db_path) = make_test_store("alert-rowid-cursor")?;
        let ts = "2026-03-06T12:00:00+00:00";
        let conn = rusqlite::Connection::open(Path::new(&db_path))?;
        conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["z-event", "warn_event_a", "warn", ts, "{\"k\":\"a\"}"],
        )?;
        assert_eq!(dispatcher.deliver_pending(&store).await?, 1);
        conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["a-event", "warn_event_b", "warn", ts, "{\"k\":\"b\"}"],
        )?;
        assert_eq!(dispatcher.deliver_pending(&store).await?, 1);
        let payloads = captured.lock().await.clone();
        assert_eq!(payloads.len(), 2);
        assert!(payloads
            .iter()
            .any(|body| body.contains("\"event_type\":\"warn_event_a\"")));
        assert!(payloads
            .iter()
            .any(|body| body.contains("\"event_type\":\"warn_event_b\"")));
        handle.abort();
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[tokio::test]
    async fn deliver_pending_returns_error_on_fatal_cursor_write_failure() -> Result<()> {
        let (server_url, captured, handle) = spawn_capture_server(1).await?;
        let dispatcher = AlertDispatcher {
            client: Client::builder()
                .timeout(Duration::from_millis(1_000))
                .build()?,
            webhook_url: Url::parse(&server_url)?,
            test_on_startup: false,
        };
        let (store, db_path) = make_test_store("alert-delivery-cursor-fatal")?;
        let now = Utc::now();
        store.insert_risk_event("warn_event", "warn", now, Some("{\"severity\":\"warn\"}"))?;

        let conn = rusqlite::Connection::open(Path::new(&db_path))?;
        conn.execute_batch(
            "CREATE TRIGGER fail_alert_delivery_cursor_insert
             BEFORE INSERT ON alert_delivery_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = dispatcher
            .deliver_pending(&store)
            .await
            .expect_err("fatal alert-delivery cursor write must propagate");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("failed to advance alert delivery cursor"),
            "expected alert-delivery cursor context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to upsert alert delivery cursor"),
            "expected sqlite cursor upsert context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert_eq!(captured.lock().await.len(), 1);
        assert_eq!(
            store.load_alert_delivery_cursor(ALERT_DELIVERY_CHANNEL)?,
            None
        );

        handle.abort();
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn from_env_rejects_invalid_timeout() {
        with_alert_env(
            Some(OsString::from("https://alerts.example.test/copybot")),
            Some(OsString::from("fast")),
            None,
            || {
                let error = AlertDispatcher::from_env().expect_err("invalid timeout must reject");
                assert!(error.to_string().contains(APP_ALERT_TIMEOUT_MS_ENV));
            },
        );
    }

    #[test]
    fn from_env_rejects_invalid_test_flag_and_external_http() {
        with_alert_env(
            Some(OsString::from("http://alerts.example.test/copybot")),
            None,
            Some(OsString::from("maybe")),
            || {
                let error = AlertDispatcher::from_env()
                    .expect_err("invalid test flag must reject before dispatcher build");
                assert!(error.to_string().contains(APP_ALERT_TEST_ON_STARTUP_ENV));
            },
        );

        with_alert_env(
            Some(OsString::from("http://alerts.example.test/copybot")),
            None,
            Some(OsString::from("true")),
            || {
                let error = AlertDispatcher::from_env()
                    .expect_err("external plaintext webhook URL must reject");
                assert!(error.to_string().contains(APP_ALERT_WEBHOOK_URL_ENV));
            },
        );
    }
}
