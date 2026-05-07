use super::*;
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::SqliteStore;
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
    let store = SqliteStore::open(Path::new(&db_path))?;
    store.ensure_system_event_tables()?;
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
