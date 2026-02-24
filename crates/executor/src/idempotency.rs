use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::{fs, path::Path, sync::Mutex};

pub(crate) struct SubmitIdempotencyStore {
    conn: Mutex<Connection>,
}

impl SubmitIdempotencyStore {
    pub(crate) fn open(path: &str) -> Result<Self> {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return Err(anyhow::anyhow!(
                "COPYBOT_EXECUTOR_IDEMPOTENCY_DB_PATH must be non-empty"
            ));
        }
        if trimmed != ":memory:" {
            ensure_parent_dir(trimmed)?;
        }
        let conn = Connection::open(trimmed)
            .with_context(|| format!("open idempotency db path={} failed", trimmed))?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS executor_submit_idempotency (
                client_order_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_executor_submit_idempotency_request_id
                ON executor_submit_idempotency(request_id);
            "#,
        )
        .context("initialize idempotency schema failed")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub(crate) fn load_submit_response(&self, client_order_id: &str) -> Result<Option<Value>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?;
        let raw: Option<String> = conn
            .query_row(
                "SELECT response_json FROM executor_submit_idempotency WHERE client_order_id = ?1",
                params![client_order_id],
                |row| row.get(0),
            )
            .optional()
            .context("idempotency lookup query failed")?;
        let Some(raw_json) = raw else {
            return Ok(None);
        };
        let parsed: Value =
            serde_json::from_str(&raw_json).context("idempotency cached response is invalid JSON")?;
        Ok(Some(parsed))
    }

    pub(crate) fn probe(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?;
        conn.query_row("SELECT 1", [], |_| Ok(()))
            .context("idempotency probe query failed")
    }

    pub(crate) fn store_submit_response(
        &self,
        client_order_id: &str,
        request_id: &str,
        response: &Value,
    ) -> Result<bool> {
        let response_json =
            serde_json::to_string(response).context("serialize idempotency response failed")?;
        let now = Utc::now().to_rfc3339();
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?;
        let changed = conn
            .execute(
            r#"
            INSERT INTO executor_submit_idempotency (
                client_order_id,
                request_id,
                response_json,
                created_at_utc,
                updated_at_utc
            ) VALUES (?1, ?2, ?3, ?4, ?4)
            ON CONFLICT(client_order_id) DO NOTHING
            "#,
            params![client_order_id, request_id, response_json, now],
        )
        .context("idempotency insert failed")?;
        Ok(changed > 0)
    }
}

fn ensure_parent_dir(path: &str) -> Result<()> {
    let target = Path::new(path);
    let Some(parent) = target.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(parent)
        .with_context(|| format!("create idempotency parent dir failed path={}", parent.display()))
}

#[cfg(test)]
mod tests {
    use super::SubmitIdempotencyStore;
    use serde_json::json;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static TEMP_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic time")
            .as_nanos();
        let seq = TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "copybot_executor_idempotency_{}_{}_{}.sqlite3",
            std::process::id(),
            nanos,
            seq
        ))
    }

    #[test]
    fn store_and_load_submit_response_round_trip() {
        let db_path = temp_db_path();
        let store = SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref())
            .expect("open store");
        let response = json!({
            "status": "ok",
            "client_order_id": "order-1",
            "tx_signature": "sig-1"
        });
        store
            .store_submit_response("order-1", "req-1", &response)
            .expect("store response");
        let loaded = store
            .load_submit_response("order-1")
            .expect("load response")
            .expect("cached response");
        assert_eq!(loaded, response);
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn store_persists_across_store_reopen() {
        let db_path = temp_db_path();
        let db_path_str = db_path.to_string_lossy().to_string();
        let response = json!({
            "status": "ok",
            "client_order_id": "order-reopen-1",
            "tx_signature": "sig-reopen-1"
        });

        {
            let store = SubmitIdempotencyStore::open(db_path_str.as_str()).expect("open store 1");
            store
                .store_submit_response("order-reopen-1", "req-reopen-1", &response)
                .expect("store response");
        }

        let reopened = SubmitIdempotencyStore::open(db_path_str.as_str()).expect("open store 2");
        let loaded = reopened
            .load_submit_response("order-reopen-1")
            .expect("load response")
            .expect("cached response");
        assert_eq!(loaded, response);
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn store_does_not_overwrite_existing_response() {
        let db_path = temp_db_path();
        let store = SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref())
            .expect("open store");
        let original = json!({
            "status": "ok",
            "client_order_id": "order-immutable-1",
            "tx_signature": "sig-original"
        });
        let duplicate = json!({
            "status": "ok",
            "client_order_id": "order-immutable-1",
            "tx_signature": "sig-duplicate"
        });
        let inserted = store
            .store_submit_response("order-immutable-1", "req-1", &original)
            .expect("first store");
        assert!(inserted);
        let inserted_again = store
            .store_submit_response("order-immutable-1", "req-2", &duplicate)
            .expect("second store");
        assert!(!inserted_again);
        let loaded = store
            .load_submit_response("order-immutable-1")
            .expect("load response")
            .expect("cached response");
        assert_eq!(loaded, original);
        let _ = std::fs::remove_file(db_path);
    }
}
