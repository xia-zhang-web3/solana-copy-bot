use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex, TryLockError,
    },
};

const MIN_CLAIM_CLEANUP_INTERVAL_SEC: i64 = 5;
const MAX_CLAIM_CLEANUP_INTERVAL_SEC: i64 = 60;
const META_KEY_CLAIM_CLEANUP_LAST_UNIX: &str = "claim_cleanup_last_unix";
const MIN_RESPONSE_CLEANUP_INTERVAL_SEC: i64 = 60;
const MAX_RESPONSE_CLEANUP_INTERVAL_SEC: i64 = 3_600;
const META_KEY_RESPONSE_CLEANUP_LAST_UNIX: &str = "response_cleanup_last_unix";
pub(crate) const DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE: u64 = 500;
pub(crate) const DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN: u64 = 4;

pub(crate) struct SubmitIdempotencyStore {
    conn: Mutex<Connection>,
    last_claim_cleanup_unix: AtomicI64,
    last_response_cleanup_unix: AtomicI64,
}

pub(crate) enum SubmitClaimOutcome {
    Claimed,
    Cached(Value),
    InFlight,
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
            CREATE INDEX IF NOT EXISTS idx_executor_submit_idempotency_updated_at
                ON executor_submit_idempotency(updated_at_utc);
            CREATE TABLE IF NOT EXISTS executor_submit_idempotency_claims (
                client_order_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                claimed_at_unix INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_executor_submit_idempotency_claims_claimed_at
                ON executor_submit_idempotency_claims(claimed_at_unix);
            CREATE TABLE IF NOT EXISTS executor_runtime_meta (
                meta_key TEXT PRIMARY KEY,
                value_int INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            );
            "#,
        )
        .context("initialize idempotency schema failed")?;
        Ok(Self {
            conn: Mutex::new(conn),
            last_claim_cleanup_unix: AtomicI64::new(0),
            last_response_cleanup_unix: AtomicI64::new(0),
        })
    }

    pub(crate) fn load_submit_response(&self, client_order_id: &str) -> Result<Option<Value>> {
        let key = client_order_id.trim();
        if key.is_empty() {
            return Err(anyhow::anyhow!("client_order_id must be non-empty"));
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?;
        let raw: Option<String> = conn
            .query_row(
                "SELECT response_json FROM executor_submit_idempotency WHERE client_order_id = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .context("idempotency lookup query failed")?;
        let Some(raw_json) = raw else {
            return Ok(None);
        };
        let parsed: Value = serde_json::from_str(&raw_json)
            .context("idempotency cached response is invalid JSON")?;
        Ok(Some(parsed))
    }

    pub(crate) fn load_cached_or_claim_submit(
        &self,
        client_order_id: &str,
        request_id: &str,
        claim_ttl_sec: u64,
    ) -> Result<SubmitClaimOutcome> {
        let key = client_order_id.trim();
        if key.is_empty() {
            return Err(anyhow::anyhow!("client_order_id must be non-empty"));
        }
        let req = request_id.trim();
        if req.is_empty() {
            return Err(anyhow::anyhow!("request_id must be non-empty"));
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?;

        let now_unix = Utc::now().timestamp();
        let ttl = i64::try_from(claim_ttl_sec).unwrap_or(i64::MAX).max(1);
        let stale_before = now_unix.saturating_sub(ttl);
        let cleanup_interval_sec = claim_cleanup_interval_sec(ttl);
        let last_cleanup_unix = self.last_claim_cleanup_unix.load(Ordering::Relaxed);
        if should_run_claim_cleanup(now_unix, last_cleanup_unix, cleanup_interval_sec) {
            let global_last_cleanup_unix = load_global_claim_cleanup_last_unix(&conn)?;
            if should_run_claim_cleanup(now_unix, global_last_cleanup_unix, cleanup_interval_sec) {
                conn.execute(
                    "DELETE FROM executor_submit_idempotency_claims WHERE claimed_at_unix <= ?1",
                    params![stale_before],
                )
                .context("idempotency stale claim cleanup failed")?;
                store_global_claim_cleanup_last_unix(&conn, now_unix)?;
                self.last_claim_cleanup_unix
                    .store(now_unix, Ordering::Relaxed);
            } else {
                self.last_claim_cleanup_unix
                    .store(global_last_cleanup_unix, Ordering::Relaxed);
            }
        }
        let raw: Option<String> = conn
            .query_row(
                "SELECT response_json FROM executor_submit_idempotency WHERE client_order_id = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .context("idempotency lookup query failed")?;
        if let Some(raw_json) = raw {
            let parsed: Value = serde_json::from_str(&raw_json)
                .context("idempotency cached response is invalid JSON")?;
            return Ok(SubmitClaimOutcome::Cached(parsed));
        }

        let insert_claim = |conn: &Connection| -> Result<usize> {
            conn.execute(
                r#"
                INSERT INTO executor_submit_idempotency_claims (
                    client_order_id,
                    request_id,
                    claimed_at_unix
                ) VALUES (?1, ?2, ?3)
                ON CONFLICT(client_order_id) DO NOTHING
                "#,
                params![key, req, now_unix],
            )
            .context("idempotency claim insert failed")
        };

        let inserted = insert_claim(&conn)?;
        if inserted > 0 {
            return Ok(SubmitClaimOutcome::Claimed);
        }

        // If claim insert conflicted, the row may still be stale. Reap stale entry for this key
        // and retry once so stale claims are self-healed even when global cleanup is throttled.
        let removed_stale_for_key = conn
            .execute(
                r#"
                DELETE FROM executor_submit_idempotency_claims
                WHERE client_order_id = ?1 AND claimed_at_unix <= ?2
                "#,
                params![key, stale_before],
            )
            .context("idempotency stale claim reclaim failed")?;
        if removed_stale_for_key > 0 {
            let inserted_after_reclaim = insert_claim(&conn)?;
            if inserted_after_reclaim > 0 {
                return Ok(SubmitClaimOutcome::Claimed);
            }
        }
        Ok(SubmitClaimOutcome::InFlight)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn run_response_cleanup_if_due(
        &self,
        response_retention_sec: u64,
        response_cleanup_batch_size: u64,
        response_cleanup_max_batches_per_run: u64,
    ) -> Result<()> {
        self.run_response_cleanup_if_due_internal(
            response_retention_sec,
            response_cleanup_batch_size,
            response_cleanup_max_batches_per_run,
            false,
        )
        .map(|_| ())
    }

    pub(crate) fn run_response_cleanup_if_due_nonblocking(
        &self,
        response_retention_sec: u64,
        response_cleanup_batch_size: u64,
        response_cleanup_max_batches_per_run: u64,
    ) -> Result<bool> {
        self.run_response_cleanup_if_due_internal(
            response_retention_sec,
            response_cleanup_batch_size,
            response_cleanup_max_batches_per_run,
            true,
        )
    }

    fn run_response_cleanup_if_due_internal(
        &self,
        response_retention_sec: u64,
        response_cleanup_batch_size: u64,
        response_cleanup_max_batches_per_run: u64,
        nonblocking: bool,
    ) -> Result<bool> {
        let now_unix = Utc::now().timestamp();
        let response_retention = i64::try_from(response_retention_sec)
            .map_err(|_| anyhow::anyhow!("idempotency response retention exceeds i64 range"))?
            .max(1);
        let response_cleanup_batch_size_i64 =
            i64::try_from(response_cleanup_batch_size).map_err(|_| {
                anyhow::anyhow!("idempotency response cleanup batch_size exceeds i64 range")
            })?;
        let response_cleanup_max_batches_usize = usize::try_from(
            response_cleanup_max_batches_per_run,
        )
        .map_err(|_| anyhow::anyhow!("idempotency response cleanup max_batches exceeds usize range"))?;
        let response_cleanup_interval = response_cleanup_interval_sec(response_retention);
        let last_response_cleanup_unix = self.last_response_cleanup_unix.load(Ordering::Relaxed);
        if !should_run_claim_cleanup(
            now_unix,
            last_response_cleanup_unix,
            response_cleanup_interval,
        ) {
            return Ok(true);
        }

        let conn = if nonblocking {
            match self.conn.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => return Ok(false),
                Err(TryLockError::Poisoned(_)) => {
                    return Err(anyhow::anyhow!("idempotency db mutex poisoned"));
                }
            }
        } else {
            self.conn
                .lock()
                .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?
        };
        let global_last_response_cleanup_unix = load_global_response_cleanup_last_unix(&conn)?;
        if should_run_claim_cleanup(
            now_unix,
            global_last_response_cleanup_unix,
            response_cleanup_interval,
        ) {
            let stale_response_before_unix = now_unix.saturating_sub(response_retention);
            let stale_response_before_rfc3339 =
                chrono::DateTime::<Utc>::from_timestamp(stale_response_before_unix, 0)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "idempotency response cleanup cutoff timestamp is out of range"
                        )
                    })?
                    .to_rfc3339();
            let cleanup_completed = delete_stale_cached_responses_in_batches(
                &conn,
                stale_response_before_rfc3339.as_str(),
                response_cleanup_batch_size_i64,
                response_cleanup_max_batches_usize,
            )?;
            if cleanup_completed {
                store_global_response_cleanup_last_unix(&conn, now_unix)?;
                self.last_response_cleanup_unix
                    .store(now_unix, Ordering::Relaxed);
            }
        } else {
            self.last_response_cleanup_unix
                .store(global_last_response_cleanup_unix, Ordering::Relaxed);
        }
        Ok(true)
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
        let key = client_order_id.trim();
        if key.is_empty() {
            return Err(anyhow::anyhow!("client_order_id must be non-empty"));
        }
        let req = request_id.trim();
        if req.is_empty() {
            return Err(anyhow::anyhow!("request_id must be non-empty"));
        }
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
                params![key, req, response_json, now],
            )
            .context("idempotency insert failed")?;
        Ok(changed > 0)
    }

    pub(crate) fn release_submit_claim(
        &self,
        client_order_id: &str,
        request_id: &str,
    ) -> Result<bool> {
        let key = client_order_id.trim();
        let req = request_id.trim();
        if key.is_empty() || req.is_empty() {
            return Ok(false);
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("idempotency db mutex poisoned"))?;
        let removed = conn
            .execute(
                "DELETE FROM executor_submit_idempotency_claims WHERE client_order_id = ?1 AND request_id = ?2",
                params![key, req],
            )
            .context("idempotency claim release failed")?;
        Ok(removed > 0)
    }
}

fn claim_cleanup_interval_sec(claim_ttl_sec: i64) -> i64 {
    (claim_ttl_sec / 2).clamp(
        MIN_CLAIM_CLEANUP_INTERVAL_SEC,
        MAX_CLAIM_CLEANUP_INTERVAL_SEC,
    )
}

fn response_cleanup_interval_sec(response_retention_sec: i64) -> i64 {
    (response_retention_sec / 2).clamp(
        MIN_RESPONSE_CLEANUP_INTERVAL_SEC,
        MAX_RESPONSE_CLEANUP_INTERVAL_SEC,
    )
}

fn delete_stale_cached_responses_in_batches(
    conn: &Connection,
    stale_response_before_rfc3339: &str,
    batch_size: i64,
    max_batches: usize,
) -> Result<bool> {
    let batch_size_usize = usize::try_from(batch_size.max(1)).unwrap_or(usize::MAX);
    for _ in 0..max_batches.max(1) {
        let deleted_rows = conn
            .execute(
                r#"
                DELETE FROM executor_submit_idempotency
                WHERE rowid IN (
                    SELECT rowid
                    FROM executor_submit_idempotency
                    WHERE updated_at_utc <= ?1
                    ORDER BY updated_at_utc
                    LIMIT ?2
                )
                "#,
                params![stale_response_before_rfc3339, batch_size.max(1)],
            )
            .context("idempotency response cleanup failed")?;
        if deleted_rows < batch_size_usize {
            return Ok(true);
        }
    }
    let has_stale_rows = conn
        .query_row(
            r#"
            SELECT 1
            FROM executor_submit_idempotency
            WHERE updated_at_utc <= ?1
            LIMIT 1
            "#,
            params![stale_response_before_rfc3339],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .context("idempotency response cleanup stale-row probe failed")?
        .is_some();
    Ok(!has_stale_rows)
}

fn should_run_claim_cleanup(
    now_unix: i64,
    last_cleanup_unix: i64,
    cleanup_interval_sec: i64,
) -> bool {
    if cleanup_interval_sec <= 0 {
        return true;
    }
    if last_cleanup_unix <= 0 {
        return true;
    }
    now_unix.saturating_sub(last_cleanup_unix) >= cleanup_interval_sec
}

fn load_global_claim_cleanup_last_unix(conn: &Connection) -> Result<i64> {
    let stored: Option<i64> = conn
        .query_row(
            "SELECT value_int FROM executor_runtime_meta WHERE meta_key = ?1",
            params![META_KEY_CLAIM_CLEANUP_LAST_UNIX],
            |row| row.get(0),
        )
        .optional()
        .context("idempotency cleanup metadata lookup failed")?;
    Ok(stored.unwrap_or(0))
}

fn store_global_claim_cleanup_last_unix(conn: &Connection, now_unix: i64) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO executor_runtime_meta (
            meta_key,
            value_int,
            updated_at_unix
        ) VALUES (?1, ?2, ?2)
        ON CONFLICT(meta_key) DO UPDATE SET
            value_int = excluded.value_int,
            updated_at_unix = excluded.updated_at_unix
        "#,
        params![META_KEY_CLAIM_CLEANUP_LAST_UNIX, now_unix],
    )
    .context("idempotency cleanup metadata update failed")?;
    Ok(())
}

fn load_global_response_cleanup_last_unix(conn: &Connection) -> Result<i64> {
    let stored: Option<i64> = conn
        .query_row(
            "SELECT value_int FROM executor_runtime_meta WHERE meta_key = ?1",
            params![META_KEY_RESPONSE_CLEANUP_LAST_UNIX],
            |row| row.get(0),
        )
        .optional()
        .context("idempotency response cleanup metadata lookup failed")?;
    Ok(stored.unwrap_or(0))
}

fn store_global_response_cleanup_last_unix(conn: &Connection, now_unix: i64) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO executor_runtime_meta (
            meta_key,
            value_int,
            updated_at_unix
        ) VALUES (?1, ?2, ?2)
        ON CONFLICT(meta_key) DO UPDATE SET
            value_int = excluded.value_int,
            updated_at_unix = excluded.updated_at_unix
        "#,
        params![META_KEY_RESPONSE_CLEANUP_LAST_UNIX, now_unix],
    )
    .context("idempotency response cleanup metadata update failed")?;
    Ok(())
}

fn ensure_parent_dir(path: &str) -> Result<()> {
    let target = Path::new(path);
    let Some(parent) = target.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "create idempotency parent dir failed path={}",
            parent.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        claim_cleanup_interval_sec, response_cleanup_interval_sec, should_run_claim_cleanup,
        DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE, DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
        META_KEY_RESPONSE_CLEANUP_LAST_UNIX, SubmitClaimOutcome, SubmitIdempotencyStore,
    };
    use chrono::{Duration, Utc};
    use rusqlite::{params, OptionalExtension};
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

    fn claim_row_count_for_key(store: &SubmitIdempotencyStore, client_order_id: &str) -> i64 {
        let conn = store.conn.lock().expect("lock conn");
        conn.query_row(
            "SELECT COUNT(1) FROM executor_submit_idempotency_claims WHERE client_order_id = ?1",
            params![client_order_id],
            |row| row.get(0),
        )
        .expect("query claim row count")
    }

    fn response_row_count(store: &SubmitIdempotencyStore) -> i64 {
        let conn = store.conn.lock().expect("lock conn");
        conn.query_row("SELECT COUNT(1) FROM executor_submit_idempotency", [], |row| {
            row.get(0)
        })
        .expect("query response row count")
    }

    fn response_cleanup_marker_value(store: &SubmitIdempotencyStore) -> Option<i64> {
        let conn = store.conn.lock().expect("lock conn");
        conn.query_row(
            "SELECT value_int FROM executor_runtime_meta WHERE meta_key = ?1",
            params![META_KEY_RESPONSE_CLEANUP_LAST_UNIX],
            |row| row.get(0),
        )
        .optional()
        .expect("query response cleanup marker")
    }

    #[test]
    fn store_and_load_submit_response_round_trip() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
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
    fn schema_includes_updated_at_index_for_response_cleanup() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let conn = store.conn.lock().expect("lock conn");
        let index_exists: i64 = conn
            .query_row(
                "SELECT COUNT(1) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params!["idx_executor_submit_idempotency_updated_at"],
                |row| row.get(0),
            )
            .expect("query index metadata");
        assert_eq!(index_exists, 1);
        drop(conn);
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn store_does_not_overwrite_existing_response() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
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

    #[test]
    fn claim_flow_returns_claimed_inflight_then_cached() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");

        let first = store
            .load_cached_or_claim_submit("order-claim-1", "req-1", 30)
            .expect("first claim");
        assert!(matches!(first, SubmitClaimOutcome::Claimed));

        let second = store
            .load_cached_or_claim_submit("order-claim-1", "req-2", 30)
            .expect("second claim");
        assert!(matches!(second, SubmitClaimOutcome::InFlight));

        let response = json!({
            "status": "ok",
            "client_order_id": "order-claim-1",
            "tx_signature": "sig-claim-1"
        });
        store
            .store_submit_response("order-claim-1", "req-1", &response)
            .expect("store response");
        store
            .release_submit_claim("order-claim-1", "req-1")
            .expect("release claim");

        let third = store
            .load_cached_or_claim_submit("order-claim-1", "req-3", 30)
            .expect("third claim");
        match third {
            SubmitClaimOutcome::Cached(value) => assert_eq!(value, response),
            _ => panic!("expected cached outcome"),
        }

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn idempotency_normalizes_client_order_id_for_store_and_lookup() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let response = json!({
            "status": "ok",
            "client_order_id": "order-ws-1",
            "tx_signature": "sig-ws-1"
        });
        store
            .store_submit_response("  order-ws-1  ", "  req-ws-1  ", &response)
            .expect("store response");
        let loaded = store
            .load_submit_response("order-ws-1")
            .expect("load normalized response")
            .expect("cached response");
        assert_eq!(loaded, response);
        let outcome = store
            .load_cached_or_claim_submit("  order-ws-1 ", "req-ws-2", 30)
            .expect("cached outcome");
        assert!(matches!(outcome, SubmitClaimOutcome::Cached(_)));
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn release_claim_requires_request_id_owner_match() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");

        let first = store
            .load_cached_or_claim_submit("order-claim-owner-1", "req-owner-1", 30)
            .expect("first claim");
        assert!(matches!(first, SubmitClaimOutcome::Claimed));

        let removed_wrong = store
            .release_submit_claim("order-claim-owner-1", "req-owner-2")
            .expect("wrong owner release");
        assert!(!removed_wrong);

        let second = store
            .load_cached_or_claim_submit("order-claim-owner-1", "req-owner-3", 30)
            .expect("second claim");
        assert!(matches!(second, SubmitClaimOutcome::InFlight));

        let removed_right = store
            .release_submit_claim("order-claim-owner-1", "req-owner-1")
            .expect("right owner release");
        assert!(removed_right);

        let third = store
            .load_cached_or_claim_submit("order-claim-owner-1", "req-owner-3", 30)
            .expect("third claim");
        assert!(matches!(third, SubmitClaimOutcome::Claimed));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn claim_cleanup_interval_sec_clamps_bounds() {
        assert_eq!(claim_cleanup_interval_sec(1), 5);
        assert_eq!(claim_cleanup_interval_sec(10), 5);
        assert_eq!(claim_cleanup_interval_sec(30), 15);
        assert_eq!(claim_cleanup_interval_sec(240), 60);
    }

    #[test]
    fn response_cleanup_interval_sec_clamps_bounds() {
        assert_eq!(response_cleanup_interval_sec(1), 60);
        assert_eq!(response_cleanup_interval_sec(120), 60);
        assert_eq!(response_cleanup_interval_sec(600), 300);
        assert_eq!(response_cleanup_interval_sec(20_000), 3_600);
    }

    #[test]
    fn should_run_claim_cleanup_respects_interval() {
        assert!(should_run_claim_cleanup(100, 0, 10));
        assert!(!should_run_claim_cleanup(100, 95, 10));
        assert!(should_run_claim_cleanup(100, 90, 10));
        assert!(should_run_claim_cleanup(100, 99, 0));
    }

    #[test]
    fn stale_claim_is_reclaimed_for_same_key_when_global_cleanup_throttled() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let now_unix = Utc::now().timestamp();

        {
            let conn = store.conn.lock().expect("lock conn");
            conn.execute(
                r#"
                INSERT INTO executor_submit_idempotency_claims (
                    client_order_id,
                    request_id,
                    claimed_at_unix
                ) VALUES (?1, ?2, ?3)
                "#,
                params!["order-stale-key-1", "req-old", now_unix.saturating_sub(300)],
            )
            .expect("insert stale claim");
        }

        // Force global cleanup throttle window so this call relies on per-key stale reclaim.
        store
            .last_claim_cleanup_unix
            .store(now_unix, Ordering::Relaxed);

        let outcome = store
            .load_cached_or_claim_submit("order-stale-key-1", "req-new", 30)
            .expect("claim outcome");
        assert!(matches!(outcome, SubmitClaimOutcome::Claimed));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn global_cleanup_marker_prevents_redundant_cleanup_across_store_instances() {
        let db_path = temp_db_path();
        let db_path_str = db_path.to_string_lossy().to_string();
        let store1 = SubmitIdempotencyStore::open(db_path_str.as_str()).expect("open store1");
        let now_unix = Utc::now().timestamp();

        {
            let conn = store1.conn.lock().expect("lock conn");
            conn.execute(
                r#"
                INSERT INTO executor_submit_idempotency_claims (
                    client_order_id,
                    request_id,
                    claimed_at_unix
                ) VALUES (?1, ?2, ?3)
                "#,
                params!["order-stale-marker-1", "req-old-1", now_unix.saturating_sub(300)],
            )
            .expect("insert stale claim 1");
        }
        assert_eq!(claim_row_count_for_key(&store1, "order-stale-marker-1"), 1);

        let first_outcome = store1
            .load_cached_or_claim_submit("probe-cleanup-marker-1", "req-probe-1", 30)
            .expect("first probe claim");
        assert!(matches!(first_outcome, SubmitClaimOutcome::Claimed));
        assert_eq!(claim_row_count_for_key(&store1, "order-stale-marker-1"), 0);

        {
            let conn = store1.conn.lock().expect("lock conn");
            conn.execute(
                r#"
                INSERT INTO executor_submit_idempotency_claims (
                    client_order_id,
                    request_id,
                    claimed_at_unix
                ) VALUES (?1, ?2, ?3)
                "#,
                params!["order-stale-marker-2", "req-old-2", now_unix.saturating_sub(300)],
            )
            .expect("insert stale claim 2");
        }
        assert_eq!(claim_row_count_for_key(&store1, "order-stale-marker-2"), 1);

        let store2 = SubmitIdempotencyStore::open(db_path_str.as_str()).expect("open store2");
        let second_outcome = store2
            .load_cached_or_claim_submit("probe-cleanup-marker-2", "req-probe-2", 30)
            .expect("second probe claim");
        assert!(matches!(second_outcome, SubmitClaimOutcome::Claimed));
        assert_eq!(
            claim_row_count_for_key(&store2, "order-stale-marker-2"),
            1,
            "global cleanup marker should suppress redundant cross-process cleanup"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn stale_cached_response_is_cleaned_when_response_cleanup_due() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let stale_updated = (Utc::now() - Duration::hours(2)).to_rfc3339();
        {
            let conn = store.conn.lock().expect("lock conn");
            conn.execute(
                r#"
                INSERT INTO executor_submit_idempotency (
                    client_order_id,
                    request_id,
                    response_json,
                    created_at_utc,
                    updated_at_utc
                ) VALUES (?1, ?2, ?3, ?4, ?4)
                "#,
                params![
                    "order-stale-response-1",
                    "req-stale-response-1",
                    "{\"status\":\"ok\"}",
                    stale_updated
                ],
            )
            .expect("insert stale cached response");
        }
        store
            .run_response_cleanup_if_due(
                60,
                DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
                DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
            )
            .expect("run response cleanup");
        let stale = store
            .load_submit_response("order-stale-response-1")
            .expect("lookup stale response");
        assert!(stale.is_none(), "stale cached response should be cleaned");
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn stale_cached_response_is_not_cleaned_on_submit_claim_path() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let stale_updated = (Utc::now() - Duration::hours(2)).to_rfc3339();
        {
            let conn = store.conn.lock().expect("lock conn");
            conn.execute(
                r#"
                INSERT INTO executor_submit_idempotency (
                    client_order_id,
                    request_id,
                    response_json,
                    created_at_utc,
                    updated_at_utc
                ) VALUES (?1, ?2, ?3, ?4, ?4)
                "#,
                params![
                    "order-stale-response-2",
                    "req-stale-response-2",
                    "{\"status\":\"ok\"}",
                    stale_updated
                ],
            )
            .expect("insert stale cached response");
        }
        let outcome = store
            .load_cached_or_claim_submit("probe-no-response-cleanup-1", "req-probe-1", 30)
            .expect("claim outcome");
        assert!(matches!(outcome, SubmitClaimOutcome::Claimed));
        let stale = store
            .load_submit_response("order-stale-response-2")
            .expect("lookup stale response");
        assert!(
            stale.is_some(),
            "submit claim path must not perform response retention cleanup"
        );
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn response_cleanup_is_bounded_per_run() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let stale_updated = (Utc::now() - Duration::hours(3)).to_rfc3339();
        let cleanup_batch_size =
            i64::try_from(DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE).expect("batch size");
        let cleanup_max_batches =
            i64::try_from(DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN).expect("max batches");
        let total_rows_to_insert = cleanup_batch_size * cleanup_max_batches + 3;
        {
            let conn = store.conn.lock().expect("lock conn");
            for index in 0..total_rows_to_insert {
                conn.execute(
                    r#"
                    INSERT INTO executor_submit_idempotency (
                        client_order_id,
                        request_id,
                        response_json,
                        created_at_utc,
                        updated_at_utc
                    ) VALUES (?1, ?2, ?3, ?4, ?4)
                    "#,
                    params![
                        format!("order-stale-bounded-{}", index),
                        format!("req-stale-bounded-{}", index),
                        "{\"status\":\"ok\"}",
                        stale_updated
                    ],
                )
                .expect("insert stale cached response row");
            }
        }
        assert_eq!(response_row_count(&store), total_rows_to_insert);

        store
            .run_response_cleanup_if_due(
                60,
                DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
                DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
            )
            .expect("run bounded response cleanup");

        let expected_remaining = total_rows_to_insert - (cleanup_batch_size * cleanup_max_batches);
        assert_eq!(response_row_count(&store), expected_remaining);

        // Marker must not advance when cleanup hit per-run batch cap, so a subsequent run can
        // continue draining backlog immediately.
        store
            .run_response_cleanup_if_due(
                60,
                DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
                DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
            )
            .expect("run bounded response cleanup second pass");
        assert_eq!(response_row_count(&store), 0);
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn response_cleanup_nonblocking_skips_when_mutex_is_busy() {
        let store = SubmitIdempotencyStore::open(":memory:").expect("open in-memory store");
        let _held_lock = store.conn.lock().expect("hold idempotency lock");
        let ran_cleanup = store
            .run_response_cleanup_if_due_nonblocking(
                60,
                DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
                DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
            )
            .expect("nonblocking cleanup call should not fail");
        assert!(
            !ran_cleanup,
            "nonblocking cleanup should report skipped when mutex is busy"
        );
    }

    #[test]
    fn response_cleanup_exact_batch_limit_advances_marker_when_fully_drained() {
        let db_path = temp_db_path();
        let store =
            SubmitIdempotencyStore::open(db_path.to_string_lossy().as_ref()).expect("open store");
        let stale_updated = (Utc::now() - Duration::hours(3)).to_rfc3339();
        let cleanup_batch_size =
            i64::try_from(DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE).expect("batch size");
        let cleanup_max_batches =
            i64::try_from(DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN).expect("max batches");
        let total_rows_to_insert = cleanup_batch_size * cleanup_max_batches;
        {
            let conn = store.conn.lock().expect("lock conn");
            for index in 0..total_rows_to_insert {
                conn.execute(
                    r#"
                    INSERT INTO executor_submit_idempotency (
                        client_order_id,
                        request_id,
                        response_json,
                        created_at_utc,
                        updated_at_utc
                    ) VALUES (?1, ?2, ?3, ?4, ?4)
                    "#,
                    params![
                        format!("order-stale-exact-{}", index),
                        format!("req-stale-exact-{}", index),
                        "{\"status\":\"ok\"}",
                        stale_updated
                    ],
                )
                .expect("insert stale cached response row");
            }
        }
        assert_eq!(response_row_count(&store), total_rows_to_insert);
        assert!(
            response_cleanup_marker_value(&store).is_none(),
            "marker should be absent before first cleanup"
        );

        store
            .run_response_cleanup_if_due(
                60,
                DEFAULT_RESPONSE_CLEANUP_DELETE_BATCH_SIZE,
                DEFAULT_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN,
            )
            .expect("run response cleanup");

        assert_eq!(response_row_count(&store), 0);
        assert!(
            response_cleanup_marker_value(&store).unwrap_or(0) > 0,
            "marker should advance when exact-limit run fully drains stale rows"
        );
        let _ = std::fs::remove_file(db_path);
    }
}
