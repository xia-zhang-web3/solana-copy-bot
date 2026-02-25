use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex,
    },
};

const MIN_CLAIM_CLEANUP_INTERVAL_SEC: i64 = 5;
const MAX_CLAIM_CLEANUP_INTERVAL_SEC: i64 = 60;

pub(crate) struct SubmitIdempotencyStore {
    conn: Mutex<Connection>,
    last_claim_cleanup_unix: AtomicI64,
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
            CREATE TABLE IF NOT EXISTS executor_submit_idempotency_claims (
                client_order_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                claimed_at_unix INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_executor_submit_idempotency_claims_claimed_at
                ON executor_submit_idempotency_claims(claimed_at_unix);
            "#,
        )
        .context("initialize idempotency schema failed")?;
        Ok(Self {
            conn: Mutex::new(conn),
            last_claim_cleanup_unix: AtomicI64::new(0),
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
            conn.execute(
                "DELETE FROM executor_submit_idempotency_claims WHERE claimed_at_unix <= ?1",
                params![stale_before],
            )
            .context("idempotency stale claim cleanup failed")?;
            self.last_claim_cleanup_unix
                .store(now_unix, Ordering::Relaxed);
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
        claim_cleanup_interval_sec, should_run_claim_cleanup, SubmitClaimOutcome,
        SubmitIdempotencyStore,
    };
    use chrono::Utc;
    use rusqlite::params;
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
}
