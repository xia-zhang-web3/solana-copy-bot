//! Independent boundary checks: temporary capture DBs only, no runtime or ledger.
use chrono::{TimeZone, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::capture_scope::{CaptureStore, SCHEMA};
use rusqlite::{params, Connection};
use tempfile::TempDir;

struct Fixture {
    _dir: TempDir,
    db: Connection,
    store: CaptureStore,
    path: std::path::PathBuf,
}

impl Fixture {
    fn new(rows: i64, bytes: i64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA).unwrap();
        db.execute("INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,?,?)", params![rows, bytes]).unwrap();
        let mut store = CaptureStore::open(&path).unwrap();
        store.start().unwrap();
        Self { _dir: dir, db, store, path }
    }
    fn request(&self, key: &str, wallet: &str) -> i64 {
        self.db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES(?,'{}',1000)", [key]).unwrap();
        let id = self.db.last_insert_rowid();
        self.db.execute("INSERT INTO capture_members(request_id,wallet) VALUES(?,?)", params![id,wallet]).unwrap();
        id
    }
    fn event(&mut self, signature: &str, wallet: &str) -> i64 {
        let seq = self.store.receive(Some(signature), wallet, 7, signature.as_bytes(), signature, 11., Some(10.)).unwrap().unwrap().seq;
        self.store.finish(seq, Some(&swap(signature, wallet)), "decoded").unwrap();
        seq
    }
}

fn swap(signature: &str, wallet: &str) -> SwapEvent {
    SwapEvent {
        wallet: wallet.into(), dex: "fixture".into(), token_in: "SOL".into(), token_out: "mint".into(),
        amount_in: 0.01, amount_out: 100., signature: signature.into(), slot: 7,
        ts_utc: Utc.timestamp_opt(10, 123_456_789).single().unwrap(), exact_amounts: None,
    }
}

#[test]
fn pending_publication_is_not_protection_and_ack_has_causal_boundary() {
    let mut f = Fixture::new(100, 1_000_000);
    let id = f.request("first", "alice");
    assert!(f.store.receive(Some("before"), "alice", 7, b"before", "before", 9., Some(8.)).unwrap().is_none());
    f.store.accept_pending(10.).unwrap();
    let ack: (String, i64, f64, i64, i64) = f.db.query_row(
        "SELECT state,ack_seq,ack_at,epoch,gap FROM capture_requests WHERE id=?", [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))).unwrap();
    assert_eq!(ack, ("ACKED".into(), 0, 10., 1, 1));
    let seq = f.event("after", "alice");
    assert!(seq > ack.1);
    assert_eq!(f.db.query_row("SELECT request_id FROM capture_events WHERE seq=?", [seq], |r| r.get::<_,i64>(0)).unwrap(), id);
}

#[test]
fn restart_fences_old_writer_and_requires_pending_restore_before_new_ack() {
    let mut f = Fixture::new(100, 1_000_000);
    f.request("old", "alice");
    f.store.accept_pending(10.).unwrap();
    let receipt = f.store.receive(Some("pending"), "alice", 7, b"exact bytes", "pending", 11., Some(10.)).unwrap().unwrap();
    let next = f.request("next", "bob");
    let mut restarted = CaptureStore::open(&f.path).unwrap();
    restarted.start().unwrap();
    assert!(f.store.finish(receipt.seq, None, "old consumer").is_err());
    assert!(f.store.receive(Some("fenced"), "alice", 7, b"x", "fenced", 12., None).is_err());
    assert!(restarted.accept_pending(12.).is_err());
    assert_eq!(restarted.pending().unwrap(), vec![(receipt.seq, b"exact bytes".to_vec(), Some(10.))]);
    restarted.finish(receipt.seq, Some(&swap("pending", "alice")), "restored").unwrap();
    restarted.accept_pending(13.).unwrap();
    let ack: (i64, i64, i64) = f.db.query_row("SELECT epoch,gap,ack_seq FROM capture_requests WHERE id=?", [next],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(ack, (2, 2, receipt.seq));
}

#[test]
fn demoted_wallet_retains_capture_only_while_obligation_unsettled() {
    let mut f = Fixture::new(100, 1_000_000);
    f.request("old", "alice");
    f.store.accept_pending(10.).unwrap();
    let buy = f.event("buy", "alice");
    f.db.execute("INSERT INTO capture_obligations(event_seq,wallet,mint) VALUES(?,'alice','mint')", [buy]).unwrap();
    f.request("new", "bob");
    f.store.accept_pending(12.).unwrap();
    assert!(f.event("sell while pinned", "alice") > buy);
    f.db.execute("UPDATE capture_obligations SET state='SETTLED' WHERE event_seq=?", [buy]).unwrap();
    assert!(f.store.receive(Some("after settlement"), "alice", 7, b"x", "settled", 13., None).unwrap().is_none());
}

#[test]
fn capacity_failure_is_durable_and_cannot_be_acked_as_coverage() {
    let mut f = Fixture::new(1, 1_000_000);
    f.request("old", "alice");
    f.store.accept_pending(10.).unwrap();
    f.event("first", "alice");
    f.request("next", "bob");
    assert!(f.store.receive(Some("overflow"), "alice", 7, b"overflow", "overflow", 12., None).is_err());
    assert!(f.store.accept_pending(13.).is_err());
    let state: (String, String, i64, i64) = f.db.query_row(
        "SELECT status,reason,gap,(SELECT count(*) FROM capture_events) FROM capture_meta", [],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(state, ("failed".into(), "capture_capacity".into(), 2, 1));
    assert_eq!(f.db.query_row("SELECT state FROM capture_requests WHERE request_key='next'", [], |r| r.get::<_,String>(0)).unwrap(), "PENDING");
}

#[test]
fn duplicate_is_immutable_and_decoded_identity_cannot_change() {
    let mut f = Fixture::new(100, 1_000_000);
    f.request("scope", "alice");
    f.store.accept_pending(10.).unwrap();
    let first = f.event("same", "alice");
    let duplicate = f.store.receive(Some("same"), "alice", 7, b"same", "same", 999., None).unwrap().unwrap();
    assert_eq!(duplicate.seq, first);
    assert_eq!(duplicate.stage, "DURABLE");
    assert!(f.store.receive(Some("same"), "alice", 7, b"different", "different", 999., None).is_err());
    let rewrapped = f.store.receive(Some("same"), "alice", 7, b"new delivery envelope", "same", 999., Some(998.)).unwrap().unwrap();
    assert_eq!(rewrapped.seq, first);
    assert_eq!(f.db.query_row("SELECT raw FROM capture_events WHERE seq=?", [first], |r| r.get::<_,Vec<u8>>(0)).unwrap(), b"same");
    let mut wrong = swap("same", "alice");
    wrong.wallet = "bob".into();
    assert!(f.store.finish(first, Some(&wrong), "wrong").is_err());
    assert_eq!(f.db.query_row("SELECT received_at FROM capture_events WHERE seq=?", [first], |r| r.get::<_,f64>(0)).unwrap(), 11.);
}
