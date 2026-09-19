//! Explicit capacity ceilings and small-fixture exhaustion, without provider I/O.
use copybot_storage_core::capture_scope::{CaptureStore, SCHEMA};
use rusqlite::{params, Connection};

#[test]
fn configured_ceiling_accepts_exact_limits_and_rejects_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capture.db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute("INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1,1)", []).unwrap();
    for (rows, bytes, accepted) in [
        (4_000_000_i64, 68_719_476_736_i64, true),
        (2_000_000, 51_539_607_552, true),
        (1_000_000, 1_073_741_824, true),
        (4_000_001, 1, false), (1, 68_719_476_737, false),
        (0, 1, false), (1, 0, false), (-1, 1, false),
    ] {
        db.execute("UPDATE capture_meta SET max_rows=?,max_bytes=?", params![rows,bytes]).unwrap();
        assert_eq!(CaptureStore::open(&path).is_ok(), accepted, "{rows}/{bytes}");
    }
}

#[test]
fn bytes_exhaustion_keeps_prior_receipt_pin_and_failure_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capture.db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute("INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,10,16385)", []).unwrap();
    db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('scope','{}',1000)", []).unwrap();
    db.execute("INSERT INTO capture_members(request_id,wallet) VALUES(1,'alice')", []).unwrap();
    let mut store = CaptureStore::open(&path).unwrap();
    store.start().unwrap();
    store.accept_pending(10.).unwrap();
    let first = store.receive(Some("first"), "alice", 1, b"x", "first", 11., Some(11.)).unwrap().unwrap();
    store.finish(first.seq, None, "fixture_rejection").unwrap();
    db.execute("INSERT INTO capture_obligations(event_seq,wallet,mint) VALUES(?,'alice','mint')", [first.seq]).unwrap();
    assert!(store.receive(Some("overflow"), "alice", 2, b"x", "overflow", 12., Some(12.)).is_err());
    let state: (String, String, i64, i64) = db.query_row(
        "SELECT status,reason,gap,used_bytes FROM capture_meta", [],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(state, ("failed".into(), "capture_capacity".into(), 2, 16385));
    drop(store);
    let mut restored = CaptureStore::open(&path).unwrap();
    restored.start().unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM capture_events", [], |r| r.get::<_,i64>(0)).unwrap(), 1);
    assert_eq!(db.query_row("SELECT count(*) FROM capture_obligations WHERE state!='SETTLED'", [], |r| r.get::<_,i64>(0)).unwrap(), 1);
    assert!(restored.receive(Some("retry"), "alice", 2, b"x", "retry", 13., Some(13.)).is_err());
}
