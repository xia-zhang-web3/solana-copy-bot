//! Independent reviewer probes. Synthetic heads test admission fences, not chain completeness.
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use copybot_core_types::association_recovery::{BlockCheckpoint, DurableCheckpoint, ReplayScope};
use copybot_storage_core::capture_scope::{CaptureStore, SCHEMA};
use rusqlite::{params, Connection};

fn head(wallets: Vec<String>) -> DurableCheckpoint {
    DurableCheckpoint {
        session: "review-synthetic-head".into(),
        sequence: 1,
        from_slot: 6,
        overlap: vec![],
        block: BlockCheckpoint {
            scope: ReplayScope {
                policy: "protected_complete_recovery_v1".into(),
                wallets,
                programs: vec!["program".into()],
                raydium_programs: vec![],
                pumpswap_programs: vec![],
            },
            observation: ParentObservation {
                child: BlockKey {
                    slot: 7,
                    hash: "11111111111111111111111111111111".into(),
                },
                parent: BlockKey {
                    slot: 6,
                    hash: "11111111111111111111111111111111".into(),
                },
                issue: None,
            },
            executed_transaction_count: 0,
            supplied_transaction_count: 0,
            claims: vec![],
        },
    }
}
fn request(db: &Connection, key: &str, wallet: &str) -> i64 {
    db.execute(
        "INSERT INTO capture_requests(request_key,payload,expires) VALUES(?,'{}',99999)",
        [key],
    )
    .unwrap();
    let id = db.last_insert_rowid();
    db.execute(
        "INSERT INTO capture_members VALUES(?,?)",
        params![id, wallet],
    )
    .unwrap();
    id
}
fn ack_current(db: &Connection, id: i64) -> bool {
    // The actual forward adapter _valid_ack predicate, evaluated on the same rows.
    db.query_row(
        "SELECT r.state='ACKED' AND r.epoch=m.epoch AND r.gap=m.gap
        AND m.status='running' AND r.ack_seq IS NOT NULL AND r.ack_at IS NOT NULL
        FROM capture_requests r JOIN capture_meta m ON m.id=1 WHERE r.id=?",
        [id],
        |r| r.get(0),
    )
    .unwrap()
}
#[test]
fn reviewer_gap_and_prepare_revoke_old_ack_before_new_wallet_baseline() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("capture.db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1000,10000000)",
        [],
    )
    .unwrap();
    let a = request(&db, "initial", "alice");
    let mut store = CaptureStore::open(&path).unwrap();
    store.start().unwrap();
    let first = store.prepare_scope().unwrap();
    let old = head(first.wallets.clone());
    store.commit_scope(&first, &old, 1.).unwrap();
    assert!(ack_current(&db, a));
    store.gap("review_transport_gap", false).unwrap();
    assert!(
        !ack_current(&db, a),
        "gap must close actual adapter admission"
    );
    assert!(
        store.needs_transition().unwrap(),
        "gap requires a new durable scope fence"
    );
    let same = store.prepare_scope().unwrap();
    store
        .commit_scope(&same, &head(same.wallets.clone()), 2.)
        .unwrap();
    assert!(ack_current(&db, a));
    let b = request(&db, "next", "bob");
    let next = store.prepare_scope().unwrap();
    assert!(
        !ack_current(&db, a),
        "prepare must atomically revoke old admission before union snapshot"
    );
    assert!(!ack_current(&db, b));
    assert!(next.baseline.is_none());
    assert_eq!(next.wallets, vec!["bob"]);
    assert!(
        store.commit_scope(&next, &old, 3.).is_err(),
        "old cursor cannot prove new wallets"
    );
    assert!(!ack_current(&db, b));
    store
        .commit_scope(&next, &head(next.wallets.clone()), 3.)
        .unwrap();
    assert!(ack_current(&db, b));
    assert!(!ack_current(&db, a));
}
#[test]
fn reviewer_pin_committed_before_prepare_is_included_and_cannot_be_retired() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("capture.db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1000,10000000)",
        [],
    )
    .unwrap();
    request(&db, "initial", "alice");
    let mut store = CaptureStore::open(&path).unwrap();
    store.start().unwrap();
    let first = store.prepare_scope().unwrap();
    store
        .commit_scope(&first, &head(first.wallets.clone()), 1.)
        .unwrap();
    db.execute("INSERT INTO capture_events(seq,signature,wallet,slot,epoch,received_at,raw,fingerprint,stage)
        VALUES(1,'old-buy','alice','7',?,1,X'00','fixture','DURABLE')",[store.consumer_epoch()]).unwrap();
    db.execute(
        "INSERT INTO capture_obligations(event_seq,wallet,mint) VALUES(1,'alice','mint')",
        [],
    )
    .unwrap();
    request(&db, "demotion", "bob");
    let next = store.prepare_scope().unwrap();
    assert_eq!(next.wallets, vec!["alice", "bob"]);
    assert!(store
        .commit_scope(&next, &head(vec!["bob".into()]), 2.)
        .is_err());
    let kept: bool = db
        .query_row(
            "SELECT state!='SETTLED' FROM capture_obligations WHERE event_seq=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(kept);
    assert!(next.baseline.is_none());
}

fn consumer_probe(
    operation: &str,
    capture: &std::path::Path,
    ledger: &std::path::Path,
    id: i64,
) -> serde_json::Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let result = std::process::Command::new(
        std::env::var("CAPTURE_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .arg(root.join("tools/tests/capture_scope_review_fixture.py"))
    .arg(operation)
    .arg(capture)
    .arg(ledger)
    .arg(id.to_string())
    .output()
    .unwrap();
    assert!(
        result.status.success(),
        "Python consumer probe failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

#[test]
fn reviewer_actual_consumer_queued_buy_after_rust_prepare_has_no_pin_lot_or_quote() {
    for fenced in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let capture = temp.path().join("capture.db");
        let ledger = temp.path().join("virtual.db");
        let db = Connection::open(&capture).unwrap();
        db.execute_batch(SCHEMA).unwrap();
        db.execute(
            "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1000,10000000)",
            [],
        )
        .unwrap();
        let saved: serde_json::Value = serde_json::from_str(include_str!(
            "../../app/tests/fixtures/capture/saved_sell_446979602.json"
        ))
        .unwrap();
        let wallet = saved["swap"]["wallet"].as_str().unwrap();
        let decision = serde_json::json!({"decision_id":"review-live","available_at":2000000000.,
            "valid_until":2000000120.,"wallets":[wallet],"admissible":true});
        db.execute("INSERT INTO capture_requests(request_key,payload,expires) VALUES('review-live',?,2000000120.)",[decision.to_string()]).unwrap();
        let id = db.last_insert_rowid();
        db.execute(
            "INSERT INTO capture_members VALUES(?,?)",
            params![id, wallet],
        )
        .unwrap();
        let mut store = CaptureStore::open(&capture).unwrap();
        store.start().unwrap();
        let initial = store.prepare_scope().unwrap();
        store
            .commit_scope(&initial, &head(initial.wallets.clone()), 2000000000.)
            .unwrap();
        let setup = consumer_probe("setup", &capture, &ledger, id);
        assert_eq!(setup["status"], "QUEUED");
        if fenced {
            request(&db, "review-demotion", "bob");
            let next = store.prepare_scope().unwrap();
            assert!(!next.wallets.iter().any(|w| w == wallet));
            assert!(!ack_current(&db, id));
        }
        let result = consumer_probe("observe", &capture, &ledger, id);
        println!("review actual-consumer fenced={fenced}: {result}");
        if fenced {
            assert_eq!(result["status"], "SKIPPED");
            assert_eq!(result["reason"], "capture_ack_not_current");
            assert_eq!(
                result["ledger_counts"],
                serde_json::json!({"events":0,"lots":0,"jobs":0})
            );
            assert_eq!(result["unsettled_pins"], 0);
        } else {
            assert_eq!(result["status"], "DELIVERED");
            assert_eq!(
                result["ledger_counts"],
                serde_json::json!({"events":1,"lots":1,"jobs":2})
            );
            assert_eq!(result["unsettled_pins"], 1);
        }
        assert_eq!(result["network_calls"], 0);
    }
}
