use super::temporary_output_fixture::OutputRoot;
use crate::protected_capture_consumer::source_quiesced;
use anyhow::Result;
use copybot_core_types::association_recovery::DurableCheckpoint;
use copybot_storage_core::capture_scope::{CaptureStore, SCHEMA};
use rusqlite::Connection;

fn current_ack(db: &Connection) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM capture_requests r,capture_meta m
         WHERE r.state='ACKED' AND r.epoch=m.epoch AND r.gap=m.gap AND m.status='running')",
        [],
        |r| r.get(0),
    )?)
}

#[test]
fn r1_quiesce_error_withdraws_known_ack_and_preserves_primary_error() -> Result<()> {
    let dir = OutputRoot::new("r1-quiesce-known")?;
    let path = dir.path().join("capture.db");
    let db = Connection::open(&path)?;
    db.execute_batch(SCHEMA)?;
    db.execute_batch(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1000,10000000);
         INSERT INTO capture_requests(request_key,payload,expires) VALUES('local','{}',99999);
         INSERT INTO capture_members VALUES(1,'alice');",
    )?;
    let mut store = CaptureStore::open(&path)?;
    store.start()?;
    let scope = store.prepare_scope()?;
    let head: DurableCheckpoint = serde_json::from_value(serde_json::json!({
        "session":"local-quiesce-test","sequence":1,"from_slot":7,"overlap":[],
        "block":{
            "scope":{
                "policy":"durable_checkpoint_replay_v1","wallets":scope.wallets,
                "programs":["program"],"raydium_programs":[],"pumpswap_programs":[]
            },
            "observation":{
                "child":{"slot":7,"hash":"11111111111111111111111111111111"},
                "parent":{"slot":6,"hash":"11111111111111111111111111111111"},
                "issue":null
            },
            "executed_transaction_count":0,"supplied_transaction_count":0,"claims":[]
        }
    }))?;
    store.commit_scope(&scope, &head, 1.)?;
    assert!(current_ack(&db)?);
    source_quiesced(&mut store, Ok(()))?;
    assert!(
        current_ack(&db)?,
        "successful quiesce does not withdraw authority"
    );
    let error =
        source_quiesced(&mut store, Err(anyhow::anyhow!("original producer error"))).unwrap_err();
    assert_eq!(error.to_string(), "original producer error");
    assert!(!current_ack(&db)?);
    assert_eq!(
        db.query_row("SELECT status,reason FROM capture_meta", [], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?,
        ("failed".into(), "protected_source_quiesce_failed".into())
    );
    Ok(())
}

#[test]
fn r1_quiesce_error_does_not_write_through_unknown_store() -> Result<()> {
    let dir = OutputRoot::new("r1-quiesce-unknown")?;
    let path = dir.path().join("capture.db");
    let db = Connection::open(&path)?;
    db.execute_batch(SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,1000,10000000)",
        [],
    )?;
    let mut store = CaptureStore::open(&path)?;
    store.start()?;
    db.execute_batch(
        "CREATE TRIGGER corrupt_gap AFTER UPDATE OF gap ON capture_meta
         BEGIN UPDATE capture_meta SET reason='unexpected' WHERE id=NEW.id; END;",
    )?;
    assert!(store
        .gap("transport", false)
        .unwrap_err()
        .to_string()
        .contains("readback mismatch"));
    let before = db.query_row("SELECT gap,status,reason FROM capture_meta", [], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    let error =
        source_quiesced(&mut store, Err(anyhow::anyhow!("original producer error"))).unwrap_err();
    assert_eq!(error.to_string(), "original producer error");
    assert_eq!(
        db.query_row("SELECT gap,status,reason FROM capture_meta", [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?,
        before,
        "unknown outcome forbids a followup cleanup write"
    );
    Ok(())
}
