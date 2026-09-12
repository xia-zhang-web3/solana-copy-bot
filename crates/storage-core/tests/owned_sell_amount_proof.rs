use anyhow::Result;
use copybot_storage_core::{ExecutionCanaryBuildPlanMetadata as Metadata, SqliteStore};
use rusqlite::Connection;
use std::path::PathBuf;

struct Db {
    _dir: tempfile::TempDir,
    path: PathBuf,
    store: SqliteStore,
    sql: Connection,
}
impl Db {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("proof.sqlite");
        let mut store = SqliteStore::open(&path)?;
        store
            .run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
        let sql = Connection::open(&path)?;
        Ok(Self {
            _dir: dir,
            path,
            store,
            sql,
        })
    }
}
fn metadata() -> Metadata {
    Metadata {
        order_id: "order".into(),
        signal_id: "signal".into(),
        client_order_id: "client".into(),
        recorded_ts: "2026-09-09T00:00:10Z".parse().unwrap(),
        http_request_started_ts: None,
        quote_response_available_ts: None,
        quote_source: Some("synthetic".into()),
        quote_event_id: Some("quote".into()),
        quote_request_ts: None,
        quote_status: Some("ok".into()),
        quote_in_amount_raw: Some("7000".into()),
        quote_out_amount_raw: Some("100000000".into()),
        quote_response_json: Some("{}".into()),
        quote_price_sol: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_source: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        slippage_bps: None,
        decision_status: None,
        decision_reason: None,
    }
}
const PROOF: &str = r#"{"version":1,"fixture":"opaque application quantity provenance"}"#;

#[test]
fn metadata_and_sell_amount_proof_roundtrip_and_legacy_replacement_are_atomic() -> Result<()> {
    let db = Db::new()?;
    let m = metadata();
    db.store
        .record_execution_canary_build_plan_metadata_with_sell_amount(&m, Some(PROOF))?;
    let reopened = SqliteStore::open(&db.path)?;
    assert_eq!(
        reopened.load_execution_canary_build_plan_metadata("order")?,
        Some(m.clone())
    );
    assert_eq!(
        reopened
            .load_execution_canary_sell_amount_proof("order")?
            .as_deref(),
        Some(PROOF)
    );
    db.sql.execute_batch("CREATE TRIGGER reject_metadata BEFORE UPDATE ON execution_canary_build_plan_metadata BEGIN SELECT RAISE(ABORT,'synthetic atomicity failure'); END;")?;
    let mut next = m.clone();
    next.quote_in_amount_raw = Some("4000".into());
    assert!(reopened
        .record_execution_canary_build_plan_metadata_with_sell_amount(&next, Some("replacement"))
        .is_err());
    assert_eq!(
        reopened.load_execution_canary_build_plan_metadata("order")?,
        Some(m)
    );
    assert_eq!(
        reopened
            .load_execution_canary_sell_amount_proof("order")?
            .as_deref(),
        Some(PROOF)
    );
    db.sql.execute_batch("DROP TRIGGER reject_metadata;")?;
    reopened.record_execution_canary_build_plan_metadata(&next)?;
    assert_eq!(
        reopened.load_execution_canary_build_plan_metadata("order")?,
        Some(next)
    );
    assert_eq!(
        reopened.load_execution_canary_sell_amount_proof("order")?,
        None
    );
    Ok(())
}

#[test]
fn legacy_missing_column_never_accepts_new_proof_and_applied_damage_errors() -> Result<()> {
    let db = Db::new()?;
    db.sql.execute_batch("ALTER TABLE execution_canary_build_plan_metadata DROP COLUMN owned_sell_amount_proof_json;")?;
    assert!(db
        .store
        .load_execution_canary_sell_amount_proof("order")
        .is_err());
    assert!(db
        .store
        .record_execution_canary_build_plan_metadata_with_sell_amount(&metadata(), None)
        .is_err());
    db.sql.execute(
        "DELETE FROM schema_migrations WHERE version='0070_owned_sell_amount_proof.sql'",
        [],
    )?;
    assert_eq!(
        db.store.load_execution_canary_sell_amount_proof("order")?,
        None
    );
    assert!(db
        .store
        .record_execution_canary_build_plan_metadata_with_sell_amount(&metadata(), Some(PROOF))
        .is_err());
    assert!(db
        .store
        .load_execution_canary_build_plan_metadata("order")?
        .is_none());
    db.store
        .record_execution_canary_build_plan_metadata(&metadata())?;
    assert_eq!(
        db.store.load_execution_canary_sell_amount_proof("order")?,
        None
    );
    Ok(())
}

#[test]
fn proof_write_without_optional_http_timing_preserves_sql_binding() -> Result<()> {
    let db = Db::new()?;
    db.sql.execute(
        "DELETE FROM schema_migrations WHERE version='0066_quote_http_timing.sql'",
        [],
    )?;
    db.sql.execute_batch(
        "ALTER TABLE execution_canary_build_plan_metadata DROP COLUMN http_request_started_ts;",
    )?;
    db.store
        .record_execution_canary_build_plan_metadata_with_sell_amount(&metadata(), Some(PROOF))?;
    let reopened = SqliteStore::open(&db.path)?;
    assert_eq!(
        reopened
            .load_execution_canary_sell_amount_proof("order")?
            .as_deref(),
        Some(PROOF)
    );
    assert_eq!(
        reopened.load_execution_canary_build_plan_metadata("order")?,
        Some(metadata())
    );
    Ok(())
}
