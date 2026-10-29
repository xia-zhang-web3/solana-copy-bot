//! Replay actual LOCAL daemon state; restore production triggers before API calls.
//! No cash components/cycles are seeded and no financial dispatch is generated here.
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::{ReceiptObservationBundle, SqliteStore};
use rusqlite::{types::Value as Sql, Connection};
use serde_json::Value;

fn fixture() -> Result<(tempfile::TempDir, std::path::PathBuf, String)> {
    let dir=tempfile::tempdir()?;let path=dir.path().join("state.sqlite");
    drop(SqliteStore::open(&path)?);
    let c=Connection::open(&path)?;
    let triggers=c.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger'")?
        .query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    c.execute_batch("PRAGMA foreign_keys=OFF;BEGIN;")?;
    for (name,_) in &triggers { c.execute_batch(&format!("DROP TRIGGER \"{name}\";"))?; }
    let v:Value=serde_json::from_str(include_str!("fixtures/local_native_cash_actual.json"))?;
    for kind in ["schema","views","indexes"] {
        for (name,sql) in v[kind].as_object().unwrap() {
            let exists=c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name=?1)",[name],|r|r.get::<_,bool>(0))?;
            if !exists { c.execute_batch(sql.as_str().unwrap())?; }
        }
    }
    for (table,data) in v["tables"].as_object().unwrap() {
        assert!(!table.contains('"'));
        c.execute(&format!("DELETE FROM \"{table}\""),[])?;
        let columns=data["columns"].as_array().unwrap().iter().map(|v|format!("\"{}\"",v.as_str().unwrap())).collect::<Vec<_>>().join(",");
        let marks=vec!["?";data["columns"].as_array().unwrap().len()].join(",");
        for row in data["rows"].as_array().unwrap() {
            let values=row.as_array().unwrap().iter().map(|v|match v {
                Value::Null=>Sql::Null,Value::String(s)=>Sql::Text(s.clone()),
                Value::Number(n)=>n.as_i64().map(Sql::Integer).unwrap_or_else(||Sql::Real(n.as_f64().unwrap())),
                _=>panic!("fixture SQL scalar"),
            }).collect::<Vec<_>>();
            c.execute(&format!("INSERT INTO \"{table}\" ({columns}) VALUES ({marks})"),rusqlite::params_from_iter(values))?;
        }
    }
    let mut restored=0;
    for trigger in v["triggers"].as_array().unwrap() {
        let exists=c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",[trigger["table"].as_str().unwrap()],|r|r.get::<_,bool>(0))?;
        if exists {c.execute_batch(trigger["sql"].as_str().unwrap())?;restored+=1;}
    }
    c.execute_batch("COMMIT;")?;
    assert_eq!(c.query_row("SELECT count(*) FROM sqlite_master WHERE type='trigger'",[],|r|r.get::<_,usize>(0))?,restored);
    assert_eq!(c.query_row("SELECT count(*) FROM execution_receipt_cash_components",[],|r|r.get::<_,i64>(0))?,0);
    let id=c.query_row("SELECT order_id FROM execution_canary_receipt_facts WHERE side='sell'",[],|r|r.get(0))?;
    Ok((dir,path,id))
}
fn bundle(store:&SqliteStore,id:&str)->Result<ReceiptObservationBundle> {
    Ok(ReceiptObservationBundle {facts:store.load_execution_canary_receipt_facts(id)?.unwrap(),
        native:store.load_receipt_native_observations(id)?.unwrap()})
}
fn count(path:&std::path::Path,table:&str)->i64 {
    Connection::open(path).unwrap().query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get(0)).unwrap()
}
#[test]
fn native_sell_cash_projection_is_idempotent_after_restart_without_promoting_legacy_buy() -> Result<()> {
    let (_dir,path,id)=fixture()?;assert!(id.starts_with("exec-canary:rpc-owned-sell:"));
    let store=SqliteStore::open(&path)?;
    // A completed historical fill uses the additive materializer, not the pre-fill bundle API.
    let first=store.materialize_receipt_cash_components(&id,Utc::now())?;
    // The saved old ELF deliberately has no durable signed fee metadata for native SELL.
    assert_eq!(first.priority_fee_lamports,None);
    assert_eq!(first.classification,"partial_unknown");
    drop(store);
    let store=SqliteStore::open(&path)?;
    store.materialize_receipt_cash_components(&id,Utc::now())?;
    assert_eq!(store.load_receipt_cash_components(&id)?,Some(first));
    assert_eq!(count(&path,"execution_receipt_cash_components"),1);
    assert_eq!(count(&path,"execution_receipt_trade_cycles"),0);
    assert_eq!(count(&path,"execution_canary_dispatch"),2);
    assert_eq!(count(&path,"fills"),2);
    let c=Connection::open(&path)?;
    assert_eq!(c.query_row("SELECT count(*) FROM execution_receipt_cash_components c JOIN execution_canary_receipt_facts r USING(order_id) WHERE r.side='buy'",[],|r|r.get::<_,i64>(0))?,0);
    Ok(())
}
#[test]
fn unknown_unbound_signature_cannot_materialize_cash_or_release_dispatch() -> Result<()> {
    let (_dir,path,id)=fixture()?;let store=SqliteStore::open(&path)?;let mut b=bundle(&store,&id)?;
    b.facts.tx_signature="LOCAL-UNKNOWN-signature".into();b.native.tx_signature=b.facts.tx_signature.clone();
    assert!(store.record_receipt_observation_bundle(&b,Utc::now()).is_err());
    assert_eq!(count(&path,"execution_receipt_cash_components"),0);
    assert_eq!(count(&path,"execution_canary_dispatch"),2);
    assert_eq!(count(&path,"execution_tiny_reservations"),2);
    assert_eq!(count(&path,"fills"),2);
    Ok(())
}
