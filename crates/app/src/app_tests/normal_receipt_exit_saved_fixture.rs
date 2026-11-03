//! Byte-pinned existing BOT database; no financial rows or grants are fabricated.
use anyhow::Result;
use copybot_storage_core::SqliteStore;
use rusqlite::Connection;
use sha2::{Digest,Sha256};
use std::path::PathBuf;
pub(super) const BOT:&str="BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
pub(super) const BUY:&str="4DrbKigvkeXUQETGpvT2GkrAtZU8D7HbGgQbgBGQsGRZS5HCb9uc2hHXCoiS2e96kfUsCFCTX3fUPh5WmXuNsWSn";
pub(super) struct Case{
 pub _dir:super::temporary_output_fixture::OutputRoot,
 pub path:PathBuf,
 pub store:SqliteStore,
}
impl Case{
 pub fn new()->Result<Self>{
  let bytes=std::fs::read(std::env::var("COPYBOT_NORMAL_EXIT_DB")?)?;
  anyhow::ensure!(format!("{:x}",Sha256::digest(&bytes))=="0075dcf49a83e380f316eb8562bb04d31fc95b3050d920a9c5ca55a68596c3c7","full_current_db_pin");
  let root=super::temporary_output_fixture::OutputRoot::new("normal-receipt-exit-current")?;
  let path=root.path().join("normal.sqlite");std::fs::write(&path,bytes)?;
  let mut store=SqliteStore::open(&path)?;
  store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"),"/../../migrations")))?;
  Ok(Self{_dir:root,path,store})
 }
 pub fn conn(&self)->Result<Connection>{Ok(Connection::open(&self.path)?)}
 pub fn parents(&self)->Result<Vec<String>>{
  let c=self.conn()?;
  ["execution_tiny_experiment","execution_tiny_native_policy","real_session_sessions","native_buy_decisions","real_session_native_decisions","native_buy_fence_epochs"].into_iter().map(|table|{
   let mut q=c.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;let n=q.column_count();
   let rows=q.query_map([],|r|(0..n).map(|i|r.get::<_,rusqlite::types::Value>(i)).collect::<rusqlite::Result<Vec<_>>>())?.collect::<rusqlite::Result<Vec<_>>>()?;
   Ok(format!("{table}:{rows:?}"))
  }).collect()
 }
}
