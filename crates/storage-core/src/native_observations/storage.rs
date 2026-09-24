use super::{merge, NativeAccountObservations, ReceiptObservationBundle};
use crate::{
    execution_canary_fill_marker::fill_exists, execution_canary_receipt_facts,
    receipt_facts_identity, SqliteDiscoveryStore,
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn load(
    conn: &Connection,
    id: &str,
) -> Result<Option<(NativeAccountObservations, Option<String>)>> {
    let row:Option<(String,String,Option<String>)>=conn.query_row("SELECT observations_json,tx_signature,conflict_reason FROM execution_receipt_native_observations WHERE order_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    row.map(|(json, signature, reason)| {
        let value: NativeAccountObservations = serde_json::from_str(&json)?;
        value.validate()?;
        ensure!(
            value.order_id == id && value.tx_signature == signature,
            "native observations stored binding conflict"
        );
        Ok((value, reason))
    })
    .transpose()
}
impl SqliteDiscoveryStore {
    pub fn load_receipt_native_observations(
        &self,
        id: &str,
    ) -> Result<Option<NativeAccountObservations>> {
        Ok(load(&self.conn, id)?.map(|r| r.0))
    }
    /// A single transaction precedes cash completion. Fresh facts stay with the
    /// caller; merged facts must never replace fresh SELL evidence.
    pub fn record_receipt_observation_bundle(
        &self,
        b: &ReceiptObservationBundle,
        now: DateTime<Utc>,
    ) -> Result<()> {
        b.facts.validate()?;
        b.native.validate()?;
        let binding = NativeAccountObservations::empty(&b.facts);
        ensure!(
            b.native.order_id == binding.order_id
                && b.native.tx_signature == binding.tx_signature
                && b.native.wallet_pubkey == binding.wallet_pubkey
                && b.native.token == binding.token
                && b.native.side == binding.side
                && b.native.slot == binding.slot,
            "native observations bundle identity mismatch"
        );
        let conflict=self.with_immediate_transaction_retry("record receipt observation bundle",|conn| {
            receipt_facts_identity::validate_identity(conn,&b.facts)?;
            let old=load(conn,&b.facts.order_id)?;
            if old.as_ref().is_some_and(|(_,r)|r.is_some()) { return Ok(true); }
            let merged=if let Some((old,_))=&old {
                match merge::merge(old,&b.native) {
                    Ok(m)=>m,
                    Err(error) if error.to_string()=="native observations known conflict" => {
                        ensure!(!fill_exists(conn,&b.facts.order_id)?,"native observations completed immutable");
                        ensure!(conn.execute("UPDATE execution_receipt_native_observations SET conflict_reason='native_observation_conflict' WHERE order_id=?1",[&b.facts.order_id])?==1,"native conflict write missing");
                        ensure!(load(conn,&b.facts.order_id)?.is_some_and(|(_,r)|r.as_deref()==Some("native_observation_conflict")),"native conflict readback mismatch");
                        return Ok(true);
                    }
                    Err(e)=>return Err(e),
                }
            } else { b.native.clone() };
            if fill_exists(conn,&b.facts.order_id)? {
                ensure!(old.as_ref().is_some_and(|(o,_)|o==&merged),"native observations completed immutable");
            }
            execution_canary_receipt_facts::record_on_conn(conn,&b.facts,now)?;
            if old.as_ref().is_none_or(|(o,_)|o!=&merged) {
                ensure!(!fill_exists(conn,&b.facts.order_id)?,"native observations completed immutable");
                ensure!(conn.execute("INSERT INTO execution_receipt_native_observations(order_id,tx_signature,observations_json,recorded_at) VALUES(?1,?2,?3,?4) ON CONFLICT(order_id) DO UPDATE SET observations_json=excluded.observations_json",params![merged.order_id,merged.tx_signature,serde_json::to_string(&merged)?,now.to_rfc3339()])?==1,"native observations write missing");
            }
            ensure!(load(conn,&b.facts.order_id)?.as_ref().is_some_and(|(o,r)|o==&merged && r.is_none()),"native observations readback mismatch");
            Ok(false)
        })?;
        ensure!(!conflict, "native_observation_conflict");
        if b.facts.order_id.starts_with("exec-canary:owner-buy:")
            || b.facts.order_id.starts_with("exec-canary:owner-exit:")
        {
            self.materialize_receipt_cash_components(&b.facts.order_id, now)?;
        }
        Ok(())
    }
}
