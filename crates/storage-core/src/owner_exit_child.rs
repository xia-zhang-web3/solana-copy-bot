//! One immutable child owner authority for an exactly proven pre-build parent failure.
use crate::{OwnerExitIntent, OwnerExitIntentRecordOutcome, SqliteDiscoveryStore};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

pub(crate) const PREFIX: &str = "source-child:";
pub(crate) fn is_child(id: &str) -> bool {
    id.starts_with(PREFIX)
}
pub(crate) fn parent_id(id: &str) -> Result<String> {
    Ok(format!(
        "source-recovery:{}",
        id.strip_prefix(PREFIX).context("owner_child_id")?
    ))
}
fn failed_snapshot(c: &Connection, parent: &OwnerExitIntent) -> Result<String> {
    c.query_row("SELECT json_object('order_id',order_id,'signal_id',signal_id,'route',route,
      'submit_ts',submit_ts,'confirm_ts',confirm_ts,'status',status,'err_code',err_code,
      'client_order_id',client_order_id,'tx_signature',tx_signature,'simulation_status',simulation_status,
      'simulation_error',simulation_error,'attempt',attempt,'applied_tip_lamports',applied_tip_lamports,
      'ata_create_rent_lamports',ata_create_rent_lamports,'network_fee_lamports_hint',network_fee_lamports_hint,
      'base_fee_lamports_hint',base_fee_lamports_hint,'priority_fee_lamports_hint',priority_fee_lamports_hint)
      FROM orders WHERE order_id=?1",[crate::owner_exit_order_id(&parent.intent_id)],|r|r.get(0))
      .context("owner_child_failed_order_missing")
}
fn before_build(
    c: &Connection,
    parent: &OwnerExitIntent,
    child: &OwnerExitIntent,
) -> Result<String> {
    let order = crate::owner_exit_order_id(&parent.intent_id);
    let snapshot = failed_snapshot(c, parent)?;
    let v: serde_json::Value = serde_json::from_str(&snapshot)?;
    ensure!(
        v["signal_id"] == crate::owner_exit_identity_id(&parent.intent_id)
            && v["client_order_id"] == crate::owner_exit_client_order_id(&parent.intent_id)
            && v["status"] == "execution_canary_failed"
            && v["err_code"] == "build_failed"
            && v["simulation_status"] == "not_run"
            && v["tx_signature"].is_null()
            && v["attempt"] == 1
            && v["route"] == parent.route
            && v["simulation_error"]
                .as_str()
                .is_some_and(|s| !s.trim().is_empty()),
        "owner_child_parent_not_before_build_failure"
    );
    let latest = ["submit_ts", "confirm_ts"]
        .into_iter()
        .filter_map(|key| v[key].as_str())
        .map(DateTime::parse_from_rfc3339)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        latest.len() == 2
            && latest
                .iter()
                .all(|t| child.activated_at >= t.with_timezone(&Utc)),
        "owner_child_authority_predates_failure"
    );
    let clean:bool=c.query_row("SELECT
      NOT EXISTS(SELECT 1 FROM execution_canary_build_plan_metadata WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM execution_canary_dispatch WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM execution_canary_receipt_facts WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM fills WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM execution_failed_expense_ledger WHERE order_id=?1)
      AND NOT EXISTS(SELECT 1 FROM owner_exit_fee_reservations WHERE run_id=?2 AND order_id<>?3)
      AND NOT EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch WHERE order_id<>?3)
      AND NOT EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE status!='complete' AND order_id<>?3)",
      params![order,parent.run_id,crate::owner_exit_order_id(&child.intent_id)],|r|r.get(0))?;
    ensure!(clean, "owner_child_parent_has_obligation_or_build_evidence");
    ensure!(
        [
            "applied_tip_lamports",
            "ata_create_rent_lamports",
            "network_fee_lamports_hint",
            "base_fee_lamports_hint",
            "priority_fee_lamports_hint"
        ]
        .iter()
        .all(|key| v[*key].is_null()),
        "owner_child_parent_has_wire_evidence"
    );
    Ok(snapshot)
}
fn same_parent(parent: &OwnerExitIntent, child: &OwnerExitIntent) -> Result<()> {
    ensure!(
        child.run_id == parent.run_id
            && child.buy_order_id == parent.buy_order_id
            && child.buy_receipt_signature == parent.buy_receipt_signature
            && child.position_id == parent.position_id
            && child.wallet == parent.wallet
            && child.signer == parent.signer
            && child.genesis_hash == parent.genesis_hash
            && child.mint == parent.mint
            && child.amount_raw == parent.amount_raw
            && child.decimals == parent.decimals
            && child.route == parent.route
            && child.min_reserve_lamports == parent.min_reserve_lamports
            && child.max_priority_fee_lamports <= parent.max_priority_fee_lamports
            && child.max_slippage_bps <= parent.max_slippage_bps
            && child.max_daily_loss_lamports <= parent.max_daily_loss_lamports
            && child.authority_sha256 != parent.authority_sha256,
        "owner_child_parent_binding_changed"
    );
    ensure!(
        child.activated_at < child.expires_at
            && child.expires_at.signed_duration_since(child.activated_at)
                <= chrono::Duration::seconds(300),
        "owner_child_window"
    );
    Ok(())
}
pub(crate) fn validate(c: &Connection, i: &OwnerExitIntent) -> Result<()> {
    let row:Option<(String,String,String,String)>=c.query_row("SELECT parent_intent_id,parent_failed_order_json,
      parent_failed_order_sha256,intent_json FROM owner_exit_child_attempts WHERE child_intent_id=?1",
      [&i.intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let (parent_id, snapshot, pin, raw) = row.context("owner_child_authority_missing")?;
    ensure!(
        parent_id == self::parent_id(&i.intent_id)?
            && serde_json::from_str::<OwnerExitIntent>(&raw)? == *i,
        "owner_child_authority_changed"
    );
    let parent: OwnerExitIntent =
        crate::owner_exit_load::load(c, &parent_id)?.context("owner_child_parent_missing")?;
    same_parent(&parent, i)?;
    ensure!(
        snapshot == before_build(c, &parent, i)?
            && format!("{:x}", Sha256::digest(snapshot.as_bytes())) == pin,
        "owner_child_failed_history_changed"
    );
    Ok(())
}
impl SqliteDiscoveryStore {
    pub(crate) fn register_owner_exit_child(
        &self,
        i: &OwnerExitIntent,
    ) -> Result<OwnerExitIntentRecordOutcome> {
        self.with_immediate_transaction_retry("register source owner child",|c|{
            if let Some(old)=crate::owner_exit_load::load(c,&i.intent_id)?{
                ensure!(old==*i,"owner_child_authority_conflict");
                return Ok(OwnerExitIntentRecordOutcome::Existing)
            }
            let parent_id=self::parent_id(&i.intent_id)?;
            let parent=crate::owner_exit_load::load(c,&parent_id)?.context("owner_child_parent_missing")?;
            same_parent(&parent,i)?;
            let snapshot=before_build(c,&parent,i)?;
            crate::owner_exit::validate(i,crate::controlled_receipt_lot::source_scoped(c,&i.run_id,&i.wallet)?)?;
            // Validate canonical finalized BUY and full unchanged own position, without requiring a child row yet.
            crate::owner_source_recovery::validate_parent(c,i,&parent_id)?;
            c.execute("INSERT INTO owner_exit_child_attempts(child_intent_id,parent_intent_id,parent_failed_order_id,
              parent_failed_order_json,parent_failed_order_sha256,intent_json,registered_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
              params![i.intent_id,parent_id,crate::owner_exit_order_id(&parent.intent_id),snapshot,
                format!("{:x}",Sha256::digest(snapshot.as_bytes())),serde_json::to_string(i)?,Utc::now().to_rfc3339()])?;
            c.execute("INSERT INTO execution_order_sources(identity_id,owner_exit_child_id)VALUES(?1,?2)",
              params![crate::owner_exit_identity_id(&i.intent_id),i.intent_id])?;
            Ok(OwnerExitIntentRecordOutcome::Inserted)
        })
    }
}
