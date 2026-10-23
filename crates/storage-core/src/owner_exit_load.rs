//! Exact parent-or-child EXIT resolution; dispatch restart never invents new authority.
use crate::{OwnerExitIntent, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}
pub(crate) fn load(c: &Connection, id: &str) -> Result<Option<OwnerExitIntent>> {
    let row = c.query_row("SELECT intent_id,run_id,buy_order_id,buy_receipt_signature,
            position_id,wallet,signer,genesis_hash,mint,amount_raw,decimals,route,activated_at,expires_at,
            authority_sha256,max_priority_fee_lamports,min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports
            FROM owner_exit_effective_intents WHERE intent_id=?1", [id], |r| {
                Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,
                    r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,
                    r.get::<_,String>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?,
                    r.get::<_,u64>(9)?,r.get::<_,u8>(10)?,r.get::<_,String>(11)?,
                    r.get::<_,String>(12)?,r.get::<_,String>(13)?,r.get::<_,String>(14)?,
                    r.get::<_,u64>(15)?,r.get::<_,u64>(16)?,r.get::<_,u32>(17)?,
                    r.get::<_,u64>(18)?))
            }).optional()?;
    row.map(|r| {
        Ok(OwnerExitIntent {
            intent_id: r.0,
            run_id: r.1,
            buy_order_id: r.2,
            buy_receipt_signature: r.3,
            position_id: r.4,
            wallet: r.5,
            signer: r.6,
            genesis_hash: r.7,
            mint: r.8,
            amount_raw: r.9,
            decimals: r.10,
            route: r.11,
            activated_at: parse_ts(&r.12)?,
            expires_at: parse_ts(&r.13)?,
            authority_sha256: r.14,
            max_priority_fee_lamports: r.15,
            min_reserve_lamports: r.16,
            max_slippage_bps: r.17,
            max_daily_loss_lamports: r.18,
        })
    })
    .transpose()
}
impl SqliteDiscoveryStore {
    pub fn load_owner_exit_intent(&self, id: &str) -> Result<Option<OwnerExitIntent>> {
        load(&self.conn, id)
    }

    pub fn list_owner_exit_intents(&self, limit: u32) -> Result<Vec<OwnerExitIntent>> {
        let ids = self.conn.prepare("SELECT intent_id FROM owner_exit_effective_intents ORDER BY activated_at DESC,intent_id DESC LIMIT ?1")?
            .query_map([limit.min(100)], |r| r.get::<_,String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter()
            .map(|id| {
                self.load_owner_exit_intent(id)?
                    .context("owner_exit_intent_missing")
            })
            .collect()
    }

    /// A claimed SELL without a fill remains a reconciliation obligation across restarts.
    pub fn list_owner_exit_recovery_intents(&self) -> Result<Vec<OwnerExitIntent>> {
        let ids = self
            .conn
            .prepare(
                "SELECT i.intent_id FROM owner_exit_effective_intents i
            JOIN execution_order_sources s ON COALESCE(s.owner_exit_child_id,s.owner_exit_intent_id)=i.intent_id
            AND s.identity_id='owner-exit:'||i.intent_id
            JOIN orders o ON o.signal_id=s.identity_id
            JOIN execution_canary_dispatch d ON d.order_id=o.order_id
            WHERE NOT EXISTS (SELECT 1 FROM fills f WHERE f.order_id=o.order_id)
            ORDER BY d.claimed_at,i.intent_id",
            )?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter()
            .map(|id| {
                self.load_owner_exit_intent(id)?
                    .context("owner_exit_recovery_missing")
            })
            .collect()
    }
}
