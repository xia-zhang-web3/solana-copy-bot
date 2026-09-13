//! Fixed variant A: one pinned experiment per database, never automatically rearmed.
mod claim;
mod snapshot;
pub(crate) use snapshot::read as owned_snapshot;
pub use snapshot::OwnedExperimentSnapshot;
mod protected;
pub use protected::{ProtectedCapitalClaim, ProtectedNativePolicy, TINY_NATIVE_ALLOWANCE};
mod owned_sell;
mod settlement;
use crate::SqliteDiscoveryStore;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
pub(crate) use claim::{reserve, reserve_transferred};
pub(crate) use owned_sell::{check_owned_sell_owner, prepare_owned_sell};
use rusqlite::{params, Connection, OptionalExtension};
pub(crate) use settlement::settle;

pub const TINY_BUY_LAMPORTS: u64 = 10_000_000;
pub const TINY_TRANSACTION_FEE: u64 = 100_000;
pub const TINY_PRIORITY_FEE: u64 = 50_000;
pub const TINY_TOTAL_FEE: u64 = 300_000;
pub const TINY_EXIT_RESERVE: u64 = 200_000;
pub const TINY_HORIZON_SECONDS: i64 = 3600;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TinyExperiment {
    pub id: String,
    pub wallet: String,
    pub activated_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub last_decision_at: DateTime<Utc>,
    pub state: String,
    pub stop_reason: Option<String>,
    pub buy_order_id: Option<String>,
    pub token: Option<String>,
    pub position_id: Option<String>,
}

/// App constructs these operands only from the current signed message and closed RPC.
#[derive(Debug, Clone)]
pub struct TinyBudgetClaim {
    pub experiment_id: String,
    pub wallet: String,
    pub tx_signature: String,
    pub message_sha256: String,
    pub transaction_sha256: String,
    /// Decoded amount only (SELL uses Some(0)); never a requested scalar.
    pub buy_lamports: Option<u64>,
    pub protected_capital: Option<ProtectedCapitalClaim>,
    pub total_fee: u64,
    pub priority_fee: u64,
    pub fee_slot: u64,
}

pub(super) fn load(conn: &Connection) -> Result<Option<TinyExperiment>> {
    let row = conn.query_row("SELECT experiment_id,wallet,activated_at,deadline,last_decision_at,
        state,stop_reason,buy_order_id,token,position_id FROM execution_tiny_experiment WHERE singleton=1", [],
        |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,
            r.get::<_,String>(4)?,r.get::<_,String>(5)?,r.get::<_,Option<String>>(6)?,
            r.get::<_,Option<String>>(7)?,r.get::<_,Option<String>>(8)?,r.get::<_,Option<String>>(9)?))).optional()?;
    row.map(
        |(
            id,
            wallet,
            activation,
            deadline,
            last,
            state,
            stop_reason,
            buy_order_id,
            token,
            position_id,
        )| {
            let activated_at = activation.parse()?;
            let deadline = deadline.parse()?;
            ensure!(
                activated_at + Duration::seconds(TINY_HORIZON_SECONDS) == deadline,
                "tiny_budget_deadline_corrupt"
            );
            Ok(TinyExperiment {
                id,
                wallet,
                activated_at,
                deadline,
                last_decision_at: last.parse()?,
                state,
                stop_reason,
                buy_order_id,
                token,
                position_id,
            })
        },
    )
    .transpose()
}

pub(super) fn stop(conn: &Connection, reason: &str) -> Result<()> {
    conn.execute("UPDATE execution_tiny_experiment SET state='stopped',stop_reason=?1 WHERE singleton=1 AND state='active'",[reason])?;
    Ok(())
}

pub(super) fn refresh(conn: &Connection, now: DateTime<Utc>) -> Result<Option<TinyExperiment>> {
    let Some(e) = load(conn)? else {
        return Ok(None);
    };
    if now >= e.deadline {
        stop(conn, "tiny_budget_deadline")?;
    }
    let (_, sell, committed) = totals(conn)?;
    if sell >= 2 {
        stop(conn, "tiny_budget_sell_slots")?;
    }
    if committed >= TINY_TOTAL_FEE {
        stop(conn, "tiny_budget_fee_exhausted")?;
    }
    if let Some(position) = &e.position_id {
        let closed: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM positions WHERE position_id=?1 AND state='closed')",
            [position],
            |r| r.get(0),
        )?;
        if closed {
            conn.execute("UPDATE execution_tiny_experiment SET state='completed',stop_reason='tiny_budget_completed' WHERE singleton=1",[])?;
        }
    }
    load(conn)
}

impl SqliteDiscoveryStore {
    /// Explicit activation only. Repeating the same identity cannot reset any field.
    pub fn activate_tiny_experiment(
        &self,
        id: &str,
        wallet: &str,
        now: DateTime<Utc>,
    ) -> Result<TinyExperiment> {
        ensure!(
            !id.is_empty()
                && id.len() <= 128
                && id.trim() == id
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "tiny_budget_invalid_id"
        );
        ensure!(
            !wallet.trim().is_empty() && wallet.trim() == wallet,
            "tiny_budget_invalid_wallet"
        );
        self.with_immediate_transaction_retry("activate tiny experiment",|conn| {
            if let Some(e)=refresh(conn,now)? {
                ensure!(e.id==id && e.wallet==wallet,"tiny_budget_activation_conflict");
                ensure!(protected::mode(conn)? == "decoded_amount", "tiny_capital_mode_conflict");
                return Ok(e);
            }
            let deadline=now.checked_add_signed(Duration::seconds(TINY_HORIZON_SECONDS)).context("tiny_budget_clock_overflow")?;
            conn.execute("INSERT INTO execution_tiny_experiment(singleton,experiment_id,wallet,activated_at,deadline,last_decision_at,state)
                VALUES(1,?1,?2,?3,?4,?3,'active')",params![id,wallet,now.to_rfc3339(),deadline.to_rfc3339()])?;
            load(conn)?.context("tiny_budget_activation_missing")
        })
    }
    pub fn load_tiny_experiment(&self, now: DateTime<Utc>) -> Result<Option<TinyExperiment>> {
        self.with_immediate_transaction_retry("read tiny experiment", |conn| refresh(conn, now))
    }
}

fn totals(conn: &Connection) -> Result<(u64, u64, u64)> {
    let (buys,sells,fees):(u64,u64,u64)=conn.query_row("SELECT COUNT(CASE WHEN side='buy' THEN 1 END),COUNT(CASE WHEN side='sell' THEN 1 END),COALESCE(SUM(COALESCE(actual_fee,fee_bound)),0) FROM execution_tiny_reservations",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let (unsigned, reserved) = owned_sell::unsigned_totals(conn)?;
    Ok((
        buys,
        sells
            .checked_add(unsigned)
            .context("tiny_budget_overflow")?,
        fees.checked_add(reserved).context("tiny_budget_overflow")?,
    ))
}
