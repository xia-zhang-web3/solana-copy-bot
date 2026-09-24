//! Explicit owner BUY origin and durable single-use dispatch authority.
use crate::{
    execution_canary_dispatch, ExecutionCanaryDispatch, ExecutionCanaryOrder,
    ExecutionCanaryRecordOutcome, ExecutionCanaryReserveResult, ExecutionDispatchClaim,
    SqliteDiscoveryStore, TinyBudgetClaim, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_SIMULATED,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnerTechnicalBuyIntent {
    pub intent_id: String,
    pub run_id: String,
    pub wallet: String,
    pub signer: String,
    pub genesis_hash: String,
    pub mint: String,
    pub amount_lamports: u64,
    pub route: String,
    pub activated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub authority_sha256: String,
    pub max_priority_fee_lamports: u64,
    pub min_reserve_lamports: u64,
    pub max_slippage_bps: u32,
    pub max_daily_loss_lamports: u64,
    pub max_open_positions: u32,
    pub max_buy_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerTechnicalBuyIntentRecordOutcome {
    Inserted,
    Existing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionOrderOrigin {
    Copy { signal_id: String },
    OwnedSell { intent_id: String },
    OwnerTechnicalBuy { intent_id: String },
    OwnerExit { intent_id: String },
}

pub fn owner_technical_buy_identity_id(intent_id: &str) -> String {
    format!("owner-buy:{intent_id}")
}
pub fn owner_technical_buy_order_id(intent_id: &str) -> String {
    format!("exec-canary:owner-buy:{intent_id}")
}
pub fn owner_technical_buy_client_order_id(intent_id: &str) -> String {
    format!("copybot:owner-buy:{intent_id}")
}

impl SqliteDiscoveryStore {
    pub fn register_owner_technical_buy_intent(
        &self,
        intent: &OwnerTechnicalBuyIntent,
    ) -> Result<OwnerTechnicalBuyIntentRecordOutcome> {
        validate(intent)?;
        self.with_immediate_transaction_retry("register owner technical BUY intent", |conn| {
            if let Some(existing) = self.load_owner_technical_buy_intent(&intent.intent_id)? {
                ensure!(existing == *intent, "owner_buy_intent_conflict");
                return Ok(OwnerTechnicalBuyIntentRecordOutcome::Existing);
            }
            conn.execute("INSERT INTO owner_technical_buy_intents(intent_id,run_id,wallet,signer,genesis_hash,mint,
                amount_lamports,route,activated_at,expires_at,authority_sha256,max_priority_fee_lamports,
                min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports,max_open_positions,max_buy_count)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,1,1)",
                params![intent.intent_id,intent.run_id,intent.wallet,intent.signer,intent.genesis_hash,
                    intent.mint,i64::try_from(intent.amount_lamports)?,intent.route,
                    intent.activated_at.to_rfc3339(),intent.expires_at.to_rfc3339(),intent.authority_sha256,
                    i64::try_from(intent.max_priority_fee_lamports)?,i64::try_from(intent.min_reserve_lamports)?,
                    i64::from(intent.max_slippage_bps),i64::try_from(intent.max_daily_loss_lamports)?])?;
            let identity = owner_technical_buy_identity_id(&intent.intent_id);
            conn.execute("INSERT INTO execution_order_sources(identity_id,owner_buy_intent_id) VALUES(?1,?2)",
                params![identity,intent.intent_id])?;
            Ok(OwnerTechnicalBuyIntentRecordOutcome::Inserted)
        })
    }

    pub fn load_owner_technical_buy_intent(
        &self,
        id: &str,
    ) -> Result<Option<OwnerTechnicalBuyIntent>> {
        let row = self.conn.query_row("SELECT intent_id,run_id,wallet,signer,genesis_hash,mint,
            amount_lamports,route,activated_at,expires_at,authority_sha256,max_priority_fee_lamports,
            min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports,max_open_positions,max_buy_count
            FROM owner_technical_buy_intents WHERE intent_id=?1", [id], |r| {
            Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,
                r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,
                r.get::<_,u64>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?,
                r.get::<_,String>(9)?,r.get::<_,String>(10)?,r.get::<_,u64>(11)?,
                r.get::<_,u64>(12)?,r.get::<_,u32>(13)?,r.get::<_,u64>(14)?,
                r.get::<_,u32>(15)?,r.get::<_,u32>(16)?))
        }).optional()?;
        row.map(|r| {
            Ok(OwnerTechnicalBuyIntent {
                intent_id: r.0,
                run_id: r.1,
                wallet: r.2,
                signer: r.3,
                genesis_hash: r.4,
                mint: r.5,
                amount_lamports: r.6,
                route: r.7,
                activated_at: parse_ts(&r.8)?,
                expires_at: parse_ts(&r.9)?,
                authority_sha256: r.10,
                max_priority_fee_lamports: r.11,
                min_reserve_lamports: r.12,
                max_slippage_bps: r.13,
                max_daily_loss_lamports: r.14,
                max_open_positions: r.15,
                max_buy_count: r.16,
            })
        })
        .transpose()
    }

    pub fn list_owner_technical_buy_intents(
        &self,
        limit: u32,
    ) -> Result<Vec<OwnerTechnicalBuyIntent>> {
        let ids = self.conn.prepare("SELECT intent_id FROM owner_technical_buy_intents ORDER BY activated_at DESC,intent_id DESC LIMIT ?1")?
            .query_map([limit.min(100)], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter()
            .map(|id| {
                self.load_owner_technical_buy_intent(id)?
                    .context("owner_buy_intent_missing")
            })
            .collect()
    }

    /// Every dispatched owner BUY remains a reconciliation obligation. Query
    /// pending dispatches directly so older runs cannot fall out of a recent
    /// intent page when new one-shot runs are added later.
    pub fn list_owner_technical_buy_recovery_intents(&self) -> Result<Vec<OwnerTechnicalBuyIntent>> {
        let ids = self.conn.prepare("SELECT i.intent_id FROM owner_technical_buy_intents i
            JOIN execution_order_sources s ON s.owner_buy_intent_id=i.intent_id
            JOIN orders o ON o.signal_id=s.identity_id
            JOIN execution_canary_dispatch d ON d.order_id=o.order_id
            WHERE (o.status IN (?1,?2,?3)
                OR EXISTS (SELECT 1 FROM execution_failed_expense_tasks e
                    WHERE e.order_id=o.order_id AND e.status='pending'))
            AND NOT EXISTS (SELECT 1 FROM fills f WHERE f.order_id=o.order_id)
            ORDER BY d.claimed_at,i.intent_id")?
            .query_map(rusqlite::params![crate::EXECUTION_STATUS_CANARY_SUBMITTED,
                crate::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
                crate::EXECUTION_STATUS_CANARY_CONFIRMED],
                |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter().map(|id| self.load_owner_technical_buy_intent(id)?
            .context("owner_buy_recovery_intent_missing")).collect()
    }

    pub fn execution_order_origin(&self, order_id: &str) -> Result<Option<ExecutionOrderOrigin>> {
        let row:Option<(String,Option<String>,Option<String>,Option<String>,Option<String>)> = self.conn.query_row(
            "SELECT o.signal_id,s.copy_signal_id,s.owned_sell_intent_id,s.owner_buy_intent_id,s.owner_exit_intent_id
             FROM orders o JOIN execution_order_sources s ON s.identity_id=o.signal_id WHERE o.order_id=?1",
            [order_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        row.map(|(identity, copy, sell, buy, exit)| match (copy, sell, buy, exit) {
            (Some(id), None, None, None) if identity == id => {
                Ok(ExecutionOrderOrigin::Copy { signal_id: id })
            }
            (None, Some(id), None, None) if identity == id => {
                Ok(ExecutionOrderOrigin::OwnedSell { intent_id: id })
            }
            (None, None, Some(id), None) if identity == owner_technical_buy_identity_id(&id) => {
                Ok(ExecutionOrderOrigin::OwnerTechnicalBuy { intent_id: id })
            }
            (None, None, None, Some(id)) if identity == crate::owner_exit_identity_id(&id) => {
                Ok(ExecutionOrderOrigin::OwnerExit { intent_id: id })
            }
            _ => anyhow::bail!("execution_order_origin_conflict"),
        })
        .transpose()
    }

    /// Reserve one canonical order; only a new durable dispatch claim may send.
    pub fn reserve_owner_technical_buy_order(
        &self,
        intent_id: &str,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionCanaryReserveResult> {
        let order_id = owner_technical_buy_order_id(intent_id);
        let identity = owner_technical_buy_identity_id(intent_id);
        let client_id = owner_technical_buy_client_order_id(intent_id);
        let inserted = self.with_immediate_transaction_retry("reserve owner technical BUY", |conn| {
            let now=clock()?;
            let intent=self.load_owner_technical_buy_intent(intent_id)?.context("owner_buy_intent_missing")?;
            active(&intent,now)?;
            let registry:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_order_sources WHERE identity_id=?1 AND owner_buy_intent_id=?2)",
                params![identity,intent_id],|r|r.get(0))?;
            ensure!(registry,"owner_buy_origin_missing");
            if let Some(old)=self.load_execution_canary_order(&order_id)? {
                ensure!(old.signal_id==identity && old.route==intent.route && old.client_order_id==client_id,
                    "owner_buy_order_conflict");
                return Ok(false);
            }
            conn.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt,simulation_status)
                VALUES(?1,?2,?3,?4,?5,?6,1,?7)",
                params![order_id,identity,intent.route,now.to_rfc3339(),EXECUTION_STATUS_CANARY_CANDIDATE,
                    client_id,EXECUTION_SIMULATION_STATUS_NOT_RUN])?;
            Ok(true)
        })?;
        let order = self
            .load_execution_canary_order(&order_id)?
            .context("owner_buy_order_missing")?;
        Ok(ExecutionCanaryReserveResult {
            outcome: if inserted {
                ExecutionCanaryRecordOutcome::Inserted
            } else {
                ExecutionCanaryRecordOutcome::Existing
            },
            order,
        })
    }

    /// The only new claim authorizes transport; Existing never authorizes resend.
    pub fn claim_owner_technical_buy_dispatch(
        &self,
        expected: &ExecutionCanaryOrder,
        dispatch: &ExecutionCanaryDispatch,
        budget: &TinyBudgetClaim,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionDispatchClaim> {
        ensure!(
            dispatch.side == "buy" && dispatch.attempt == 1 && expected.attempt == 1,
            "owner_buy_dispatch_side_or_attempt"
        );
        self.with_immediate_transaction_retry("claim owner technical BUY dispatch", |conn| {
            let intent_id=dispatch.signal_id.strip_prefix("owner-buy:").context("owner_buy_dispatch_origin")?;
            let intent=self.load_owner_technical_buy_intent(intent_id)?.context("owner_buy_intent_missing")?;
            ensure!(dispatch.order_id==owner_technical_buy_order_id(intent_id)
                && dispatch.client_order_id==owner_technical_buy_client_order_id(intent_id)
                && expected.order_id==dispatch.order_id && expected.signal_id==dispatch.signal_id
                && expected.client_order_id==dispatch.client_order_id && expected.route==dispatch.route
                && dispatch.route==intent.route && dispatch.wallet==intent.wallet
                && dispatch.token==intent.mint && intent.signer==intent.wallet
                && budget.experiment_id==intent.run_id && budget.wallet==intent.wallet
                && budget.tx_signature==dispatch.tx_signature
                && budget.message_sha256==dispatch.message_sha256
                && budget.transaction_sha256==dispatch.transaction_sha256
                && budget.priority_fee<=intent.max_priority_fee_lamports
                && budget_amount(budget)==Some(intent.amount_lamports),
                "owner_buy_dispatch_identity");
            ensure!(!dispatch.tx_signature.is_empty() && dispatch.message_sha256.len()==64
                && dispatch.transaction_sha256.len()==64,"owner_buy_dispatch_binding_missing");
            if let Some(old)=self.load_execution_canary_dispatch(&dispatch.order_id)? {
                ensure!(old==*dispatch,"dispatch_identity_conflict");
                return Ok(ExecutionDispatchClaim::Existing);
            }
            let now=clock()?;
            active(&intent,now)?;
            let current=self.load_execution_canary_order(&expected.order_id)?.context("owner_buy_order_missing")?;
            ensure!(current==*expected && current.status==EXECUTION_STATUS_CANARY_SIMULATED
                && current.tx_signature.as_deref().is_none_or(|s|s.trim().is_empty()),
                "owner_buy_order_changed");
            ensure!(matches!(self.execution_order_origin(&current.order_id)?,
                Some(ExecutionOrderOrigin::OwnerTechnicalBuy{intent_id:id}) if id==intent.intent_id),
                "owner_buy_origin_changed");
            ensure!(!self.execution_canary_unresolved_buy()?,"unresolved_buy_dispatch");
            ensure!(!self.execution_canary_accounting_pending()?,"confirmed_accounting_pending");
            ensure!(self.execution_canary_receipt_submit_block_reason(&dispatch.order_id,&dispatch.token,"buy")?.is_none(),
                "dispatch_receipt_blocked");
            execution_canary_dispatch::insert(conn,dispatch,now)?;
            crate::tiny_experiment::reserve(conn,dispatch,Some(budget),now)?;
            execution_canary_dispatch::mark_submitted(conn,dispatch,now)?;
            ensure!(clock()?<intent.expires_at,"owner_buy_intent_expired");
            Ok(ExecutionDispatchClaim::New)
        })
    }
}

pub(crate) fn verify_tiny_budget_owner_buy(
    conn: &rusqlite::Connection,
    experiment_id: &str,
    dispatch: &ExecutionCanaryDispatch,
    budget: &TinyBudgetClaim,
) -> Result<()> {
    let installed = conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='owner_technical_buy_intents'")?
        .exists([])?;
    if !installed { return Ok(()); }
    let owner = conn.query_row("SELECT intent_id,wallet,mint,amount_lamports
        FROM owner_technical_buy_intents WHERE run_id=?1", [experiment_id],
        |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,
            r.get::<_,String>(2)?,r.get::<_,u64>(3)?))).optional()?;
    if let Some((id,wallet,mint,amount)) = owner {
        ensure!(dispatch.order_id == owner_technical_buy_order_id(&id)
            && dispatch.signal_id == owner_technical_buy_identity_id(&id)
            && dispatch.wallet == wallet && dispatch.token == mint
            && budget.buy_lamports == Some(amount), "owner_buy_budget_exclusive");
    }
    Ok(())
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}
fn budget_amount(b: &TinyBudgetClaim) -> Option<u64> {
    b.buy_lamports
        .or_else(|| b.protected_capital.as_ref().map(|v| v.requested_lamports))
}
fn active(i: &OwnerTechnicalBuyIntent, now: DateTime<Utc>) -> Result<()> {
    ensure!(
        now >= i.activated_at && now < i.expires_at,
        "owner_buy_intent_expired"
    );
    Ok(())
}
fn validate(i: &OwnerTechnicalBuyIntent) -> Result<()> {
    for (name, value) in [("intent_id", &i.intent_id), ("run_id", &i.run_id)] {
        ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_:.".contains(&b)),
            "owner_buy_invalid_{name}"
        );
    }
    ensure!(
        !i.wallet.is_empty()
            && i.wallet == i.signer
            && !i.genesis_hash.is_empty()
            && !i.mint.is_empty()
            && !i.route.is_empty()
            && i.activated_at < i.expires_at,
        "owner_buy_intent_identity"
    );
    ensure!(
        i.amount_lamports > 0
            && i.amount_lamports <= crate::tiny_experiment::TINY_BUY_LAMPORTS
            && i.max_priority_fee_lamports <= crate::tiny_experiment::TINY_PRIORITY_FEE
            && i.min_reserve_lamports > 0
            && i.max_slippage_bps <= 10_000
            && i.max_open_positions == 1
            && i.max_buy_count == 1,
        "owner_buy_intent_limits"
    );
    ensure!(
        i.authority_sha256.len() == 64 && i.authority_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "owner_buy_intent_authority_hash"
    );
    Ok(())
}
