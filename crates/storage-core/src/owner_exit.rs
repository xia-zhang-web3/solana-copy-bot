//! One-position owner EXIT authority and durable, one-use SELL dispatch.
use crate::{
    execution_canary_dispatch, ExecutionCanaryDispatch, ExecutionCanaryOrder,
    ExecutionCanaryRecordOutcome, ExecutionCanaryReserveResult, ExecutionDispatchClaim,
    SqliteDiscoveryStore, TinyBudgetClaim, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_STATUS_CANARY_CANDIDATE, EXECUTION_STATUS_CANARY_SIMULATED,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

pub(crate) use crate::owner_exit_binding::{ensure_buy_binding, token_side, validate_sell_receipt};

pub const OWNER_EXIT_BUY_ORDER: &str =
    "exec-canary:owner-buy:copybot-owner-buy-20260924-04-usdc-01";
pub const OWNER_EXIT_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const OWNER_EXIT_RAW: u64 = 1_167_085;
pub const OWNER_EXIT_DECIMALS: u8 = 6;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnerExitIntent {
    pub intent_id: String,
    pub run_id: String,
    pub buy_order_id: String,
    pub buy_receipt_signature: String,
    pub position_id: String,
    pub wallet: String,
    pub signer: String,
    pub genesis_hash: String,
    pub mint: String,
    pub amount_raw: u64,
    pub decimals: u8,
    pub route: String,
    pub activated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub authority_sha256: String,
    pub max_priority_fee_lamports: u64,
    pub min_reserve_lamports: u64,
    pub max_slippage_bps: u32,
    pub max_daily_loss_lamports: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerExitIntentRecordOutcome {
    Inserted,
    Existing,
}

pub fn owner_exit_identity_id(id: &str) -> String {
    format!("owner-exit:{id}")
}
pub fn owner_exit_order_id(id: &str) -> String {
    format!("exec-canary:owner-exit:{id}")
}
pub fn owner_exit_client_order_id(id: &str) -> String {
    format!("copybot:owner-exit:{id}")
}

impl SqliteDiscoveryStore {
    pub fn register_owner_exit_intent(
        &self,
        intent: &OwnerExitIntent,
    ) -> Result<OwnerExitIntentRecordOutcome> {
        validate(intent)?;
        self.with_immediate_transaction_retry("register owner EXIT", |conn| {
            if let Some(old) = self.load_owner_exit_intent(&intent.intent_id)? {
                ensure!(old == *intent, "owner_exit_intent_conflict");
                return Ok(OwnerExitIntentRecordOutcome::Existing);
            }
            ensure_buy_binding(conn, intent)?;
            conn.execute("INSERT INTO owner_exit_intents(intent_id,run_id,buy_order_id,buy_receipt_signature,
                position_id,wallet,signer,genesis_hash,mint,amount_raw,decimals,route,activated_at,expires_at,
                authority_sha256,max_priority_fee_lamports,min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                params![intent.intent_id,intent.run_id,intent.buy_order_id,intent.buy_receipt_signature,
                    intent.position_id,intent.wallet,intent.signer,intent.genesis_hash,intent.mint,
                    i64::try_from(intent.amount_raw)?,i64::from(intent.decimals),intent.route,
                    intent.activated_at.to_rfc3339(),intent.expires_at.to_rfc3339(),intent.authority_sha256,
                    i64::try_from(intent.max_priority_fee_lamports)?,i64::try_from(intent.min_reserve_lamports)?,
                    i64::from(intent.max_slippage_bps),i64::try_from(intent.max_daily_loss_lamports)?])?;
            conn.execute("INSERT INTO execution_order_sources(identity_id,owner_exit_intent_id) VALUES(?1,?2)",
                params![owner_exit_identity_id(&intent.intent_id),intent.intent_id])?;
            Ok(OwnerExitIntentRecordOutcome::Inserted)
        })
    }

    pub fn load_owner_exit_intent(&self, id: &str) -> Result<Option<OwnerExitIntent>> {
        let row = self.conn.query_row("SELECT intent_id,run_id,buy_order_id,buy_receipt_signature,
            position_id,wallet,signer,genesis_hash,mint,amount_raw,decimals,route,activated_at,expires_at,
            authority_sha256,max_priority_fee_lamports,min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports
            FROM owner_exit_intents WHERE intent_id=?1", [id], |r| {
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

    pub fn list_owner_exit_intents(&self, limit: u32) -> Result<Vec<OwnerExitIntent>> {
        let ids = self.conn.prepare("SELECT intent_id FROM owner_exit_intents ORDER BY activated_at DESC,intent_id DESC LIMIT ?1")?
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
                "SELECT i.intent_id FROM owner_exit_intents i
            JOIN execution_order_sources s ON s.owner_exit_intent_id=i.intent_id
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

    pub fn reserve_owner_exit_order(
        &self,
        id: &str,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionCanaryReserveResult> {
        let order_id = owner_exit_order_id(id);
        let identity = owner_exit_identity_id(id);
        let client_id = owner_exit_client_order_id(id);
        let inserted=self.with_immediate_transaction_retry("reserve owner EXIT", |conn| {
            let now=clock()?;
            let intent=self.load_owner_exit_intent(id)?.context("owner_exit_intent_missing")?;
            active(&intent,now)?;
            ensure_buy_binding(conn,&intent)?;
            let source:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_order_sources
                WHERE identity_id=?1 AND owner_exit_intent_id=?2)",params![identity,id],|r|r.get(0))?;
            ensure!(source,"owner_exit_origin_missing");
            if let Some(old)=self.load_execution_canary_order(&order_id)? {
                ensure!(old.signal_id==identity && old.route==intent.route
                    && old.client_order_id==client_id && old.attempt==1,"owner_exit_order_conflict");
                return Ok(false);
            }
            conn.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt,simulation_status)
                VALUES(?1,?2,?3,?4,?5,?6,1,?7)",
                params![order_id,identity,intent.route,now.to_rfc3339(),
                    EXECUTION_STATUS_CANARY_CANDIDATE,client_id,EXECUTION_SIMULATION_STATUS_NOT_RUN])?;
            Ok(true)
        })?;
        Ok(ExecutionCanaryReserveResult {
            outcome: if inserted {
                ExecutionCanaryRecordOutcome::Inserted
            } else {
                ExecutionCanaryRecordOutcome::Existing
            },
            order: self
                .load_execution_canary_order(&order_id)?
                .context("owner_exit_order_missing")?,
        })
    }

    /// A crash before the durable dispatch claim cannot have sent through the
    /// daemon transport. Rebuild the same order with a fresh quote. A claimed
    /// dispatch or receipt is never rearmed here. One local failed attempt may
    /// be retried with a new quote under the original unexpired authority.
    pub fn rearm_owner_exit_undispatched(&self, id: &str) -> Result<bool> {
        let order_id = owner_exit_order_id(id);
        self.with_immediate_transaction_retry("rearm undispatched owner EXIT", |conn| {
            let intent = self.load_owner_exit_intent(id)?.context("owner_exit_intent_missing")?;
            ensure_buy_binding(conn, &intent)?;
            let order = self.load_execution_canary_order(&order_id)?
                .context("owner_exit_order_missing")?;
            ensure!(order.signal_id == owner_exit_identity_id(id)
                && order.client_order_id == owner_exit_client_order_id(id)
                && order.route == intent.route && order.attempt == 1,
                "owner_exit_rearm_identity");
            let safe_state = matches!(order.status.as_str(),
                EXECUTION_STATUS_CANARY_CANDIDATE
                | crate::EXECUTION_STATUS_CANARY_BUILT
                | EXECUTION_STATUS_CANARY_SIMULATED
                | crate::EXECUTION_STATUS_CANARY_FAILED);
            if !safe_state { return Ok(false); }
            let obligations: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM execution_canary_dispatch WHERE order_id=?1)
                 OR EXISTS(SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?1)
                 OR EXISTS(SELECT 1 FROM fills WHERE order_id=?1)
                 OR EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE order_id=?1)",
                [&order_id], |row| row.get(0))?;
            ensure!(!obligations && order.tx_signature.as_deref().is_none_or(str::is_empty),
                "owner_exit_rearm_has_obligation");
            if order.status == crate::EXECUTION_STATUS_CANARY_FAILED {
                ensure!(matches!(order.err_code.as_deref(),
                    Some(crate::EXECUTION_ERROR_BUILD_FAILED
                        | crate::EXECUTION_ERROR_SIMULATION_FAILED
                        | crate::EXECUTION_ERROR_SIGNING_ENVELOPE_FAILED
                        | crate::EXECUTION_ERROR_SUBMIT_PLAN_FAILED)),
                    "owner_exit_failed_reason_not_retryable");
                let used: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM owner_exit_pre_dispatch_rearms WHERE intent_id=?1)",
                    [id], |row| row.get(0))?;
                if used { return Ok(false); }
                conn.execute("INSERT INTO owner_exit_pre_dispatch_rearms(intent_id,count)
                    VALUES(?1,1)", [id])?;
            }
            if order.status != EXECUTION_STATUS_CANARY_CANDIDATE {
                conn.execute("UPDATE orders SET status=?2,simulation_status=?3,
                    simulation_error=NULL,err_code=NULL WHERE order_id=?1",
                    params![order_id, EXECUTION_STATUS_CANARY_CANDIDATE,
                        EXECUTION_SIMULATION_STATUS_NOT_RUN])?;
            }
            Ok(true)
        })
    }

    pub fn owner_exit_failed_retry_available(&self, id: &str) -> Result<bool> {
        let used: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM owner_exit_pre_dispatch_rearms WHERE intent_id=?1)",
            [id], |row| row.get(0))?;
        Ok(!used)
    }

    /// Existing claim is a reconciliation instruction, never another send permission.
    pub fn claim_owner_exit_dispatch(
        &self,
        expected: &ExecutionCanaryOrder,
        dispatch: &ExecutionCanaryDispatch,
        budget: &TinyBudgetClaim,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ExecutionDispatchClaim> {
        ensure!(
            dispatch.side == "sell" && dispatch.attempt == 1 && expected.attempt == 1,
            "owner_exit_dispatch_side_or_attempt"
        );
        self.with_immediate_transaction_retry("claim owner EXIT dispatch", |conn| {
            let id = dispatch
                .signal_id
                .strip_prefix("owner-exit:")
                .context("owner_exit_dispatch_origin")?;
            let intent = self
                .load_owner_exit_intent(id)?
                .context("owner_exit_intent_missing")?;
            ensure!(
                dispatch.order_id == owner_exit_order_id(id)
                    && dispatch.client_order_id == owner_exit_client_order_id(id)
                    && expected.order_id == dispatch.order_id
                    && expected.signal_id == dispatch.signal_id
                    && expected.client_order_id == dispatch.client_order_id
                    && expected.route == dispatch.route
                    && dispatch.route == intent.route
                    && dispatch.wallet == intent.wallet
                    && dispatch.token == intent.mint
                    && intent.signer == intent.wallet
                    && budget.experiment_id == intent.run_id
                    && budget.wallet == intent.wallet
                    && budget.tx_signature == dispatch.tx_signature
                    && budget.message_sha256 == dispatch.message_sha256
                    && budget.transaction_sha256 == dispatch.transaction_sha256
                    && budget.buy_lamports == Some(0)
                    && budget.protected_capital.is_none()
                    && budget.total_fee <= crate::tiny_experiment::TINY_TRANSACTION_FEE
                    && budget.priority_fee <= intent.max_priority_fee_lamports
                    && budget.priority_fee <= crate::tiny_experiment::TINY_PRIORITY_FEE
                    && budget.priority_fee <= budget.total_fee,
                "owner_exit_dispatch_identity_or_budget"
            );
            ensure!(
                !dispatch.tx_signature.is_empty()
                    && dispatch.message_sha256.len() == 64
                    && dispatch.transaction_sha256.len() == 64,
                "owner_exit_dispatch_binding_missing"
            );
            if let Some(old) = self.load_execution_canary_dispatch(&dispatch.order_id)? {
                ensure!(old == *dispatch, "owner_exit_dispatch_conflict");
                crate::owner_exit_fee::existing(conn, &intent, dispatch, budget)?;
                return Ok(ExecutionDispatchClaim::Existing);
            }
            let now = clock()?;
            active(&intent, now)?;
            ensure_buy_binding(conn, &intent)?;
            let current = self
                .load_execution_canary_order(&expected.order_id)?
                .context("owner_exit_order_missing")?;
            ensure!(
                current == *expected
                    && current.status == EXECUTION_STATUS_CANARY_SIMULATED
                    && current
                        .tx_signature
                        .as_deref()
                        .is_none_or(|s| s.trim().is_empty()),
                "owner_exit_order_changed"
            );
            ensure!(
                matches!(self.execution_order_origin(&current.order_id)?,
                Some(crate::ExecutionOrderOrigin::OwnerExit{intent_id:origin}) if origin==id),
                "owner_exit_origin_changed"
            );
            ensure!(
                self.execution_canary_receipt_submit_block_reason(
                    &dispatch.order_id,
                    &dispatch.token,
                    "sell"
                )?
                .is_none(),
                "owner_exit_receipt_blocked"
            );
            crate::owner_exit_fee::claim(conn, &intent, dispatch, budget)?;
            execution_canary_dispatch::insert(conn, dispatch, now)?;
            execution_canary_dispatch::mark_submitted(conn, dispatch, now)?;
            ensure!(clock()? < intent.expires_at, "owner_exit_intent_expired");
            Ok(ExecutionDispatchClaim::New)
        })
    }
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}
fn active(i: &OwnerExitIntent, now: DateTime<Utc>) -> Result<()> {
    ensure!(
        now >= i.activated_at && now < i.expires_at,
        "owner_exit_intent_expired"
    );
    Ok(())
}
fn validate(i: &OwnerExitIntent) -> Result<()> {
    for value in [&i.intent_id, &i.run_id] {
        ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_:.".contains(&b)),
            "owner_exit_invalid_id"
        );
    }
    ensure!(
        i.buy_order_id == OWNER_EXIT_BUY_ORDER
            && i.position_id == format!("exec-canary-pos:{}", OWNER_EXIT_BUY_ORDER)
            && !i.buy_receipt_signature.is_empty()
            && i.wallet == i.signer
            && !i.wallet.is_empty()
            && !i.genesis_hash.is_empty()
            && i.mint == OWNER_EXIT_MINT
            && i.amount_raw == OWNER_EXIT_RAW
            && i.decimals == OWNER_EXIT_DECIMALS
            && !i.route.is_empty()
            && i.activated_at < i.expires_at
            && i.max_priority_fee_lamports <= crate::tiny_experiment::TINY_PRIORITY_FEE
            && i.min_reserve_lamports > 0
            && i.max_slippage_bps <= 10_000,
        "owner_exit_intent_identity_or_limits"
    );
    ensure!(
        i.authority_sha256.len() == 64 && i.authority_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "owner_exit_authority_hash"
    );
    Ok(())
}
