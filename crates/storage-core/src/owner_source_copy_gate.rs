//! Read-only, receipt-bound SOURCE exit barrier. No observation becomes a fill.
use crate::{ExecutionOrderOrigin, SqliteStore, EXECUTION_STATUS_CANARY_CONFIRMED};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::association_delivery::{CandidateGeneration, Terminal};
use rusqlite::OptionalExtension;

pub struct SourceCopyGateBinding<'a> {
    pub source_wallet: &'a str,
    pub source_signature: &'a str,
    pub source_slot: u64,
    pub source_mint: &'a str,
    pub source_input_lamports: u64,
    pub source_output_raw: u64,
    pub source_decimals: u8,
    pub genesis_hash: &'a str,
    pub bot_wallet: &'a str,
    pub bot_run_id: &'a str,
    pub bot_policy_identity: &'a str,
    pub activated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedNativeBotBuy {
    pub order_id: String,
    pub tx_signature: String,
    pub position_id: String,
    pub amount_raw: u64,
    pub decimals: u8,
}

impl SqliteStore {
    /// One consistent live SQLite snapshot, including committed WAL. No cache,
    /// migrations, promotion, freshness fabrication or trade mutation is allowed.
    pub fn confirmed_native_bot_buy_for_source(
        &self,
        b: &SourceCopyGateBinding<'_>,
        now: DateTime<Utc>,
    ) -> Result<Option<ConfirmedNativeBotBuy>> {
        ensure!(self.is_read_only(), "source_copy_gate_requires_read_only");
        let tx = self
            .conn
            .unchecked_transaction()
            .context("source_copy_gate_snapshot")?;
        let result = self.source_copy_gate_snapshot(b, now)?;
        tx.commit()?;
        Ok(result)
    }

    fn source_copy_gate_snapshot(
        &self,
        b: &SourceCopyGateBinding<'_>,
        now: DateTime<Utc>,
    ) -> Result<Option<ConfirmedNativeBotBuy>> {
        let c = &self.conn;
        let Some(a) = crate::native_buy::cohort::load(c)? else {
            return Ok(None);
        };
        if b.source_wallet == b.bot_wallet
            || now < b.activated_at
            || now >= b.expires_at
            || a.run_id != b.bot_run_id
            || a.wallet_ids != [b.source_wallet]
            || a.policy_identity != b.bot_policy_identity
            || a.activated_at < b.activated_at
            || a.deadline > b.expires_at
            || now >= a.deadline
        {
            return Ok(None);
        }
        let Some(binding) = crate::native_buy::cohort::binding(c, b.source_signature)? else {
            return Ok(None);
        };
        let Some(d) = crate::native_buy::verify::load(c, b.source_signature)? else {
            return Ok(None);
        };
        let signal = format!("native-buy-v1:{}", b.source_signature);
        let Some(first) = crate::association_inbox::identity(c, b.source_signature)? else {
            return Ok(None);
        };
        let Some(Terminal::ProviderAsserted(terminal)) = &first.terminal else {
            return Ok(None);
        };
        let source = &first.admission.facts;
        let Some(amounts) = &source.exact_amounts else {
            return Ok(None);
        };
        let admitted = DateTime::parse_from_rfc3339(&d.admitted_at)?.with_timezone(&Utc);
        let finalized = d
            .finalized_at
            .as_deref()
            .map(DateTime::parse_from_rfc3339)
            .transpose()?;
        if d.late
            || d.signature != b.source_signature
            || d.signal_id != signal
            || d.decision_id != format!("native-buy-decision-v1:{}", b.source_signature)
            || d.wallet != b.source_wallet
            || d.mint != b.source_mint
            || u64::try_from(d.slot).ok() != Some(b.source_slot)
            || u64::try_from(d.amount).ok() != Some(b.source_input_lamports)
            || binding.run_id != a.run_id
            || binding.wallet != d.wallet
            || binding.mint != d.mint
            || binding.admitted_at != d.admitted_at
            || binding.policy_identity != a.policy_identity
            || admitted < a.activated_at
            || admitted >= a.deadline
            || admitted > now
            || finalized.is_none_or(|f| f < admitted || f > now)
            || d.finalized_slot != Some(d.slot)
            || first.conflict
            || first.recovery
            || first.candidate != CandidateGeneration::Unknown
            || first.first_session != d.session
            || u64::try_from(d.sequence).ok() != Some(first.first_sequence)
            || serde_json::to_string(&first.admission)? != d.admission
            || source.signature != b.source_signature
            || source.wallet != b.source_wallet
            || source.slot != b.source_slot
            || source.token_in != "So11111111111111111111111111111111111111112"
            || source.token_out != b.source_mint
            || source.program_fallback
            || terminal.signature != b.source_signature
            || terminal.slot != b.source_slot
            || terminal.blockhash.is_empty()
            || amounts.amount_in_raw != b.source_input_lamports.to_string()
            || amounts.amount_in_decimals != 9
            || amounts.amount_out_raw != b.source_output_raw.to_string()
            || amounts.amount_out_decimals != b.source_decimals
        {
            return Ok(None);
        }
        let epoch: Option<(String,u64,String,String,String)> = c.query_row(
            "SELECT session,processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fence_epochs WHERE epoch_id=?1",
            [binding.epoch_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        let Some((session, slot, sampled, genesis, policy)) = epoch else {
            return Ok(None);
        };
        let sampled = DateTime::parse_from_rfc3339(&sampled)?.with_timezone(&Utc);
        if session != d.session
            || slot == 0
            || slot >= b.source_slot
            || genesis != b.genesis_hash
            || policy != a.policy_identity
            || sampled < a.activated_at
            || sampled > admitted
        {
            return Ok(None);
        }
        let ids = c
            .prepare("SELECT order_id FROM orders WHERE signal_id=?1 LIMIT 2")?
            .query_map([&signal], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ids.len() != 1 {
            return Ok(None);
        }
        let id = &ids[0];
        let order = self
            .load_execution_canary_order(id)?
            .context("source_copy_gate_order")?;
        if order.status != EXECUTION_STATUS_CANARY_CONFIRMED
            || order.attempt != 1
            || order.route != "jupiter_swap_instructions"
            || self.execution_order_origin(id)?
                != Some(ExecutionOrderOrigin::Copy {
                    signal_id: signal.clone(),
                })
        {
            return Ok(None);
        }
        let Some(dispatch) = self.load_execution_canary_dispatch(id)? else {
            return Ok(None);
        };
        let Some(proof) = self.load_execution_canary_receipt_proof(id)? else {
            return Ok(None);
        };
        let Some(facts) = crate::receipt_facts_rows::load(c, id)? else {
            return Ok(None);
        };
        if dispatch.wallet != b.bot_wallet
            || dispatch.signal_id != signal
            || dispatch.token != b.source_mint
            || dispatch.side != "buy"
            || dispatch.attempt != 1
            || dispatch.route != order.route
            || dispatch.client_order_id != order.client_order_id
            || order.tx_signature.as_deref() != Some(dispatch.tx_signature.as_str())
            || proof.tx_signature != dispatch.tx_signature
            || facts.tx_signature != dispatch.tx_signature
            || proof.wallet_pubkey != b.bot_wallet
            || facts.wallet_pubkey != b.bot_wallet
            || proof.token != b.source_mint
            || facts.token != b.source_mint
            || proof.side != "buy"
            || facts.side != "buy"
            || proof.reason != "accounting_complete"
            || !matches!(
                proof.confirmation_status.as_str(),
                "confirmed" | "finalized"
            )
            || proof.slot != Some(facts.slot)
            || facts.slot < b.source_slot
            || proof.confirmed_at < a.activated_at
            || proof.confirmed_at >= a.deadline
            || proof.confirmed_at > now
            || facts.transaction_fee.is_none()
            || facts.fee_payer.as_deref() != Some(b.bot_wallet)
        {
            return Ok(None);
        }
        let Some(raw) = facts
            .token_delta
            .filter(|q| q.raw > 0 && q.decimals == b.source_decimals)
        else {
            return Ok(None);
        };
        let Some(position) =
            crate::execution_canary_position_open::load_open_position_by_token(c, b.source_mint)?
        else {
            return Ok(None);
        };
        if position.position_id != format!("exec-canary-pos:{id}")
            || position
                .qty_exact
                .is_none_or(|q| i128::from(q.raw()) != raw.raw || q.decimals() != raw.decimals)
        {
            return Ok(None);
        }
        let attribution = crate::execution_canary_buy_attribution::read_on_conn(c, b.source_mint)?;
        let crate::ExecutionCanaryBuyAttribution::Open(attribution) = attribution else {
            return Ok(None);
        };
        if !attribution.unproven_links.is_empty()
            || attribution.proven_contributors.len() != 1
            || !attribution.proven_contributors.iter().any(|p| {
                p.order_id == *id
                    && p.signal_id == signal
                    && p.source_wallet == b.source_wallet
                    && p.tx_signature == dispatch.tx_signature
            })
        {
            return Ok(None);
        }
        let Some(tiny) = crate::tiny_experiment::load(c)? else {
            return Ok(None);
        };
        if tiny.id != b.bot_run_id
            || tiny.wallet != b.bot_wallet
            || tiny.state != "active"
            || tiny.stop_reason.is_some()
            || tiny.buy_order_id.as_deref() != Some(id.as_str())
            || tiny.token.as_deref() != Some(b.source_mint)
            || tiny.position_id.as_deref() != Some(position.position_id.as_str())
            || self.execution_canary_accounting_pending()?
            || self.execution_canary_unresolved_buy()?
        {
            return Ok(None);
        }
        Ok(Some(ConfirmedNativeBotBuy {
            order_id: id.clone(),
            tx_signature: dispatch.tx_signature,
            position_id: position.position_id,
            amount_raw: u64::try_from(raw.raw)?,
            decimals: raw.decimals,
        }))
    }
}
