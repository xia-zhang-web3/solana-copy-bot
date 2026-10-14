//! Recovery of one already-accounted native BUY. No admission, BUY or age renewal.
use super::{cohort, verify};
use crate::{OwnerExitIntent, SqliteStore};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::association_delivery::{CandidateGeneration, Terminal};
use rusqlite::{params, Connection};

pub(crate) fn validate_parent(c: &Connection, i: &OwnerExitIntent) -> Result<()> {
    let a = cohort::load(c)?.context("native_recovery_cohort_missing")?;
    let experiment = crate::tiny_experiment::load(c)?.context("native_recovery_budget_missing")?;
    let floor: (String,String,u64)=c.query_row(
        "SELECT wallet,floor_lamports,allowance FROM execution_tiny_native_policy WHERE experiment_id=?1",
        [&a.run_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let fresh = i.activated_at != a.activated_at || i.expires_at != a.deadline;
    ensure!(
        i.run_id == a.run_id
            && experiment.id == a.run_id
            && experiment.wallet == i.wallet
            && experiment.deadline == a.deadline
            && experiment.buy_order_id.as_deref() == Some(&i.buy_order_id)
            && experiment.token.as_deref() == Some(&i.mint)
            && experiment.position_id.as_deref() == Some(&i.position_id)
            && (experiment.state == "active" || (fresh && experiment.state == "stopped"
                && experiment.stop_reason.as_deref() == Some("tiny_budget_deadline")))
            && floor.0 == i.wallet
            && floor.2 == crate::TINY_NATIVE_ALLOWANCE
            && i.min_reserve_lamports >= floor.1.parse::<u64>()?,
        "native_recovery_original_budget_changed"
    );
    if i.activated_at == a.activated_at && i.expires_at == a.deadline {
        // Existing645 recovery remains confined to its original window.
    } else {
        let original = crate::OwnerNativeRecoveryOriginal {
            activated_at: a.activated_at, expires_at: a.deadline,
            policy_identity: a.policy_identity.clone(),
        };
        ensure!(i.authority_sha256 == crate::owner_native_recovery_authority_sha256(i, &original)?,
            "native_recovery_original_authority_changed");
    }
    ensure_binding(
        c,
        &i.buy_order_id,
        &i.buy_receipt_signature,
        &i.wallet,
        &i.genesis_hash,
        &i.mint,
        &i.position_id,
        i.amount_raw,
        i.decimals,
    )
}

impl SqliteStore {
    pub fn validate_owner_native_recovery_exit(
        &self,
        i: &OwnerExitIntent,
        now: DateTime<Utc>,
    ) -> Result<()> {
        ensure!(
            i.intent_id
                == format!(
                    "native-recovery:{}",
                    i.buy_order_id
                        .strip_prefix("exec-canary:native-buy-v1:")
                        .context("native_recovery_buy_identity")?
                ),
            "native_recovery_exit_identity"
        );
        validate_parent(&self.conn, i)?;
        ensure!(
            now >= i.activated_at && now < i.expires_at,
            "native_recovery_original_deadline"
        );
        Ok(())
    }
}

pub(crate) fn ensure_binding(
    c: &Connection,
    buy: &str,
    signature: &str,
    wallet: &str,
    genesis: &str,
    mint: &str,
    position: &str,
    raw: u64,
    decimals: u8,
) -> Result<()> {
    let source = buy
        .strip_prefix("exec-canary:native-buy-v1:")
        .context("native_recovery_buy_origin")?;
    let a = cohort::load(c)?.context("native_recovery_cohort_missing")?;
    let b = cohort::binding(c, source)?.context("native_recovery_cohort_binding_missing")?;
    let d = verify::load(c, source)?.context("native_recovery_decision_missing")?;
    let first = crate::association_inbox::identity(c, source)?
        .context("native_recovery_admission_missing")?;
    let Terminal::ProviderAsserted(terminal) = first
        .terminal
        .as_ref()
        .context("native_recovery_terminal_missing")?
    else {
        anyhow::bail!("native_recovery_terminal_unproven")
    };
    let facts = &first.admission.facts;
    let amounts = facts
        .exact_amounts
        .as_ref()
        .context("native_recovery_source_amount_missing")?;
    let admitted = DateTime::parse_from_rfc3339(&d.admitted_at)?.with_timezone(&Utc);
    let finalized = DateTime::parse_from_rfc3339(
        d.finalized_at
            .as_deref()
            .context("native_recovery_source_unfinalized")?,
    )?
    .with_timezone(&Utc);
    ensure!(
        !source.is_empty()
            && d.signature == source
            && d.signal_id == format!("native-buy-v1:{source}")
            && d.decision_id == format!("native-buy-decision-v1:{source}")
            && !d.late
            && d.slot > 0
            && d.wallet != wallet
            && a.wallet_ids == [d.wallet.clone()]
            && d.mint == mint
            && d.amount == 10_000_000
            && a.max_buy_count == 1
            && a.mint_policy == super::CLASSIC_SPL_MINT_POLICY
            && b.run_id == a.run_id
            && b.wallet == d.wallet
            && b.mint == d.mint
            && b.admitted_at == d.admitted_at
            && b.policy_identity == a.policy_identity
            && admitted >= a.activated_at
            && admitted < a.deadline
            && finalized >= admitted
            && finalized < a.deadline
            && d.finalized_slot == Some(d.slot)
            && !first.conflict
            && !first.recovery
            && first.candidate == CandidateGeneration::Unknown
            && first.first_session == d.session
            && u64::try_from(d.sequence).ok() == Some(first.first_sequence)
            && serde_json::to_string(&first.admission)? == d.admission
            && facts.signature == source
            && facts.wallet == d.wallet
            && facts.slot == d.slot as u64
            && facts.token_in == "So11111111111111111111111111111111111111112"
            && facts.token_out == mint
            && !facts.program_fallback
            && terminal.signature == source
            && terminal.slot == facts.slot
            && !terminal.blockhash.is_empty()
            && terminal.blockhash.len() <= 128
            && amounts.amount_in_raw == "10000000"
            && amounts.amount_in_decimals == 9
            && amounts.amount_out_raw.parse::<u64>()? > 0
            && amounts.amount_out_decimals == decimals,
        "native_recovery_source_parent_changed"
    );
    let epoch:(String,u64,String,String,String)=c.query_row(
        "SELECT session,processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fence_epochs WHERE epoch_id=?1",
        [b.epoch_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    let sampled = DateTime::parse_from_rfc3339(&epoch.2)?.with_timezone(&Utc);
    ensure!(
        epoch.0 == d.session
            && epoch.1 > 0
            && epoch.1 < facts.slot
            && epoch.3 == genesis
            && epoch.4 == a.policy_identity
            && sampled >= a.activated_at
            && sampled <= admitted
            && admitted - sampled <= Duration::seconds(120),
        "native_recovery_original_fence_changed"
    );
    let receipt =
        crate::receipt_facts_rows::load(c, buy)?.context("native_recovery_buy_facts_missing")?;
    ensure!(
        receipt.order_id == buy
            && receipt.tx_signature == signature
            && receipt.wallet_pubkey == wallet
            && receipt.token == mint
            && receipt.side == "buy"
            && receipt.slot >= facts.slot
            && receipt.wallet_is_fee_payer() == Some(true)
            && receipt
                .transaction_fee
                .is_some_and(|f| f.as_u64() > 0 && f.as_u64() <= 100000)
            && receipt.wallet_native_delta.as_i128() < 0
            && receipt
                .token_delta
                .is_some_and(|q| q.raw == i128::from(raw) && q.decimals == decimals),
        "native_recovery_typed_buy_receipt_changed"
    );
    // Exact original order/dispatch/receipt/fill/cost/position. No lot or basis is inserted or rewritten.
    let bound:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM orders o
        JOIN execution_order_sources s ON s.identity_id=o.signal_id
        JOIN execution_canary_dispatch d ON d.order_id=o.order_id
        JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        JOIN execution_canary_receipt_facts f ON f.order_id=o.order_id
        JOIN fills v ON v.order_id=o.order_id JOIN positions x ON x.position_id=v.position_id
        JOIN execution_tiny_reservations tr ON tr.order_id=o.order_id
        JOIN execution_tiny_capital_evidence ce ON ce.order_id=o.order_id
        JOIN execution_tiny_native_policy np ON np.experiment_id=tr.experiment_id
        WHERE o.order_id=?1 AND o.signal_id=?8 AND s.copy_signal_id=o.signal_id
        AND s.owner_buy_intent_id IS NULL AND s.owner_exit_intent_id IS NULL AND s.owned_sell_intent_id IS NULL
        AND o.status='execution_canary_confirmed' AND o.attempt=1 AND o.route='jupiter_swap_instructions'
        AND o.client_order_id=d.client_order_id AND o.signal_id=d.signal_id AND o.route=d.route
        AND o.tx_signature=?2 AND d.tx_signature=?2 AND p.tx_signature=?2 AND f.tx_signature=?2
        AND d.wallet=?3 AND p.wallet_pubkey=?3 AND f.wallet_pubkey=?3 AND d.side='buy' AND d.attempt=1
        AND d.token=?4 AND p.token=?4 AND f.token=?4 AND v.token=?4 AND x.token=?4
        AND p.side='buy' AND f.side='buy' AND p.reason='accounting_complete'
        AND p.confirmation_status IN ('confirmed','finalized') AND p.slot=f.slot
        AND CAST(f.slot AS INTEGER)>=?9 AND f.fee_payer=?3 AND CAST(f.transaction_fee AS INTEGER)>0
        AND CAST(f.transaction_fee AS INTEGER)<=100000 AND CAST(f.wallet_native_delta AS INTEGER)<0
        AND f.token_delta_raw=?5 AND f.token_decimals=?6 AND f.token_coverage!='unresolved'
        AND v.qty_raw=?5 AND v.qty_decimals=?6 AND x.qty_raw=?5 AND x.qty_decimals=?6
        AND tr.experiment_id=?10 AND tr.wallet=?3 AND tr.side='buy' AND tr.tx_signature=?2
        AND tr.message_sha256=d.message_sha256 AND tr.transaction_sha256=d.transaction_sha256
        AND tr.buy_lamports IS NULL AND ce.requested_lamports=10000000
        AND ce.floor_lamports=CAST(np.floor_lamports AS INTEGER) AND length(ce.request_sha256)=64
        AND ce.request_sha256 NOT GLOB '*[^a-fA-F0-9]*'
        AND tr.actual_fee=CAST(f.transaction_fee AS INTEGER)
        AND tr.outcome='successful' AND tr.fee_bound<=100000 AND tr.priority_fee<=50000
        AND v.accounting_basis='legacy_unclassified' AND v.notional_lamports=-CAST(f.wallet_native_delta AS INTEGER)
        AND x.cost_lamports=v.notional_lamports AND x.position_id=?7 AND x.state='open'
        AND x.accounting_bucket='execution_canary'
        AND NOT EXISTS(SELECT 1 FROM fills other WHERE other.position_id=x.position_id AND other.order_id!=o.order_id AND other.order_id!=?11)
        AND (SELECT count(*) FROM orders same WHERE same.signal_id=o.signal_id)=1)",
        params![buy,signature,wallet,mint,raw.to_string(),decimals,position,d.signal_id,d.slot,a.run_id,crate::owner_exit_order_id(&format!("native-recovery:{source}"))],|r|r.get(0))?;
    ensure!(
        bound && position == format!("exec-canary-pos:{buy}"),
        "native_recovery_buy_receipt_position_changed"
    );
    let confirmed: String = c.query_row(
        "SELECT confirmed_at FROM execution_canary_receipt_proofs WHERE order_id=?1",
        [buy],
        |r| r.get(0),
    )?;
    let confirmed = DateTime::parse_from_rfc3339(&confirmed)?.with_timezone(&Utc);
    ensure!(
        confirmed >= a.activated_at && confirmed < a.deadline,
        "native_recovery_buy_confirmation_window"
    );
    Ok(())
}
