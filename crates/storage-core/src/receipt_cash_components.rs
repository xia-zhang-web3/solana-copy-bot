//! Additive classification of confirmed receipt cash. The original fill remains a
//! wallet cash basis; a component is known only with matching receipt evidence.
use crate::{
    native_observations::storage, receipt_facts_identity, receipt_facts_rows,
    ExecutionCanaryReceiptFacts, NativeAccountObservations, NativeInstructionObservation,
    SqliteDiscoveryStore,
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

const SYSTEM: &str = "11111111111111111111111111111111";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const WSOL: &str = "So11111111111111111111111111111111111111112";

/// Signed swap cash is negative for BUY and positive for SELL. Rent is the
/// confirmed change in the target token account's lamports, not a trading loss.
/// None means unproven, including a zero that has not been independently shown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptCashComponents {
    pub order_id: String,
    pub tx_signature: String,
    pub side: String,
    pub wallet_native_delta_lamports: String,
    pub swap_native_delta_lamports: Option<String>,
    pub transaction_fee_lamports: Option<String>,
    pub priority_fee_lamports: Option<String>,
    pub target_ata_rent_delta_lamports: Option<String>,
    pub unclassified_native_delta_lamports: Option<String>,
    pub classification: String,
}

impl SqliteDiscoveryStore {
    /// Safe for a copied historical DB: creates additive evidence only, never
    /// changes a BUY fill, position basis, receipt or completed order.
    pub fn materialize_receipt_cash_components(
        &self,
        order_id: &str,
        recorded_at: DateTime<Utc>,
    ) -> Result<ReceiptCashComponents> {
        self.with_immediate_transaction_retry("receipt cash components", |conn| {
            let facts = receipt_facts_rows::load(conn, order_id)?
                .ok_or_else(|| anyhow::anyhow!("cash components receipt missing"))?;
            receipt_facts_identity::validate_identity(conn, &facts)?;
            let observations = storage::load(conn, order_id)?
                .ok_or_else(|| anyhow::anyhow!("cash components observations missing"))?;
            ensure!(observations.1.is_none(), "cash components observations conflict");
            let buy_amount = if facts.side == "buy" {
                conn.query_row("SELECT i.amount_lamports FROM orders o JOIN owner_technical_buy_intents i ON o.signal_id='owner-buy:'||i.intent_id WHERE o.order_id=?1", [order_id], |r| r.get::<_, i64>(0)).optional()?
                    .and_then(|n| u64::try_from(n).ok())
            } else { None };
            let mut derived = derive_receipt_cash_components(&facts, &observations.0, buy_amount)?;
            derived.priority_fee_lamports = crate::receipt_priority_fee::confirmed_priority_fee(conn, &facts,
                derived.transaction_fee_lamports.as_deref())?
                .map(|value| value.to_string());
            let old: Option<String> = conn.query_row(
                "SELECT components_json FROM execution_receipt_cash_components WHERE order_id=?1",
                [order_id], |r| r.get(0),
            ).optional()?;
            let stored = if let Some(old) = old {
                let old: ReceiptCashComponents = serde_json::from_str(&old)?;
                merge_known(old, derived)?
            } else { derived };
            conn.execute("INSERT INTO execution_receipt_cash_components(order_id,tx_signature,components_json,recorded_at) VALUES(?1,?2,?3,?4) ON CONFLICT(order_id) DO UPDATE SET components_json=excluded.components_json",
                params![order_id, stored.tx_signature, serde_json::to_string(&stored)?, recorded_at.to_rfc3339()])?;
            Ok(stored)
        })
    }

    pub fn load_receipt_cash_components(
        &self,
        order_id: &str,
    ) -> Result<Option<ReceiptCashComponents>> {
        let row: Option<(String, String)> = self.conn.query_row(
            "SELECT tx_signature,components_json FROM execution_receipt_cash_components WHERE order_id=?1",
            [order_id], |r| Ok((r.get(0)?,r.get(1)?)),
        ).optional()?;
        row.map(|(signature, json)| {
            let value: ReceiptCashComponents = serde_json::from_str(&json)?;
            let facts = receipt_facts_rows::load(&self.conn, order_id)?
                .ok_or_else(|| anyhow::anyhow!("cash components receipt missing"))?;
            ensure!(
                value.order_id == order_id
                    && value.tx_signature == signature
                    && value.tx_signature == facts.tx_signature
                    && value.side == facts.side
                    && value.wallet_native_delta_lamports
                        == facts.wallet_native_delta.as_i128().to_string(),
                "cash components identity conflict"
            );
            Ok(value)
        })
        .transpose()
    }
}

fn merge_known(
    old: ReceiptCashComponents,
    new: ReceiptCashComponents,
) -> Result<ReceiptCashComponents> {
    ensure!(
        old.order_id == new.order_id
            && old.tx_signature == new.tx_signature
            && old.side == new.side
            && old.wallet_native_delta_lamports == new.wallet_native_delta_lamports,
        "cash components immutable identity conflict"
    );
    let mut out = old;
    for (left, right) in [
        (
            &mut out.swap_native_delta_lamports,
            new.swap_native_delta_lamports,
        ),
        (
            &mut out.transaction_fee_lamports,
            new.transaction_fee_lamports,
        ),
        (&mut out.priority_fee_lamports, new.priority_fee_lamports),
        (
            &mut out.target_ata_rent_delta_lamports,
            new.target_ata_rent_delta_lamports,
        ),
        (
            &mut out.unclassified_native_delta_lamports,
            new.unclassified_native_delta_lamports,
        ),
    ] {
        if let (Some(a), Some(b)) = (left.as_ref(), right.as_ref()) {
            ensure!(a == b, "cash components known value conflict");
        } else if left.is_none() {
            *left = right;
        }
    }
    out.classification = if out.unclassified_native_delta_lamports.as_deref() == Some("0") {
        "decomposed"
    } else {
        "partial_unknown"
    }
    .into();
    Ok(out)
}

/// The observation bundle is bound to a successful confirmed getTransaction
/// receipt. Unsupported Jupiter instructions do not turn unknown cash into zero.
pub fn derive_receipt_cash_components(
    facts: &ExecutionCanaryReceiptFacts,
    observations: &NativeAccountObservations,
    owner_buy_amount: Option<u64>,
) -> Result<ReceiptCashComponents> {
    facts.validate()?;
    observations.validate()?;
    ensure!(
        observations.order_id == facts.order_id
            && observations.tx_signature == facts.tx_signature
            && observations.wallet_pubkey == facts.wallet_pubkey
            && observations.token == facts.token
            && observations.side == facts.side
            && observations.slot == facts.slot.to_string(),
        "cash components receipt binding mismatch"
    );
    let fee = if facts.wallet_is_fee_payer() == Some(true) {
        facts.transaction_fee.map(|v| v.as_u64())
    } else {
        None
    };
    let rent = target_ata_rent(facts, observations);
    let swap = if facts.side == "buy" {
        owner_buy_amount
            .filter(|amount| confirmed_buy_input(facts, observations, *amount))
            .map(|amount| -i128::from(amount))
    } else {
        confirmed_sell_output(facts, observations).map(i128::from)
    };
    let unclassified = match (fee, rent, swap) {
        (Some(fee), Some(rent), Some(swap)) => {
            // wallet = swap - fee - rent_deposit (+ rent_refund). A nonzero
            // residual is explicit other cash, not silently called exchange.
            Some(facts.wallet_native_delta.as_i128() - swap + i128::from(fee) + rent)
        }
        _ => None,
    };
    Ok(ReceiptCashComponents {
        order_id: facts.order_id.clone(),
        tx_signature: facts.tx_signature.clone(),
        side: facts.side.clone(),
        wallet_native_delta_lamports: facts.wallet_native_delta.as_i128().to_string(),
        swap_native_delta_lamports: swap.map(|n| n.to_string()),
        transaction_fee_lamports: fee.map(|n| n.to_string()),
        // The confirmed total transaction fee does not by itself prove its
        // base/priority split. No hint or quote is substituted here.
        priority_fee_lamports: None,
        target_ata_rent_delta_lamports: rent.map(|n| n.to_string()),
        unclassified_native_delta_lamports: unclassified.map(|n| n.to_string()),
        classification: if unclassified == Some(0) {
            "decomposed"
        } else {
            "partial_unknown"
        }
        .into(),
    })
}

fn field<'a>(ix: &'a NativeInstructionObservation, key: &str) -> Option<&'a str> {
    ix.fields.get(key)?.value.as_deref()
}
fn ix_is(ix: &NativeInstructionObservation, program: &str, kind: &str) -> bool {
    ix.program_id.value.as_deref() == Some(program)
        && ix.instruction_type.value.as_deref() == Some(kind)
        && ix.coverage == crate::ObservationCoverage::Known
}
fn count_ix(
    obs: &NativeAccountObservations,
    pred: impl Fn(&NativeInstructionObservation) -> bool,
) -> usize {
    obs.instructions.iter().filter(|ix| pred(ix)).count()
}
fn target_ata_rent(f: &ExecutionCanaryReceiptFacts, o: &NativeAccountObservations) -> Option<i128> {
    let accounts: Vec<_> = o
        .accounts
        .iter()
        .filter(|a| {
            [&a.pre_token, &a.post_token].iter().any(|end| {
                end.mint.value.as_deref() == Some(&f.token)
                    && end.token_owner.value.as_deref() == Some(&f.wallet_pubkey)
                    && matches!(end.token_program.value.as_deref(), Some(TOKEN | TOKEN_2022))
            })
        })
        .collect();
    if accounts.len() != 1 {
        return None;
    }
    let a = accounts[0];
    let pre = a.native_pre.value.as_ref()?.parse::<u64>().ok()?;
    let post = a.native_post.value.as_ref()?.parse::<u64>().ok()?;
    let delta = i128::from(post) - i128::from(pre);
    if delta == 0 {
        return Some(0);
    }
    if delta > 0 && pre == 0 {
        let matched = count_ix(o, |ix| {
            ix_is(ix, SYSTEM, "createAccount")
                && field(ix, "source") == Some(&f.wallet_pubkey)
                && field(ix, "newAccount") == Some(&a.pubkey)
                && field(ix, "lamports") == Some(&post.to_string())
        });
        let ata = count_ix(o, |ix| {
            ix_is(ix, ATA, "createIdempotent")
                && field(ix, "account") == Some(&a.pubkey)
                && field(ix, "wallet") == Some(&f.wallet_pubkey)
                && field(ix, "mint") == Some(&f.token)
        });
        if matched == 1 && ata == 1 {
            return Some(delta);
        }
    }
    if delta < 0 && post == 0 {
        let closed = count_ix(o, |ix| {
            matches!(ix.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
                && ix.instruction_type.value.as_deref() == Some("closeAccount")
                && field(ix, "account") == Some(&a.pubkey)
                && field(ix, "destination") == Some(&f.wallet_pubkey)
                && field(ix, "owner") == Some(&f.wallet_pubkey)
        });
        if closed == 1 {
            return Some(delta);
        }
    }
    None
}

fn confirmed_buy_input(
    f: &ExecutionCanaryReceiptFacts,
    o: &NativeAccountObservations,
    amount: u64,
) -> bool {
    if amount == 0 || f.token_delta.is_none_or(|d| d.raw <= 0) {
        return false;
    }
    let amount = amount.to_string();
    let funded: Vec<_> = o
        .instructions
        .iter()
        .filter(|ix| {
            ix_is(ix, SYSTEM, "transfer")
                && field(ix, "source") == Some(&f.wallet_pubkey)
                && field(ix, "lamports") == Some(&amount)
        })
        .collect();
    if funded.len() != 1 {
        return false;
    }
    let Some(account) = field(funded[0], "destination") else {
        return false;
    };
    count_ix(o, |ix| {
        ix_is(ix, ATA, "createIdempotent")
            && field(ix, "account") == Some(account)
            && field(ix, "wallet") == Some(&f.wallet_pubkey)
            && field(ix, "mint") == Some(WSOL)
    }) == 1
        && count_ix(o, |ix| {
            matches!(ix.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
                && ix.instruction_type.value.as_deref() == Some("transfer")
                && field(ix, "source") == Some(account)
                && field(ix, "authority") == Some(&f.wallet_pubkey)
                && field(ix, "amount") == Some(&amount)
        }) == 1
        && count_ix(o, |ix| {
            matches!(ix.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
                && ix.instruction_type.value.as_deref() == Some("closeAccount")
                && field(ix, "account") == Some(account)
                && field(ix, "destination") == Some(&f.wallet_pubkey)
        }) == 1
}

fn confirmed_sell_output(
    f: &ExecutionCanaryReceiptFacts,
    o: &NativeAccountObservations,
) -> Option<u64> {
    if f.token_delta.is_none_or(|d| d.raw >= 0) {
        return None;
    }
    let mut values = Vec::new();
    for ix in &o.instructions {
        if !matches!(ix.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
            || ix.instruction_type.value.as_deref() != Some("transfer")
        {
            continue;
        }
        let Some(account) = field(ix, "destination") else {
            continue;
        };
        let owned = count_ix(o, |other| {
            ix_is(other, TOKEN, "initializeAccount3")
                && field(other, "account") == Some(account)
                && field(other, "mint") == Some(WSOL)
                && field(other, "owner") == Some(&f.wallet_pubkey)
        }) == 1;
        let closed = count_ix(o, |other| {
            matches!(other.program_id.value.as_deref(), Some(TOKEN | TOKEN_2022))
                && other.instruction_type.value.as_deref() == Some("closeAccount")
                && field(other, "account") == Some(account)
                && field(other, "destination") == Some(&f.wallet_pubkey)
        }) == 1;
        if owned && closed {
            if let Some(n) = field(ix, "amount").and_then(|s| s.parse::<u64>().ok()) {
                values.push(n);
            }
        }
    }
    if values.len() == 1 {
        Some(values[0])
    } else {
        None
    }
}
