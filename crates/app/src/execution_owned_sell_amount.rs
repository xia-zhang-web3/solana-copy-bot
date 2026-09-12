//! Exact owned quantity provenance for an unsigned SELL attempt, never a BUY receipt amount.
use super::{on_order, refused, Snapshot};
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionSubmitRequest};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{ExecutionCanaryOwnedPosition, SqliteStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Position {
    id: String,
    token: String,
    bucket: String,
    opened_ts: DateTime<Utc>,
    raw: u64,
    decimals: u8,
}
impl Position {
    fn read(p: &ExecutionCanaryOwnedPosition) -> Result<Self> {
        let q = p
            .qty_exact
            .filter(|q| q.raw() > 0)
            .ok_or_else(|| refused(&p.token, "source_sell_amount_exact_missing"))?;
        Ok(Self {
            id: p.position_id.clone(),
            token: p.token.clone(),
            bucket: p.accounting_bucket.clone(),
            opened_ts: p.opened_ts,
            raw: q.raw(),
            decimals: q.decimals(),
        })
    }
    fn recheck(&self, store: &SqliteStore) -> Result<()> {
        let current = store
            .load_execution_canary_open_position(&self.token)?
            .ok_or_else(|| refused(&self.token, "source_sell_amount_position_missing"))?;
        if Self::read(&current)? != *self {
            return Err(refused(&self.token, "source_sell_amount_stale"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Proof {
    version: u8,
    position: Position,
    order_id: String,
    signal_id: String,
    client_order_id: String,
    attempt: u32,
    route: String,
    source_wallet: String,
    execution_wallet: String,
    wallet_raw: u64,
    selected_raw: u64,
    quote_hash: String,
}

/// Captured before wallet/quote awaits; cannot be replaced by a fresh snapshot of cached inputs.
pub(crate) struct Selection {
    position: Position,
}
impl Selection {
    pub(crate) fn new(p: &ExecutionCanaryOwnedPosition) -> Result<Self> {
        Ok(Self {
            position: Position::read(p)?,
        })
    }
    pub(crate) fn recheck(&self, store: &SqliteStore) -> Result<()> {
        self.position.recheck(store)
    }
    pub(crate) fn amount(&self, wallet_raw: u64, decimals: u8) -> Result<u64> {
        if decimals != self.position.decimals || wallet_raw == 0 {
            return Err(refused(
                &self.position.token,
                "source_sell_amount_wallet_conflict",
            ));
        }
        Ok(self.position.raw.min(wallet_raw))
    }
    pub(crate) fn finish(
        &self,
        mut metadata: ExecutionBuildPlanMetadata,
        source: Option<&Snapshot>,
        execution_wallet: &str,
        wallet_raw: u64,
        selected_raw: u64,
    ) -> Result<ExecutionBuildPlanMetadata> {
        validate_quote(&metadata, selected_raw, &self.position.token)?;
        metadata.owned_sell_amount = match source.and_then(|s| s.order.as_ref().map(|o| (s, o))) {
            Some((s, o)) => Some(Proof {
                version: 1,
                position: self.position.clone(),
                order_id: o.order_id.clone(),
                signal_id: o.signal_id.clone(),
                client_order_id: o.client_order_id.clone(),
                attempt: o.attempt,
                route: o.route.clone(),
                source_wallet: s.signal.wallet_id.clone(),
                execution_wallet: execution_wallet.into(),
                wallet_raw,
                selected_raw,
                quote_hash: quote_hash(&metadata)?,
            }),
            None => None,
        };
        Ok(metadata)
    }
}

fn quote_hash(m: &ExecutionBuildPlanMetadata) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        &m.quote_source,
        &m.quote_event_id,
        m.quote_request_ts,
        m.http_request_started_ts,
        &m.quote_status,
        &m.quote_in_amount_raw,
        &m.quote_out_amount_raw,
        &m.quote_response_json,
        &m.route_plan_json,
    ))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn validate_quote(m: &ExecutionBuildPlanMetadata, raw: u64, id: &str) -> Result<()> {
    let expected = raw.to_string();
    let value = m
        .quote_response_json
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
    let value = value.as_ref().map(|v| v.get("quote").unwrap_or(v));
    if raw == 0
        || m.quote_in_amount_raw.as_deref() != Some(expected.as_str())
        || value
            .and_then(|v| v.get("inAmount"))
            .and_then(|v| v.as_str())
            != Some(expected.as_str())
    {
        return Err(refused(id, "source_sell_amount_quote_conflict"));
    }
    Ok(())
}
impl Proof {
    pub(crate) fn recheck(&self, store: &SqliteStore) -> Result<()> {
        self.position
            .recheck(store)
            .map_err(|e| on_order(e, &self.order_id))
    }
    pub(crate) fn validate(&self, r: &ExecutionSubmitRequest) -> Result<()> {
        let result = (|| {
            if self.version != 1
                || self.order_id != r.order_id
                || self.signal_id != r.signal_id
                || self.client_order_id != r.client_order_id
                || self.attempt != r.attempt
                || self.route != r.route
                || self.source_wallet != r.wallet_id
                || self.execution_wallet != r.wallet_pubkey
                || self.position.token != r.token
                || self.selected_raw != self.position.raw.min(self.wallet_raw)
                || self.quote_hash != quote_hash(&r.metadata)?
            {
                return Err(refused(&r.order_id, "source_sell_amount_proof_conflict"));
            }
            validate_quote(&r.metadata, self.selected_raw, &r.order_id)
        })();
        result.map_err(|e| on_order(e, &r.order_id))
    }
}
pub(crate) fn request(store: &SqliteStore, r: &ExecutionSubmitRequest) -> Result<Proof> {
    let proof = r.metadata.owned_sell_amount.as_ref().ok_or_else(|| {
        on_order(
            refused(&r.order_id, "source_sell_amount_proof_missing"),
            &r.order_id,
        )
    })?;
    proof.validate(r)?;
    proof.recheck(store)?;
    Ok(proof.clone())
}
pub(crate) fn decode(store: &SqliteStore, order: &str) -> Result<Option<Proof>> {
    store
        .load_execution_canary_sell_amount_proof(order)?
        .map(|s| {
            serde_json::from_str(&s)
                .map_err(|_| on_order(refused(order, "source_sell_amount_proof_invalid"), order))
        })
        .transpose()
}
