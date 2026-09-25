//! Durable quote-only last observation. No legacy quote table or financial writes.
use crate::{association_inbox::InboxLimits, SqliteDiscoveryStore};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{OptionalExtension, TransactionBehavior};
#[path = "ordered_sell_quote_capacity.rs"]
mod capacity;
#[path = "ordered_sell_quote_completion.rs"]
mod completion;
#[path = "ordered_sell_quote_rows.rs"]
mod rows;
#[path = "ordered_sell_quote_schema.rs"]
pub mod schema;
#[path = "ordered_sell_quote_snapshot.rs"]
pub(crate) mod snapshot;
#[path = "ordered_sell_quote_types.rs"]
mod types;
pub use types::*;
const SOL: &str = "So11111111111111111111111111111111111111112";
#[path = "fractional_sell.rs"]
pub mod fractional;
pub const MAX_QUOTE_AGE_MS: i64 = 5_000;

impl SqliteDiscoveryStore {
    pub fn strict_sell_quote_snapshot(
        &self,
        id: &str,
        limits: InboxLimits,
        endpoint: &str,
    ) -> Result<std::result::Result<QuoteBinding, String>> {
        let tx = self.conn.unchecked_transaction()?;
        schema::required(&tx)?;
        let result = snapshot::read(&tx, id, limits, endpoint)?;
        tx.commit()?;
        Ok(result)
    }
    /// One raw cursor visit, one short transaction; callers bound visits/jobs per tick.
    /// A refusal advances the cursor too. Expired leases retry automatically after restart.
    pub fn claim_strict_sell_quote(
        &self,
        limits: InboxLimits,
        endpoint: &str,
        clock: impl FnMut() -> DateTime<Utc>,
    ) -> Result<QuoteClaimStep> {
        self.claim_strict_sell_quote_with_capacity(
            limits,
            endpoint,
            clock,
            QuoteCapacity::PRODUCTION,
        )
    }
    /// Same production path with a tighter quote-only cap; cannot increase hard caps.
    /// Separates quote capacity from financial/inbox domains for bounded verification.
    pub fn claim_strict_sell_quote_with_capacity(
        &self,
        limits: InboxLimits,
        endpoint: &str,
        clock: impl FnMut() -> DateTime<Utc>,
        requested: QuoteCapacity,
    ) -> Result<QuoteClaimStep> {
        self.claim_quote_mode(limits, endpoint, clock, requested, false)
    }
    /// Same leases/cursor/three-attempt budget. A completed observation without
    /// a financial owner can resume after a crash; an owned handoff cannot rearm.
    pub fn claim_strict_sell_quote_for_owned_preparation(
        &self,
        limits: InboxLimits,
        endpoint: &str,
        clock: impl FnMut() -> DateTime<Utc>,
    ) -> Result<QuoteClaimStep> {
        self.claim_quote_mode(limits, endpoint, clock, QuoteCapacity::PRODUCTION, true)
    }
    fn claim_quote_mode(
        &self,
        limits: InboxLimits,
        endpoint: &str,
        mut clock: impl FnMut() -> DateTime<Utc>,
        requested: QuoteCapacity,
        preparation: bool,
    ) -> Result<QuoteClaimStep> {
        let caps = capacity::caps(limits, requested);
        schema::durable_writer(&self.conn)?;
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        schema::required(&tx)?;
        let cursor: Option<String> = tx
            .query_row(
                "SELECT intent_id FROM ordered_sell_quote_cursor WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let mut id:Option<String>=tx.query_row("SELECT intent_id FROM ordered_source_sell_intents WHERE intent_id>?1 ORDER BY intent_id LIMIT 1",[cursor.as_deref().unwrap_or("")],|r|r.get(0)).optional()?;
        if id.is_none() {
            id = tx
                .query_row(
                    "SELECT intent_id FROM ordered_source_sell_intents ORDER BY intent_id LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
        }
        let Some(id) = id else {
            tx.commit()?;
            return Ok(QuoteClaimStep::Empty);
        };
        rows::cursor(&tx, &id)?;
        if preparation
            && tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs WHERE intent_id=?1)",
                [&id],
                |r| r.get::<_, bool>(0),
            )?
        {
            tx.commit()?;
            return Ok(QuoteClaimStep::Skipped);
        }
        let old = rows::load(&tx, &id)?;
        let now = clock();
        if let Some(old) = &old {
            if old.record.is_none()
                && old
                    .lease
                    .as_deref()
                    .map(str::parse::<DateTime<Utc>>)
                    .transpose()?
                    .is_some_and(|t| t > now)
            {
                tx.commit()?;
                return Ok(QuoteClaimStep::Skipped);
            }
        }
        let current = snapshot::read(&tx, &id, limits, endpoint)?;
        if let (Ok(binding), Some(old)) = (&current, &old) {
            if let Some(wire) = &old.record {
                let saved: QuoteObservation = serde_json::from_str(wire)?;
                if saved.binding.as_ref() == Some(binding)
                    && ((saved.outcome == QuoteOutcome::Current
                        && (!preparation || fresh(&saved, now)))
                        || old.binding_attempt >= 3)
                {
                    tx.commit()?;
                    return Ok(QuoteClaimStep::Skipped);
                }
            }
        }
        let current_wire = current
            .as_ref()
            .ok()
            .map(serde_json::to_string)
            .transpose()?;
        let same_binding = old.as_ref().is_some_and(|r| r.binding == current_wire);
        let binding_attempt = if same_binding {
            old.as_ref().unwrap().binding_attempt + 1
        } else {
            1
        };
        if binding_attempt > 3 {
            // A crashed third attempt becomes explicit Unknown at lease expiry.
            // No endless network retry for an unchanged binding. State/amount change
            // starts a new bounded set; first intent identity is never rewritten.
            let old = old
                .as_ref()
                .context("strict quote retry budget row missing")?;
            let result = QuoteObservation {
                version: 1,
                binding: current.ok(),
                outcome: QuoteOutcome::Unknown,
                reason: Some("unchanged_binding_attempt_budget_exhausted".into()),
                http_started: None,
                http_response: None,
                quote_response_available_ts: None,
                http_ended: now,
                response_in_raw: None,
                response_out_raw: None,
                response_sha256: None,
                event_time: None,
                event_delay_ns: None,
            };
            let row = rows::Row {
                attempt: old.attempt,
                binding_attempt: old.binding_attempt,
                owner: old.owner.clone(),
                lease: None,
                binding: old.binding.clone(),
                record: Some(serde_json::to_string(&result)?),
            };
            rows::save(&tx, &id, Some(old), &row)?;
            rows::budget(&tx, limits)?;
            tx.commit()?;
            ensure!(
                rows::load(&self.conn, &id)?.as_ref() == Some(&row),
                "strict quote exhausted readback lost"
            );
            return Ok(QuoteClaimStep::Skipped);
        }
        let now = clock();
        let attempt = old
            .as_ref()
            .map_or(Some(1), |r| r.attempt.checked_add(1))
            .context("strict quote attempt overflow")?;
        let owner = uuid::Uuid::new_v4().to_string();
        let lease_until = now + Duration::seconds(30);
        let (binding, record, result) = match current {
            Ok(binding) => {
                let wire = serde_json::to_string(&binding)?;
                let claim = QuoteClaim {
                    intent_id: id.clone(),
                    attempt,
                    owner: owner.clone(),
                    binding,
                    lease_until,
                };
                (Some(wire), None, QuoteClaimStep::Claimed(claim))
            }
            Err(reason) => {
                let observation = QuoteObservation {
                    version: 1,
                    binding: None,
                    outcome: QuoteOutcome::Unknown,
                    reason: Some(reason),
                    http_started: None,
                    http_response: None,
                    quote_response_available_ts: None,
                    http_ended: now,
                    response_in_raw: None,
                    response_out_raw: None,
                    response_sha256: None,
                    event_time: None,
                    event_delay_ns: None,
                };
                (
                    None,
                    Some(serde_json::to_string(&observation)?),
                    QuoteClaimStep::Skipped,
                )
            }
        };
        let row = rows::Row {
            attempt,
            binding_attempt,
            owner,
            lease: record.is_none().then(|| lease_until.to_rfc3339()),
            binding,
            record,
        };
        if let Some(refusal) = capacity::refusal(&tx, caps, &id, old.as_ref(), &row)? {
            // Proven projection only: no insert/update of result, no network permit.
            // Preserve the cursor visit in this transaction, including restart.
            tx.commit()?;
            capacity::cursor_readback(&self.conn, &id)?;
            ensure!(
                rows::load(&self.conn, &id)? == old,
                "strict quote capacity row readback changed"
            );
            return Ok(QuoteClaimStep::CapacityRefused(refusal));
        }
        rows::save(&tx, &id, old.as_ref(), &row)?;
        rows::budget(&tx, limits)?;
        ensure!(
            clock() < lease_until,
            "strict quote claim lease expired before commit"
        );
        tx.commit()?;
        ensure!(
            rows::load(&self.conn, &id)?.as_ref() == Some(&row),
            "strict quote postcommit readback lost"
        );
        ensure!(
            clock() < lease_until,
            "strict quote claim lease expired during commit/readback"
        );
        Ok(result)
    }
    /// Recheck the claim and all current predicates after each network wait.
    pub fn recheck_strict_sell_quote(
        &self,
        claim: &QuoteClaim,
        limits: InboxLimits,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let tx = self.conn.unchecked_transaction()?;
        schema::required(&tx)?;
        let row = rows::load(&tx, &claim.intent_id)?.context("strict quote claim missing")?;
        ensure!(
            row.owner == claim.owner
                && row.attempt == claim.attempt
                && row.lease.as_deref() == Some(claim.lease_until.to_rfc3339().as_str())
                && row.record.is_none()
                && row.binding.as_deref() == Some(serde_json::to_string(&claim.binding)?.as_str()),
            "strict quote claim CAS lost"
        );
        let current = snapshot::read(&tx, &claim.intent_id, limits, &claim.binding.endpoint)?;
        let valid = now < claim.lease_until && current.as_ref().ok() == Some(&claim.binding);
        tx.commit()?;
        Ok(valid)
    }
    /// Last historical observation, freshly qualified in this read snapshot.
    /// Raw persisted Current only describes completion time, never perpetual freshness.
    pub fn load_strict_sell_quote(
        &self,
        id: &str,
        limits: InboxLimits,
        now: DateTime<Utc>,
    ) -> Result<Option<QuoteObservation>> {
        let tx = self.conn.unchecked_transaction()?;
        schema::required(&tx)?;
        let Some(wire) = rows::load(&tx, id)?.and_then(|r| r.record) else {
            return Ok(None);
        };
        let mut result: QuoteObservation = serde_json::from_str(&wire)?;
        if result.outcome == QuoteOutcome::Current {
            let b = result
                .binding
                .as_ref()
                .context("strict quote result missing binding")?;
            let current = snapshot::read(&tx, id, limits, &b.endpoint)?;
            if current.as_ref().ok() != Some(b) || !fresh(&result, now) {
                result.outcome = QuoteOutcome::Stale;
                result.reason = Some("current_snapshot_or_http_age_changed".into());
            }
        }
        tx.commit()?;
        Ok(Some(result))
    }
    /// Compare the completed record without another graph walk. The caller
    /// must pair this with a freshly validated owned snapshot on the same
    /// connection and a data_version fence around both reads. Later external
    /// commits require full re-evaluation; the handoff does its own atomic
    /// graph check before any dispatch.
    pub fn matches_persisted_current_strict_quote(
        &self,
        expected: &QuoteObservation,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(binding) = expected.binding.as_ref() else {
            return Ok(false);
        };
        let tx = self.conn.unchecked_transaction()?;
        schema::required(&tx)?;
        let row = rows::load(&tx, &binding.intent_id)?;
        let binding_wire = serde_json::to_string(binding)?;
        let matches = row
            .and_then(|row| {
                if row.lease.is_some() || row.binding.as_deref() != Some(binding_wire.as_str()) {
                    return None;
                }
                row.record
            })
            .map(|wire| serde_json::from_str::<QuoteObservation>(&wire))
            .transpose()?
            .as_ref()
            == Some(expected)
            && expected.outcome == QuoteOutcome::Current
            && fresh(expected, now);
        tx.commit()?;
        Ok(matches)
    }
}
pub(crate) fn fresh(r: &QuoteObservation, now: DateTime<Utc>) -> bool {
    let Some(b) = &r.binding else {
        return false;
    };
    let (Some(start), Some(response)) = (r.http_started, r.http_response) else {
        return false;
    };
    r.version == 1
        && r.event_time.is_none()
        && r.event_delay_ns.is_none()
        && r.outcome == QuoteOutcome::Current
        && start <= response
        && response <= r.http_ended
        && r.http_ended <= now
        && start <= now
        && now - start <= Duration::milliseconds(MAX_QUOTE_AGE_MS)
        && r.response_in_raw.as_deref() == Some(b.raw.to_string().as_str())
        && r.response_out_raw
            .as_deref()
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|v| v > 0)
        && r.response_sha256
            .as_ref()
            .is_some_and(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}
