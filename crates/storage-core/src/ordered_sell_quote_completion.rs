//! Completion clock is sampled after lock acquisition, before commit, and after
//! commit/readback. A postcommit expiry has one bounded CAS downgrade, never promotion.
use super::*;
impl SqliteDiscoveryStore {
    pub fn complete_strict_sell_quote(
        &self,
        claim: &QuoteClaim,
        limits: InboxLimits,
        mut result: QuoteObservation,
        mut clock: impl FnMut() -> DateTime<Utc>,
    ) -> Result<QuoteObservation> {
        ensure!(
            result.version == 1
                && result.binding.as_ref() == Some(&claim.binding)
                && result.event_time.is_none()
                && result.event_delay_ns.is_none(),
            "strict quote result identity/time conflict"
        );
        schema::durable_writer(&self.conn)?;
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        schema::required(&tx)?;
        let old =
            rows::load(&tx, &claim.intent_id)?.context("strict quote completion claim missing")?;
        ensure!(
            old.owner == claim.owner
                && old.attempt == claim.attempt
                && old.lease.as_deref() == Some(claim.lease_until.to_rfc3339().as_str())
                && old.binding.as_deref() == Some(serde_json::to_string(&claim.binding)?.as_str()),
            "strict quote completion CAS lost"
        );
        ensure!(
            old.record.is_none(),
            "strict quote completion already exists; not this observation"
        );
        let current = snapshot::read(&tx, &claim.intent_id, limits, &claim.binding.endpoint)?;
        if current.as_ref().ok() != Some(&claim.binding) {
            result.outcome = QuoteOutcome::Stale;
            result.reason = Some(
                current
                    .err()
                    .unwrap_or_else(|| "snapshot_amount_changed".into()),
            );
        }
        qualify(&mut result, claim, clock());
        result.reason = result
            .reason
            .map(|reason| reason.chars().take(512).collect());
        let mut row = old.clone();
        row.lease = None;
        row.record = Some(serde_json::to_string(&result)?);
        reserved(&old, &row)?;
        rows::save(&tx, &claim.intent_id, Some(&old), &row)?;
        rows::budget(&tx, limits)?;
        // A trigger, readback, budget query or storage write can also wait.
        let before = result.clone();
        qualify(&mut result, claim, clock());
        if result != before {
            let prior = row.clone();
            row.record = Some(serde_json::to_string(&result)?);
            reserved(&old, &row)?;
            rows::save(&tx, &claim.intent_id, Some(&prior), &row)?;
        }
        tx.commit()?;
        ensure!(
            rows::load(&self.conn, &claim.intent_id)?.as_ref() == Some(&row),
            "strict quote completion postcommit readback lost"
        );
        let before = result.clone();
        qualify(&mut result, claim, clock());
        if before != result {
            // No unbounded repair loop. An unknown SQL/commit/readback failure is
            // returned as error; no Current is published by this invocation.
            let tx =
                rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
            schema::required(&tx)?;
            let mut downgraded = row.clone();
            downgraded.record = Some(serde_json::to_string(&result)?);
            reserved(&old, &downgraded)?;
            rows::save(&tx, &claim.intent_id, Some(&row), &downgraded)?;
            rows::budget(&tx, limits)?;
            tx.commit()?;
            ensure!(
                rows::load(&self.conn, &claim.intent_id)?.as_ref() == Some(&downgraded),
                "strict quote downgrade postcommit readback lost"
            );
            // Already non-current: additional waiting cannot promote the observation.
        }
        Ok(result)
    }
}
fn reserved(old: &rows::Row, new: &rows::Row) -> Result<()> {
    ensure!(
        new.record
            .as_ref()
            .is_some_and(|r| r.len() <= capacity::record_limit(old.binding.as_deref())),
        "strict quote completion exceeds reserved record bytes"
    );
    Ok(())
}
fn qualify(result: &mut QuoteObservation, claim: &QuoteClaim, now: DateTime<Utc>) {
    if result.outcome != QuoteOutcome::Current {
        return;
    }
    if now >= claim.lease_until {
        result.outcome = QuoteOutcome::Stale;
        result.reason = Some("quote_lease_expired".into());
    } else if !fresh(result, now) {
        result.outcome = QuoteOutcome::Unknown;
        result.reason = Some("http_clock_or_exact_response_unknown".into());
    }
}
