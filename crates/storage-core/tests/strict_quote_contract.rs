#[path = "common/strict_quote_fixture.rs"]
mod f;
use anyhow::Result;
use chrono::{Duration, Utc};
use f::*;

#[test]
fn strict_quote_exact_claim_result_duplicate_and_legacy_exclusion() -> Result<()> {
    let f = fixture()?;
    let now = Utc::now();
    let c = claim(&f, now)?;
    assert_eq!((c.binding.raw, c.binding.decimals), (7000, 3));
    assert_eq!(c.binding.source_signature, "leaderbuy");
    assert!(!c.binding.snapshot_version.contains("encoded"));
    assert!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, || now)?,
        QuoteClaimStep::Skipped
    ));
    let obs = observation(&c, now);
    let stored =
        f.db.store
            .complete_strict_sell_quote(&c, limits(), obs.clone(), || now)?;
    assert_eq!(stored, obs);
    assert_eq!(
        f.db.store.load_strict_sell_quote(ID, limits(), now)?,
        Some(obs.clone())
    );
    assert!(f
        .db
        .store
        .complete_strict_sell_quote(&c, limits(), obs, || now)
        .is_err());
    assert_eq!(count(&f, "ordered_sell_quote_results")?, 1);
    assert_eq!(count(&f, "execution_quote_canary_events")?, 0);
    assert!(f.db.store.load_execution_source_sell_intent(ID)?.is_none());
    assert!(f
        .db
        .store
        .load_execution_canary_order_by_signal(ID)?
        .is_none());
    Ok(())
}
#[test]
fn current_record_fast_check_requires_external_version_fence() -> Result<()> {
    let f = fixture()?;
    let now = Utc::now();
    let c = claim(&f, now)?;
    let current =
        f.db.store
            .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?;
    assert!(f
        .db
        .store
        .matches_persisted_current_strict_quote(&current, now)?);
    let mut altered = current.clone();
    altered.response_out_raw = Some("1".into());
    assert!(!f
        .db
        .store
        .matches_persisted_current_strict_quote(&altered, now)?);
    assert!(!f
        .db
        .store
        .matches_persisted_current_strict_quote(&current, now + Duration::seconds(6),)?);
    let before = f.db.store.sqlite_data_version()?;
    f.db.conn()?.execute(
        "UPDATE positions SET qty_raw='4000',qty=4 WHERE token='mint'",
        [],
    )?;
    assert_ne!(f.db.store.sqlite_data_version()?, before);
    assert!(f
        .db
        .store
        .matches_persisted_current_strict_quote(&current, now)?);
    assert_eq!(
        f.db.store
            .load_strict_sell_quote(ID, limits(), now)?
            .unwrap()
            .outcome,
        QuoteOutcome::Stale
    );
    Ok(())
}
#[test]
fn strict_quote_http_clock_and_response_unknown_pairs() -> Result<()> {
    for case in [
        "valid",
        "missing",
        "future_ns",
        "end_future_ns",
        "response_before",
        "old",
        "wrong_amount",
        "zero_output",
        "event_time",
    ] {
        let f = fixture()?;
        let now = Utc::now();
        let c = claim(&f, now)?;
        let mut o = observation(&c, now);
        match case {
            "missing" => o.http_started = None,
            "future_ns" => o.http_started = Some(now + Duration::nanoseconds(1)),
            "end_future_ns" => o.http_ended = now + Duration::nanoseconds(1),
            "response_before" => o.http_response = Some(now - Duration::nanoseconds(1)),
            "old" => o.http_started = Some(now - Duration::seconds(6)),
            "wrong_amount" => o.response_in_raw = Some("6999".into()),
            "zero_output" => o.response_out_raw = Some("0".into()),
            "event_time" => o.event_time = Some(now),
            _ => {}
        }
        let result =
            f.db.store
                .complete_strict_sell_quote(&c, limits(), o, || now);
        if case == "event_time" {
            assert!(result.is_err());
            continue;
        }
        assert_eq!(
            result?.outcome,
            if case == "valid" {
                QuoteOutcome::Current
            } else {
                QuoteOutcome::Unknown
            },
            "{case}"
        );
    }
    Ok(())
}
#[test]
fn strict_quote_late_state_changes_stale_then_no_false_current() -> Result<()> {
    for case in [
        "amount",
        "decimals",
        "generation",
        "conflict",
        "pending",
        "contributors",
        "shadow",
    ] {
        let mut f = fixture()?;
        let now = Utc::now();
        let c = claim(&f, now)?;
        let sql = f.db.conn()?;
        match case {
            "amount" => {
                sql.execute(
                    "UPDATE positions SET qty_raw='4000',qty=4 WHERE token='mint'",
                    [],
                )?;
            }
            "decimals" => {
                sql.execute(
                    "UPDATE positions SET qty_raw=NULL,qty_decimals=NULL WHERE token='mint'",
                    [],
                )?;
            }
            "generation" => {
                sql.execute(
                    "UPDATE positions SET opened_ts='2026-09-07T12:00:01+00:00' WHERE token='mint'",
                    [],
                )?;
            }
            "conflict" => {
                sql.execute(
                    "UPDATE association_inbox_identities SET conflict=1 WHERE signature='sell'",
                    [],
                )?;
            }
            "pending" => {
                f.db.seed("shadow:pending:leader:buy:mint", "leader", "buy")?;
            }
            "contributors" => {
                let order =
                    f.db.seed("shadow:new-source:leader:buy:mint", "leader", "buy")?;
                f.db.buy(&order)?;
            }
            "shadow" => {
                let a = facts("unknown-shadow", "leader", true);
                insert(&f, &a)?;
            }
            _ => unreachable!(),
        }
        assert!(
            !f.db.store.recheck_strict_sell_quote(&c, limits(), now)?,
            "{case}"
        );
        let out =
            f.db.store
                .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?;
        assert_eq!(out.outcome, QuoteOutcome::Stale, "{case}");
        assert_eq!(
            f.db.store
                .load_strict_sell_quote(ID, limits(), now)?
                .unwrap()
                .outcome,
            QuoteOutcome::Stale
        );
        f.db.reopen()?; // Store reader only; never bootstrap inbox through a test reader.
        if case == "amount" {
            assert_eq!(claim(&f, now)?.binding.raw, 4000);
        } else {
            assert!(matches!(
                f.db.store
                    .claim_strict_sell_quote(limits(), ENDPOINT, || now)?,
                QuoteClaimStep::Skipped
            ));
        }
    }
    Ok(())
}
#[test]
fn strict_quote_restart_expiry_first_binding_and_cas_loss() -> Result<()> {
    let mut f = fixture()?;
    let now = Utc::now();
    let first = f
        .inbox
        .load_ordered_source_sell_intent_history(ID)?
        .unwrap();
    let old = claim(&f, now)?;
    f.db.reopen()?;
    assert!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, || now)?,
        QuoteClaimStep::Skipped
    ));
    let next = claim(&f, now + Duration::seconds(31))?;
    assert!(next.attempt > old.attempt);
    assert_ne!(next.owner, old.owner);
    assert_eq!(next.binding, old.binding);
    assert!(f
        .db
        .store
        .recheck_strict_sell_quote(&old, limits(), now)
        .is_err());
    assert!(f
        .db
        .store
        .complete_strict_sell_quote(&old, limits(), observation(&old, now), || now)
        .is_err());
    assert_eq!(
        f.inbox
            .load_ordered_source_sell_intent_history(ID)?
            .unwrap(),
        first
    );
    Ok(())
}
#[test]
fn strict_quote_completed_observation_revalidates_at_every_read() -> Result<()> {
    let f = fixture()?;
    let now = Utc::now();
    let c = claim(&f, now)?;
    f.db.store
        .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?;
    assert_eq!(
        f.db.store
            .load_strict_sell_quote(ID, limits(), now + Duration::seconds(6))?
            .unwrap()
            .outcome,
        QuoteOutcome::Stale
    );
    f.db.conn()?.execute(
        "UPDATE positions SET qty_raw='4000',qty=4 WHERE token='mint'",
        [],
    )?;
    assert_eq!(
        f.db.store
            .load_strict_sell_quote(ID, limits(), now)?
            .unwrap()
            .outcome,
        QuoteOutcome::Stale
    );
    assert_eq!(claim(&f, now)?.binding.raw, 4000);
    Ok(())
}
#[test]
fn strict_quote_refused_a_does_not_block_ready_b_and_cursor_wraps() -> Result<()> {
    let mut f = fixture()?;
    let a = facts("sell-b", "leader", false);
    f.admit(a.clone())?;
    f.terminal(&a, 4, 42, "block")?;
    f.drain()?;
    assert!(matches!(
        f.inbox
            .stage_ordered_source_sell_intent("sell-b", PROVIDER_ORDER_STRICT_V1)?,
        OrderedSellStage::Inserted(_)
    ));
    f.conflict("sell")?;
    f.drain()?;
    let now = Utc::now();
    assert!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, || now)?,
        QuoteClaimStep::Skipped
    ));
    let c = claim(&f, now)?;
    assert_eq!(c.intent_id, "source-sell:sell-b");
    let saved =
        f.db.store
            .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?;
    assert_eq!(saved.outcome, QuoteOutcome::Current);
    assert_eq!(
        f.db.store
            .load_strict_sell_quote(ID, limits(), now)?
            .unwrap()
            .outcome,
        QuoteOutcome::Unknown
    );
    Ok(())
}
#[test]
fn strict_quote_bounded_failures_and_no_periodic_requote_for_completed_binding() -> Result<()> {
    let f = fixture()?;
    let now = Utc::now();
    for n in 0..3 {
        let c = claim(&f, now + Duration::seconds(n))?;
        let mut o = observation(&c, now + Duration::seconds(n));
        o.outcome = QuoteOutcome::Unknown;
        o.reason = Some("offline HTTP error".into());
        f.db.store
            .complete_strict_sell_quote(&c, limits(), o, || now + Duration::seconds(n))?;
    }
    assert!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, || now + Duration::seconds(10))?,
        QuoteClaimStep::Skipped
    ));
    f.db.conn()?.execute(
        "UPDATE positions SET qty_raw='4000',qty=4 WHERE token='mint'",
        [],
    )?;
    let c = claim(&f, now)?;
    assert_eq!(c.binding.raw, 4000);
    f.db.store
        .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)?;
    assert!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, || now + Duration::seconds(10))?,
        QuoteClaimStep::Skipped
    ));
    assert_eq!(
        f.db.store
            .load_strict_sell_quote(ID, limits(), now + Duration::seconds(10))?
            .unwrap()
            .outcome,
        QuoteOutcome::Stale
    );
    Ok(())
}
#[test]
fn strict_quote_claim_lease_identity_cannot_be_extended_by_caller() -> Result<()> {
    let f = fixture()?;
    let now = Utc::now();
    let mut c = claim(&f, now)?;
    c.lease_until += Duration::seconds(1);
    assert!(f
        .db
        .store
        .recheck_strict_sell_quote(&c, limits(), now)
        .is_err());
    assert!(f
        .db
        .store
        .complete_strict_sell_quote(&c, limits(), observation(&c, now), || now)
        .is_err());
    assert!(f
        .db
        .store
        .load_strict_sell_quote(ID, limits(), now)?
        .is_none());
    Ok(())
}
