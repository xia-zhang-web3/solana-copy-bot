//! Same actual owned SQLite graph; mocked providers, no keys or network sends.
use super::{
    association_parent_fixture as parent, protected_usdc_fixture as f,
    protected_usdc_prepare as money,
};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::{ordered_sell_quote::*, rpc_owned_sell_handoff::Handoff, SqliteStore};
use serde_json::json;

fn observation(c: &QuoteClaim) -> QuoteObservation {
    let now = Utc::now();
    QuoteObservation {
        version: 1,
        binding: Some(c.binding.clone()),
        outcome: QuoteOutcome::Current,
        reason: None,
        http_started: Some(now),
        http_response: Some(now),
        quote_response_available_ts: Some(now),
        http_ended: now,
        response_in_raw: Some(c.binding.raw.to_string()),
        response_out_raw: Some("1000".into()),
        response_sha256: Some("a".repeat(64)),
        event_time: None,
        event_delay_ns: None,
    }
}
async fn preparing() -> Result<(f::Case, Handoff)> {
    let mut f = f::Case::new().await?;
    let c = f.claim()?;
    let c = f.bind(c).await?;
    let q =
        f.db.store
            .complete_strict_sell_quote(&c, parent::limits(), observation(&c), Utc::now)?;
    let s =
        f.db.store
            .owned_sell_snapshot(&c.binding, parent::limits())?;
    let h = f.db.store.reserve_owned_sell_handoff(
        &s,
        &q,
        parent::limits(),
        &crate::execution_owned_sell_rpc::identity(&f.config)?,
        "MOCK authority",
        "protected-usdc",
        &f.config.canary_wallet_pubkey,
        Utc::now,
    )?;
    Ok((f, h))
}
fn refuse(f: &f::Case, h: &Handoff) -> Result<()> {
    f.db.store.record_owned_sell_simulation_refusal(
        h,
        parent::limits(),
        &"a".repeat(64),
        151,
        Utc::now(),
    )
}
#[tokio::test]
async fn owned_sell_recovery_retains_hold_history_and_excludes_stale_owner() -> Result<()> {
    let (mut f, h) = preparing().await?;
    let reserved_at: String =
        f.db.sql
            .query_row("SELECT reserved_at FROM rpc_owned_sell_handoffs", [], |r| {
                r.get(0)
            })?;
    refuse(&f, &h)?;
    // A delayed original future cannot complete after its durable refusal.
    assert!(f
        .db
        .store
        .complete_owned_sell_handoff(
            &h,
            parent::limits(),
            "old",
            &"b".repeat(64),
            5000,
            0,
            Utc::now
        )
        .is_err());
    let c = f.claim()?;
    let c = f.bind(c).await?;
    let q =
        f.db.store
            .complete_strict_sell_quote(&c, parent::limits(), observation(&c), Utc::now)?;
    let s =
        f.db.store
            .owned_sell_snapshot(&c.binding, parent::limits())?;
    let new = f.db.store.recover_owned_sell_handoff(
        &s,
        &q,
        parent::limits(),
        &h.config_sha256,
        "fresh MOCK authority",
        &h.experiment_id,
        &h.wallet,
        Utc::now,
    )?;
    assert_ne!(new.owner, h.owner);
    assert_eq!(new.order_id, h.order_id);
    assert!(new.quote.http_started.unwrap() > h.quote.http_ended);
    assert!(f
        .db
        .store
        .recheck_owned_sell_handoff(&h, parent::limits(), Utc::now())
        .is_err());
    assert!(f
        .db
        .store
        .recover_owned_sell_handoff(
            &s,
            &q,
            parent::limits(),
            &h.config_sha256,
            "MOCK",
            &h.experiment_id,
            &h.wallet,
            Utc::now
        )
        .is_err());
    let (count, fee): (u64, u64) = f.db.sql.query_row(
        "SELECT count(*),sum(fee_reserve) FROM rpc_owned_sell_handoffs",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!((count, fee), (1, 100000));
    assert_eq!(
        f.db.sql
            .query_row("SELECT reserved_at FROM rpc_owned_sell_handoffs", [], |r| r
                .get::<_, String>(0))?,
        reserved_at
    );
    let old: String = f.db.sql.query_row(
        "SELECT handoff FROM owned_sell_simulation_refusals WHERE owner=?1",
        [&h.owner],
        |r| r.get(0),
    )?;
    assert_eq!(old, serde_json::to_string(&h)?);
    assert_eq!(
        f.db.sql.query_row(
            "SELECT count(*) FROM owned_sell_preparation_recoveries",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn owned_sell_recovery_late_negative_records_without_reviving_old_quote() -> Result<()> {
    let (f, h) = preparing().await?;
    let late = h.deadline + chrono::Duration::seconds(5);
    f.db.store.record_owned_sell_simulation_refusal(
        &h,
        parent::limits(),
        &"a".repeat(64),
        151,
        late,
    )?;
    assert!(f
        .db
        .store
        .recheck_owned_sell_handoff(&h, parent::limits(), late)
        .is_err());
    assert!(f
        .db
        .store
        .recover_owned_sell_handoff(
            &h.snapshot,
            &h.quote,
            parent::limits(),
            &h.config_sha256,
            "MOCK",
            &h.experiment_id,
            &h.wallet,
            || late
        )
        .is_err());
    assert_eq!(
        f.db.sql
            .query_row("SELECT deadline FROM rpc_owned_sell_handoffs", [], |r| {
                r.get::<_, String>(0)
            })?,
        h.deadline.to_rfc3339()
    );
    Ok(())
}
#[tokio::test]
async fn owned_sell_recovery_prepared_and_unknown_dispatch_cannot_become_refusals() -> Result<()> {
    for dispatched in [false, true] {
        let mut f = f::Case::new().await?;
        let c = f.claim()?;
        let c = f.bind(c).await?;
        let (p, _) = money::prepare(&f, &c)?;
        if dispatched {
            let (d, b) = money::dispatch(&p);
            f.db.store
                .claim_owned_sell_dispatch(&p, &d, &b, || Ok(Utc::now()))?;
        }
        assert!(refuse(&f, &p.handoff).is_err());
        assert!(matches!(
            f.db.store.claim_strict_sell_quote_for_owned_preparation(
                parent::limits(),
                "http://127.0.0.1:1/",
                Utc::now
            )?,
            QuoteClaimStep::Skipped
        ));
        assert_eq!(
            f.db.sql.query_row(
                "SELECT count(*) FROM owned_sell_simulation_refusals",
                [],
                |r| r.get::<_, u64>(0)
            )?,
            0
        );
    }
    Ok(())
}
#[tokio::test]
async fn owned_sell_recovery_dead_authority_and_two_contenders() -> Result<()> {
    let (mut f, h) = preparing().await?;
    refuse(&f, &h)?;
    let c = f.claim()?;
    let c = f.bind(c).await?;
    let q =
        f.db.store
            .complete_strict_sell_quote(&c, parent::limits(), observation(&c), Utc::now)?;
    let s =
        f.db.store
            .owned_sell_snapshot(&c.binding, parent::limits())?;
    let expired = f
        .config
        .technical_cohort
        .as_ref()
        .unwrap()
        .deadline
        .parse::<chrono::DateTime<Utc>>()?
        + chrono::Duration::seconds(1);
    assert!(f
        .db
        .store
        .recover_owned_sell_handoff(
            &s,
            &q,
            parent::limits(),
            &h.config_sha256,
            "MOCK",
            &h.experiment_id,
            &h.wallet,
            || expired
        )
        .is_err());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut threads = vec![];
    for _ in 0..2 {
        let path = f.db.path.clone();
        let s = s.clone();
        let q = q.clone();
        let h = h.clone();
        let b = barrier.clone();
        threads.push(std::thread::spawn(move || -> Result<bool> {
            let store = SqliteStore::open(path)?;
            b.wait();
            Ok(store
                .recover_owned_sell_handoff(
                    &s,
                    &q,
                    parent::limits(),
                    &h.config_sha256,
                    "MOCK",
                    &h.experiment_id,
                    &h.wallet,
                    Utc::now,
                )
                .is_ok())
        }));
    }
    let wins = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(wins.iter().filter(|v| **v).count(), 1);
    assert_eq!(
        f.db.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r
                .get::<_, u64>(0))?,
        1
    );
    Ok(())
}
#[test]
fn owned_sell_recovery_only_valid_simulation_rejection_yields_proof() {
    use crate::execution_guarded_generic_sell::owned_simulation::{verify, Rejection};
    let rejected = json!({"jsonrpc":"2.0","id":"execution-swap-transaction-simulate",
        "result":{"context":{"slot":151},"value":{"err":{"InstructionError":[0,{"Custom":6001}]}}}});
    assert!(verify(&rejected, "MOCK", "unsigned")
        .unwrap_err()
        .is::<Rejection>());
    for value in [
        json!({"jsonrpc":"2.0","id":"wrong","result":rejected["result"]}),
        json!({"error":{"code":-1}}),
        json!({"result":{"value":{"err":true}}}),
    ] {
        assert!(!verify(&value, "MOCK", "unsigned")
            .unwrap_err()
            .is::<Rejection>());
    }
}
