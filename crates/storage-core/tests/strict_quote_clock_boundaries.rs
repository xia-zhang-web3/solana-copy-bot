#[path = "common/strict_quote_fixture.rs"]
mod f;
use anyhow::{ensure, Result};
use chrono::{Duration, Utc};
use f::*;
#[test]
fn r1_completion_rechecks_clock_at_lock_precommit_and_postreadback() -> Result<()> {
    for boundary in 1..=3 {
        for elapsed in [
            Duration::milliseconds(5000),
            Duration::milliseconds(5000) + Duration::nanoseconds(1),
            Duration::seconds(31),
        ] {
            let f = fixture()?;
            let now = Utc::now();
            let c = claim(&f, now)?;
            let mut calls = 0;
            let out = f.db.store.complete_strict_sell_quote(
                &c,
                limits(),
                observation(&c, now),
                || {
                    calls += 1;
                    if calls >= boundary {
                        now + elapsed
                    } else {
                        now
                    }
                },
            )?;
            let expected = if elapsed == Duration::milliseconds(5000) {
                QuoteOutcome::Current
            } else if elapsed >= Duration::seconds(30) {
                QuoteOutcome::Stale
            } else {
                QuoteOutcome::Unknown
            };
            ensure!(
                out.outcome == expected,
                "boundary{boundary} {elapsed}: {out:?}"
            );
            let wire: String = f.db.conn()?.query_row(
                "SELECT record FROM ordered_sell_quote_results",
                [],
                |r| r.get(0),
            )?;
            ensure!(serde_json::from_str::<QuoteObservation>(&wire)? == out);
            ensure!(
                out.http_started == Some(now)
                    && out.http_response == Some(now)
                    && out.http_ended == now
            );
            if expected != QuoteOutcome::Current {
                ensure!(matches!(
                    f.db.store
                        .claim_strict_sell_quote(limits(), ENDPOINT, || now + elapsed)?,
                    QuoteClaimStep::Claimed(_)
                ));
            }
        }
    }
    Ok(())
}
#[test]
fn r1_claim_never_extends_lease_during_commit_and_readback() -> Result<()> {
    for boundary in [3, 4] {
        let f = fixture()?;
        let now = Utc::now();
        let mut calls = 0;
        let result = f.db.store.claim_strict_sell_quote(limits(), ENDPOINT, || {
            calls += 1;
            if calls >= boundary {
                now + Duration::seconds(31)
            } else {
                now
            }
        });
        ensure!(result.is_err() && calls == boundary);
        ensure!(count(&f, "ordered_sell_quote_results")? == i64::from(boundary == 4));
        if boundary == 4 {
            let lease: String = f.db.conn()?.query_row(
                "SELECT lease_until FROM ordered_sell_quote_results",
                [],
                |r| r.get(0),
            )?;
            ensure!(lease.parse::<chrono::DateTime<Utc>>()? == now + Duration::seconds(30));
        }
        ensure!(matches!(
            f.db.store
                .claim_strict_sell_quote(limits(), ENDPOINT, || now + Duration::seconds(31))?,
            QuoteClaimStep::Claimed(_)
        ));
    }
    Ok(())
}
#[test]
fn r1_actual_claim_clock_begins_after_writer_wait() -> Result<()> {
    let f = fixture()?;
    let store = copybot_storage_core::SqliteStore::open(&f.db.path)?;
    store.set_busy_timeout(std::time::Duration::from_secs(2))?;
    let sql = f.db.conn()?;
    sql.execute_batch("BEGIN IMMEDIATE")?;
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(()).unwrap();
        store.claim_strict_sell_quote(limits(), ENDPOINT, Utc::now)
    });
    rx.recv()?;
    std::thread::sleep(std::time::Duration::from_millis(200));
    let released = Utc::now();
    sql.execute_batch("COMMIT")?;
    let QuoteClaimStep::Claimed(c) = worker.join().unwrap()? else {
        anyhow::bail!("claim refused")
    };
    ensure!(c.lease_until >= released + Duration::seconds(30));
    Ok(())
}
