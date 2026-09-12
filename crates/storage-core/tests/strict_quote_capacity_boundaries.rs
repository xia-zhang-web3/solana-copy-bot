#[path = "common/strict_quote_fixture.rs"]
mod f;
use anyhow::{ensure, Result};
use chrono::Utc;
use f::*;
fn add(f: &mut F, name: &str, index: u64) -> Result<()> {
    let a = facts(name, "leader", false);
    f.admit(a.clone())?;
    f.terminal(&a, index, 42, "block")?;
    f.drain()?;
    ensure!(matches!(
        f.inbox
            .stage_ordered_source_sell_intent(name, PROVIDER_ORDER_STRICT_V1)?,
        OrderedSellStage::Inserted(_)
    ));
    Ok(())
}
fn bytes(f: &F) -> Result<usize> {
    Ok(f.db.conn()?.query_row("SELECT coalesce(sum(512+length(CAST(intent_id AS BLOB))+length(CAST(owner AS BLOB))+coalesce(length(CAST(binding AS BLOB)),0)+coalesce(length(CAST(record AS BLOB)),min(coalesce(length(CAST(binding AS BLOB)),0)+4096,131072))),0) FROM ordered_sell_quote_results",[],|r|r.get(0))?)
}
#[test]
fn r1_independent_count_cap_advances_refused_a_to_retained_b() -> Result<()> {
    let mut f = fixture()?;
    let cap = QuoteCapacity {
        count: 4,
        bytes: 16 << 20,
    };
    for n in 0..3 {
        add(&mut f, &format!("sell-x{n}"), n + 4)?;
    }
    for _ in 0..4 {
        let QuoteClaimStep::Claimed(c) =
            f.db.store
                .claim_strict_sell_quote_with_capacity(limits(), ENDPOINT, Utc::now, cap)?
        else {
            anyhow::bail!("setup")
        };
        f.db.store.complete_strict_sell_quote(
            &c,
            limits(),
            observation(&c, Utc::now()),
            Utc::now,
        )?;
    }
    add(&mut f, "zz-new", 10)?;
    f.conflict("zz-new")?;
    f.drain()?;
    f.db.conn()?.execute(
        "UPDATE positions SET qty_raw='4000',qty=4 WHERE token='mint'",
        [],
    )?;
    ensure!(
        f.db.store
            .strict_sell_quote_snapshot(ID, limits(), ENDPOINT)?
            .unwrap()
            .raw
            == 4000
    );
    let QuoteClaimStep::CapacityRefused(refusal) =
        f.db.store
            .claim_strict_sell_quote_with_capacity(limits(), ENDPOINT, Utc::now, cap)?
    else {
        anyhow::bail!("expected exact refusal")
    };
    ensure!(refusal.new_row && refusal.dimension == "count" && refusal.projected_count == 5);
    // Reopen store only: never reset inbox bootstrap or manually update cursor/status.
    f.db.reopen()?;
    let QuoteClaimStep::Claimed(c) =
        f.db.store
            .claim_strict_sell_quote_with_capacity(limits(), ENDPOINT, Utc::now, cap)?
    else {
        anyhow::bail!("B must progress")
    };
    ensure!(c.intent_id == ID && c.binding.raw == 4000);
    ensure!(
        f.db.store
            .complete_strict_sell_quote(&c, limits(), observation(&c, Utc::now()), Utc::now)?
            .outcome
            == QuoteOutcome::Current
    );
    ensure!(count(&f, "ordered_sell_quote_results")? == 4);
    // All intake/financial domains fit the unchanged general 1000 / 8MiB limits.
    ensure!(f
        .db
        .store
        .strict_sell_quote_snapshot(ID, limits(), ENDPOINT)?
        .is_ok());
    Ok(())
}
#[test]
fn r1_byte_projection_reserves_completion_and_allows_fitting_b_replacement() -> Result<()> {
    let mut f = fixture()?;
    let c = claim(&f, Utc::now())?;
    let reserved = bytes(&f)?;
    let mut large = observation(&c, Utc::now());
    large.reason = Some("\\\"".repeat(500));
    f.db.store
        .complete_strict_sell_quote(&c, limits(), large, Utc::now)?;
    ensure!(bytes(&f)? <= reserved);
    add(&mut f, "zz-ready", 5)?;
    f.db.conn()?.execute(
        "UPDATE positions SET qty_raw='4000',qty=4 WHERE token='mint'",
        [],
    )?;
    ensure!(
        f.db.store
            .strict_sell_quote_snapshot(ID, limits(), ENDPOINT)?
            .unwrap()
            .raw
            == 4000
    );
    let cap = QuoteCapacity {
        count: 4096,
        bytes: reserved,
    };
    let QuoteClaimStep::CapacityRefused(r) =
        f.db.store
            .claim_strict_sell_quote_with_capacity(limits(), ENDPOINT, Utc::now, cap)?
    else {
        anyhow::bail!("A requires a new reservation")
    };
    ensure!(r.new_row && r.dimension == "bytes");
    let QuoteClaimStep::Claimed(c) =
        f.db.store
            .claim_strict_sell_quote_with_capacity(limits(), ENDPOINT, Utc::now, cap)?
    else {
        anyhow::bail!("B reservation must fit")
    };
    ensure!(c.intent_id == ID && c.binding.raw == 4000 && bytes(&f)? <= reserved);
    f.db.store
        .complete_strict_sell_quote(&c, limits(), observation(&c, Utc::now()), Utc::now)?;
    ensure!(bytes(&f)? <= reserved && count(&f, "ordered_sell_quote_results")? == 1);
    Ok(())
}
#[test]
fn r1_no_claim_without_exact_completion_headroom() -> Result<()> {
    let f = fixture()?;
    let c = claim(&f, Utc::now())?;
    let reserved = bytes(&f)?;
    f.db.store
        .complete_strict_sell_quote(&c, limits(), observation(&c, Utc::now()), Utc::now)?;
    let second = fixture()?;
    // Fixture-specific exact reservation includes first timestamp; derive via refusal.
    let QuoteClaimStep::CapacityRefused(r) =
        second.db.store.claim_strict_sell_quote_with_capacity(
            limits(),
            ENDPOINT,
            Utc::now,
            QuoteCapacity { count: 4, bytes: 1 },
        )?
    else {
        anyhow::bail!("probe refusal")
    };
    ensure!(r.projected_bytes.abs_diff(reserved) < 128);
    let need = r.projected_bytes;
    ensure!(matches!(
        second.db.store.claim_strict_sell_quote_with_capacity(
            limits(),
            ENDPOINT,
            Utc::now,
            QuoteCapacity {
                count: 4,
                bytes: need - 1
            }
        )?,
        QuoteClaimStep::CapacityRefused(_)
    ));
    ensure!(count(&second, "ordered_sell_quote_results")? == 0);
    let QuoteClaimStep::Claimed(c) = second.db.store.claim_strict_sell_quote_with_capacity(
        limits(),
        ENDPOINT,
        Utc::now,
        QuoteCapacity {
            count: 4,
            bytes: need,
        },
    )?
    else {
        anyhow::bail!("exact boundary fits")
    };
    let mut o = observation(&c, Utc::now());
    o.reason = Some("\u{1}".repeat(512));
    second
        .db
        .store
        .complete_strict_sell_quote(&c, limits(), o, Utc::now)?;
    ensure!(bytes(&second)? <= need);
    Ok(())
}
#[test]
fn r1_capacity_cursor_sql_failure_is_not_observable_refusal_success() -> Result<()> {
    for sql in [
        "SELECT RAISE(IGNORE);",
        "SELECT RAISE(ABORT,'cursor fault');",
    ] {
        let f = fixture()?;
        let db = f.db.conn()?;
        db.execute_batch(&format!("CREATE TRIGGER cursor_fault BEFORE INSERT ON ordered_sell_quote_cursor BEGIN {sql} END;"))?;
        ensure!(f
            .db
            .store
            .claim_strict_sell_quote_with_capacity(
                limits(),
                ENDPOINT,
                Utc::now,
                QuoteCapacity { count: 0, bytes: 0 }
            )
            .is_err());
        ensure!(
            count(&f, "ordered_sell_quote_cursor")? == 0
                && count(&f, "ordered_sell_quote_results")? == 0
        );
    }
    Ok(())
}

#[test]
fn r1_max_record_headroom_refuses_before_http_permit() -> Result<()> {
    let f = fixture()?;
    let endpoint = format!("{ENDPOINT}{}", "x".repeat(131072));
    let result =
        f.db.store
            .claim_strict_sell_quote(limits(), &endpoint, Utc::now)?;
    let QuoteClaimStep::CapacityRefused(r) = result else {
        anyhow::bail!("record headroom must refuse")
    };
    ensure!(r.dimension == "record_bytes" && count(&f, "ordered_sell_quote_results")? == 0);
    ensure!(matches!(
        f.db.store
            .claim_strict_sell_quote(limits(), ENDPOINT, Utc::now)?,
        QuoteClaimStep::Claimed(_)
    ));
    Ok(())
}
