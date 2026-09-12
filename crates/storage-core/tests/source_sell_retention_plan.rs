#[path = "common/source_sell_promotion_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn sample(unrelated: usize, pins: usize) -> Result<usize> {
    let d = Db::new()?;
    d.proven("buy", "source-a")?;
    let staged = prepare(&d, "pin-00000", "source-a")?;
    let cutoff = d.now + Duration::seconds(10);
    let mut c = d.conn()?;
    let tx = c.transaction()?;
    // Large synthetic history only exercises the selector, not execution proof.
    for n in 1..pins + unrelated {
        let name = format!("pin-{n:05}");
        tx.execute("INSERT INTO execution_source_sell_intents SELECT
            ?1,?1,source_wallet,dex,token,token_out,amount_in,amount_out,slot,event_ts,
            amount_in_raw,amount_in_decimals,amount_out_raw,amount_out_decimals,
            position_id,buy_fill_id,buy_order_id,buy_signal_id,buy_tx_signature,buy_execution_wallet,staged_at
            FROM execution_source_sell_intents WHERE intent_id=?2", [&name, &staged.intent_id])?;
        // Unrelated observations are fresh and beyond the cutoff; their staging is irrelevant.
        let ts = if n < pins {
            d.now + Duration::seconds(1)
        } else {
            cutoff + Duration::days(1)
        };
        tx.execute(
            "INSERT INTO observed_swaps SELECT ?1,wallet_id,dex,token_in,token_out,
            qty_in,qty_out,slot,?2,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals
            FROM observed_swaps WHERE signature='pin-00000'",
            [&name, &ts.to_rfc3339()],
        )?;
    }
    tx.commit()?;
    let mut b = d.sell("ordinary", "other");
    b.ts_utc += Duration::seconds(1);
    d.store.insert_observed_swap(&b)?;
    let source = include_str!("../src/observed_retention.rs");
    let pin = source
        .split("const PIN: &str = \"")
        .nth(1)
        .unwrap()
        .split("\";")
        .next()
        .unwrap();
    let pending = source
        .split("let pending = if")
        .nth(1)
        .unwrap()
        .split('"')
        .nth(1)
        .unwrap();
    let sql = source
        .split("let sql = format!(")
        .nth(1)
        .unwrap()
        .trim_start()
        .strip_prefix('"')
        .unwrap()
        .split('"')
        .next()
        .unwrap()
        .replace("{pin}", pin)
        .replace("{pending}", pending);
    let plan = c
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .query_map(rusqlite::params![cutoff.to_rfc3339(), 1], |r| {
            r.get::<_, String>(3)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        plan.iter()
            .any(|p| p.contains("SEARCH s USING INDEX") && p.contains("event_signature=?")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|p| p.contains("SEARCH p USING INDEX") && p.contains("position_id=?")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|p| p.contains("SEARCH observed_swaps USING") && p.contains("ts<?")),
        "{plan:?}"
    );
    assert!(
        !plan
            .iter()
            .any(|p| p.contains("SCAN s") || p.contains("SCAN p") || p.contains("AUTOMATIC")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|p| p.contains("SEARCH h USING INDEX") && p.contains("signature=?")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|p| p.contains("SCAN h")), "{plan:?}");
    let steps = Arc::new(AtomicUsize::new(0));
    let count = steps.clone();
    c.progress_handler(
        1,
        Some(move || {
            count.fetch_add(1, Ordering::Relaxed);
            false
        }),
    );
    // Execute the production helper, including validation and boundary metadata, in its real transaction.
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    assert_eq!(
        copybot_storage_core::observed_retention::delete_before_batch(&tx, cutoff, 1, None)?,
        1
    );
    tx.commit()?;
    c.progress_handler(0, None::<fn() -> bool>);
    let vm = steps.load(Ordering::Relaxed);
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM observed_swaps WHERE ts<?1",
            [cutoff.to_rfc3339()],
            |r| r.get::<_, usize>(0)
        )?,
        pins
    );
    assert!(!c
        .prepare("SELECT 1 FROM observed_swaps WHERE signature='ordinary'")?
        .exists([])?);
    eprintln!("B52_PLAN unrelated={unrelated} pins={pins} limit=1 VM={vm} {plan:?}");
    Ok(vm)
}

#[test]
fn actual_retention_uses_indexed_pin_probes_but_limit_does_not_bound_pinned_scan() -> Result<()> {
    let small = sample(0, 1)?;
    let unrelated = sample(20_000, 1)?;
    let pinned = sample(20_000, 1_000)?;
    assert!(
        unrelated < small * 2,
        "unrelated history should not require a ledger scan: {small}/{unrelated}"
    );
    assert!(
        pinned > unrelated * 3,
        "LIMIT1 must not hide the linear pinned prefix cost"
    );
    assert!(
        pinned < 150_000,
        "unexpected work beyond indexed prefix probes: {pinned}"
    );
    Ok(())
}
