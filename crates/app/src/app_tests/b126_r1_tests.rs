use super::{
    b126_r1_fixture::*,
    b126_runtime_fixture::{failed, sell},
    receipt_rpc_fixture::Rpc,
};
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use serde_json::{json, Value};

async fn exits(
    case: &str,
    fee: Option<Value>,
    coverage: ReceiptFeeCoverage,
    payer_known: bool,
) -> Result<()> {
    let mut f = dispatched().await?;
    let buy = f.request.order_id.clone();
    let rpc = Rpc::new(receipt(&f, fee, payer_known)).await?;
    let outcome = reconcile(&f, &rpc, &buy).await?;
    assert_eq!(outcome.confirmed, 1, "{outcome:?}");
    assert_eq!(
        outcome.buy_opened, 1,
        "must reach actual canonical BUY accounting"
    );
    reopen(&mut f)?;
    let facts = f.store.load_execution_canary_receipt_facts(&buy)?.unwrap();
    assert_eq!(facts.fee_coverage, coverage);
    assert_eq!(
        facts.fee_payer.as_deref(),
        payer_known.then_some(f.config.canary_wallet_pubkey.as_str())
    );
    assert_eq!(facts.wallet_native_delta.as_i128(), -10005000);
    assert_eq!(facts.token_delta.unwrap().raw, 123456);
    let known = coverage == ReceiptFeeCoverage::Known && payer_known;
    let budget: (Option<u64>, u64, Option<String>) = f.conn()?.query_row(
        "SELECT actual_fee,fee_bound,outcome FROM execution_tiny_reservations WHERE order_id=?1",
        [&buy],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(
        budget,
        (
            known.then_some(5000),
            100000,
            known.then(|| "successful".into())
        )
    );
    let before = accounting(&f)?;
    let calls = rpc.calls.lock().unwrap().len();
    let again = reconcile(&f, &rpc, &buy).await?;
    assert_eq!(again.reason.as_deref(), Some("receipt_already_accounted"));
    assert_eq!(
        rpc.calls.lock().unwrap().len(),
        calls,
        "no historical receipt fetch"
    );
    assert_eq!(
        accounting(&f)?,
        before,
        "no duplicate accounting or reserve release"
    );
    if !known {
        let mut enriched = facts.clone();
        enriched.transaction_fee = Some(copybot_core_types::Lamports::new(5000));
        enriched.fee_coverage = ReceiptFeeCoverage::Known;
        enriched.fee_payer = Some(f.config.canary_wallet_pubkey.clone());
        assert!(
            f.store
                .record_execution_canary_receipt_facts(&enriched, f.now)
                .is_err(),
            "historical facts remain immutable"
        );
        assert_eq!(accounting(&f)?, before);
    }
    readback(&f, case, "open")?;
    for (i, id) in ["exit1", "exit2", "exit3"].into_iter().enumerate() {
        sell(&mut f, id)?;
        let env = f.build().await?.envelope.unwrap();
        let out = super::entry_risk_clock_fixture::at(f.now + Duration::seconds(1), f.submit(&env))
            .await?;
        readback(&f, case, &format!("claim-{i}"))?;
        if i < 2 {
            assert_eq!(
                out.submitted, 1,
                "{case}: protected SELL {i} must pass: {out:?}"
            );
            failed(&f, 7000)?;
            let once = accounting(&f)?;
            failed(&f, 7000)?;
            assert_eq!(accounting(&f)?, once, "failed fee is applied once");
        } else {
            assert_eq!(out.submitted, 0);
            assert_eq!(out.reason.as_deref(), Some("tiny_budget_stopped"));
        }
        reopen(&mut f)?;
    }
    assert_eq!(f.sends(), 3);
    let position = f
        .store
        .load_execution_canary_open_position(&f.request.token)?
        .unwrap();
    assert_eq!(position.position_id, format!("exec-canary-pos:{buy}"));
    assert_eq!(position.qty_exact.unwrap().raw(), 123456);
    assert_eq!(
        position.cost_lamports.unwrap().as_u64(),
        10005000,
        "BUY native debit unchanged"
    );
    let final_budget: (Option<u64>, u64) = f.conn()?.query_row(
        "SELECT actual_fee,fee_bound FROM execution_tiny_reservations WHERE order_id=?1",
        [&buy],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(final_budget, (known.then_some(5000), 100000));
    assert_eq!(
        f.conn()?
            .query_row("SELECT COUNT(*) FROM fills", [], |r| r.get::<_, u64>(0))?,
        1
    );
    readback(&f, case, "stopped")?;
    rpc.finish().await?;
    f.finish().await
}

#[tokio::test]
async fn b126_r1_known_fee_control_exits() -> Result<()> {
    exits("known", Some(json!(5000)), ReceiptFeeCoverage::Known, true).await
}
#[tokio::test]
async fn b126_r1_missing_fee_keeps_reserve_and_allows_exits() -> Result<()> {
    exits("missing", None, ReceiptFeeCoverage::Missing, true).await
}
#[tokio::test]
async fn b126_r1_null_fee_keeps_reserve_and_allows_exits() -> Result<()> {
    exits("null", Some(Value::Null), ReceiptFeeCoverage::Missing, true).await
}
#[tokio::test]
async fn b126_r1_invalid_fee_keeps_reserve_and_allows_exits() -> Result<()> {
    for (case, fee) in [
        ("negative", json!(-1)),
        ("fractional", json!(1.5)),
        ("string", json!("5000")),
        ("overflow", serde_json::from_str("18446744073709551616")?),
    ] {
        exits(case, Some(fee), ReceiptFeeCoverage::Invalid, true).await?;
    }
    Ok(())
}
#[tokio::test]
async fn b126_r1_unknown_payer_keeps_reserve_and_allows_exits() -> Result<()> {
    exits(
        "unknown-payer",
        Some(json!(5000)),
        ReceiptFeeCoverage::Known,
        false,
    )
    .await
}
