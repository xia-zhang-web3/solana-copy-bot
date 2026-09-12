use super::entry_cost_cash_rpc_fixture::cash_server;
use super::failed_expense_runtime_tests::failed_receipt;
use super::receipt_reconciliation_fixture::{add_order, config, receipt, Fixture, TOKEN};
use anyhow::Result;
use chrono::Duration;
use serde_json::{json, Value};

async fn settle(f: &Fixture, mut value: Value, failed: bool, seconds: i64) -> Result<()> {
    let signature = f
        .store
        .load_execution_canary_order(&f.order_id)?
        .unwrap()
        .tx_signature
        .unwrap();
    value["result"]["transaction"]["signatures"] = json!([signature]);
    let (url, task) = cash_server(value, failed).await?;
    let result = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &f.store,
        &config(&url),
        &f.order_id,
        &reqwest::Client::new(),
        &url,
        f.now + Duration::seconds(seconds),
        200,
    )
    .await;
    assert_eq!(task.await??, ["getSignatureStatuses", "getTransaction"]);
    let result = result?;
    assert_eq!(result.confirmation_confirmed, usize::from(!failed));
    assert_eq!(result.confirmation_failed, usize::from(failed));
    Ok(())
}
fn next(f: &mut Fixture, name: &str) -> Result<()> {
    f.order_id = add_order(&f.store, name, "sell", TOKEN, f.now, false)?;
    f.store
        .mark_execution_canary_submitted(&f.order_id, f.now, &format!("cash-{name}"))?;
    Ok(())
}
fn snapshot(f: &Fixture) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    let conn = f.conn()?;
    let names = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'execution_%' OR name IN ('orders','positions','fills')) ORDER BY name")?
        .query_map([], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for name in names {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {name} ORDER BY rowid"))?;
        let columns = stmt.column_count();
        rows.extend(
            stmt.query_map([], |r| {
                (0..columns)
                    .map(|i| r.get_ref(i).map(|v| format!("{v:?}")))
                    .collect()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    Ok(rows)
}

#[tokio::test]
async fn entry_cost_actual_buy_partial_full_sell_and_failed_fee_count_cash_once() -> Result<()> {
    let mut f = Fixture::new("buy")?;
    settle(&f, receipt("buy", -10_007_000), false, 1).await?;
    next(&mut f, "failed")?;
    settle(&f, failed_receipt("sell", 3), true, 2).await?;
    assert_eq!(
        f.store
            .execution_canary_entry_cost(f.now + Duration::seconds(3))?
            .known_total_lamports
            .as_deref()
            .unwrap(),
        "3"
    );
    for (i, sold, cash) in [(0, 2000, 3_000_000), (1, 5000, 6_000_000)] {
        next(&mut f, &format!("partial-{i}"))?;
        let mut value = receipt("sell", cash);
        value["result"]["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] =
            json!(sold.to_string());
        value["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("0");
        settle(&f, value, false, 4 + i).await?;
        f.reopen()?;
        let cost = f
            .store
            .execution_canary_entry_cost(f.now + Duration::seconds(8))?;
        assert_eq!(
            cost.known_total_lamports.as_deref().unwrap(),
            if i == 0 { "3" } else { "1147860" }
        );
        assert_eq!(
            cost.cash_loss.additional_loss_lamports.as_deref(),
            Some(if i == 0 { "0" } else { "140857" })
        );
        if i == 1 {
            assert!(cost.check_cap(0.001147860)?.exhausted);
        }
    }
    let state: (String, String, i64, i64) = f.conn()?.query_row(
        "SELECT state,qty_raw,cost_lamports,pnl_lamports FROM positions",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    assert_eq!(state, ("closed".into(), "0".into(), 0, -1_007_000));
    assert_eq!(
        f.conn()?.query_row(
            "SELECT count(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    let before = snapshot(&f)?;
    for _ in 0..3 {
        f.reopen()?;
        let cost = f
            .store
            .execution_canary_entry_cost(f.now + Duration::seconds(8))?;
        assert_eq!(cost.closed_loss.loss_lamports, "1007000");
        assert_eq!(
            cost.failed_expenses.known_wallet_fee_lamports.as_deref(),
            Some("3")
        );
        assert_eq!(cost.known_total_lamports.as_deref().unwrap(), "1147860");
        assert_eq!(snapshot(&f)?, before);
    }
    Ok(())
}
