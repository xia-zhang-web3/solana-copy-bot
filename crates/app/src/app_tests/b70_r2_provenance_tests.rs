use super::{b64_http_fixture as http, b70_job_fixture::Jobs, b70_r2_fixture::Ready, *};
use anyhow::{ensure, Result};

#[tokio::test]
async fn b70_r2_actual_cost_qty_id_unknown_foreign_mutations_stay_refused() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    for (case, change) in [
        ("cost_to_signal", "cost_lamports=67000000"),
        ("cost_plus_one", "cost_lamports=cost_lamports+1"),
        ("cost_sol", "cost_sol=cost_sol+0.000000001"),
        ("qty", "qty=qty+0.000001"),
        ("raw_qty", "qty_raw='1'"),
        ("id", "id=id+100"),
        ("foreign", "wallet_id='foreign'"),
        ("unknown_cost", "cost_lamports=NULL"),
        ("unknown_qty", "qty_raw=NULL, qty_decimals=NULL"),
        ("missing", "qty=qty"),
    ] {
        let mut f = Ready::new(0.067, false).await?;
        let origin = f.origin.take().unwrap();
        let proof = origin.buy_receipt.as_ref().unwrap();
        proof.verify(&f.f.f.store, &f.swap)?;
        let sql = if case == "missing" {
            "DELETE FROM shadow_lots WHERE token='TokenA'".into()
        } else {
            format!("UPDATE shadow_lots SET {change} WHERE token='TokenA'")
        };
        f.f.f.conn()?.execute(&sql, [])?;
        ensure!(proof.verify(&f.f.f.store, &f.swap).is_err(), "{case}");
        f.runner
            .resume_hot_buy(
                &f.f.f.store,
                origin,
                &f.f.f.follow,
                false,
                &mut f.risk,
                &OperatorEmergencyStop::from_env(),
                true,
                Utc::now(),
            )
            .await?;
        ensure!(
            f.f.f
                .store
                .execution_quote_entry_refused(&f.signal.signal_id)?,
            "{case}"
        );
        ensure!(!f.orders()?);
        f.unchanged_quote_facts()?;
    }
    Ok(())
}

#[tokio::test]
async fn b70_r2_late_priority_completion_and_replay_cannot_clear_refusal() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let seed = f.admission()?;
    let (output, served) = tokio::join!(seed.job.run(), http::serve(listener, Default::default()));
    served?;
    f.runner
        .finish_hot_quote(f.store(), &seed.origin, Ok(output), Utc::now(), None)?;
    drop(seed.origin);
    let before = f.event()?;
    ensure!(before.priority_fee_lamports.is_none());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    f.config.priority_fee_canary_enabled = true;
    f.config.priority_fee_canary_rpc_url = format!("http://{}", listener.local_addr()?);
    f.config.priority_fee_canary_timeout_ms = 2_000;
    f.runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.config.clone());
    let job = f.admission()?;
    let origin = job.origin;
    let (output, served) = tokio::join!(job.job.run(), async {
        let fee = http::accept(&listener).await?;
        ensure!(fee.body["method"] == "qn_estimatePriorityFees");
        // The owner refuses A while its old priority request is still held.
        f.runner
            .refuse_hot_buy(f.store(), &origin, "hot_quote_operator_stop")?;
        fee.reply(
            200,
            serde_json::json!({"jsonrpc":"2.0","result":{"recommended":0}}),
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    });
    served?;
    ensure!(
        f.runner
            .finish_hot_quote(f.store(), &origin, Ok(output), Utc::now(), None)?
            == "hot_quote_entry_changed"
    );
    // A second refusal also preserves the first reason and the original provider facts.
    f.runner
        .refuse_hot_buy(f.store(), &origin, "hot_quote_stale")?;
    let signal = origin.signal_id();
    drop(origin);
    let refused = f.event()?;
    ensure!(refused.decision_reason.as_deref() == Some("hot_buy_refused:hot_quote_operator_stop"));
    let mut normalized = refused.clone();
    normalized.decision_status = before.decision_status.clone();
    normalized.decision_reason = before.decision_reason.clone();
    ensure!(normalized == before);
    f.store().record_execution_quote_canary_event(&before)?;
    ensure!(f.event()? == refused && f.counts()? == (1, 2));
    let reopened = SqliteStore::open(f.path())?;
    let quotes = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.config.clone());
    ensure!(!f.runner.entry_pending(&signal));
    ensure!(quotes
        .prepare_hot_quote(&reopened, &f.swap, Utc::now())?
        .is_none());
    quotes
        .process_tick(&reopened, "shadow_recorded", Utc::now(), f.swap.ts_utc, 1)
        .await?;
    ensure!(f.event()? == refused);
    http::no_more(&listener).await?;
    Ok(())
}
