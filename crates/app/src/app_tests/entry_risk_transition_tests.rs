use super::buy_retry_safety_fixture::rows;
use super::entry_risk_clock_fixture::sequence;
use super::entry_risk_same_tick_tests::TickRpc;
use super::execution_state_machine_tiny_submit_route::tiny_route_keypair;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::ExecutionCanaryRunner;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

async fn fixed_pair(name: &str) -> Result<(RuntimeFixture, String, TickRpc)> {
    let mut f = RuntimeFixture::new(name, 10_000_000, 100, 10_000_000, 100, false).await?;
    f.finish().await?;
    // Prepare clean input rows at a fixed historical event time, before any order exists.
    f.now = DateTime::parse_from_rfc3339("2020-01-01T23:59:54Z")?.with_timezone(&Utc);
    f.signal.ts = f.now;
    let conn = rusqlite::Connection::open(&f.db_path)?;
    conn.execute("UPDATE copy_signals SET ts=?1", [f.now.to_rfc3339()])?;
    conn.execute(
        "UPDATE execution_quote_canary_events SET signal_ts=?1,request_ts=?1",
        [f.now.to_rfc3339()],
    )?;
    let mut second = f.signal.clone();
    second.signal_id.push_str(":second");
    second.ts += Duration::seconds(1);
    f.store.insert_copy_signal(&second)?;
    let mut quote = f
        .store
        .load_latest_execution_quote_canary_entry_event(&f.signal.signal_id)?
        .unwrap();
    quote.event_id.push_str(":second");
    quote.signal_id = Some(second.signal_id.clone());
    quote.signal_ts = Some(second.ts);
    quote.request_ts = second.ts;
    f.store.record_execution_quote_canary_event(&quote)?;
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM orders", [], |r| r.get::<_, i64>(0))?,
        0
    );
    f.config.quote_canary_enabled = false;
    f.config.canary_batch_limit = 2;
    f.config.canary_max_open_positions = 10;
    f.config.canary_max_daily_loss_sol = 0.000007;
    let key = tiny_route_keypair(81);
    let rpc = TickRpc::new(key.public_key, key.pubkey).await?;
    f.config.quote_canary_base_url = rpc.url.clone();
    f.config.submit_adapter_http_url = rpc.url.clone();
    Ok((f, second.signal_id, rpc))
}

#[tokio::test]
async fn entry_risk_single_tick_rechecks_clock_and_uses_new_utc_day() -> Result<()> {
    for cross_midnight in [false, true] {
        let (f, second, mut rpc) = fixed_pair(&format!("r1-midnight-{cross_midnight}")).await?;
        let tick = f.now + Duration::seconds(4); // 23:59:58 event/tick boundary.
        let first_claim = tick + Duration::milliseconds(200);
        let decision = tick + Duration::milliseconds(if cross_midnight { 3000 } else { 300 });
        let out = sequence(
            // Selection + candidate, then five clock checks through final claim.
            // The second candidate sees a later instant after the durable failed fee.
            [tick + Duration::milliseconds(100), first_claim]
                .into_iter()
                .chain(std::iter::repeat_n(first_claim, 5))
                .chain([decision]),
            ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, tick),
        )
        .await;
        rpc.finish().await?;
        let out = out?;
        let calls = rpc.calls.lock().unwrap().clone();
        let sends = calls
            .iter()
            .filter(|s| s.as_str() == "sendTransaction")
            .count();
        assert_eq!(sends, 1, "a new UTC day never rearms the lifetime BUY slot");
        assert_eq!(out.candidates, 2);
        assert_eq!(
            f.store
                .load_execution_canary_order_by_signal(&second)?
                .is_some(),
            cross_midnight
        );
        let cost = out.state_machine_entry_cost.unwrap();
        assert_eq!(cost.as_of, decision.to_rfc3339());
        assert_eq!(
            cost.since,
            if cross_midnight {
                "2020-01-02T00:00:00+00:00"
            } else {
                "2020-01-01T00:00:00+00:00"
            }
        );
        assert_eq!(
            cost.known_total_lamports.as_deref().unwrap(),
            if cross_midnight { "0" } else { "7000" }
        );
        assert_eq!(cost, f.store.execution_canary_entry_cost(decision)?);
        assert_eq!(
            out.state_machine_skipped_reason,
            if cross_midnight {
                None
            } else {
                Some("max_daily_loss")
            }
        );
        let conn = rusqlite::Connection::open(&f.db_path)?;
        let first = f
            .store
            .load_execution_canary_order_by_signal(&f.signal.signal_id)?
            .unwrap();
        assert_eq!(first.submit_ts, first_claim);
        let experiment = f.store.load_tiny_experiment(decision)?.unwrap();
        assert_eq!(experiment.state, "stopped");
        assert_eq!(
            experiment.stop_reason.as_deref(),
            Some("tiny_budget_buy_failed")
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM execution_tiny_reservations WHERE side='buy'",
                [],
                |r| r.get::<_, u64>(0)
            )?,
            1
        );
        if cross_midnight {
            let candidate = f
                .store
                .load_execution_canary_order_by_signal(&second)?
                .unwrap();
            assert!(candidate.tx_signature.is_none());
            assert!(f
                .store
                .load_execution_canary_dispatch(&candidate.order_id)?
                .is_none());
        }
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM execution_failed_expense_ledger",
                [],
                |r| r.get::<_, usize>(0)
            )?,
            sends
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM fills", [], |r| r.get::<_, usize>(0))?,
            0
        );
        // Half-open history and original attribution still show all fees in the old day.
        assert_eq!(
            f.store
                .execution_canary_entry_cost(tick)?
                .known_total_lamports
                .as_deref()
                .unwrap(),
            "0"
        );
        assert_eq!(
            f.store
                .execution_canary_entry_cost(tick + Duration::seconds(1))?
                .known_total_lamports
                .as_deref()
                .unwrap(),
            (7000 * sends).to_string()
        );
        eprintln!("R1 midnight={cross_midnight}, tick={tick}, last_decision={decision}, sends={sends}, calls={calls:?}");
    }
    Ok(())
}

#[tokio::test]
async fn entry_risk_backwards_clock_does_not_allow_buy_after_selection() -> Result<()> {
    let (f, _, mut rpc) = fixed_pair("r1-backwards").await?;
    let tick = f.now + Duration::seconds(4);
    let before = rows(&f)?;
    let out = sequence(
        [
            tick + Duration::milliseconds(900),
            tick + Duration::milliseconds(500),
        ],
        ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, tick),
    )
    .await;
    rpc.finish().await?;
    let out = out?;
    assert_eq!(out.candidates, 2);
    assert_eq!(out.state_machine_safety_blocked, 2);
    assert_eq!(
        out.state_machine_skipped_reason,
        Some("risk_decision_clock_unordered")
    );
    assert!(rpc.calls.lock().unwrap().is_empty());
    assert_eq!(rows(&f)?, before);
    Ok(())
}
