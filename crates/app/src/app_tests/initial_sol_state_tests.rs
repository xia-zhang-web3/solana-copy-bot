use super::initial_sol_rpc_fixture::FundingRpc;
use super::native_rpc_fixture::{Fixture as Rpc, Reply};
use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn initial_sol_postawait_state_changes_preserve_order_on_rpc_success_and_error() -> Result<()>
{
    for rpc_error in [false, true] {
        for change in [
            "submitted",
            "confirmed",
            "pending",
            "signature",
            "attempt",
            "signal",
            "receipt",
        ] {
            let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
            let envelope = f.build().await?.envelope.unwrap();
            let conn = Arc::new(Mutex::new(f.conn()?));
            let id = f.request.order_id.clone();
            let signal = f.request.signal_id.clone();
            let before = f.store.load_execution_canary_order(&id)?.unwrap();
            let server = Rpc::start_with_in_flight(3, move |r| {
                if r["method"] == "getFeeForMessage" {
                    let c = conn.lock().unwrap();
                    match change {
                        "submitted" | "confirmed" | "pending" => {
                            let status = match change {
                                "submitted" => "execution_canary_submitted",
                                "confirmed" => "execution_canary_confirmed",
                                _ => "execution_canary_confirmed_unreconciled",
                            };
                            c.execute("UPDATE orders SET status=?2, tx_signature='other-live-signature' WHERE order_id=?1", rusqlite::params![id,status]).unwrap();
                        }
                        "signature" => { c.execute("UPDATE orders SET tx_signature='other-live-signature' WHERE order_id=?1", [&id]).unwrap(); }
                        "attempt" => { c.execute("UPDATE orders SET attempt=attempt+1 WHERE order_id=?1", [&id]).unwrap(); }
                        "signal" => { c.execute("UPDATE copy_signals SET status='other-current-status' WHERE signal_id=?1", [&signal]).unwrap(); }
                        "receipt" => {
                            c.execute("INSERT INTO copy_signals (signal_id,wallet_id,side,token,notional_sol,ts,status,notional_lamports,notional_origin) SELECT 'new-pending-signal', wallet_id, side, token, notional_sol, ts, status, notional_lamports, notional_origin FROM copy_signals WHERE signal_id=?1", [&signal]).unwrap();
                            c.execute("INSERT INTO orders (order_id,signal_id,route,submit_ts,confirm_ts,status,err_code,client_order_id,tx_signature,simulation_status,simulation_error,attempt) SELECT 'exec-canary:new-pending', 'new-pending-signal', route, submit_ts, confirm_ts, 'execution_canary_confirmed_unreconciled', err_code, 'other-client', 'pending-signature', simulation_status, simulation_error, attempt FROM orders WHERE order_id=?1", [&id]).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }
                let mut reply = Reply::json(FundingRpc::default().reply(r));
                if rpc_error && r["method"] == "getFeeForMessage" { reply.body = b"{".to_vec(); }
                reply
            }).await?;
            f.config.submit_adapter_http_url = server.endpoint.clone();
            let result = f.submit(&envelope).await;
            f.finish().await?;
            let trace = server.finish().await?;
            let out = result?;
            assert_eq!(
                out.reason.as_deref(),
                Some("initial_sol_order_changed"),
                "{change}/{rpc_error}: {out:?}"
            );
            assert_eq!(out.failed, 0);
            assert!(trace
                .iter()
                .all(|t| t.request["method"] != "sendTransaction"));
            let after = f
                .store
                .load_execution_canary_order(&f.request.order_id)?
                .unwrap();
            assert_eq!(after.err_code, before.err_code);
            assert_eq!(after.simulation_error, before.simulation_error);
            if ["submitted", "confirmed", "pending", "signature"].contains(&change) {
                assert_eq!(after.tx_signature.as_deref(), Some("other-live-signature"));
            } else {
                assert_eq!(after.tx_signature, before.tx_signature);
            }
            if change == "attempt" {
                assert_eq!(after.attempt, before.attempt + 1);
            }
            assert_eq!(
                f.conn()?.query_row(
                    "SELECT COUNT(*) FROM execution_failed_expense_ledger",
                    [],
                    |r| r.get::<_, i64>(0)
                )?,
                0
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_postawait_new_loss_fresh_clock_and_sql_error_are_no_send() -> Result<()> {
    for change in ["loss", "clock", "sql"] {
        let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
        f.config.canary_max_open_positions = 10;
        f.store.record_execution_canary_open_position(
            "loss-source",
            "OtherLossMint",
            1.0,
            Some(copybot_core_types::TokenQuantity::new(1, 0)),
            0.02,
            f.now,
        )?;
        let envelope = f.build().await?.envelope.unwrap();
        let before = f
            .store
            .load_execution_canary_order(&f.request.order_id)?
            .unwrap();
        let conn = Arc::new(Mutex::new(f.conn()?));
        let stamp = (f.now + chrono::Duration::seconds(1)).to_rfc3339();
        let server = Rpc::start_with_in_flight(3, move |r| {
            if r["method"] == "getFeeForMessage" {
                let c = conn.lock().unwrap();
                match change {
                    "loss" => { c.execute("UPDATE positions SET state='closed', closed_ts=?1, pnl_lamports=-20000000, pnl_sol=-0.02 WHERE token='OtherLossMint'", [&stamp]).unwrap(); }
                    "sql" => { c.execute("ALTER TABLE orders RENAME TO delayed_orders", []).unwrap(); }
                    _ => {}
                }
            }
            Reply::json(FundingRpc::default().reply(r))
        }).await?;
        f.config.submit_adapter_http_url = server.endpoint.clone();
        let sampled = if change == "clock" {
            f.now
        } else {
            f.now + chrono::Duration::seconds(2)
        };
        let out = super::entry_risk_clock_fixture::at(sampled, f.submit(&envelope)).await;
        let trace = server.finish().await?;
        if change == "sql" {
            f.conn()?
                .execute("ALTER TABLE delayed_orders RENAME TO orders", [])?;
        }
        let out = out?;
        let expected = match change {
            "loss" => "max_daily_loss",
            "clock" => "risk_decision_clock_unordered",
            _ => "tiny_submit_state_unavailable",
        };
        assert_eq!(out.reason.as_deref(), Some(expected), "{change}: {out:?}");
        assert_eq!(out.failed, 0);
        assert_eq!(
            f.store
                .load_execution_canary_order(&f.request.order_id)?
                .unwrap(),
            before
        );
        assert_eq!(trace.len(), 3);
        assert!(trace
            .iter()
            .all(|t| t.request["method"] != "sendTransaction"));
        // Same transport/gate failure conditions do not apply to an eligible SELL B.
        let mut signal = f
            .store
            .load_copy_signal_by_signal_id(&f.request.signal_id)?
            .unwrap();
        signal.signal_id = "separate-allowed-sell".into();
        signal.side = "sell".into();
        f.store.insert_copy_signal(&signal)?;
        let order = f
            .store
            .reserve_execution_canary_order(&signal.signal_id, &f.config.canary_route, f.now)?
            .order;
        f.request.order_id = order.order_id;
        f.request.client_order_id = order.client_order_id;
        f.request.signal_id = signal.signal_id;
        f.request.attempt = order.attempt;
        f.make_sell()?;
        let sell = f.build().await?.envelope.unwrap();
        let server = Rpc::start(false, |r| {
            assert_eq!(r["method"], "sendTransaction");
            Reply::json(json!({"result":"synthetic-sell-after-funding-error"}))
        })
        .await?;
        f.config.submit_adapter_http_url = server.endpoint.clone();
        let result = f.submit(&sell).await;
        f.finish().await?;
        let trace = server.finish().await?;
        assert_eq!(result?.submitted, 1);
        assert_eq!(trace.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_postawait_durable_proof_change_is_rechecked_before_send() -> Result<()> {
    let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let conn = Arc::new(Mutex::new(f.conn()?));
    let id = f.request.order_id.clone();
    let server = Rpc::start_with_in_flight(3, move |r| {
        if r["method"] == "getFeeForMessage" {
            conn.lock().unwrap().execute("UPDATE execution_canary_build_plan_metadata SET priority_fee_json='{}' WHERE order_id=?1", [&id]).unwrap();
        }
        Reply::json(FundingRpc::default().reply(r))
    }).await?;
    f.config.submit_adapter_http_url = server.endpoint.clone();
    let out = f.submit(&envelope).await;
    f.finish().await?;
    let trace = server.finish().await?;
    let out = out?;
    assert_eq!(out.failed, 1, "{out:?}");
    assert_eq!(
        out.error.as_deref(),
        Some("priority_fee_durable_proof_missing")
    );
    assert_eq!(trace.len(), 3);
    assert!(trace
        .iter()
        .all(|t| t.request["method"] != "sendTransaction"));
    assert_eq!(
        f.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}
