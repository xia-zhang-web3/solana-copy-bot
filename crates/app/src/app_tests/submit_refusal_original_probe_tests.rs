// Independent reviewer probe. Wire as a sibling of app_tests only in an isolated/root test run.
// R1 contract: original RED preserved in evidence; verify paired diagnostic and actual event.
use super::initial_sol_rpc_fixture::FundingRpc;
use super::native_rpc_fixture::{Fixture as Rpc, Reply};
use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;
use serde_json::json;
use std::sync::{Arc, Mutex};

async fn seed_hot(f: &Fixture) -> Result<copybot_core_types::CopySignalRow> {
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&f.request.signal_id)?
        .unwrap();
    f.conn()?.execute(
        "DELETE FROM orders WHERE order_id=?1",
        [&f.request.order_id],
    )?;
    super::execution_state_machine_tiny_submit_route::record_tiny_route_quote(
        &f.store, &signal, f.now,
    )?;
    f.conn()?.execute(
        "UPDATE execution_quote_canary_events SET quote_price_sol=?1, quote_response_json=?2,
         quote_in_amount_raw=?3, quote_out_amount_raw=?4, route_plan_json=?5 WHERE signal_id=?6",
        rusqlite::params![
            f.request.metadata.quote_price_sol,
            f.request.metadata.quote_response_json,
            f.request.metadata.quote_in_amount_raw,
            f.request.metadata.quote_out_amount_raw,
            f.request.metadata.route_plan_json,
            f.request.signal_id
        ],
    )?;
    Ok(signal)
}

#[tokio::test]
async fn root_b26_postawait_rejection_reaches_actual_tick_summary() -> Result<()> {
    // The generic guarded control uses the same production submit boundary as direct BUY.
    // Its quote/builder server stays unchanged; the real submit endpoint handles simulation,
    // funding and send, and mutates SQLite only once collection has actually started.
    for mutate in [false, true] {
        let mut f = Fixture::new(Route::Metis, 200_000, 200_000).await?;
        f.wire.lock().unwrap().guard = Some(50_000_001);
        f.config.canary_max_open_positions = 10;
        f.config.max_confirm_seconds = 1;
        let signal = seed_hot(&f).await?;
        let conn = Arc::new(Mutex::new(f.conn()?));
        let signal_id = signal.signal_id.clone();
        let before_error: Arc<Mutex<Option<Option<String>>>> = Arc::new(Mutex::new(None));
        let saved_error = before_error.clone();
        let server = Rpc::start(false, move |r| {
            if r["method"] == "getFeeForMessage" && mutate {
                *saved_error.lock().unwrap() = Some(
                    conn.lock()
                        .unwrap()
                        .query_row(
                            "SELECT simulation_error FROM orders WHERE signal_id=?1",
                            [&signal_id],
                            |row| row.get::<_, Option<String>>(0),
                        )
                        .unwrap(),
                );
                assert_eq!(
                    conn.lock().unwrap().execute(
                        "UPDATE copy_signals SET status='root-current-state' WHERE signal_id=?1",
                        [&signal_id],
                    ).unwrap(),
                    1,
                );
            }
            if r["method"] == "simulateTransaction" {
                return Reply::json(json!({
                    "jsonrpc":"2.0", "id":r["id"],
                    "result":{"context":{"slot":42},"value":{"err":null,"logs":[]}}
                }));
            }
            Reply::json(FundingRpc::default().reply(r))
        })
        .await?;
        f.config.submit_adapter_http_url = server.endpoint.clone();
        let state_result = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(2),
            crate::execution_canary_route::process_canary_state_machine_for_route(
                &f.config, &f.store, &signal, f.now,
            ),
        )
        .await;
        f.finish().await?;
        let trace = server.finish().await?;
        let state = state_result?;
        assert_eq!(state.signing_envelope_built, 1, "{state:?}");
        assert_eq!(state.failed, 0, "{state:?}");
        assert_eq!(
            trace
                .iter()
                .filter(|t| t.request["method"] == "getFeeForMessage")
                .count(),
            1
        );
        assert_eq!(
            trace
                .iter()
                .filter(|t| t.request["method"] == "getMultipleAccounts")
                .count(),
            1
        );
        assert_eq!(
            trace
                .iter()
                .filter(|t| t.request["method"] == "getMinimumBalanceForRentExemption")
                .count(),
            1
        );
        let sends = trace
            .iter()
            .filter(|t| t.request["method"] == "sendTransaction")
            .count();
        assert_eq!(sends, usize::from(!mutate));
        let order = f
            .store
            .load_execution_canary_order_by_signal(&signal.signal_id)?
            .unwrap();
        let state_reason = state.last_error.clone();
        let state_rejected = state.submit_ready_rejected;
        if mutate {
            assert_eq!(state_reason.as_deref(), Some("initial_sol_order_changed"));
            assert_eq!(state_rejected, 1);
            assert_eq!(
                order.status,
                copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
            );
            assert!(order.tx_signature.is_none());
            assert!(order.err_code.is_none());
            assert_eq!(
                order.simulation_error,
                before_error.lock().unwrap().clone().unwrap(),
                "preserve the pre-existing simulation diagnostic"
            );
        } else {
            assert_eq!(state_rejected, 0);
            assert!(order.tx_signature.is_some(), "positive control must submit");
        }
        let mut tick = crate::execution_canary::ExecutionCanaryTickSummary::default();
        crate::execution_canary_summary::apply_state_machine_summary(&mut tick, state);
        // ID itself is retained in a single-state summary; the suspected missing part is reason.
        assert_eq!(
            tick.last_state_machine_order_id.as_deref(),
            Some(order.order_id.as_str())
        );
        eprintln!(
            "ROOT_B26_POSTAWAIT {}",
            json!({
                "mutated_during_collection": mutate,
                "sends": sends,
                "state_reason": state_reason,
                "state_submit_ready_rejected": state_rejected,
                "tick_last_error": tick.last_error,
                "tick_skipped_reason": tick.state_machine_skipped_reason,
                "tick_safety_blocked": tick.state_machine_safety_blocked,
                "tick_failed": tick.state_machine_failed,
                "tick_order_id_matches": true,
                "persisted_status": order.status,
                "persisted_error": order.err_code,
            })
        );
        let event = super::submit_refusal_fixture::capture(|| {
            crate::telemetry::record_execution_canary_tick(&tick)
        });
        super::submit_refusal_fixture::check_event(
            &event,
            if mutate {
                order.order_id.as_str()
            } else {
                "none"
            },
            if mutate {
                "initial_sol_order_changed"
            } else {
                "none"
            },
            usize::from(mutate),
        );
        if mutate {
            assert_eq!(
                tick.pre_submit_refusals.reason(),
                "initial_sol_order_changed",
                "no-write post-await rejection must remain observable in the actual tick summary",
            );
        }
    }
    Ok(())
}
