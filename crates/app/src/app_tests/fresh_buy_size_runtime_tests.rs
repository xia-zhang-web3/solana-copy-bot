use super::fresh_buy_size_fixture::close;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use anyhow::Result;
use copybot_storage_core::{EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_FAILED};

async fn assert_runtime(retry: bool, execute: bool) -> Result<()> {
    let (i0, o0, i1, o1) = if execute {
        (20_000_000, 200, 10_000_000, 100)
    } else {
        (10_000_000, 100, 20_000_000, 180)
    };
    let mut f = RuntimeFixture::new(
        &format!("fresh-size-runtime-{retry}-{execute}"),
        i0,
        o0,
        i1,
        o1,
        retry,
    )
    .await?;
    let result = async {
        if retry {
            let summary = f.sweep().await?;
            assert_eq!(
                summary.entry_gate_blocked,
                usize::from(!execute),
                "{summary:?}"
            );
            assert_eq!(
                summary.signing_envelope_built,
                usize::from(execute),
                "{summary:?}"
            );
            assert_eq!(summary.simulated, usize::from(execute));
            if !execute {
                assert_eq!(
                    summary.skipped_reason,
                    Some("fresh_submit_quote_slippage_above_limit")
                );
            }
        } else {
            let summary = f.hot().await?;
            assert_eq!(summary.quote_entry_existing, 1);
            assert_eq!(
                summary.state_machine_entry_gate_blocked,
                usize::from(!execute),
                "{summary:?}"
            );
            assert_eq!(
                summary.state_machine_reserved,
                usize::from(execute),
                "{summary:?}"
            );
            assert_eq!(summary.state_machine_built, usize::from(execute));
            assert_eq!(summary.state_machine_simulated, usize::from(execute));
        }
        let order = f
            .store
            .load_execution_canary_order_by_signal(&f.signal.signal_id)?;
        if execute {
            super::initial_sol_rpc_fixture::assert_funded_buy_trace(
                &f.calls(),
                &[
                    "quote",
                    "build-instructions",
                    "build-transaction",
                    "simulateTransaction",
                    "sendTransaction",
                    "getSignatureStatuses",
                    "getTransaction",
                ],
            );
            let order = order.unwrap();
            assert_eq!(order.status, EXECUTION_STATUS_CANARY_CONFIRMED);
            assert_eq!(
                order.tx_signature.as_deref(),
                f.submitted_signature().as_deref()
            );
            assert_eq!(order.attempt, if retry { 2 } else { 1 });
            let m = f
                .store
                .load_execution_canary_build_plan_metadata(&order.order_id)?
                .unwrap();
            assert_eq!(m.quote_in_amount_raw.as_deref(), Some("10000000"));
            assert_eq!(m.quote_out_amount_raw.as_deref(), Some("100"));
            close(m.quote_price_sol.unwrap(), 0.0001, 1e-18);
            close(m.slippage_bps.unwrap(), 0.0, 1e-9);
            assert_eq!(m.priority_fee_lamports, Some(22_000));
            assert_eq!(
                m.quote_event_id.as_deref(),
                Some(format!("quote:entry:{}", f.signal.signal_id).as_str())
            );
            let response: serde_json::Value =
                serde_json::from_str(m.quote_response_json.as_deref().unwrap())?;
            assert_eq!(
                response.pointer("/_copybot/outDecimals"),
                Some(&serde_json::json!(0))
            );
            let position = f
                .store
                .load_execution_canary_open_position(&f.signal.token)?
                .unwrap();
            assert_eq!(
                position.qty_exact,
                Some(copybot_core_types::TokenQuantity::new(100, 0))
            );
            close(position.cost_sol, 0.010007, 1e-12);
        } else {
            assert_eq!(
                f.calls(),
                ["quote"],
                "no build, simulation, signing transport or receipt calls"
            );
            assert_eq!(f.store.execution_canary_open_position_count()?, 0);
            if retry {
                let order = order.unwrap();
                assert_eq!(order.status, EXECUTION_STATUS_CANARY_FAILED);
                assert_eq!(order.tx_signature, None);
                assert_eq!(order.err_code.as_deref(), Some("build_failed"));
                let m = f
                    .store
                    .load_execution_canary_build_plan_metadata(&order.order_id)?
                    .unwrap();
                assert_eq!(
                    m.quote_in_amount_raw.as_deref(),
                    Some("10000000"),
                    "blocked retry does not replace durable build metadata"
                );
            } else {
                assert!(order.is_none());
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.finish().await?;
    result
}

#[tokio::test]
async fn fresh_buy_size_first_hot_buy_a_blocks_before_build() -> Result<()> {
    assert_runtime(false, false).await
}
#[tokio::test]
async fn fresh_buy_size_first_hot_buy_b_reaches_signed_submit_and_receipt() -> Result<()> {
    assert_runtime(false, true).await
}
#[tokio::test]
async fn fresh_buy_size_reopened_retry_a_blocks_before_build() -> Result<()> {
    assert_runtime(true, false).await
}
#[tokio::test]
async fn fresh_buy_size_reopened_retry_b_uses_durable_metadata_without_entry_quote() -> Result<()> {
    assert_runtime(true, true).await
}

#[tokio::test]
async fn fresh_buy_size_runtime_missing_old_input_is_unknown() -> Result<()> {
    for retry in [false, true] {
        let f = RuntimeFixture::new(
            &format!("fresh-size-missing-{retry}"),
            20_000_000,
            200,
            10_000_000,
            100,
            retry,
        )
        .await?;
        let table = if retry {
            "execution_canary_build_plan_metadata"
        } else {
            "execution_quote_canary_events"
        };
        let conn = rusqlite::Connection::open(&f.db_path)?;
        conn.execute(&format!("UPDATE {table} SET quote_in_amount_raw=NULL"), [])?;
        drop(conn);
        if retry {
            let s = f.sweep().await?;
            assert_eq!(s.entry_gate_blocked, 1);
            assert_eq!(s.skipped_reason, Some("fresh_submit_quote_invalid_price"));
            assert_eq!(s.signing_envelope_built, 0);
        } else {
            let s = f.hot().await?;
            assert_eq!(s.state_machine_entry_gate_blocked, 1);
            assert_eq!(s.state_machine_reserved, 0);
        }
        assert_eq!(f.calls(), ["quote"]);
    }
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_periodic_prefilter_keeps_original_input_check() -> Result<()> {
    let f = RuntimeFixture::new(
        "fresh-size-periodic",
        20_000_000,
        200,
        10_000_000,
        100,
        false,
    )
    .await?;
    let summary = super::ExecutionCanaryRunner::new(f.config.clone())
        .process_tick(&f.store, f.now)
        .await?;
    assert_eq!(summary.candidates, 0);
    assert_eq!(summary.state_machine_reserved, 0);
    assert!(f.calls().is_empty());
    assert!(f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .is_none());
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_hot_disabled_and_kill_switch_do_not_request_quotes() -> Result<()> {
    for kill_switch in [false, true] {
        let mut f = RuntimeFixture::new(
            &format!("fresh-size-disabled-{kill_switch}"),
            20_000_000,
            200,
            10_000_000,
            100,
            false,
        )
        .await?;
        let flag = f.db_path.with_extension("kill");
        if kill_switch {
            std::fs::write(&flag, b"test-only")?;
            f.config.canary_kill_switch_path = flag.to_string_lossy().into_owned();
        } else {
            f.config.canary_enabled = false;
        }
        let summary = f.hot().await?;
        assert_eq!(
            summary.skipped_reason,
            Some(if kill_switch {
                "kill_switch_active"
            } else {
                "disabled"
            })
        );
        assert_eq!(summary.state_machine_reserved, 0);
        assert!(f.calls().is_empty());
        if kill_switch {
            std::fs::remove_file(flag)?;
        }
    }
    Ok(())
}
