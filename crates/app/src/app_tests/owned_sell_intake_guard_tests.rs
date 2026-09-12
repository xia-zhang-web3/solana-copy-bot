use super::open_risk_sell_fixture::TOKEN;
use super::owned_sell_intake_fixture::Intake;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::TokenQuantity;

#[tokio::test]
async fn owned_sell_intake_permission_and_position_negative_controls() -> Result<()> {
    for case in [
        "no_position",
        "closed_position",
        "wrong_mint",
        "unfollowed",
        "db_demoted",
        "temporal_miss",
        "future_position",
        "latest_buy",
        "global_closed",
        "shadow_disabled",
    ] {
        let mut f = Intake::new(0.1, 120, 600_000).await?;
        match case {
            "no_position" => {
                f.conn()?.execute("DELETE FROM positions", [])?;
            }
            "closed_position" => {
                f.f.store.close_execution_canary_open_position(
                    TOKEN,
                    7.0,
                    Some(TokenQuantity::new(7000, 3)),
                    0.01,
                    1e-12,
                    f.f.now,
                )?;
            }
            "wrong_mint" => f.f.swap.token_in = "OtherMint".into(),
            "unfollowed" => f.f.follow.active.clear(),
            "db_demoted" => {
                f.f.store.deactivate_follow_wallet(
                    "leader",
                    f.f.now - Duration::seconds(1),
                    "test-demote",
                )?;
            }
            "temporal_miss" => {
                f.conn()?.execute(
                    "UPDATE followlist SET added_at = ?1",
                    [f.f.now.to_rfc3339()],
                )?;
            }
            "future_position" => {
                f.conn()?.execute(
                    "UPDATE positions SET opened_ts = ?1",
                    [f.f.now.to_rfc3339()],
                )?;
            }
            "latest_buy" => {
                f.f.prior_order("buy", f.f.now, true)?;
            }
            "shadow_disabled" => {
                let mut cfg = copybot_config::ShadowConfig::default();
                cfg.enabled = false;
                f.f.service = copybot_shadow::ShadowService::new(cfg);
            }
            _ => {}
        }
        f.dispatch(case == "global_closed", false).await?;
        assert!(f.drain().await?.is_none(), "{case}");
        assert!(
            f.f.store.load_copy_signal_by_signal_id(&f.id())?.is_none(),
            "{case}"
        );
        assert_eq!(f.f.sends(), 0);
        assert_eq!(f.f.store.shadow_open_lots_count()?, 0);
        let reason = match case {
            "no_position" | "closed_position" | "wrong_mint" => Some("owned_sell_no_position"),
            "db_demoted" => Some("owned_sell_source_not_active"),
            "temporal_miss" => Some("owned_sell_source_temporal_miss"),
            "future_position" => Some("owned_sell_before_position"),
            "latest_buy" => Some("owned_sell_before_latest_buy"),
            _ => None,
        };
        if let Some(reason) = reason {
            assert_eq!(f.reasons.get(reason), Some(&1), "{case}");
            let saved: String = f.conn()?.query_row(
                "SELECT reason FROM execution_quote_canary_shadow_gate_events WHERE signal_id = ?1 AND status = 'shadow_dropped'",
                [f.id()], |row| row.get(0))?;
            assert_eq!(saved, reason, "{case}");
        }
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_downstream_guards_survive_intent_and_restart() -> Result<()> {
    for guard in [
        "pending_receipt",
        "in_flight",
        "invalid_signer",
        "fee_cap",
        "kill_switch",
        "canary_disabled",
        "tiny_disabled",
        "quote_disabled",
    ] {
        let mut f = Intake::new(
            0.1,
            120,
            if guard == "fee_cap" {
                2_500_001
            } else {
                600_000
            },
        )
        .await?;
        f.dispatch(false, false).await?;
        let signal = f.drain().await?.expect("raw intent");
        match guard {
            "pending_receipt" => {
                f.f.prior_order("sell", f.f.now - Duration::hours(1), true)?;
            }
            "in_flight" => {
                f.f.prior_order("sell", f.f.now, false)?;
            }
            "invalid_signer" => f.f.config.execution_signer_pubkey.clear(),
            "kill_switch" => std::fs::write(&f.f.config.canary_kill_switch_path, b"stop")?,
            "canary_disabled" => f.f.config.canary_enabled = false,
            "tiny_disabled" => f.f.config.canary_tiny_submit_enabled = false,
            "quote_disabled" => f.f.config.quote_canary_enabled = false,
            _ => {}
        }
        f.f.reopen()?;
        let summary = f.hot(&signal).await?;
        assert_eq!(f.f.sends(), 0, "{guard}: {summary:?}");
        match guard {
            "pending_receipt" | "in_flight" => assert_eq!(
                summary.state_machine_skipped_reason,
                Some("sell_token_in_flight")
            ),
            "invalid_signer" => assert_eq!(
                summary.state_machine_skipped_reason,
                Some("missing_execution_signer_pubkey")
            ),
            "kill_switch" => assert_eq!(summary.skipped_reason, Some("kill_switch_active")),
            "canary_disabled" => assert_eq!(summary.skipped_reason, Some("disabled")),
            "fee_cap" => {
                assert_eq!(summary.state_machine_failed, 1);
                let order =
                    f.f.store
                        .load_execution_canary_order_by_signal(&f.id())?
                        .unwrap();
                assert!(
                    order
                        .simulation_error
                        .as_deref()
                        .unwrap_or("")
                        .contains("priority_fee_cap_exceeded"),
                    "{order:?}"
                );
            }
            _ => assert_eq!(summary.state_machine_reserved, 0),
        }
        assert_eq!(f.f.store.shadow_open_lots_count()?, 0);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_queued_work_rechecks_shadow_and_owned_state() -> Result<()> {
    for change in ["shadow_buy", "owned_closed", "newer_buy", "source_demoted"] {
        let mut f = Intake::new(0.1, 120, 600_000).await?;
        let key = crate::shadow_scheduler::ShadowTaskKey {
            wallet: "leader".into(),
            token: TOKEN.into(),
        };
        // A predecessor owns the scheduler key; the raw SELL must wait for it.
        f.scheduler.inflight_shadow_keys.insert(key.clone());
        f.dispatch(false, true).await?;
        assert_eq!(f.scheduler.pending_shadow_task_count, 1);
        assert!(f.drain().await?.is_none());
        assert_eq!(f.counts()?, (0, 0, 0, 0));
        match change {
            "shadow_buy" => {
                f.f.store.insert_shadow_lot_exact(
                    "leader",
                    TOKEN,
                    20.0,
                    Some(TokenQuantity::new(20_000, 3)),
                    0.2,
                    f.f.swap.ts_utc - Duration::seconds(1),
                )?;
            }
            "owned_closed" => {
                f.conn()?
                    .execute("UPDATE positions SET state = 'closed'", [])?;
            }
            "newer_buy" => {
                f.f.prior_order("buy", f.f.now, false)?;
            }
            _ => {
                f.f.store
                    .deactivate_follow_wallet("leader", f.f.now, "test-demote")?;
            }
        }
        f.scheduler.mark_task_complete(&key);
        let signal = f.drain().await?;
        if change == "shadow_buy" {
            let signal = signal.expect("real shadow risk takes precedence");
            assert_eq!(signal.closed_qty, 10.0);
            assert_eq!(
                f.f.store
                    .load_copy_signal_by_signal_id(&f.id())?
                    .unwrap()
                    .status,
                "shadow_recorded"
            );
            assert_eq!(
                f.f.store.list_shadow_lots("leader", TOKEN)?[0]
                    .qty_exact
                    .unwrap()
                    .raw(),
                10_000
            );
            let outcome = f.hot(&signal).await?;
            // The queued Shadow/owned selection still reaches dispatch. This legacy
            // position has no explicit variant A activation and cannot spend its budget.
            assert_eq!(outcome.pre_submit_refusals.reason(), "tiny_budget_inactive");
            assert_eq!(outcome.pre_submit_refusals.count(), 1);
            assert_eq!(f.f.sends(), 0);
        } else {
            assert!(signal.is_none(), "{change}");
            assert_eq!(f.f.sends(), 0);
        }
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn owned_sell_intake_final_submit_rechecks_position_after_build() -> Result<()> {
    use crate::execution_submit_adapter::*;
    for change in ["future", "closed", "latest_buy"] {
        let mut f = Intake::new(0.1, 120, 600_000).await?;
        f.dispatch(false, false).await?;
        f.drain().await?.unwrap();
        let signal = f.f.store.load_copy_signal_by_signal_id(&f.id())?.unwrap();
        let order = f
            .f
            .store
            .reserve_execution_canary_order(&signal.signal_id, &f.f.config.canary_route, f.f.now)?
            .order;
        let request = ExecutionSubmitRequest {
            order_id: order.order_id.clone(),
            signal_id: signal.signal_id,
            client_order_id: order.client_order_id,
            attempt: order.attempt,
            route: order.route,
            wallet_id: signal.wallet_id,
            token: signal.token,
            side: signal.side,
            buy_size_sol: 0.01,
            slippage_tolerance_bps: 500,
            wallet_pubkey: f.f.config.canary_wallet_pubkey.clone(),
            entry_route_plan_json: None,
            metadata: super::priority_fee_fixture::metadata(),
        };
        let envelope = super::priority_fee_fixture::envelope(&f.f.store, &request, f.f.now)?;
        f.f.store
            .mark_execution_canary_built(&order.order_id, f.f.now)?;
        f.f.store.mark_execution_canary_simulated(
            &order.order_id,
            f.f.now,
            copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        let expected = match change {
            "future" => {
                f.conn()?
                    .execute("UPDATE positions SET opened_ts=?1", [f.f.now.to_rfc3339()])?;
                "sell_before_position"
            }
            "closed" => {
                f.conn()?.execute("DELETE FROM positions", [])?;
                "no_owned_position"
            }
            _ => {
                f.f.prior_order("buy", f.f.now, false)?;
                "sell_before_latest_buy"
            }
        };
        let out = crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
            &f.f.store,
            &NoSubmitExecutionAdapter,
            &request,
            &envelope,
            &crate::execution_canary_submit_contract::ExecutionTinySubmitGate {
                buy_safety_config: None,
                allow_rpc_submit: true,
                pretrade_max_priority_fee_lamports: 500_000,
                pretrade_min_sol_reserve: 0.05,
                execution_wallet_pubkey: request.wallet_pubkey.clone(),
                submit_timeout_ms: 500,
            },
            &RpcExecutionSubmitTransport::new(f.f.config.submit_adapter_http_url.clone()),
            f.f.now,
        )
        .await?;
        assert_eq!(out.reason.as_deref(), Some(expected));
        assert_eq!(out.submit_ready_rejected, 1);
        assert_eq!(f.f.sends(), 0);
        assert_eq!(
            f.f.store
                .load_execution_canary_order(&order.order_id)?
                .unwrap()
                .status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
        );
        f.finish().await?;
    }
    Ok(())
}
