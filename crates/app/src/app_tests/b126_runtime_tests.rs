use super::{b126_runtime_fixture::*, entry_risk_clock_fixture as clock};
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn b126_actual_buy_two_sells_then_stop_preserves_open_and_reconciliation() -> Result<()> {
    let mut f = fixture().await?;
    let env = f.build().await?.envelope.unwrap();
    assert_eq!(
        clock::at(f.now + Duration::seconds(1), f.submit(&env))
            .await?
            .submitted,
        1
    );
    finish_buy(&f, 5000)?;
    let buy_id = f.request.order_id.clone();
    for id in ["exit1", "exit2", "exit3"] {
        sell(&mut f, id)?;
        let env = f.build().await?.envelope.unwrap();
        let before = f.calls.lock().unwrap().len();
        let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
        let methods: Vec<_> = f.calls.lock().unwrap()[before..]
            .iter()
            .filter_map(|(_, v)| v["method"].as_str().map(str::to_owned))
            .collect();
        if id != "exit3" {
            assert_eq!(out.submitted, 1, "{out:?}");
            assert_eq!(
                methods,
                vec!["getFeeForMessage", "sendTransaction"],
                "SELL submit does no account/rent scan"
            );
            failed(&f, 7000)?;
        } else {
            assert_eq!(out.submitted, 0);
            assert_eq!(out.reason.as_deref(), Some("tiny_budget_stopped"));
            assert!(methods.is_empty());
        }
    }
    assert_eq!(f.sends(), 3);
    let position = f
        .store
        .load_execution_canary_open_position(&f.request.token)?
        .unwrap();
    assert_eq!(position.position_id, format!("exec-canary-pos:{buy_id}"));
    assert_eq!(position.qty_exact.map(|v| v.raw()), Some(123456));
    let e = f.store.load_tiny_experiment(f.now)?.unwrap();
    assert_eq!(e.state, "stopped");
    let spent: u64 = f.conn()?.query_row(
        "SELECT SUM(actual_fee) FROM execution_tiny_reservations",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(spent, 19000);
    f.finish().await
}
#[tokio::test]
async fn b126_actual_fee_priority_exact_caps_plus_one_and_missing() -> Result<()> {
    for (total, price, allowed) in [
        (Some(100000), 35714, true),
        (Some(100001), 35000, false),
        (Some(100000), 35715, false),
        (None, 10000, false),
    ] {
        let mut f = fixture().await?;
        f.price.store(price, Ordering::SeqCst);
        f.request.metadata.priority_fee_json = Some(
            crate::execution_priority_fee::sample_quicknode_fee(
                &serde_json::json!({"recommended":price}),
            )?
            .1,
        );
        f.funding.lock().unwrap().fee = total;
        let env = f.build().await?.envelope.unwrap();
        let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
        assert_eq!(
            f.sends(),
            usize::from(allowed),
            "total={total:?} price={price} {out:?}"
        );
        if !allowed {
            assert!(f
                .store
                .load_execution_canary_dispatch(&f.request.order_id)?
                .is_none());
        }
        f.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn b126_actual_missing_activation_and_changed_config_are_fail_closed() -> Result<()> {
    for case in ["absent", "no_activation", "changed", "removed"] {
        let mut f = fixture().await?;
        match case {
            "absent" => f.config.tiny_experiment = Default::default(),
            "no_activation" => f.config.tiny_experiment.activate = false,
            _ => {
                f.store.activate_tiny_experiment(
                    "previous",
                    &f.config.canary_wallet_pubkey,
                    f.now,
                )?;
                if case == "removed" {
                    f.config.tiny_experiment = Default::default();
                }
            }
        }
        let env = f.build().await?.envelope.unwrap();
        let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
        assert_eq!(out.submitted, 0, "{case} {out:?}");
        assert_eq!(f.sends(), 0);
        f.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn b126_actual_after_await_deadline_refuses_and_existing_claim_never_sends_twice(
) -> Result<()> {
    let mut f = fixture().await?;
    let env = f.build().await?.envelope.unwrap();
    f.store.activate_tiny_experiment(
        "local-variant-a",
        &f.config.canary_wallet_pubkey,
        f.now - Duration::seconds(3598),
    )?;
    // Funding safety samples first, budget activation/fee observation next, final decision at deadline.
    let out = clock::sequence(
        [
            f.now + Duration::seconds(1),
            f.now + Duration::seconds(1),
            f.now + Duration::seconds(1),
            f.now + Duration::seconds(2),
        ],
        f.submit(&env),
    )
    .await?;
    assert_eq!(f.sends(), 0, "{out:?}");
    assert_eq!(
        f.store
            .load_tiny_experiment(f.now + Duration::seconds(2))?
            .unwrap()
            .state,
        "stopped"
    );
    f.finish().await?;
    let mut f = fixture().await?;
    let env = f.build().await?.envelope.unwrap();
    let intent = crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
        &f.request,
        &env,
        "rpc".into(),
    )?;
    let state = crate::execution_tiny_submit_state::eligible(&f.store, &f.request).unwrap();
    let gate =
        crate::execution_canary_submit_contract::ExecutionTinySubmitGate::from_config(&f.config);
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(
        f.config.submit_adapter_http_url.clone(),
    );
    for _ in 0..2 {
        let out = clock::at(
            f.now + Duration::seconds(1),
            crate::execution_tiny_submit_state::dispatch::send(
                &f.store, &f.request, &intent, &env, &gate, &transport, &state, f.now,
            ),
        )
        .await?;
        assert_eq!(out.submitted, 1);
    }
    assert_eq!(f.sends(), 1);
    f.finish().await
}

#[tokio::test]
async fn b126_actual_late_successful_and_failed_recovery_after_deadline() -> Result<()> {
    use super::{b53_fixture::Fixture, b53_http::Chain};
    for chain in [Chain::Settled, Chain::Failed] {
        let mut f = Fixture::new().await?;
        f.seed("late", 0)?;
        f.tick(0).await?;
        let id = f.order("late")?.order_id;
        let signature = f.order("late")?.tx_signature.unwrap();
        assert_eq!(f.sends().len(), 1);
        let reserved: u64 = f.conn()?.query_row(
            "SELECT SUM(fee_bound) FROM execution_tiny_reservations WHERE actual_fee IS NULL",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(reserved, 100000);
        f.store.load_tiny_experiment(f.now + Duration::hours(2))?;
        f.rpc.state.lock().unwrap().chain.insert(signature, chain);
        f.reopen()?;
        f.tick(7201).await?;
        f.reopen()?;
        f.tick(7202).await?;
        let row: (u64, u64) = f.conn()?.query_row(
            "SELECT COUNT(*),SUM(actual_fee) FROM execution_tiny_reservations",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(row, (1, 7000));
        assert_eq!(f.sends().len(), 1);
        assert_eq!(
            f.store.execution_canary_fill_exists(&id)?,
            matches!(chain, Chain::Settled)
        );
        assert_eq!(
            f.store
                .load_tiny_experiment(f.now + Duration::hours(2))?
                .unwrap()
                .state,
            "stopped"
        );
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn b126_actual_buy_amount_exact_and_plus_one_use_current_payload() -> Result<()> {
    for amount in [10_000_000_u64, 10_000_001] {
        let mut f = fixture().await?;
        f.request.metadata.quote_in_amount_raw = Some(amount.to_string());
        let mut q: serde_json::Value =
            serde_json::from_str(f.request.metadata.quote_response_json.as_deref().unwrap())?;
        q["inAmount"] = serde_json::json!(amount.to_string());
        f.request.metadata.quote_response_json = Some(q.to_string());
        let env = f.build().await?.envelope.unwrap();
        let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
        assert_eq!(
            f.sends(),
            usize::from(amount == 10_000_000),
            "{amount} {out:?}"
        );
        if amount > 10_000_000 {
            assert_eq!(out.reason.as_deref(), Some("tiny_budget_buy_amount"));
        }
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn b126_actual_sell_missing_total_fee_keeps_slot_and_reserve_unused() -> Result<()> {
    let mut f = fixture().await?;
    let env = f.build().await?.envelope.unwrap();
    assert_eq!(
        clock::at(f.now + Duration::seconds(1), f.submit(&env))
            .await?
            .submitted,
        1
    );
    finish_buy(&f, 5000)?;
    sell(&mut f, "exit")?;
    let env = f.build().await?.envelope.unwrap();
    f.funding.lock().unwrap().fee = None;
    let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
    assert_eq!(out.reason.as_deref(), Some("tiny_budget_fee_unknown"));
    assert_eq!(f.sends(), 1);
    assert_eq!(
        f.conn()?.query_row(
            "SELECT COUNT(*) FROM execution_tiny_reservations",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        1
    );
    f.finish().await
}

#[tokio::test]
async fn b126_existing_stricter_priority_cap_is_not_raised() -> Result<()> {
    let mut f = fixture().await?;
    let env = f.build().await?.envelope.unwrap();
    f.config.pretrade_max_priority_fee_lamports = 13999;
    let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
    assert_eq!(out.submitted, 0);
    assert_eq!(f.sends(), 0);
    assert!(out
        .error
        .as_deref()
        .is_some_and(|r| r.starts_with("priority_fee_cap_exceeded")));
    f.finish().await
}

#[tokio::test]
async fn b126_sell_unbound_rpc_response_refuses_before_claim() -> Result<()> {
    use super::native_rpc_fixture::{Fixture, Reply};
    let mut f = fixture().await?;
    let env = f.build().await?.envelope.unwrap();
    assert_eq!(
        clock::at(f.now + Duration::seconds(1), f.submit(&env))
            .await?
            .submitted,
        1
    );
    finish_buy(&f, 5000)?;
    sell(&mut f, "exit")?;
    let env = f.build().await?.envelope.unwrap();
    let rpc=Fixture::start(false,|_|Reply::json(serde_json::json!({"jsonrpc":"2.0","id":"unbound","result":{"context":{"slot":70},"value":19000}}))).await?;
    f.config.submit_adapter_http_url = rpc.endpoint.clone();
    let out = clock::at(f.now + Duration::seconds(1), f.submit(&env)).await?;
    assert_eq!(out.reason.as_deref(), Some("native_rpc_id"));
    assert_eq!(f.sends(), 1);
    assert!(f
        .store
        .load_execution_canary_dispatch(&f.request.order_id)?
        .is_none());
    let requests = rpc.finish().await?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].request["method"], "getFeeForMessage");
    f.finish().await
}
