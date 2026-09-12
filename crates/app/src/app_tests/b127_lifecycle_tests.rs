use super::{
    b126_r1_fixture as receipt,
    b126_runtime_fixture::{failed, sell},
    b127_causal_fixture::*,
    b127_runtime_tests::prepared,
    priority_fee_route_fixture::Route,
    receipt_rpc_fixture::Rpc,
};
use anyhow::Result;
use copybot_storage_core::ReceiptFeeCoverage;
#[tokio::test]
async fn b127_canonical_missing_buy_fee_open_two_sells_and_once_accounting() -> Result<()> {
    let mut f = prepared(Route::Metis).await?;
    f.funding.lock().unwrap().fee = Some(100_000);
    let envelope = f.build().await?.envelope.unwrap();
    assert_eq!(f.submit(&envelope).await?.submitted, 1);
    let buy = f.request.order_id.clone();
    let anchor = f.store.tiny_native_policy(
        "local-variant-a",
        &f.request.wallet_pubkey,
        chrono::Utc::now(),
    )?;
    let rpc = Rpc::new(receipt::receipt(&f, None, true)).await?;
    let out = receipt::reconcile(&f, &rpc, &buy).await?;
    assert_eq!((out.confirmed, out.buy_opened), (1, 1));
    receipt::reopen(&mut f)?;
    assert_eq!(
        f.store
            .load_execution_canary_receipt_facts(&buy)?
            .unwrap()
            .fee_coverage,
        ReceiptFeeCoverage::Missing
    );
    let once = receipt::accounting(&f)?;
    assert_eq!(
        receipt::reconcile(&f, &rpc, &buy).await?.reason.as_deref(),
        Some("receipt_already_accounted")
    );
    assert_eq!(receipt::accounting(&f)?, once);
    f.wire.lock().unwrap().bundle = None; // existing generic SELL fixture; no new floor/scan
    for (i, id) in ["protected-exit1", "protected-exit2", "protected-exit3"]
        .into_iter()
        .enumerate()
    {
        sell(&mut f, id)?;
        let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
            &f.request.wallet_pubkey,
            "test-wallet",
        )?;
        let mut bundle = super::generic_sell_synthetic_fixture::bundle(wallet, 1_400_000, 10_000);
        bundle["blockhashWithMetadata"]["blockhash"] = serde_json::json!(vec![20 + i as u8; 32]);
        f.wire.lock().unwrap().bundle = Some(bundle);
        let env = f.build().await?.envelope.unwrap();
        let before = f.calls.lock().unwrap().len();
        let out = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(1),
            f.submit(&env),
        )
        .await?;
        if i < 2 {
            assert_eq!(out.submitted, 1, "{out:?}");
            let methods = f.calls.lock().unwrap()[before..]
                .iter()
                .filter_map(|(_, v)| v["method"].as_str().map(str::to_owned))
                .collect::<Vec<_>>();
            assert_eq!(methods, vec!["getFeeForMessage", "sendTransaction"]);
            failed(&f, 7000)?;
            let once = receipt::accounting(&f)?;
            failed(&f, 7000)?;
            assert_eq!(receipt::accounting(&f)?, once);
        } else {
            assert_eq!(out.submitted, 0);
        }
        receipt::reopen(&mut f)?;
    }
    let row:(Option<u64>,u64,Option<String>,Option<u64>)=f.conn()?.query_row("SELECT actual_fee,fee_bound,outcome,buy_lamports FROM execution_tiny_reservations WHERE order_id=?1",[&buy],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    assert_eq!(row, (None, 100_000, None, None));
    assert_eq!(f.sends(), 3);
    assert_eq!(
        f.store
            .load_execution_canary_open_position(&f.request.token)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        123456
    );
    let stored: (String, String) = f.conn()?.query_row(
        "SELECT initial_lamports,floor_lamports FROM execution_tiny_native_policy",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(stored, (B.to_string(), F.to_string()));
    assert_eq!(
        anchor.deadline,
        anchor.activated_at + chrono::Duration::hours(1)
    );
    receipt::readback(&f, "b127-missing", "stopped")?;
    rpc.finish().await?;
    f.finish().await
}
#[tokio::test]
async fn b127_actual_daemon_restart_and_failed_buy_never_rearm() -> Result<()> {
    let mut f = super::b53_fixture::Fixture::new().await?;
    f.config.tiny_experiment.policy_mode = copybot_config::TinyPolicyMode::ProtectedNativeCapital;
    f.config.quote_canary_slippage_bps = 500; // Match the fixed quote fixture request.
    f.backend.funding.lock().unwrap().balance = B;
    f.seed("b127-a", 0)?;
    let out = f.tick(0).await?;
    assert_eq!(f.sends().len(), 1, "{out:?} order={:?}", f.order("b127-a")?);
    let a = f.order("b127-a")?;
    let policy = f.store.tiny_native_policy(
        "local-variant-a",
        &f.config.canary_wallet_pubkey,
        f.now + chrono::Duration::seconds(1),
    )?;
    f.rpc
        .state
        .lock()
        .unwrap()
        .chain
        .insert(a.tx_signature.unwrap(), super::b53_http::Chain::Failed);
    f.reopen()?;
    f.tick(2).await?;
    f.backend.funding.lock().unwrap().balance += 900_000_000;
    f.backend.wire.lock().unwrap().blockhash = 17;
    f.seed("b127-b", 3)?;
    f.reopen()?;
    f.tick(3).await?;
    assert_eq!(f.sends().len(), 1);
    assert!(
        f.store
            .tiny_native_policy(
                "local-variant-a",
                &f.config.canary_wallet_pubkey,
                f.now + chrono::Duration::seconds(4)
            )
            .is_err(),
        "failed BUY keeps experiment stopped"
    );
    let pinned: (String,String,String,String) = f.conn()?.query_row(
        "SELECT p.initial_lamports,p.floor_lamports,e.activated_at,e.deadline FROM execution_tiny_native_policy p JOIN execution_tiny_experiment e USING(experiment_id)", [],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    assert_eq!(
        pinned,
        (
            policy.initial_lamports.to_string(),
            policy.floor_lamports.to_string(),
            policy.activated_at.to_rfc3339(),
            policy.deadline.to_rfc3339()
        )
    );
    let row: (u64, u64) = f.conn()?.query_row(
        "SELECT COUNT(*),SUM(actual_fee) FROM execution_tiny_reservations",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(row, (1, 7000));
    f.finish().await
}

#[tokio::test]
async fn b127_actual_retry_reassembles_new_blockhash_at_original_floor() -> Result<()> {
    for extension in [false, true] {
        let mut f = prepared(Route::Direct).await?;
        f.config.quote_canary_slippage_bps = 500;
        f.wire.lock().unwrap().extension = extension;
        f.sync_config();
        let old = f.build().await?.envelope.unwrap();
        let old_wire = crate::execution_transaction_wire::decode_message(
            old.signed_transaction_base64.as_ref().unwrap(),
            |_| Ok(()),
        )?;
        let anchor = f.store.tiny_native_policy(
            "local-variant-a",
            &f.request.wallet_pubkey,
            chrono::Utc::now(),
        )?;
        // No transport occurred. This existing canonical retry marker requires a new build.
        f.store.mark_execution_canary_retry_after_submit_not_sent(
            &f.request.order_id,
            f.now,
            crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON,
        )?;
        f.wire.lock().unwrap().blockhash = 17;
        let outcome = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(2),
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &f.config, &f.store, f.now,
            ),
        )
        .await?
        .unwrap();
        assert_eq!(f.sends(), usize::from(!extension), "{outcome:?}");
        let simulated = f
            .calls
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(_, v)| v["method"] == "simulateTransaction")
            .unwrap()
            .1["params"][0]
            .as_str()
            .unwrap()
            .to_owned();
        let wallet =
            crate::execution_pumpswap_accounts::parse_pubkey(&f.request.wallet_pubkey, "wallet")?;
        let verified =
            crate::execution_native_floor::verify_final_native_floor(&simulated, wallet, F)?;
        let new_wire = crate::execution_transaction_wire::decode_message(&simulated, |_| Ok(()))?;
        assert_eq!(new_wire.recent_blockhash, [17; 32]);
        assert_ne!(
            verified.binding().message_bytes,
            old_wire.binding.message_bytes
        );
        assert_eq!(
            f.store.tiny_native_policy(
                "local-variant-a",
                &f.request.wallet_pubkey,
                f.now + chrono::Duration::seconds(3)
            )?,
            anchor
        );
        if !extension {
            let sent = f
                .calls
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(_, v)| v["method"] == "sendTransaction")
                .unwrap()
                .1["params"][0]
                .as_str()
                .unwrap()
                .to_owned();
            trace(&f, &sent, F)?;
        } else {
            assert_eq!(
                outcome.last_error.as_deref(),
                Some("initial_sol_unsupported_setup")
            );
        }
        f.finish().await?;
    }
    Ok(())
}
