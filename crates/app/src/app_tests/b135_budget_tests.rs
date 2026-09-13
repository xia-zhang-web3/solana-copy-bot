use super::{b135_fixture::Fixture, b135_server::Server};
use anyhow::Result;
use chrono::Utc;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::*;
fn failed_sell(f: &Fixture, n: usize) -> Result<()> {
    let now = Utc::now();
    let id = format!("b135-prior-{n}");
    let wallet = f.meta["our"]["signer"].as_str().unwrap();
    let token = f.meta["our"]["token_out"].as_str().unwrap();
    let signal = CopySignalRow {
        signal_id: id.clone(),
        wallet_id: f.meta["source"]["signer"].as_str().unwrap().into(),
        side: "sell".into(),
        token: token.into(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    };
    f.db.store.insert_copy_signal(&signal)?;
    let o =
        f.db.store
            .reserve_execution_canary_order(&id, "tiny", now)?
            .order;
    f.db.store.mark_execution_canary_built(&o.order_id, now)?;
    let o = f.db.store.mark_execution_canary_simulated(
        &o.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    let d = ExecutionCanaryDispatch {
        order_id: o.order_id.clone(),
        signal_id: id,
        client_order_id: o.client_order_id.clone(),
        route: o.route.clone(),
        attempt: o.attempt,
        wallet: wallet.into(),
        token: token.into(),
        side: "sell".into(),
        tx_signature: format!("synthetic-failed-{n}"),
        message_sha256: "c".repeat(64),
        transaction_sha256: "d".repeat(64),
    };
    let p = TinyBudgetClaim {
        experiment_id: "b135".into(),
        wallet: wallet.into(),
        tx_signature: d.tx_signature.clone(),
        message_sha256: d.message_sha256.clone(),
        transaction_sha256: d.transaction_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: 10000,
        priority_fee: 5000,
        fee_slot: 151,
    };
    f.db.store
        .claim_tiny_experiment_dispatch(&o, &signal, &d, &p, now)?;
    let err = serde_json::json!({"InstructionError":[0,{"Custom":7}]});
    f.db.store.detect_failed_expense(
        &o.order_id,
        wallet,
        "signature_status",
        "confirmed",
        Some(151),
        &err,
        now,
    )?;
    f.db.store.apply_failed_expense(
        &o.order_id,
        &FailedTransactionFacts {
            tx_signature: d.tx_signature,
            wallet: wallet.into(),
            slot: 151,
            commitment: "confirmed".into(),
            transaction_error: err,
            transaction_fee_lamports: Some("10000".into()),
            fee_coverage: FailedExpenseCoverage::Known,
            payer: Some(wallet.into()),
            payer_coverage: FailedExpenseCoverage::Known,
            wallet_native_pre_lamports: Some("100000000".into()),
            wallet_native_post_lamports: Some("99990000".into()),
            native_coverage: FailedExpenseCoverage::Known,
        },
        now,
    )?;
    Ok(())
}
#[tokio::test]
async fn b135_current_budget_first_second_sell_and_third_refusal() -> Result<()> {
    for previous in 0..=2 {
        let f = Fixture::new().await?;
        for n in 0..previous {
            failed_sell(&f, n)?;
        }
        let server = Server::new().await?;
        let c = f.config(&server.url)?;
        f.ingress(&c).await?;
        let r = f.runner(&c)?;
        if previous < 2 {
            f.drive(&r).await?;
            assert_eq!(f.handoffs()?, 1);
            assert_eq!(
                f.db.sql.query_row(
                    "SELECT COUNT(*) FROM execution_tiny_reservations WHERE side='sell'",
                    [],
                    |r| r.get::<_, usize>(0)
                )? + 1,
                previous + 1
            );
            let error = format!("{:#}", failed_sell(&f, 9).unwrap_err());
            assert!(
                error.contains("owner_pending") || error.contains("stopped"),
                "{error}"
            );
        } else {
            let e = f.drive(&r).await.unwrap_err().to_string();
            assert!(e.contains("stopped"), "{e}");
            assert_eq!(f.handoffs()?, 0);
        }
        server.healthy();
    }
    Ok(())
}
#[tokio::test]
async fn b135_unknown_buy_fee_and_protected_policy_remain_bound() -> Result<()> {
    let f = Fixture::with_protected(true).await?;
    f.db.sql.execute(
        "UPDATE execution_canary_receipt_facts SET transaction_fee=NULL,fee_coverage='missing'",
        [],
    )?;
    let server = Server::new().await?;
    let mut c = f.config(&server.url)?;
    c.execution.tiny_experiment.policy_mode =
        copybot_config::TinyPolicyMode::ProtectedNativeCapital;
    f.ingress(&c).await?;
    let policy = f.rows("execution_tiny_native_policy")?;
    let reserved = f.rows("execution_tiny_reservations")?;
    f.drive(&f.runner(&c)?).await?;
    assert_eq!(f.rows("execution_tiny_native_policy")?, policy);
    assert_eq!(f.rows("execution_tiny_reservations")?, reserved);
    assert!(f.db.sql.query_row("SELECT actual_fee IS NULL AND fee_bound=100000 FROM execution_tiny_reservations WHERE side='buy'",[],|r|r.get::<_,bool>(0))?);
    server.healthy();
    Ok(())
}
#[tokio::test]
async fn b135_total_fee_refusal_retains_unsigned_reservation() -> Result<()> {
    let f = Fixture::new().await?;
    let server = Server::new().await?;
    *server.fault.lock().unwrap() = "total_fee".into();
    let c = f.config(&server.url)?;
    f.ingress(&c).await?;
    let before = f.rows("positions")?;
    let e = f.drive(&f.runner(&c)?).await.unwrap_err().to_string();
    assert!(e.contains("fee_or_payload"), "{e}");
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.handoffs()?, 0);
    assert_eq!(f.rows("rpc_owned_sell_handoffs")?.len(), 1);
    server.healthy();
    Ok(())
}
