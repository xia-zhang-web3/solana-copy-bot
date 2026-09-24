//! The daemon tick exercises owner authority, shared adapter, dispatch and accounting.
use super::owner_buy_fixture::{self as fixture, SignedAdapter, MINT};
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_storage_core::{
    owner_technical_buy_order_id, ExecutionOrderOrigin, SqliteStore,
};
use std::sync::Arc;

struct Case {
    root: super::temporary_output_fixture::OutputRoot,
    store: SqliteStore,
    config: copybot_config::ExecutionConfig,
    server: fixture::Server,
    adapter: Arc<SignedAdapter>,
}
impl Case {
    async fn new() -> Result<Self> {
        let root = super::temporary_output_fixture::OutputRoot::new("owner-buy")?;
        let (payload, signature, payer) = fixture::signed_payload()?;
        let wallet = bs58::encode(payer).into_string();
        let server = fixture::Server::start(wallet.clone(), signature.clone()).await?;
        let mut store = SqliteStore::open(root.path().join("state.db"))?;
        store.run_migrations(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../migrations"))?;
        let config = fixture::config(&wallet, &server.url,
            &root.path().join("stop").to_string_lossy());
        copybot_config::validate_owner_technical_buy(&config)?;
        let adapter = Arc::new(SignedAdapter { config: config.clone(), payload,
            signature, fail_simulation: false, wrong_blueprint: false,
            simulation_time: None, kill_during_simulation: false,
            simulation_calls: Default::default(), signing_calls: Default::default() });
        Ok(Self { root, store, config, server, adapter })
    }
    async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone())
            .for_ingestion(&fixture::ingestion(), &self.root.path().join("state.db").to_string_lossy())?
            .with_owner_buy_adapter(self.adapter.clone())
            .process_tick(&self.store, Utc::now()).await
    }
    fn order_id(&self) -> String { owner_technical_buy_order_id("owner-test-intent") }
    fn calls(&self, method: &str) -> usize {
        self.server.calls.lock().unwrap().iter().filter(|m| *m == method).count()
    }
}

#[tokio::test]
async fn owner_buy_daemon_tick_opens_accounted_position_once_without_discovery() -> Result<()> {
    let c = Case::new().await?;
    let sql = rusqlite::Connection::open(c.root.path().join("state.db"))?;
    let signals: i64 = sql.query_row("SELECT count(*) FROM copy_signals", [], |r| r.get(0))?;
    let followed: i64 = sql.query_row("SELECT count(*) FROM followlist", [], |r| r.get(0))?;
    assert_eq!((signals, followed), (0, 0));
    assert!(c.store.load_tiny_experiment(Utc::now())?.is_none());
    let outcome = c.tick().await?;
    assert_eq!(outcome.state_machine_reserved, 1, "{outcome:?}");
    assert_eq!(c.calls("sendTransaction"), 1, "{outcome:?}");
    assert!(c.store.execution_canary_fill_exists(&c.order_id())?);
    let experiment = c.store.load_tiny_experiment(Utc::now())?.expect("daemon activated tiny budget");
    assert_eq!(experiment.id, "owner-test-run");
    let facts = c.store.load_execution_canary_receipt_facts(&c.order_id())?
        .expect("mock receipt recorded");
    assert_eq!(facts.tx_signature, c.adapter.signature);
    assert_eq!(facts.wallet_pubkey, c.config.canary_wallet_pubkey);
    assert_eq!(facts.token, MINT);
    assert_eq!(facts.transaction_fee.map(|fee| fee.as_u64()), Some(19_000));
    assert_eq!(facts.token_delta.expect("token delta").raw, 1_000);
    assert_eq!(facts.wallet_native_delta.as_i128(), -10_019_000);
    let position = c.store.load_execution_canary_open_position(MINT)?.expect("buy position");
    assert_eq!(position.cost_lamports.expect("exact buy basis").as_u64(), 10_019_000);
    let (basis, initial_result): (u64, i64) = sql.query_row(
        "SELECT cost_lamports,pnl_lamports FROM positions WHERE position_id=?1",
        [&position.position_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    assert_eq!((basis, initial_result), (10_019_000, 0));
    let reservation: (u64, Option<u64>, u64, Option<String>) = sql.query_row(
        "SELECT fee_bound,actual_fee,buy_lamports,outcome FROM execution_tiny_reservations WHERE order_id=?1",
        [c.order_id()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
    assert_eq!(reservation.1, Some(19_000));
    assert_eq!(reservation.2, 10_000_000);
    assert_eq!(reservation.3.as_deref(), Some("successful"));
    assert!(reservation.0 >= reservation.1.unwrap());
    assert!(c.store.load_execution_canary_open_position(MINT)?.is_some());
    assert!(matches!(c.store.execution_order_origin(&c.order_id())?,
        Some(ExecutionOrderOrigin::OwnerTechnicalBuy { intent_id })
            if intent_id == "owner-test-intent"));
    assert_eq!(sql.query_row("SELECT count(*) FROM copy_signals", [], |r| r.get::<_,i64>(0))?, 0);
    let second = c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1, "{second:?}");
    let reopened = SqliteStore::open(c.root.path().join("state.db"))?;
    let mut disabled = c.config.clone();
    disabled.owner_technical_buy.as_mut().unwrap().activate = false;
    let result = crate::execution_canary::ExecutionCanaryRunner::new(disabled)
        .for_ingestion(&fixture::ingestion(), &c.root.path().join("state.db").to_string_lossy())?
        .with_owner_buy_adapter(c.adapter.clone())
        .process_tick(&reopened, Utc::now()).await?;
    assert_eq!(c.calls("sendTransaction"), 1, "{result:?}");
    Ok(())
}

#[tokio::test]
async fn owner_buy_does_not_promote_a_copy_buy_with_discovery_red() -> Result<()> {
    let c = Case::new().await?;
    let sql = rusqlite::Connection::open(c.root.path().join("state.db"))?;
    sql.execute("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status)
        VALUES('unadmitted-copy','unfollowed-leader','buy',?1,0.01,?2,'shadow_recorded')",
        rusqlite::params![MINT,Utc::now().to_rfc3339()])?;
    assert_eq!(sql.query_row("SELECT count(*) FROM followlist", [], |r| r.get::<_,i64>(0))?, 0);
    let result = c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1, "{result:?}");
    assert!(c.store.load_execution_canary_order_by_signal("unadmitted-copy")?.is_none());
    assert!(c.store.execution_canary_fill_exists(&c.order_id())?);
    Ok(())
}

#[tokio::test]
async fn owner_buy_wrong_chain_mint_and_quote_are_rejected_before_reserve() -> Result<()> {
    for mode in ["wrong_genesis", "token2022", "wrong_mint", "wrong_amount"] {
        let c = Case::new().await?;
        *c.server.mode.lock().unwrap() = mode;
        let result = c.tick().await?;
        assert!(result.state_machine_skipped_reason.is_some(), "{mode}: {result:?}");
        assert!(c.store.load_execution_canary_order(&c.order_id())?.is_none(), "{mode}");
        assert_eq!(c.calls("sendTransaction"), 0, "{mode}");
    }
    Ok(())
}

#[tokio::test]
async fn owner_buy_expired_and_unapproved_have_no_new_risk() -> Result<()> {
    let mut c = Case::new().await?;
    c.config.owner_technical_buy.as_mut().unwrap().activate = false;
    c.tick().await?;
    assert_eq!(c.calls("getGenesisHash"), 0);
    c.config.owner_technical_buy.as_mut().unwrap().activate = true;
    c.config.owner_technical_buy.as_mut().unwrap().expires_at =
        (Utc::now() - Duration::seconds(1)).to_rfc3339();
    let result = c.tick().await?;
    assert_eq!(result.state_machine_skipped_reason, Some("owner_buy_authority_inactive"));
    assert_eq!(c.calls("sendTransaction"), 0);
    assert!(c.store.load_execution_canary_order(&c.order_id())?.is_none());
    Ok(())
}

#[tokio::test]
async fn owner_buy_simulation_failure_and_unknown_send_never_retry() -> Result<()> {
    let mut failed = Case::new().await?;
    Arc::make_mut(&mut failed.adapter).fail_simulation = true;
    let result = failed.tick().await?;
    assert_eq!(failed.calls("sendTransaction"), 0, "{result:?}");
    assert!(failed.store.load_execution_canary_order(&failed.order_id())?.is_some());
    failed.tick().await?;
    assert_eq!(failed.calls("sendTransaction"), 0);

    let unknown = Case::new().await?;
    *unknown.server.mode.lock().unwrap() = "unknown_send";
    let result = unknown.tick().await?;
    assert_eq!(unknown.calls("sendTransaction"), 1, "{result:?}");
    assert!(unknown.store.load_execution_canary_dispatch(&unknown.order_id())?.is_some());
    assert!(!unknown.store.execution_canary_fill_exists(&unknown.order_id())?);
    unknown.tick().await?;
    assert_eq!(unknown.calls("sendTransaction"), 1);
    *unknown.server.mode.lock().unwrap() = "ok";
    let reopened = SqliteStore::open(unknown.root.path().join("state.db"))?;
    let mut disabled = unknown.config.clone();
    disabled.owner_technical_buy.as_mut().unwrap().activate = false;
    let after_restart = crate::execution_canary::ExecutionCanaryRunner::new(disabled)
        .for_ingestion(&fixture::ingestion(),
            &unknown.root.path().join("state.db").to_string_lossy())?
        .with_owner_buy_adapter(unknown.adapter.clone())
        .process_tick(&reopened, Utc::now()).await?;
    assert_eq!(unknown.calls("sendTransaction"), 1, "{after_restart:?}");
    assert!(reopened.execution_canary_fill_exists(&unknown.order_id())?);
    Ok(())
}

#[tokio::test]
async fn owner_buy_identity_and_financial_caps_fail_before_rpc() -> Result<()> {
    let c = Case::new().await?;
    let mut wrong_wallet = c.config.clone();
    wrong_wallet.owner_technical_buy.as_mut().unwrap().wallet_pubkey =
        "11111111111111111111111111111111".into();
    assert!(copybot_config::validate_owner_technical_buy(&wrong_wallet).is_err());
    let mut wrong_amount = c.config.clone();
    wrong_amount.owner_technical_buy.as_mut().unwrap().amount_lamports = 9_000_000;
    assert!(copybot_config::validate_owner_technical_buy(&wrong_amount).is_err());
    let mut over_priority = c.config.clone();
    over_priority.owner_technical_buy.as_mut().unwrap().max_priority_fee_lamports = 50_001;
    assert!(copybot_config::validate_owner_technical_buy(&over_priority).is_err());
    let mut over_slippage = c.config.clone();
    over_slippage.quote_canary_buy_slippage_bps = 100;
    assert!(copybot_config::validate_owner_technical_buy(&over_slippage).is_err());
    let mut under_reserve = c.config.clone();
    under_reserve.pretrade_min_sol_reserve = 0.049;
    assert!(copybot_config::validate_owner_technical_buy(&under_reserve).is_err());
    let mut over_loss = c.config.clone();
    over_loss.canary_max_daily_loss_sol = 0.021;
    assert!(copybot_config::validate_owner_technical_buy(&over_loss).is_err());
    assert_eq!(c.calls("getGenesisHash"), 0);
    assert_eq!(c.calls("sendTransaction"), 0);
    Ok(())
}

#[tokio::test]
async fn owner_buy_rejects_adapter_blueprint_amount_substitution() -> Result<()> {
    let mut c = Case::new().await?;
    Arc::get_mut(&mut c.adapter).expect("fixture owns adapter").wrong_blueprint = true;
    let result = c.tick().await;
    assert!(result.is_err() || result?.state_machine_skipped_reason.is_some());
    assert_eq!(c.calls("sendTransaction"), 0);
    assert!(c.store.load_execution_canary_dispatch(&c.order_id())?.is_none());
    Ok(())
}

#[tokio::test]
async fn owner_buy_simulation_boundary_rechecks_expiry_kill_and_quote_before_signing() -> Result<()> {
    use std::sync::atomic::Ordering;
    for (fault, expected) in [
        ("expiry", "owner_buy_expired"),
        ("kill", "owner_buy_kill_switch"),
        ("quote", "owner_buy_quote_stale"),
    ] {
        let mut c = Case::new().await?;
        let clock = Utc::now() + Duration::seconds(1);
        let adapter = Arc::get_mut(&mut c.adapter).unwrap();
        match fault {
            "expiry" => adapter.simulation_time = Some(
                chrono::DateTime::parse_from_rfc3339(
                    &c.config.owner_technical_buy.as_ref().unwrap().expires_at,
                )?.with_timezone(&Utc)),
            "kill" => adapter.kill_during_simulation = true,
            "quote" => adapter.simulation_time = Some(clock + Duration::seconds(31)),
            _ => unreachable!(),
        }
        let error = super::entry_risk_clock_fixture::at(clock, c.tick()).await
            .expect_err("authority must be rechecked after simulation");
        assert!(format!("{error:#}").contains(expected), "{fault}: {error:#}");
        assert_eq!(c.adapter.simulation_calls.load(Ordering::SeqCst), 1, "{fault}");
        assert_eq!(c.adapter.signing_calls.load(Ordering::SeqCst), 0, "{fault}");
        assert_eq!(c.calls("sendTransaction"), 0, "{fault}");
        assert!(c.store.load_execution_canary_dispatch(&c.order_id())?.is_none());
        assert!(!c.store.execution_canary_fill_exists(&c.order_id())?);
    }
    Ok(())
}

#[tokio::test]
async fn owner_buy_failed_receipt_recovers_after_restart_without_resend_or_duplicate_fee() -> Result<()> {
    let c = Case::new().await?;
    *c.server.mode.lock().unwrap() = "failed_receipt_unavailable";
    let result = c.tick().await?;
    assert_eq!(c.calls("sendTransaction"), 1, "{result:?}");
    let order = c.store.load_execution_canary_order(&c.order_id())?.unwrap();
    assert_eq!(order.status, copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED);
    let task = c.store.load_failed_expense_task(&c.order_id())?.unwrap();
    assert_eq!(task.status, "pending");
    assert_eq!(task.reason, "failed_receipt_unavailable");
    assert_eq!(c.store.list_owner_technical_buy_recovery_intents()?.len(), 1);
    let sql = rusqlite::Connection::open(c.root.path().join("state.db"))?;
    assert_eq!(sql.query_row("SELECT count(*) FROM execution_failed_expense_ledger", [],
        |r| r.get::<_, i64>(0))?, 0);
    let held: (u64, Option<u64>, Option<String>) = sql.query_row(
        "SELECT fee_bound,actual_fee,outcome FROM execution_tiny_reservations WHERE order_id=?1",
        [c.order_id()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    assert!(held.0 >= 19_000);
    assert_eq!((held.1, held.2), (None, None));

    let order_id = c.order_id();
    let Case { root, store, config, server, adapter } = c;
    drop(sql);
    drop(store);
    *server.mode.lock().unwrap() = "failed_receipt_available";
    let reopened = SqliteStore::open(root.path().join("state.db"))?;
    let mut disabled = config;
    let policy = disabled.owner_technical_buy.as_mut().unwrap();
    policy.activate = false;
    policy.expires_at = (Utc::now() - Duration::seconds(1)).to_rfc3339();
    std::fs::write(&disabled.canary_kill_switch_path, "stop")?;
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(disabled)
        .for_ingestion(&fixture::ingestion(), &root.path().join("state.db").to_string_lossy())?
        .with_owner_buy_adapter(adapter);
    runner.process_tick(&reopened, Utc::now()).await?;
    let recovered = reopened.load_failed_expense_task(&order_id)?.unwrap();
    assert_eq!(recovered.status, "complete");
    assert!(reopened.list_owner_technical_buy_recovery_intents()?.is_empty());
    let calls = |method: &str| server.calls.lock().unwrap().iter().filter(|m| *m == method).count();
    let receipt_calls = calls("getTransaction");
    runner.process_tick(&reopened, Utc::now()).await?;
    assert_eq!(calls("getTransaction"), receipt_calls);
    assert_eq!(calls("sendTransaction"), 1);
    let sql = rusqlite::Connection::open(root.path().join("state.db"))?;
    let ledger: (i64, String, String) = sql.query_row(
        "SELECT count(*),wallet_fee_lamports,transaction_fee_lamports FROM execution_failed_expense_ledger",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    assert_eq!(ledger, (1, "19000".into(), "19000".into()));
    let reservation: (Option<u64>, Option<String>) = sql.query_row(
        "SELECT actual_fee,outcome FROM execution_tiny_reservations WHERE order_id=?1",
        [&order_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    assert_eq!(reservation, (Some(19_000), Some("failed".into())));
    assert!(!reopened.execution_canary_fill_exists(&order_id)?);
    assert!(reopened.load_execution_canary_open_position(MINT)?.is_none());
    Ok(())
}
