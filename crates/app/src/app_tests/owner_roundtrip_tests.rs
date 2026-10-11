//! Causal daemon roundtrip. All provider responses, quotes and chain receipts are mocks.
use super::owner_roundtrip_fixture::{Case,RECEIVED};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::{owner_exit_order_id,SqliteStore};
const EXIT:&str="execution-only:owner-test-intent";

#[tokio::test]
async fn owner_roundtrip_actual_daemon_adapter_sells_actual_receipt_quantity_once() -> Result<()> {
    let mut c=Case::new().await?;
    assert_eq!(c.store.run_migrations(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?,0);
    assert_eq!(c.sql()?.query_row("SELECT count(*) FROM schema_migrations WHERE version='0090_owner_exit_receipt_amount.sql'",[],|r|r.get::<_,u32>(0))?,1);
    assert!(!c.sql()?.prepare("PRAGMA foreign_key_check")?.exists([])?);
    let result=c.tick().await?;
    assert_eq!(c.calls("sendTransaction"),2,"{result:?} exit={:?}",c.store.load_execution_canary_order(&owner_exit_order_id(EXIT))?);
    assert_eq!(c.calls("quote"),2,"{result:?} exit={:?}",c.store.load_execution_canary_order(&owner_exit_order_id(EXIT))?);
    let buy=c.store.load_execution_canary_receipt_facts(&c.order_id())?.unwrap();
    let sell=c.store.load_execution_canary_receipt_facts(&owner_exit_order_id(EXIT))?.unwrap();
    assert_eq!(buy.token_delta.unwrap().raw,i128::from(RECEIVED));
    assert_eq!(sell.token_delta.unwrap().raw,-i128::from(RECEIVED));
    assert_eq!(buy.transaction_fee.unwrap().as_u64(),19_000);
    assert_eq!(sell.transaction_fee.unwrap().as_u64(),19_000);
    let exit=c.store.load_owner_exit_intent(EXIT)?.unwrap();
    assert_eq!(exit.buy_order_id,c.order_id());assert_eq!(exit.buy_receipt_signature,buy.tx_signature);
    assert_eq!(exit.amount_raw,RECEIVED);assert_ne!(exit.amount_raw,1_167_085);
    assert!(c.store.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
    assert_eq!(c.store.execution_canary_open_position_count()?,0);
    let sql=c.sql()?;
    for table in ["copy_signals","followlist","observed_swaps"] {
        assert_eq!(sql.query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get::<_,u32>(0))?,0);
    }
    assert_eq!(sql.query_row("SELECT qty_raw FROM positions WHERE position_id=?1",[exit.position_id],|r|r.get::<_,String>(0))?,"0");
    assert_eq!(sql.query_row("SELECT state,actual_fee FROM owner_exit_fee_reservations",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?)))?,("successful".into(),19_000));
    c.tick().await?;assert_eq!(c.calls("sendTransaction"),2);assert_eq!(c.calls("quote"),2);
    Ok(())
}
#[tokio::test]
async fn owner_roundtrip_quote_failure_consumes_side_before_io_and_never_retries() -> Result<()> {
    for mode in ["buy_quote_failure","sell_quote_failure"] {
        let c=Case::new().await?;c.state.lock().unwrap().mode=mode;
        let result=c.tick().await?;let quotes=c.calls("quote");let sends=c.calls("sendTransaction");
        assert_eq!(sends,if mode.starts_with("buy") {0} else {1},"{mode}:{result:?}");
        let id=if mode.starts_with("buy") {c.order_id()} else {owner_exit_order_id(EXIT)};
        assert_eq!(c.store.load_execution_canary_order(&id)?.unwrap().status,copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED);
        c.state.lock().unwrap().mode="";c.tick().await?;
        let reopened=SqliteStore::open(c.root.path().join("state.db"))?;
        crate::execution_canary::ExecutionCanaryRunner::new(c.config.clone()).process_tick(&reopened,Utc::now()).await?;
        assert_eq!(c.calls("quote"),quotes);assert_eq!(c.calls("sendTransaction"),sends);
    }
    Ok(())
}
#[tokio::test]
async fn owner_roundtrip_unknown_buy_and_sell_hold_obligation_and_never_resend() -> Result<()> {
    for mode in ["unknown_buy","unknown_sell"] {
        let c=Case::new().await?;c.state.lock().unwrap().mode=mode;
        let result=c.tick().await?;let sends=c.calls("sendTransaction");
        assert_eq!(sends,if mode.ends_with("buy") {1} else {2},"{mode}:{result:?}");
        let quotes=c.calls("quote");c.tick().await?;
        let reopened=SqliteStore::open(c.root.path().join("state.db"))?;
        let mut disabled=c.config.clone();disabled.owner_technical_buy.as_mut().unwrap().activate=false;
        crate::execution_canary::ExecutionCanaryRunner::new(disabled.clone()).process_tick(&reopened,Utc::now()).await?;
        assert_eq!(c.calls("quote"),quotes);assert_eq!(c.calls("sendTransaction"),sends);
        let id=if mode.ends_with("buy") {c.order_id()} else {owner_exit_order_id(EXIT)};
        assert!(!reopened.execution_canary_fill_exists(&id)?);
        assert!(reopened.load_execution_canary_dispatch(&id)?.is_some());
        c.state.lock().unwrap().mode="";
        crate::execution_canary::ExecutionCanaryRunner::new(disabled).process_tick(&reopened,Utc::now()).await?;
        assert!(reopened.execution_canary_fill_exists(&id)?);assert_eq!(c.calls("sendTransaction"),sends);
    }
    Ok(())
}
#[tokio::test]
async fn owner_roundtrip_partial_sell_remains_open_and_is_not_resent() -> Result<()> {
    let c=Case::new().await?;c.state.lock().unwrap().mode="partial_sell";
    c.tick().await?;assert_eq!(c.calls("sendTransaction"),2);
    let p=c.store.load_execution_canary_open_position(super::owner_exit_test_fixture::USDC)?.unwrap();
    assert_eq!(p.qty_exact.unwrap().raw(),7);
    assert!(c.store.execution_canary_fill_exists(&owner_exit_order_id(EXIT))?);
    c.tick().await?;assert_eq!(c.calls("sendTransaction"),2);assert_eq!(c.calls("quote"),2);
    assert!(!c.store.rearm_owner_exit_undispatched(EXIT)?);
    Ok(())
}
#[tokio::test]
async fn owner_roundtrip_sell_wire_amount_substitution_fails_before_send_and_never_retries() -> Result<()> {
    let c=Case::new().await?;c.state.lock().unwrap().mode="sell_wrong_input";
    c.tick().await?;assert_eq!(c.calls("sendTransaction"),1);
    assert_eq!(c.store.load_execution_canary_order(&owner_exit_order_id(EXIT))?.unwrap().status,copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED);
    assert_eq!(c.store.execution_canary_open_position_count()?,1);
    c.state.lock().unwrap().mode="";c.tick().await?;
    assert_eq!(c.calls("sendTransaction"),1);assert_eq!(c.calls("quote"),2);
    Ok(())
}
#[tokio::test]
async fn owner_roundtrip_rejects_unbound_exit_authority_and_parent_or_amount_substitution() -> Result<()> {
    let c=Case::new().await?;c.tick().await?;
    let mut derived=crate::execution_owner_roundtrip::exit_config(&c.config,&c.store)?.unwrap();
    copybot_config::validate_owner_exit(&derived)?;
    derived.owner_technical_buy=None;assert!(copybot_config::validate_owner_exit(&derived).is_err());
    let mut wrong=c.store.load_owner_exit_intent(EXIT)?.unwrap();
    wrong.intent_id="execution-only:another-parent".into();
    assert!(c.store.register_owner_exit_intent(&wrong).is_err());
    let mut cap=c.config.clone();cap.owner_technical_buy.as_mut().unwrap().max_daily_loss_lamports=20_000_001;
    assert!(copybot_config::validate_owner_technical_buy(&cap).is_err());
    Ok(())
}

#[test]
fn owner_roundtrip_amount_migration_preserves_existing_identity_and_incoming_references() -> Result<()> {
    let mut c=rusqlite::Connection::open_in_memory()?;
    c.execute_batch("PRAGMA foreign_keys=OFF;CREATE TABLE orders(order_id TEXT PRIMARY KEY,signal_id TEXT);CREATE TABLE positions(position_id TEXT PRIMARY KEY);CREATE TEMP TABLE owner_exit_rebuild_guard(enabled INTEGER);INSERT INTO owner_exit_rebuild_guard VALUES(1);")?;
    c.execute_batch(include_str!("../../../../migrations/0086_owner_exit_intent.sql"))?;
    c.execute_batch("INSERT INTO orders VALUES('old-buy','owner-buy:old');INSERT INTO positions VALUES('old-position');CREATE TABLE incoming(exit_id TEXT REFERENCES owner_exit_intents(intent_id) ON DELETE RESTRICT);")?;
    c.execute("INSERT INTO owner_exit_intents VALUES('old-exit','old-run','old-buy','old-signature','old-position','wallet','wallet','genesis',?1,1167085,6,'route','start','expiry',?2,22000,160200031,50,20000000)",rusqlite::params![super::owner_exit_test_fixture::USDC,"a".repeat(64)])?;
    c.execute("INSERT INTO incoming VALUES('old-exit')",[])?;
    let before:String=c.query_row("SELECT group_concat(intent_id||':'||run_id||':'||amount_raw) FROM owner_exit_intents",[],|r|r.get(0))?;
    c.pragma_update(None,"foreign_keys",true)?;
    copybot_storage_core::fill_cash_migration::with_constraints(&mut c,true,|conn| copybot_storage_core::owner_exit_amount_migration::apply(conn,include_str!("../../../../migrations/0090_owner_exit_receipt_amount.sql")))?;
    assert!(c.pragma_query_value(None,"foreign_keys",|r|r.get::<_,bool>(0))?);
    assert_eq!(c.query_row("SELECT group_concat(intent_id||':'||run_id||':'||amount_raw) FROM owner_exit_intents",[],|r|r.get::<_,String>(0))?,before);
    assert_eq!(c.query_row("PRAGMA foreign_key_list(incoming)",[],|r|r.get::<_,String>(2))?,"owner_exit_intents");
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    assert!(c.execute("UPDATE owner_exit_intents SET amount_raw=1700007 WHERE intent_id='old-exit'",[]).is_err());
    assert!(c.execute("DELETE FROM owner_exit_intents WHERE intent_id='old-exit'",[]).is_err());
    let ddl:String=c.query_row("SELECT sql FROM sqlite_master WHERE name='owner_exit_intents'",[],|r|r.get(0))?;
    assert!(ddl.contains("CHECK(amount_raw>0)"));assert!(ddl.contains("CHECK(decimals=6)"));
    assert!(ddl.contains("CHECK(mint='EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')"));
    assert!(copybot_storage_core::owner_exit_amount_migration::apply(&c,include_str!("../../../../migrations/0090_owner_exit_receipt_amount.sql")).is_err());
    Ok(())
}

#[test]
fn owner_roundtrip_exact_stopped_package_config_can_be_materialized_without_runtime_io() -> Result<()> {
    let Ok(path)=std::env::var("COPYBOT_EXECUTION_ONLY_CONFIG_PROOF") else {return Ok(())};
    let cfg=copybot_config::load_from_path(&path)?;
    assert!(copybot_config::owner_execution_roundtrip(&cfg.execution));
    assert!(!cfg.execution.enabled);assert!(!cfg.execution.owner_technical_buy.as_ref().unwrap().activate);
    assert!(cfg.execution.native_fresh_buy.is_none());assert!(cfg.execution.owner_exit.is_none());
    let text=std::fs::read_to_string(&path)?;
    let (prefix,owner)=text.split_once("[execution.owner_technical_buy]").unwrap();
    let start=Utc::now();let end=start+chrono::Duration::seconds(14_400);
    let materialized=owner.lines().map(|line| {
        if line.starts_with("activate =") {"activate = true".to_string()}
        else if line.starts_with("activated_at =") {format!("activated_at = {:?}",start.to_rfc3339())}
        else if line.starts_with("expires_at =") {format!("expires_at = {:?}",end.to_rfc3339())}
        else {line.to_owned()}
    }).collect::<Vec<_>>().join("\n");
    let root=super::temporary_output_fixture::OutputRoot::new("exact-package-config")?;
    let target=root.path().join("active.toml");
    std::fs::write(&target,format!("{prefix}[execution.owner_technical_buy]\n{materialized}\n"))?;
    let active=copybot_config::load_from_path(target)?;
    crate::config_contract::validate_execution_runtime_contract(&active.execution,&active.system.env)?;
    crate::config_contract::validate_live_ingestion_source_contract(&active.ingestion.source,&active.system.env)?;
    crate::risk_contract::validate_execution_risk_contract(&active.risk)?;
    crate::runtime_bootstrap::validate_live_execution_policy_contract(&active.execution,&active.risk,&active.system.env)?;
    assert!(active.execution.owner_technical_buy.as_ref().unwrap().activate);
    assert!(!active.execution.enabled);
    assert_eq!(active.execution.owner_technical_buy.unwrap().amount_lamports,10_000_000);
    Ok(())
}
