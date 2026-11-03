//! Exact failed live state is copied; only a separately authorized LOCAL child changes.
#[path = "common/normal_exit_current.rs"]
mod fixture;
use anyhow::{ensure, Result};
use chrono::{Duration, Utc};
use copybot_storage_core::*;
use fixture::Case;
use sha2::{Digest, Sha256};

fn case() -> Result<Case> {
    let bytes = std::fs::read(std::env::var("COPYBOT_FAILED_EXIT_DB")?)?;
    ensure!(format!("{:x}",Sha256::digest(&bytes)) ==
        "400cc50e58fc0a7524238a9cefafd10e0e9f3a6f48ca45c394eca92dc7f3f27f", "failed DB pin");
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("failed.sqlite");
    std::fs::write(&path, bytes)?;
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"),"/../../migrations")))?;
    let mut intent = store.load_owner_exit_intent(&format!("bot-receipt-exit:{}",fixture::SOURCE_BUY))?.unwrap();
    intent.intent_id = format!("bot-receipt-child:{}",fixture::SOURCE_BUY);
    intent.authority_sha256 = "f".repeat(64);
    intent.activated_at = Utc::now();
    intent.expires_at = intent.activated_at + Duration::seconds(300);
    Ok(Case{_dir:dir,path,store,intent})
}

fn parent(f: &Case) -> Result<String> {
    Ok(f.conn()?.query_row("SELECT json_object('status',status,'error',simulation_error,'signature',tx_signature,
        'submit',submit_ts,'confirm',confirm_ts,'attempt',attempt) FROM orders WHERE order_id=?1",
        [owner_exit_order_id(&format!("bot-receipt-exit:{}",fixture::SOURCE_BUY))],|r|r.get(0))?)
}

#[test]
#[ignore="exact failed BOT snapshot; no network or signer"]
fn bot_child_preserves_parent_and_closes_normal_cycle_once() -> Result<()> {
    let f = case()?;
    let original = parent(&f)?;
    let parents = f.parents()?;
    let (order,dispatch,budget) = f.dispatch()?;
    assert_eq!(f.store.claim_owner_exit_dispatch(&order,&dispatch,&budget,||Ok(f.intent.activated_at))?,ExecutionDispatchClaim::New);
    assert_eq!(f.store.claim_owner_exit_dispatch(&order,&dispatch,&budget,||Ok(f.intent.expires_at))?,ExecutionDispatchClaim::Existing);
    assert!(!f.store.rearm_owner_exit_undispatched(&f.intent.intent_id)?);
    let restart=SqliteStore::open(&f.path)?;
    assert_eq!(restart.list_owner_exit_recovery_intents()?,vec![f.intent.clone()]);
    f.settle(&dispatch)?;
    f.store.refresh_local_bounded_session(f.intent.activated_at)?;
    f.store.apply_execution_canary_sell_settlement(&f.receipt(&dispatch),f.intent.activated_at)?;
    assert_eq!(parent(&f)?,original);
    assert_eq!(f.parents()?,parents);
    let c=f.conn()?;
    assert_eq!(c.query_row("SELECT state||':'||qty_raw||':'||cost_lamports FROM positions",[],|r|r.get::<_,String>(0))?,"closed:0:0");
    assert_eq!(c.query_row("SELECT state FROM real_session_cycles",[],|r|r.get::<_,String>(0))?,"closed");
    assert_eq!(c.query_row("SELECT count(*) FROM owner_exit_child_attempts",[],|r|r.get::<_,u64>(0))?,1);
    assert_eq!(c.query_row("SELECT count(*) FROM execution_canary_dispatch",[],|r|r.get::<_,u64>(0))?,2);
    assert_eq!(c.query_row("SELECT count(*) FROM fills",[],|r|r.get::<_,u64>(0))?,2);
    assert_eq!(c.query_row("SELECT actual_fee FROM owner_exit_fee_reservations",[],|r|r.get::<_,u64>(0))?,27000);
    assert!(c.execute("DELETE FROM owner_exit_child_attempts",[]).is_err());
    assert!(c.execute("UPDATE owner_exit_child_attempts SET intent_json='{}'",[]).is_err());
    Ok(())
}

#[test]
#[ignore="exact failed BOT snapshot; causal LOCAL corruptions"]
fn bot_child_rejects_signed_unknown_build_and_changed_bindings() -> Result<()> {
    for sql in [
        "UPDATE orders SET tx_signature='signed' WHERE status='execution_canary_failed'",
        "UPDATE orders SET status='execution_canary_submitted' WHERE status='execution_canary_failed'",
        "UPDATE orders SET simulation_status='passed' WHERE status='execution_canary_failed'",
        "UPDATE orders SET network_fee_lamports_hint=5000 WHERE status='execution_canary_failed'",
        "UPDATE orders SET simulation_error='not quote failure' WHERE status='execution_canary_failed'",
        "UPDATE positions SET qty_raw='914599'",
        "UPDATE real_session_native_decisions SET cycle_id='foreign'",
    ] {
        let f=case()?;f.corrupt_clone(sql)?;
        assert!(f.store.register_owner_exit_intent(&f.intent).is_err(),"accepted {sql}");
        assert_eq!(f.conn()?.query_row("SELECT count(*) FROM owner_exit_child_attempts",[],|r|r.get::<_,u64>(0))?,0);
    }
    for field in ["amount","receipt","authority","early","long","id"] {
        let mut f=case()?;
        match field {
            "amount"=>f.intent.amount_raw+=1,
            "receipt"=>f.intent.buy_receipt_signature.push('x'),
            "authority"=>f.intent.authority_sha256=f.store.list_owner_exit_intents(1)?[0].authority_sha256.clone(),
            "early"=>{f.intent.activated_at="2026-11-03T11:00:00Z".parse()?;f.intent.expires_at=f.intent.activated_at+Duration::seconds(300)},
            "long"=>f.intent.expires_at+=Duration::seconds(1),
            _=>f.intent.intent_id.push('x'),
        }
        assert!(f.store.register_owner_exit_intent(&f.intent).is_err(),"accepted {field}");
    }
    Ok(())
}
