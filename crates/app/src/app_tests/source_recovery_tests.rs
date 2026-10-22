//! New SOURCE recovery only. Quote/simulation/signing/confirmation are explicit LOCAL mocks.
use super::{source_recovery_fixture::Case,source_recovery_submit_fixture::Adapter};
use anyhow::Result;
use chrono::{Duration,Utc};
use copybot_config::*;
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
#[ignore="Pinned public COPY08 receipt/accounting export supplied by offline integration env"]
async fn source_recovery_saved_real_buy_normal_runner_to_exact_quote()->Result<()>{
    let c=Case::saved().await?;
    assert_eq!(c.intent.amount_raw,1_034_558);
    assert_eq!(c.store.owner_source_recovery_native_remaining(&c.intent)?,4_995_000);
    let parent=c.store.load_owner_technical_buy_intent(&c.config.controlled_receipt_lot.as_ref().unwrap().source_buy_intent_id)?.unwrap();
    assert!(parent.expires_at<Utc::now(),"original BUY window is expired and never renewed");
    let mut old=c.config.clone();old.owner_exit.as_mut().unwrap().policy=OWNER_EXIT_V1.into();
    assert!(validate_owner_exit(&old).unwrap_err().to_string().contains("owner_exit_exact_position_amount"));
    let out=c.runner(c.config.clone()).await?;
    assert_eq!(c.server.count("quote"),1,"{out:?}");
    let requests=c.server.state.lock().unwrap().calls.clone();
    let raw_http=c.server.state.lock().unwrap().raw_http.clone();
    assert!(raw_http.iter().any(|r|r["http_method"]=="GET"
        && r["path_and_query"].as_str().unwrap().starts_with("/swap/v1/quote?")));
    assert_eq!(requests.iter().find(|r|r["method"]=="quote").unwrap()["params"]["amount"],"1034558");
    assert_eq!(c.server.count("sendTransaction"),0);
    assert_eq!(c.sql()?.query_row("SELECT count(*) FROM owner_exit_intents",[],|r|r.get::<_,u32>(0))?,1);
    assert_eq!(c.sql()?.query_row("SELECT count(*) FROM orders WHERE signal_id LIKE 'native-buy%'",[],|r|r.get::<_,u32>(0))?,0);
    assert_eq!(c.store.load_owner_technical_buy_intent(&parent.intent_id)?,Some(parent));
    if let Ok(output)=std::env::var("SOURCE_RECOVERY_OUTPUT"){
        let path=std::path::Path::new(&output);std::fs::create_dir_all(path)?;
        std::fs::write(path.join("REAL_BUY_RUNNER_QUOTE.json"),serde_json::to_vec_pretty(&json!({
            "kind":"LOCAL_SAVED_REAL_BUY_ONLY_NOT_LIVE_SELL","own_raw":1034558,"native_remaining":4995000,
            "old_policy":"owner_exit_exact_position_amount","requests":requests,"raw_http":raw_http,
            "intent":c.intent,"signs":0,"sends":0,
            "BUY_parent_unchanged":true,"BOT_actions":0,"simulation":"explicit refusal before signing"}))?)?;
        c.sql()?.execute("VACUUM INTO ?1",[path.join("real-BUY-after-quote.db").to_string_lossy().as_ref()])?;
    }
    Ok(())
}

#[tokio::test]
async fn source_recovery_mock_sell_closes_once_and_unknown_restart_never_resends()->Result<()>{
    for unknown in [false,true]{
        let c=Case::synthetic().await?;c.server.state.lock().unwrap().unknown=unknown;
        let adapter=Adapter{config:c.config.clone(),server:&c.server};
        let result=crate::execution_owner_exit::tick_with_adapter(&c.config,&c.store,Utc::now(),&adapter).await?;
        let order=owner_exit_order_id(&c.intent.intent_id);
        assert_eq!(c.server.count("sendTransaction"),1,"{result:?}");
        if unknown{
            assert!(!c.store.execution_canary_fill_exists(&order)?);
            assert!(c.store.load_execution_canary_dispatch(&order)?.is_some());
        }else{assert!(c.store.execution_canary_fill_exists(&order)?,"{result:?}");}
        let reopened=SqliteStore::open(&c.path)?;
        let mut stopped=c.config.clone();stopped.owner_exit.as_mut().unwrap().activate=false;
        // UNKNOWN reconciliation has no authority to send another transaction.
        crate::execution_owner_exit::tick_with_adapter(&stopped,&reopened,Utc::now(),&adapter).await?;
        assert_eq!(c.server.count("sendTransaction"),1);
        c.server.state.lock().unwrap().unknown=false;
        crate::execution_owner_exit::tick_with_adapter(&stopped,&reopened,Utc::now(),&adapter).await?;
        assert!(reopened.execution_canary_fill_exists(&order)?);
        assert_eq!(c.server.count("sendTransaction"),1);
        assert!(reopened.load_execution_canary_open_position(OWNER_EXIT_MINT)?.is_none());
        assert_eq!(c.sql()?.query_row("SELECT count(*) FROM fills",[],|r|r.get::<_,u32>(0))?,2);
        assert_eq!(c.sql()?.query_row("SELECT count(*),actual_fee FROM owner_exit_fee_reservations",[],|r|Ok((r.get::<_,u32>(0)?,r.get::<_,u64>(1)?)))?,(1,5000));
        if let Ok(root)=std::env::var("SOURCE_RECOVERY_OUTPUT"){
            let p=std::path::Path::new(&root).join(if unknown{"mock-unknown-reconciled"}else{"mock-success"});
            std::fs::create_dir_all(&p)?;
            c.sql()?.execute("VACUUM INTO ?1",[p.join("closed.db").to_string_lossy().as_ref()])?;
            std::fs::write(p.join("RESULT.json"),serde_json::to_vec_pretty(&json!({
                "kind":"LOCAL_SYNTHETIC_SIGNING_AMM_QUOTE_CONFIRMATION_ONLY","intent":c.intent,
                "position":"closed","own_raw":1034558,"fee_buy":5000,"fee_sell":5000,
                "mock_send_calls":c.server.count("sendTransaction"),"real_signs":0,"real_sends":0,
                "unknown_reconciled_without_resend":unknown,"cash_pnl":"not claimed"}))?)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_recovery_identity_amount_duplicate_window_and_stop_refuse()->Result<()>{
    let c=Case::synthetic().await?;
    for change in 0..5{
        let mut i=c.intent.clone();match change{0=>i.buy_receipt_signature.push('x'),1=>i.amount_raw+=1,
            2=>i.wallet=super::receipt_lot_custody_fixture::SOURCE.into(),3=>i.buy_order_id.push('x'),_=>i.run_id.push('x')}
        assert!(c.store.register_owner_exit_intent(&i).is_err(),"mutation{change}");
    }
    c.store.register_owner_exit_intent(&c.intent)?;
    let mut alias=c.intent.clone();alias.intent_id="source-recovery:other".into();
    assert!(c.store.register_owner_exit_intent(&alias).is_err());
    // A valid old roundtrip alias still cannot acquire this already claimed BUY.
    let b=c.store.load_owner_technical_buy_intent(&c.config.controlled_receipt_lot.as_ref().unwrap().source_buy_intent_id)?.unwrap();
    let mut alias=c.intent.clone();alias.intent_id=format!("execution-only:{}",b.intent_id);
    alias.activated_at=b.activated_at;alias.expires_at=b.expires_at;
    let refusal=c.store.register_owner_exit_intent(&alias).unwrap_err();
    assert!(format!("{refusal:#}").contains("UNIQUE constraint failed"),"{refusal:#}");
    let mut changed=c.intent.clone();changed.expires_at+=Duration::seconds(1);
    assert!(c.store.register_owner_exit_intent(&changed).is_err());
    assert!(!c.store.rearm_owner_exit_undispatched(&c.intent.intent_id)?);
    let mut config=c.config.clone();config.owner_exit.as_mut().unwrap().expires_at=(c.intent.activated_at+Duration::seconds(301)).to_rfc3339();
    assert!(crate::execution_owner_exit_authority::configured(&config).unwrap_err().to_string().contains("source_recovery_window_cap"));
    let mut parent_changed=c.config.clone();parent_changed.owner_technical_buy.as_mut().unwrap().expires_at=(Utc::now()+Duration::days(1)).to_rfc3339();
    assert!(crate::execution_owner_exit_authority::current(&parent_changed,&c.store,&c.intent,Utc::now()).unwrap_err().to_string().contains("source_recovery_parent_changed"));
    assert!(crate::execution_owner_exit_authority::current(&c.config,&c.store,&c.intent,c.intent.expires_at).unwrap_err().to_string().contains("owner_exit_expired"));
    std::fs::write(&c.config.canary_kill_switch_path,"STOP")?;
    let stopped=c.runner(c.config.clone()).await?;assert_eq!(c.server.count("quote"),0,"{stopped:?}");
    assert_eq!(c.server.count("sendTransaction"),0);
    // Ordinary source roundtrip still waits for its real native BOT BUY.
    let mut roundtrip=c.config.clone();roundtrip.owner_exit=None;
    assert!(!crate::execution_owner_source_gate::ready(&roundtrip,&c.store,Utc::now())?);
    Ok(())
}

#[tokio::test]
async fn source_recovery_actual_shortage_cap_and_foreign_holdings()->Result<()>{
    for (held,passes) in [(1_034_557,false),(1_034_558,true),(987_654_321,true)]{
        let c=Case::synthetic().await?;c.server.state.lock().unwrap().held=held;
        let result=c.runner(c.config.clone()).await?;
        assert_eq!(c.server.count("quote"),usize::from(passes),"{held}:{result:?}");
        assert_eq!(c.server.count("sendTransaction"),0);
    }
    for (balance,rent,fee,reason) in [(1,2_039_280,5000,"owner_exit_native_floor_insufficient"),
        (10_000_000,5_000_000,5000,"source_recovery_native_cap"),
        (10_000_000,2_039_280,100_001,"owner_exit_fee_cap")]{
        let c=Case::synthetic().await?;
        {let mut s=c.server.state.lock().unwrap();s.native=balance;s.rent=rent;s.fee=fee;}
        let adapter=Adapter{config:c.config.clone(),server:&c.server};
        let result=crate::execution_owner_exit::tick_with_adapter(&c.config,&c.store,Utc::now(),&adapter).await?;
        assert_eq!(c.server.count("sendTransaction"),0,"{balance}/{rent}/{fee}:{result:?}");
        assert!(format!("{result:?}").contains(reason),"{reason}: {result:?}");
    }
    Ok(())
}
