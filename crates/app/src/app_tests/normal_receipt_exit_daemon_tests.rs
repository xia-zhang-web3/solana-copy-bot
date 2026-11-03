//! Full pinned normal BOT state through the actual daemon runner; external I/O is LOCAL.
use super::normal_receipt_exit_saved_fixture as saved;
use anyhow::{ensure,Result};
use chrono::{Duration,Utc};
use copybot_storage_core::SqliteStore;
use std::sync::{Arc,Mutex,atomic::Ordering};
use std::collections::BTreeMap;
use super::{copy_cycle_peer_fixture::Peer,owner_buy_fixture::SignedAdapter};

#[tokio::test]
#[ignore="requires exact full current BOT DB and published receipt-exit config; no real keys/network"]
async fn normal_receipt_exit_full_current_daemon_unknown_restart_closes_once()->Result<()>{
 super::normal_receipt_exit_mock::scope(run()).await
}

async fn run()->Result<()>{
 let f=saved::Case::new()?;
 let parents=f.parents()?;
 let mut c=copybot_config::load_from_path(std::env::var("COPYBOT_NORMAL_EXIT_CONFIG")?)?.execution;
 let key=crate::execution_pumpswap_accounts::parse_pubkey(saved::BOT,"LOCAL actual public key")?;
 let peer=Peer::new(key,914598,464668600,Arc::new(Mutex::new(BTreeMap::new()))).await?;
 {
  let mut s=peer.state.lock().unwrap();s.genesis="5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into();
  // Retained send identity only; no provider/send or copied financial row occurs.
  s.sends.push(saved::BUY.into());s.unknown_sell=true;
  s.bank.accounts.get_mut(&key).unwrap().lamports=255915094;
  let mint=crate::execution_pumpswap_accounts::parse_pubkey(copybot_storage_core::OWNER_EXIT_MINT,"USDC")?;
  let ata=crate::execution_pumpswap_accounts::associated_token_address(&key,&mint,&crate::execution_pumpswap_accounts::token_program_id());
  s.bank.accounts.get_mut(&ata).unwrap().token.as_mut().unwrap().2=914698;
 }
 c.canary_kill_switch_path=f._dir.path().join("STOP").to_string_lossy().into_owned();
 c.submit_adapter_http_url=peer.url.clone();c.owned_sell_preparation.as_mut().unwrap().rpc_url=peer.url.clone();
 c.quote_canary_base_url=format!("{}/swap/v1",peer.url);c.quote_canary_api_key="LOCAL-MOCK".into();
 c.execution_signer_keypair_path="LOCAL-NO-REAL-KEY".into();c.max_confirm_seconds=1;c.submit_timeout_ms=1000;
 let now=Utc::now();let p=c.owner_exit.as_mut().unwrap();p.activate=false;p.activated_at=(now-Duration::seconds(1)).to_rfc3339();p.expires_at=(now+Duration::seconds(180)).to_rfc3339();
 let mut adapter_config=c.clone();adapter_config.owner_exit.as_mut().unwrap().activate=true;
 let adapter=Arc::new(SignedAdapter{config:adapter_config,payload:String::new(),signature:super::normal_receipt_exit_mock::MARKER.into(),fail_simulation:false,wrong_blueprint:false,simulation_time:None,kill_during_simulation:false,simulation_calls:Default::default(),signing_calls:Default::default()});
 let mut inactive=crate::execution_canary::ExecutionCanaryRunner::new(c.clone());inactive.owner_exit_adapter=Some(adapter.clone());
 crate::execution_only_loop::run_until(SqliteStore::open(&f.path)?,inactive,1,async{tokio::time::sleep(std::time::Duration::from_millis(25)).await;Ok(())}).await?;
 ensure!(peer.state.lock().unwrap().calls.is_empty(),"inactive daemon called provider");
 ensure!(f.parents()?==parents,"inactive daemon changed grants");
 // Existing normal state requires the explicit receipt-only profile.
 let mut missing=c.clone();missing.owner_exit=None;
 ensure!(Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(missing).process_tick(&f.store,now)).await.is_err());
 let mut other=c.clone();other.owner_exit.as_mut().unwrap().policy=copybot_config::OWNER_EXIT_V1.into();
 ensure!(Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(other).process_tick(&f.store,now)).await.is_err());
 // A foreign scope and wrong receipt quantity refuse before external work.
 let mut foreign=c.clone();foreign.controlled_receipt_lot.as_mut().unwrap().source_buy_intent_id.push('x');
 ensure!(Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(foreign).process_tick(&f.store,now)).await.is_err());
 let mut wrong=c.clone();wrong.owner_exit.as_mut().unwrap().activate=true;wrong.owner_exit.as_mut().unwrap().amount_raw-=1;
 ensure!(Box::pin(crate::execution_canary::ExecutionCanaryRunner::new(wrong).process_tick(&f.store,now)).await.is_err());
 ensure!(peer.state.lock().unwrap().calls.is_empty());
 c.owner_exit.as_mut().unwrap().activate=true;
 let mut runner=crate::execution_canary::ExecutionCanaryRunner::new(c.clone());runner.owner_exit_adapter=Some(adapter.clone());
 let first=Box::pin(runner.process_tick(&f.store,Utc::now()-Duration::milliseconds(1))).await?;
 peer.healthy()?;ensure!(peer.sends()==2,"one new mocked SELL required: {first:?}");
 ensure!(adapter.simulation_calls.load(Ordering::SeqCst)==1 && adapter.signing_calls.load(Ordering::SeqCst)==1,"one simulation/sign required");
 let conn=f.conn()?;ensure!(conn.query_row("SELECT count(*) FROM execution_canary_unresolved_dispatch",[],|r|r.get::<_,u64>(0))?==1,"UNKNOWN must remain pending");
 ensure!(f.parents()?==parents,"BUY grants changed");
 let restart=SqliteStore::open(&f.path)?;
 let mut recovery=c.clone();recovery.owner_exit.as_mut().unwrap().activate=false;
 let mut restarted=crate::execution_canary::ExecutionCanaryRunner::new(recovery);restarted.owner_exit_adapter=Some(adapter.clone());
 Box::pin(restarted.process_tick(&restart,Utc::now())).await?;ensure!(peer.sends()==2,"UNKNOWN resend");
 peer.state.lock().unwrap().unknown_sell=false;
 for _ in 0..3 {Box::pin(restarted.process_tick(&restart,Utc::now())).await?;}
 let active_closed=crate::execution_canary::ExecutionCanaryRunner::new(c.clone());
 Box::pin(active_closed.process_tick(&SqliteStore::open(&f.path)?,Utc::now())).await?;
 peer.healthy()?;ensure!(peer.sends()==2 && adapter.signing_calls.load(Ordering::SeqCst)==1,"post receipt resend");
 ensure!(conn.query_row("SELECT count(*) FROM fills",[],|r|r.get::<_,u64>(0))?==2);
 ensure!(conn.query_row("SELECT state FROM real_session_cycles",[],|r|r.get::<_,String>(0))?=="closed");
 ensure!(conn.query_row("SELECT state||':'||qty_raw||':'||cost_lamports FROM positions",[],|r|r.get::<_,String>(0))?=="closed:0:0");
 ensure!(conn.query_row("SELECT count(*) FROM owner_exit_fee_reservations WHERE state='successful' AND actual_fee=5000",[],|r|r.get::<_,u64>(0))?==1);
 ensure!(conn.query_row("SELECT count(*) FROM execution_canary_unresolved_dispatch",[],|r|r.get::<_,u64>(0))?==0);
 ensure!(f.parents()?==parents,"old session/BUY/tiny parents changed");
 let s=peer.state.lock().unwrap();let token=s.bank.accounts.values().find_map(|a|a.token.filter(|(m,w,_)|*w==key && bs58::encode(m).into_string()==copybot_storage_core::OWNER_EXIT_MINT)).unwrap();ensure!(token.2==100,"personal inventory changed");
 ensure!(s.calls.iter().filter(|x|x.as_str()=="sendTransaction").count()==1);
 if let Ok(out)=std::env::var("COPYBOT_NORMAL_EXIT_EVIDENCE"){
  let out=std::path::Path::new(&out);std::fs::create_dir_all(out)?;conn.execute("VACUUM INTO ?",[out.join("settled-daemon.sqlite").to_string_lossy().as_ref()])?;
  std::fs::write(out.join("RESULT.json"),serde_json::to_vec_pretty(&serde_json::json!({"status":"PASS_FULL_CURRENT_DB_DAEMON_EXIT_UNKNOWN_RESTART","mock_signatures":true,"real_signer_reads":0,"new_mock_sell_sends":1,"new_buys":0,"new_source_sends":0,"stream_connections":0,"personal_raw":100,"actual_mock_fee":5000,"provider_calls":s.calls,"receipt_signature":s.sends[1]}))?)?;
 }
 Ok(())
}
