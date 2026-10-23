//! LOCAL-only recovery fixture. No provider, real key, or transaction submission.
use super::{owner_buy_fixture as buy, owner_exit_test_fixture as exit, wsol_lifecycle_fixture as w};
use crate::execution_submit_adapter::*;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{Duration,Utc};
use copybot_config::*;
use copybot_storage_core::*;
use serde_json::{json,Value};
use std::{path::PathBuf,sync::{Arc,Mutex}};
use tokio::io::{AsyncReadExt,AsyncWriteExt};

pub(super) struct State {
    pub wallet:String,pub genesis:String,pub held:u64,pub native:u64,pub rent:u64,pub fee:u64,
    pub slot:u64,pub unknown:bool,pub calls:Vec<Value>,pub raw_http:Vec<Value>,pub receipt:Value,
    pub quote_snapshot:Option<PathBuf>,
}
pub(super) struct Server {pub url:String,pub state:Arc<Mutex<State>>,task:tokio::task::JoinHandle<Result<()>>}
impl Drop for Server {fn drop(&mut self){self.task.abort();}}
impl Server {
    pub async fn start(wallet:&str,held:u64)->Result<Self>{
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url=format!("http://{}",listener.local_addr()?);
        let state=Arc::new(Mutex::new(State{wallet:wallet.into(),genesis:buy::GENESIS.into(),held,native:10_000_000,
            rent:2_039_280,fee:5000,slot:460119951,unknown:false,calls:vec![],raw_http:vec![],receipt:Value::Null,quote_snapshot:None}));
        let shared=state.clone();
        let task=tokio::spawn(async move {loop{
            let(mut socket,_)=listener.accept().await?;let mut bytes=Vec::new();
            let(end,len)=loop{let mut chunk=[0;4096];let n=socket.read(&mut chunk).await?;
                ensure!(n>0&&bytes.len()<1<<20,"LOCAL request cap");bytes.extend_from_slice(&chunk[..n]);
                if let Some(at)=bytes.windows(4).position(|v|v==b"\r\n\r\n"){
                    let head=String::from_utf8_lossy(&bytes[..at]);
                    let len=head.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length:")
                        .and_then(|v|v.trim().parse().ok())).unwrap_or(0);
                    if bytes.len()>=at+4+len{break(at,len)}
                }
            };
            let head=String::from_utf8_lossy(&bytes[..end]);
            let method=head.split_whitespace().next().unwrap();
            let target=head.split_whitespace().nth(1).unwrap();
            ensure!(method=="GET" && target.split('?').next()==Some("/swap/v1/quote")
                || method=="POST" && target=="/rpc","LOCAL exact accepted broker route");
            let request=if head.starts_with("GET "){
                let url=reqwest::Url::parse(&format!("http://localhost{}",head.split_whitespace().nth(1).unwrap()))?;
                json!({"method":"quote","params":url.query_pairs().into_owned().collect::<std::collections::HashMap<_,_>>()})
            }else{serde_json::from_slice::<Value>(&bytes[end+4..end+4+len])?};
            let value={let mut state=shared.lock().unwrap();
                let raw=json!({"http_method":method,"path_and_query":target,
                    "body":String::from_utf8_lossy(&bytes[end+4..end+4+len]),"recorded_at":Utc::now()});
                state.raw_http.push(raw.clone());
                if method=="GET" {
                    if let (Some(db),Ok(root))=(&state.quote_snapshot,std::env::var("SOURCE_RECOVERY_OUTPUT")){
                        let root=PathBuf::from(root);std::fs::create_dir_all(&root)?;
                        let destination=root.join("QUOTE_INPUT_STATE.db");
                        ensure!(!destination.exists(),"LOCAL quote snapshot must be new");
                        {
                            let source=rusqlite::Connection::open_with_flags(db,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                            let mut copy=rusqlite::Connection::open(&destination)?;
                            rusqlite::backup::Backup::new(&source,&mut copy)?.run_to_completion(32,std::time::Duration::ZERO,None)?;
                        }
                        std::fs::write(root.join("QUOTE_INPUT_BINDING.json"),serde_json::to_vec_pretty(&json!({
                            "kind":"LOCAL_SAVED_REAL_BUY_DAEMON_QUOTE_BOUNDARY_BEFORE_RESPONSE_OR_SIMULATION",
                            "request":raw,"snapshot_sha256":crate::execution_owned_sell_rpc::digest(std::fs::read(destination)?),
                            "manual_financial_SQL":false,"snapshot":"SQLite backup of own testcopy only"}))?)?;
                    }
                }
                response(&request,&mut state)?};
            let body=value.to_string();socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
        }});
        Ok(Self{url,state,task})
    }
    pub fn count(&self,method:&str)->usize{self.state.lock().unwrap().calls.iter().filter(|r|r["method"]==method).count()}
}
fn response(r:&Value,s:&mut State)->Result<Value>{
    s.calls.push(r.clone());let m=r["method"].as_str().context("LOCAL method")?;
    if m=="quote"{
        let raw=r["params"]["amount"].as_str().context("LOCAL exact quote amount")?;
        let mut q=exit::quote();q["contextSlot"]=json!(s.slot);q["inAmount"]=json!(raw);q["routePlan"][0]["swapInfo"]["inAmount"]=json!(raw);
        return Ok(q)
    }
    let result=match m{
        "getGenesisHash"=>json!(s.genesis),
        "getAccountInfo"=>{
            let address=r["params"][0].as_str().unwrap();
            let mut data=if address==exit::USDC{vec![0;82]}else{vec![0;165]};
            if address==exit::USDC{data[44]=6;data[45]=1;}else{
                data[..32].copy_from_slice(&crate::execution_pumpswap_accounts::parse_pubkey(exit::USDC,"LOCAL mint")?);
                data[32..64].copy_from_slice(&crate::execution_pumpswap_accounts::parse_pubkey(&s.wallet,"LOCAL wallet")?);
                data[64..72].copy_from_slice(&s.held.to_le_bytes());
            }
            json!({"context":{"slot":s.slot},"value":{"owner":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","executable":false,"data":[STANDARD.encode(data),"base64"]}})
        },
        "getBalance"=>json!({"context":{"slot":s.slot},"value":s.native}),
        "getMultipleAccounts"=>json!({"context":{"slot":s.slot},"value":r["params"][0].as_array().unwrap().iter().map(|key|if key.as_str()==Some(&s.wallet){json!({"lamports":s.native,"owner":"11111111111111111111111111111111","executable":false,"data":["","base64"]})}else{Value::Null}).collect::<Vec<_>>()}),
        "getMinimumBalanceForRentExemption"=>json!(s.rent),
        "getFeeForMessage"=>json!({"context":{"slot":s.slot},"value":s.fee}),
        "sendTransaction"=>{let raw=STANDARD.decode(r["params"][0].as_str().unwrap())?;json!(if s.unknown{"wrong signature".into()}else{bs58::encode(&raw[1..65]).into_string()})},
        "getSignatureStatuses"=>if s.unknown{json!({"value":[null]})}else{json!({"value":[{"err":null,"slot":s.receipt["result"]["slot"],"confirmationStatus":"finalized"}]})},
        "getTransaction"=>s.receipt["result"].clone(),
        _=>anyhow::bail!("LOCAL forbidden method:{m}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}
pub(super) fn recovery(c:&mut ExecutionConfig,s:&SqliteStore)->Result<OwnerExitIntent>{
    let scope=c.controlled_receipt_lot.as_ref().context("LOCAL scope")?;
    let buy_id=owner_technical_buy_order_id(&scope.source_buy_intent_id);
    let facts=s.load_execution_canary_receipt_facts(&buy_id)?.context("LOCAL BUY")?;
    let parent=c.owner_technical_buy.as_mut().unwrap();parent.activate=false;
    let now=Utc::now();
    c.canary_entry_submit_enabled=false;c.native_fresh_buy=None;c.tiny_experiment.activate=false;
    c.max_confirm_seconds=1;
    c.owner_exit=Some(OwnerExitConfig{ source_recovery_parent_intent_id: None,policy:OWNER_SOURCE_RECOVERY_EXIT_V1.into(),activate:true,
        run_id:parent.run_id.clone(),intent_id:format!("source-recovery:{}",parent.intent_id),
        buy_order_id:buy_id.clone(),buy_receipt_signature:facts.tx_signature,
        position_id:format!("exec-canary-pos:{buy_id}"),wallet_pubkey:parent.wallet_pubkey.clone(),
        signer_pubkey:parent.signer_pubkey.clone(),genesis_hash:parent.genesis_hash.clone(),mint:parent.mint.clone(),
        amount_raw:u64::try_from(facts.token_delta.unwrap().raw)?,decimals:6,route:parent.route.clone(),
        activated_at:(now-Duration::seconds(1)).to_rfc3339(),expires_at:(now+Duration::seconds(299)).to_rfc3339(),
        max_priority_fee_lamports:22_000,min_reserve_lamports:0,max_slippage_bps:50,max_daily_loss_lamports:20_000_000});
    copybot_config::validate_owner_technical_buy(c)?;
    crate::execution_owner_exit_authority::configured(c)?.context("LOCAL recovery")
}
pub(super) struct Case {pub root:super::temporary_output_fixture::OutputRoot,pub path:PathBuf,
    pub store:SqliteStore,pub config:ExecutionConfig,pub server:Server,pub intent:OwnerExitIntent}
impl Case{
    pub async fn saved()->Result<Self>{
        let root=super::temporary_output_fixture::OutputRoot::new("source-recovery-real-BUY")?;
        let path=root.path().join("state.db");
        let source=PathBuf::from(std::env::var("SOURCE_RECOVERY_SAVED_DB")?);
        // Input is a stopped pinned export. Never open the source writable, even for SHM.
        std::fs::copy(&source,&path)?;
        for suffix in ["-wal","-shm"] {
            let original=PathBuf::from(format!("{}{suffix}",source.display()));
            if original.exists(){std::fs::copy(&original,PathBuf::from(format!("{}{suffix}",path.display())))?;}
        }
        let store=SqliteStore::open(&path)?;
        let raw=std::fs::read_to_string(std::env::var("SOURCE_RECOVERY_SAVED_CONFIG")?)?;
        let mut config=toml::from_str::<AppConfig>(&raw)?.execution;
        let projection:Value=serde_json::from_slice(&std::fs::read(std::env::var("SOURCE_RECOVERY_PARENT_PROJECTION")?)?)?;
        let id=config.controlled_receipt_lot.as_ref().unwrap().source_buy_intent_id.clone();
        let parent=store.load_owner_technical_buy_intent(&id)?.context("LOCAL immutable parent")?;
        let b=config.owner_technical_buy.as_mut().unwrap();
        b.activated_at=parent.activated_at.to_rfc3339();b.expires_at=parent.expires_at.to_rfc3339();
        b.source_copy_exit_gate.as_mut().unwrap().bot_policy_identity=projection["native_bot_policy_identity"].as_str().unwrap().into();
        ensure!(crate::execution_owner_buy_authority::parent_identity(&config)?==parent,"LOCAL actual parent projection");
        let server=Server::start(&parent.wallet,16_224_558).await?;
        server.state.lock().unwrap().genesis=parent.genesis_hash;
        config.submit_adapter_http_url=format!("{}/rpc",server.url);config.quote_canary_base_url=format!("{}/swap/v1",server.url);
        config.canary_kill_switch_path=root.path().join("STOP").to_string_lossy().into_owned();
        let intent=recovery(&mut config,&store)?;
        server.state.lock().unwrap().quote_snapshot=Some(path.clone());
        // Use the normal startup parser/normalizer/validator on this exact pairing.
        let mut whole=toml::from_str::<toml::Value>(&raw)?;
        let e=whole["execution"].as_table_mut().unwrap();
        for (key,value) in [("submit_adapter_http_url",config.submit_adapter_http_url.clone()),
            ("quote_canary_base_url",config.quote_canary_base_url.clone()),
            ("canary_kill_switch_path",config.canary_kill_switch_path.clone())]{e.insert(key.into(),toml::Value::String(value));}
        e.insert("canary_entry_submit_enabled".into(),toml::Value::Boolean(false));
        let parent=e["owner_technical_buy"].as_table_mut().unwrap();
        let actual=config.owner_technical_buy.as_ref().unwrap();
        parent.insert("activate".into(),toml::Value::Boolean(false));
        parent.insert("activated_at".into(),toml::Value::String(actual.activated_at.clone()));
        parent.insert("expires_at".into(),toml::Value::String(actual.expires_at.clone()));
        parent["source_copy_exit_gate"].as_table_mut().unwrap().insert("bot_policy_identity".into(),
            toml::Value::String(actual.source_copy_exit_gate.as_ref().unwrap().bot_policy_identity.clone()));
        let p=config.owner_exit.as_ref().unwrap();
        e.insert("owner_exit".into(),toml::Value::try_from(json!({"policy":p.policy,"activate":p.activate,
            "run_id":p.run_id,"intent_id":p.intent_id,"buy_order_id":p.buy_order_id,
            "buy_receipt_signature":p.buy_receipt_signature,"position_id":p.position_id,
            "wallet_pubkey":p.wallet_pubkey,"signer_pubkey":p.signer_pubkey,"genesis_hash":p.genesis_hash,
            "mint":p.mint,"amount_raw":p.amount_raw,"decimals":p.decimals,"route":p.route,
            "activated_at":p.activated_at,"expires_at":p.expires_at,
            "max_priority_fee_lamports":p.max_priority_fee_lamports,"min_reserve_lamports":p.min_reserve_lamports,
            "max_slippage_bps":p.max_slippage_bps,"max_daily_loss_lamports":p.max_daily_loss_lamports}))?);
        let file=root.path().join("startup.LOCAL.toml");std::fs::write(&file,toml::to_string(&whole)?)?;
        let loaded=copybot_config::load_from_path(&file)?.execution;
        copybot_config::validate_owner_technical_buy(&loaded)?;
        ensure!(crate::execution_owner_exit_authority::configured(&loaded)?.as_ref()==Some(&intent),"LOCAL full startup config pairing");
        Ok(Self{root,path,store,config,server,intent})
    }
    pub async fn synthetic()->Result<Self>{
        let root=super::temporary_output_fixture::OutputRoot::new("source-recovery")?;
        let path=root.path().join("state.db");let mut store=SqliteStore::open(&path)?;
        store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"),"/../../migrations")))?;
        let scope=controlled_receipt_lot::Scope{policy:CONTROLLED_RECEIPT_LOT_V1.into(),source_run_id:"LOCAL-source".into(),
            bot_run_id:"LOCAL-bot".into(),source_wallet:exit::wallet(),bot_wallet:super::receipt_lot_custody_fixture::SOURCE.into(),
            source_buy_intent_id:"LOCAL-source-buy".into(),source_state_db_path:path.to_string_lossy().into_owned()};
        store.register_controlled_receipt_lot_scope(&scope,&scope.source_wallet)?;
        let bot=super::wsol_lifecycle_source_fixture::bot_config(&scope)?;
        let mut config=super::wsol_lifecycle_source_fixture::config(&scope,root.path(),&bot)?;
        let parent=crate::execution_owner_buy_authority::configured(&config)?.unwrap();
        store.register_owner_technical_buy_intent(&parent)?;
        let order=store.reserve_owner_technical_buy_order(&parent.intent_id,||Ok(Utc::now()))?.order;
        let (wire,receipt)=w::external_mock(&order,&parent.wallet,"buy",1034558,super::receipt_lot_custody_fixture::BUY_SIG,100)?;
        w::submitted(&store,&path,&order,&parent.wallet,"buy",super::receipt_lot_custody_fixture::BUY_SIG,&wire)?;
        let server=Server::start(&parent.wallet,16_224_558).await?;
        server.state.lock().unwrap().receipt=receipt;
        assert_eq!(record_execution_rpc_confirmation_boundary(&store,&reqwest::Client::new(),&format!("{}/rpc",server.url),
            &order.order_id,&parent.wallet,Utc::now(),1000).await?.buy_opened,1);
        config.submit_adapter_http_url=format!("{}/rpc",server.url);config.quote_canary_base_url=format!("{}/swap/v1",server.url);
        let intent=recovery(&mut config,&store)?;
        Ok(Self{root,path,store,config,server,intent})
    }
    pub fn sql(&self)->Result<rusqlite::Connection>{Ok(rusqlite::Connection::open(&self.path)?)}
    pub async fn runner(&self,c:ExecutionConfig)->Result<crate::execution_canary::ExecutionCanaryTickSummary>{
        let adapter=Arc::new(buy::SignedAdapter{config:c.clone(),payload:String::new(),signature:String::new(),
            fail_simulation:true,wrong_blueprint:false,simulation_time:None,kill_during_simulation:false,
            simulation_calls:Default::default(),signing_calls:Default::default()});
        let mut runner=crate::execution_canary::ExecutionCanaryRunner::new(c);
        runner.owner_exit_adapter=Some(adapter.clone());
        let result=runner.process_tick(&self.store,Utc::now()).await?;
        assert_eq!(adapter.signing_calls.load(std::sync::atomic::Ordering::SeqCst),0);
        if self.server.count("quote")>0 {
            assert_eq!(adapter.simulation_calls.load(std::sync::atomic::Ordering::SeqCst),1,"{result:?}");
        }
        Ok(result)
    }
}
