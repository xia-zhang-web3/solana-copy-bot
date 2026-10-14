//! Early recovery on synthetic history, or an untouched copy of saved COPY40.
//! All quote/RPC/simulation/send replies are loopback mocks, never live evidence.
use super::owner_native_recovery_fixture::NativeCase;
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_config::{OwnerNativeRecoveryConfig, RecoveryUsdcBaselineConfig};
use copybot_storage_core::SqliteStore;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) async fn early_case() -> Result<NativeCase> {
    let mut c = Box::pin(NativeCase::new()).await?;
    let p = c.config.owner_exit.as_mut().unwrap();
    let wallet = crate::execution_pumpswap_accounts::parse_pubkey(&p.wallet_pubkey, "mock_wallet")?;
    let mint = crate::execution_pumpswap_accounts::parse_pubkey(&p.mint, "mock_mint")?;
    let ata = crate::execution_pumpswap_accounts::associated_token_address(
        &wallet, &mint, &crate::execution_pumpswap_accounts::token_program_id());
    p.native_recovery = Some(OwnerNativeRecoveryConfig {
        policy: copybot_config::FRESH_NATIVE_RECOVERY_V1.into(),
        original_activated_at: p.activated_at.clone(),
        original_expires_at: p.expires_at.clone(),
        original_policy_identity: "b".repeat(64),
        protected_bot_usdc_baseline: RecoveryUsdcBaselineConfig {
            policy: copybot_config::RECOVERY_USDC_100_V1.into(),
            amount_raw: 100, token_account: bs58::encode(ata).into_string(),
        },
    });
    let mut s = c.provider.state.lock().unwrap();
    s.protected_baseline_raw = 100;
    s.protected_balance_slot = Some(122);
    drop(s);
    Ok(c)
}

pub(super) struct SavedCase {
    pub root: super::temporary_output_fixture::OutputRoot,
    pub config: copybot_config::ExecutionConfig,
    pub store: SqliteStore,
    pub state: Arc<Mutex<super::owner_roundtrip_fixture::State>>,
    pub adapter: Arc<super::owner_buy_fixture::SignedAdapter>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl Drop for SavedCase { fn drop(&mut self) { self.task.abort(); } }
impl SavedCase {
    pub async fn new() -> Result<Self> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let root = super::temporary_output_fixture::OutputRoot::new("saved40-early100")?;
        let captured = std::env::var("COPYBOT_NATIVE_RECOVERY_PARENT_DB")?;
        let path = root.path().join("state.db");
        std::fs::copy(&captured, &path)?;
        ensure!(std::fs::read(&captured)? == std::fs::read(&path)?, "saved40_copy_changed");
        let mut config = copybot_config::load_from_path(
            std::env::var("COPYBOT_OWNER_NATIVE_RECOVERY_CONFIG")?)?.execution;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        config.submit_adapter_http_url = url.clone();
        config.quote_canary_base_url = format!("{url}/swap/v1");
        config.canary_kill_switch_path = root.path().join("STOP").to_string_lossy().into_owned();
        config.execution_signer_keypair_path = "NEVER_LOAD_REAL_OR_MOCK_OWNER_KEY".into();
        let p = config.owner_exit.as_mut().unwrap();
        p.activate = true;
        let original = p.native_recovery.as_ref().unwrap();
        p.activated_at = original.original_activated_at.clone();
        p.expires_at = original.original_expires_at.clone();
        let payer = crate::execution_pumpswap_accounts::parse_pubkey(&p.wallet_pubkey, "saved40_wallet")?;
        let raw = p.amount_raw;
        let floor = p.min_reserve_lamports;
        let genesis = p.genesis_hash.clone();
        let mut instructions = super::owner_exit_test_fixture::instructions(payer)?;
        let swap = instructions.iter_mut().find(|i| i.data.starts_with(&[229,23,203,151,122,227,173,42])).unwrap();
        swap.data[16..24].copy_from_slice(&raw.to_le_bytes());
        let wire = crate::execution_native_floor::prepare_final_native_floor(payer, [9;32], &instructions, floor)?;
        let mut bytes = STANDARD.decode(wire.payload())?;
        // Explicit mock signature: no real key is loaded or signing performed.
        bytes[1..65].copy_from_slice(&[37;64]);
        let payload = STANDARD.encode(bytes);
        let adapter = Arc::new(super::owner_buy_fixture::SignedAdapter {
            config: config.clone(), payload, signature: bs58::encode([37;64]).into_string(),
            fail_simulation: false, wrong_blueprint: false, simulation_time: None,
            kill_during_simulation: false, simulation_calls: Default::default(),
            signing_calls: Default::default(),
        });
        let mut initial = super::owner_roundtrip_fixture::State::default();
        initial.sent = Some("MOCK_PRIOR_CONFIRMED_BUY".into());
        initial.received_raw = Some(raw);
        initial.protected_baseline_raw = 100;
        initial.protected_balance_slot = Some(456954056);
        let state = Arc::new(Mutex::new(initial));
        let shared = state.clone();
        let kill = config.canary_kill_switch_path.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut chunk = [0;4096]; let n = socket.read(&mut chunk).await?;
                    ensure!(n > 0 && bytes.len() < 1<<20, "saved40_mock_request_size");
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(at) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..at]);
                        let length = header.lines().find_map(|l| l.to_ascii_lowercase()
                            .strip_prefix("content-length:").and_then(|v| v.trim().parse().ok())).unwrap_or(0);
                        if bytes.len() >= at+4+length { break (at,length); }
                    }
                };
                let header = String::from_utf8_lossy(&bytes[..end]);
                let request = if header.starts_with("GET ") {
                    let uri = reqwest::Url::parse(&format!("http://localhost{}", header.split_whitespace().nth(1).unwrap()))?;
                    let params: std::collections::HashMap<_,_> = uri.query_pairs().into_owned().collect();
                    serde_json::json!({"method":"quote","params":params})
                } else { serde_json::from_slice(&bytes[end+4..end+4+length])? };
                let mut response = super::owner_roundtrip_fixture::response(&request, payer, &kill, &mut shared.lock().unwrap())?;
                match request["method"].as_str() {
                    Some("getGenesisHash") => response["result"] = serde_json::json!(genesis),
                    Some("getSignatureStatuses") if response["result"]["value"][0].is_object() => response["result"]["value"][0]["slot"] = serde_json::json!(456954056),
                    Some("getTransaction") if response["result"].is_object() => response["result"]["slot"] = serde_json::json!(456954056),
                    _ => {},
                }
                let body = response.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
            }
        });
        Ok(Self { root, store: SqliteStore::open(path)?, config, state, adapter, task })
    }
    pub fn calls(&self, name: &str) -> usize {
        self.state.lock().unwrap().calls.iter().filter(|c| c.as_str() == name).count()
    }
    /// Saved owner's key is never available to this test. Substitute ONLY the
    /// signing/transport seam; real native parent, quote/wire/SQLite guards and
    /// receipt settlement run on the exact copied graph. This is not submit proof.
    pub async fn mock_atomic_transport_and_reconcile(&self) -> Result<()> {
        use crate::execution_submit_adapter::{ExecutionSubmitAdapter, ExecutionSubmitRequest};
        use copybot_storage_core::{ExecutionCanaryDispatch, ExecutionDispatchClaim, TinyBudgetClaim};
        let now = Utc::now();
        let i = crate::execution_owner_exit_authority::configured(&self.config)?.unwrap();
        self.store.register_owner_exit_intent(&i)?;
        let check = || crate::execution_owner_exit_authority::current(&self.config, &self.store, &i, Utc::now());
        check()?;
        let http = reqwest::Client::builder().no_proxy().build()?;
        crate::execution_owner_native_recovery::verify_balance(&http, &self.config, &self.store, &i, &check).await?;
        let metadata = crate::execution_owner_exit_quote::fetch(&http, &self.config, &i).await?;
        let order = self.store.reserve_owner_exit_order(&i.intent_id, || Ok(now))?.order;
        let request = ExecutionSubmitRequest {
            order_id: order.order_id.clone(), signal_id: order.signal_id.clone(),
            client_order_id: order.client_order_id.clone(), attempt: 1,
            route: i.route.clone(), wallet_id: i.wallet.clone(), wallet_pubkey: i.wallet.clone(),
            token: i.mint.clone(), side: "sell".into(), buy_size_sol: 0.01,
            slippage_tolerance_bps: 50, entry_route_plan_json: None, metadata,
        };
        let plan = self.adapter.build_transaction_plan(&request)?;
        self.store.mark_execution_canary_built(&order.order_id, now)?;
        self.store.mark_execution_canary_simulated(&order.order_id, now,
            copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED, None)?;
        crate::execution_owner_exit_authority::plan(&self.store, &request, &plan)?;
        crate::execution_owner_exit_wire::verify(&request, &self.adapter.payload)?;
        check()?;
        let d = ExecutionCanaryDispatch {
            order_id: order.order_id.clone(), signal_id: order.signal_id,
            client_order_id: order.client_order_id, route: i.route.clone(), attempt: 1,
            wallet: i.wallet.clone(), token: i.mint.clone(), side: "sell".into(),
            tx_signature: self.adapter.signature.clone(),
            transaction_sha256: crate::execution_owned_sell_rpc::digest(&self.adapter.payload),
            message_sha256: "7".repeat(64),
        };
        let budget = TinyBudgetClaim {
            experiment_id: i.run_id.clone(), wallet: i.wallet.clone(),
            tx_signature: d.tx_signature.clone(), message_sha256: d.message_sha256.clone(),
            transaction_sha256: d.transaction_sha256.clone(), buy_lamports: Some(0),
            protected_capital: None, total_fee: 19000, priority_fee: 0, fee_slot: 456954056,
        };
        let simulated = self.store.load_execution_canary_order(&d.order_id)?.unwrap();
        assert_eq!(self.store.claim_owner_exit_dispatch(&simulated, &d, &budget, || Ok(now))?, ExecutionDispatchClaim::New);
        // An exact persisted duplicate claim grants no additional transport.
        assert_eq!(self.store.claim_owner_exit_dispatch(&simulated, &d, &budget, || Ok(now))?, ExecutionDispatchClaim::Existing);
        let response: serde_json::Value = http.post(&self.config.submit_adapter_http_url)
            .json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":[self.adapter.payload]}))
            .send().await?.json().await?;
        assert_eq!(response["result"].as_str(), Some(d.tx_signature.as_str()));
        crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
            &self.store, &self.config, &d.order_id, &http,
            &self.config.submit_adapter_http_url, now, 1000).await?;
        Ok(())
    }
    pub async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        let mut runner = crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone());
        runner.owner_exit_adapter = Some(self.adapter.clone());
        Box::pin(runner.process_tick(&self.store, Utc::now())).await
    }
}
