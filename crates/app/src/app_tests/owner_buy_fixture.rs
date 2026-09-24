//! Isolated synthetic key and loopback provider for the daemon owner BUY path.
use crate::execution_signing_envelope::{
    build_signed_transaction_execution_envelope, ExecutionSignedTransactionPayload,
    ExecutionSigningEnvelope,
};
use crate::execution_submit_adapter::{
    execution_submit_intent_from_signed_envelope, ExecutionSimulationFuture,
    ExecutionSimulationResult, ExecutionSubmitAdapter, ExecutionSubmitPlan,
    ExecutionSubmitRequest, ExecutionTransactionPlan, NoSubmitExecutionAdapter,
    JupiterMetisDryRunExecutionAdapter,
};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{Duration, Utc};
use copybot_config::{
    AssociationDeliveryConfig, DeliveryBudget, ExecutionConfig, IngestionConfig,
    OwnedSellPreparationConfig, OwnerTechnicalBuyConfig, OWNER_TECHNICAL_BUY_V1,
    RPC_FINALIZED_OWNED_SELL_V1,
};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) const MINT: &str = "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8";
pub(super) const GENESIS: &str = "11111111111111111111111111111111";

impl crate::execution_canary::ExecutionCanaryRunner {
    pub(crate) fn with_owner_buy_adapter(mut self, adapter: Arc<SignedAdapter>) -> Self {
        self.owner_buy_adapter = Some(adapter);
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SignedAdapter {
    pub config: ExecutionConfig,
    pub payload: String,
    pub signature: String,
    pub fail_simulation: bool,
    pub wrong_blueprint: bool,
    pub simulation_time: Option<chrono::DateTime<Utc>>,
    pub kill_during_simulation: bool,
    pub simulation_calls: Arc<AtomicUsize>,
    pub signing_calls: Arc<AtomicUsize>,
}
impl ExecutionSubmitAdapter for SignedAdapter {
    fn native_floor_config(&self) -> Result<&ExecutionConfig> { Ok(&self.config) }
    fn priority_fee_cap(&self) -> u64 { self.config.pretrade_max_priority_fee_lamports }
    fn build_transaction_plan(&self, r: &ExecutionSubmitRequest) -> Result<ExecutionTransactionPlan> {
        let mut plan = JupiterMetisDryRunExecutionAdapter::new(self.config.clone()).build_transaction_plan(r)?;
        if self.wrong_blueprint {
            plan.swap_blueprint.as_mut().expect("real builder provided blueprint")
                .input_amount_raw = "1".into();
        }
        Ok(plan)
    }
    fn simulate_transaction_plan<'a>(&'a self, _: &'a ExecutionTransactionPlan) -> ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            self.simulation_calls.fetch_add(1, Ordering::SeqCst);
            if self.kill_during_simulation {
                std::fs::write(&self.config.canary_kill_switch_path, "stop")?;
            }
            if let Some(time) = self.simulation_time {
                super::entry_risk_clock_fixture::advance_to(time);
            }
            Ok(ExecutionSimulationResult {
            status: if self.fail_simulation { "failed" } else { "ok" }.into(),
            error: self.fail_simulation.then(|| "synthetic_simulation_failed".into()),
        }) })
    }
    fn build_signing_envelope(&self, r: &ExecutionSubmitRequest, p: &ExecutionTransactionPlan) -> Result<ExecutionSigningEnvelope> {
        self.signing_calls.fetch_add(1, Ordering::SeqCst);
        let mut envelope = build_signed_transaction_execution_envelope(r, p,
            ExecutionSignedTransactionPayload { signed_transaction_base64:self.payload.clone(),
                tx_signature_hint:Some(self.signature.clone()) })?;
        envelope.priority_fee_proof = Some(crate::execution_priority_fee_proof::prove(
            r, &self.payload, self.priority_fee_cap())?);
        Ok(envelope)
    }
    fn plan_submit(&self, r: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        NoSubmitExecutionAdapter.plan_submit(r)
    }
    fn plan_submit_with_envelope(&self, r: &ExecutionSubmitRequest, e: &ExecutionSigningEnvelope) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::SubmitReady(
            execution_submit_intent_from_signed_envelope(r, e, "owner-buy-test".into())?))
    }
}

pub(super) fn signed_payload() -> Result<(String, String, [u8; 32])> {
    let key = SigningKey::from_bytes(&[11; 32]);
    let payer = key.verifying_key().to_bytes();
    let mut instructions = super::priority_fee_fixture::budget(200_000, 10_000);
    instructions.push(super::native_funding_fixture::transfer(payer, [52; 32], 10_000_000));
    let floor = crate::execution_native_floor::prepare_final_native_floor(
        payer, [9; 32], &instructions, 50_000_001)?;
    let mut wire = STANDARD.decode(floor.payload())?;
    let signature = key.sign(&wire[65..]);
    wire[1..65].copy_from_slice(&signature.to_bytes());
    Ok((STANDARD.encode(wire), bs58::encode(signature.to_bytes()).into_string(), payer))
}

pub(super) fn config(wallet: &str, url: &str, kill: &str) -> ExecutionConfig {
    let now = Utc::now();
    let mut c = ExecutionConfig::default();
    c.canary_enabled = true;
    c.canary_dry_run = true;
    c.canary_tiny_submit_enabled = true;
    c.quote_canary_enabled = true;
    c.canary_route = "jupiter_swap_instructions".into();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    c.execution_signer_keypair_path = "never-load-synthetic-test-key".into();
    c.canary_kill_switch_path = kill.into();
    c.canary_buy_size_sol = 0.01;
    c.quote_canary_buy_size_sol = 0.01;
    c.quote_canary_buy_slippage_bps = 500;
    c.canary_max_open_positions = 1;
    c.canary_max_daily_loss_sol = 0.02;
    c.pretrade_min_sol_reserve = 0.05;
    c.pretrade_max_priority_fee_lamports = 50_000;
    c.swap_instructions_dry_run_enabled = true;
    c.swap_transaction_dry_run_enabled = true;
    c.submit_adapter_http_url = url.into();
    c.quote_canary_base_url = format!("{url}/swap/v1");
    c.tiny_experiment.id = Some("owner-test-run".into());
    c.owned_sell_preparation = Some(OwnedSellPreparationConfig {
        policy: RPC_FINALIZED_OWNED_SELL_V1.into(), tiny_dispatch: true,
        fractional_inventory: Some("whole_wallet_parent_program_fraction_v1".into()),
        rpc_url: url.into(), genesis_hash: GENESIS.into(),
        identity: "owner-test-run".into(),
    });
    c.owner_technical_buy = Some(OwnerTechnicalBuyConfig {
        policy: OWNER_TECHNICAL_BUY_V1.into(), activate: true,
        run_id: "owner-test-run".into(), intent_id: "owner-test-intent".into(),
        wallet_pubkey: wallet.into(), signer_pubkey: wallet.into(),
        genesis_hash: GENESIS.into(), mint: MINT.into(), amount_lamports: 10_000_000,
        route: c.canary_route.clone(),
        activated_at: (now - Duration::seconds(5)).to_rfc3339(),
        expires_at: (now + Duration::minutes(20)).to_rfc3339(),
        max_priority_fee_lamports: 50_000, min_reserve_lamports: 50_000_000,
        max_slippage_bps: 500, max_daily_loss_lamports: 20_000_000,
        max_open_positions: 1, max_buy_count: 1,
    });
    c
}

pub(super) fn ingestion() -> IngestionConfig {
    let mut i = IngestionConfig::default();
    i.source = "yellowstone_grpc".into();
    i.yellowstone_delivery_mode = "durable_association_v1".into();
    let b = DeliveryBudget { count: 4, bytes: 4096 };
    i.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b.clone(), blocks: b.clone(), history: b.clone(),
        outputs: b.clone(), queue: b.clone(), inbox: b,
        input_bytes: 4096, metadata_bytes: 4096,
        pending_ttl_ms: 1000, block_ttl_ms: 1000, history_ttl_ms: 1000,
        tick_ms: 1000, sqlite_busy_ms: 1000,
    });
    i
}

pub(super) struct Server {
    pub url: String,
    pub calls: Arc<Mutex<Vec<String>>>,
    pub mode: Arc<Mutex<&'static str>>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl Drop for Server { fn drop(&mut self) { self.task.abort(); } }
impl Server {
    pub async fn start(wallet: String, signature: String) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let mode = Arc::new(Mutex::new("ok"));
        let fault = mode.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                let (header_end, body_len) = loop {
                    let mut chunk = [0u8; 4096];
                    let n = socket.read(&mut chunk).await?;
                    ensure!(n > 0 && bytes.len() < 1 << 20, "owner_fixture_request_size");
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(at) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..at]);
                        let len = header.lines().find_map(|line| line.to_ascii_lowercase()
                            .strip_prefix("content-length:").and_then(|v| v.trim().parse().ok())).unwrap_or(0);
                        if bytes.len() >= at + 4 + len { break (at, len); }
                    }
                };
                let header = String::from_utf8_lossy(&bytes[..header_end]);
                let request = if header.starts_with("GET ") {
                    let path = header.split_whitespace().nth(1).unwrap();
                    let url = reqwest::Url::parse(&format!("http://localhost{path}"))?;
                    let params: std::collections::HashMap<_,_> = url.query_pairs().into_owned().collect();
                    json!({"method":"quote","params":params})
                } else {
                    serde_json::from_slice::<Value>(&bytes[header_end+4..header_end+4+body_len])?
                };
                let method = request["method"].as_str().unwrap_or(if request.get("quoteResponse").is_some() { "instructions" } else { "unknown" });
                seen.lock().unwrap().push(method.into());
                let mode = *fault.lock().unwrap();
                let response = response(&request, mode, &wallet, &signature)?;
                let body = response.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
            }
        });
        Ok(Self { url, calls, mode, task })
    }
}

fn response(r: &Value, mode: &str, wallet: &str, signature: &str) -> Result<Value> {
    if r["method"] == "quote" {
        return Ok(json!({"inputMint":r["params"]["inputMint"],
            "outputMint":if mode=="wrong_mint" { "11111111111111111111111111111111" } else { MINT },
            "inAmount":if mode=="wrong_amount" { "9999999" } else { "10000000" },
            "outAmount":"10000","otherAmountThreshold":"9500","swapMode":"ExactIn",
            "slippageBps":500,"routePlan":[{"swapInfo":{"label":"Jupiter"}}],
            "outDecimals":3}));
    }
    let result = match r["method"].as_str() {
        Some("getGenesisHash") => json!(if mode=="wrong_genesis" { "wrong" } else { GENESIS }),
        Some("getAccountInfo") => {
            let mut mint = [0u8; 82]; mint[44]=3; mint[45]=1;
            json!({"context":{"slot":120},"value":{"owner":if mode=="token2022" {
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" } else {
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" },
                "executable":false,"data":[STANDARD.encode(mint),"base64"]}})
        }
        Some("getFeeForMessage") => json!({"context":{"slot":120},"value":19000}),
        Some("getMultipleAccounts") => json!({"context":{"slot":120},"value":r["params"][0]
            .as_array().unwrap().iter().map(|key| if key.as_str()==Some(wallet) {
                json!({"lamports":1_000_000_000u64,"owner":"11111111111111111111111111111111",
                    "executable":false,"data":["","base64"]})
            } else { Value::Null }).collect::<Vec<_>>() }),
        Some("getMinimumBalanceForRentExemption") => json!(2_039_280),
        Some("sendTransaction") => json!(if mode=="unknown_send" { "other-signature" } else { signature }),
        Some("getSignatureStatuses") => if mode=="unknown_send" {
            json!({"value":[null]})
        } else { json!({"value":[{"err":if mode.starts_with("failed_receipt") {
            json!({"InstructionError":[0,{"Custom":1}]}) } else { Value::Null },"slot":120,
            "confirmationStatus":"confirmed"}]}) },
        Some("getTransaction") if mode=="failed_receipt_unavailable" => Value::Null,
        Some("getTransaction") => json!({"slot":120,
            "transaction":{"signatures":[signature],"message":{"accountKeys":[
                {"pubkey":wallet,"signer":true,"writable":true},
                {"pubkey":bs58::encode([42;32]).into_string(),"signer":false,"writable":true}]}},
            "meta":{"err":if mode=="failed_receipt_available" {
                    json!({"InstructionError":[0,{"Custom":1}]}) } else { Value::Null },
                "fee":19000,"preBalances":[1_000_000_000,2_039_280],
                "postBalances":[if mode=="failed_receipt_available" {999_981_000} else {989_981_000},2_039_280],
                "preTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,
                    "uiTokenAmount":{"amount":"0","decimals":3}}],
                "postTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":MINT,
                    "uiTokenAmount":{"amount":"1000","decimals":3}}]}}),
        _ => anyhow::bail!("owner_fixture_unexpected_method"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}
