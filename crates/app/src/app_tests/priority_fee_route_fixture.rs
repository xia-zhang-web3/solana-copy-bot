use super::execution_pump_fun_direct_builder_contract::{
    pumpswap_global_config_data, pumpswap_pool_data, rpc_account_json,
};
use super::initial_sol_rpc_fixture::FundingRpc;
use super::priority_fee_fixture::{total_json, transaction};
use crate::execution_signing_envelope::*;
use crate::execution_submit_adapter::*;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use ed25519_dalek::SigningKey;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) const TOKEN: &str = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
const POOL: &str = "FhmcZfBdmvaYdphQ4qcd8qjs6wUv1Viic4UkiWqiZxxd";
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Route {
    Metis,
    MetisV0,
    Direct,
    Paid,
    DirectFallback,
    PaidFallback,
}

#[derive(Default)]
pub(super) struct WireOverrides {
    pub bundle: Option<Value>,
    pub transaction: Option<String>,
    pub guard: Option<u64>, // Explicit synthetic guarded-provider control; default stays unguarded.
    pub extension: bool,
    pub blockhash: u8,
    pub token2022: bool,
    pub submit_error: bool,
}

pub(super) struct Fixture {
    pub store: SqliteStore,
    pub request: ExecutionSubmitRequest,
    pub adapter: CountSigner,
    pub config: ExecutionConfig,
    pub now: DateTime<Utc>,
    pub calls: Arc<Mutex<Vec<(String, Value)>>>,
    pub price: Arc<AtomicU64>,
    pub wire: Arc<Mutex<WireOverrides>>,
    pub funding: Arc<Mutex<FundingRpc>>,
    pub simulation_responses: Arc<Mutex<std::collections::VecDeque<Value>>>,
    server: tokio::task::JoinHandle<()>,
    path: PathBuf,
    key_path: PathBuf,
}

impl Fixture {
    pub async fn new(route: Route, price: u64, limit: u32) -> Result<Self> {
        let now = Utc::now();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "copybot-fee-{}-{unique}-{sequence}.db",
            std::process::id()
        ));
        let key_path = path.with_extension("synthetic-key.json");
        let key = SigningKey::from_bytes(&[11; 32]);
        let payer = key.verifying_key().to_bytes();
        std::fs::write(
            &key_path,
            serde_json::to_vec(&[key.to_bytes().to_vec(), payer.to_vec()].concat())?,
        )?;
        let mut store = SqliteStore::open(&path)
            .with_context(|| format!("open isolated priority fixture {}", path.display()))?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let mut config = ExecutionConfig::default();
        config.canary_enabled = true;
        config.canary_dry_run = true;
        config.canary_tiny_submit_enabled = true;
        config.canary_route =
            crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.into();
        config.canary_wallet_pubkey = bs58::encode(payer).into_string();
        config.execution_signer_pubkey = config.canary_wallet_pubkey.clone();
        config.execution_signer_keypair_path = key_path.to_string_lossy().into();
        config.pretrade_max_priority_fee_lamports = 500_000;
        config.quote_canary_base_url = url.clone();
        config.submit_adapter_http_url = url;
        config.swap_instructions_dry_run_enabled = true;
        config.swap_transaction_dry_run_enabled = true;
        config.quote_canary_pump_fun_parallel_enabled =
            matches!(route, Route::Paid | Route::PaidFallback);
        config.quote_canary_timeout_ms = 500;
        config.submit_timeout_ms = 500;
        config.max_submit_attempts = 3;
        config.canary_buy_size_sol = 0.01;
        let signal = copybot_core_types::CopySignalRow {
            signal_id: "fee-signal".into(),
            wallet_id: "leader".into(),
            side: "buy".into(),
            token: TOKEN.into(),
            notional_sol: 0.01,
            notional_lamports: Some(copybot_core_types::Lamports::new(10_000_000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "shadow_recorded".into(),
        };
        store.insert_copy_signal(&signal)?;
        let order = store
            .reserve_execution_canary_order(&signal.signal_id, &config.canary_route, now)?
            .order;
        let route_plan = match route {
            Route::Direct | Route::DirectFallback => {
                json!([{"swapInfo":{"label":"Pump.fun Amm","ammKey":POOL}}])
            }
            Route::Paid | Route::PaidFallback => json!([{"swapInfo":{"label":"Pump.fun Amm"}}]),
            Route::Metis | Route::MetisV0 => json!([{"swapInfo":{"label":"Metis"}}]),
        };
        let quote = json!({"inputMint":"So11111111111111111111111111111111111111112","outputMint":TOKEN,
            "inAmount":"10000000","outAmount":"123456","otherAmountThreshold":"110000",
            "swapMode":"ExactIn","slippageBps":500,"routePlan":route_plan,"priceImpactPct":"0"});
        let metadata = ExecutionBuildPlanMetadata {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            quote_source: Some(
                if route == Route::Paid {
                    crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID
                } else {
                    crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS
                }
                .into(),
            ),
            quote_event_id: Some("fee-quote".into()),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("10000000".into()),
            quote_out_amount_raw: Some("123456".into()),
            quote_response_json: Some(quote.to_string()),
            quote_price_sol: Some(0.01 / 123456.0),
            route_plan_json: Some(route_plan.to_string()),
            slippage_bps: Some(0.0),
            decision_status: Some("would_execute".into()),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: if route == Route::Direct {
                None
            } else {
                Some(120_000)
            },
            priority_fee_json: Some(if route == Route::Direct {
                crate::execution_priority_fee::sample_quicknode_fee(&json!({"recommended":price}))?
                    .1
            } else {
                total_json(120_000)
            }),
            ..Default::default()
        };
        let request = ExecutionSubmitRequest {
            order_id: order.order_id,
            signal_id: signal.signal_id,
            client_order_id: order.client_order_id,
            attempt: order.attempt,
            route: order.route,
            wallet_id: signal.wallet_id,
            token: signal.token,
            side: signal.side,
            buy_size_sol: 0.01,
            slippage_tolerance_bps: 500,
            wallet_pubkey: config.canary_wallet_pubkey.clone(),
            entry_route_plan_json: None,
            metadata,
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let price = Arc::new(AtomicU64::new(price));
        let simulation_responses = Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let wire = Arc::new(Mutex::new(WireOverrides::default()));
        let funding = Arc::new(Mutex::new(FundingRpc::default()));
        let server = tokio::spawn(serve(
            listener,
            route,
            payer,
            limit,
            price.clone(),
            calls.clone(),
            quote,
            simulation_responses.clone(),
            wire.clone(),
            funding.clone(),
        ));
        let adapter = CountSigner {
            inner: JupiterMetisDryRunExecutionAdapter::new(config.clone()),
            count: AtomicUsize::new(0),
            cap: config.pretrade_max_priority_fee_lamports,
        };
        Ok(Self {
            store,
            request,
            adapter,
            config,
            now,
            calls,
            price,
            simulation_responses,
            wire,
            funding,
            server,
            path,
            key_path,
        })
    }

    pub fn sync_config(&mut self) {
        self.adapter = CountSigner {
            inner: JupiterMetisDryRunExecutionAdapter::new(self.config.clone()),
            count: AtomicUsize::new(0),
            cap: self.config.pretrade_max_priority_fee_lamports,
        };
    }
    pub fn conn(&self) -> Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(&self.path)?)
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.server.abort();
        match (&mut self.server).await {
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(e.into()),
            Ok(()) => Ok(()),
        }
    }

    pub fn make_sell(&mut self) -> Result<()> {
        rusqlite::Connection::open(&self.path)?.execute(
            "UPDATE copy_signals SET side = 'sell' WHERE signal_id = ?1",
            [&self.request.signal_id],
        )?;
        self.request.side = "sell".into();
        self.request.metadata.quote_in_amount_raw = Some("123456".into());
        self.request.metadata.quote_out_amount_raw = Some("10000000".into());
        let mut quote: Value = serde_json::from_str(
            self.request
                .metadata
                .quote_response_json
                .as_deref()
                .unwrap(),
        )?;
        quote["inputMint"] = json!(TOKEN);
        quote["outputMint"] = json!("So11111111111111111111111111111111111111112");
        quote["inAmount"] = json!("123456");
        quote["outAmount"] = json!("10000000");
        quote["otherAmountThreshold"] = json!("9500000");
        self.request.metadata.quote_response_json = Some(quote.to_string());
        self.store.record_execution_canary_open_position(
            "fee-owned",
            TOKEN,
            123456.0,
            Some(copybot_core_types::TokenQuantity::new(123456, 0)),
            0.01,
            self.now,
        )?;
        super::owned_sell_fixture::bind(&self.store, &mut self.request, 123456, 0)?;
        Ok(())
    }

    pub async fn build(
        &self,
    ) -> Result<crate::execution_canary_signing_contract::ExecutionSigningEnvelopeOutcome> {
        let plan = self.adapter.build_transaction_plan(&self.request)?;
        crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
            &self.store,
            &plan,
            self.now,
        )?;
        self.store
            .mark_execution_canary_built(&self.request.order_id, self.now)?;
        let simulation = self.adapter.simulate_transaction_plan(&plan).await?;
        assert_eq!(
            simulation.status,
            copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED
        );
        self.store.mark_execution_canary_simulated(
            &self.request.order_id,
            self.now,
            &simulation.status,
            None,
        )?;
        crate::execution_canary_signing_contract::record_execution_signing_envelope(
            &self.store,
            &self.adapter,
            &self.request,
            &plan,
            self.now,
        )
    }

    pub async fn submit(
        &self,
        envelope: &ExecutionSigningEnvelope,
    ) -> Result<crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome> {
        crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
            &self.store,
            &self.adapter,
            &self.request,
            envelope,
            &crate::execution_canary_submit_contract::ExecutionTinySubmitGate::from_config(
                &self.config,
            ),
            &RpcExecutionSubmitTransport::new(self.config.submit_adapter_http_url.clone()),
            self.now,
        )
        .await
    }
    pub fn sends(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, v)| v["method"] == "sendTransaction")
            .count()
    }
    pub fn signatures(&self) -> usize {
        self.adapter.count.load(Ordering::SeqCst)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(&self.key_path);
    }
}

pub(super) struct CountSigner {
    inner: JupiterMetisDryRunExecutionAdapter,
    count: AtomicUsize,
    cap: u64,
}
impl ExecutionSubmitAdapter for CountSigner {
    fn native_floor_config(&self) -> Result<&ExecutionConfig> {
        self.inner.native_floor_config()
    }
    fn priority_fee_cap(&self) -> u64 {
        self.cap
    }
    fn build_transaction_plan(
        &self,
        request: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        self.inner.build_transaction_plan(request)
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        plan: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        self.inner.simulate_transaction_plan(plan)
    }
    fn sign_serialized_transaction(
        &self,
        request: &ExecutionSubmitRequest,
        plan: &ExecutionTransactionPlan,
        payload: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner
            .sign_serialized_transaction(request, plan, payload)
    }
    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        self.inner.plan_submit(request)
    }
    fn plan_submit_with_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        envelope: &ExecutionSigningEnvelope,
    ) -> Result<ExecutionSubmitPlan> {
        self.inner.plan_submit_with_envelope(request, envelope)
    }
}

async fn serve(
    listener: tokio::net::TcpListener,
    route: Route,
    payer: [u8; 32],
    limit: u32,
    price: Arc<AtomicU64>,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
    quote: Value,
    simulation_responses: Arc<Mutex<std::collections::VecDeque<Value>>>,
    wire: Arc<Mutex<WireOverrides>>,
    funding: Arc<Mutex<FundingRpc>>,
) {
    let mut accounts = 0;
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (path, body) = read_request(&mut stream).await;
        calls.lock().unwrap().push((path.clone(), body.clone()));
        let response = if body["id"]
            .as_str()
            .is_some_and(|s| s.starts_with("native-funding-"))
        {
            let delay = funding.lock().unwrap().delay_ms;
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            funding.lock().unwrap().reply(&body)
        } else if path.starts_with("GET /quote?") {
            quote.clone()
        } else if path.contains("/pump-fun/") && route == Route::PaidFallback {
            json!({"error":"synthetic paid builder unavailable"})
        } else if path.contains("swap-instructions") {
            if let Some(bundle) = wire.lock().unwrap().bundle.clone() {
                bundle
            } else if body["quoteResponse"]["outputMint"]
                == crate::execution_quote_canary_helpers::SOL_MINT
                && !path.contains("/pump-fun/")
            {
                super::generic_sell_synthetic_fixture::bundle(
                    payer,
                    limit,
                    price.load(Ordering::SeqCst),
                )
            } else {
                json!({"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"instructions":[{"programId":"synthetic"}],"simulationError":null})
            }
        } else if path.contains("/swap ") {
            let override_tx = wire.lock().unwrap().transaction.clone();
            if let Some(tx) = override_tx {
                json!({"swapTransaction":tx,"tx":tx,"simulationError":null})
            } else {
                json!({"swapTransaction":mock_transaction(route, payer, limit, price.load(Ordering::SeqCst), wire.lock().unwrap().guard),"tx":mock_transaction(route, payer, limit, price.load(Ordering::SeqCst), wire.lock().unwrap().guard),"simulationError":null})
            }
        } else {
            match body["method"].as_str().unwrap_or("") {
                "getMultipleAccounts" => {
                    accounts += 1;
                    if route == Route::DirectFallback {
                        json!({"error":{"message":"synthetic direct unavailable"}})
                    } else {
                        let data = if accounts % 2 == 1 {
                            vec![
                                rpc_account_json(
                                    &pumpswap_global_config_data(),
                                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
                                ),
                                rpc_account_json(
                                    &{
                                        let mut data = pumpswap_pool_data(TOKEN);
                                        if wire.lock().unwrap().extension { data.truncate(245); }
                                        data
                                    },
                                    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
                                ),
                            ]
                        } else {
                            vec![
                                rpc_account_json(
                                    &[],
                                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                                ),
                                rpc_account_json(
                                    &[],
                                    if wire.lock().unwrap().token2022 {
                                        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
                                    } else { "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" },
                                ),
                            ]
                        };
                        json!({"result":{"value":data.iter().map(|s| serde_json::from_str::<Value>(s).unwrap()).collect::<Vec<_>>()}})
                    }
                }
                "getLatestBlockhash" => {
                    json!({"result":{"value":{"blockhash":bs58::encode([wire.lock().unwrap().blockhash; 32]).into_string(),"lastValidBlockHeight":1}}})
                }
                "simulateTransaction" => {
                    simulation_responses.lock().unwrap().pop_front().unwrap_or_else(||
                        json!({"jsonrpc":"2.0","id":body["id"],"result":{"context":{"slot":42},"value":{"err":null,"logs":[],"unitsConsumed":1}}}))
                }
                "sendTransaction" => if wire.lock().unwrap().submit_error {
                    json!({"error":{"message":"synthetic not sent"}})
                } else { json!({"result":"synthetic-fee-submit"}) },
                "getSignatureStatuses" => json!({"result":{"value":[null]}}),
                other => panic!("unexpected loopback fee request: {path} {other}"),
            }
        };
        let text = response.to_string();
        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}", text.len()).as_bytes()).await.unwrap();
    }
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 8192];
        let count = stream.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..end]);
            let length = header
                .lines()
                .find_map(|s| {
                    s.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|n| n.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + length {
                return (
                    header.lines().next().unwrap().into(),
                    if length == 0 {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap()
                    },
                );
            }
        }
    }
}

fn mock_transaction(
    route: Route,
    payer: [u8; 32],
    limit: u32,
    price: u64,
    guard: Option<u64>,
) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let tx = if let Some(r) = guard {
        crate::execution_native_floor::prepare_final_native_floor(
            payer,
            [9; 32],
            &super::priority_fee_fixture::budget(limit, price),
            r,
        )
        .unwrap()
        .payload()
        .to_owned()
    } else {
        transaction(payer, limit, price)
    };
    if route != Route::MetisV0 {
        return tx;
    }
    let mut bytes = STANDARD.decode(tx).unwrap();
    bytes.insert(65, 0x80);
    bytes.push(0);
    STANDARD.encode(bytes)
}
