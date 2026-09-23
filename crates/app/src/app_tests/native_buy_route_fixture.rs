//! In-process external BUY operands for the native route tests.
use crate::execution_signing_envelope::{
    build_signed_transaction_execution_envelope, ExecutionSignedTransactionPayload,
    ExecutionSigningEnvelope,
};
use crate::execution_submit_adapter::{
    execution_submit_intent_from_signed_envelope, ExecutionBuildPlanMetadata,
    ExecutionSimulationFuture, ExecutionSimulationResult, ExecutionSubmitAdapter,
    ExecutionSubmitPlan, ExecutionSubmitRequest, ExecutionTransactionPlan,
    NoSubmitExecutionAdapter,
};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use copybot_config::{
    ExecutionConfig, NativeFreshBuyConfig, OwnedSellPreparationConfig,
    PROCESSED_SLOT_FENCE_AVAILABILITY_V1, RPC_FINALIZED_OWNED_SELL_V1,
};
use copybot_core_types::association_delivery::{
    AdmissionFacts, BlockTime, CandidateGeneration, CheckedFacts, Delivery, DeliveryEvent,
    InfoIdentity, MessageTime, ProviderAssertion, Terminal,
};
use copybot_core_types::association_parent::ParentObservation;
use copybot_core_types::CopySignalRow;
use copybot_core_types::ExactSwapAmounts;
use copybot_storage_core::{
    association_inbox::AssociationInbox, SqliteStore,
};
use crate::association_consumer::AssociationConsumer;
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use copybot_ingestion::{IngestionService, ReplayInput};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{json, Value};

pub(super) const SOL: &str = "So11111111111111111111111111111111111111112";
pub(super) const GENESIS: &str = "11111111111111111111111111111111";
pub(super) const MINT: &str = "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8";
pub(super) const LEADER: &str = "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi";
pub(super) const SOURCE_SIGNATURE: &str =
    "31R69oCVXJaEuUtWz7Cx4BChaiqeXPkfUGHJg6WKUUry3FTn1V7wkn2s11GA83AVTxksTENbh1A3Lh667zuRSpxj";

pub(super) struct SignedAdapter {
    pub(super) config: ExecutionConfig,
    pub(super) payload: String,
    pub(super) signature: String,
    pub(super) substitute_cohort_at_simulation: Option<String>,
}
impl ExecutionSubmitAdapter for SignedAdapter {
    fn native_floor_config(&self) -> Result<&ExecutionConfig> {
        Ok(&self.config)
    }
    fn priority_fee_cap(&self) -> u64 {
        self.config.pretrade_max_priority_fee_lamports
    }
    fn build_transaction_plan(
        &self,
        request: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        NoSubmitExecutionAdapter.build_transaction_plan(request)
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        _: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        Box::pin(async move {
            if let Some(path) = &self.substitute_cohort_at_simulation {
                rusqlite::Connection::open(path)?.execute(
                    "UPDATE discovery_candidate_sources SET source_cohort='substituted' WHERE wallet_id=?1",[LEADER])?;
            }
            Ok(ExecutionSimulationResult {
                status: "ok".into(),
                error: None,
            })
        })
    }
    fn build_signing_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        plan: &ExecutionTransactionPlan,
    ) -> Result<ExecutionSigningEnvelope> {
        let mut envelope = build_signed_transaction_execution_envelope(
            request,
            plan,
            ExecutionSignedTransactionPayload {
                signed_transaction_base64: self.payload.clone(),
                tx_signature_hint: Some(self.signature.clone()),
            },
        )?;
        envelope.priority_fee_proof = Some(crate::execution_priority_fee_proof::prove(
            request,
            &self.payload,
            self.priority_fee_cap(),
        )?);
        Ok(envelope)
    }
    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        NoSubmitExecutionAdapter.plan_submit(request)
    }
    fn plan_submit_with_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        envelope: &ExecutionSigningEnvelope,
    ) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::SubmitReady(
            execution_submit_intent_from_signed_envelope(request, envelope, "native-test".into())?,
        ))
    }
}

pub(super) fn config(wallet: &str) -> ExecutionConfig {
    let mut c = ExecutionConfig::default();
    c.canary_enabled = true;
    c.canary_dry_run = true;
    c.canary_tiny_submit_enabled = true;
    c.quote_canary_enabled = true;
    c.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.into();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    c.execution_signer_keypair_path = "never-load-test-key".into();
    c.pretrade_min_sol_reserve = 0.05;
    c.pretrade_max_priority_fee_lamports = 50_000;
    c.canary_buy_size_sol = 0.01;
    c.quote_canary_buy_size_sol = 0.01;
    c.quote_canary_buy_slippage_bps = 500;
    c.swap_instructions_dry_run_enabled = true;
    c.swap_transaction_dry_run_enabled = true;
    c.submit_adapter_http_url = "http://127.0.0.1:1".into();
    c.quote_canary_base_url = "http://127.0.0.1:1/swap/v1".into();
    c.tiny_experiment.id = Some("fractional-test".into());
    c.native_fresh_buy = Some(NativeFreshBuyConfig {
        policy: PROCESSED_SLOT_FENCE_AVAILABILITY_V1.into(),
    });
    c.owned_sell_preparation = Some(OwnedSellPreparationConfig {
        policy: RPC_FINALIZED_OWNED_SELL_V1.into(),
        tiny_dispatch: true,
        fractional_inventory: Some("whole_wallet_parent_program_fraction_v1".into()),
        rpc_url: c.submit_adapter_http_url.clone(),
        genesis_hash: GENESIS.into(),
        identity: "native-buy-test".into(),
    });
    c
}

pub(super) fn delivery(sequence: u64, event: DeliveryEvent) -> Delivery {
    Delivery {
        session: "native-session-A".into(),
        sequence,
        arrival_offset_ns: sequence,
        event,
    }
}
pub(super) fn admission() -> AdmissionFacts {
    AdmissionFacts {
        facts: CheckedFacts {
            signature: SOURCE_SIGNATURE.into(),
            slot: 100,
            wallet: LEADER.into(),
            token_in: SOL.into(),
            token_out: MINT.into(),
            amount_in_bits: 0.01f64.to_bits(),
            amount_out_bits: 10.0f64.to_bits(),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "10000000".into(),
                amount_in_decimals: 9,
                amount_out_raw: "10000".into(),
                amount_out_decimals: 3,
            }),
            programs: vec!["program".into()],
            dex: "fixture".into(),
            program_fallback: false,
        },
        info: InfoIdentity {
            encoded: vec![1, 2, 3],
            float_bits: vec![],
        },
        message_time: MessageTime::Missing,
    }
}

pub(super) async fn actual_source_replay(
    store: &SqliteStore, path: &str, execution: &ExecutionConfig,
) -> Result<()> {
    let input = crate::app_tests::b135_fixture::inputs();
    let metadata: Value = serde_json::from_slice(&std::fs::read(input.join("chain.json"))?)?;
    let mut config = crate::app_tests::association_fixture::config(&metadata);
    config.execution = execution.clone();
    copybot_config::validate_association_delivery(&config)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut ingestion = IngestionService::with_replay(
        &config, receiver, "native-route-source".into(),
    )?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut ingestion, &config.ingestion, &config.execution, path,
    ).await?.expect("durable source consumer");
    let mut rpc = Parsed(|request: Value| async move {
        let result = match request["method"].as_str() {
            Some("getGenesisHash") => json!(GENESIS),
            Some("getSlot") => json!(99),
            other => anyhow::bail!("unexpected source fence method: {other:?}"),
        };
        Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    });
    consumer.poll_with_transport(store, Some(&mut rpc)).await?;
    for (offset, name) in [(1, "source"), (2, "block-100")] {
        sender.send(ReplayInput::Update { offset_ns: offset,
            payload: std::fs::read(input.join(format!("{name}.pb")))?,
        }).await?;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            consumer.poll_with_transport(store, Some(&mut rpc)).await?;
            if store.list_native_buy_pending(1)?.iter()
                .any(|pending| pending.signature == SOURCE_SIGNATURE) { break; }
        }
        Ok::<_, anyhow::Error>(())
    }).await??;
    Ok(())
}
pub(super) fn quote(
    store: &SqliteStore,
    signal: &CopySignalRow,
    config: &ExecutionConfig,
    decision_id: &str,
    now: chrono::DateTime<Utc>,
    bad_price: bool,
) -> Result<ExecutionBuildPlanMetadata> {
    let event_id = format!("quote:entry:{}", signal.signal_id);
    let route = json!([{"swapInfo":{"label":"Metis"}}]);
    let initial_out = if bad_price { "9000" } else { "10000" };
    let initial_threshold = if bad_price { "8550" } else { "9500" };
    let body = json!({"inputMint":SOL,"outputMint":MINT,"inAmount":"10000000",
        "outAmount":initial_out,"otherAmountThreshold":initial_threshold,"swapMode":"ExactIn",
        "slippageBps":500,"routePlan":route,"priceImpactPct":"0"});
    let sample = crate::execution_quote_canary_helpers::QuoteSample {
        http_request_started_ts: Some(now),
        quote_response_available_ts: Some(now),
        in_amount: "10000000".into(), out_amount: initial_out.into(),
        response_json: body.to_string(), price_impact_pct: Some(0.0),
        route_plan_json: Some(route.to_string()), in_decimals: Some(9),
        out_decimals: Some(3), latency_ms: 0,
    };
    let priority = crate::execution_quote_canary_helpers::PriorityFeeSample {
        status: "ok".into(), lamports: Some(2000),
        json: Some(crate::app_tests::priority_fee_fixture::total_json(2000)),
        error: None,
    };
    let guard = crate::execution_canary_route::NativeBuyGuard::new(
        config, &signal.signal_id, decision_id, now,
    )?;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(config.clone());
    let summary = runner.process_native_buy_signal_with_mock_external_quote(
        store, signal, &guard, now, sample.clone(), Some(&priority),
    )?;
    assert_eq!(summary.entry_inserted, 1);
    let saved = store.load_execution_quote_canary_event_by_id(&event_id)?
        .expect("native quote event");
    assert!(saved.signal_ts.is_none());
    assert!(saved.decision_delay_ms.is_none());
    assert_eq!(saved.shadow_price_sol, Some(0.001));
    if bad_price {
        assert!(saved.slippage_bps.is_some_and(|bps| bps > 500.0));
        assert_eq!(saved.decision_status.as_deref(), Some("would_skip"));
    } else {
        assert_eq!(saved.slippage_bps, Some(0.0));
        assert_eq!(saved.decision_status.as_deref(), Some("would_execute"));
    }
    let metadata = crate::execution_build_plan_metadata::load_execution_build_plan_metadata(
        store, &signal.signal_id,
    )?;
    let mut fresh_sample = sample;
    let fresh_out = if bad_price { "9000" } else { "9900" };
    fresh_sample.out_amount = fresh_out.into();
    let mut fresh_body: Value = serde_json::from_str(&fresh_sample.response_json)?;
    fresh_body["outAmount"] = json!(fresh_out);
    fresh_body["otherAmountThreshold"] = json!(if bad_price { "8550" } else { "9405" });
    fresh_sample.response_json = fresh_body.to_string();
    fresh_sample.http_request_started_ts = Some(Utc::now());
    fresh_sample.quote_response_available_ts = Some(Utc::now());
    let refreshed = crate::execution_build_plan_refresh::
        refresh_tiny_buy_build_plan_metadata_with_mock_external_quote(config, metadata, fresh_sample);
    assert_eq!(refreshed.quote_event_id.as_deref(), Some(event_id.as_str()));
    if bad_price {
        assert_eq!(refreshed.decision_status.as_deref(), Some("would_skip"));
    } else {
        assert_eq!(refreshed.decision_status.as_deref(), Some("would_execute"));
        assert!(refreshed.slippage_bps.is_some_and(|bps| bps > 100.0 && bps < 500.0));
        assert_eq!(refreshed.decision_reason.as_deref(),
            Some(crate::execution_build_plan_refresh::FRESH_SUBMIT_QUOTE_WITHIN_SLIPPAGE));
    }
    Ok(refreshed)
}

pub(super) fn signed_payload() -> Result<(String, String, [u8; 32])> {
    signed_payload_for_lamports(10_000_000)
}

pub(super) fn signed_payload_for_lamports(lamports: u64) -> Result<(String, String, [u8; 32])> {
    signed_payload_for_lamports_and_floor(lamports, 50_000_001)
}

pub(super) fn signed_payload_for_lamports_and_floor(
    lamports: u64, reserve_lamports: u64,
) -> Result<(String, String, [u8; 32])> {
    let key = SigningKey::from_bytes(&[11; 32]);
    let payer = key.verifying_key().to_bytes();
    let mut instructions = crate::app_tests::priority_fee_fixture::budget(200_000, 10_000);
    instructions.push(crate::app_tests::native_funding_fixture::transfer(
        payer, [52; 32], lamports,
    ));
    let floor = crate::execution_native_floor::prepare_final_native_floor(
        payer,
        [9; 32],
        &instructions,
        reserve_lamports,
    )?;
    let mut wire = STANDARD.decode(floor.payload())?;
    let signature = key.sign(&wire[65..]);
    wire[1..65].copy_from_slice(&signature.to_bytes());
    Ok((
        STANDARD.encode(wire),
        bs58::encode(signature.to_bytes()).into_string(),
        payer,
    ))
}

pub(super) fn persist_unowned_sell(
    inbox: &mut AssociationInbox,
    native: &Value,
    now: DateTime<Utc>,
) -> Result<()> {
    let sell: AdmissionFacts =
        serde_json::from_value(native["anchors"][2]["identity"]["admission"].clone())?;
    inbox.persist_at(
        &delivery(3, DeliveryEvent::Admission(sell.clone())),
        &CandidateGeneration::Unknown,
        now,
    )?;
    inbox.persist_at(
        &delivery(
            4,
            DeliveryEvent::Terminal {
                signature: sell.facts.signature.clone(),
                expected: sell.clone(),
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: 150,
                    blockhash: native["anchors"][2]["terminal"]["ProviderAsserted"]["blockhash"]
                        .as_str()
                        .unwrap()
                        .into(),
                    signature: sell.facts.signature.clone(),
                    transaction_index: 0,
                    block_time: BlockTime::Missing,
                }),
            },
        ),
        &CandidateGeneration::Unknown,
        now,
    )?;
    for (i, path) in native["parent_paths"]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
        .enumerate()
    {
        let edge = &path["edges"][0];
        let parent = ParentObservation {
            child: serde_json::from_value(edge["child"].clone())?,
            parent: serde_json::from_value(edge["parent"].clone())?,
            issue: None,
        };
        inbox.persist_at(
            &delivery(5 + i as u64, DeliveryEvent::Parent(parent)),
            &CandidateGeneration::Unknown,
            now,
        )?;
    }
    for _ in 0..20 {
        if !inbox.has_sell_preparation_work()? {
            break;
        }
        inbox.recover_sell_preparation()?;
    }
    Ok(())
}
