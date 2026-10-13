//! Explicit modeled USDC source frames / external receipts; actual SQLite accounting.
use super::{association_fixture as af, association_parent_fixture as parent};
use anyhow::{Context, Result};
use chrono::Utc;
use copybot_config::{ExecutionConfig, ProtectedBotUsdcBaselineConfig};
use copybot_storage_core::{
    ordered_sell_quote::{fractional::inventory::Evidence, QuoteClaim},
    *,
};
use prost::Message;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use yellowstone_grpc_proto::prelude::*;
pub(super) const USDC: &str = copybot_storage_core::OWNER_EXIT_MINT;
pub(super) const H: u64 = 7000;
pub(super) struct Case {
    pub db: af::Db,
    pub config: ExecutionConfig,
    pub evidence: Evidence,
}
fn replace(bytes: &mut Vec<u8>, old: &[u8], new: &[u8]) {
    if bytes.len() < old.len() {
        return;
    }
    let at: Vec<_> = bytes
        .windows(old.len())
        .enumerate()
        .filter_map(|(i, b)| (b == old).then_some(i))
        .collect();
    for i in at {
        bytes[i..i + old.len()].copy_from_slice(new);
    }
}
fn modeled(info: &mut SubscribeUpdateTransactionInfo, old: &str) {
    let old = bs58::decode(old).into_vec().unwrap();
    let mint = bs58::decode(USDC).into_vec().unwrap();
    let m = info.transaction.as_mut().unwrap().message.as_mut().unwrap();
    for k in &mut m.account_keys {
        replace(k, &old, &mint);
    }
    for i in &mut m.instructions {
        replace(&mut i.data, &old, &mint);
    }
    let meta = info.meta.as_mut().unwrap();
    for g in &mut meta.inner_instructions {
        for i in &mut g.instructions {
            replace(&mut i.data, &old, &mint);
        }
    }
    for b in meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances)
    {
        if b.mint == bs58::encode(&old).into_string() {
            b.mint = USDC.into();
            let q = b.ui_token_amount.as_mut().unwrap();
            q.decimals = 6;
            q.ui_amount = q.amount.parse::<f64>().unwrap() / 1e6;
            q.ui_amount_string = q.ui_amount.to_string();
        }
    }
    if m.account_keys[0]
        == bs58::decode("US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx")
            .into_vec()
            .unwrap()
    {
        for (rows, n) in [
            (&mut meta.pre_token_balances, 100u64),
            (&mut meta.post_token_balances, 100 + H),
        ] {
            for b in rows {
                if b.owner == "US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx" && b.mint == USDC {
                    let q = b.ui_token_amount.as_mut().unwrap();
                    q.amount = n.to_string();
                    q.ui_amount = n as f64 / 1e6;
                    q.ui_amount_string = q.ui_amount.to_string();
                }
            }
        }
    }
}
fn modeled_frame(name: &str, old: &str) -> Result<Vec<u8>> {
    let raw = std::fs::read(super::b135_fixture::inputs().join(format!("{name}.pb")))?;
    let mut u = SubscribeUpdate::decode(raw.as_slice())?;
    match u.update_oneof.as_mut().unwrap() {
        subscribe_update::UpdateOneof::Transaction(t) => {
            modeled(t.transaction.as_mut().unwrap(), old)
        }
        subscribe_update::UpdateOneof::Block(b) => {
            for t in &mut b.transactions {
                modeled(t, old);
            }
        }
        _ => {}
    }
    Ok(u.encode_to_vec())
}
fn rewrite_json(v: &mut Value, old: &str) {
    match v {
        Value::String(s) if s == old => *s = USDC.into(),
        Value::Array(a) => {
            for x in a {
                rewrite_json(x, old);
            }
        }
        Value::Object(o) => {
            for x in o.values_mut() {
                rewrite_json(x, old);
            }
            if let Some(Value::Number(n)) = o.get_mut("decimals") {
                if n.as_u64() == Some(3) {
                    *n = 6.into();
                }
            }
        }
        _ => {}
    }
}
pub(super) fn inventory(c: &ExecutionConfig, held: u64) -> Value {
    let ata = c
        .owned_sell_preparation
        .as_ref()
        .unwrap()
        .protected_bot_usdc_baseline
        .as_ref()
        .unwrap()
        .token_account
        .clone();
    json!({"context":{"slot":151},"value":[{"pubkey":ata,"account":{"owner":copybot_storage_core::ordered_sell_quote::fractional::inventory::TOKEN,"data":{"parsed":{"type":"account","info":{"owner":c.canary_wallet_pubkey,"mint":USDC,"tokenAmount":{"amount":held.to_string(),"decimals":6}}}}}}]})
}
impl Case {
    pub async fn new() -> Result<Self> {
        let output = super::temporary_output_fixture::OutputRoot::new("protected-usdc")?;
        // URI remains alive through both store/sql connections; no production DB or key.
        let path = format!(
            "file:protected-usdc-{}?mode=memory&cache=shared",
            output.path().display()
        );
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let db = af::Db {
            path: path.clone().into(),
            sql: rusqlite::Connection::open(&path)?,
            store,
        };
        let mut meta: Value = serde_json::from_slice(&std::fs::read(
            super::b135_fixture::inputs().join("chain.json"),
        )?)?;
        let old = meta["our"]["token_out"].as_str().unwrap().to_owned();
        let wallet = meta["our"]["signer"].as_str().unwrap().to_owned();
        let mut c = super::protected_usdc_prepare::config(&wallet)?;
        let payer = crate::execution_pumpswap_accounts::parse_pubkey(&wallet, "mock-wallet")?;
        let mint = crate::execution_pumpswap_accounts::parse_pubkey(USDC, "mock-mint")?;
        let ata = crate::execution_pumpswap_accounts::associated_token_address(
            &payer,
            &mint,
            &crate::execution_pumpswap_accounts::token_program_id(),
        );
        c.owned_sell_preparation
            .as_mut()
            .unwrap()
            .protected_bot_usdc_baseline = Some(ProtectedBotUsdcBaselineConfig {
            policy: copybot_config::PROTECTED_BOT_USDC_100_V1.into(),
            amount_raw: 100,
            token_account: bs58::encode(ata).into_string(),
        });
        // Mock only admission source transaction operands; native association/parser executes.
        rewrite_json(&mut meta, &old);
        let mut ac = af::config(&meta);
        ac.execution = c.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let authority = crate::execution_technical_cohort::authority(&c)?.unwrap();
        let scope = crate::execution_technical_cohort::admission_wallets(&authority, &c)?;
        let mut ingestion = copybot_ingestion::IngestionService::with_replay_scoped(
            &ac,
            rx,
            "protected-usdc-source".into(),
            Some(scope),
        )?;
        let mut consumer = crate::association_consumer::AssociationConsumer::start_with_execution(
            &mut ingestion,
            &ac.ingestion,
            &c,
            &db.path.to_string_lossy(),
        )
        .await?
        .unwrap();
        let names: Vec<_> = parent::frames(&meta)
            .into_iter()
            .filter(|n| n != "sell" && n != "block-150")
            .collect();
        let producer = tokio::spawn(async move {
            for (n, name) in names.iter().enumerate() {
                tx.send(copybot_ingestion::ReplayInput::Update {
                    offset_ns: n as u64 + 1,
                    payload: modeled_frame(name, &old)?,
                })
                .await?;
            }
            tx.send(copybot_ingestion::ReplayInput::End(9)).await?;
            Ok::<_, anyhow::Error>(())
        });
        let mut rpc = crate::execution_owned_sell_rpc::fractional::transport::Parsed(|r: Value| {
            std::future::ready(Ok(
                json!({"jsonrpc":"2.0","id":r["id"],"result":match r["method"].as_str().unwrap(){"getGenesisHash"=>json!("11111111111111111111111111111111"),"getSlot"=>json!(99),_=>panic!("unexpected fence RPC")}}),
            ))
        });
        loop {
            match consumer
                .poll_with_transport(&db.store, Some(&mut rpc))
                .await
            {
                Ok(()) => {}
                Err(e) if e.to_string() == "association delivery stopped" => break,
                Err(e) => return Err(e),
            }
        }
        producer.await??;
        let source = meta["source"]["signature"].as_str().unwrap();
        let now = Utc::now();
        assert!(db.store.native_buy_record_finalized(
            source,
            100,
            copybot_storage_core::native_buy::SPL_TOKEN_PROGRAM,
            now
        )?);
        let candidate = db
            .store
            .native_buy_ready(source, now, 120)?
            .context("actual native BUY candidate")?;
        let signal = candidate.signal_id;
        let order = db
            .store
            .reserve_execution_canary_order(&signal, "jupiter_swap_instructions", now)?
            .order;
        let binding = copybot_storage_core::native_buy::NativeBuyActivationBinding {
            signal_id: signal,
            decision_id: candidate.decision_id,
            policy_identity: crate::execution_native_buy_rpc::policy_identity(&c)?,
            max_age_seconds: 120,
            order_id: order.order_id.clone(),
            client_order_id: order.client_order_id.clone(),
            attempt: order.attempt,
            route: order.route.clone(),
        };
        db.store.prepare_tiny_native_policy_for_native_buy(
            "protected-usdc",
            &wallet,
            1000000000,
            160200031,
            120,
            now,
            &binding,
            || Ok(now),
        )?;
        let buy = order.order_id;
        db.store.mark_execution_canary_built(&buy, now)?;
        db.store.mark_execution_canary_simulated(
            &buy,
            now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        // External submit/confirmation are explicit mocks. Production receipt parser and fill writer below.
        let sig = meta["our"]["signature"].as_str().unwrap();
        db.store.mark_execution_canary_submitted(&buy, now, sig)?;
        db.sql.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,message_sha256,transaction_sha256,claimed_at,transport_note) SELECT order_id,signal_id,client_order_id,route,attempt,?1,?2,'buy',tx_signature,?3,?4,submit_ts,'unknown' FROM orders WHERE order_id=?5",rusqlite::params![wallet,USDC,"a".repeat(64),"b".repeat(64),buy])?;
        db.sql.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) SELECT order_id,'protected-usdc',tx_signature,?1,'buy',?2,?3,10000000,100000,10000,'120',submit_ts FROM orders WHERE order_id=?4",rusqlite::params![wallet,"a".repeat(64),"b".repeat(64),buy])?;
        let receipt = receipt(&wallet, sig, "buy", 100, 100 + H, 120);
        let result = confirm(&db.store, &buy, &wallet, receipt).await?;
        assert_eq!(result.buy_opened, 1);
        let facts = db
            .store
            .load_execution_canary_receipt_facts(&buy)?
            .context("BUY facts")?;
        assert_eq!(facts.token_delta.unwrap().raw, i128::from(H));
        assert_eq!(
            db.store
                .load_execution_canary_open_position(USDC)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            H
        );
        let pos = db.store.load_execution_canary_open_position(USDC)?.unwrap();
        db.sql.execute(
            "UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3",
            rusqlite::params![buy, USDC, pos.position_id],
        )?;
        // SOURCE SELL is delivered only after the real BUY receipt has opened H.
        let (mut sell_consumer, sell_tx) =
            af::start(&db, &ac, "protected-usdc-source-sell").await?;
        let old = "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8";
        let producer = tokio::spawn(async move {
            for (n, name) in ["sell", "block-150"].iter().enumerate() {
                sell_tx
                    .send(copybot_ingestion::ReplayInput::Update {
                        offset_ns: n as u64 + 1,
                        payload: modeled_frame(name, old)?,
                    })
                    .await?;
            }
            sell_tx.send(copybot_ingestion::ReplayInput::End(9)).await?;
            Ok::<_, anyhow::Error>(())
        });
        af::drain(&mut sell_consumer, &db).await?;
        producer.await??;
        let mut e = super::fractional::fractional_synthetic_fixture::evidence(false)?;
        rewrite_json(
            &mut e.block,
            &meta["our"]["token_out"].as_str().unwrap().to_owned(),
        );
        // Rebuild Evidence from original generic mock, preserving N/D proof algorithm.
        let old = "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8";
        rewrite_json(&mut e.block, old);
        for page in &mut e.pages {
            rewrite_json(&mut page.response, old);
        }
        e.pages[1].response["value"][1]["account"]["data"]["parsed"]["info"]["tokenAmount"]
            ["amount"] = json!("0");
        e.execution_accounts = inventory(&c, 100 + H);
        Ok(Self {
            db,
            config: c,
            evidence: e,
        })
    }
    pub fn claim(&self) -> Result<QuoteClaim> {
        let step = self
            .db
            .store
            .claim_strict_sell_quote_for_owned_preparation(
                parent::limits(),
                "http://127.0.0.1:1/",
                Utc::now,
            )?;
        let copybot_storage_core::ordered_sell_quote::QuoteClaimStep::Claimed(c) = step else {
            anyhow::bail!("protected_claim: {step:?}");
        };
        Ok(c)
    }
    pub async fn bind(&mut self, claim: QuoteClaim) -> Result<QuoteClaim> {
        let e = self.evidence.clone();
        let genesis = self
            .config
            .owned_sell_preparation
            .as_ref()
            .unwrap()
            .genesis_hash
            .clone();
        crate::execution_owned_sell_rpc::fractional::bind(
            &mut self.db.store,
            &self.config,
            claim,
            parent::limits(),
            move |r| {
                let value = match r["method"].as_str().unwrap() {
                    "getGenesisHash" => json!(genesis),
                    "getBlock" if r["params"][0] == e.slot => e.block.clone(),
                    "getBlock" => e.parent.clone(),
                    "getTokenAccountsByOwnerAtSlot" => e
                        .pages
                        .iter()
                        .find(|p| r["params"][1]["programId"] == p.program)
                        .unwrap()
                        .response
                        .clone(),
                    "getTokenAccountsByOwner" => e.execution_accounts.clone(),
                    _ => panic!("unexpected mocked RPC"),
                };
                std::future::ready(Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":value})))
            },
        )
        .await
    }
}
pub(super) fn receipt(
    wallet: &str,
    sig: &str,
    side: &str,
    pre: u64,
    post: u64,
    slot: u64,
) -> Value {
    let (n0, n1) = if side == "buy" {
        (1000000000, 989981000)
    } else {
        (989981000, 1001976000)
    };
    json!({"jsonrpc":"2.0","result":{"slot":slot,"transaction":{"signatures":[sig],"message":{"accountKeys":[{"pubkey":wallet,"signer":true,"writable":true},{"pubkey":bs58::encode([42;32]).into_string(),"signer":false,"writable":true}]}},"meta":{"err":null,"fee":if side=="buy"{19000}else{5000},"preBalances":[n0,1488440],"postBalances":[n1,1488440],"preTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":USDC,"uiTokenAmount":{"amount":pre.to_string(),"decimals":6}}],"postTokenBalances":[{"accountIndex":1,"owner":wallet,"mint":USDC,"uiTokenAmount":{"amount":post.to_string(),"decimals":6}}]}}})
}
pub(super) async fn confirm(
    store: &SqliteStore,
    order: &str,
    wallet: &str,
    receipt: Value,
) -> Result<crate::execution_submit_adapter::ExecutionConfirmationBoundaryOutcome> {
    let payer = crate::execution_pumpswap_accounts::parse_pubkey(wallet, "mock-wallet")?;
    let mut instructions = super::priority_fee_fixture::budget(200000, 0);
    instructions.push(super::native_funding_fixture::transfer(
        payer, [52; 32], 10000000,
    ));
    let wire = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            payer,
            [9; 32],
            &instructions,
        )?,
    );
    let io = crate::execution_canary_route::NativeBuyMockIo {
        runner: None,
        initial_sol: crate::execution_native_rpc::synthetic_classic_funding(
            &wire, payer, 1000000000, 19000, 120,
        )?,
        fee_lamports: 19000,
        fee_slot: 120,
        expected_message_sha256: "mock-external-confirm-only".into(),
        submit_signature: None,
        confirmation: json!({"result":{"value":[{"err":null,"slot":receipt["result"]["slot"],"confirmationStatus":"confirmed"}]}}),
        receipt,
        counts: Arc::new(Mutex::new(Default::default())),
    };
    crate::execution_submit_adapter::test_record_execution_rpc_confirmation_boundary_inner(
        store,
        &reqwest::Client::new(),
        "http://127.0.0.1:1",
        order,
        wallet,
        Utc::now(),
        1000,
        Some(&io),
    )
    .await
}
