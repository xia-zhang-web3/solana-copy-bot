//! LOCAL modeled native events and mocked confirmations. No signer or send path.
use super::{
    association_fixture as af, association_parent_fixture as parent, protected_usdc_fixture as old,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_storage_core::*;
use serde_json::{json, Value};
pub(super) const SOURCE: &str = "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi";
pub(super) const BUY_SIG: &str =
    "31R69oCVXJaEuUtWz7Cx4BChaiqeXPkfUGHJg6WKUUry3FTn1V7wkn2s11GA83AVTxksTENbh1A3Lh667zuRSpxj";
pub(super) const SELL_SIG: &str =
    "41d8jCJEnTMrriqGyfJvamJYsakyHb8VhtvTWeFyiDnx4NhAWjBR8fYo11PjgZFEhSdogM4Q31EZWY8dgVrdfjwb";
pub(super) struct Case {
    pub bot: old::Case,
    pub source: SqliteStore,
    pub source_path: std::path::PathBuf,
    pub root: super::temporary_output_fixture::OutputRoot,
    pub scope: controlled_receipt_lot::Scope,
    pub buy: OwnerTechnicalBuyIntent,
    pub source_config: Option<copybot_config::ExecutionConfig>,
}
impl Case {
    pub async fn new() -> Result<Self> {
        Self::build(false).await
    }
    pub async fn wsol() -> Result<Self> {
        Self::build(true).await
    }
    async fn build(wsol: bool) -> Result<Self> {
        let root = super::temporary_output_fixture::OutputRoot::new("receipt-lot-custody")?;
        let source_path = root.path().join("source.db");
        let mut source = SqliteStore::open(&source_path)?;
        source.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let scope = controlled_receipt_lot::Scope {
            policy: controlled_receipt_lot::POLICY.into(),
            source_run_id: "receipt-source-local".into(),
            bot_run_id: "protected-usdc".into(),
            source_wallet: SOURCE.into(),
            bot_wallet: "US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx".into(),
            source_buy_intent_id: "receipt-source-buy".into(),
            source_state_db_path: source_path.to_string_lossy().into_owned(),
        };
        source.register_controlled_receipt_lot_scope(&scope, SOURCE)?;
        let now = Utc::now();
        let bot_config = if wsol {
            Some(super::wsol_lifecycle_source_fixture::bot_config(&scope)?)
        } else {
            None
        };
        let source_config = bot_config
            .as_ref()
            .map(|bot| super::wsol_lifecycle_source_fixture::config(&scope, root.path(), bot))
            .transpose()?;
        let buy = if let Some(c) = &source_config {
            crate::execution_owner_buy_authority::configured(c)?.unwrap()
        } else {
            OwnerTechnicalBuyIntent {
                intent_id: scope.source_buy_intent_id.clone(),
                run_id: scope.source_run_id.clone(),
                wallet: SOURCE.into(),
                signer: SOURCE.into(),
                genesis_hash: super::owner_buy_fixture::GENESIS.into(),
                mint: old::USDC.into(),
                amount_lamports: 10_000_000,
                route: "jupiter_swap_instructions".into(),
                activated_at: now - Duration::seconds(1),
                expires_at: now + Duration::seconds(599),
                authority_sha256: "a".repeat(64),
                max_priority_fee_lamports: 22000,
                min_reserve_lamports: 0,
                max_slippage_bps: 50,
                max_daily_loss_lamports: 20_000_000,
                max_open_positions: 1,
                max_buy_count: 1,
            }
        };
        source.register_owner_technical_buy_intent(&buy)?;
        let order = source
            .reserve_owner_technical_buy_order(&buy.intent_id, || Ok(now))?
            .order;
        let (payload, receipt) = if let Some(c) = &source_config {
            super::wsol_lifecycle_source_fixture::buy(c, &source, &order).await?
        } else {
            super::wsol_lifecycle_fixture::external_mock(
                &order, SOURCE, "buy", 10000, BUY_SIG, 100,
            )?
        };
        let source_artifact = (payload.clone(), receipt.clone());
        super::wsol_lifecycle_fixture::submitted(
            &source,
            &source_path,
            &order,
            SOURCE,
            "buy",
            BUY_SIG,
            &payload,
        )?;
        let out = old::confirm(&source, &order.order_id, SOURCE, receipt).await?;
        assert_eq!(out.buy_opened, 1);
        if wsol {
            let facts = source
                .load_execution_canary_receipt_facts(&order.order_id)?
                .unwrap();
            let fill_id: i64 = rusqlite::Connection::open(&source_path)?.query_row(
                "SELECT id FROM fills WHERE order_id=?1",
                [&order.order_id],
                |r| r.get(0),
            )?;
            let anchor = copybot_storage_core::association_sell_preparation::ReceiptAnchor {
                contributor: ProvenBuyContributor {
                    fill_id,
                    order_id: order.order_id.clone(),
                    signal_id: order.signal_id.clone(),
                    source_wallet: SOURCE.into(),
                    tx_signature: BUY_SIG.into(),
                },
                slot: 100,
                wallet: SOURCE.into(),
                token: old::USDC.into(),
                raw: "10000".into(),
                decimals: 6,
                receipt_fingerprint: "LOCAL actual canonical facts".into(),
            };
            crate::execution_owned_sell_rpc::decode::buy(
                &source_artifact.1["result"],
                &anchor,
                &facts,
            )?;
        }
        let bot = if let Some(c) = bot_config {
            old::Case::actual_source(scope.clone(), c, source_artifact.0, source_artifact.1).await?
        } else {
            old::Case::controlled_before_sell(scope.clone()).await?
        };
        copybot_config::validate_controlled_receipt_lot(&bot.config)?;
        if let Some(c) = &source_config {
            bot.db.sql.execute(
                "VACUUM INTO ?1",
                [&c.owner_technical_buy
                    .as_ref()
                    .unwrap()
                    .source_copy_exit_gate
                    .as_ref()
                    .unwrap()
                    .bot_state_db_path],
            )?;
        }
        Ok(Self {
            bot,
            source,
            source_path,
            root,
            scope,
            buy,
            source_config,
        })
    }
    pub fn source_dispatch(&self) -> Result<String> {
        if self.source_config.is_some() {
            return super::wsol_lifecycle_source_fixture::sell(self);
        }
        self.source_dispatch_amount(10_000)
    }
    pub fn source_dispatch_amount(&self, amount: u64) -> Result<String> {
        let now = Utc::now();
        let bid = owner_technical_buy_order_id(&self.buy.intent_id);
        let pos = self
            .source
            .load_execution_canary_open_position(old::USDC)?
            .context("source own position")?;
        let i = OwnerExitIntent {
            intent_id: format!("execution-only:{}", self.buy.intent_id),
            run_id: self.buy.run_id.clone(),
            buy_order_id: bid,
            buy_receipt_signature: BUY_SIG.into(),
            position_id: pos.position_id,
            wallet: SOURCE.into(),
            signer: SOURCE.into(),
            genesis_hash: self.buy.genesis_hash.clone(),
            mint: old::USDC.into(),
            amount_raw: amount,
            decimals: 6,
            route: self.buy.route.clone(),
            activated_at: self.buy.activated_at,
            expires_at: self.buy.expires_at,
            authority_sha256: "c".repeat(64),
            max_priority_fee_lamports: 22000,
            min_reserve_lamports: 0,
            max_slippage_bps: 50,
            max_daily_loss_lamports: 20_000_000,
        };
        self.source.register_owner_exit_intent(&i)?;
        let order = self
            .source
            .reserve_owner_exit_order(&i.intent_id, || Ok(now))?
            .order;
        let (payload, _) = super::wsol_lifecycle_fixture::external_mock(
            &order, SOURCE, "sell", amount, SELL_SIG, 150,
        )?;
        super::wsol_lifecycle_fixture::submitted(
            &self.source,
            &self.source_path,
            &order,
            SOURCE,
            "sell",
            SELL_SIG,
            &payload,
        )?;
        Ok(order.order_id)
    }
    pub async fn source_confirm(&self, order: &str) -> Result<()> {
        if self.source_config.is_some() {
            let receipt: Value = serde_json::from_slice(&std::fs::read(
                self.root.path().join("actual-source-sell-receipt.json"),
            )?)?;
            let out = old::confirm(&self.source, order, SOURCE, receipt).await?;
            assert_eq!(out.sell_closed, 1);
            if let Ok(root) = std::env::var("WSOL_LIFECYCLE_FIXTURE_DIR") {
                rusqlite::Connection::open(&self.source_path)?.execute(
                    "VACUUM INTO ?1",
                    [std::path::Path::new(&root)
                        .join("source-confirmed.db")
                        .to_string_lossy()
                        .as_ref()],
                )?;
            }
            return Ok(());
        }
        let i = self
            .source
            .load_owner_exit_intent(&format!("execution-only:{}", self.buy.intent_id))?
            .unwrap();
        let order_record = self.source.load_execution_canary_order(order)?.unwrap();
        let (_, receipt) = super::wsol_lifecycle_fixture::external_mock(
            &order_record,
            SOURCE,
            "sell",
            i.amount_raw,
            SELL_SIG,
            150,
        )?;
        let out = old::confirm(&self.source, order, SOURCE, receipt).await?;
        assert_eq!(out.sell_closed, 1);
        Ok(())
    }
    pub async fn native_sell(&self) -> Result<()> {
        let mut meta: Value = serde_json::from_slice(&std::fs::read(
            super::b135_fixture::inputs().join("chain.json"),
        )?)?;
        meta["our"]["token_out"] = json!(old::USDC);
        if self.source_config.is_some() {
            super::wsol_lifecycle_events::protocols(&mut meta);
        }
        let mut cfg = af::config(&meta);
        cfg.execution = self.bot.config.clone();
        let (mut consumer, sender) =
            af::start(&self.bot.db, &cfg, "receipt-lot-native-sell").await?;
        let actual = if self.source_config.is_some() {
            Some((
                std::fs::read_to_string(self.root.path().join("actual-source-sell-wire"))?,
                serde_json::from_slice::<Value>(&std::fs::read(
                    self.root.path().join("actual-source-sell-receipt.json"),
                )?)?,
            ))
        } else {
            None
        };
        let producer = tokio::spawn(async move {
            for (n, name) in ["sell", "block-150"].iter().enumerate() {
                let mut payload =
                    old::modeled_frame(name, "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8")?;
                if let Some((wire, receipt)) = &actual {
                    payload = super::wsol_lifecycle_events::replace(payload, wire, receipt)?;
                }
                if *name == "sell" {
                    sender
                        .send(copybot_ingestion::ReplayInput::Update {
                            offset_ns: 0,
                            payload: foreign_frame_with(payload.clone(), 209)?,
                        })
                        .await?;
                }
                sender
                    .send(copybot_ingestion::ReplayInput::Update {
                        offset_ns: n as u64 + 1,
                        payload,
                    })
                    .await?;
            }
            sender.send(copybot_ingestion::ReplayInput::End(9)).await?;
            Ok::<_, anyhow::Error>(())
        });
        af::drain(&mut consumer, &self.bot.db).await?;
        producer.await??;
        Ok(())
    }
    pub fn accounts(&self, held: u64) -> Result<Value> {
        let ata = crate::execution_controlled_receipt_lot::canonical(&self.bot.config)?;
        Ok(
            json!({"context":{"slot":151},"value":[{"pubkey":ata,"account":{"owner":ordered_sell_quote::fractional::inventory::TOKEN,"data":{"parsed":{"type":"account","info":{"owner":self.scope.bot_wallet,"mint":old::USDC,"tokenAmount":{"amount":held.to_string(),"decimals":6}}}}}}, {"pubkey":"foreign-second-usdc-account","account":{"arbitrary":"other inventory is not test custody"}}]}),
        )
    }
    pub async fn bind(
        &mut self,
        claim: ordered_sell_quote::QuoteClaim,
        held: u64,
    ) -> Result<ordered_sell_quote::QuoteClaim> {
        let accounts = self.accounts(held)?;
        let genesis = self
            .bot
            .config
            .owned_sell_preparation
            .as_ref()
            .unwrap()
            .genesis_hash
            .clone();
        crate::execution_owned_sell_rpc::fractional::bind(&mut self.bot.db.store,&self.bot.config,claim,parent::limits(),move|r|std::future::ready(Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":match r["method"].as_str().unwrap(){"getGenesisHash"=>json!(genesis),"getTokenAccountsByOwner"=>accounts.clone(),other=>panic!("unexpected LOCAL mock request {other}")}})))).await
    }
    pub fn export_closed(&self, order: &str) -> Result<()> {
        let Ok(dir) = std::env::var("RECEIPT_LOT_CUSTODY_FIXTURE_DIR") else {
            return Ok(());
        };
        let p = std::path::Path::new(&dir);
        self.bot.db.sql.execute(
            "VACUUM INTO ?1",
            [p.join("bot-closed.db").to_string_lossy().as_ref()],
        )?;
        let facts = self
            .bot
            .db
            .store
            .load_execution_canary_receipt_facts(order)?
            .unwrap();
        let cash = self
            .bot
            .db
            .store
            .load_execution_canary_cash_settlement(order)?
            .unwrap();
        std::fs::write(
            p.join("bot-closed-receipt.json"),
            serde_json::to_vec_pretty(
                &json!({"order_id":facts.order_id,"signature":facts.tx_signature,"wallet":facts.wallet_pubkey,"slot":facts.slot,"token":facts.token,"side":facts.side,"token_delta_raw":facts.token_delta.map(|d|d.raw.to_string()),"fee_lamports":facts.transaction_fee.map(|f|f.as_u64())}),
            )?,
        )?;
        std::fs::write(
            p.join("bot-closed-cash.json"),
            serde_json::to_vec_pretty(
                &json!({"sold_raw":cash.sold_quantity.raw(),"remaining_raw":cash.remaining_quantity.raw(),"remaining_entry_basis":cash.remaining_entry_basis.as_u64()}),
            )?,
        )?;
        Ok(())
    }
    pub fn export(&self, claim: &ordered_sell_quote::QuoteClaim) -> Result<()> {
        let Ok(dir) = std::env::var("RECEIPT_LOT_CUSTODY_FIXTURE_DIR") else {
            return Ok(());
        };
        let p = std::path::Path::new(&dir);
        std::fs::create_dir_all(p)?;
        self.bot.db.sql.execute(
            "VACUUM INTO ?1",
            [p.join("bot-prequote.db").to_string_lossy().as_ref()],
        )?;
        rusqlite::Connection::open(&self.source_path)?.execute(
            "VACUUM INTO ?1",
            [p.join("source-confirmed.db").to_string_lossy().as_ref()],
        )?;
        std::fs::write(
            p.join("decision.json"),
            serde_json::to_vec_pretty(claim.binding.fractional.as_ref().unwrap())?,
        )?;
        std::fs::write(
            p.join("scope.json"),
            serde_json::to_vec_pretty(&self.scope)?,
        )?;
        let mut events = serde_json::Map::new();
        for signature in [BUY_SIG, SELL_SIG] {
            let admission: String = self.bot.db.sql.query_row(
                "SELECT admission FROM association_inbox_identities WHERE signature=?1",
                [signature],
                |r| r.get(0),
            )?;
            events.insert(signature.into(), serde_json::from_str(&admission)?);
        }
        std::fs::write(
            p.join("native-events.json"),
            serde_json::to_vec_pretty(&events)?,
        )?;
        std::fs::write(
            p.join("fixture_manifest.json"),
            serde_json::to_vec_pretty(
                &json!({"kind":"LOCAL_SYNTHETIC_NATIVE_AND_MOCK_CONFIRMATION","base":"14ff294c883a685068547c1d51fbe4ec94012d47","input":"accepted b135 protobuf operands modeled to USDC six decimals; extra foreign event signatures modeled","source_buy_signature":BUY_SIG,"source_sell_signature":SELL_SIG,"source_acquired_raw":10000,"bot_acquired_raw":old::H,"initial_bot_nonlot_raw":100,"quote":"MOCK; no live availability proof","signatures_created":0,"external_requests":0,"source_checkpoint":"source-confirmed.db already closed","bot_prequote_checkpoint":"bot-prequote.db own lot open","bot_closed_checkpoint":"bot-closed.db after same test canonical SELL accounting"}),
            )?,
        )?;
        Ok(())
    }
}
pub(super) fn submitted(
    store: &SqliteStore,
    path: &std::path::Path,
    o: &ExecutionCanaryOrder,
    wallet: &str,
    side: &str,
    sig: &str,
) -> Result<()> {
    let now = Utc::now();
    store.mark_execution_canary_built(&o.order_id, now)?;
    store.mark_execution_canary_simulated(
        &o.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(&o.order_id, now, sig)?;
    rusqlite::Connection::open(path)?.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,message_sha256,transaction_sha256,claimed_at,transport_note) VALUES(?1,?2,?3,?4,1,?5,?6,?7,?8,?9,?10,?11,'LOCAL mocked confirmation; no send')",rusqlite::params![o.order_id,o.signal_id,o.client_order_id,o.route,wallet,old::USDC,side,sig,"a".repeat(64),"b".repeat(64),now.to_rfc3339()])?;
    Ok(())
}

pub(super) fn foreign_frame(payload: Vec<u8>) -> Result<Vec<u8>> {
    foreign_frame_with(payload, 208)
}
fn foreign_frame_with(payload: Vec<u8>, byte: u8) -> Result<Vec<u8>> {
    use prost::Message;
    use yellowstone_grpc_proto::prelude::*;
    let mut u = SubscribeUpdate::decode(payload.as_slice())?;
    if let Some(subscribe_update::UpdateOneof::Transaction(t)) = &mut u.update_oneof {
        let i = t.transaction.as_mut().unwrap();
        i.signature = vec![byte; 64];
        i.transaction.as_mut().unwrap().signatures = vec![vec![byte; 64]];
    }
    Ok(u.encode_to_vec())
}
