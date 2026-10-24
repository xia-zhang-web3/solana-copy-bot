//! One LOCAL controlled USDC scope. Configuration keys and chain facts are synthetic.
use super::owner_source_gate_fixture::GateCase;
use super::{
    controlled_copy_pipeline_fixture as frames,
    copy_cycle_peer_fixture::{History, Peer},
};
use crate::association_consumer::AssociationConsumer;
use anyhow::{ensure, Context, Result};
use copybot_config::{AppConfig, ControlledReceiptLotConfig};
use copybot_ingestion::{IngestionService, ReplayInput};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

pub(super) const BOT_RAW: u64 = frames::RECEIVED - 17;
#[derive(Default)]
pub(super) struct CycleOptions {
    pub phase: Option<&'static str>,
    pub source_window: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    pub prior_exit: Option<copybot_config::PriorBotReceiptExitConfig>,
    pub source_intent: Option<String>,
}
pub(super) struct Cycle {
    pub case: GateCase,
    pub app: AppConfig,
    pub source: Peer,
    pub bot: Peer,
    pub consumer: AssociationConsumer,
    sender: tokio::sync::mpsc::Sender<ReplayInput>,
    sequence: u64,
}
fn endpoint(c: &mut copybot_config::ExecutionConfig, url: &str) {
    c.submit_adapter_http_url = url.into();
    c.quote_canary_base_url = format!("{url}/swap/v1");
    c.priority_fee_canary_rpc_url = url.into();
    c.priority_fee_canary_enabled = true;
    c.quote_canary_api_key = "LOCAL-model-only".into();
    c.quote_canary_timeout_ms = 2000;
    c.submit_timeout_ms = 100;
    c.max_confirm_seconds = 1;
    c.max_submit_attempts = 1;
    c.pretrade_min_sol_reserve = 0.0;
    if let Some(p) = &mut c.owned_sell_preparation {
        p.rpc_url = url.into();
    }
}
impl Cycle {
    pub fn new() -> impl std::future::Future<Output = Result<Self>> {
        Self::with_options(CycleOptions::default())
    }
    pub fn with_options(
        options: CycleOptions,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self>>>> {
        Box::pin(async move {
            let mut case = Box::pin(GateCase::new()).await?;
            let config = &mut case.source.config;
            let owner = config.owner_technical_buy.as_mut().unwrap();
            if let Some(phase) = options.phase {
                owner.run_id = format!("LOCAL-{phase}-source-run");
                owner.intent_id = format!("LOCAL-{phase}-source-buy");
                owner.source_copy_exit_gate.as_mut().unwrap().bot_run_id =
                    format!("LOCAL-{phase}-bot-run");
                config.tiny_experiment.id = Some(owner.run_id.clone());
                config.owned_sell_preparation.as_mut().unwrap().identity = owner.run_id.clone();
            }
            if let Some((activated, expires)) = options.source_window {
                owner.activated_at = activated.to_rfc3339();
                owner.expires_at = expires.to_rfc3339();
            }
            if let Some(intent) = options.source_intent {
                owner.intent_id = intent;
            }
            config.prior_bot_receipt_exit = options.prior_exit;
            let mut app = frames::config(&case)?;
            let history: History = Arc::new(Mutex::new(BTreeMap::new()));
            let source_wallet = crate::execution_pumpswap_accounts::parse_pubkey(
                &case.source.config.canary_wallet_pubkey,
                "LOCAL source",
            )?;
            let bot_key = ed25519_dalek::SigningKey::from_bytes(&[19; 32]);
            let source = Peer::new(source_wallet, frames::RECEIVED, 120, history.clone()).await?;
            let bot = Peer::new(bot_key.verifying_key().to_bytes(), BOT_RAW, 121, history).await?;
            let bot_key_path = case.source.root.path().join("LOCAL-bot-key.json");
            std::fs::write(
                &bot_key_path,
                serde_json::to_vec(&bot_key.to_keypair_bytes().as_slice())?,
            )?;
            endpoint(&mut app.execution, &bot.url);
            app.execution.execution_signer_keypair_path = bot_key_path.to_string_lossy().into();
            app.execution.tiny_experiment.policy_mode =
                copybot_config::TinyPolicyMode::ProtectedNativeCapital;
            let p = case.source.config.owner_technical_buy.as_ref().unwrap();
            let controlled = ControlledReceiptLotConfig {
                policy: copybot_config::CONTROLLED_RECEIPT_LOT_V1.into(),
                source_run_id: p.run_id.clone(),
                bot_run_id: app.execution.tiny_experiment.id.clone().unwrap(),
                source_wallet: case.source.config.canary_wallet_pubkey.clone(),
                bot_wallet: app.execution.canary_wallet_pubkey.clone(),
                source_buy_intent_id: p.intent_id.clone(),
                source_state_db_path: case
                    .source
                    .root
                    .path()
                    .join("state.db")
                    .to_string_lossy()
                    .into(),
                wsol_lifecycle: copybot_config::TRANSACTION_OWNED_WSOL_V1.into(),
            };
            app.execution.controlled_receipt_lot = Some(controlled.clone());
            let sell = app.execution.owned_sell_preparation.as_mut().unwrap();
            sell.fractional_inventory = Some(copybot_config::CONTROLLED_RECEIPT_LOT_V1.into());
            sell.protected_bot_usdc_baseline = None;
            endpoint(&mut case.source.config, &source.url);
            case.source.config.controlled_receipt_lot = Some(controlled);
            case.source.config.owned_sell_preparation = None;
            let p = case.source.config.owner_technical_buy.as_mut().unwrap();
            p.min_reserve_lamports = 0;
            p.source_copy_exit_gate
                .as_mut()
                .unwrap()
                .bot_policy_identity =
                crate::execution_native_buy_rpc::policy_identity(&app.execution)?;
            copybot_config::validate_controlled_receipt_lot(&app.execution)?;
            copybot_config::validate_controlled_receipt_lot(&case.source.config)?;
            copybot_config::validate_association_delivery(&app)?;
            crate::execution_controlled_receipt_lot::register(
                &case.source.config,
                &case.source.store,
            )?;
            crate::execution_controlled_receipt_lot::register(&app.execution, &case.bot)?;
            // Scope follows actual daemon wallets. No fixture constructor fabricates a BOT trade.
            let authority = crate::execution_technical_cohort::authority(&app.execution)?.unwrap();
            let scope =
                crate::execution_technical_cohort::admission_wallets(&authority, &app.execution)?;
            let (sender, receiver) = tokio::sync::mpsc::channel(8);
            let mut ingestion = IngestionService::with_replay_scoped(
                &app,
                receiver,
                "LOCAL-controlled-copy-cycle".into(),
                Some(scope),
            )?;
            let consumer = AssociationConsumer::start_with_execution(
                &mut ingestion,
                &app.ingestion,
                &app.execution,
                &case.path.to_string_lossy(),
            )
            .await?
            .context("actual controlled copy consumer")?;
            let mut cycle = Self {
                case,
                app,
                source,
                bot,
                consumer,
                sender,
                sequence: 0,
            };
            Box::pin(cycle.poll()).await?;
            cycle.consumer.next_fence_at = Some(tokio::time::Instant::now());
            Box::pin(cycle.poll()).await?;
            Ok(cycle)
        })
    }
    pub async fn poll(&mut self) -> Result<()> {
        let mut rpc =
            crate::execution_owned_sell_rpc::fractional::transport::Parsed(|r: Value| async move {
                let result = match r["method"].as_str() {
                    Some("getGenesisHash") => json!(super::owner_buy_fixture::GENESIS),
                    Some("getSlot") => json!(119),
                    other => anyhow::bail!("LOCAL fence method {other:?}"),
                };
                Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
            });
        // Cancel only a pending poll; the real consumer keeps its durable writer.
        if let Ok(result) = tokio::time::timeout(
            Duration::from_millis(50),
            self.consumer
                .poll_with_transport(&self.case.bot, Some(&mut rpc)),
        )
        .await
        {
            result?;
        }
        Ok(())
    }
    pub async fn event(
        &mut self,
        peer_is_source: bool,
        side: usize,
        duplicate: bool,
    ) -> Result<String> {
        let (wire, receipt) = if peer_is_source {
            self.source.artifact(side)?
        } else {
            self.bot.artifact(side)?
        };
        let slot = receipt["result"]["slot"].as_u64().unwrap();
        let signature = receipt["result"]["transaction"]["signatures"][0]
            .as_str()
            .unwrap()
            .to_owned();
        let info = super::wsol_lifecycle_events::info(&wire, &receipt, 0)?;
        let mut payloads = frames::frames(SubscribeUpdateTransaction {
            slot,
            transaction: Some(info),
            ..Default::default()
        });
        if duplicate {
            payloads.insert(1, payloads[0].clone());
        }
        let input_dir = std::env::var("COPYBOT_COPY_CYCLE_EVIDENCE_DIR")
            .ok()
            .map(|root| {
                std::path::PathBuf::from(root).join(format!(
                    "ingress-{}-{side}",
                    if peer_is_source { "source" } else { "bot" }
                ))
            });
        if let Some(dir) = &input_dir {
            std::fs::create_dir(dir)?;
            std::fs::write(dir.join("actual.signed.base64"), &wire)?;
            std::fs::write(
                dir.join("actual.receipt.json"),
                serde_json::to_vec_pretty(&receipt)?,
            )?;
            for (n, payload) in payloads.iter().enumerate() {
                std::fs::write(dir.join(format!("frame-{n}.pb")), payload)?;
            }
        }
        for payload in payloads {
            self.sequence += 1;
            self.sender
                .send(ReplayInput::Update {
                    offset_ns: self.sequence,
                    payload,
                })
                .await?;
        }
        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.poll().await?;
                use rusqlite::OptionalExtension;
                let terminal: Option<String> = self.case.sql()?.query_row(
                    "SELECT terminal FROM association_inbox_identities WHERE signature=?1",
                    [&signature], |r|r.get(0),
                ).optional()?.flatten();
                let asserted = terminal.map(|raw|serde_json::from_str::<copybot_core_types::association_delivery::Terminal>(&raw)).transpose()?;
                let finalized_delivery = matches!(asserted,
                    Some(copybot_core_types::association_delivery::Terminal::ProviderAsserted(ref p))
                    if p.signature == signature && p.slot == slot);
                if frames::admission(&self.case, &signature)?.is_some()
                    && finalized_delivery
                    && self
                        .consumer
                        .diagnostic_snapshots()
                        .0
                        .last_durably_stored_parent_slot
                        >= slot
                {
                    break Ok::<_, anyhow::Error>(());
                }
            }
        })
        .await;
        if completed.is_err() {
            let (wire, persisted) = self.consumer.diagnostic_snapshots();
            let admission = frames::admission(&self.case, &signature)?.is_some();
            let db = self.case.sql()?;
            let parents: i64 =
                db.query_row("SELECT count(*) FROM association_parent_blocks", [], |r| {
                    r.get(0)
                })?;
            let matching_parents: i64 = db.query_row("SELECT count(*) FROM association_parent_blocks WHERE json_extract(first_observation,'$.child.slot')=?1", [slot], |r|r.get(0))?;
            let facts = json!({"signature":signature,"slot":slot,"admission":admission,"parents":parents,
                "matching_parents":matching_parents,"ingress":format!("{wire:?}"),"persisted":format!("{persisted:?}")});
            if let Some(dir) = &input_dir {
                std::fs::write(dir.join("FAILURE.json"), serde_json::to_vec_pretty(&facts)?)?;
                db.execute(
                    "VACUUM main INTO ?1",
                    [dir.join("bot.sqlite").to_string_lossy().as_ref()],
                )?;
                self.case.source.sql()?.execute(
                    "VACUUM main INTO ?1",
                    [dir.join("source.sqlite").to_string_lossy().as_ref()],
                )?;
            }
            anyhow::bail!("LOCAL canonical event ingress timeout: {facts}");
        }
        completed.unwrap()?;
        if let Some(dir) = &input_dir {
            // Block Parent is emitted before the transaction Terminal. Retain
            // the fully consumed boundary, never infer readiness from Parent alone.
            let db = self.case.sql()?;
            db.execute(
                "VACUUM main INTO ?1",
                [dir.join("bot.sqlite").to_string_lossy().as_ref()],
            )?;
            self.case.source.sql()?.execute(
                "VACUUM main INTO ?1",
                [dir.join("source.sqlite").to_string_lossy().as_ref()],
            )?;
            let (terminal, conflict, recovery): (Option<String>,bool,bool) = db.query_row(
                "SELECT terminal,conflict,recovery FROM association_inbox_identities WHERE signature=?1",
                [&signature], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            let decisions: i64 =
                db.query_row("SELECT count(*) FROM native_buy_decisions", [], |r| {
                    r.get(0)
                })?;
            std::fs::write(
                dir.join("NATIVE_STATUS.json"),
                serde_json::to_vec_pretty(&json!({
                    "signature":signature,"slot":slot,"terminal":terminal,"conflict":conflict,"recovery":recovery,
                    "decisions":decisions,"pending":self.case.bot.list_native_buy_pending(8)?.len()
                }))?,
            )?;
        }
        Ok(signature)
    }
    pub fn runner(&self) -> Result<crate::execution_canary::ExecutionCanaryRunner> {
        crate::execution_canary::ExecutionCanaryRunner::new(self.app.execution.clone())
            .for_ingestion(&self.app.ingestion, &self.case.path.to_string_lossy())
    }
    pub fn check_peers(&self) -> Result<()> {
        self.source.healthy()?;
        self.bot.healthy()
    }
    pub fn count(&self, table: &str) -> Result<i64> {
        ensure!(
            [
                "orders",
                "fills",
                "execution_canary_dispatch",
                "execution_tiny_reservations"
            ]
            .contains(&table),
            "LOCAL count table"
        );
        Ok(self
            .case
            .sql()?
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
    }
    pub fn export(&self, root: &std::path::Path) -> Result<()> {
        use sha2::{Digest, Sha256};
        let primary = root.join("primary");
        std::fs::create_dir(&primary)?; // never replace an earlier primary snapshot
        for (role, sql) in [
            ("source", self.case.source.sql()?),
            ("bot", self.case.sql()?),
        ] {
            sql.execute(
                "VACUUM main INTO ?1",
                [primary
                    .join(format!("{role}.sqlite"))
                    .to_string_lossy()
                    .as_ref()],
            )?;
        }
        for (role, peer) in [("source", &self.source), ("bot", &self.bot)] {
            for (side, at) in [("buy", 0), ("sell", 1)] {
                let (wire, receipt) = peer.artifact(at)?;
                std::fs::write(primary.join(format!("{role}-{side}.signed.base64")), wire)?;
                std::fs::write(
                    primary.join(format!("{role}-{side}.receipt.json")),
                    serde_json::to_vec_pretty(&receipt)?,
                )?;
            }
            std::fs::write(
                primary.join(format!("{role}-provider-requests.json")),
                serde_json::to_vec_pretty(&peer.state.lock().unwrap().requests)?,
            )?;
        }
        let mut files = BTreeMap::new();
        for entry in std::fs::read_dir(&primary)? {
            let entry = entry?;
            let bytes = std::fs::read(entry.path())?;
            files.insert(
                entry.file_name().to_string_lossy().into_owned(),
                json!({"bytes":bytes.len(),"sha256":format!("{:x}",Sha256::digest(&bytes))}),
            );
        }
        std::fs::write(
            primary.join("MANIFEST.json"),
            serde_json::to_vec_pretty(&json!({
                "kind":"LOCAL_PRIMARY_DAEMON_DB_AND_EXTERNAL_TRANSPORT_ARTIFACTS",
                "source_db":"VACUUM main INTO from SOURCE daemon store",
                "bot_db":"VACUUM main INTO from same BOT consumer/runner store",
                "wire":"actual daemon-signed LOCAL transaction bytes; no private keys exported",
                "rpc":"actual loopback requests; no provider credentials",
                "files":files
            }))?,
        )?;
        Ok(())
    }
}
