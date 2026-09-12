use super::{source_sell_ingress_fixture::Ingress, *};
use copybot_core_types::ExactSwapAmounts;
use serde_json::{json, Value};

pub(super) struct Fixture {
    pub f: Ingress,
    pub discovery: DiscoveryService,
    pub execution: ExecutionConfig,
    pub ingestion: IngestionConfig,
    pub shadow: ShadowConfig,
    pub risk: RiskConfig,
    pub journal: copybot_config::RecentRawJournalConfig,
    pub position: String,
}
impl Fixture {
    pub async fn new(url: &str, hot: bool) -> Result<Self> {
        let mut f = Ingress::new()?;
        f.buy("b70-position-p", "source-b")?;
        let position = f.position()?;
        f.follow_source("leader-a")?;
        let discovery_config = copybot_config::DiscoveryConfig::default();
        let discovery =
            DiscoveryService::new(discovery_config.clone(), permissive_shadow_quality());
        let now = Utc::now();
        let cursor = DiscoveryRuntimeCursor {
            ts_utc: now - chrono::Duration::seconds(1),
            slot: 42,
            signature: "b70-publication".into(),
        };
        f.store.upsert_discovery_runtime_cursor(&cursor)?;
        f.store.set_discovery_publication_state_with_identity(&DiscoveryPublicationStateUpdate {
            runtime_mode:DiscoveryRuntimeMode::Healthy,reason:"bounded b70 fixture".into(),last_published_at:Some(now),
            last_published_window_start:Some(now-chrono::Duration::days(discovery_config.scoring_window_days.max(1) as i64)),
            published_scoring_source:Some("discovery_v2_operational_window".into()),published_wallet_ids:Some(vec!["leader-a".into()]),
        },false,Some(&discovery.discovery_v2_publication_policy_fingerprint(false)),Some(&cursor))?;
        // Close the helper writer before the actual daemon startup opens its own writer.
        f.finish().await?;
        let mut execution = super::b58_fixture::config(url.into());
        execution.canary_enabled = true;
        execution.canary_dry_run = true;
        execution.canary_tiny_submit_enabled = false;
        execution.canary_interval_seconds = 1;
        execution.canary_max_signal_age_seconds = 5;
        execution.canary_kill_switch_path = f
            .path
            .with_extension("disabled-stop")
            .to_string_lossy()
            .into();
        execution.quote_canary_enabled = hot;
        execution.quote_canary_pump_fun_parallel_enabled = true;
        let mut ingestion = IngestionConfig::default();
        ingestion.source = "mock".into();
        let mut shadow = ShadowConfig::default();
        shadow.restart_recovery_enabled = false;
        let mut risk = RiskConfig::default();
        risk.shadow_killswitch_enabled = false;
        let mut journal = copybot_config::RecentRawJournalConfig::default();
        journal.path = f
            .path
            .with_extension("recent.sqlite")
            .to_string_lossy()
            .into();
        Ok(Self {
            f,
            discovery,
            execution,
            ingestion,
            shadow,
            risk,
            journal,
            position,
        })
    }
    pub fn buy(&self) -> SwapEvent {
        SwapEvent {
            signature: "b70-hot-buy-a".into(),
            wallet: "leader-a".into(),
            dex: "pumpswap".into(),
            token_in: crate::execution_quote_canary_helpers::SOL_MINT.into(),
            token_out: "TokenA".into(),
            amount_in: 1.0,
            amount_out: 5.0,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".into(),
                amount_out_raw: "5000000".into(),
                amount_in_decimals: 9,
                amount_out_decimals: 6,
            }),
            slot: 43,
            ts_utc: Utc::now(),
        }
    }
    pub fn sell(&self) -> SwapEvent {
        self.f.sell("b70-owned-sell-b", "source-b")
    }
    pub fn snapshot(&self) -> Result<Value> {
        let b = self.sell();
        let observed: i64 = self.f.conn()?.query_row(
            "SELECT COUNT(*) FROM observed_swaps WHERE signature=?1",
            [&b.signature],
            |r| r.get(0),
        )?;
        let handoff = self.f.store.load_source_sell_handoff(&b.signature)?;
        let staged = self.f.staged(&b.signature)?;
        Ok(
            json!({"observed_sell_rows":observed,"handoff":handoff.map(|h|json!({"sequence":h.sequence,"position":h.original_position_id,"disposition":h.disposition})),"staged":staged.map(|s|json!({"id":s.intent_id,"position":s.position_id,"staged_at":s.staged_at})),"open_position":self.f.position()?}),
        )
    }
    pub async fn run(&self) -> Result<()> {
        let store = SqliteStore::open(&self.f.path)?;
        crate::app_loop::run_app_loop(
            store,
            IngestionService::build(&self.ingestion)?,
            self.discovery.clone(),
            self.f.shadow.clone(),
            self.execution.clone(),
            self.risk.clone(),
            self.ingestion.clone(),
            self.shadow.clone(),
            self.f.path.to_string_lossy().into(),
            3600,
            copybot_config::HistoryRetentionConfig::default(),
            self.journal.clone(),
            3600,
            3600,
            30,
            "mock".into(),
            3600,
            false,
            0,
            false,
            None,
        )
        .await
    }
    pub fn save(&self, label: &str, observation: Value) -> Result<()> {
        let Ok(directory) = std::env::var("B70_CAPTURE_DIR") else {
            return Ok(());
        };
        let dir = PathBuf::from(directory);
        std::fs::create_dir_all(&dir)?;
        let json = dir.join(format!("{label}.json"));
        anyhow::ensure!(!json.exists(), "no overwriting earlier evidence");
        std::fs::write(json, serde_json::to_vec_pretty(&observation)?)?;
        let db = dir.join(format!("{label}.sqlite"));
        self.f
            .conn()?
            .execute("VACUUM INTO ?1", [db.to_string_lossy().as_ref()])?;
        println!("B70_OBSERVATION {observation}");
        Ok(())
    }
}
