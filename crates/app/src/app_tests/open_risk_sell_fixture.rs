use super::open_risk_sell_rpc_fixture::{serve, Trace};
use super::open_risk_sell_task_fixture::RpcTask;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{ExecutionConfig, ShadowConfig};
use copybot_core_types::{CopySignalRow, ExactSwapAmounts, SwapEvent, TokenQuantity};
use copybot_shadow::{FollowSnapshot, ShadowProcessOutcome, ShadowService, ShadowSignalResult};
use copybot_storage_core::SqliteStore;
use ed25519_dalek::SigningKey;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub(super) const TOKEN: &str = "FNVhryGP7Epjbr9iWLv75ABCYzY3au4icYHNdExn9jZ3";
pub(super) const SOL: &str = "So11111111111111111111111111111111111111112";

pub(super) struct Fixture {
    pub store: SqliteStore,
    pub config: ExecutionConfig,
    pub service: ShadowService,
    pub follow: FollowSnapshot,
    pub swap: SwapEvent,
    pub now: DateTime<Utc>,
    pub calls: Arc<Mutex<Vec<(String, Value)>>>,
    pub receipt: Arc<Mutex<Option<Value>>>,
    pub responses: Trace,
    pub dir: PathBuf,
    server: RpcTask,
}

impl Fixture {
    pub async fn new(byte_price: u64) -> Result<Self> {
        let now = Utc::now();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "copybot-open-risk-{}-{unique}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&dir)?;
        let key = SigningKey::from_bytes(&[13; 32]);
        let payer = key.verifying_key().to_bytes();
        let key_path = dir.join("synthetic-key.json");
        std::fs::write(
            &key_path,
            serde_json::to_vec(&[key.to_bytes().to_vec(), payer.to_vec()].concat())?,
        )?;
        let mut store = SqliteStore::open(dir.join("test.db"))?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let opened = now - Duration::minutes(5);
        store.activate_follow_wallet("leader", opened - Duration::seconds(1), "test")?;
        store.insert_shadow_lot_exact(
            "leader",
            TOKEN,
            20.0,
            Some(TokenQuantity::new(20_000, 3)),
            0.2,
            opened,
        )?;
        store.record_execution_canary_open_position(
            "owned-buy",
            TOKEN,
            7.0,
            Some(TokenQuantity::new(7_000, 3)),
            0.07,
            opened,
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let mut config = ExecutionConfig::default();
        config.quote_canary_enabled = true;
        config.priority_fee_canary_enabled = true;
        config.priority_fee_canary_rpc_url = url.clone();
        config.quote_canary_base_url = url.clone();
        config.submit_adapter_http_url = url;
        config.canary_enabled = true;
        config.canary_dry_run = true;
        config.canary_tiny_submit_enabled = true;
        config.canary_route =
            crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.into();
        config.canary_wallet_pubkey = bs58::encode(payer).into_string();
        config.execution_signer_pubkey = config.canary_wallet_pubkey.clone();
        config.execution_signer_keypair_path = key_path.to_string_lossy().into();
        config.canary_kill_switch_path = dir.join("kill-switch").to_string_lossy().into();
        config.pretrade_max_priority_fee_lamports = 500_000;
        config.swap_instructions_dry_run_enabled = true;
        config.swap_transaction_dry_run_enabled = true;
        config.quote_canary_pump_fun_parallel_enabled = false;
        config.quote_canary_timeout_ms = 500;
        config.submit_timeout_ms = 500;
        config.max_confirm_seconds = 1;
        config.max_submit_attempts = 3;
        let mut shadow = ShadowConfig::default();
        shadow.enabled = true;
        shadow.quality_gates_enabled = false;
        shadow.copy_notional_sol = 0.5;
        shadow.min_leader_notional_sol = 0.25;
        shadow.max_signal_lag_seconds = 30;
        let swap = SwapEvent {
            wallet: "leader".into(),
            dex: "pumpswap".into(),
            token_in: TOKEN.into(),
            token_out: SOL.into(),
            amount_in: 10.0,
            amount_out: 0.1,
            signature: "raw-small-late-sell".into(),
            slot: 100,
            ts_utc: now - Duration::minutes(2),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "10000".into(),
                amount_in_decimals: 3,
                amount_out_raw: "100000000".into(),
                amount_out_decimals: 9,
            }),
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let receipt = Arc::new(Mutex::new(None));
        let responses = Arc::new(Mutex::new(Vec::new()));
        let server_calls = calls.clone();
        let server_receipt = receipt.clone();
        let server = RpcTask::new(tokio::spawn(serve(
            listener,
            payer,
            byte_price,
            server_calls,
            server_receipt,
            responses.clone(),
        )));
        Ok(Self {
            store,
            config,
            service: ShadowService::new(shadow),
            follow: FollowSnapshot::from_active_wallets(["leader".into()].into()),
            swap,
            now,
            calls,
            receipt,
            responses,
            dir,
            server,
        })
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.server.finish().await
    }

    pub async fn quote_raw_sell(&self) -> Result<(ShadowSignalResult, String)> {
        self.store.insert_observed_swap(&self.swap)?;
        let signal =
            match self
                .service
                .process_swap(&self.store, &self.swap, &self.follow, self.now)?
            {
                ShadowProcessOutcome::Recorded(signal) => signal,
                other => panic!("raw open-risk SELL must reach quote pipeline: {other:?}"),
            };
        assert_eq!(signal.closed_qty, 10.0);
        let saved = self
            .store
            .load_copy_signal_by_signal_id(&signal.signal_id)?
            .unwrap();
        assert_eq!(saved.ts, self.swap.ts_utc);
        let closes = self
            .store
            .list_execution_quote_canary_close_candidates_for_signal(&signal.signal_id, 10)?;
        assert_eq!(closes.len(), 1);
        let id = format!("quote:close:{}", closes[0].id);
        let runner =
            crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(self.config.clone());
        let summary = runner
            .process_recorded_shadow_signal(&self.store, &signal, self.now)
            .await?;
        assert_eq!(summary.close_inserted, 1);
        let quote = self
            .store
            .load_execution_quote_canary_event_by_id(&id)?
            .unwrap();
        assert_eq!(quote.signal_ts, Some(self.swap.ts_utc));
        assert_eq!(quote.request_ts, self.now);
        assert_eq!(quote.quote_status, "ok");
        assert_eq!(quote.quote_in_amount_raw.as_deref(), Some("10000"));
        Ok((signal, id))
    }

    pub async fn submit(
        &self,
        id: &str,
    ) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
        Ok(
            crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
                &self.config,
                &self.store,
                id,
                self.now,
            )
            .await?
            .expect("enabled tiny route"),
        )
    }
    pub fn sends(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, v)| v["method"] == "sendTransaction")
            .count()
    }
    pub fn close_count(&self, signal_id: &str) -> Result<u64> {
        Ok(
            rusqlite::Connection::open(self.dir.join("test.db"))?.query_row(
                "SELECT COUNT(*) FROM shadow_closed_trades WHERE signal_id = ?1",
                [signal_id],
                |r| r.get(0),
            )?,
        )
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(self.dir.join("test.db"))?;
        Ok(())
    }
    // Prior orders are independent negative controls, never the tested SELL signal.
    pub fn prior_order(&self, side: &str, ts: DateTime<Utc>, confirmed: bool) -> Result<String> {
        self.store.insert_copy_signal(&CopySignalRow {
            signal_id: format!("prior-{side}"),
            wallet_id: "leader".into(),
            side: side.into(),
            token: TOKEN.into(),
            notional_sol: 0.07,
            notional_lamports: None,
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.into(),
            ts,
            status: "shadow_recorded".into(),
        })?;
        let order = self
            .store
            .reserve_execution_canary_order(
                &format!("prior-{side}"),
                &self.config.canary_route,
                ts,
            )?
            .order;
        if confirmed {
            self.store
                .mark_execution_canary_built(&order.order_id, ts)?;
            self.store.mark_execution_canary_simulated(
                &order.order_id,
                ts,
                copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
                None,
            )?;
            self.store.mark_execution_canary_submitted(
                &order.order_id,
                ts,
                "prior-synthetic-tx",
            )?;
            self.store
                .mark_execution_canary_confirmed(&order.order_id, ts)?;
        }
        Ok(order.order_id)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
