//! Native admission/BUY graph is synthetic; actual SELL adapter/signer use loopback mocks.
use super::{owner_roundtrip_fixture::Case, owner_source_gate_fixture::GateCase};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{ExecutionConfig, OwnerExitConfig, OWNER_NATIVE_RECOVERY_EXIT_V1};
use copybot_storage_core::SqliteStore;

pub(super) struct NativeCase {
    pub gate: GateCase,
    pub provider: Case,
    pub config: ExecutionConfig,
    pub buy: String,
    pub exit: String,
}
impl NativeCase {
    pub async fn new() -> Result<Self> {
        let mut gate = Box::pin(GateCase::new()).await?;
        Box::pin(gate.source.tick()).await?; // A mocked historic SOURCE BUY, never an action of recovery.
        gate.seed_bot()?; // Accepted fixture records a genuine native-origin graph, synthetic receipt.
        let provider = Box::pin(Case::with_seed([19; 32])).await?;
        let buy = gate.bot_order.clone().unwrap();
        let facts = gate
            .bot
            .load_execution_canary_receipt_facts(&buy)?
            .context("mock native BUY")?;
        let position = gate
            .bot
            .load_execution_canary_open_position(&facts.token)?
            .unwrap();
        let p = gate.source.config.owner_technical_buy.as_ref().unwrap();
        let run = p.source_copy_exit_gate.as_ref().unwrap().bot_run_id.clone();
        let now = Utc::now().to_rfc3339();
        gate.sql()?.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,actual_fee,outcome,reserved_at,reconciled_at)
            VALUES(?1,?2,?3,?4,'buy',?5,?5,NULL,19000,14000,?6,19000,'successful',?7,?7)",
            rusqlite::params![buy,run,facts.tx_signature,facts.wallet_pubkey,"c".repeat(64),facts.slot.to_string(),now])?;
        gate.sql()?.execute(
            "INSERT INTO execution_tiny_capital_evidence VALUES(?1,10000000,985000000,?2)",
            rusqlite::params![buy, "d".repeat(64)],
        )?;
        let mut config = provider.config.clone();
        config.owner_technical_buy = None;
        config.native_fresh_buy = None;
        config.technical_cohort = None;
        config.owned_sell_preparation = None;
        config.canary_entry_submit_enabled = false;
        config.tiny_experiment.id = Some(run.clone());
        config.tiny_experiment.activate = false;
        let source = buy.strip_prefix("exec-canary:native-buy-v1:").unwrap();
        let exit = format!("native-recovery:{source}");
        config.owner_exit = Some(OwnerExitConfig {
            native_recovery: None,
            policy: OWNER_NATIVE_RECOVERY_EXIT_V1.into(),
            activate: true,
            run_id: run,
            intent_id: exit.clone(),
            buy_order_id: buy.clone(),
            buy_receipt_signature: facts.tx_signature,
            position_id: position.position_id,
            wallet_pubkey: config.canary_wallet_pubkey.clone(),
            signer_pubkey: config.execution_signer_pubkey.clone(),
            genesis_hash: p.genesis_hash.clone(),
            mint: facts.token,
            amount_raw: facts.token_delta.unwrap().raw as u64,
            decimals: 6,
            route: "jupiter_swap_instructions".into(),
            activated_at: p.activated_at.clone(),
            expires_at: p.expires_at.clone(),
            max_priority_fee_lamports: 22000,
            min_reserve_lamports: 985000000,
            max_slippage_bps: 50,
            max_daily_loss_lamports: 20000000,
        });
        {
            let mut state = provider.state.lock().unwrap();
            state.sent = Some("MOCK_PRIOR_NATIVE_BUY".into());
            state.received_raw = Some(config.owner_exit.as_ref().unwrap().amount_raw);
        }
        copybot_config::validate_owner_exit(&config)?;
        Ok(Self {
            gate,
            provider,
            config,
            buy,
            exit,
        })
    }
    pub async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        self.at(Utc::now()).await
    }
    pub async fn at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        Box::pin(
            crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone())
                .process_tick(&self.gate.bot, now),
        )
        .await
    }
    pub fn reopen(&self) -> Result<SqliteStore> {
        SqliteStore::open(&self.gate.path)
    }
}
