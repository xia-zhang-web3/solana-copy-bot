//! Explicit mocks: accepted SOURCE adapter uses loopback; BOT receipt is synthetic.
//! This tests the barrier, not live quote availability or a provider's copy event.
use super::owner_roundtrip_fixture::{Case, RECEIVED};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{OwnerSourceCopyGateConfig, CONFIRMED_NATIVE_BOT_BUY_V1};
use copybot_core_types::{association_delivery::*, Lamports, TokenQuantity};
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    native_buy::*,
    *,
};

pub(super) const EXIT: &str = "execution-only:owner-test-intent";

pub(super) fn classic_exit_bundle(mut bundle: serde_json::Value) -> Result<serde_json::Value> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde_json::json;
    let a = bundle["swapInstruction"]["accounts"]
        .as_array()
        .unwrap()
        .clone();
    let r = &a[9..];
    let mut accounts = a[..9].to_vec();
    accounts.extend_from_slice(&r[..4]);
    let dummy = |n: u8, writable: bool| {
        json!({"pubkey":bs58::encode([n;32]).into_string(),
        "isSigner":false,"isWritable":writable})
    };
    accounts.push(dummy(65, true));
    accounts.extend_from_slice(&r[4..6]);
    for n in 66..=73 {
        accounts.push(dummy(n, !matches!(n, 66 | 73)));
    }
    accounts.extend_from_slice(&r[6..9]);
    let mut data = STANDARD.decode(bundle["swapInstruction"]["data"].as_str().unwrap())?;
    data[12] = 7;
    bundle["swapInstruction"]["accounts"] = json!(accounts);
    bundle["swapInstruction"]["data"] = json!(STANDARD.encode(data));
    Ok(bundle)
}
pub(super) struct GateCase {
    pub source: Case,
    pub bot: SqliteStore,
    pub path: std::path::PathBuf,
    pub bot_order: Option<String>,
}
impl GateCase {
    pub async fn new() -> Result<Self> {
        let mut source = Case::new().await?;
        source.state.lock().unwrap().source_profile = true;
        let path = source.root.path().join("bot.db");
        let mut bot = SqliteStore::open(&path)?;
        bot.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let wallet = bs58::encode(
            ed25519_dalek::SigningKey::from_bytes(&[19; 32])
                .verifying_key()
                .to_bytes(),
        )
        .into_string();
        source
            .config
            .owner_technical_buy
            .as_mut()
            .unwrap()
            .source_copy_exit_gate = Some(OwnerSourceCopyGateConfig {
            policy: CONFIRMED_NATIVE_BOT_BUY_V1.into(),
            bot_state_db_path: path.to_string_lossy().into_owned(),
            bot_wallet_pubkey: wallet,
            bot_run_id: "bot-source-gate-local".into(),
            bot_policy_identity: "b".repeat(64),
            bot_kill_switch_path: source
                .root
                .path()
                .join("BOT-STOP")
                .to_string_lossy()
                .into_owned(),
        });
        copybot_config::validate_owner_technical_buy(&source.config)?;
        Ok(Self {
            source,
            bot,
            path,
            bot_order: None,
        })
    }
    pub fn sql(&self) -> Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(&self.path)?)
    }
    pub fn ready(&self) -> Result<bool> {
        crate::execution_owner_source_gate::ready(
            &self.source.config,
            &self.source.store,
            Utc::now(),
        )
    }
    pub fn seed_bot(&mut self) -> Result<()> {
        self.seed_bot_with_signature(None)
    }
    pub fn seed_bot_with_signature(&mut self, signature: Option<&str>) -> Result<()> {
        let now = Utc::now();
        let p = self.source.config.owner_technical_buy.as_ref().unwrap();
        let g = p.source_copy_exit_gate.as_ref().unwrap();
        let mut source = self
            .source
            .store
            .load_execution_canary_receipt_facts(&self.source.order_id())?
            .context("source BUY")?;
        if let Some(signature) = signature {
            source.tx_signature = signature.into();
        }
        let mut inbox = AssociationInbox::open(
            &self.path,
            InboxLimits {
                count: 100,
                bytes: 1 << 20,
                busy_ms: 100,
            },
        )?;
        inbox.register_technical_cohort_authority(&TechnicalCohortAuthority {
            run_id: g.bot_run_id.clone(),
            wallet_ids: vec![p.wallet_pubkey.clone()],
            mint_policy: CLASSIC_SPL_MINT_POLICY.into(),
            activated_at: DateTime::parse_from_rfc3339(&p.activated_at)?.with_timezone(&Utc),
            deadline: DateTime::parse_from_rfc3339(&p.expires_at)?.with_timezone(&Utc),
            max_buy_count: 1,
            policy_identity: g.bot_policy_identity.clone(),
        })?;
        inbox.record_native_buy_fence_epoch(&NativeBuyFence {
            session: "native-session-A".into(),
            processed_slot: source.slot - 1,
            sampled_at: now - chrono::Duration::milliseconds(1),
            genesis_hash: p.genesis_hash.clone(),
            policy_identity: g.bot_policy_identity.clone(),
        })?;
        let a = AdmissionFacts {
            facts: CheckedFacts {
                signature: source.tx_signature.clone(),
                slot: source.slot,
                wallet: p.wallet_pubkey.clone(),
                token_in: crate::execution_quote_canary_helpers::SOL_MINT.into(),
                token_out: p.mint.clone(),
                amount_in_bits: 0.01_f64.to_bits(),
                amount_out_bits: (RECEIVED as f64 / 1e6).to_bits(),
                programs: vec!["mock-raydium".into()],
                dex: "fixture".into(),
                program_fallback: false,
                exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
                    amount_in_raw: "10000000".into(),
                    amount_out_raw: RECEIVED.to_string(),
                    amount_in_decimals: 9,
                    amount_out_decimals: 6,
                }),
            },
            info: InfoIdentity {
                encoded: vec![1, 2, 3],
                float_bits: vec![],
            },
            message_time: MessageTime::Missing,
        };
        let delivery = |sequence, event| Delivery {
            session: "native-session-A".into(),
            sequence,
            arrival_offset_ns: sequence,
            event,
        };
        inbox.persist_at(
            &delivery(1, DeliveryEvent::Admission(a.clone())),
            &CandidateGeneration::Unknown,
            now,
        )?;
        inbox.persist_at(
            &delivery(
                2,
                DeliveryEvent::Terminal {
                    signature: source.tx_signature.clone(),
                    expected: a,
                    result: Terminal::ProviderAsserted(ProviderAssertion {
                        signature: source.tx_signature.clone(),
                        slot: source.slot,
                        blockhash: "mock-block".into(),
                        transaction_index: 1,
                        block_time: BlockTime::Missing,
                    }),
                },
            ),
            &CandidateGeneration::Unknown,
            now,
        )?;
        assert!(self.bot.native_buy_record_finalized(
            &source.tx_signature,
            source.slot,
            SPL_TOKEN_PROGRAM,
            now
        )?);
        let candidate = self
            .bot
            .native_buy_ready(&source.tx_signature, now, 120)?
            .context("native candidate")?;
        let signal = self
            .bot
            .load_copy_signal_by_signal_id(&candidate.signal_id)?
            .unwrap();
        let order = self
            .bot
            .reserve_execution_canary_order(&signal.signal_id, "jupiter_swap_instructions", now)?
            .order;
        self.bot.prepare_tiny_native_policy_for_native_buy(
            &g.bot_run_id,
            &g.bot_wallet_pubkey,
            1_000_000_000,
            160_200_031,
            source.slot + 1,
            now,
            &NativeBuyActivationBinding {
                signal_id: signal.signal_id.clone(),
                decision_id: candidate.decision_id,
                policy_identity: g.bot_policy_identity.clone(),
                max_age_seconds: 120,
                order_id: order.order_id.clone(),
                client_order_id: order.client_order_id.clone(),
                attempt: 1,
                route: order.route.clone(),
            },
            || Ok(now),
        )?;
        self.bot.mark_execution_canary_built(&order.order_id, now)?;
        self.bot.mark_execution_canary_simulated(
            &order.order_id,
            now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        let signature = bs58::encode([23; 64]).into_string();
        // Synthetic dispatch operands only; no transaction is signed or submitted.
        self.sql()?.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at) VALUES(?1,?2,?3,?4,1,?5,?6,'buy',?7,?8,?8,?9)",
            rusqlite::params![order.order_id,signal.signal_id,order.client_order_id,order.route,g.bot_wallet_pubkey,p.mint,signature,"c".repeat(64),now.to_rfc3339()])?;
        self.bot
            .mark_execution_canary_submitted(&order.order_id, now, &signature)?;
        self.bot.mark_execution_canary_confirmed_unreconciled(
            &order.order_id,
            &ExecutionCanaryReceiptProof {
                tx_signature: signature.clone(),
                wallet_pubkey: g.bot_wallet_pubkey.clone(),
                token: p.mint.clone(),
                side: "buy".into(),
                confirmation_status: "confirmed".into(),
                slot: Some(source.slot + 1),
                confirmed_at: now,
                reason: "mock_accounting_pending".into(),
            },
            now,
        )?;
        let mut facts = source;
        facts.order_id = order.order_id.clone();
        facts.tx_signature = signature;
        facts.wallet_pubkey = g.bot_wallet_pubkey.clone();
        facts.fee_payer = Some(g.bot_wallet_pubkey.clone());
        facts.slot += 1;
        // The follower may receive a different amount. SOURCE still exits only its own BUY quantity.
        facts.token_delta.as_mut().unwrap().raw -= 17;
        self.bot
            .record_execution_canary_receipt_facts(&facts, now)?;
        let cost = u64::try_from(-facts.wallet_native_delta.as_i128())?;
        self.bot.confirm_execution_canary_buy_fill(
            &order.order_id,
            &p.mint,
            (RECEIVED - 17) as f64 / 1e6,
            Some(TokenQuantity::new(RECEIVED - 17, 6)),
            cost as f64 / 1e9,
            now,
            now,
            Some(Lamports::new(cost)),
        )?;
        self.sql()?.execute("UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3 WHERE singleton=1",
            rusqlite::params![order.order_id,p.mint,format!("exec-canary-pos:{}",order.order_id)])?;
        self.bot_order = Some(order.order_id);
        Ok(())
    }
    pub fn assert_held(&self) -> Result<()> {
        assert!(!self.ready()?);
        assert!(crate::execution_owner_roundtrip::exit_config(
            &self.source.config,
            &self.source.store
        )?
        .is_none());
        assert!(self.source.store.load_owner_exit_intent(EXIT)?.is_none());
        assert_eq!(self.source.calls("quote"), 1);
        assert_eq!(self.source.calls("sendTransaction"), 1);
        Ok(())
    }
}
