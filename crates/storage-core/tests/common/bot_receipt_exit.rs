#![allow(dead_code)]
#[path = "association.rs"]
mod association;
#[path = "bot_receipt_exit_source.rs"]
mod source;
use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::AssociationInbox, native_buy::*, *};
use rusqlite::{params, Connection};

pub const BOT: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
pub const SOURCE: &str = "SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS";
pub const RUN: &str = "old-native-bot-run";
pub const SOURCE_SIG: &str = "source-native-buy";
pub const BUY_SIG: &str = "native-buy-receipt";

pub struct Case {
    pub _dir: tempfile::TempDir,
    pub path: std::path::PathBuf,
    pub store: SqliteStore,
    pub intent: OwnerExitIntent,
}
impl Case {
    pub fn new() -> Result<Self> {
        let (dir, path) = association::db();
        let now = Utc.with_ymd_and_hms(2026, 10, 24, 15, 34, 30).unwrap();
        let mut inbox = AssociationInbox::open(&path, association::limits())?;
        let scope = controlled_receipt_lot::Scope {
            policy: controlled_receipt_lot::POLICY.into(), source_run_id: "old-source-run".into(),
            bot_run_id: RUN.into(), source_wallet: SOURCE.into(), bot_wallet: BOT.into(),
            source_buy_intent_id: "old-source-buy".into(), source_state_db_path: dir.path().join("source.db").to_str().unwrap().into(),
        };
        source::seed(&scope, now)?;
        inbox.register_controlled_receipt_lot_scope(&scope, BOT)?;
        let authority = TechnicalCohortAuthority { run_id: RUN.into(), wallet_ids: vec![SOURCE.into()],
            mint_policy: CLASSIC_SPL_MINT_POLICY.into(), activated_at: now,
            deadline: now + Duration::seconds(280), max_buy_count: 1, policy_identity: "old-policy".into() };
        inbox.register_technical_cohort_authority(&authority)?;
        inbox.record_native_buy_fence_epoch(&NativeBuyFence { session: "session-A".into(),
            processed_slot: 41, sampled_at: now + Duration::seconds(1),
            genesis_hash: "saved-genesis".into(), policy_identity: "old-policy".into() })?;
        let mut facts = association::facts();
        facts.facts.signature = SOURCE_SIG.into(); facts.facts.wallet = SOURCE.into();
        facts.facts.token_in = "So11111111111111111111111111111111111111112".into();
        facts.facts.token_out = OWNER_EXIT_MINT.into(); facts.facts.slot = 42;
        facts.facts.exact_amounts = Some(copybot_core_types::ExactSwapAmounts {
            amount_in_raw: "10000000".into(), amount_in_decimals: 9,
            amount_out_raw: "988701".into(), amount_out_decimals: 6 });
        inbox.persist_at(&association::event(1, DeliveryEvent::Admission(facts.clone())),
            &CandidateGeneration::Unknown, now + Duration::seconds(2))?;
        inbox.persist_at(&association::event(2, DeliveryEvent::Terminal {
            signature: SOURCE_SIG.into(), expected: facts.clone(),
            result: Terminal::ProviderAsserted(ProviderAssertion { signature: SOURCE_SIG.into(),
                slot: 42, blockhash: "source-block".into(), transaction_index: 1,
                block_time: BlockTime::Missing }) }), &CandidateGeneration::Unknown, now + Duration::seconds(2))?;
        let store = SqliteStore::open(&path)?;
        assert!(store.native_buy_record_finalized(SOURCE_SIG, 42, SPL_TOKEN_PROGRAM, now + Duration::seconds(3))?);
        let native = store.native_buy_ready(SOURCE_SIG, now + Duration::seconds(3), 120)?.unwrap();
        let order = store.reserve_execution_canary_order(&native.signal_id, "jupiter_swap_instructions",
            now + Duration::seconds(3))?.order;
        let binding = NativeBuyActivationBinding { signal_id: native.signal_id,
            decision_id: native.decision_id, policy_identity: "old-policy".into(), max_age_seconds: 120,
            order_id: order.order_id.clone(), client_order_id: order.client_order_id.clone(),
            attempt: 1, route: order.route.clone() };
        store.prepare_tiny_native_policy_for_native_buy(RUN, BOT, 265_762_817, 0, 43,
            now + Duration::seconds(3), &binding, || Ok(now + Duration::seconds(3)))?;
        let position = format!("exec-canary-pos:{}", order.order_id);
        let c = Connection::open(&path)?;
        c.execute("UPDATE orders SET status='execution_canary_confirmed',tx_signature=?2,
            simulation_status='passed' WHERE order_id=?1", params![order.order_id, BUY_SIG])?;
        c.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,
            wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
            VALUES(?1,?2,?3,?4,1,?5,?6,'buy',?7,?8,?9,?10)", params![order.order_id,order.signal_id,
            order.client_order_id,order.route,BOT,OWNER_EXIT_MINT,BUY_SIG,"a".repeat(64),"b".repeat(64),now.to_rfc3339()])?;
        c.execute("INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,token,
            side,confirmation_status,slot,confirmed_at,last_attempt_at,reason)
            VALUES(?1,?2,?3,?4,'buy','finalized','44',?5,?5,'accounting_complete')",
            params![order.order_id,BUY_SIG,BOT,OWNER_EXIT_MINT,now.to_rfc3339()])?;
        c.execute("INSERT INTO execution_canary_receipt_facts(order_id,tx_signature,wallet_pubkey,token,
            side,slot,wallet_native_pre,wallet_native_post,wallet_native_delta,transaction_fee,fee_coverage,
            fee_payer,token_delta_raw,token_decimals,token_coverage,wsol_coverage,decomposition,recorded_at)
            VALUES(?1,?2,?3,?4,'buy','44','265762817','255707817','-10055000','55000','known',?3,
            '988620',6,'paired_balances','observed','unresolved',?5)",
            params![order.order_id,BUY_SIG,BOT,OWNER_EXIT_MINT,now.to_rfc3339()])?;
        c.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,cost_lamports,
            qty_raw,qty_decimals,accounting_bucket) VALUES(?1,?2,0.988620,0.010055,?3,'open',10055000,
            '988620',6,'execution_canary')",params![position,OWNER_EXIT_MINT,now.to_rfc3339()])?;
        c.execute("INSERT INTO fills(order_id,token,qty,avg_price,notional_lamports,qty_raw,qty_decimals,
            position_id) VALUES(?1,?2,0.988620,0.01,10055000,'988620',6,?3)",params![order.order_id,OWNER_EXIT_MINT,position])?;
        c.execute("UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3 WHERE singleton=1",
            params![order.order_id,OWNER_EXIT_MINT,position])?;
        c.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,
            side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,actual_fee,
            outcome,reserved_at,reconciled_at) VALUES(?1,?2,?3,?4,'buy',?5,?6,NULL,55000,50000,'43',55000,
            'successful',?7,?7)",params![order.order_id,RUN,BUY_SIG,BOT,"b".repeat(64),"a".repeat(64),now.to_rfc3339()])?;
        let known = |v: &str| NativeObservation::known(v, ObservationSource::RpcTokenBalance);
        let endpoint = |raw: &str| NativeTokenEndpoint { mint: known(OWNER_EXIT_MINT), token_owner: known(BOT),
            token_program: known(SPL_TOKEN_PROGRAM), decimals: known("6"), raw: known(raw) };
        let observations = NativeAccountObservations { order_id: order.order_id.clone(),tx_signature: BUY_SIG.into(),
            wallet_pubkey: BOT.into(),token: OWNER_EXIT_MINT.into(),side: "buy".into(),slot: "44".into(),
            accounts: vec![NativeAccountObservation { account_index: 1,pubkey: "saved-bot-usdc-ata".into(),
                native_pre: NativeObservation::known("1488440",ObservationSource::RpcNativeBalance),
                native_post: NativeObservation::known("1488440",ObservationSource::RpcNativeBalance),
                native_delta: NativeObservation::known("0",ObservationSource::RpcNativeBalance),
                pre_token: endpoint("100"),post_token: endpoint("988720"),relevance: vec!["target_mint".into()] }],
            instructions: vec![],accounts_coverage: ObservationCoverage::Known,
            instructions_coverage: ObservationCoverage::Known,reasons: vec![] };
        c.execute("INSERT INTO execution_receipt_native_observations VALUES(?1,?2,?3,NULL,?4)",
            params![order.order_id,BUY_SIG,serde_json::to_string(&observations)?,now.to_rfc3339()])?;
        let intent = OwnerExitIntent { intent_id: format!("bot-receipt-exit:{SOURCE_SIG}"),run_id: RUN.into(),
            buy_order_id: order.order_id,buy_receipt_signature: BUY_SIG.into(),position_id: position,
            wallet: BOT.into(),signer: BOT.into(),genesis_hash: "saved-genesis".into(),mint: OWNER_EXIT_MINT.into(),
            amount_raw: 988620,decimals: 6,route: "jupiter_swap_instructions".into(),
            activated_at: authority.deadline + Duration::seconds(1),expires_at: authority.deadline + Duration::seconds(301),
            authority_sha256: "c".repeat(64),max_priority_fee_lamports: 22000,min_reserve_lamports: 0,
            max_slippage_bps: 50,max_daily_loss_lamports: 20000000 };
        Ok(Self { _dir: dir,path,store,intent })
    }
    pub fn conn(&self) -> Result<Connection> { Ok(Connection::open(&self.path)?) }
    pub fn parent_rows(&self) -> Result<Vec<String>> {
        let c = self.conn()?;
        ["execution_tiny_experiment","execution_tiny_native_policy","native_buy_technical_cohort",
            "native_buy_decisions","native_buy_cohort_decisions","native_buy_fence_epochs"]
            .into_iter().map(|t| {
                let mut q = c.prepare(&format!("SELECT * FROM {t} ORDER BY 1"))?;
                let n = q.column_count();
                Ok(format!("{t}:{:?}", q.query_map([],|r|(0..n).map(|i|r.get::<_,rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>())?.collect::<rusqlite::Result<Vec<_>>>()?))
            }).collect()
    }
    pub fn dispatch(&self) -> Result<(ExecutionCanaryOrder,ExecutionCanaryDispatch,TinyBudgetClaim)> {
        self.store.register_owner_exit_intent(&self.intent)?;
        let now = self.intent.activated_at;
        let order = self.store.reserve_owner_exit_order(&self.intent.intent_id,||Ok(now))?.order;
        self.store.mark_execution_canary_built(&order.order_id,now)?;
        let order = self.store.mark_execution_canary_simulated(&order.order_id,now,EXECUTION_SIMULATION_STATUS_PASSED,None)?;
        let d = ExecutionCanaryDispatch { order_id: order.order_id.clone(),signal_id: order.signal_id.clone(),
            client_order_id: order.client_order_id.clone(),route: order.route.clone(),attempt: 1,wallet: BOT.into(),
            token: OWNER_EXIT_MINT.into(),side: "sell".into(),tx_signature: "one-exit-signature".into(),
            transaction_sha256: "d".repeat(64),message_sha256: "e".repeat(64) };
        let budget = TinyBudgetClaim { experiment_id: RUN.into(),wallet: BOT.into(),tx_signature: d.tx_signature.clone(),
            message_sha256: d.message_sha256.clone(),transaction_sha256: d.transaction_sha256.clone(),
            buy_lamports: Some(0),protected_capital: None,total_fee: 27000,priority_fee: 22000,fee_slot: 50 };
        Ok((order,d,budget))
    }
}
