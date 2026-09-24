//! Full daemon tick with a synthetic copy of the accepted BUY ownership chain.
use super::{owner_buy_fixture, owner_exit_test_fixture as fixture};
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_config::{OwnerExitConfig, OWNER_EXIT_V1};
use copybot_storage_core::{
    NativeAccountObservation, NativeAccountObservations, NativeInstructionObservation,
    NativeObservation as Obs, NativeTokenEndpoint, ObservationCoverage as Cov,
    ObservationSource as Src, OwnerTechnicalBuyIntent, SqliteStore, OWNER_EXIT_BUY_ORDER,
    OWNER_EXIT_MINT,
};
use rusqlite::{params, Connection};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

const BUY_SIG: &str = "synthetic-confirmed-buy-signature";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SYSTEM: &str = "11111111111111111111111111111111";
const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const WSOL: &str = "So11111111111111111111111111111111111111112";
fn known(value: impl ToString) -> Obs {
    Obs::known(value, Src::ParsedInstruction)
}
fn endpoint(wallet: &str, raw: &str) -> NativeTokenEndpoint {
    NativeTokenEndpoint {
        mint: known(OWNER_EXIT_MINT),
        token_owner: known(wallet),
        token_program: known(TOKEN),
        decimals: known("6"),
        raw: known(raw),
    }
}
fn ix(
    index: u32,
    program: &str,
    kind: &str,
    fields: &[(&str, &str)],
) -> NativeInstructionObservation {
    NativeInstructionObservation {
        outer_index: index,
        inner_index: None,
        stack_height: Obs::unknown(Cov::Missing),
        program_id: known(program),
        instruction_type: known(kind),
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), known(*v)))
            .collect::<BTreeMap<_, _>>(),
        coverage: Cov::Known,
    }
}
fn buy_observations(wallet: &str) -> NativeAccountObservations {
    let mut out = NativeAccountObservations {
        order_id: OWNER_EXIT_BUY_ORDER.into(),
        tx_signature: BUY_SIG.into(),
        wallet_pubkey: wallet.into(),
        token: OWNER_EXIT_MINT.into(),
        side: "buy".into(),
        slot: "120".into(),
        accounts: vec![NativeAccountObservation {
            account_index: 1,
            pubkey: "synthetic-usdc-ata".into(),
            native_pre: known("0"),
            native_post: known("1488440"),
            native_delta: known("1488440"),
            pre_token: endpoint(wallet, "0"),
            post_token: endpoint(wallet, "1167085"),
            relevance: vec!["target_mint".into()],
        }],
        instructions: vec![],
        accounts_coverage: Cov::Missing,
        instructions_coverage: Cov::Unsupported,
        reasons: vec!["synthetic_offline_receipt".into()],
    };
    out.instructions.push(ix(
        0,
        ATA,
        "createIdempotent",
        &[
            ("account", "synthetic-usdc-ata"),
            ("wallet", wallet),
            ("mint", OWNER_EXIT_MINT),
        ],
    ));
    out.instructions.push(ix(
        1,
        SYSTEM,
        "createAccount",
        &[
            ("source", wallet),
            ("newAccount", "synthetic-usdc-ata"),
            ("lamports", "1488440"),
        ],
    ));
    out.instructions.push(ix(
        2,
        ATA,
        "createIdempotent",
        &[
            ("account", "synthetic-wsol-ata"),
            ("wallet", wallet),
            ("mint", WSOL),
        ],
    ));
    out.instructions.push(ix(
        3,
        SYSTEM,
        "transfer",
        &[
            ("source", wallet),
            ("destination", "synthetic-wsol-ata"),
            ("lamports", "10000000"),
        ],
    ));
    out.instructions.push(ix(
        4,
        TOKEN,
        "transfer",
        &[
            ("source", "synthetic-wsol-ata"),
            ("authority", wallet),
            ("destination", "synthetic-pool"),
            ("amount", "10000000"),
        ],
    ));
    out.instructions.push(ix(
        5,
        TOKEN,
        "closeAccount",
        &[("account", "synthetic-wsol-ata"), ("destination", wallet)],
    ));
    out
}
fn seed_buy(store: &SqliteStore, path: &PathBuf, wallet: &str) -> Result<()> {
    let now = Utc::now();
    let buy = OwnerTechnicalBuyIntent {
        intent_id: "copybot-owner-buy-20260924-04-usdc-01".into(),
        run_id: "synthetic-old-buy-run".into(),
        wallet: wallet.into(),
        signer: wallet.into(),
        genesis_hash: owner_buy_fixture::GENESIS.into(),
        mint: OWNER_EXIT_MINT.into(),
        amount_lamports: 10_000_000,
        route: "jupiter_swap_instructions".into(),
        activated_at: now - Duration::minutes(5),
        expires_at: now + Duration::minutes(5),
        authority_sha256: "a".repeat(64),
        max_priority_fee_lamports: 50_000,
        min_reserve_lamports: 160_200_031,
        max_slippage_bps: 50,
        max_daily_loss_lamports: 20_000_000,
        max_open_positions: 1,
        max_buy_count: 1,
    };
    store.register_owner_technical_buy_intent(&buy)?;
    let order = store
        .reserve_owner_technical_buy_order(&buy.intent_id, || Ok(now))?
        .order;
    assert_eq!(order.order_id, OWNER_EXIT_BUY_ORDER);
    let c = Connection::open(path)?;
    c.execute(
        "UPDATE orders SET status='execution_canary_confirmed',
        tx_signature=?1,simulation_status='passed' WHERE order_id=?2",
        params![BUY_SIG, OWNER_EXIT_BUY_ORDER],
    )?;
    c.execute(
        "INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,
        attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
        VALUES(?1,?2,?3,?4,1,?5,?6,'buy',?7,?8,?9,?10)",
        params![
            order.order_id,
            order.signal_id,
            order.client_order_id,
            order.route,
            wallet,
            OWNER_EXIT_MINT,
            BUY_SIG,
            "b".repeat(64),
            "c".repeat(64),
            now.to_rfc3339()
        ],
    )?;
    c.execute(
        "INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,
        token,side,confirmation_status,slot,confirmed_at,last_attempt_at,reason)
        VALUES(?1,?2,?3,?4,'buy','finalized','120',?5,?5,'accounting_complete')",
        params![
            OWNER_EXIT_BUY_ORDER,
            BUY_SIG,
            wallet,
            OWNER_EXIT_MINT,
            now.to_rfc3339()
        ],
    )?;
    c.execute(
        "INSERT INTO execution_canary_receipt_facts(order_id,tx_signature,wallet_pubkey,
        token,side,slot,wallet_native_pre,wallet_native_post,wallet_native_delta,
        transaction_fee,fee_coverage,fee_payer,token_delta_raw,token_decimals,
        token_coverage,wsol_coverage,decomposition,recorded_at)
        VALUES(?1,?2,?3,?4,'buy','120','175200031','163706591','-11493440',
        '5000','known',?3,'1167085',6,'proven_lifecycle','observed','unresolved',?5)",
        params![
            OWNER_EXIT_BUY_ORDER,
            BUY_SIG,
            wallet,
            OWNER_EXIT_MINT,
            now.to_rfc3339()
        ],
    )?;
    let position = format!("exec-canary-pos:{OWNER_EXIT_BUY_ORDER}");
    c.execute(
        "INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
        cost_lamports,qty_raw,qty_decimals,pnl_lamports,accounting_bucket)
        VALUES(?1,?2,1.167085,0.01149344,?3,'open',11493440,'1167085',6,0,'execution_canary')",
        params![position, OWNER_EXIT_MINT, now.to_rfc3339()],
    )?;
    c.execute(
        "INSERT INTO fills(order_id,token,qty,avg_price,notional_lamports,qty_raw,
        qty_decimals,position_id) VALUES(?1,?2,1.167085,0.009847988792590088,
        11493440,'1167085',6,?3)",
        params![OWNER_EXIT_BUY_ORDER, OWNER_EXIT_MINT, position],
    )?;
    c.execute(
        "INSERT INTO execution_receipt_native_observations(
        order_id,tx_signature,observations_json,recorded_at) VALUES(?1,?2,?3,?4)",
        params![
            OWNER_EXIT_BUY_ORDER,
            BUY_SIG,
            serde_json::to_string(&buy_observations(wallet))?,
            now.to_rfc3339()
        ],
    )?;
    Ok(())
}

struct Case {
    root: super::temporary_output_fixture::OutputRoot,
    path: PathBuf,
    store: SqliteStore,
    config: copybot_config::ExecutionConfig,
    server: fixture::Server,
    adapter: Arc<owner_buy_fixture::SignedAdapter>,
}
impl Case {
    async fn new() -> Result<Self> {
        let root = super::temporary_output_fixture::OutputRoot::new("owner-exit")?;
        let path = root.path().join("state.db");
        let wallet = fixture::wallet();
        let server = fixture::Server::start().await?;
        let mut store = SqliteStore::open(&path)?;
        store
            .run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
        seed_buy(&store, &path, &wallet)?;
        let signer = root.path().join("synthetic-key.json");
        std::fs::write(
            &signer,
            serde_json::to_vec(&fixture::key().to_keypair_bytes().to_vec())?,
        )?;
        let mut config = owner_buy_fixture::config(
            &wallet,
            &server.url,
            &root.path().join("stop").to_string_lossy(),
        );
        config.owner_technical_buy = None;
        config.execution_signer_keypair_path = signer.to_string_lossy().into_owned();
        config.pretrade_min_sol_reserve = 0.161;
        config.quote_canary_sell_slippage_bps = 50;
        config.max_submit_attempts = 1;
        config.canary_entry_submit_enabled = false;
        config.tiny_experiment.id = Some("synthetic-owner-exit-run".into());
        config.tiny_experiment.activate = false;
        let now = Utc::now();
        config.owner_exit = Some(OwnerExitConfig {
            policy: OWNER_EXIT_V1.into(),
            activate: true,
            run_id: "synthetic-owner-exit-run".into(),
            intent_id: "synthetic-owner-exit-intent".into(),
            buy_order_id: OWNER_EXIT_BUY_ORDER.into(),
            buy_receipt_signature: BUY_SIG.into(),
            position_id: format!("exec-canary-pos:{OWNER_EXIT_BUY_ORDER}"),
            wallet_pubkey: wallet.clone(),
            signer_pubkey: wallet.clone(),
            genesis_hash: owner_buy_fixture::GENESIS.into(),
            mint: OWNER_EXIT_MINT.into(),
            amount_raw: 1_167_085,
            decimals: 6,
            route: "jupiter_swap_instructions".into(),
            activated_at: (now - Duration::seconds(5)).to_rfc3339(),
            expires_at: (now + Duration::minutes(15)).to_rfc3339(),
            max_priority_fee_lamports: 50_000,
            min_reserve_lamports: 160_200_031,
            max_slippage_bps: 50,
            max_daily_loss_lamports: 20_000_000,
        });
        copybot_config::validate_owner_exit(&config)?;
        let floor = crate::execution_native_floor_policy::reserve_lamports(
            config.pretrade_min_sol_reserve,
        )?;
        let (payload, signature) = fixture::signed_payload(floor)?;
        let adapter = Arc::new(owner_buy_fixture::SignedAdapter {
            config: config.clone(),
            payload,
            signature,
            fail_simulation: false,
            wrong_blueprint: false,
            simulation_time: None,
            kill_during_simulation: false,
            simulation_calls: Default::default(),
            signing_calls: Default::default(),
        });
        Ok(Self {
            root,
            path,
            store,
            config,
            server,
            adapter,
        })
    }
    async fn tick(
        &self,
        store: &SqliteStore,
        config: copybot_config::ExecutionConfig,
    ) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        let mut runner = crate::execution_canary::ExecutionCanaryRunner::new(config)
            .for_ingestion(
                &owner_buy_fixture::ingestion(),
                &self.path.to_string_lossy(),
            )?;
        runner.owner_exit_adapter = Some(self.adapter.clone());
        runner.process_tick(store, Utc::now()).await
    }
    fn order_id(&self) -> String {
        copybot_storage_core::owner_exit_order_id("synthetic-owner-exit-intent")
    }
}

#[tokio::test]
async fn daemon_owner_exit_confirms_exact_close_and_unknown_restart_never_resends() -> Result<()> {
    for unknown in [false, true] {
        let c = Case::new().await?;
        assert_eq!(c.server.calls("sendTransaction"), 0);
        if unknown {
            c.server.state.lock().unwrap().mode = "unknown";
        }
        let first = c.tick(&c.store, c.config.clone()).await?;
        let order=c.store.load_execution_canary_order(&c.order_id())?;
        let calls=c.server.state.lock().unwrap().calls.clone();
        assert_eq!(c.server.calls("sendTransaction"), 1, "{first:?} {order:?} {calls:?}");
        if unknown {
            assert!(!c.store.execution_canary_fill_exists(&c.order_id())?);
            let fee: (String, u64) = Connection::open(&c.path)?.query_row(
                "SELECT state,fee_bound FROM owner_exit_fee_reservations WHERE order_id=?1",
                [c.order_id()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!(fee, ("pending".into(), 5000));
            c.server.state.lock().unwrap().mode = "ok";
        }
        let reopened = SqliteStore::open(&c.path)?;
        let mut config = c.config.clone();
        config.owner_exit.as_mut().unwrap().activate = false;
        let second = c.tick(&reopened, config).await?;
        assert_eq!(c.server.calls("sendTransaction"), 1, "{second:?}");
        assert!(
            reopened.execution_canary_fill_exists(&c.order_id())?,
            "{second:?}"
        );
        let position: (String, String) = Connection::open(&c.path)?.query_row(
            "SELECT state,qty_raw FROM positions WHERE position_id=?1",
            [format!("exec-canary-pos:{OWNER_EXIT_BUY_ORDER}")],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(position, ("closed".into(), "0".into()));
        let buy_cash = reopened.load_receipt_cash_components(OWNER_EXIT_BUY_ORDER)?
            .expect("confirmed BUY cash classification");
        assert_eq!(buy_cash.swap_native_delta_lamports.as_deref(), Some("-10000000"));
        assert_eq!(buy_cash.transaction_fee_lamports.as_deref(), Some("5000"));
        assert_eq!(buy_cash.target_ata_rent_delta_lamports.as_deref(), Some("1488440"));
        assert_eq!(buy_cash.unclassified_native_delta_lamports.as_deref(), Some("0"));
        let sell_cash = reopened.load_receipt_cash_components(&c.order_id())?
            .expect("confirmed SELL cash classification");
        assert_eq!(sell_cash.swap_native_delta_lamports.as_deref(), Some("12000000"));
        assert_eq!(sell_cash.transaction_fee_lamports.as_deref(), Some("5000"));
        assert_eq!(sell_cash.priority_fee_lamports.as_deref(), Some("0"));
        assert_eq!(sell_cash.target_ata_rent_delta_lamports.as_deref(), Some("0"));
        assert_eq!(sell_cash.unclassified_native_delta_lamports.as_deref(), Some("0"));
        let cycle = reopened.load_receipt_trade_cycle(&c.order_id())?
            .expect("confirmed BUY to SELL accounting");
        assert_eq!(cycle.state, "closed");
        assert_eq!(cycle.economic_result_lamports.as_deref(), Some("1990000"));
        assert_eq!(cycle.wallet_cash_result_lamports.as_deref(), Some("501560"));
        assert_eq!(
            Connection::open(&c.path)?
                .query_row("SELECT count(*) FROM fills", [], |r| r.get::<_, u64>(0))?,
            2
        );
        assert_eq!(c.server.calls("sendTransaction"), 1);
        let _ = &c.root;
    }
    Ok(())
}
