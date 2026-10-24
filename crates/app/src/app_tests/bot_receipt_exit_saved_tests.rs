//! Actual stopped COPY12 copy; local quote/RPC only, simulation refuses before signing.
use super::{copy_cycle_peer_fixture::Peer, owner_buy_fixture::SignedAdapter};
use anyhow::{ensure, Context, Result};
use chrono::{Duration, Utc};
use copybot_config::{ExecutionConfig, OwnerExitConfig, OWNER_BOT_RECEIPT_EXIT_V1};
use copybot_storage_core::{OwnerExitIntent, SqliteStore, OWNER_EXIT_MINT};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{atomic::Ordering, Arc, Mutex},
};

fn digest(p: &std::path::Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(std::fs::read(p)?)))
}

fn scope_rows(c: &Connection) -> Result<Vec<String>> {
    ["native_buy_technical_cohort", "execution_tiny_experiment"]
        .iter()
        .map(|table| {
            let mut st = c.prepare(&format!("SELECT * FROM {table}"))?;
            let count = st.column_count();
            let row = st.query_row([], |r| {
                (0..count)
                    .map(|n| r.get_ref(n).map(|v| format!("{v:?}")))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?;
            Ok(format!("{table}:{row:?}"))
        })
        .collect()
}

fn configure(c: &mut ExecutionConfig, i: &OwnerExitIntent, url: &str, stop: &str) {
    c.technical_cohort.as_mut().unwrap().activate = false;
    c.tiny_experiment.activate = false;
    c.native_fresh_buy = None;
    c.canary_entry_submit_enabled = false;
    c.canary_kill_switch_path = stop.into();
    c.execution_signer_keypair_path = "LOCAL-ABSENT-KEY-MUST-NEVER-OPEN".into();
    c.submit_adapter_http_url = url.into();
    c.owned_sell_preparation.as_mut().unwrap().rpc_url = url.into();
    c.quote_canary_base_url = format!("{url}/swap/v1");
    c.quote_canary_api_key = "LOCAL-MODEL-ONLY".into();
    c.priority_fee_canary_enabled = false;
    c.submit_timeout_ms = 1000;
    c.owner_exit = Some(OwnerExitConfig {
        source_recovery_parent_intent_id: None,
        policy: OWNER_BOT_RECEIPT_EXIT_V1.into(),
        activate: true,
        run_id: i.run_id.clone(),
        intent_id: i.intent_id.clone(),
        buy_order_id: i.buy_order_id.clone(),
        buy_receipt_signature: i.buy_receipt_signature.clone(),
        position_id: i.position_id.clone(),
        wallet_pubkey: i.wallet.clone(),
        signer_pubkey: i.signer.clone(),
        genesis_hash: i.genesis_hash.clone(),
        mint: i.mint.clone(),
        amount_raw: i.amount_raw,
        decimals: i.decimals,
        route: i.route.clone(),
        activated_at: i.activated_at.to_rfc3339(),
        expires_at: i.expires_at.to_rfc3339(),
        max_priority_fee_lamports: i.max_priority_fee_lamports,
        min_reserve_lamports: 0,
        max_slippage_bps: u64::from(i.max_slippage_bps),
        max_daily_loss_lamports: i.max_daily_loss_lamports,
    });
}

#[tokio::test]
#[ignore = "requires byte-pinned private COPY12 stopped BOT snapshot/config"]
async fn bot_receipt_exit_saved_expired_native_lot_reaches_production_simulation_once() -> Result<()>
{
    let snapshot = PathBuf::from(std::env::var("BOT_RECEIPT_EXIT_SNAPSHOT")?);
    let expected = "7858b29da4e71ddfa98086ba2dde9d465fc4bff474f5c711c5e2924064a6c66d";
    ensure!(
        digest(&snapshot)? == expected,
        "actual COPY12 snapshot changed"
    );
    let root = super::temporary_output_fixture::OutputRoot::new("bot-receipt-exit-actual")?;
    let path = root.path().join("LOCAL-bot-copy.db");
    std::fs::copy(&snapshot, &path)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let mut store = SqliteStore::open(&path)?;
    let sql = Connection::open(&path)?;
    let preserved = scope_rows(&sql)?;
    let expired: String = sql.query_row(
        "SELECT deadline FROM native_buy_technical_cohort",
        [],
        |r| r.get(0),
    )?;
    let now = Utc::now();
    ensure!(chrono::DateTime::parse_from_rfc3339(&expired)? < now);
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let (buy, sig, position, wallet, genesis, run, slot): (String,String,String,String,String,String,u64) = sql.query_row(
        "SELECT o.order_id,o.tx_signature,p.position_id,f.wallet_pubkey,n.genesis_hash,c.run_id,CAST(f.slot AS INTEGER)
         FROM orders o JOIN fills v ON v.order_id=o.order_id
         JOIN positions p ON p.position_id=v.position_id
         JOIN execution_canary_receipt_facts f ON f.order_id=o.order_id
         JOIN native_buy_decisions d ON d.signal_id=o.signal_id
         JOIN native_buy_fences n ON n.session=d.first_session
         CROSS JOIN native_buy_technical_cohort c WHERE f.side='buy'",
        [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
    let intent = OwnerExitIntent {
        intent_id: format!(
            "bot-receipt-exit:{}",
            buy.strip_prefix("exec-canary:native-buy-v1:")
                .context("native origin")?
        ),
        run_id: run,
        buy_order_id: buy.clone(),
        buy_receipt_signature: sig,
        position_id: position,
        wallet: wallet.clone(),
        signer: wallet.clone(),
        genesis_hash: genesis.clone(),
        mint: OWNER_EXIT_MINT.into(),
        amount_raw: 988620,
        decimals: 6,
        route: "jupiter_swap_instructions".into(),
        activated_at: now - Duration::seconds(1),
        expires_at: now + Duration::seconds(120),
        authority_sha256: "a".repeat(64),
        max_priority_fee_lamports: 22_000,
        min_reserve_lamports: 0,
        max_slippage_bps: 50,
        max_daily_loss_lamports: 20_000_000,
    };
    let mut legacy = intent.clone();
    legacy.intent_id = "source-recovery:unsupported-native-BOT".into();
    let old_refusal = format!("{:#}", store.register_owner_exit_intent(&legacy).unwrap_err());
    ensure!(
        old_refusal.contains("owner_exit_intent_identity_or_limits"),
        "{old_refusal}"
    );
    let key = crate::execution_pumpswap_accounts::parse_pubkey(&wallet, "saved BOT")?;
    let peer = Peer::new(key, 988620, slot, Arc::new(Mutex::new(BTreeMap::new()))).await?;
    {
        let mut s = peer.state.lock().unwrap();
        s.genesis = genesis;
        let mint = crate::execution_pumpswap_accounts::parse_pubkey(OWNER_EXIT_MINT, "saved USDC")?;
        let ata = crate::execution_pumpswap_accounts::associated_token_address(
            &key,
            &mint,
            &crate::execution_pumpswap_accounts::token_program_id(),
        );
        s.bank
            .accounts
            .get_mut(&ata)
            .unwrap()
            .token
            .as_mut()
            .unwrap()
            .2 = 988720;
    }
    let config_path = PathBuf::from(std::env::var("BOT_RECEIPT_EXIT_CONFIG")?);
    let mut cfg = copybot_config::load_from_path(config_path)?.execution;
    configure(
        &mut cfg,
        &intent,
        &peer.url,
        &root.path().join("STOP").to_string_lossy(),
    );
    copybot_config::validate_owner_exit(&cfg)?;
    copybot_config::validate_technical_cohort(&cfg)?;
    let intent = crate::execution_owner_exit_authority::configured(&cfg)?.unwrap();
    let adapter = SignedAdapter {
        config: cfg.clone(),
        payload: String::new(),
        signature: String::new(),
        fail_simulation: true,
        wrong_blueprint: false,
        simulation_time: None,
        kill_during_simulation: false,
        simulation_calls: Default::default(),
        signing_calls: Default::default(),
    };
    let first = Box::pin(crate::execution_owner_exit::tick_with_adapter(&cfg, &store, now, &adapter)).await?;
    peer.healthy()?;
    ensure!(
        adapter.simulation_calls.load(Ordering::SeqCst) == 1,
        "{first:?}"
    );
    ensure!(adapter.signing_calls.load(Ordering::SeqCst) == 0 && peer.sends() == 0);
    let proof = store.owner_bot_receipt_exit_proof(&intent)?;
    ensure!(proof.personal_raw == 100 && proof.expected_wallet_total_raw == 988720);
    let client = reqwest::Client::new();
    for wrong in [988620, 988719, 988721] {
        {
            let mut s = peer.state.lock().unwrap();
            let ata = crate::execution_pumpswap_accounts::parse_pubkey(&proof.token_account, "LOCAL token")?;
            s.bank.accounts.get_mut(&ata).unwrap().token.as_mut().unwrap().2 = wrong;
        }
        let refused = crate::execution_owner_exit_quote::verify_balance(
            &client, &cfg, &store, &intent,
            &|| crate::execution_owner_exit_authority::current(&cfg, &store, &intent, Utc::now()),
        ).await.unwrap_err();
        ensure!(refused.to_string().contains("bot_receipt_exit_wallet_inventory_changed"), "{refused:#}");
    }
    {
        let mut s = peer.state.lock().unwrap();
        let ata = crate::execution_pumpswap_accounts::parse_pubkey(&proof.token_account, "LOCAL token")?;
        s.bank.accounts.get_mut(&ata).unwrap().token.as_mut().unwrap().2 = proof.expected_wallet_total_raw;
    }
    let mut b = super::owner_roundtrip_fixture::Case::new().await?;
    b.config.prior_bot_receipt_exit = Some(copybot_config::PriorBotReceiptExitConfig {
        policy: copybot_config::PRIOR_BOT_RECEIPT_EXIT_V1.into(),
        state_db_path: path.to_string_lossy().into_owned(),
        exit_intent_id: intent.intent_id.clone(), buy_order_id: intent.buy_order_id.clone(),
        buy_receipt_signature: intent.buy_receipt_signature.clone(), position_id: intent.position_id.clone(),
        wallet_pubkey: intent.wallet.clone(), mint: intent.mint.clone(), amount_raw: intent.amount_raw, decimals: 6,
    });
    let held = Box::pin(b.tick()).await?;
    ensure!(held.state_machine_skipped_reason == Some("prior_bot_receipt_exit_pending"));
    ensure!(b.calls("quote") == 0 && b.calls("sendTransaction") == 0);
    ensure!(b.sql()?.query_row("SELECT count(*) FROM orders", [], |r| r.get::<_,u64>(0))? == 0);
    let reopened = SqliteStore::open(&path)?;
    let second = Box::pin(crate::execution_owner_exit::tick_with_adapter(
        &cfg, &reopened, Utc::now(), &adapter)).await?;
    ensure!(
        second.existing == 1
            && adapter.simulation_calls.load(Ordering::SeqCst) == 1
            && peer.sends() == 0
    );
    ensure!(
        scope_rows(&sql)? == preserved,
        "old cohort/tiny SQL must remain byte-identical"
    );
    ensure!(digest(&snapshot)? == expected);
    if let Ok(dir) = std::env::var("BOT_RECEIPT_EXIT_EVIDENCE") {
        let out = PathBuf::from(dir);
        std::fs::create_dir_all(&out)?;
        sql.execute(
            "VACUUM INTO ?1",
            [out.join("actual-copy12-local-proof.db")
                .to_string_lossy()
                .as_ref()],
        )?;
        std::fs::write(
            out.join("RESULT.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "kind":"ACTUAL_COPY12_NATIVE_RECEIPT_COPY_WITH_LOCAL_PROVIDER_SIMULATION_REFUSAL",
                "snapshot_sha256":expected,"expired_old_SQL_deadline":expired,"old_refusal":old_refusal,
                "new_intent":intent,"personal_raw":proof.personal_raw,"wallet_total_raw":proof.expected_wallet_total_raw,
                "simulation_calls":1,"signing_calls":0,"sends":0,"duplicate_after_restart_existing":true,
            "old_SQL_unchanged":true,"original_state_unchanged":true,
            "wrong_wallet_raw_rejected":[988620,988719,988721],"failed_A_new_B_BUY_orders":0,
                "mock_boundary":"LOCAL balance/quote and deliberately failing simulation; no actual signer/readiness/live completion"
            }))?,
        )?;
    }
    Ok(())
}
