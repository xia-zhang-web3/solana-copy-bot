//! Actual app select/handler/writer regression. All times/blocks are LOCAL fixtures.
use super::*;
use super::{b70_hooks, r1_protected_transport_fixture::Server};
use copybot_config::{
    AssociationDeliveryConfig, DeliveryBudget, HttpRecoveryConfig, ProtectedRecoveryConfig,
};
use prost::Message;
use std::sync::atomic::Ordering;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateBlock},
    prost_types::Timestamp,
};

fn key(value: u8) -> String {
    bs58::encode([value; 32]).into_string()
}

fn live_inputs() -> Result<(Vec<SubscribeUpdate>, Vec<String>)> {
    let now = Utc::now();
    let mut messages = vec![];
    let mut signatures = vec![];
    for (n, raw) in [
        include_bytes!("../../tests/fixtures/capture/quote_sol_wallet_06_sell.pb").as_slice(),
        include_bytes!("../../tests/fixtures/capture/quote_sol_wallet_11_buy.pb").as_slice(),
    ]
    .into_iter()
    .enumerate()
    {
        let mut update = SubscribeUpdate::decode(raw)?;
        update.created_at = Some(Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32 + n as i32,
        });
        let Some(UpdateOneof::Transaction(tx)) = &update.update_oneof else {
            anyhow::bail!("saved transaction fixture");
        };
        signatures.push(bs58::encode(&tx.transaction.as_ref().unwrap().signature).into_string());
        messages.push(update);
    }
    let first_slot = match messages[0].update_oneof.as_ref().unwrap() {
        UpdateOneof::Transaction(t) => t.slot,
        _ => unreachable!(),
    };
    messages.insert(
        0,
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Block(SubscribeUpdateBlock {
                slot: first_slot - 1,
                parent_slot: first_slot - 2,
                blockhash: key(11),
                parent_blockhash: key(10),
                executed_transaction_count: 0,
                ..Default::default()
            })),
            ..Default::default()
        },
    );
    Ok((messages, signatures))
}

fn ingestion(path: &Path, checkpoint: &Path, url: &str) -> Result<IngestionConfig> {
    let db = Connection::open(path)?;
    db.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    db.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,10000,67108864)",
        [],
    )?;
    let mut c = IngestionConfig::default();
    c.source = "yellowstone_grpc".into();
    c.yellowstone_grpc_url = url.into();
    c.yellowstone_x_token = "LOCAL-bootstrap-fixture".into();
    c.yellowstone_delivery_mode = "durable_association_v1".into();
    c.capture_scope_db = Some(path.to_string_lossy().into());
    c.capture_recovery = Some(ProtectedRecoveryConfig {
        policy: "protected_complete_recovery_v1".into(),
        checkpoint_directory: checkpoint.to_string_lossy().into(),
        migrations_directory: Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../migrations")
            .canonicalize()?
            .to_string_lossy()
            .into(),
        max_scope_wallets: 128,
        max_scope_revisions: 16,
        provenance_policy: "historical_missed_no_execution_v1".into(),
    });
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(32, 8 << 20),
        blocks: b(32, 16 << 20),
        history: b(64, 8 << 20),
        outputs: b(32, 8 << 20),
        queue: b(32, 16 << 20),
        inbox: b(10000, 16 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 8 << 20,
        pending_ttl_ms: 60000,
        block_ttl_ms: 60000,
        history_ttl_ms: 120000,
        tick_ms: 1000,
        sqlite_busy_ms: 100,
    });
    c.yellowstone_http_recovery = Some(HttpRecoveryConfig {
        broker_url: "http://127.0.0.1:1".into(),
        broker_token: String::new(),
        range_slots: 1024,
        max_response_bytes: 8 << 20,
        timeout_ms: 3000,
        fetch_concurrency: 1,
        raw_window_blocks: Some(1),
        anchor_evidence_dir: None,
    });
    Ok(c)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_protected_empty_scope_live_bootstrap_reaches_main_discovery_input() -> Result<()> {
    let _serial = b70_hooks::acquire().await;
    let (store, path) = make_test_store("r1-empty-protected-bootstrap")?;
    let owned = path.with_extension("bootstrap-owned");
    std::fs::create_dir(&owned)?;
    let owned = owned.canonicalize()?;
    let capture_path = owned.join("capture.db");
    let checkpoint = owned.join("checkpoints");
    std::fs::create_dir(&checkpoint)?;
    let (messages, signatures) = live_inputs()?;
    let server = Server::start(messages).await?;
    let ingestion = ingestion(&capture_path, &checkpoint, &server.url)?;
    let mut config = copybot_config::AppConfig::default();
    config.ingestion = ingestion.clone();
    config.execution.enabled = false;
    config.execution.canary_tiny_submit_enabled = false;
    config.execution.canary_entry_submit_enabled = false;
    config.execution.canary_enabled = false;
    config.execution.quote_canary_enabled = false;
    copybot_config::validate_association_delivery(&config)?;
    let sql = Connection::open(&path)?;
    assert_eq!(
        sql.query_row("SELECT count(*) FROM followlist WHERE active=1", [], |r| {
            r.get::<_, i64>(0)
        })?,
        0
    );
    assert_eq!(
        sql.query_row("SELECT count(*) FROM observed_swaps", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    let quality = permissive_shadow_quality();
    let discovery =
        DiscoveryService::new(copybot_config::DiscoveryConfig::default(), quality.clone());
    let shadow = ShadowService::new(quality);
    let mut risk = RiskConfig::default();
    risk.shadow_killswitch_enabled = false;
    let mut shadow_config = ShadowConfig::default();
    shadow_config.restart_recovery_enabled = false;
    let mut journal = copybot_config::RecentRawJournalConfig::default();
    journal.path = owned.join("recent-raw.db").to_string_lossy().into();
    let service = IngestionService::build_for_app(&config)?;
    let (installed, _unused_legacy_sender) = b70_hooks::Installed::new();
    let h = &installed.0;
    let daemon = crate::app_loop::run_app_loop(
        store,
        service,
        discovery,
        shadow,
        config.execution,
        risk,
        ingestion,
        shadow_config,
        path.to_string_lossy().into(),
        3600,
        copybot_config::HistoryRetentionConfig::default(),
        journal,
        3600,
        3600,
        30,
        "yellowstone_grpc".into(),
        3600,
        false,
        0,
        false,
        None,
    );
    let controller = async {
        let result:Result<()> = async {
            tokio::time::timeout(StdDuration::from_secs(5),async {
                loop {
                    let found:i64=sql.query_row("SELECT count(*) FROM observed_swaps WHERE signature IN (?1,?2)",[&signatures[0],&signatures[1]],|r|r.get(0))?;
                    if found==2 {break Ok::<_,anyhow::Error>(());}
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                }
            }).await.context("actual app dispatch failed: ordinary LIVE rows absent from main Discovery input")??;
            let independent=SqliteStore::open(&path)?;
            let raw=independent.load_observed_swaps_since(Utc::now()-chrono::Duration::minutes(1))?;
            for signature in &signatures {assert!(raw.iter().any(|row| &row.signature==signature));}
            let capture=Connection::open(&capture_path)?;
            for table in ["capture_requests","capture_members","capture_events","capture_obligations","capture_recovery","capture_recovery_outbox"] {
                assert_eq!(capture.query_row(&format!("SELECT count(*) FROM {table}"),[],|r|r.get::<_,i64>(0))?,0,"outsider must not receive protected admission: {table}");
            }
            assert_eq!(server.subscriptions.load(Ordering::SeqCst),1,"no independent paid subscription");
            assert_eq!(sql.query_row("SELECT count(*) FROM followlist WHERE active=1",[],|r|r.get::<_,i64>(0))?,0,"Discovery not replaced with fixed wallets");
            println!("R1_ACTUAL_APP_BOOTSTRAP main_discovery_rows=2 protected_admissions=0 initial_scope=empty subscriptions=1 provider_calls=0 signatures={signatures:?}");
            Ok(())
        }.await;
        h.stop();
        result
    };
    let (daemon_result, controller_result) =
        tokio::time::timeout(StdDuration::from_secs(12), async {
            tokio::join!(daemon, controller)
        })
        .await?;
    daemon_result?;
    controller_result?;
    assert_eq!(h.count("checked_shutdown", ""), 1);
    drop(installed);
    drop(sql);
    drop(server);
    std::fs::remove_dir_all(owned)?;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    Ok(())
}
