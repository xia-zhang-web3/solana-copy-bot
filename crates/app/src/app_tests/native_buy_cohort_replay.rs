use super::*;

pub(super) async fn hold_sell_under_ingress(
    server: &Server,
    case: &super::super::native_buy_runner_tests::Case,
    hold: Option<(tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>)>,
) -> Result<()> {
    if let Some((reached, release)) = hold {
        reached.await.context("SELL simulation never reached")?;
        let (blockhash_reached, blockhash_release) = server.hold("isBlockhashValid");
        let monitor = Connection::open(&case.path)?;
        let parents = || -> Result<i64> {
            Ok(monitor.query_row("SELECT count(*) FROM association_parent_blocks", [], |r| r.get(0))?)
        };
        let before_simulation = parents()?;
        wait_parent_commit(&parents, before_simulation, "SELL simulation").await?;
        release.send(()).map_err(|_| anyhow::anyhow!("SELL simulation release lost"))?;
        blockhash_reached.await.context("SELL blockhash check never reached")?;
        let before_blockhash = parents()?;
        wait_parent_commit(&parents, before_blockhash, "SELL blockhash").await?;
        blockhash_release.send(()).map_err(|_| anyhow::anyhow!("SELL blockhash release lost"))?;
    }
    Ok(())
}
async fn wait_parent_commit(parents: &impl Fn() -> Result<i64>, before: i64, stage: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if parents()? > before { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.with_context(|| format!("no committed parent during {stage}"))?
}

pub(super) fn apply_sustained_capacity(app: &mut copybot_config::AppConfig) {
    let limits = app.ingestion.yellowstone_association.as_mut().unwrap();
    limits.blocks.count = 192;
    limits.blocks.bytes = 512 << 20;
    limits.metadata_bytes = 96 << 20;
    limits.inbox.count = 500_000;
    limits.inbox.bytes = 512 << 20;
    limits.sqlite_busy_ms = 5_000;
}
pub(super) fn sell_quote_window(db: &Connection) -> Result<(i64, i64)> {
    let (quote, deadline): (String, String) = db.query_row(
        "SELECT quote,deadline FROM rpc_owned_sell_handoffs LIMIT 1", [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let quote: copybot_storage_core::ordered_sell_quote::QuoteObservation = serde_json::from_str(&quote)?;
    let started = quote.http_started.context("SELL quote start")?;
    let deadline = chrono::DateTime::parse_from_rfc3339(&deadline)?.with_timezone(&chrono::Utc);
    let consumed: String = db.query_row(
        "SELECT consumed_at FROM rpc_owned_sell_dispatches LIMIT 1", [], |r| r.get(0),
    )?;
    let consumed = chrono::DateTime::parse_from_rfc3339(&consumed)?.with_timezone(&chrono::Utc);
    Ok(((consumed - started).num_milliseconds(), (deadline - consumed).num_milliseconds()))
}
pub(super) fn assert_long_snapshot_binding(db: &Connection) -> Result<()> {
    let wire: String = db.query_row(
        "SELECT binding FROM ordered_sell_quote_results LIMIT 1", [], |r| r.get(0),
    )?;
    let binding: Value = serde_json::from_str(&wire)?;
    anyhow::ensure!(binding["snapshot_version"].as_str().is_some_and(
        |v| v.starts_with("strict_quote_snapshot_v2:sha256:") && v.len() == 96,
    ), "long parent snapshot digest missing");
    Ok(())
}

fn replace_equal(mut wire: Vec<u8>, old: &[u8], new: &[u8]) -> Result<Vec<u8>> {
    anyhow::ensure!(old.len() == new.len(), "fixture replacement length");
    let mut count = 0;
    for at in 0..=wire.len() - old.len() {
        if &wire[at..at + old.len()] == old {
            wire[at..at + old.len()].copy_from_slice(new);
            count += 1;
        }
    }
    anyhow::ensure!(count > 0, "fixture replacement missing");
    Ok(wire)
}

fn rewrite_identity(
    wire: Vec<u8>,
    old_wallet: &str,
    wallet: &str,
    old_signature: &str,
    signature: &str,
) -> Result<Vec<u8>> {
    let old_raw_wallet = bs58::decode(old_wallet).into_vec()?;
    let raw_wallet = bs58::decode(wallet).into_vec()?;
    let old_raw_signature = bs58::decode(old_signature).into_vec()?;
    let raw_signature = bs58::decode(signature).into_vec()?;
    let wire = replace_equal(wire, &old_raw_wallet, &raw_wallet)?;
    let wire = replace_equal(wire, old_wallet.as_bytes(), wallet.as_bytes())?;
    replace_equal(wire, &old_raw_signature, &raw_signature)
}

#[path = "native_buy_cohort_sustained.rs"]
pub(super) mod sustained_load;

pub(super) async fn replay_cohort_sell_after_buy<F>(
    case: &super::super::native_buy_runner_tests::Case,
    sustained: bool,
    on_sell: F,
) -> Result<String>
where
    F: std::future::Future<Output = Result<String>>,
{
    cohort_sell_after_buy(case, sustained, false, on_sell).await
}

pub(super) async fn transport_cohort_sell_after_buy<F>(
    case: &super::super::native_buy_runner_tests::Case,
    on_sell: F,
) -> Result<String>
where
    F: std::future::Future<Output = Result<String>>,
{
    cohort_sell_after_buy(case, false, true, on_sell).await
}

async fn cohort_sell_after_buy<F>(
    case: &super::super::native_buy_runner_tests::Case,
    sustained: bool,
    actual_transport: bool,
    on_sell: F,
) -> Result<String>
where
    F: std::future::Future<Output = Result<String>>,
{
    let input = crate::app_tests::b136_fixture::inputs();
    let chain: Value = serde_json::from_slice(&std::fs::read(input.join("chain.json"))?)?;
    let mut app = super::super::association_fixture::config(&chain);
    app.execution = case.config.clone();
    if sustained {
        apply_sustained_capacity(&mut app);
    }
    copybot_config::validate_association_delivery(&app)?;
    let authority = crate::execution_technical_cohort::authority(&case.config)?
        .context("active test cohort")?;
    let scope = crate::execution_technical_cohort::admission_wallets(&authority, &case.config)?;
    let inbox_limits = copybot_storage_core::association_inbox::InboxLimits {
        count: if sustained { 500_000 } else { 20_000 },
        bytes: if sustained { 512 << 20 } else { 128 << 20 },
        busy_ms: if sustained { 5_000 } else { 100 },
    };
    let source_wallet = chain["source"]["signer"]
        .as_str()
        .context("source wallet")?
        .to_owned();
    let source_signature = chain["source"]["signature"]
        .as_str()
        .context("source signature")?
        .to_owned();
    let old_wallet = chain["our"]["signer"]
        .as_str()
        .context("fixture bot wallet")?;
    let old_signature = chain["our"]["signature"]
        .as_str()
        .context("fixture bot signature")?;
    let source = std::fs::read(input.join("source.pb"))?;
    let our = rewrite_identity(
        std::fs::read(input.join("cohort-our.pb"))?,
        old_wallet,
        &case.wallet,
        old_signature,
        &case.signature,
    )?;
    let block_our = rewrite_identity(
        std::fs::read(input.join("cohort-block-120.pb"))?,
        old_wallet,
        &case.wallet,
        old_signature,
        &case.signature,
    )?;
    let frames = [
        source.clone(),
        our,
        std::fs::read(input.join("sell.pb"))?,
        std::fs::read(input.join("block-100.pb"))?,
        block_our,
        std::fs::read(input.join("block-150.pb"))?,
    ];
    if actual_transport {
        let directory = std::path::PathBuf::from(std::env::var("COHORT_TRANSPORT_DIR")?);
        std::fs::create_dir_all(&directory)?;
        let prior_signature = bs58::encode([92_u8; 64]).into_string();
        let prior = rewrite_identity(
            source.clone(), &source_wallet, &source_wallet,
            &source_signature, &prior_signature,
        )?;
        for (name, bytes) in [
            ("pre-source.pb", prior), ("source.pb", frames[0].clone()),
            ("cohort-our.pb", frames[1].clone()),
            ("sell.pb", frames[2].clone()),
            ("block-100.pb", frames[3].clone()),
            ("cohort-block-120.pb", frames[4].clone()),
            ("block-150.pb", frames[5].clone()),
        ] {
            std::fs::write(directory.join(name), bytes)?;
        }
        std::fs::write(directory.join("streams-ready"), b"ready")?;
        let relay_url = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(url) = std::fs::read_to_string(directory.join("relay-url.txt")) {
                    break Ok::<_, anyhow::Error>(url);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await??;
        app.ingestion.yellowstone_grpc_url = relay_url.trim().into();
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut ingestion = if actual_transport {
        IngestionService::build_for_app(&app)?
    } else {
        IngestionService::with_replay_scoped(
            &app, receiver, "cohort-source-sell".into(), Some(scope),
        )?
    };
    let mut consumer = crate::association_consumer::AssociationConsumer::start_with_execution(
        &mut ingestion, &app.ingestion, &app.execution, &case.path,
    ).await?.context("cohort ingress consumer")?;
    let before_usage =
        copybot_storage_core::association_inbox::AssociationInbox::open_ordered_sell_consumer(
            &case.path, inbox_limits,
        )?.usage()?;
    let source_signature_for_replay = source_signature.clone();
    let bot_wallet = case.wallet.clone();
    let bot_signature = case.signature.clone();
    let old_wallet = old_wallet.to_owned();
    let old_signature = old_signature.to_owned();
    let (settled_tx, settled_rx) = tokio::sync::oneshot::channel();
    let producer = (!actual_transport).then(|| tokio::spawn(async move {
        for n in 0..742u16 {
            let foreign_wallet = bs58::encode([2u8; 32]).into_string();
            let mut raw_signature = [91u8; 64];
            raw_signature[..2].copy_from_slice(&n.to_le_bytes());
            let foreign_signature = bs58::encode(raw_signature).into_string();
            let payload = rewrite_identity(
                source.clone(),
                &source_wallet,
                &foreign_wallet,
                &source_signature_for_replay,
                &foreign_signature,
            )?;
            sender
                .send(ReplayInput::Update {
                    offset_ns: u64::from(n) + 1,
                    payload,
                })
                .await?;
        }
        if sustained {
            let tail = sustained_load::send(
                &sender,
                &input,
                &old_wallet,
                &old_signature,
                &bot_wallet,
                &bot_signature,
                settled_rx,
            )
            .await?;
            anyhow::ensure!(tail >= 2, "parent writer did not overlap daemon SELL");
        } else {
            for (n, payload) in frames.into_iter().enumerate() {
                sender
                    .send(ReplayInput::Update {
                        offset_ns: 743 + n as u64,
                        payload,
                    })
                    .await?;
            }
            sender.send(ReplayInput::End(750)).await?;
        }
        Ok::<(), anyhow::Error>(())
    }));
    let mut rpc = crate::execution_owned_sell_rpc::fractional::transport::Parsed(
        |request: Value| async move {
            let result = match request["method"].as_str() {
                Some("getGenesisHash") => json!("11111111111111111111111111111111"),
                Some("getSlot") => json!(99),
                other => anyhow::bail!("unexpected cohort fence method: {other:?}"),
            };
            Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
        },
    );
    let (completed_tx, mut completed_rx) = tokio::sync::oneshot::channel();
    let consume = tokio::time::timeout(
        Duration::from_secs(if sustained { 900 } else { 10 }),
        async {
            loop {
                let poll_started = std::time::Instant::now();
                let result = if actual_transport {
                    tokio::select! {
                        _ = &mut completed_rx => break,
                        result = consumer.poll_with_transport(&case.store, Some(&mut rpc)) => result,
                    }
                } else {
                    consumer.poll_with_transport(&case.store, Some(&mut rpc)).await
                };
                if sustained && poll_started.elapsed() > Duration::from_secs(2) {
                    eprintln!("sustained ingress poll elapsed_ms={}", poll_started.elapsed().as_millis());
                }
                match result {
                    Ok(()) => {}
                    Err(error) if error.to_string() == "association delivery stopped" => break,
                    Err(error) => return Err(error),
                }
            }
            Ok::<(), anyhow::Error>(())
        },
    );
    let settle = async {
        let id = on_sell.await?;
        let _ = settled_tx.send(());
        let _ = completed_tx.send(());
        Ok::<String, anyhow::Error>(id)
    };
    let (_, sell_id) = tokio::try_join!(async { consume.await??; Ok::<(), anyhow::Error>(()) }, settle)?;
    if let Some(producer) = producer { producer.await??; }
    if actual_transport {
        let (wire, persisted) = consumer.diagnostic_snapshots();
        anyhow::ensure!(wire.reconnects >= 1
            && wire.reconnect_stages[copybot_ingestion::TransportStage::Stream as usize] >= 1
            && wire.reconnect_classes[copybot_ingestion::TransportClass::Unavailable as usize] >= 1,
            "transport reconnect cause missing: {wire:?}");
        anyhow::ensure!(wire.selected_source >= 2 && wire.selected_bot >= 1
            && wire.admissions >= 3,
            "selected source/bot admissions missing: {wire:?}");
        anyhow::ensure!(persisted.session_gaps >= 1 && persisted.admissions >= 3
            && persisted.last_parent_slot == Some(150),
            "consumer lost gap or current parent: {persisted:?}");
        anyhow::ensure!(wire.queue_wait_over_100ms == 0,
            "fixture queue fell behind consumer: {wire:?}");
    }
    let db = Connection::open(&case.path)?;
    let foreign: i64 = db.query_row(
        "SELECT count(*) FROM association_inbox_identities WHERE signature NOT IN (?1,?2,?3)",
        rusqlite::params![
            source_signature,
            case.signature,
            chain["sell"]["signature"].as_str()
        ],
        |r| r.get(0),
    )?;
    assert_eq!(foreign, i64::from(actual_transport),
        "unexpected identities beyond the deliberate pre-gap source");
    let inbox =
        copybot_storage_core::association_inbox::AssociationInbox::open_ordered_sell_consumer(
            &case.path,
            inbox_limits,
        )?;
    let after_usage = inbox.usage()?;
    assert!(
        after_usage.0 <= before_usage.0 + if sustained { 200_000 } else { 100 }
            && after_usage.1 <= before_usage.1 + (200 << 20),
        "742 foreign swaps grew the logical inbox: {before_usage:?} -> {after_usage:?}"
    );
    if sustained {
        // Forty thousand is not needed: at one block per 400 ms, the entire
        // immutable 14,400 s window contains 36,000 blocks. Scale every
        // metered domain in this causal fixture, including SELL dependencies.
        let scale = 36_000_u128 / sustained_load::FILLER_COUNT as u128;
        let projected_rows =
            before_usage.0 as u128 + (after_usage.0 - before_usage.0) as u128 * scale + 1_024;
        let projected_bytes =
            before_usage.1 as u128 + (after_usage.1 - before_usage.1) as u128 * scale + (16 << 20);
        eprintln!("sustained logical_usage={after_usage:?} projected_4h_rows={projected_rows} projected_4h_bytes={projected_bytes}");
        assert!(
            projected_rows < 500_000 * 3 / 4 && projected_bytes < (512_u128 << 20) * 3 / 4,
            "four-hour logical meter projection: rows={projected_rows} bytes={projected_bytes}"
        );
        // Two long parent relations (receipt->SELL and chain->SELL) charge
        // four reader units per edge; this bound is separate from stored rows.
        assert!(2 * 4 * 36_000 + 1_024 < 500_000);
        assert!(2_u128 * 4_096 * 36_000 + (16 << 20) < 512_u128 << 20);
    }
    let preparation = inbox.sell_preparation(chain["sell"]["signature"].as_str().unwrap())?;
    assert!(
        preparation.is_some(),
        "streamed source SELL lacks preparation"
    );
    Ok(sell_id)
}
