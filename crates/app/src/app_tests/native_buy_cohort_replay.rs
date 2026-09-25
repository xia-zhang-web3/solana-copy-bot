use super::*;

pub(super) fn apply_sustained_capacity(app: &mut copybot_config::AppConfig) {
    let limits = app.ingestion.yellowstone_association.as_mut().unwrap();
    limits.blocks.count = 192;
    limits.blocks.bytes = 512 << 20;
    limits.metadata_bytes = 96 << 20;
    limits.inbox.count = 500_000;
    limits.inbox.bytes = 512 << 20;
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

pub(super) async fn replay_cohort_sell_after_buy(
    case: &super::super::native_buy_runner_tests::Case,
    sustained: bool,
) -> Result<()> {
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
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut ingestion = IngestionService::with_replay_scoped(
        &app,
        receiver,
        "cohort-source-sell".into(),
        Some(scope),
    )?;
    let mut consumer = crate::association_consumer::AssociationConsumer::start_with_execution(
        &mut ingestion,
        &app.ingestion,
        &app.execution,
        &case.path,
    )
    .await?
    .context("cohort ingress consumer")?;
    let inbox_limits = copybot_storage_core::association_inbox::InboxLimits {
        count: if sustained { 500_000 } else { 20_000 },
        bytes: if sustained { 512 << 20 } else { 128 << 20 },
        busy_ms: 100,
    };
    let before_usage =
        copybot_storage_core::association_inbox::AssociationInbox::open_ordered_sell_consumer(
            &case.path,
            inbox_limits,
        )?
        .usage()?;
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
    let source_signature_for_replay = source_signature.clone();
    let bot_wallet = case.wallet.clone();
    let bot_signature = case.signature.clone();
    let old_wallet = old_wallet.to_owned();
    let old_signature = old_signature.to_owned();
    let producer = tokio::spawn(async move {
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
            sustained_load::send(
                &sender,
                &input,
                &old_wallet,
                &old_signature,
                &bot_wallet,
                &bot_signature,
            )
            .await?;
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
    });
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
    tokio::time::timeout(
        Duration::from_secs(if sustained { 900 } else { 10 }),
        async {
            loop {
                match consumer
                    .poll_with_transport(&case.store, Some(&mut rpc))
                    .await
                {
                    Ok(()) => {}
                    Err(error) if error.to_string() == "association delivery stopped" => break,
                    Err(error) => return Err(error),
                }
            }
            Ok::<(), anyhow::Error>(())
        },
    )
    .await??;
    producer.await??;
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
    assert_eq!(foreign, 0, "foreign swaps reached durable identity storage");
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
    Ok(())
}
