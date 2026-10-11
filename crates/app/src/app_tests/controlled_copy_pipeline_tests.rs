//! Explicit mock bridge, not four confirmed live trades or live quote availability.
use super::{controlled_copy_pipeline_fixture as f, owner_source_gate_fixture::GateCase};
use crate::association_consumer::AssociationConsumer;
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use anyhow::{Context, Result};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::native_buy::SPL_TOKEN_PROGRAM;
use serde_json::{json, Value};

#[tokio::test]
async fn controlled_copy_current_usdc_frames_reach_native_graph_without_operator_bot_trade(
) -> Result<()> {
    let case = GateCase::new().await?;
    case.source.tick().await?;
    assert!(case
        .source
        .store
        .execution_canary_fill_exists(&case.source.order_id())?);
    case.assert_held()?;
    let receipt = case
        .source
        .store
        .load_execution_canary_receipt_facts(&case.source.order_id())?
        .unwrap();
    let c = f::config(&case)?;
    assert_ne!(c.execution.canary_wallet_pubkey, receipt.wallet_pubkey);
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let authority = crate::execution_technical_cohort::authority(&c.execution)?.unwrap();
    let scope = crate::execution_technical_cohort::admission_wallets(&authority, &c.execution)?;
    let mut ingestion = IngestionService::with_replay_scoped(
        &c,
        receiver,
        "controlled-current-model".into(),
        Some(scope),
    )?;
    let mut consumer = AssociationConsumer::start_with_execution(
        &mut ingestion,
        &c.ingestion,
        &c.execution,
        &case.path.to_string_lossy(),
    )
    .await?
    .context("normal daemon association consumer")?;
    let fence_slot = receipt.slot - 1;
    let mut rpc = Parsed(move |request: Value| async move {
        let result = match request["method"].as_str() {
            Some("getGenesisHash") => json!(super::owner_buy_fixture::GENESIS),
            Some("getSlot") => json!(fence_slot),
            other => anyhow::bail!("unexpected mock method {other:?}"),
        };
        Ok(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    });
    consumer
        .poll_with_transport(&case.bot, Some(&mut rpc))
        .await?;
    consumer.next_fence_at = Some(tokio::time::Instant::now());
    consumer
        .poll_with_transport(&case.bot, Some(&mut rpc))
        .await?;
    let mut buy_frames = f::frames(f::buy(
        &receipt.wallet_pubkey,
        &receipt.tx_signature,
        receipt.slot,
    ));
    buy_frames.insert(1, buy_frames[0].clone());
    for (n, payload) in buy_frames.into_iter().enumerate() {
        sender
            .send(ReplayInput::Update {
                offset_ns: n as u64 + 1,
                payload,
            })
            .await?;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while case.bot.list_native_buy_pending(1)?.is_empty() {
            consumer
                .poll_with_transport(&case.bot, Some(&mut rpc))
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let pending = case.bot.list_native_buy_pending(1)?.remove(0);
    assert_eq!(
        (
            &pending.signature,
            &pending.wallet,
            &pending.mint,
            pending.slot
        ),
        (
            &receipt.tx_signature,
            &receipt.wallet_pubkey,
            &f::USDC.to_owned(),
            receipt.slot
        )
    );
    let a = f::admission(&case, &receipt.tx_signature)?.unwrap();
    let exact = a.facts.exact_amounts.as_ref().unwrap();
    assert_eq!(
        (
            &exact.amount_in_raw,
            &exact.amount_out_raw,
            exact.amount_out_decimals
        ),
        (&"10000000".to_owned(), &f::RECEIVED.to_string(), 6)
    );
    assert!(case.bot.native_buy_record_finalized(
        &receipt.tx_signature,
        receipt.slot,
        SPL_TOKEN_PROGRAM,
        chrono::Utc::now()
    )?);
    let candidate = case
        .bot
        .native_buy_ready(&receipt.tx_signature, chrono::Utc::now(), 120)?
        .unwrap();
    assert_eq!(candidate.amount_lamports, 10_000_000);
    assert_eq!(
        case.sql()?
            .query_row("SELECT count(*) FROM native_buy_decisions", [], |r| r
                .get::<_, i64>(0))?,
        1
    );
    let signal = case
        .bot
        .load_copy_signal_by_signal_id(&candidate.signal_id)?
        .unwrap();
    assert_eq!(
        (&signal.wallet_id, &signal.token),
        (&receipt.wallet_pubkey, &f::USDC.to_owned())
    );
    // No BOT order/position is manufactured here. The accepted runner checks
    // consume this canonical signal; their external execution proof is separate.
    assert_eq!(
        case.sql()?
            .query_row("SELECT count(*) FROM orders", [], |r| r.get::<_, i64>(0))?,
        0
    );
    case.assert_held()?;
    let sell = f::sell(&receipt.wallet_pubkey, receipt.slot + 1);
    let sell_signature = bs58::encode(&sell.transaction.as_ref().unwrap().signature).into_string();
    for (n, payload) in f::frames(sell).into_iter().enumerate() {
        sender
            .send(ReplayInput::Update {
                offset_ns: n as u64 + 4,
                payload,
            })
            .await?;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while f::admission(&case, &sell_signature)?.is_none() {
            consumer
                .poll_with_transport(&case.bot, Some(&mut rpc))
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let a = f::admission(&case, &sell_signature)?.unwrap();
    assert_eq!(
        (&a.facts.wallet, &a.facts.token_in, &a.facts.token_out),
        (
            &receipt.wallet_pubkey,
            &f::USDC.to_owned(),
            &super::owner_buy_wire_fixture::quote()["inputMint"]
                .as_str()
                .unwrap()
                .to_owned()
        )
    );
    let exact = a.facts.exact_amounts.unwrap();
    assert_eq!(
        (
            exact.amount_in_raw,
            exact.amount_out_raw,
            exact.amount_in_decimals,
            exact.amount_out_decimals
        ),
        (f::RECEIVED.to_string(), f::SOLD_SOL.to_string(), 6, 9)
    );
    // Premature SOURCE SELL is observation-only with no canonical BOT BUY receipt.
    assert_eq!(
        case.sql()?
            .query_row("SELECT count(*) FROM orders", [], |r| r.get::<_, i64>(0))?,
        0
    );
    case.assert_held()?;
    assert_eq!(case.source.calls("sendTransaction"), 1);
    Ok(())
}

#[tokio::test]
async fn controlled_copy_current_scope_old_foreign_and_premature_sell_create_no_bot_trade(
) -> Result<()> {
    for kind in ["old_buy", "foreign_buy", "premature_sell"] {
        let case = GateCase::new().await?;
        let source = &case
            .source
            .config
            .owner_technical_buy
            .as_ref()
            .unwrap()
            .wallet_pubkey;
        let signature = bs58::encode([41; 64]).into_string();
        let foreign = bs58::encode(
            ed25519_dalek::SigningKey::from_bytes(&[23; 32])
                .verifying_key()
                .to_bytes(),
        )
        .into_string();
        let frame = match kind {
            "old_buy" => f::buy(source, &signature, 118),
            "foreign_buy" => f::buy(&foreign, &signature, 120),
            "premature_sell" => f::sell(source, 120),
            _ => unreachable!(),
        };
        let id = bs58::encode(&frame.transaction.as_ref().unwrap().signature).into_string();
        f::negative_replay(&case, frame).await?;
        assert!(case.bot.list_native_buy_pending(1)?.is_empty(), "{kind}");
        assert_eq!(
            case.sql()?
                .query_row("SELECT count(*) FROM native_buy_decisions", [], |r| r
                    .get::<_, i64>(0))?,
            0,
            "{kind}"
        );
        for table in ["orders", "copy_signals", "execution_source_sell_intents"] {
            assert_eq!(
                case.sql()?
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get::<_, i64>(0))?,
                0,
                "{kind}/{table}"
            );
        }
        assert_eq!(
            f::admission(&case, &id)?.is_some(),
            kind != "foreign_buy",
            "{kind}"
        );
        assert!(crate::execution_owner_roundtrip::exit_config(
            &case.source.config,
            &case.source.store
        )?
        .is_none());
        assert_eq!(case.source.calls("sendTransaction"), 0, "{kind}");
    }
    Ok(())
}
