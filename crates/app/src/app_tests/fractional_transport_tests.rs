use super::{
    fractional_financial_fixture as money, fractional_fixture::Fixture,
    fractional_transport_fixture as t,
};
use crate::execution_owned_sell_rpc::{body, fractional::transport};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::*;
use serde_json::{json, Value};
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn fractional_transport_one_collector_no_wait_or_rearm() -> Result<()> {
    let mut first = Fixture::new().await?;
    let mut second = Fixture::new().await?;
    let one = first.claim()?;
    let two = second.claim()?;
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let mut mock = t::Mock::new(&first, "none")?;
    mock.hold = Some((started.clone(), release.clone()));
    let waiting =
        async {
            started.notified().await;
            let mock = t::Mock::new(&second, "none")?;
            let calls = mock.calls.clone();
            let error = t::bind(&mut second, two.clone(), mock).await.unwrap_err();
            assert!(error.to_string().contains("fraction_collection_capacity"));
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let count: i64 = second.db.sql.query_row(
                "SELECT count(*) FROM fractional_sell_decisions",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(count, 0);
            release.notify_one();
            Ok::<_, anyhow::Error>(())
        };
    let (result, capacity) = tokio::join!(t::bind(&mut first, one, mock), waiting);
    assert_eq!(result?.binding.raw, 250);
    capacity?;
    // This was proven unsent, never a consumed collecting/UNKNOWN intent.
    let mock = t::Mock::new(&second, "none")?;
    assert_eq!(t::bind(&mut second, two, mock).await?.binding.raw, 250);
    Ok(())
}
#[tokio::test]
async fn fractional_transport_large_native_250_750() -> Result<()> {
    for mode in ["none", "no_length"] {
        let mut f = Fixture::new().await?;
        money::budget(&f)?;
        let claim = f.claim()?;
        let mock = t::Mock::new(&f, mode)?;
        let expected =
            crate::execution_owned_sell_rpc::digest(serde_json::to_vec(&mock.evidence.block)?);
        let claim = t::bind(&mut f, claim, mock).await?;
        assert_eq!(claim.binding.raw, 250);
        let saved: String =
            f.db.sql
                .query_row("SELECT evidence FROM fractional_sell_decisions", [], |r| {
                    r.get(0)
                })?;
        let value: Value = serde_json::from_str(&saved)?;
        assert_eq!(
            crate::execution_owned_sell_rpc::digest(serde_json::to_vec(&value["block"])?),
            expected
        );
        let p = money::prepare(&f, &claim)?;
        let d = money::dispatch(&f, &p)?;
        let facts = money::receipt(&d, 250);
        f.db.store.mark_execution_canary_confirmed_unreconciled(
            &d.order_id,
            &ExecutionCanaryReceiptProof {
                tx_signature: d.tx_signature.clone(),
                wallet_pubkey: d.wallet.clone(),
                token: d.token.clone(),
                side: "sell".into(),
                confirmation_status: "confirmed".into(),
                slot: Some(151),
                confirmed_at: Utc::now(),
                reason: "synthetic byte-transport receipt".into(),
            },
            Utc::now(),
        )?;
        f.db.store
            .record_execution_canary_receipt_facts(&facts, Utc::now())?;
        f.db.store
            .apply_execution_canary_sell_settlement(&facts, Utc::now())?;
        let reopened = SqliteStore::open(&f.db.path)?;
        let pos = reopened
            .load_execution_canary_open_position(&d.token)?
            .unwrap();
        assert_eq!(pos.qty_exact.unwrap().raw(), 750);
        assert_eq!(pos.cost_lamports.unwrap().as_u64(), 1071);
        assert_eq!(
            reopened
                .load_execution_canary_cash_settlement(&d.order_id)?
                .unwrap()
                .allocated_entry_basis
                .as_u64(),
            357
        );
    }
    Ok(())
}
#[tokio::test]
async fn fractional_transport_parent_conflict_during_collection_never_proves_sell() -> Result<()> {
    let mut f = Fixture::new().await?;
    let claim = f.claim()?;
    let mock = t::Mock::new(&f, "parent_conflict")?;
    let error = t::bind(&mut f, claim, mock).await.unwrap_err();
    assert!(error.to_string().contains("strict_policy") || error.to_string().contains("fraction_generation_changed"), "{error:#}");
    let state: String = f.db.sql.query_row(
        "SELECT state FROM fractional_sell_decisions", [], |r| r.get(0),
    )?;
    assert_eq!(state, "collecting");
    assert_eq!(f.db.sql.query_row(
        "SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r.get::<_, i64>(0),
    )?, 0);
    assert_eq!(f.db.sql.query_row(
        "SELECT count(*) FROM ordered_sell_quote_results WHERE record IS NOT NULL", [], |r| r.get::<_, i64>(0),
    )?, 0);
    Ok(())
}
#[tokio::test]
async fn fractional_transport_saved_twelve_capacity_only() -> Result<()> {
    for i in 1..=12 {
        let raw = super::fractional_synthetic_fixture::capacity_body(i)?;
        assert!((2_556_083..=8 << 20).contains(&raw.len()));
        let id: Value = serde_json::from_slice::<Value>(&raw)?["id"].clone();
        let request = json!({"jsonrpc":"2.0","id":id,"method":"getBlock","params":[150,{"transactionDetails":"full"}]});
        let mut chunks = t::chunks(&raw);
        let mut budget = body::Budget::default();
        let v = transport::receive(
            &request,
            body::Head {
                endpoint_matches: true,
                status: 200,
                content_length: Some(raw.len() as u64),
            },
            Instant::now() + Duration::from_millis(1500),
            &mut budget,
            &mut || Ok(()),
            &mut chunks,
            t::next,
        )
        .await?;
        assert!(!v["result"]["transactions"].as_array().unwrap().is_empty());
        // Capacity/envelope only. No assertion of financial semantic support.
    }
    Ok(())
}
#[tokio::test]
async fn fractional_transport_refusals_consumed_without_rearm() -> Result<()> {
    for fault in [
        "declared",
        "chunked",
        "id",
        "result",
        "status",
        "endpoint",
        "truncated",
        "invalid",
        "stopped",
        "stale",
        "expired",
        "aggregate",
    ] {
        let mut f = Fixture::new().await?;
        let mut claim = f.claim()?;
        if fault == "expired" {
            claim.lease_until = Utc::now() + chrono::Duration::seconds(1);
            f.db.sql.execute(
                "UPDATE ordered_sell_quote_results SET lease_until=?1",
                [claim.lease_until.to_rfc3339()],
            )?;
        }
        let mut mock = t::Mock::new(&f, fault)?;
        if fault == "aggregate" {
            mock.budget.charge(&vec![b' '; (32 << 20) - (1 << 20)])?;
        }
        let calls = mock.calls.clone();
        let body_chunks = mock.body_chunks.clone();
        let err = t::bind(&mut f, claim.clone(), mock)
            .await
            .unwrap_err()
            .to_string();
        if matches!(fault, "stale" | "expired" | "stopped") {
            assert!(
                body_chunks.load(std::sync::atomic::Ordering::SeqCst) >= 2,
                "refused before body: {fault}"
            );
        }
        if fault == "stale" {
            assert!(err.contains("fraction_decision_changed"), "{err}");
        }
        if fault == "expired" {
            assert!(err.contains("deadline"), "{err}");
        }
        if fault == "stopped" {
            assert!(err.contains("cancelled"), "{err}");
        }
        assert!(calls.load(std::sync::atomic::Ordering::SeqCst) <= 2);
        t::refused(&f, &claim)?;
        let mock = t::Mock::new(&f, "none")?;
        let calls = mock.calls.clone();
        assert!(t::bind(&mut f, claim, mock).await.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    Ok(())
}
#[tokio::test]
async fn fractional_transport_persisted_bytes_and_encoder_caps() -> Result<()> {
    let mut f = Fixture::new().await?;
    let claim = f.claim()?;
    // Synthetic retained quota rows; no financial identity/admission is inferred.
    for alias in ["capacity-one", "capacity-two"] {
        f.db.sql.execute("INSERT INTO ordered_source_sell_intents(intent_id,signature,version,policy,record) VALUES('source-sell:'||?1,?1,1,'provider_order_strict_v1','{}')",[alias])?;
        f.db.sql.execute("INSERT INTO fractional_sell_decisions(intent_id,base_binding,claim_owner,decision_id,producer_identity,state,evidence) VALUES('source-sell:'||?1,'{}','quota',?1,?2,'collecting',?3)",rusqlite::params![alias,"a".repeat(64),"é".repeat(16<<20)])?;
    }
    let mock = t::Mock::new(&f, "none")?;
    let err = t::bind(&mut f, claim.clone(), mock)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("fraction_evidence_capacity"), "{err}");
    assert!(!f.db.store.has_owned_sell_handoff(&claim.intent_id)?);
    let total: i64 = f.db.sql.query_row(
        "SELECT sum(length(CAST(evidence AS BLOB))) FROM fractional_sell_decisions",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(total, 64 << 20);
    let mut e = super::fractional_tests::evidence()?;
    e.block["padding"] = json!("x".repeat(32 << 20));
    assert!(
        copybot_storage_core::ordered_sell_quote::fractional::inventory::encode_evidence(&e)
            .unwrap_err()
            .to_string()
            .contains("fraction_evidence_bound")
    );
    Ok(())
}
#[tokio::test]
async fn fractional_transport_small_caps_deadline_memory_and_lengths() -> Result<()> {
    for method in [
        "getTransaction",
        "getGenesisHash",
        "getTokenAccountsByOwnerAtSlot",
        "getBlock",
    ] {
        let request = json!({"jsonrpc":"2.0","id":1,"method":method,"params":[150,{"transactionDetails":"none"}]});
        assert_eq!(body::fractional_limit(&request), 1 << 20);
        let raw = vec![b' '; (1 << 20) + 1];
        let mut chunks = t::chunks(&raw);
        assert!(transport::receive(
            &request,
            body::Head {
                endpoint_matches: true,
                status: 200,
                content_length: None
            },
            Instant::now() + Duration::from_secs(1),
            &mut Default::default(),
            &mut || Ok(()),
            &mut chunks,
            t::next
        )
        .await
        .is_err());
    }
    let head = body::Head {
        endpoint_matches: true,
        status: 200,
        content_length: None,
    };
    let deadline = Instant::now() + Duration::from_millis(10);
    let err = body::bytes(head, 1 << 20, deadline, &mut || Ok(()), &mut (), |_| {
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(Some(vec![0u8]))
        })
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("deadline"));
    let mut one = t::chunks(b"{}");
    assert!(body::bytes(
        body::Head {
            content_length: Some(1),
            ..head
        },
        1 << 20,
        Instant::now() + Duration::from_secs(1),
        &mut || Ok(()),
        &mut one,
        t::next
    )
    .await
    .is_err());
    let mut chunks = t::chunks(&vec![b' '; 2 << 20]);
    let mut checks = 0;
    let err = body::bytes(
        head,
        8 << 20,
        Instant::now() + Duration::from_secs(1),
        &mut || {
            checks += 1;
            anyhow::ensure!(checks < 4, "stopped_body_guard");
            Ok(())
        },
        &mut chunks,
        t::next,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("stopped_body_guard"));
    // Before JSON allocation: dense arrays cannot amplify bounded wire without limit.
    assert!(body::Budget::default()
        .charge(&b"0,".repeat(1 << 20))
        .is_err());
    Ok(())
}
