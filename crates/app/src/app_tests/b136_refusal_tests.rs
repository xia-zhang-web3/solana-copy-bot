use super::{
    b136_config, b136_endpoint_tests::sends, b136_fixture::Fixture, b136_server::Server,
    strict_quote_fixture as q,
};
use anyhow::Result;
use std::time::Duration;
pub(super) async fn refusal(
    f: &Fixture,
    r: &crate::execution_canary::ExecutionCanaryRunner,
) -> Result<String> {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            match q::tick(r, &f.db).await {
                Err(e) => return format!("{e:#}"),
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .map_err(Into::into)
}
#[tokio::test]
async fn b136_held_await_mutations_refuse_before_claim() -> Result<()> {
    for method in ["isBlockhashValid", "getFeeForMessage:2"] {
        for (name,sql) in [
            ("generation","UPDATE positions SET opened_ts='2026-09-10T00:00:00Z'"),
            ("raw","UPDATE positions SET qty_raw='4000',qty=4"),
            ("contributor","UPDATE copy_signals SET wallet_id='other'"),
            ("receipt","UPDATE execution_canary_receipt_facts SET wallet_native_post='42'"),
            ("config","UPDATE rpc_owned_sell_handoffs SET config_sha256='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'"),
            ("payload","UPDATE rpc_owned_sell_handoffs SET unsigned_payload='changed'"),
            ("quote","UPDATE rpc_owned_sell_handoffs SET quote='{}'"),
            ("quote_result","UPDATE ordered_sell_quote_results SET record='{}'"),
            ("experiment","UPDATE execution_tiny_experiment SET state='stopped',stop_reason='operator_stop'"),
            ("clock","UPDATE execution_tiny_experiment SET activated_at=strftime('%Y-%m-%dT%H:%M:%fZ',activated_at,'-1 second'),deadline=strftime('%Y-%m-%dT%H:%M:%fZ',deadline,'-1 second')"),
            ("policy","UPDATE execution_tiny_experiment SET policy_mode='protected_native_capital'"),
            ("kill",""),
        ] {
            let f=Fixture::new().await?;let s=Server::new().await?;let c=b136_config::load(&f,&s.url,true)?;
            f.ingress(&c).await?;let r=f.runner(&c)?;let (at,release)=s.hold(method);
            q::tick(&r,&f.db).await?;tokio::time::timeout(Duration::from_secs(4),at).await??;
            if name=="kill" {std::fs::write(&c.execution.canary_kill_switch_path,b"test")?;} else {f.db.sql.execute(sql,[])?;}
            release.send(()).unwrap();
            let error=if name=="kill" {
                tokio::time::sleep(Duration::from_millis(60)).await;
                assert_eq!(r.process_tick(&f.db.store,chrono::Utc::now()).await?.skipped_reason,Some("kill_switch_active"));
                "kill_switch_active".to_string()
            } else {refusal(&f,&r).await?};
            assert!(!error.is_empty());assert_eq!(sends(&s),0,"{name}:{error}");
            assert_eq!(f.rows("rpc_owned_sell_dispatches")?.len(),0);
            assert_eq!(f.rows("execution_tiny_reservations")?.len(),1);
            assert_eq!(f.rows("rpc_owned_sell_handoffs")?.len(),1);
            let keys=super::b135_hooks::count(&c.execution.execution_signer_keypair_path);
            assert_eq!(keys,if method=="isBlockhashValid" {0} else {1},"{name}");
            println!("B136_HELD {method} {name} keyloads={keys} send=0: {error}");s.healthy();
        }
    }
    Ok(())
}
#[tokio::test]
async fn b136_fee_and_blockhash_unknown_stale_or_over_refuse() -> Result<()> {
    for fault in [
        "blockhash_false",
        "blockhash_null",
        "blockhash_stale",
        "fee_stale",
        "fee_null",
        "fee_over",
        "fee_stale_after",
        "fee_null_after",
        "fee_over_after",
    ] {
        let f = Fixture::new().await?;
        let s = Server::new().await?;
        *s.fault.lock().unwrap() = fault.into();
        let c = b136_config::load(&f, &s.url, true)?;
        f.ingress(&c).await?;
        let r = f.runner(&c)?;
        let before = f.rows("positions")?;
        let error = refusal(&f, &r).await?;
        assert_eq!(sends(&s), 0);
        assert!(f.rows("rpc_owned_sell_dispatches")?.is_empty());
        assert_eq!(f.rows("positions")?, before);
        assert_eq!(
            super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
            if fault.ends_with("after") { 1 } else { 0 }
        );
        println!("B136_PROOF_REFUSAL {fault}: {error}");
        s.healthy();
    }
    Ok(())
}
#[tokio::test]
async fn b136_quote_expiry_and_cancelled_owner_remain_held() -> Result<()> {
    for cancel in [false, true] {
        let f = Fixture::new().await?;
        let s = Server::new().await?;
        let mut c = b136_config::load(&f, &s.url, true)?;
        c.execution.quote_canary_timeout_ms = 10000;
        copybot_config::validate_association_delivery(&c)?;
        f.ingress(&c).await?;
        let r = f.runner(&c)?;
        let (at, release) = s.hold("isBlockhashValid");
        q::tick(&r, &f.db).await?;
        tokio::time::timeout(Duration::from_secs(4), at).await??;
        let before = f.rows("rpc_owned_sell_handoffs")?;
        if cancel {
            drop(r);
        } else {
            tokio::time::sleep(Duration::from_millis(5100)).await;
            let error = refusal(&f, &r).await?;
            assert!(
                error.contains("deadline") || error.contains("timed out"),
                "{error}"
            );
            let deadline: String =
                f.db.sql
                    .query_row("SELECT deadline FROM rpc_owned_sell_handoffs", [], |r| {
                        r.get(0)
                    })?;
            assert!(chrono::Utc::now() > deadline.parse::<chrono::DateTime<chrono::Utc>>()?);
            drop(r);
        }
        release.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        f.ingress(&c).await?;
        for _ in 0..3 {
            q::tick(&f.runner(&c)?, &f.db).await?;
        }
        assert_eq!(sends(&s), 0);
        assert_eq!(
            super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
            0
        );
        assert_eq!(f.rows("rpc_owned_sell_handoffs")?, before);
        assert!(f.rows("rpc_owned_sell_dispatches")?.is_empty());
        println!("B136_HELD_OWNER cancelled={cancel} restart_send=0");
    }
    Ok(())
}
#[tokio::test]
async fn b136_atomic_transfer_rolls_back_every_write() -> Result<()> {
    for (table, event) in [
        ("execution_order_sources", "INSERT"),
        ("orders", "INSERT"),
        ("execution_canary_dispatch", "INSERT"),
        ("rpc_owned_sell_dispatches", "INSERT"),
        ("orders", "UPDATE"),
        ("execution_tiny_reservations", "INSERT"),
    ] {
        let f = Fixture::new().await?;
        let s = Server::new().await?;
        let c = b136_config::load(&f, &s.url, true)?;
        f.ingress(&c).await?;
        let before = f.rows("positions")?;
        f.db.sql.execute_batch(&format!("CREATE TRIGGER b136_abort BEFORE {event} ON {table} BEGIN SELECT RAISE(ABORT,'b136_atomic_rollback'); END;"))?;
        let error = refusal(&f, &f.runner(&c)?).await?;
        assert!(error.contains("b136_atomic_rollback"), "{table}:{error}");
        assert_eq!(sends(&s), 0);
        assert_eq!(f.rows("positions")?, before);
        for t in [
            "orders",
            "execution_order_sources",
            "execution_canary_dispatch",
            "execution_tiny_reservations",
            "rpc_owned_sell_handoffs",
        ] {
            assert_eq!(f.rows(t)?.len(), 1, "{t}");
        }
        assert!(f.rows("rpc_owned_sell_dispatches")?.is_empty());
        println!("B136_ATOMIC {event} {table} rollback=complete send=0");
    }
    Ok(())
}
