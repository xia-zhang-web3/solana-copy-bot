#[path = "common/normal_exit_current.rs"]
mod f;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use f::*;

#[test]
#[ignore = "full pinned current BOT database; no network/signers"]
fn normal_current_full_db_receipt_exit_proof() -> Result<()> {
    let f = Case::new()?;
    let proof = f.store.owner_bot_receipt_exit_proof(&f.intent)?;
    assert_eq!(
        (
            proof.personal_raw,
            proof.expected_wallet_total_raw,
            proof.native_remaining,
            proof.buy_slot
        ),
        (100, 914698, 4945000, 464668204)
    );
    f.store.register_owner_exit_intent(&f.intent)?;
    Ok(())
}

#[test]
#[ignore = "full current DB; explicitly mocked new SELL, no keys/network"]
fn normal_current_full_path_unknown_restart_receipt_cycle_fee_once() -> Result<()> {
    let f = Case::new()?;
    let parents = f.parents()?;
    let (o, d, b) = f.dispatch()?;
    assert_eq!(
        f.store
            .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.activated_at))?,
        ExecutionDispatchClaim::New
    );
    assert_eq!(f.parents()?, parents, "BUY/session grants were changed");
    let c = f.conn()?;
    let fee: (String, u64, Option<u64>) = c.query_row(
        "SELECT state,fee_bound,actual_fee FROM owner_exit_fee_reservations",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(fee, ("pending".into(), 27000, None));
    assert_eq!(
        c.query_row("SELECT count(*) FROM real_session_orders", [], |r| r
            .get::<_, u64>(0))?,
        2
    );
    f.store
        .refresh_local_bounded_session(f.intent.activated_at)?;
    assert_eq!(
        c.query_row("SELECT state FROM real_session_cycles", [], |r| r
            .get::<_, String>(0))?,
        "active"
    );
    let restarted = SqliteStore::open(&f.path)?;
    assert_eq!(
        restarted.list_owner_exit_recovery_intents()?,
        vec![f.intent.clone()]
    );
    assert_eq!(
        restarted.claim_owner_exit_dispatch(&o, &d, &b, || Ok(
            f.intent.expires_at + Duration::seconds(1)
        ))?,
        ExecutionDispatchClaim::Existing
    );
    assert!(!restarted.rearm_owner_exit_undispatched(&f.intent.intent_id)?);
    let mut other = d.clone();
    other.tx_signature.push('x');
    assert!(restarted
        .claim_owner_exit_dispatch(&o, &other, &b, || Ok(f.intent.activated_at))
        .is_err());
    f.settle(&d)?;
    f.store
        .refresh_local_bounded_session(f.intent.activated_at + Duration::seconds(1))?;
    assert_eq!(
        c.query_row("SELECT state FROM real_session_cycles", [], |r| r
            .get::<_, String>(0))?,
        "closed"
    );
    assert_eq!(
        c.query_row(
            "SELECT state,qty_raw,cost_lamports FROM positions",
            [],
            |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u64>(2)?
            ))
        )?,
        ("closed".into(), "0".into(), 0)
    );
    f.store
        .record_execution_canary_receipt_facts(&f.receipt(&d), f.intent.activated_at)?;
    f.store
        .apply_execution_canary_sell_settlement(&f.receipt(&d), f.intent.activated_at)?;
    f.store.refresh_local_bounded_session(f.intent.expires_at)?;
    assert_eq!(
        c.query_row("SELECT count(*) FROM fills", [], |r| r.get::<_, u64>(0))?,
        2
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM execution_canary_dispatch", [], |r| {
            r.get::<_, u64>(0)
        })?,
        2
    );
    assert_eq!(
        c.query_row(
            "SELECT state,actual_fee FROM owner_exit_fee_reservations",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?))
        )?,
        ("successful".into(), 27000)
    );
    assert_eq!(f.parents()?, parents);
    assert!(
        f.store.owner_bot_receipt_exit_proof(&f.intent).is_err(),
        "closed lot cannot authorize new send"
    );
    eprintln!("NORMAL_CURRENT_FULL_PATH original_buy_retained=true unknown_restart=Existing one_mock_sell_dispatch=true receipts=2 fills=2 position=closed cycle=closed fee_once=27000 new_real_sends=0");
    Ok(())
}

#[test]
#[ignore = "full current DB clones; causal identity/partial/UNKNOWN corruptions only"]
fn normal_current_full_db_causal_proof_refusals() -> Result<()> {
    for sql in [
        "UPDATE real_session_orders SET cycle_id='foreign-cycle'",
        "UPDATE real_session_cycles SET scope_json=json_set(scope_json,'$.source_buy_intent_id','foreign-cycle')",
        "UPDATE real_session_native_decisions SET cycle_id='foreign-cycle'",
        "DELETE FROM real_session_native_decisions",
        "UPDATE native_buy_fence_epochs SET processed_slot=999999999",
        "UPDATE execution_canary_receipt_facts SET tx_signature='mismatched-receipt'",
        "DELETE FROM execution_canary_receipt_facts",
        "UPDATE execution_canary_receipt_proofs SET reason='accounting_pending'",
        "UPDATE execution_tiny_reservations SET actual_fee=NULL,outcome=NULL,reconciled_at=NULL",
        "UPDATE execution_tiny_reservations SET transaction_sha256=lower(hex(zeroblob(32)))",
        "UPDATE execution_tiny_experiment SET buy_order_id='artificial-legacy-singleton',token='artificial-token',position_id='artificial-position'",
        "UPDATE positions SET cost_lamports=10055001",
        "UPDATE positions SET qty_raw='914599'",
        "UPDATE execution_receipt_native_observations SET conflict_reason='conflict'",
    ] {
        let f=Case::new()?;f.corrupt_clone(sql)?;
        assert!(f.store.owner_bot_receipt_exit_proof(&f.intent).is_err(),"accepted {sql}");
    }
    let capped = Case::new()?;
    assert!(capped.conn()?.execute("UPDATE execution_tiny_native_policy SET allowance=15000001", []).is_err());
    for field in [
        "amount", "receipt", "position", "run", "wallet", "genesis", "early", "long",
    ] {
        let f = Case::new()?;
        let mut i = f.intent.clone();
        match field {
            "amount" => i.amount_raw += 100,
            "receipt" => i.buy_receipt_signature.push('x'),
            "position" => i.position_id.push('x'),
            "run" => i.run_id.push('x'),
            "wallet" => {
                i.wallet.push('x');
                i.signer = i.wallet.clone()
            }
            "genesis" => i.genesis_hash.push('x'),
            "early" => i.activated_at -= Duration::hours(1),
            "long" => i.expires_at += Duration::seconds(1),
            _ => unreachable!(),
        }
        assert!(
            f.store.owner_bot_receipt_exit_proof(&i).is_err(),
            "accepted {field}"
        );
    }
    Ok(())
}

#[test]
#[ignore = "full current DB; atomic rollback, pending and partial settlement"]
fn normal_current_exit_atomic_claim_and_partial_receipt_stay_closed_to_new_sends() -> Result<()> {
    let f = Case::new()?;
    let (o, d, b) = f.dispatch()?;
    let calls = std::cell::Cell::new(0);
    assert!(f
        .store
        .claim_owner_exit_dispatch(&o, &d, &b, || {
            let n = calls.get();
            calls.set(n + 1);
            Ok(if n == 0 {
                f.intent.activated_at
            } else {
                f.intent.expires_at
            })
        })
        .is_err());
    let c = f.conn()?;
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM owner_exit_fee_reservations",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM real_session_orders", [], |r| r
            .get::<_, u64>(0))?,
        1
    );
    assert!(f
        .store
        .load_execution_canary_dispatch(&o.order_id)?
        .is_none());
    for corruption in ["UNKNOWN", "partial-proof", "missing-fill"] {
        let f = Case::new()?;
        let (o, d, b) = f.dispatch()?;
        f.store
            .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.activated_at))?;
        if corruption != "UNKNOWN" {
            f.settle(&d)?;
        }
        let sql=match corruption {
            "UNKNOWN"=>"UPDATE positions SET state='closed',qty_raw='0',qty=0,cost_lamports=0,cost_sol=0",
            "partial-proof"=>"UPDATE execution_canary_receipt_proofs SET reason='accounting_pending' WHERE side='sell'",
            _=>"DELETE FROM fills WHERE order_id LIKE 'exec-canary:owner-exit:%'",
        };
        f.corrupt_clone(sql)?;
        let result = f.store.refresh_local_bounded_session(f.intent.activated_at);
        let state: String =
            f.conn()?
                .query_row("SELECT state FROM real_session_cycles", [], |r| r.get(0))?;
        assert!(
            result.is_err() || state == "active",
            "false closed {corruption}"
        );
        assert_eq!(
            SqliteStore::open(&f.path)?
                .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.expires_at))?,
            ExecutionDispatchClaim::Existing
        );
    }
    Ok(())
}

#[test]
#[ignore = "full current DB; explicit foreign UNKNOWN obligation"]
fn normal_current_foreign_unknown_blocks_new_owner_dispatch() -> Result<()> {
    let f = Case::new()?;
    let (o, d, b) = f.dispatch()?;
    f.conn()?.execute_batch("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status)
        VALUES('foreign','leader','sell','other-token',0.01,'2026-11-02T23:00:00Z','pending');
        INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt)
        VALUES('exec-canary:foreign','foreign','jupiter_swap_instructions','2026-11-02T23:00:00Z',
        'execution_canary_submitted','foreign-client',1);
        INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,
        wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
        VALUES('exec-canary:foreign','foreign','foreign-client','jupiter_swap_instructions',1,
        'foreign-wallet','other-token','sell','foreign-signature',lower(hex(zeroblob(32))),lower(hex(zeroblob(32))),
        '2026-11-02T23:00:00Z')")?;
    let e = f
        .store
        .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.activated_at))
        .unwrap_err();
    assert!(
        format!("{e:#}").contains("bot_receipt_exit_foreign_obligation"),
        "{e:#}"
    );
    assert!(f
        .store
        .load_execution_canary_dispatch(&o.order_id)?
        .is_none());
    assert_eq!(
        f.conn()?
            .query_row("SELECT count(*) FROM real_session_orders", [], |r| r
                .get::<_, u64>(0))?,
        1
    );
    Ok(())
}
