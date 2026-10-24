#[path = "common/bot_receipt_exit.rs"]
mod f;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use f::*;

#[test]
fn expired_native_parent_gets_only_new_exact_exit_and_keeps_parent_rows() -> Result<()> {
    let f = Case::new()?;
    let before = f.parent_rows()?;
    let proof = f.store.owner_bot_receipt_exit_proof(&f.intent)?;
    assert_eq!(proof.personal_raw,100);
    assert_eq!(proof.expected_wallet_total_raw,988720);
    assert_eq!(proof.native_remaining,4945000);
    assert_eq!(proof.buy_slot,44);
    assert_eq!(proof.token_account,"saved-bot-usdc-ata");
    assert_eq!(f.store.register_owner_exit_intent(&f.intent)?,OwnerExitIntentRecordOutcome::Inserted);
    assert_eq!(f.store.register_owner_exit_intent(&f.intent)?,OwnerExitIntentRecordOutcome::Existing);
    let (order,d,budget) = f.dispatch()?;
    assert_eq!(f.store.claim_owner_exit_dispatch(&order,&d,&budget,||Ok(f.intent.activated_at))?,ExecutionDispatchClaim::New);
    assert_eq!(f.parent_rows()?,before);
    assert!(!f.store.rearm_owner_exit_undispatched(&f.intent.intent_id)?);
    assert!(!f.store.owner_exit_failed_retry_available(&f.intent.intent_id)?);
    Ok(())
}

#[test]
fn native_identity_amount_window_and_personal_inventory_are_not_interchangeable() -> Result<()> {
    let f = Case::new()?;
    for field in ["buy","receipt","position","wallet","run","genesis","amount","personal","early","long"] {
        let mut i = f.intent.clone();
        match field {
            "buy" => i.buy_order_id.push('x'),
            "receipt" => i.buy_receipt_signature.push('x'),
            "position" => i.position_id.push('x'),
            "wallet" => { i.wallet=SOURCE.into(); i.signer=SOURCE.into(); },
            "run" => i.run_id.push('x'),
            "genesis" => i.genesis_hash.push('x'),
            "amount" => i.amount_raw-=1,
            "personal" => i.amount_raw+=100,
            "early" => i.activated_at-=Duration::seconds(2),
            "long" => i.expires_at+=Duration::seconds(1),
            _=>unreachable!(),
        }
        assert!(f.store.register_owner_exit_intent(&i).is_err(),"{field}");
    }
    assert_eq!(f.conn()?.query_row("SELECT count(*) FROM owner_exit_intents",[],|r|r.get::<_,i64>(0))?,0);
    Ok(())
}

#[test]
fn current_position_or_observation_changes_refuse_before_reserve_or_dispatch() -> Result<()> {
    for sql in [
        "UPDATE positions SET qty_raw='988619'",
        "UPDATE positions SET cost_lamports=10055001",
        "UPDATE execution_receipt_native_observations SET conflict_reason='conflict'",
        "UPDATE execution_receipt_native_observations SET observations_json=json_set(observations_json,'$.accounts[0].pre_token.raw.coverage','missing')",
        "UPDATE execution_receipt_native_observations SET observations_json=json_set(observations_json,'$.accounts[0].post_token.raw.value','988721')",
    ] {
        let f=Case::new()?;
        let (order,d,budget)=f.dispatch()?;
        f.conn()?.execute_batch(sql)?;
        assert!(f.store.reserve_owner_exit_order(&f.intent.intent_id,||Ok(f.intent.activated_at)).is_err());
        assert!(f.store.claim_owner_exit_dispatch(&order,&d,&budget,||Ok(f.intent.activated_at)).is_err());
        assert!(f.store.load_execution_canary_dispatch(&order.order_id)?.is_none());
    }
    Ok(())
}

#[test]
fn restart_unknown_and_closed_position_never_authorize_another_send() -> Result<()> {
    let f=Case::new()?;
    let (order,d,budget)=f.dispatch()?;
    assert_eq!(f.store.claim_owner_exit_dispatch(&order,&d,&budget,||Ok(f.intent.activated_at))?,ExecutionDispatchClaim::New);
    let s=SqliteStore::open(&f.path)?;
    assert_eq!(s.list_owner_exit_recovery_intents()?,vec![f.intent.clone()]);
    f.conn()?.execute_batch("UPDATE positions SET state='closed',qty_raw='0',cost_lamports=0")?;
    assert_eq!(s.claim_owner_exit_dispatch(&order,&d,&budget,||Ok(f.intent.expires_at+Duration::seconds(1)))?,ExecutionDispatchClaim::Existing);
    assert!(!s.rearm_owner_exit_undispatched(&f.intent.intent_id)?);
    let fee:(String,u64,Option<u64>)=f.conn()?.query_row("SELECT state,fee_bound,actual_fee FROM owner_exit_fee_reservations",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    assert_eq!(fee,("pending".into(),27000,None));
    let mut foreign=d.clone();foreign.tx_signature.push('x');
    assert!(s.claim_owner_exit_dispatch(&order,&foreign,&budget,||Ok(f.intent.activated_at)).is_err());
    Ok(())
}

#[test]
fn migration_keeps_source_rule_but_does_not_admit_arbitrary_zero_reserve_bot_exit() -> Result<()> {
    let f=Case::new()?;
    f.store.register_owner_exit_intent(&f.intent)?;
    assert!(f.conn()?.execute("UPDATE owner_exit_intents SET amount_raw=1",[]).is_err());
    assert!(f.conn()?.execute("DELETE FROM owner_exit_intents",[]).is_err());
    assert!(!f.conn()?.prepare("PRAGMA foreign_key_check")?.exists([])?);
    let trigger:String=f.conn()?.query_row("SELECT sql FROM sqlite_master WHERE name='owner_exit_intents_zero_scope'",[],|r|r.get(0))?;
    assert!(trigger.contains("$.source_wallet")&&trigger.contains("$.bot_wallet"));
    let source=SqliteStore::open(f.path.parent().unwrap().join("source.db"))?;
    let parent=source.load_owner_technical_buy_intent("old-source-buy")?.unwrap();
    let buy=owner_technical_buy_order_id(&parent.intent_id);
    let source_exit=OwnerExitIntent {
        intent_id:format!("execution-only:{}",parent.intent_id),run_id:parent.run_id,
        buy_order_id:buy.clone(),buy_receipt_signature:SOURCE_SIG.into(),
        position_id:format!("exec-canary-pos:{buy}"),wallet:SOURCE.into(),signer:SOURCE.into(),
        amount_raw:988701,activated_at:parent.activated_at,expires_at:parent.expires_at,
        ..f.intent.clone()
    };
    assert_eq!(source.register_owner_exit_intent(&source_exit)?,OwnerExitIntentRecordOutcome::Inserted);
    // A fresh DB has no EXIT row, so the trigger (not uniqueness) must reject this alias.
    let other=Case::new()?;
    let columns="intent_id,run_id,buy_order_id,buy_receipt_signature,position_id,wallet,signer,genesis_hash,mint,amount_raw,decimals,route,activated_at,expires_at,authority_sha256,max_priority_fee_lamports,min_reserve_lamports,max_slippage_bps,max_daily_loss_lamports";
    let from=f.conn()?;
    let mut q=from.prepare(&format!("SELECT {columns} FROM owner_exit_intents"))?;
    let mut row=q.query_row([],|r|(0..19).map(|i|r.get::<_,rusqlite::types::Value>(i)).collect::<rusqlite::Result<Vec<_>>>())?;
    row[0]=rusqlite::types::Value::Text("wrong-bot-exit-alias".into());
    let placeholders=(1..=19).map(|n|format!("?{n}")).collect::<Vec<_>>().join(",");
    let error=other.conn()?.execute(&format!("INSERT INTO owner_exit_intents({columns}) VALUES({placeholders})"),rusqlite::params_from_iter(row)).unwrap_err();
    assert!(error.to_string().contains("zero reserve requires immutable scope"));
    Ok(())
}

#[test]
fn foreign_unknown_or_failed_expense_blocks_the_new_exit_claim() -> Result<()> {
    for kind in ["unknown", "failed-expense"] {
        let f=Case::new()?;
        let (order,d,budget)=f.dispatch()?;
        let c=f.conn()?;
        c.execute_batch("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status)
            VALUES('foreign','leader','sell','other-token',0.01,'2026-10-24T15:34:31Z','pending');
            INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt)
            VALUES('exec-canary:foreign','foreign','jupiter_swap_instructions','2026-10-24T15:34:31Z',
            'execution_canary_submitted','foreign-client',1)")?;
        if kind=="unknown" {
            c.execute_batch("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,
                route,attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
                VALUES('exec-canary:foreign','foreign','foreign-client','jupiter_swap_instructions',1,
                'foreign-wallet','other-token','sell','foreign-signature',lower(hex(zeroblob(32))),lower(hex(zeroblob(32))),
                '2026-10-24T15:34:31Z')")?;
        } else {
            c.execute_batch("INSERT INTO execution_failed_expense_tasks(order_id,tx_signature,attempt,
                route,wallet,token,side,operation_at,detected_at,failure_source,failure_error_json,
                commitment,status,reason)VALUES('exec-canary:foreign','foreign-signature',1,
                'jupiter_swap_instructions','foreign-wallet','other-token','sell','2026-10-24T15:34:31Z',
                '2026-10-24T15:34:31Z','receipt_meta','{}','finalized','pending','unresolved')")?;
        }
        let error=f.store.claim_owner_exit_dispatch(&order,&d,&budget,||Ok(f.intent.activated_at)).unwrap_err();
        assert!(format!("{error:#}").contains("bot_receipt_exit_foreign_obligation"),"{kind}: {error:#}");
        assert!(f.store.load_execution_canary_dispatch(&order.order_id)?.is_none());
    }
    Ok(())
}

#[test]
fn expiry_during_atomic_claim_rolls_back_dispatch_fee_and_order_status() -> Result<()> {
    let f=Case::new()?;
    let (order,d,budget)=f.dispatch()?;
    let reads=std::cell::Cell::new(0);
    let result=f.store.claim_owner_exit_dispatch(&order,&d,&budget,|| {
        let n=reads.get();reads.set(n+1);
        Ok(if n==0 { f.intent.activated_at } else { f.intent.expires_at })
    });
    let error=result.unwrap_err();
    assert!(format!("{error:#}").contains("owner_exit_intent_expired"),"unexpected claim refusal: {error:#}");
    assert!(f.store.load_execution_canary_dispatch(&order.order_id)?.is_none());
    assert_eq!(f.conn()?.query_row("SELECT count(*) FROM owner_exit_fee_reservations",[],|r|r.get::<_,i64>(0))?,0);
    assert_eq!(f.store.load_execution_canary_order(&order.order_id)?.unwrap(),order);
    Ok(())
}

#[test]
fn actual_partial_aggregate_coverage_keeps_known_target_proof_but_never_unknown_target() -> Result<()> {
    let f=Case::new()?;
    let c=f.conn()?;
    let raw:String=c.query_row("SELECT observations_json FROM execution_receipt_native_observations",[],|r|r.get(0))?;
    let mut observations:NativeAccountObservations=serde_json::from_str(&raw)?;
    observations.accounts_coverage=ObservationCoverage::Missing;
    observations.instructions_coverage=ObservationCoverage::Unsupported;
    observations.reasons=vec!["parsed_instruction_partial_or_unsupported".into(),"token_account_fields_partial".into()];
    let mut unrelated=observations.accounts[0].clone();
    unrelated.account_index=99;unrelated.pubkey="unrelated-wallet-instruction-account".into();
    unrelated.pre_token=NativeTokenEndpoint::unknown(ObservationCoverage::Missing);
    unrelated.post_token=NativeTokenEndpoint::unknown(ObservationCoverage::Missing);
    unrelated.relevance=vec!["wallet_instruction_link".into()];
    observations.accounts.push(unrelated);
    observations.validate()?;
    c.execute("UPDATE execution_receipt_native_observations SET observations_json=?1",
        [serde_json::to_string(&observations)?])?;
    let proof=f.store.owner_bot_receipt_exit_proof(&f.intent)?;
    assert_eq!((proof.personal_raw,proof.expected_wallet_total_raw),(100,988720));
    f.store.register_owner_exit_intent(&f.intent)?;
    observations.accounts[0].pre_token.raw=NativeObservation::unknown(ObservationCoverage::Missing);
    observations.validate()?;
    c.execute("UPDATE execution_receipt_native_observations SET observations_json=?1",
        [serde_json::to_string(&observations)?])?;
    let error=f.store.owner_bot_receipt_exit_proof(&f.intent).unwrap_err();
    assert!(format!("{error:#}").contains("bot_receipt_exit_observation_unknown"),"{error:#}");
    Ok(())
}

#[test]
fn guarded_claim_clock_reuses_atomic_snapshot_for_public_receipt_proof() -> Result<()> {
    let f=Case::new()?;
    let before=f.parent_rows()?;
    let (order,d,budget)=f.dispatch()?;
    let calls=std::cell::Cell::new(0);
    let outcome=f.store.claim_owner_exit_dispatch(&order,&d,&budget,|| {
        let proof=f.store.owner_bot_receipt_exit_proof(&f.intent)?;
        assert_eq!((proof.personal_raw,proof.expected_wallet_total_raw),(100,988720));
        calls.set(calls.get()+1);
        Ok(f.intent.activated_at)
    })?;
    assert_eq!(outcome,ExecutionDispatchClaim::New);
    assert_eq!(calls.get(),2);
    assert_eq!(f.parent_rows()?,before);
    assert_eq!(f.store.load_execution_canary_dispatch(&order.order_id)?,Some(d));
    Ok(())
}
