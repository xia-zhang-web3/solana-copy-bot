#[path = "common/association.rs"]
mod fixture;
#[path = "common/native_buy_decision_case.rs"]
mod decision_case;
use decision_case::Case;

use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::association_delivery::{
    AdmissionFacts, BlockTime, CandidateGeneration, DeliveryEvent, ProviderAssertion, Terminal,
    Unresolved,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{NativeBuyFence, SPL_TOKEN_PROGRAM, STATUS},
    SqliteStore,
};
use rusqlite::params;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

#[test]
fn first_fenced_admission_needs_terminal_finality_and_unchanged_cohort() -> Result<()> {
    let path = std::path::PathBuf::from(format!(
        "file:native-buy-{}-{}?mode=memory&cache=shared",
        std::process::id(), NEXT.fetch_add(1,Ordering::Relaxed)
    ));
    let mut anchor = SqliteStore::open(&path)?;
    anchor.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"),"/../../migrations")))?;
    copybot_storage_core::ensure_discovery_v2_schema(&anchor)?;
    let now = Utc::now();
    let wallet = "leader";
    let mint = "classic-mint";
    let window = "2026-09-22T00:00:00+00:00";
    let sql = rusqlite::Connection::open(&path)?;
    sql.execute("INSERT INTO followlist(wallet_id,added_at,active) VALUES(?1,?2,1)",
        params![wallet,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_candidate_sources(wallet_id,source_cohort,window_start,updated_at) VALUES(?1,'candidate',?2,?3)",
        params![wallet,window,(now-Duration::seconds(3)).to_rfc3339()])?;
    sql.execute("INSERT INTO discovery_strategy_state(id,publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json,updated_at) VALUES(1,'healthy',?1,?2,'policy','[\"leader\"]',?3)",
        params![(now-Duration::seconds(3)).to_rfc3339(),window,now.to_rfc3339()])?;
    let mut admission = fixture::facts();
    admission.facts.signature = "fresh-buy".into();
    admission.facts.wallet = wallet.into();
    admission.facts.token_in = "So11111111111111111111111111111111111111112".into();
    admission.facts.token_out = mint.into();
    admission.facts.exact_amounts.as_mut().unwrap().amount_in_raw = "1428".into();
    admission.facts.exact_amounts.as_mut().unwrap().amount_out_raw = "1000".into();
    admission.facts.exact_amounts.as_mut().unwrap().amount_out_decimals = 3;
    let mut inbox = AssociationInbox::open(&path, fixture::limits())?;
    let fence = NativeBuyFence {
        session: "session-A".into(),
        processed_slot: 6,
        sampled_at: now-Duration::seconds(1),
        genesis_hash: "genesis".into(),
        policy_identity: "execution-policy".into(),
    };
    inbox.record_native_buy_fence(&fence)?;
    let event = fixture::event(1, DeliveryEvent::Admission(admission.clone()));
    inbox.persist_at(&event, &CandidateGeneration::Unknown, now)?;
    let store = SqliteStore::open(&path)?;
    assert!(store.list_native_buy_pending(2)?.is_empty());
    assert!(store.native_buy_ready("fresh-buy", now, 10)?.is_none());
    let terminal = fixture::event(2, DeliveryEvent::Terminal {
        signature:"fresh-buy".into(), expected:admission,
        result:Terminal::ProviderAsserted(ProviderAssertion {
            slot:7, blockhash:"blockhash".into(), signature:"fresh-buy".into(),
            transaction_index:1, block_time:BlockTime::Missing,
        }),
    });
    inbox.persist_at(&terminal, &CandidateGeneration::Unknown, now)?;
    assert_eq!(store.list_native_buy_pending(2)?.len(), 1);
    assert!(!store.native_buy_record_finalized("fresh-buy",7,"TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",now)?);
    assert!(store.native_buy_record_finalized("fresh-buy",7,SPL_TOKEN_PROGRAM,now)?);
    assert_eq!(store.list_native_buy_finalized(2,now,10)?.len(),1);
    let candidate = store.native_buy_ready("fresh-buy",now+Duration::seconds(1),10)?.unwrap();
    assert_eq!(candidate.amount_lamports,1428);
    assert_eq!(store.load_copy_signal_by_signal_id(&candidate.signal_id)?.unwrap().status,STATUS);
    assert!(store.native_buy_recheck(&candidate.signal_id,&candidate.decision_id,now+Duration::seconds(2),10)?);
    assert_eq!(store.native_buy_policy_identity(&candidate.signal_id)?.as_deref(),Some("execution-policy"));
    drop(inbox);
    let _reopened = AssociationInbox::open(&path,fixture::limits())?;
    assert!(store.native_buy_recheck(&candidate.signal_id,&candidate.decision_id,now+Duration::seconds(2),10)?);
    sql.execute("UPDATE discovery_candidate_sources SET source_cohort='changed' WHERE wallet_id=?1",[wallet])?;
    assert!(!store.native_buy_recheck(&candidate.signal_id,&candidate.decision_id,now+Duration::seconds(2),10)?);
    assert!(store.list_native_buy_finalized(2,now+Duration::seconds(2),10)?.is_empty());
    sql.execute("UPDATE discovery_candidate_sources SET source_cohort='candidate' WHERE wallet_id=?1",[wallet])?;
    assert!(store.native_buy_recheck(&candidate.signal_id,&candidate.decision_id,now+Duration::seconds(2),10)?);
    assert!(store.native_buy_ready("fresh-buy",now+Duration::seconds(11),10)?.is_none());
    Ok(())
}

#[test]
fn stale_replayed_and_foreign_first_admissions_never_promote() -> Result<()> {
    for fault in ["no_fence","old_slot","wrong_wallet","wrong_mint","wrong_cohort"] {
        let mut c = Case::new()?;
        if fault != "no_fence" { c.fence("session-A",if fault == "old_slot" {7} else {6})?; }
        match fault {
            "wrong_wallet" => c.admission.facts.wallet="foreign".into(),
            "wrong_mint" => c.admission.facts.token_out="So11111111111111111111111111111111111111112".into(),
            "wrong_cohort" => { c.sql.execute("DELETE FROM discovery_candidate_sources",[])?; },
            _ => {}
        }
        c.admit()?;
        let asserted = c.asserted();
        c.terminal(asserted)?;
        let store = c.store()?;
        assert!(store.list_native_buy_pending(2)?.is_empty(),"{fault}");
        assert!(!store.native_buy_record_finalized(&c.admission.facts.signature,7,SPL_TOKEN_PROGRAM,c.now)?,"{fault}");
        assert!(store.native_buy_ready(&c.admission.facts.signature,c.now,10)?.is_none(),"{fault}");
        if fault == "no_fence" {
            assert!(c.fence("session-A",6).is_err());
            c.inbox.persist_at(&fixture::event(3,DeliveryEvent::Admission(c.admission.clone())),
                &CandidateGeneration::Unknown,c.now)?;
            assert!(store.list_native_buy_pending(2)?.is_empty(),"late fence/replay cannot mint authority");
        }
    }
    Ok(())
}

#[test]
fn unresolved_conflicting_terminal_and_restart_do_not_rebind_buy() -> Result<()> {
    for fault in ["missing_terminal","unresolved","wrong_signature","wrong_slot"] {
        let mut c = Case::new()?;
        c.fence("session-A",6)?;
        c.admit()?;
        if fault == "unresolved" {
            c.terminal(Terminal::Unresolved(Unresolved::EndOfStream))?;
        } else if fault != "missing_terminal" {
            let mut bad = c.asserted();
            let Terminal::ProviderAsserted(ref mut p) = bad else { unreachable!() };
            if fault == "wrong_signature" { p.signature="other".into(); } else { p.slot=8; }
            c.terminal(bad)?;
        }
        let store = c.store()?;
        assert!(store.list_native_buy_pending(2)?.is_empty(),"{fault}");
        assert!(!store.native_buy_record_finalized("source-buy",7,SPL_TOKEN_PROGRAM,c.now)?,"{fault}");
        c.inbox = AssociationInbox::open(&c.path,fixture::limits())?;
        assert!(store.native_buy_ready("source-buy",c.now,10)?.is_none(),"{fault}");
    }
    Ok(())
}

#[test]
fn duplicate_restart_policy_session_and_signal_substitution_fail_closed() -> Result<()> {
    let mut c = Case::new()?;
    c.fence("session-A",6)?;
    c.admit()?;
    let asserted = c.asserted();
    c.terminal(asserted)?;
    let store = c.store()?;
    assert!(store.native_buy_record_finalized("source-buy",7,SPL_TOKEN_PROGRAM,c.now)?);
    let first = store.native_buy_ready("source-buy",c.now,10)?.unwrap();
    assert_eq!(store.native_buy_policy_identity(&first.signal_id)?.as_deref(),Some("config-v1"));
    c.inbox = AssociationInbox::open(&c.path,fixture::limits())?;
    let mut replay = fixture::event(3,DeliveryEvent::Admission(c.admission.clone()));
    replay.session="session-B".into();
    c.inbox.persist_at(&replay,&CandidateGeneration::Unknown,c.now)?;
    assert_eq!(store.native_buy_ready("source-buy",c.now,10)?.unwrap(),first);
    let decisions:i64=c.sql.query_row("SELECT count(*) FROM native_buy_decisions",[],|r|r.get(0))?;
    assert_eq!(decisions,1);
    c.sql.execute("UPDATE copy_signals SET token='other' WHERE signal_id=?1",[&first.signal_id])?;
    assert!(!store.native_buy_recheck(&first.signal_id,&first.decision_id,c.now,10)?);
    c.sql.execute("UPDATE copy_signals SET token='classic-mint' WHERE signal_id=?1",[&first.signal_id])?;
    assert!(store.native_buy_recheck(&first.signal_id,&first.decision_id,c.now,10)?);
    c.fence("session-C",8)?;
    assert!(!store.native_buy_recheck(&first.signal_id,&first.decision_id,c.now,10)?);
    assert_ne!(store.native_buy_policy_identity(&first.signal_id)?.as_deref(),Some("different-policy"));
    Ok(())
}
