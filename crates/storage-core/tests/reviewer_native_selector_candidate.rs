//! Reviewer controls use the same isolated RAM Case fixture as native_buy_decision.
#[path = "native_buy_decision.rs"]
mod native_buy_decision;
#[path = "common/association.rs"]
mod fixture;
#[path = "common/native_buy_decision_case.rs"]
mod decision_case;
use decision_case::Case;
use anyhow::Result;
use chrono::Utc;
use copybot_core_types::association_delivery::{
    CandidateGeneration, DeliveryEvent, Terminal, Unresolved,
};
use copybot_storage_core::{native_buy::SPL_TOKEN_PROGRAM, SqliteStore};

#[test]
fn reviewer_case_modules_keep_simultaneous_ram_databases_distinct() -> Result<()> {
    let reviewer = Case::new()?;
    let decision = native_buy_decision::decision_case::Case::new()?;
    assert_ne!(reviewer.path, decision.path);
    reviewer.sql.execute(
        "INSERT INTO followlist(wallet_id,added_at,active) VALUES('reviewer-only',?1,1)",
        [Utc::now().to_rfc3339()],
    )?;
    let present = |sql: &rusqlite::Connection| -> Result<i64> {
        Ok(sql.query_row(
            "SELECT count(*) FROM followlist WHERE wallet_id='reviewer-only'",
            [],
            |row| row.get(0),
        )?)
    };
    assert_eq!(present(&reviewer.sql)?, 1);
    assert_eq!(present(&decision.sql)?, 0);
    Ok(())
}

fn reviewer_two_native_decisions(finalize_old: bool) -> Result<(Case, SqliteStore)> {
    let mut c = Case::new()?;
    c.fence("session-A", 6)?;
    c.admit()?;
    let old_terminal = if finalize_old {
        c.asserted()
    } else {
        Terminal::Unresolved(Unresolved::EndOfStream)
    };
    c.terminal(old_terminal)?;
    let store = c.store()?;
    if finalize_old {
        assert!(store.native_buy_record_finalized(
            "source-buy", 7, SPL_TOKEN_PROGRAM, Utc::now()
        )?);
    }

    // A new actual admission belongs to the current session. The older row is
    // durably invalid because its session is closed/replaced; it is not deleted.
    c.fence("session-B", 6)?;
    c.admission.facts.signature = "source-buy-new".into();
    let mut admission = fixture::event(1, DeliveryEvent::Admission(c.admission.clone()));
    admission.session = "session-B".into();
    c.inbox.persist_at(&admission, &CandidateGeneration::Unknown, Utc::now())?;
    let mut terminal = fixture::event(2, DeliveryEvent::Terminal {
        signature: c.admission.facts.signature.clone(),
        expected: c.admission.clone(),
        result: c.asserted(),
    });
    terminal.session = "session-B".into();
    c.inbox.persist_at(&terminal, &CandidateGeneration::Unknown, Utc::now())?;
    Ok((c, store))
}

#[test]
fn reviewer_native_pending_limit_advances_past_old_invalid_admission() -> Result<()> {
    let (c, store) = reviewer_two_native_decisions(false)?;
    let visible = store.list_native_buy_pending(2)?;
    assert_eq!(visible.len(), 1, "control: newer admission is eligible");
    assert_eq!(visible[0].signature, "source-buy-new");
    let first = store.list_native_buy_pending(1)?;
    let reopened = c.store()?;
    let second = reopened.list_native_buy_pending(1)?;
    assert!(first.is_empty() || first[0].signature == "source-buy-new");
    assert_eq!(second[0].signature, "source-buy-new");
    Ok(())
}

#[test]
fn reviewer_native_finalized_limit_advances_past_old_invalid_admission() -> Result<()> {
    let (c, store) = reviewer_two_native_decisions(true)?;
    assert!(store.native_buy_record_finalized(
        "source-buy-new", 7, SPL_TOKEN_PROGRAM, Utc::now()
    )?);
    let visible = store.list_native_buy_finalized(2, Utc::now(), 10)?;
    assert_eq!(visible.len(), 1, "control: newer finalized admission is eligible");
    assert_eq!(visible[0].signature, "source-buy-new");
    let first = store.list_native_buy_finalized(1, Utc::now(), 10)?;
    let reopened = c.store()?;
    let second = reopened.list_native_buy_finalized(1, Utc::now(), 10)?;
    assert!(first.is_empty() || first[0].signature == "source-buy-new");
    assert_eq!(second[0].signature, "source-buy-new");
    Ok(())
}
