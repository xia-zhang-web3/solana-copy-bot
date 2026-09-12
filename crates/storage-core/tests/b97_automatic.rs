#[path = "common/b97_automatic.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::association_inbox::AssociationInbox;
use f::*;

#[test]
fn b97_explicit_observation_unchanged_automatic_direct_and_bootstrap_once() -> Result<()> {
    let mut manual = within()?;
    pair(&manual, 0)?;
    let old = manual.read()?;
    let before = money(&manual)?;
    automatic(&mut manual)?;
    assert!(manual.drain()? > 0);
    pair(&manual, 1)?;
    assert_eq!(intent(&manual, "sell")?.unwrap().first, old.first);
    assert_eq!(manual.read()?.historical_initial, old.historical_initial);
    assert_eq!(money(&manual)?, before);
    let saved = intent(&manual, "sell")?;
    for _ in 0..2 {
        automatic(&mut manual)?;
        manual.drain()?;
        pair(&manual, 1)?;
        assert_eq!(intent(&manual, "sell")?, saved);
    }
    let mut f = new()?;
    f.anchors()?;
    let before = money(&f)?;
    f.sell()?;
    pair(&f, 1)?; // direct event, no explicit stage
    f.drain()?;
    assert_eq!(money(&f)?, before);
    assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    Ok(())
}

#[test]
fn b97_late_source_receipt_and_last_parent_stage_without_new_sell() -> Result<()> {
    let mut f = new()?;
    f.sell()?;
    f.drain()?;
    pair(&f, 0)?;
    let first = f.read()?.first;
    f.anchors()?;
    f.drain()?;
    pair(&f, 1)?;
    assert_eq!(intent(&f, "sell")?.unwrap().first, first);
    let mut f = new()?;
    anchors(&mut f, true)?;
    pair(&f, 0)?;
    for e in graph() {
        put(&mut f, e)?;
    }
    f.drain()?;
    pair(&f, 1)?;
    assert_eq!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    Ok(())
}

#[test]
fn b97_late_shadow_origin_wakes_without_changing_first_or_selected_witness() -> Result<()> {
    let mut f = new()?;
    let origin = facts("late-origin", "leader", true);
    let lot = insert(&f, &origin)?; // existing lot + known signature before SELL
    let before = money(&f)?;
    f.anchors()?;
    f.sell()?;
    f.drain()?;
    pair(&f, 0)?;
    let first = f.read()?.first;
    assert_eq!(f.db.conn()?.query_row("SELECT first_identity IS NULL FROM association_sell_dependencies WHERE sell_signature='sell' AND anchor_signature='late-origin'", [], |r| r.get::<_,bool>(0))?, true);
    f.admit(origin.clone())?;
    f.terminal(&origin, 4, 42, "block")?;
    f.drain()?;
    pair(&f, 1)?;
    let saved = intent(&f, "sell")?.unwrap();
    assert_eq!(saved.first, first);
    assert_eq!(saved.staged_evaluation.shadow.unwrap().lots[0].lot_id, lot);
    assert_eq!(money(&f)?, before);
    f.conflict("late-origin")?;
    f.drain()?;
    pair(&f, 1)?;
    assert_ne!(fresh(&f)?, OrderedSellDecision::ValidatedNow);
    assert_eq!(intent(&f, "sell")?.unwrap().first, first);
    Ok(())
}

#[test]
fn b97_many_dependents_unknown_a_does_not_stall_b_and_idle_does_not_spin() -> Result<()> {
    let mut f = new()?;
    let unknown = facts("A-unknown", "leader", false);
    f.event(
        DeliveryEvent::Admission(unknown.clone()),
        CandidateGeneration::Unknown,
    )?;
    f.terminal(&unknown, 3, 42, "block")?;
    for n in 0..17 {
        let a = facts(&format!("B-{n:02}"), "leader", false);
        f.admit(a.clone())?;
        f.terminal(&a, 3, 42, "block")?;
    }
    f.drain()?;
    pair(&f, 0)?;
    f.anchors()?;
    let mut turns = 0;
    let mut prior = count(&f, "ordered_source_sell_intents")?;
    while f.inbox.has_sell_preparation_work()? {
        f.inbox.recover_sell_preparation()?;
        let next = count(&f, "ordered_source_sell_intents")?;
        assert!(
            (0..=1).contains(&(next - prior)),
            "one dependent per recovery turn"
        );
        prior = next;
        turns += 1;
        assert!(turns < 100);
    }
    assert!(turns >= 17);
    pair(&f, 17)?;
    assert!(intent(&f, "A-unknown")?.is_none());
    let idle = protocol(&f)?;
    for _ in 0..3 {
        f.inbox.recover_sell_preparation()?;
    }
    assert_eq!(protocol(&f)?, idle);
    f.conflict("leaderbuy")?;
    assert!(f.inbox.has_sell_preparation_work()?);
    f.drain()?;
    pair(&f, 17)?;
    assert_ne!(
        f.inbox
            .revalidate_ordered_source_sell_intent("source-sell:B-00")?,
        OrderedSellDecision::ValidatedNow
    );
    Ok(())
}

#[test]
fn b97_restart_pending_anchor_unknown_first_and_generation_changes_refuse() -> Result<()> {
    for mode in ["pending", "unknown", "generation"] {
        let mut f = new()?;
        if mode != "pending" {
            f.anchors()?;
        }
        let a = facts("sell", "leader", false);
        let candidate = if mode == "unknown" {
            CandidateGeneration::Unknown
        } else {
            f.db.store.association_candidate(&a.facts)
        };
        f.event(DeliveryEvent::Admission(a.clone()), candidate)?;
        if mode == "generation" {
            f.db.conn()?
                .execute("UPDATE positions SET position_id='B'", [])?;
        }
        if mode == "pending" {
            f.admit(facts("leaderbuy", "leader", true))?;
        }
        automatic(&mut f)?; // pending identity recovery is irreversible
        f.terminal(&a, 3, 42, "block")?;
        if mode == "pending" {
            f.anchors()?;
        }
        f.drain()?;
        pair(&f, 0)?;
    }
    // Explicit open does not stage even when strict schema/history exists.
    let mut f = within()?;
    f.inbox = AssociationInbox::open(&f.db.path, limits())?;
    f.drain()?;
    pair(&f, 0)?;
    Ok(())
}

#[test]
fn b97_fresh_financial_and_shadow_changes_before_attempt_refuse() -> Result<()> {
    for mutation in ["lot", "financial", "conflict"] {
        let mut f = within()?; // retained positive, observation-only
        match mutation {
            "lot" => {
                f.db.store
                    .insert_shadow_lot("leader", "mint", 1.0, 1.0, f.db.now)?;
            }
            "financial" => {
                f.db.conn()?
                    .execute("UPDATE orders SET status='execution_canary_submitted'", [])?;
            }
            _ => {
                f.conflict("leaderbuy")?;
            }
        }
        let before = money(&f)?;
        automatic(&mut f)?;
        f.drain()?;
        pair(&f, 0)?;
        assert_eq!(money(&f)?, before);
    }
    Ok(())
}
