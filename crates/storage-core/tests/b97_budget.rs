#[path = "common/b97_automatic.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
use f::*;

#[test]
fn b97_exact_count_byte_boundary_existing_replay_and_restart_do_not_bypass_cap() -> Result<()> {
    for bytes in [false, true] {
        let mut f = new()?;
        f.anchors()?;
        f.sell()?;
        f.drain()?;
        let prior = f.inbox.usage()?;
        let event = pulse(0);
        let row_bytes = 512 + event.session.len() + serde_json::to_vec(&event)?.len();
        let l = if bytes {
            InboxLimits {
                bytes: prior.1 + row_bytes,
                ..limits()
            }
        } else {
            InboxLimits {
                count: prior.0 + 1,
                ..limits()
            }
        };
        let mut inbox = open(&f, l)?;
        exhaust(&mut inbox)?;
        inbox.persist(&event, &CandidateGeneration::Unknown)?;
        exhaust(&mut inbox)?;
        let full = inbox.usage()?;
        assert_eq!(
            if bytes { full.1 } else { full.0 },
            if bytes { l.bytes } else { l.count }
        );
        let kept = protocol(&f)?;
        assert!(inbox
            .persist(&pulse(1), &CandidateGeneration::Unknown)
            .is_err());
        assert_eq!(protocol(&f)?, kept);
        // Same event at exact limit has no new event/claim/intent charge.
        inbox.persist(&event, &CandidateGeneration::Unknown)?;
        assert_eq!(inbox.usage()?, full);
        inbox = open(&f, l)?;
        exhaust(&mut inbox)?;
        assert_eq!(inbox.usage()?, full);
        assert_eq!(protocol(&f)?, kept);
        assert!(inbox
            .persist(&pulse(1), &CandidateGeneration::Unknown)
            .is_err());
        pair(&f, 1)?;
        let smaller = if bytes {
            InboxLimits {
                bytes: full.1 - 1,
                ..l
            }
        } else {
            InboxLimits {
                count: full.0 - 1,
                ..l
            }
        };
        assert!(open(&f, smaller).is_err());
        assert_eq!(protocol(&f)?, kept);
    }
    Ok(())
}

#[test]
fn b97_automatic_strict_blobs_are_charged_but_legacy_claims_are_excluded() -> Result<()> {
    let mut f = new()?;
    f.anchors()?;
    f.sell()?;
    f.drain()?;
    let automatic = f.inbox.usage()?;
    let observation = AssociationInbox::open(&f.db.path, limits())?.usage()?;
    assert_eq!(automatic.0, observation.0 + 2);
    let c = f.db.conn()?;
    let bytes:i64 = c.query_row("SELECT (SELECT 512+length(CAST(intent_id AS BLOB))+length(CAST(signature AS BLOB))+length(CAST(policy AS BLOB))+length(CAST(record AS BLOB)) FROM ordered_source_sell_intents) + (SELECT 512+length(CAST(signature AS BLOB))+length(CAST(owner AS BLOB))+length(CAST(intent_id AS BLOB)) FROM source_sell_signature_claims)", [], |r|r.get(0))?;
    assert_eq!(automatic.1, observation.1 + usize::try_from(bytes)?);
    c.execute("INSERT INTO source_sell_signature_claims VALUES('legacy-other','legacy','source-sell:legacy-other')", [])?;
    assert_eq!(f.inbox.usage()?, automatic);
    Ok(())
}

#[test]
fn b97_stage_overflow_rolls_back_cursor_and_remains_error_after_reopen() -> Result<()> {
    for bytes in [false, true] {
        let mut f = within()?;
        let initial = f.inbox.usage()?;
        // Enough for all already retained evidence; insufficient for new strict blobs.
        let l = if bytes {
            InboxLimits {
                bytes: initial.1 + 1,
                ..limits()
            }
        } else {
            InboxLimits {
                count: initial.0 + 1,
                ..limits()
            }
        };
        for _ in 0..2 {
            let mut i = open(&f, l)?;
            let mut failed = false;
            for _ in 0..10 {
                let before = protocol(&f)?;
                match i.recover_sell_preparation() {
                    Ok(()) => {}
                    Err(e) => {
                        assert!(e.to_string().contains("inbox full"), "{e:#}");
                        assert_eq!(protocol(&f)?, before);
                        assert!(i.has_sell_preparation_work()?);
                        failed = true;
                        break;
                    }
                }
            }
            assert!(failed);
            pair(&f, 0)?;
            assert_eq!(i.usage()?, initial);
        }
        // Same retained database can still be read through its unchanged explicit API.
        f.inbox = AssociationInbox::open(&f.db.path, limits())?;
        pair(&f, 0)?;
    }
    Ok(())
}

#[test]
fn b97_wake_dependency_is_charged_and_ignore_aborts_whole_preparation() -> Result<()> {
    let mut f = new()?;
    insert(&f, &facts("origin", "leader", true))?;
    f.anchors()?;
    f.db.conn()?.execute_batch("CREATE TRIGGER wake_ignore BEFORE INSERT ON association_sell_dependencies WHEN NEW.anchor_signature='origin' BEGIN SELECT RAISE(IGNORE); END;")?;
    let before = protocol(&f)?;
    assert!(f.admit(facts("sell", "leader", false)).is_err());
    assert_eq!(protocol(&f)?, before);
    pair(&f, 0)?;
    Ok(())
}
