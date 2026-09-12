use super::source_sell_ingress_fixture::Ingress;
use crate::source_sell_staging::StageNotice;
use anyhow::Result;
use copybot_storage_core::ExecutionSourceSellReject as Reject;
use std::sync::Arc;

#[tokio::test]
async fn source_sell_ingress_captures_position_before_durable_ack_changes_state() -> Result<()> {
    let mut f = Ingress::new()?;
    f.buy("a", "source-a")?;
    let event = f.sell("ack-boundary", "source-a");
    // Test-only writer-side state change at the commit/ACK boundary. No source
    // membership is injected: the original BUY was confirmed by the real writer.
    f.conn()?.execute_batch(
        "CREATE TRIGGER close_during_observation AFTER INSERT ON observed_swaps
        BEGIN UPDATE positions SET state='closed' WHERE token='mint'; END;",
    )?;
    f.send(&event, true).await?;
    assert_eq!(
        f.stage_completion().await?.notice,
        StageNotice::Rejected(Reject::NoOwnedPosition)
    );
    assert!(f.staged(&event.signature)?.is_none());
    f.finish().await?;
    Ok(())
}

#[tokio::test]
async fn source_sell_ingress_followed_demoted_empty_restart_and_publication_closed_stage(
) -> Result<()> {
    for mode in ["followed", "demoted", "empty_restart", "publication_closed"] {
        let mut f = Ingress::new()?;
        let order = f.buy("buy-a", "source-a")?;
        let position = f.position()?;
        f.follow_source("source-a")?;
        if mode != "followed" {
            f.store
                .deactivate_follow_wallet("source-a", f.now, "demoted")?;
            let follow = Arc::make_mut(&mut f.follow);
            follow.active.clear();
            follow.demoted_at.insert("source-a".into(), f.now);
        }
        if matches!(mode, "empty_restart" | "publication_closed") {
            f.reopen()?;
        }
        let mut event = f.sell("source-sell-a", "source-a");
        // Valid original SELL; follower shadow sizing rounds its copy below one
        // raw unit. Legacy's existing InvalidSizing gate has no runnable effect.
        event.amount_in = 0.001;
        event.exact_amounts.as_mut().unwrap().amount_in_raw = "1".into();
        let before = f.money()?;
        f.send(&event, mode == "publication_closed").await?;
        let completion = f.stage_completion().await?;
        assert_eq!(completion.notice, StageNotice::Staged, "{mode}");
        assert_eq!(completion.signature, event.signature);
        f.shadow_completion().await?;
        let row = f.staged(&event.signature)?.unwrap();
        assert_eq!(row.position_id, position);
        assert_eq!(row.buy_witness.order_id, order);
        assert_eq!(row.buy_witness.source_wallet, event.wallet);
        assert_eq!(row.buy_witness.tx_signature, "receipt:buy-a");
        assert_eq!(format!("{:?}", row.event), format!("{event:?}"));
        assert_eq!(f.money()?, before, "{mode}");
        f.reopen()?;
        assert_eq!(
            format!("{:?}", f.staged(&event.signature)?.unwrap()),
            format!("{row:?}")
        );
        f.send(&event, true).await?;
        assert_eq!(f.stage_completion().await?.notice, StageNotice::Existing);
        assert_eq!(f.money()?, before);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_ingress_unknown_foreign_buy_and_nonowned_do_not_gain_authority() -> Result<()>
{
    for case in [
        "unknown",
        "foreign",
        "signer",
        "buy",
        "non_owned",
        "unclassified",
    ] {
        let mut f = Ingress::new()?;
        if case == "unknown" {
            f.store.record_execution_canary_open_position(
                "imported", "mint", 7.0, None, 0.01, f.now,
            )?;
        } else {
            f.buy("buy-a", "source-a")?;
        }
        let mut event = f.sell(
            "control",
            if case == "foreign" {
                "other"
            } else if case == "signer" {
                "execution-wallet"
            } else {
                "source-a"
            },
        );
        match case {
            "buy" => std::mem::swap(&mut event.token_in, &mut event.token_out),
            "non_owned" => event.token_in = "not-owned".into(),
            "unclassified" => event.token_out = "other-token".into(),
            _ => {}
        }
        let before = f.money()?;
        f.send(&event, true).await?;
        if matches!(case, "unknown" | "foreign" | "signer") {
            assert_eq!(
                f.stage_completion().await?.notice,
                StageNotice::Rejected(Reject::SourceNotProven)
            );
        } else {
            assert!(f.scheduler.source_sells.is_empty());
        }
        assert!(f.staged(&event.signature)?.is_none());
        assert_eq!(f.money()?, before, "{case}");
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_ingress_generation_is_captured_before_worker_and_not_retargeted_on_retry(
) -> Result<()> {
    for later in [false, true] {
        let mut f = Ingress::new()?;
        f.buy("a", "source-a")?;
        let old = f.position()?;
        let event = f.sell("old-event", "source-a");
        let release = f.pause_worker();
        f.send(&event, true).await?;
        // Durable acknowledgement already arrived; only the proof worker is paused.
        assert_eq!(
            f.conn()?.query_row(
                "SELECT count(*) FROM observed_swaps WHERE signature=?1",
                [&event.signature],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
        f.store
            .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close-a", f.now)?;
        if later {
            f.now += chrono::Duration::seconds(2);
        }
        f.buy("b", "source-a")?;
        let new = f.position()?;
        assert_ne!(old, new);
        release.send(())?;
        assert_eq!(
            f.stage_completion().await?.notice,
            StageNotice::Rejected(Reject::GenerationMismatch)
        );
        f.send(&event, true).await?;
        assert_eq!(
            f.stage_completion().await?.notice,
            StageNotice::Rejected(Reject::GenerationMismatch)
        );
        assert!(f.staged(&event.signature)?.is_none());
        let new_event = f.sell("new-event", "source-a");
        f.send(&new_event, true).await?;
        assert_eq!(f.stage_completion().await?.notice, StageNotice::Staged);
        assert_eq!(f.staged(&new_event.signature)?.unwrap().position_id, new);
        f.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn source_sell_ingress_first_persistence_does_not_hide_legacy_owned_or_shadow_sell(
) -> Result<()> {
    for shadow_lot in [false, true] {
        let mut f = Ingress::new()?;
        f.buy("a", "source-a")?;
        f.follow_source("source-a")?;
        let mut event = f.sell("legacy", "source-a");
        if shadow_lot {
            f.store
                .insert_shadow_lot("source-a", "mint", 7.0, 0.7, f.now)?;
            f.lots.insert(("source-a".into(), "mint".into()));
        } else {
            event.amount_out = 0.0000001; // Below legacy entry minimum => owned intent fallback.
            event.exact_amounts.as_mut().unwrap().amount_out_raw = "100".into();
        }
        f.send(&event, false).await?;
        let first_notice = f.stage_completion().await?.notice; // Staged or independently rejected by existing guards.
        let signal = f
            .shadow_completion()
            .await?
            .expect("first legacy delivery must run");
        let saved = f
            .store
            .load_copy_signal_by_signal_id(&signal.signal_id)?
            .unwrap();
        assert_eq!(
            saved.status,
            if shadow_lot {
                "shadow_recorded"
            } else {
                "execution_sell_intent"
            }
        );
        let after_first = f.money()?;
        f.send(&event, false).await?;
        assert_eq!(
            f.stage_completion().await?.notice,
            if first_notice == StageNotice::Staged {
                assert!(!shadow_lot);
                StageNotice::Existing // Exact committed delivery is idempotent; no new authority.
            } else {
                let expected = StageNotice::Rejected(if shadow_lot {
                    Reject::ShadowRiskPresent
                } else {
                    Reject::SignalAlreadyExists
                });
                assert_eq!(first_notice, expected);
                expected
            }
        );
        assert!(f.shadow_completion().await?.is_none());
        assert_eq!(f.money()?, after_first);
        f.finish().await?;
    }
    Ok(())
}
