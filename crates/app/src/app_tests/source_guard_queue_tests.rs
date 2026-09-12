use super::source_guard_fixture::Fixture;
use anyhow::Result;
use copybot_storage_core::*;

async fn queue(candidate: bool, with_a: bool) -> Result<()> {
    let mut f = Fixture::legacy_parent().await?;
    f.config.canary_batch_limit = 1;
    let a = if with_a { Some(f.retry()?) } else { None };
    f.f.replace(4000)?;
    let b = super::source_sell_sweep_fixture::add_signal(&f.f, "valid-b")?;
    f.f.signal = b;
    f.event_id = "quote:valid-b".into();
    let b_id = f.retry()?;
    if candidate {
        for (index, id) in [Some(b_id.clone()), a.clone()]
            .into_iter()
            .flatten()
            .enumerate()
        {
            let at = f.f.now + chrono::Duration::seconds(2 + index as i64);
            f.f.store.mark_execution_canary_failed(
                &id,
                at,
                EXECUTION_ERROR_BUILD_FAILED,
                "transient quote failure",
            )?;
            f.f.store
                .mark_execution_canary_failed_build_retry_candidate(
                    &id,
                    at,
                    "retry_failed_sell_with_owned_position_amount",
                )?;
        }
    }
    let before_a = a
        .as_ref()
        .map(|id| f.f.store.load_execution_canary_order(id).unwrap().unwrap());
    let mut refusals = 0;
    let mut last = None;
    for _ in 0..3 {
        let out = if candidate {
            crate::execution_canary_route::process_failed_sell_simulation_sweep(
                &f.config,
                &f.f.store,
                f.f.now + chrono::Duration::seconds(5),
            )
            .await?
            .unwrap()
        } else {
            f.retry_sweep().await?
        };
        refusals += out.source_sell_refusals.count();
        if out.signing_envelope_built > 0 {
            last = Some(out);
            break;
        }
    }
    f.finish().await?;
    assert_eq!(
        f.rpc.count("sendTransaction"),
        1,
        "B must progress in family candidate={candidate}, A={with_a}"
    );
    let last = last.expect("B signing must be reported");
    assert_eq!(last.last_order_id.as_deref(), Some(b_id.as_str()));
    if let Some(a) = a {
        assert!(refusals > 0);
        assert_eq!(f.f.store.load_execution_canary_order(&a)?, before_a);
        assert_eq!(last.source_sell_refusals.order_id(), a);
        let mut tick = crate::execution_canary::ExecutionCanaryTickSummary::default();
        crate::execution_canary_summary::apply_state_machine_summary(&mut tick, last);
        let event = super::submit_refusal_fixture::capture(|| {
            crate::telemetry::record_execution_canary_tick(&tick)
        });
        assert_eq!(event["source_sell_refusal_id"], a);
        assert_eq!(
            event["source_sell_refusal_reason"],
            "source_sell_generation_mismatch"
        );
    }
    Ok(())
}
#[tokio::test]
async fn source_guard_priority_candidate_a_cannot_starve_b() -> Result<()> {
    queue(true, true).await
}
#[tokio::test]
async fn source_guard_simulated_not_sent_a_cannot_starve_b() -> Result<()> {
    queue(false, true).await
}
#[tokio::test]
async fn source_guard_early_queue_b_without_a_controls() -> Result<()> {
    queue(true, false).await?;
    queue(false, false).await
}
