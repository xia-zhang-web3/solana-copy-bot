use super::source_guard_fixture::Fixture;
use anyhow::Result;

#[tokio::test]
async fn source_guard_stale_owned_quote_stops_before_provider() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.f.replace(4000)?;
    let before = f.state()?;
    let result = f.owned_quote().await;
    f.finish().await?;
    let result = result?;
    assert_eq!(f.rpc.count("quote"), 0, "{result:?}");
    assert_eq!(before, f.state()?);
    Ok(())
}

#[tokio::test]
async fn source_guard_stale_failed_retry_does_not_mutate_order() -> Result<()> {
    let mut f = Fixture::new().await?;
    super::source_write_off_fixture::fail_order(
        &f.f.store,
        &f.f.signal.signal_id,
        &f.config.canary_route,
        f.f.now,
        false,
        1,
    )?;
    f.f.replace(4000)?;
    let before = f.state()?;
    let result = f.tiny().await;
    f.finish().await?;
    result?;
    assert_eq!(before, f.state()?);
    assert_eq!(f.rpc.count("getTokenAccountsByOwner"), 0);
    Ok(())
}

#[tokio::test]
async fn source_guard_generation_change_during_amount_never_starts_quote() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.rpc.state.lock().unwrap().mutate_at = Some("getTokenAccountsByOwner");
    let result = f.tiny().await;
    f.finish().await?;
    let result = result?;
    assert_eq!(f.rpc.count("quote"), 0, "{result:?}");
    assert_eq!(
        f.rpc.state.lock().unwrap().after_mutation.as_ref(),
        Some(&f.state()?)
    );
    Ok(())
}

#[tokio::test]
async fn source_guard_generation_change_during_simulation_preserves_post_await_state() -> Result<()>
{
    for retry in [false, true] {
        let mut f = Fixture::new().await?;
        if retry {
            f.retry()?;
        }
        f.rpc.state.lock().unwrap().mutate_at = Some("simulateTransaction");
        let result = if retry {
            f.retry_sweep().await
        } else {
            f.tiny().await
        };
        f.finish().await?;
        let result = result?;
        assert_eq!(
            result.signing_envelope_built, 0,
            "retry={retry}: {result:?}"
        );
        assert_eq!(f.rpc.count("sendTransaction"), 0);
        assert_eq!(
            f.rpc.state.lock().unwrap().after_mutation.as_ref(),
            Some(&f.state()?)
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_guard_valid_promoted_tiny_reaches_actual_signed_submit_control() -> Result<()> {
    let mut f = Fixture::new().await?;
    let result = f.tiny().await;
    f.finish().await?;
    let result = result?;
    assert_eq!(result.signing_envelope_built, 1, "{result:?}");
    assert_eq!(f.rpc.count("sendTransaction"), 1);
    Ok(())
}
