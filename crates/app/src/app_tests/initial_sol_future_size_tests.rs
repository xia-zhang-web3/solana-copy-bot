use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;
use std::future::Future;

fn future_size<F: Future>(_: impl FnOnce() -> F) -> usize {
    std::mem::size_of::<F>()
}

#[tokio::test]
async fn initial_sol_future_footprint() -> Result<()> {
    let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let intent = crate::execution_submit_adapter::execution_submit_intent_from_signed_envelope(
        &f.request,
        &envelope,
        "synthetic".into(),
    )?;
    let gate =
        crate::execution_canary_submit_contract::ExecutionTinySubmitGate::from_config(&f.config);
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(
        f.config.submit_adapter_http_url.clone(),
    );
    let state = crate::execution_tiny_submit_state::eligible(&f.store, &f.request)
        .map_err(|e| anyhow::anyhow!(e))?;
    let integration = future_size(|| {
        crate::execution_initial_sol_submit::before_send(
            &f.store, &f.request, &envelope, &intent, &gate, &transport, &state, f.now,
        )
    });
    let collector =
        future_size(|| crate::execution_initial_sol::collect_and_check("", 1000, "", [0; 32], 1));
    assert!(
        integration <= 1024,
        "keep funding await out of nested daemon stack: {integration}"
    );
    eprintln!("B26_FUTURE_BYTES integration={integration} collector={collector}");
    f.finish().await?;
    Ok(())
}
