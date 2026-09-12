use super::source_guard_fixture::Fixture;
use crate::execution_signing_envelope::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Spy {
    inner: JupiterMetisDryRunExecutionAdapter,
    signed: AtomicUsize,
    cap: u64,
}
impl ExecutionSubmitAdapter for Spy {
    fn build_transaction_plan(
        &self,
        r: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        self.inner.build_transaction_plan(r)
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        p: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        self.inner.simulate_transaction_plan(p)
    }
    fn native_floor_config(&self) -> Result<&copybot_config::ExecutionConfig> {
        self.inner.native_floor_config()
    }
    fn priority_fee_cap(&self) -> u64 {
        self.cap
    }
    fn sign_serialized_transaction(
        &self,
        r: &ExecutionSubmitRequest,
        p: &ExecutionTransactionPlan,
        b: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        self.signed.fetch_add(1, Ordering::SeqCst);
        self.inner.sign_serialized_transaction(r, p, b)
    }
    fn plan_submit(&self, r: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        self.inner.plan_submit(r)
    }
    fn plan_submit_with_envelope(
        &self,
        r: &ExecutionSubmitRequest,
        e: &ExecutionSigningEnvelope,
    ) -> Result<ExecutionSubmitPlan> {
        self.inner.plan_submit_with_envelope(r, e)
    }
}
async fn prepared(f: &Fixture) -> Result<(ExecutionSubmitRequest, ExecutionTransactionPlan, Spy)> {
    let request = f.request()?;
    let spy = Spy {
        inner: JupiterMetisDryRunExecutionAdapter::new(f.config.clone()),
        signed: AtomicUsize::new(0),
        cap: f.config.pretrade_max_priority_fee_lamports,
    };
    let plan = spy.build_transaction_plan(&request)?;
    crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
        &f.f.store, &plan, f.f.now,
    )?;
    f.f.store
        .mark_execution_canary_built(&request.order_id, f.f.now)?;
    let simulation = spy.simulate_transaction_plan(&plan).await?;
    assert_eq!(
        simulation.status,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED
    );
    f.f.store.mark_execution_canary_simulated(
        &request.order_id,
        f.f.now,
        &simulation.status,
        None,
    )?;
    Ok((request, plan, spy))
}
#[tokio::test]
async fn source_guard_direct_signing_contract_stale_never_calls_underlying_signer() -> Result<()> {
    let mut f = Fixture::new().await?;
    let (request, plan, spy) = prepared(&f).await?;
    f.f.replace(4000)?;
    let before = f.state()?;
    let out = crate::execution_canary_signing_contract::record_execution_signing_envelope(
        &f.f.store, &spy, &request, &plan, f.f.now,
    );
    f.finish().await?;
    let out = out?;
    assert_eq!(spy.signed.load(Ordering::SeqCst), 0, "{out:?}");
    assert!(out.envelope.is_none());
    assert_eq!(f.state()?, before);
    Ok(())
}
#[tokio::test]
async fn source_guard_direct_signing_valid_control_and_existing_final_submit_guard() -> Result<()> {
    for stale in [false, true] {
        let mut f = Fixture::new().await?;
        let (request, plan, spy) = prepared(&f).await?;
        let out = crate::execution_canary_signing_contract::record_execution_signing_envelope(
            &f.f.store, &spy, &request, &plan, f.f.now,
        )?;
        assert_eq!(spy.signed.load(Ordering::SeqCst), 1, "{out:?}");
        let envelope = out
            .envelope
            .expect("valid payload and fee proof actually signed");
        if stale {
            f.f.replace(4000)?;
        }
        let submit = crate::execution_canary_submit_contract::record_execution_tiny_submit_plan(
            &f.f.store,
            &spy,
            &request,
            &envelope,
            &crate::execution_canary_submit_contract::ExecutionTinySubmitGate::from_config(
                &f.config,
            ),
            &RpcExecutionSubmitTransport::new(f.config.submit_adapter_http_url.clone()),
            f.f.now,
        )
        .await;
        f.finish().await?;
        let submit = submit?;
        assert_eq!(
            f.rpc.count("sendTransaction"),
            usize::from(!stale),
            "{submit:?}"
        );
        if stale {
            assert_eq!(
                submit.reason.as_deref(),
                Some("source_sell_generation_mismatch")
            );
        }
    }
    Ok(())
}
#[tokio::test]
async fn source_guard_cached_not_sent_stale_stops_before_new_work() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.retry()?;
    f.f.replace(4000)?;
    let before = f.state()?;
    let out = f.retry_sweep().await;
    f.finish().await?;
    let out = out?;
    assert_eq!(f.rpc.count("simulateTransaction"), 0, "{out:?}");
    assert_eq!(out.signing_envelope_built, 0);
    assert_eq!(f.rpc.count("sendTransaction"), 0);
    assert_eq!(f.state()?, before);
    Ok(())
}
#[tokio::test]
async fn source_guard_retry_simulation_race_stops_before_signing() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.retry()?;
    f.rpc.state.lock().unwrap().mutate_at = Some("simulateTransaction");
    let out = f.retry_sweep().await;
    f.finish().await?;
    let out = out?;
    assert_eq!(out.signing_envelope_built, 0, "{out:?}");
    assert_eq!(
        f.rpc.state.lock().unwrap().after_mutation.as_ref(),
        Some(&f.state()?)
    );
    Ok(())
}

#[tokio::test]
async fn source_guard_legacy_sell_signing_control_uses_durable_unpromoted_signal() -> Result<()> {
    let mut f = Fixture::new().await?;
    // Create a separate ordinary legacy signal, keeping the proven BUY and promoted A intact.
    let mut legacy = f.f.signal.clone();
    legacy.signal_id = "legacy-sell-control".into();
    legacy.status = "shadow_recorded".into();
    assert!(f.f.store.insert_copy_signal(&legacy)?);
    let mut quote = super::source_write_off_fixture::quote(&legacy, f.f.now);
    quote.event_id = "quote:legacy-sell-control".into();
    f.f.store.record_execution_quote_canary_event(&quote)?;
    f.f.signal = legacy;
    f.event_id = quote.event_id;
    let (request, plan, spy) = prepared(&f).await?;
    let out = crate::execution_canary_signing_contract::record_execution_signing_envelope(
        &f.f.store, &spy, &request, &plan, f.f.now,
    );
    f.finish().await?;
    let out = out?;
    assert_eq!(spy.signed.load(Ordering::SeqCst), 1, "{out:?}");
    assert!(out.envelope.is_some());
    Ok(())
}

#[tokio::test]
async fn source_guard_signing_does_not_trust_caller_buy_or_mixed_plan() -> Result<()> {
    for mutation in ["side", "signal", "plan"] {
        let mut f = Fixture::new().await?;
        let (mut request, mut plan, spy) = prepared(&f).await?;
        match mutation {
            "side" => request.side = "buy".into(),
            "signal" => request.signal_id = "other-signal".into(),
            _ => plan.token = "other-mint".into(),
        }
        let before = f.state()?;
        let out = crate::execution_canary_signing_contract::record_execution_signing_envelope(
            &f.f.store, &spy, &request, &plan, f.f.now,
        );
        f.finish().await?;
        let out = out?;
        assert!(out.source_refusal.is_some(), "{mutation}: {out:?}");
        assert_eq!(spy.signed.load(Ordering::SeqCst), 0);
        assert!(out.envelope.is_none());
        assert_eq!(f.state()?, before);
    }
    Ok(())
}
