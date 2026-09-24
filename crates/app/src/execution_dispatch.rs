//! Final synchronous identity/claim boundary followed immediately by one transport.
use super::SubmitState;
use crate::execution_canary_submit_contract::{
    ExecutionSubmitPlanOutcome, ExecutionTinySubmitGate,
};
use crate::execution_submit_adapter::{
    build_submit_transport_attempt, ExecutionSubmitIntent, ExecutionSubmitRequest,
    ExecutionSubmitTransportOutcome, RpcExecutionSubmitTransport,
};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use copybot_storage_core::{ExecutionCanaryDispatch, ExecutionDispatchClaim, SqliteStore};

pub(crate) fn identity(
    request: &ExecutionSubmitRequest,
    intent: &ExecutionSubmitIntent,
) -> Result<ExecutionCanaryDispatch> {
    let binding = crate::execution_transaction_wire::decode_message(
        &intent.signed_transaction_base64,
        |_| Ok(()),
    )?
    .binding;
    let bytes = STANDARD.decode(&intent.signed_transaction_base64)?;
    let signature_end = bytes.len() - binding.message_bytes.len();
    let signature_start = signature_end - binding.signature_count * 64;
    ensure!(
        bs58::encode(binding.accounts[0].pubkey).into_string() == request.wallet_pubkey,
        "dispatch_wallet_mismatch"
    );
    for (signature, key) in bytes[signature_start..signature_end]
        .chunks_exact(64)
        .zip(&binding.accounts)
    {
        ed25519_dalek::VerifyingKey::from_bytes(&key.pubkey)?.verify_strict(
            &binding.message_bytes,
            &ed25519_dalek::Signature::from_slice(signature)?,
        )?;
    }
    let signature = bs58::encode(&bytes[signature_start..signature_start + 64]).into_string();
    ensure!(
        intent
            .tx_signature_hint
            .as_deref()
            .is_none_or(|hint| hint == signature),
        "dispatch_hint_payload_mismatch"
    );
    Ok(ExecutionCanaryDispatch {
        order_id: request.order_id.clone(),
        signal_id: request.signal_id.clone(),
        client_order_id: request.client_order_id.clone(),
        route: request.route.clone(),
        attempt: request.attempt,
        wallet: request.wallet_pubkey.clone(),
        token: request.token.clone(),
        side: request.side.to_ascii_lowercase(),
        tx_signature: signature,
        transaction_sha256: binding.transaction_sha256,
        message_sha256: binding.message_sha256,
    })
}

pub(crate) async fn send(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    intent: &ExecutionSubmitIntent,
    envelope: &crate::execution_signing_envelope::ExecutionSigningEnvelope,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    state: &SubmitState,
    now: DateTime<Utc>,
) -> Result<ExecutionSubmitPlanOutcome> {
    send_guarded(store, request, intent, envelope, gate, transport, state, now, None).await
}

pub(crate) async fn send_guarded(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    intent: &ExecutionSubmitIntent,
    envelope: &crate::execution_signing_envelope::ExecutionSigningEnvelope,
    gate: &ExecutionTinySubmitGate,
    transport: &RpcExecutionSubmitTransport,
    state: &SubmitState,
    now: DateTime<Utc>,
    native: Option<&crate::execution_canary_route::NativeBuyGuard>,
) -> Result<ExecutionSubmitPlanOutcome> {
    let identity = identity(request, intent)?;
    // Existing identity is reconciliation-only even after stop/config removal/deadline.
    if let Some(old) = store.load_execution_canary_dispatch(&request.order_id)? {
        ensure!(old == identity, "dispatch_identity_conflict");
        return Ok(ExecutionSubmitPlanOutcome {
            submitted: 1,
            tx_signature: Some(identity.tx_signature),
            reason: Some("dispatch_existing_reconcile_only".into()),
            ..Default::default()
        });
    }
    let tick_at = now;
    let (budget, now) = match super::budget::prepare(
        store, request, intent, envelope, gate, transport, state, now, native,
    )
    .await
    {
        Ok(proof) => proof,
        Err(error) if error.downcast_ref::<rusqlite::Error>().is_some() => return Err(error),
        Err(error) => return Ok(budget_refusal(&request.order_id, error)),
    };
    if let Some(guard) = native {
        if !guard.check(store)? {
            return Ok(super::reject("native_buy_decision_changed"));
        }
    }
    let mut attempt = build_submit_transport_attempt(intent, gate.submit_timeout_ms, now)?;
    attempt.tx_signature_hint = Some(identity.tx_signature.clone());
    let clock = || {
        if let Some(guard) = native {
            let now = crate::execution_canary_safety::risk_clock::decision_time(tick_at)
                .ok_or_else(|| anyhow::anyhow!("native_buy_decision_clock"))?;
            ensure!(guard.check_at(store, now)?, "native_buy_decision_changed");
            return Ok(now);
        }
        if let SubmitState::Owned(p) = state {
            let c = gate
                .buy_safety_config
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("tiny_budget_config_missing"))?;
            crate::execution_owned_sell_prepare::submit::guard::live(request)?;
            crate::execution_owned_sell_prepare::submit::guard::config(c, p)?;
        }
        let now = crate::execution_canary_safety::risk_clock::decision_time(tick_at)
            .ok_or_else(|| anyhow::anyhow!("tiny_budget_clock"))?;
        if let SubmitState::OwnerTechnicalBuy { intent, .. } = state {
            let config = gate.buy_safety_config.as_ref()
                .ok_or_else(|| anyhow::anyhow!("owner_buy_config_missing"))?;
            crate::execution_owner_buy_authority::current(config, store, intent, now)?;
            crate::execution_owner_buy_authority::fresh_quote(request, now)?;
        }
        if let SubmitState::OwnerExit { intent, .. } = state {
            let config = gate.buy_safety_config.as_ref()
                .ok_or_else(|| anyhow::anyhow!("owner_exit_config_missing"))?;
            crate::execution_owner_exit_authority::current(config, store, intent, now)?;
            crate::execution_owner_exit_authority::fresh_quote(request, now)?;
        }
        Ok(now)
    };
    let claim_result = match state {
        SubmitState::Owned(p) => store.claim_owned_sell_dispatch(p, &identity, &budget, clock),
        SubmitState::OwnerTechnicalBuy { order, .. } =>
            store.claim_owner_technical_buy_dispatch(order, &identity, &budget, clock),
        SubmitState::OwnerExit { order, .. } =>
            store.claim_owner_exit_dispatch(order, &identity, &budget, clock),
        SubmitState::Legacy { order, signal } => store
            .claim_tiny_experiment_dispatch_with_clock(order, signal, &identity, &budget, clock),
    };
    let claim = match claim_result {
        Ok(claim) => claim,
        Err(error) if error.downcast_ref::<rusqlite::Error>().is_some() => return Err(error),
        Err(error) => return Ok(budget_refusal(&request.order_id, error)),
    };
    let reason = if claim == ExecutionDispatchClaim::New {
        // No other await and no SQLite lock between committed permission and send.
        #[cfg(test)]
        let submitted = if let Some(mock) = native.and_then(|guard| guard.mock_io()) {
            mock.count(|counts| counts.send += 1);
            tokio::task::yield_now().await;
            Ok(ExecutionSubmitTransportOutcome::SubmittedUnknown {
                idempotency_key: attempt.idempotency_key.clone(),
                tx_signature: mock.submit_signature.clone(),
            })
        } else {
            transport.submit(&attempt).await
        };
        #[cfg(not(test))]
        let submitted = transport.submit(&attempt).await;
        let note = match submitted {
            Ok(ExecutionSubmitTransportOutcome::SubmittedUnknown { tx_signature, .. })
                if tx_signature.as_deref() == Some(&identity.tx_signature) =>
            {
                "dispatch_outcome_unknown"
            }
            Ok(ExecutionSubmitTransportOutcome::SubmittedUnknown { .. }) => {
                "dispatch_response_signature_mismatch"
            }
            Ok(ExecutionSubmitTransportOutcome::NotSent { .. }) => "dispatch_rpc_rejection_unknown",
            Err(_) => "dispatch_transport_error_unknown",
        };
        store.note_execution_canary_dispatch(&identity, note)?;
        note
    } else {
        "dispatch_existing_reconcile_only"
    };
    Ok(ExecutionSubmitPlanOutcome {
        submitted: 1,
        tx_signature: Some(identity.tx_signature),
        idempotency_key: Some(intent.idempotency_key.clone()),
        reason: Some(reason.into()),
        ..Default::default()
    })
}

fn budget_refusal(order_id: &str, error: anyhow::Error) -> ExecutionSubmitPlanOutcome {
    let mut outcome = super::reject("tiny_budget_refused");
    let reason = error.root_cause().to_string();
    outcome.pre_submit_refusal = Some(crate::execution_submit_refusal::PreSubmitRefusal::budget(
        order_id, &reason,
    ));
    outcome.reason = Some(reason);
    outcome
}
