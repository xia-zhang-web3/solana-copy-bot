//! Canonical live adapter. Recovery deliberately has no SwapEvent representation.
use anyhow::{ensure, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::{
    association_delivery::{
        CheckedFacts, MessageTime, OrdinaryLive, ProtectedObservation, ProtectedSourceKind,
    },
    SwapEvent,
};

pub use crate::source::durable::protected_equivalence::{
    checked_protected_info_equivalent, checked_protected_info_fingerprint, reconstruct_stream,
};

pub fn protected_stream_swap(
    config: &IngestionConfig,
    observation: &ProtectedObservation,
) -> Result<Option<SwapEvent>> {
    ensure!(
        observation.source_kind == ProtectedSourceKind::Stream && observation.block.is_none(),
        "recovered_observation_is_not_a_fresh_swap"
    );
    let Some(f) = &observation.facts else {
        return Ok(None);
    };
    ensure!(
        observation.signature.as_deref() == Some(f.signature.as_str())
            && observation.wallet == f.wallet
            && observation.slot == f.slot,
        "protected_facts_identity"
    );
    Ok(checked_stream_swap(
        f,
        &observation.message_time,
        config.raydium_program_ids.clone(),
        config.pumpswap_program_ids.clone(),
    ))
}

pub fn ordinary_live_swap(
    config: &IngestionConfig,
    packet: &OrdinaryLive,
) -> Result<Option<SwapEvent>> {
    ensure!(
        matches!(&packet.message_time, MessageTime::CreatedAt { .. }),
        "ordinary_intake_requires_genuine_stream_created_at"
    );
    Ok(checked_stream_swap(
        &packet.facts,
        &packet.message_time,
        config.raydium_program_ids.clone(),
        config.pumpswap_program_ids.clone(),
    ))
}

pub(crate) fn checked_stream_swap(
    f: &CheckedFacts,
    time: &MessageTime,
    raydium: Vec<String>,
    pumpswap: Vec<String>,
) -> Option<SwapEvent> {
    let MessageTime::CreatedAt { seconds, nanos } = *time else {
        return None;
    };
    let ts_utc = chrono::DateTime::from_timestamp(seconds, nanos)?;
    let raw = crate::source::RawSwapObservation {
        signature: f.signature.clone(),
        slot: f.slot,
        signer: f.wallet.clone(),
        token_in: f.token_in.clone(),
        token_out: f.token_out.clone(),
        amount_in: f64::from_bits(f.amount_in_bits),
        amount_out: f64::from_bits(f.amount_out_bits),
        exact_amounts: f.exact_amounts.clone(),
        program_ids: f.programs.clone(),
        dex_hint: f.dex.clone(),
        ts_utc,
    };
    crate::parser::SwapParser::new(raydium, pumpswap).parse(raw)
}

/// No new epoch is installed here; the actor owns fencing and membership ACK.
pub fn restore_stream(
    config: &IngestionConfig,
    store: &mut copybot_storage_core::capture_scope::CaptureStore,
) -> Result<()> {
    crate::source::scoped_capture::restore_stream(config, store)
}
