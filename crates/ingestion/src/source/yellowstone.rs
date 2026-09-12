use anyhow::Result;
use yellowstone_grpc_proto::prelude::{
    subscribe_update, SubscribeUpdate, SubscribeUpdateTransaction,
};

use super::yellowstone_facts::decode_yellowstone_swap_facts;
use super::yellowstone_message_time::YellowstoneMessageTime;
#[cfg(test)]
pub(super) use super::yellowstone_proto::parse_proto_ui_amount;
pub(super) use super::yellowstone_request::build_yellowstone_subscribe_request;
use super::{RawSwapObservation, YellowstoneParsedUpdate, YellowstoneRuntimeConfig};

pub(super) fn parse_yellowstone_update(
    update: SubscribeUpdate,
    runtime_config: &YellowstoneRuntimeConfig,
) -> Result<Option<YellowstoneParsedUpdate>> {
    let created_at = update.created_at.clone();
    let Some(update_oneof) = update.update_oneof else {
        return Ok(None);
    };
    match update_oneof {
        subscribe_update::UpdateOneof::Transaction(transaction_update) => {
            parse_yellowstone_transaction_update(transaction_update, created_at, runtime_config)
                .map(|raw| raw.map(YellowstoneParsedUpdate::Observation))
        }
        subscribe_update::UpdateOneof::Ping(_) => Ok(Some(YellowstoneParsedUpdate::Ping)),
        _ => Ok(None),
    }
}

fn parse_yellowstone_transaction_update(
    tx_update: SubscribeUpdateTransaction,
    created_at: Option<yellowstone_grpc_proto::prost_types::Timestamp>,
    runtime_config: &YellowstoneRuntimeConfig,
) -> Result<Option<RawSwapObservation>> {
    let decoded = decode_yellowstone_swap_facts(
        &tx_update,
        &runtime_config.interested_program_ids,
        &runtime_config.raydium_program_ids,
        &runtime_config.pumpswap_program_ids,
    );
    if decoded.used_program_fallback {
        runtime_config
            .telemetry
            .note_parse_fallback("missing_program_ids_fallback");
    }
    let Some(facts) = decoded.facts? else {
        return Ok(None);
    };
    let YellowstoneMessageTime::AvailableCreatedAt(ts_utc) =
        YellowstoneMessageTime::from_created_at(created_at.as_ref())
    else {
        return Ok(None);
    };
    // Legacy compatibility only: created_at remains the observation timestamp.
    // Its availability does not prove chain/event time or block identity.
    Ok(Some(RawSwapObservation {
        signature: facts.signature,
        slot: facts.slot,
        signer: facts.signer,
        token_in: facts.token_in,
        token_out: facts.token_out,
        amount_in: facts.amount_in,
        amount_out: facts.amount_out,
        exact_amounts: facts.exact_amounts,
        program_ids: facts.program_ids,
        dex_hint: facts.dex_hint,
        ts_utc,
    }))
}

#[cfg(test)]
#[path = "yellowstone_tests.rs"]
mod tests;
