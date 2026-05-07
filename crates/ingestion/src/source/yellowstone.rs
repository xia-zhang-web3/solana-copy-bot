use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use yellowstone_grpc_proto::prelude::{
    subscribe_update, SubscribeUpdate, SubscribeUpdateTransaction,
};

#[cfg(test)]
pub(super) use super::yellowstone_proto::parse_proto_ui_amount;
use super::yellowstone_proto::{
    build_exact_swap_amounts, decode_signature_from_proto, extract_program_ids_from_proto,
    infer_swap_from_proto_balances, proto_account_keys, tx_meta_has_error,
};
pub(super) use super::yellowstone_request::build_yellowstone_subscribe_request;
use super::{
    normalize_program_ids_or_fallback, HeliusWsSource, RawSwapObservation, YellowstoneParsedUpdate,
    YellowstoneRuntimeConfig,
};

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
    if tx_update.slot == 0 {
        return Err(anyhow!("missing slot in yellowstone update"));
    }
    let Some(tx_info) = tx_update.transaction else {
        return Err(anyhow!("missing status in yellowstone update"));
    };
    if tx_info.is_vote {
        return Ok(None);
    }

    let Some(meta) = tx_info.meta.as_ref() else {
        return Err(anyhow!("missing status in yellowstone update"));
    };
    if tx_meta_has_error(meta) {
        return Ok(None);
    }

    let Some(transaction) = tx_info.transaction.as_ref() else {
        return Err(anyhow!("missing signer in yellowstone update"));
    };
    let Some(message) = transaction.message.as_ref() else {
        return Err(anyhow!("missing signer in yellowstone update"));
    };

    let account_keys = proto_account_keys(message, meta);
    if account_keys.is_empty() {
        return Err(anyhow!("missing signer in yellowstone update"));
    }

    let signer_index = 0;
    let signer = account_keys.get(signer_index).cloned().unwrap_or_default();
    if signer.is_empty() {
        return Err(anyhow!("missing signer in yellowstone update"));
    }

    let program_ids = match normalize_program_ids_or_fallback(
        extract_program_ids_from_proto(message, meta, &account_keys),
        &runtime_config.interested_program_ids,
        runtime_config.telemetry.as_ref(),
        "missing program ids in yellowstone update",
    )? {
        Some(value) => value,
        None => return Ok(None),
    };

    let (token_in, amount_in, token_out, amount_out) =
        match infer_swap_from_proto_balances(meta, signer_index, &signer) {
            Some(value) => value,
            None => return Ok(None),
        };
    if !amount_in.amount.is_finite()
        || !amount_out.amount.is_finite()
        || amount_in.amount <= 0.0
        || amount_out.amount <= 0.0
    {
        return Ok(None);
    }

    let signature = decode_signature_from_proto(&tx_info)
        .ok_or_else(|| anyhow!("missing transaction signature in yellowstone update"))?;
    let logs = meta.log_messages.clone();
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        &runtime_config.raydium_program_ids,
        &runtime_config.pumpswap_program_ids,
    );

    let ts_utc = created_at
        .as_ref()
        .and_then(|timestamp| {
            if timestamp.nanos < 0 || timestamp.nanos >= 1_000_000_000 {
                return None;
            }
            DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        })
        .unwrap_or_else(Utc::now);

    Ok(Some(RawSwapObservation {
        signature,
        slot: tx_update.slot,
        signer,
        token_in,
        token_out,
        amount_in: amount_in.amount,
        amount_out: amount_out.amount,
        exact_amounts: build_exact_swap_amounts(&amount_in, &amount_out),
        program_ids: program_ids.into_iter().collect(),
        dex_hint,
        ts_utc,
    }))
}

#[cfg(test)]
#[path = "yellowstone_tests.rs"]
mod tests;
