use super::*;
use anyhow::{ensure, Context};
use yellowstone_grpc_proto::prelude::{CommitmentLevel, SubscribeRequest};

pub(super) fn validate(description: &Value) -> Result<policy::DecoderPolicy> {
    let encoded = description["subscribe_request_base64"]
        .as_str()
        .context("missing recorded request")?;
    // Above the maximum request produced by probe81's <=32 program IDs.
    ensure!(
        !encoded.is_empty() && encoded.len() <= 16_384,
        "recorded request size bound"
    );
    // Reuse pinned tonic's binary metadata base64 decoder without a new dependency.
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "request-bin",
        reqwest::header::HeaderValue::from_str(encoded)
            .context("invalid recorded request base64")?,
    );
    let metadata = tonic::metadata::MetadataMap::from_headers(headers);
    let bytes = metadata
        .get_bin("request-bin")
        .context("missing encoded request")?
        .to_bytes()
        .context("invalid recorded request base64")?;
    ensure!(bytes.len() <= 12_288, "decoded request size bound");
    let request =
        SubscribeRequest::decode(bytes.as_ref()).context("invalid recorded request protobuf")?;
    ensure!(
        request.transactions.len() == 1 && request.blocks.len() == 1,
        "unsupported transaction/block subscription shape"
    );
    let tx = request
        .transactions
        .get("copybot-swaps")
        .context("unsupported transaction filter name")?;
    let block = request
        .blocks
        .get("copybot-association")
        .context("unsupported block filter name")?;
    ensure!(
        request.commitment == Some(CommitmentLevel::Confirmed as i32),
        "request commitment mismatch"
    );
    ensure!(
        request.accounts.is_empty()
            && request.slots.is_empty()
            && request.transactions_status.is_empty()
            && request.blocks_meta.is_empty()
            && request.entry.is_empty()
            && request.accounts_data_slice.is_empty()
            && request.ping.is_none()
            && request.from_slot.is_none(),
        "undeclared request subscription or option"
    );
    ensure!(
        tx.vote == Some(false)
            && tx.failed == Some(false)
            && tx.signature.is_none()
            && tx.account_exclude.is_empty()
            && tx.account_required.is_empty(),
        "unsupported transaction filter options"
    );
    ensure!(
        block.account_include == tx.account_include
            && block.include_transactions == Some(true)
            && block.include_accounts == Some(false)
            && block.include_entries == Some(false),
        "block filter mismatch"
    );
    // Exactly the supported description and decoded request, including program order.
    let expected = json!({"program_ids":tx.account_include,"commitment":"confirmed",
        "transactions":{"vote":false,"failed":false,"account_include":tx.account_include},
        "blocks":{"account_include":tx.account_include,"include_transactions":true,
            "include_accounts":false,"include_entries":false},"subscribe_request_base64":encoded});
    ensure!(*description == expected, "request description mismatch");
    // Supported shape has one entry per map: roundtrip is deterministic and refuses
    // unknown wire fields/duplicate fields rather than silently losing request data.
    ensure!(
        request.encode_to_vec() == bytes.as_ref(),
        "unsupported request wire representation"
    );
    policy::DecoderPolicy::from_recorded(&tx.account_include)
}
