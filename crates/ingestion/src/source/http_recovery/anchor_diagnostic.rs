//! Optional pre-gate evidence; comparison and ACK policy stay in their existing owners.
use super::{anchor_evidence::Pair, identity, identity_difference, RecoveredBlock};
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

pub(super) fn compare_preserved(
    pair: Pair,
    grpc: &SubscribeUpdateBlock,
    http: &SubscribeUpdateBlock,
) -> Result<bool> {
    // No comparison occurs before both independently complete sides are durable.
    let matches = identity::block_equivalent(http, grpc);
    let difference = if matches {
        None
    } else {
        identity_difference::first(grpc, http)
    };
    let first_path = difference
        .as_ref()
        .map(|d| d.path.as_str())
        .unwrap_or(if matches {
            ""
        } else {
            "runtime_predicate_unresolved"
        });
    let value = difference.as_ref().map(|d| d.json()).unwrap_or(if matches {
        Value::Null
    } else {
        json!({"path":"runtime_predicate_unresolved","grpc":null,"http":null})
    });
    pair.complete(grpc, matches, value)?;
    tracing::info!(slot=grpc.slot,comparison=if matches {"MATCH"} else {"MISMATCH"},
        first_field_path=first_path,evidence_pair=%pair.name,evidence_saved=true,
        "confirmed HTTP live anchor compared");
    Ok(matches)
}

pub(crate) fn admit_recovered(
    directory: Option<&str>,
    grpc: &SubscribeUpdateBlock,
    recovered: &mut RecoveredBlock,
) -> Result<()> {
    let matches = if let Some(pair) = recovered.anchor_evidence.take() {
        compare_preserved(pair, grpc, &recovered.block)?
    } else {
        ensure!(directory.is_none(), "anchor_evidence_missing");
        identity::block_equivalent(&recovered.block, grpc)
    };
    ensure!(matches, "http_recovery_live_anchor_conflict");
    Ok(())
}
