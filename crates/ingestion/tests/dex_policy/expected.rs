//! Test oracle reads exact protobuf program references, independent of SwapParser.
use anyhow::{ensure, Context, Result};
use copybot_config::IngestionConfig;
use prost::Message;
use std::collections::BTreeSet;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

pub fn label(raw: &[u8], config: &IngestionConfig) -> Result<&'static str> {
    let update = SubscribeUpdate::decode(raw)?;
    let Some(UpdateOneof::Transaction(tx)) = update.update_oneof else {
        anyhow::bail!("policy oracle needs transaction");
    };
    let info = tx.transaction.context("transaction info")?;
    let message = info
        .transaction
        .context("transaction")?
        .message
        .context("message")?;
    let meta = info.meta.context("meta")?;
    let keys: Vec<_> = message
        .account_keys
        .iter()
        .chain(meta.loaded_writable_addresses.iter())
        .chain(meta.loaded_readonly_addresses.iter())
        .map(|key| bs58::encode(key).into_string())
        .collect();
    let mut invoked = BTreeSet::new();
    for ix in &message.instructions {
        invoked.insert(
            keys.get(ix.program_id_index as usize)
                .context("outer program index")?
                .clone(),
        );
    }
    for group in &meta.inner_instructions {
        for ix in &group.instructions {
            invoked.insert(
                keys.get(ix.program_id_index as usize)
                    .context("inner program index")?
                    .clone(),
            );
        }
    }
    for log in &meta.log_messages {
        let words: Vec<_> = log.split_whitespace().take(2).collect();
        if words.len() == 2 && words[0] == "Program" {
            invoked.insert(words[1].to_string());
        }
    }
    let ray = config
        .raydium_program_ids
        .iter()
        .any(|id| invoked.contains(id));
    let pump = config
        .pumpswap_program_ids
        .iter()
        .any(|id| invoked.contains(id));
    // Saved live controls have explicit program evidence; never infer a venue from amounts.
    ensure!(
        ray || pump,
        "saved supported swap has no configured family evidence"
    );
    Ok(if ray && pump {
        "multi_dex"
    } else if ray {
        "raydium"
    } else {
        "pumpswap"
    })
}
