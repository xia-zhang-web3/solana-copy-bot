use super::supported_data;
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::{Message, TransactionStatusMeta};

pub(crate) fn proto_has_supported_swap(
    message: &Message,
    meta: &TransactionStatusMeta,
    pumpswap_program_ids: &HashSet<String>,
) -> bool {
    // Retain every position, including malformed keys, across all three segments.
    let keys: Vec<&[u8]> = message
        .account_keys
        .iter()
        .chain(&meta.loaded_writable_addresses)
        .chain(&meta.loaded_readonly_addresses)
        .map(Vec::as_slice)
        .collect();
    let supported = |program_index: u32, accounts: &[u8], data: &[u8]| {
        let Some(program) = keys
            .get(program_index as usize)
            .filter(|key| key.len() == 32)
        else {
            return false;
        };
        supported_data(data)
            && pumpswap_program_ids.contains(&bs58::encode(program).into_string())
            && !accounts.is_empty()
            && accounts.iter().all(|index| {
                keys.get(usize::from(*index))
                    .is_some_and(|key| key.len() == 32)
            })
    };
    message
        .instructions
        .iter()
        .any(|ix| supported(ix.program_id_index, &ix.accounts, &ix.data))
        || meta
            .inner_instructions
            .iter()
            .filter(|group| (group.index as usize) < message.instructions.len())
            .flat_map(|group| &group.instructions)
            .any(|ix| supported(ix.program_id_index, &ix.accounts, &ix.data))
}
