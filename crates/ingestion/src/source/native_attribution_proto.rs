use super::{attribute, wire, Attribution, Instruction, Row, View};
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::{Message, TokenBalance, TransactionStatusMeta};

pub(in crate::source) fn infer(
    message: &Message,
    meta: &TransactionStatusMeta,
    signer: &str,
    programs: &HashSet<String>,
) -> Attribution {
    // Keep every original position, including malformed keys; append writable then readonly.
    let keys: Vec<_> = message
        .account_keys
        .iter()
        .chain(&meta.loaded_writable_addresses)
        .chain(&meta.loaded_readonly_addresses)
        .map(|k| (k.len() == 32).then(|| bs58::encode(k).into_string()))
        .collect();
    let instruction = |p: u32, a: &[u8], data: &[u8], depth| Instruction {
        program: keys.get(p as usize).cloned().flatten(),
        accounts: a
            .iter()
            .map(|i| keys.get(*i as usize)?.as_ref().map(|_| *i as usize))
            .collect(),
        data: Some(data.to_vec()),
        depth,
    };
    let v = View {
        successful: meta.err.is_none(),
        owned_sol: rows(&meta.pre_token_balances, Some(signer))
            .zip(rows(&meta.post_token_balances, Some(signer))),
        first_signer: message
            .header
            .as_ref()
            .filter(|h| {
                h.num_required_signatures > 0
                    && h.num_required_signatures as usize <= message.account_keys.len()
            })
            .and_then(|_| keys.first().cloned().flatten()),
        top: Some(
            message
                .instructions
                .iter()
                .map(|i| instruction(i.program_id_index, &i.accounts, &i.data, None))
                .collect(),
        ),
        inner: (!meta.inner_instructions_none).then(|| {
            meta.inner_instructions
                .iter()
                .map(|g| {
                    (
                        g.index as usize,
                        g.instructions
                            .iter()
                            .map(|i| {
                                instruction(
                                    i.program_id_index,
                                    &i.accounts,
                                    &i.data,
                                    i.stack_height,
                                )
                            })
                            .collect(),
                    )
                })
                .collect()
        }),
        pre: Some(meta.pre_balances.clone()),
        post: Some(meta.post_balances.clone()),
        pre_tokens: rows(&meta.pre_token_balances, None),
        post_tokens: rows(&meta.post_token_balances, None),
        keys,
    };
    attribute(&v, signer, programs)
}

fn rows(rows: &[TokenBalance], only_owned_sol: Option<&str>) -> Option<Vec<Row>> {
    rows.iter()
        .filter(|r| {
            only_owned_sol.is_none_or(|signer| r.owner == signer && r.mint == super::SOL_MINT)
        })
        .map(|r| {
            let a = r.ui_token_amount.as_ref()?;
            let (raw, decimals) = wire::amount(
                &a.amount,
                u64::from(a.decimals),
                (!a.ui_amount_string.is_empty()).then_some(a.ui_amount_string.as_str()),
                Some(a.ui_amount),
            )?;
            Some(Row {
                index: r.account_index as usize,
                mint: super::key(&r.mint)?,
                owner: super::key(&r.owner)?,
                program: super::key(&r.program_id)?,
                raw,
                decimals,
            })
        })
        .collect()
}
