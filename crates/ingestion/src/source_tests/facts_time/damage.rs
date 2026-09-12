use super::*;
use yellowstone_grpc_proto::prelude::TransactionError;

pub(super) const NAMES: &[&str] = &[
    "zero_slot",
    "missing_status",
    "vote",
    "failed",
    "missing_meta",
    "missing_tx",
    "missing_message",
    "missing_keys",
    "empty_signer",
    "missing_signature",
    "missing_program_error",
    "unsupported_instruction",
    "no_balances",
    "bad_owned_amount",
];

pub(super) fn apply(update: &mut SubscribeUpdate, name: &str) {
    let subscribe_update::UpdateOneof::Transaction(tx) = update.update_oneof.as_mut().unwrap()
    else {
        panic!()
    };
    if name == "zero_slot" {
        tx.slot = 0;
        return;
    }
    if name == "missing_status" {
        tx.transaction = None;
        return;
    }
    let info = tx.transaction.as_mut().unwrap();
    match name {
        "vote" => info.is_vote = true,
        "failed" => info.meta.as_mut().unwrap().err = Some(TransactionError { err: vec![1] }),
        "missing_meta" => info.meta = None,
        "missing_tx" => info.transaction = None,
        "missing_message" => info.transaction.as_mut().unwrap().message = None,
        "missing_signature" => {
            info.signature.clear();
            info.transaction.as_mut().unwrap().signatures.clear();
        }
        "missing_keys" => info
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .account_keys
            .clear(),
        "empty_signer" => info
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .account_keys[0]
            .clear(),
        "missing_program_error" => {
            info.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions
                .clear();
            let meta = info.meta.as_mut().unwrap();
            meta.inner_instructions.clear();
            meta.log_messages.clear();
        }
        "unsupported_instruction" => {
            for ix in &mut info
                .transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .instructions
            {
                ix.data.clear();
            }
            for group in &mut info.meta.as_mut().unwrap().inner_instructions {
                for ix in &mut group.instructions {
                    ix.data.clear();
                }
            }
        }
        "no_balances" => {
            let meta = info.meta.as_mut().unwrap();
            meta.pre_token_balances.clear();
            meta.post_token_balances.clear();
        }
        "bad_owned_amount" => {
            let keys = &info
                .transaction
                .as_ref()
                .unwrap()
                .message
                .as_ref()
                .unwrap()
                .account_keys;
            let signer = bs58::encode(&keys[0]).into_string();
            for row in &mut info.meta.as_mut().unwrap().pre_token_balances {
                if row.owner == signer {
                    row.ui_token_amount = None;
                }
            }
        }
        _ => unreachable!(),
    }
}
