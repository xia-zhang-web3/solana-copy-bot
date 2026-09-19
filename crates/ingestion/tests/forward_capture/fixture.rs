//! Saved RPC facts reconstructed as protobuf; envelope time is explicitly synthetic.
use serde_json::Value;

use yellowstone_grpc_proto::prelude::*;

fn bytes(v: &Value) -> Vec<u8> {
    bs58::decode(v.as_str().unwrap()).into_vec().unwrap()
}
fn list<T>(v: &Value, f: impl Fn(&Value) -> T) -> Vec<T> {
    v.as_array()
        .map(|a| a.iter().map(f).collect())
        .unwrap_or_default()
}
fn ix(v: &Value) -> CompiledInstruction {
    CompiledInstruction {
        program_id_index: v["programIdIndex"].as_u64().unwrap() as u32,
        accounts: list(&v["accounts"], |n| n.as_u64().unwrap() as u8),
        data: bytes(&v["data"]),
    }
}
fn tokens(v: &Value) -> Vec<TokenBalance> {
    list(v, |t| TokenBalance {
        account_index: t["accountIndex"].as_u64().unwrap() as u32,
        mint: t["mint"].as_str().unwrap().into(),
        owner: t["owner"].as_str().unwrap_or("").into(),
        program_id: t["programId"].as_str().unwrap_or("").into(),
        ui_token_amount: Some(UiTokenAmount {
            amount: t["uiTokenAmount"]["amount"].as_str().unwrap().into(),
            decimals: t["uiTokenAmount"]["decimals"].as_u64().unwrap() as u32,
            ui_amount: t["uiTokenAmount"]["uiAmount"].as_f64().unwrap_or(0.0),
            ui_amount_string: t["uiTokenAmount"]["uiAmountString"]
                .as_str()
                .unwrap()
                .into(),
        }),
    })
}
pub fn update(f: &Value) -> SubscribeUpdate {
    let r = &f["result"];
    let m = &r["transaction"]["message"];
    let meta = &r["meta"];
    assert!(meta["err"].is_null(), "saved successful transaction");
    SubscribeUpdate {
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds: r["blockTime"].as_i64().unwrap(),
            nanos: 0,
        }),
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot: r["slot"].as_u64().unwrap(),
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: bytes(&f["signature"]),
                    is_vote: false,
                    transaction: Some(Transaction {
                        signatures: list(&r["transaction"]["signatures"], bytes),
                        message: Some(Message {
                            header: Some(MessageHeader {
                                num_required_signatures: m["header"]["numRequiredSignatures"]
                                    .as_u64()
                                    .unwrap()
                                    as u32,
                                num_readonly_signed_accounts: m["header"]
                                    ["numReadonlySignedAccounts"]
                                    .as_u64()
                                    .unwrap()
                                    as u32,
                                num_readonly_unsigned_accounts: m["header"]
                                    ["numReadonlyUnsignedAccounts"]
                                    .as_u64()
                                    .unwrap()
                                    as u32,
                            }),
                            account_keys: list(&m["accountKeys"], bytes),
                            recent_blockhash: bytes(&m["recentBlockhash"]),
                            instructions: list(&m["instructions"], ix),
                            versioned: r["version"] == 0,
                            address_table_lookups: list(&m["addressTableLookups"], |a| {
                                MessageAddressTableLookup {
                                    account_key: bytes(&a["accountKey"]),
                                    writable_indexes: list(&a["writableIndexes"], |v| {
                                        v.as_u64().unwrap() as u8
                                    }),
                                    readonly_indexes: list(&a["readonlyIndexes"], |v| {
                                        v.as_u64().unwrap() as u8
                                    }),
                                }
                            }),
                        }),
                    }),
                    meta: Some(TransactionStatusMeta {
                        fee: meta["fee"].as_u64().unwrap(),
                        pre_balances: list(&meta["preBalances"], |v| v.as_u64().unwrap()),
                        post_balances: list(&meta["postBalances"], |v| v.as_u64().unwrap()),
                        pre_token_balances: tokens(&meta["preTokenBalances"]),
                        post_token_balances: tokens(&meta["postTokenBalances"]),
                        log_messages: list(&meta["logMessages"], |v| v.as_str().unwrap().into()),
                        inner_instructions: list(&meta["innerInstructions"], |g| {
                            InnerInstructions {
                                index: g["index"].as_u64().unwrap() as u32,
                                instructions: list(&g["instructions"], |v| {
                                    let i = ix(v);
                                    InnerInstruction {
                                        program_id_index: i.program_id_index,
                                        accounts: i.accounts,
                                        data: i.data,
                                        stack_height: v["stackHeight"].as_u64().map(|n| n as u32),
                                    }
                                }),
                            }
                        }),
                        loaded_writable_addresses: list(
                            &meta["loadedAddresses"]["writable"],
                            bytes,
                        ),
                        loaded_readonly_addresses: list(
                            &meta["loadedAddresses"]["readonly"],
                            bytes,
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
        )),
        ..Default::default()
    }
}
