use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

fn bytes(v: &Value) -> Vec<u8> {
    bs58::decode(v.as_str().unwrap()).into_vec().unwrap()
}

fn tokens(v: &Value) -> Vec<TokenBalance> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let amount = &row["uiTokenAmount"];
            TokenBalance {
                account_index: row["accountIndex"].as_u64().unwrap() as u32,
                mint: row["mint"].as_str().unwrap().to_string(),
                owner: row["owner"].as_str().unwrap().to_string(),
                program_id: row["programId"].as_str().unwrap().to_string(),
                ui_token_amount: Some(UiTokenAmount {
                    amount: amount["amount"].as_str().unwrap().to_string(),
                    decimals: amount["decimals"].as_u64().unwrap() as u32,
                    ui_amount: amount["uiAmount"].as_f64().unwrap_or_default(),
                    ui_amount_string: amount["uiAmountString"].as_str().unwrap().to_string(),
                }),
            }
        })
        .collect()
}

fn compiled(v: &Value, keys: &[String]) -> CompiledInstruction {
    CompiledInstruction {
        program_id_index: v["programId"]
            .as_str()
            .and_then(|id| keys.iter().position(|k| k == id))
            .unwrap_or(999) as u32,
        accounts: v["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| keys.iter().position(|x| x == k.as_str().unwrap()).unwrap() as u8)
            .collect(),
        data: bytes(&v["data"]),
    }
}

pub(super) fn update(f: &Value) -> SubscribeUpdate {
    let result = &f["result"];
    let meta = &result["meta"];
    let message = &result["transaction"]["message"];
    let accounts = message["accountKeys"].as_array().unwrap();
    let keys: Vec<String> = accounts
        .iter()
        .map(|k| k["pubkey"].as_str().unwrap().to_string())
        .collect();
    let timestamp = &f["created_at"];
    SubscribeUpdate {
        filters: vec!["copybot-swaps".to_string()],
        created_at: (!timestamp.is_null()).then(|| {
            yellowstone_grpc_proto::prost_types::Timestamp {
                seconds: timestamp["seconds"].as_i64().unwrap(),
                nanos: timestamp["nanos"].as_i64().unwrap() as i32,
            }
        }),
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot: result["slot"].as_u64().unwrap(),
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: bytes(&f["signature"]),
                    is_vote: false,
                    transaction: Some(Transaction {
                        signatures: result["transaction"]["signatures"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(bytes)
                            .collect(),
                        message: Some(Message {
                            header: Some(MessageHeader {
                                num_required_signatures: accounts
                                    .iter()
                                    .filter(|k| k["signer"] == true)
                                    .count()
                                    as u32,
                                num_readonly_signed_accounts: 0,
                                num_readonly_unsigned_accounts: accounts
                                    .iter()
                                    .filter(|k| k["signer"] == false && k["writable"] == false)
                                    .count()
                                    as u32,
                            }),
                            account_keys: accounts.iter().map(|k| bytes(&k["pubkey"])).collect(),
                            recent_blockhash: bytes(&message["recentBlockhash"]),
                            instructions: message["instructions"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|v| compiled(v, &keys))
                                .collect(),
                            ..Default::default()
                        }),
                    }),
                    meta: Some(TransactionStatusMeta {
                        err: None,
                        fee: meta["fee"].as_u64().unwrap(),
                        pre_balances: meta["preBalances"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| v.as_u64().unwrap())
                            .collect(),
                        post_balances: meta["postBalances"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| v.as_u64().unwrap())
                            .collect(),
                        pre_token_balances: tokens(&meta["preTokenBalances"]),
                        post_token_balances: tokens(&meta["postTokenBalances"]),
                        log_messages: meta["logMessages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| v.as_str().unwrap().to_string())
                            .collect(),
                        inner_instructions: meta["innerInstructions"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|group| InnerInstructions {
                                index: group["index"].as_u64().unwrap() as u32,
                                instructions: group["instructions"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .map(|v| {
                                        let ix = compiled(v, &keys);
                                        InnerInstruction {
                                            program_id_index: ix.program_id_index,
                                            accounts: ix.accounts,
                                            data: ix.data,
                                            stack_height: Some(2),
                                        }
                                    })
                                    .collect(),
                            })
                            .collect(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
        )),
    }
}
