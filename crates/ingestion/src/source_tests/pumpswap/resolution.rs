use super::*;
use yellowstone_grpc_proto::prelude::{
    subscribe_update, MessageAddressTableLookup, SubscribeUpdate, SubscribeUpdateTransactionInfo,
    TransactionError,
};

fn info(update: &mut SubscribeUpdate) -> &mut SubscribeUpdateTransactionInfo {
    let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof else {
        panic!("not tx")
    };
    tx.transaction.as_mut().unwrap()
}

fn observed(label: &str, update: SubscribeUpdate) -> Result<Option<RawSwapObservation>> {
    let wire = update.encode_to_vec();
    if let Ok(dir) = std::env::var("B43_CAPTURE_DIR") {
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            std::path::Path::new(&dir).join(format!("{label}.pb")),
            &wire,
        )?;
    }
    let decoded = SubscribeUpdate::decode(wire.as_slice())?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let result = yellowstone::parse_yellowstone_update(decoded, &source.runtime_config);
    if let Ok(dir) = std::env::var("B43_CAPTURE_DIR") {
        let outcome = match &result {
            Ok(Some(YellowstoneParsedUpdate::Observation(raw))) => {
                json!({"raw_present":true,"signature":raw.signature,"signer":raw.signer,"slot":raw.slot,"ts_utc":raw.ts_utc,"token_in":raw.token_in,"amount_in":raw.amount_in,"token_out":raw.token_out,"amount_out":raw.amount_out,"exact_amounts":raw.exact_amounts})
            }
            Ok(None) => json!({"raw_present":false}),
            Err(error) => json!({"error":error.to_string()}),
            _ => panic!("unexpected ping"),
        };
        std::fs::write(
            std::path::Path::new(&dir).join(format!("{label}-proto-result.json")),
            serde_json::to_vec_pretty(&outcome)?,
        )?;
    }
    Ok(match result? {
        Some(YellowstoneParsedUpdate::Observation(raw)) => Some(raw),
        None => None,
        _ => panic!("unexpected ping"),
    })
}

fn load_addresses(update: &mut SubscribeUpdate, writable: usize) {
    let tx = info(update);
    let message = tx.transaction.as_mut().unwrap().message.as_mut().unwrap();
    let mut loaded = message.account_keys.split_off(10);
    message.versioned = true;
    message
        .header
        .as_mut()
        .unwrap()
        .num_readonly_unsigned_accounts = 0;
    message.address_table_lookups = vec![MessageAddressTableLookup {
        account_key: vec![88; 32],
        writable_indexes: (0..writable as u8).collect(),
        readonly_indexes: (writable as u8..loaded.len() as u8).collect(),
    }];
    let meta = tx.meta.as_mut().unwrap();
    meta.loaded_readonly_addresses = loaded.split_off(writable);
    meta.loaded_writable_addresses = loaded;
}

#[test]
fn yellowstone_static_loaded_writable_and_readonly_resolve_top_and_inner() -> Result<()> {
    for writable in [7, 8] {
        for inner in [false, true] {
            for buy in [false, true] {
                let f = super::cases::swap_fixture(buy);
                let mut update = proto::update(&f);
                if inner {
                    let tx = info(&mut update);
                    let msg = tx.transaction.as_mut().unwrap().message.as_mut().unwrap();
                    let original = msg.instructions[0].clone();
                    msg.instructions[0].program_id_index = 15;
                    msg.instructions[0].data = vec![];
                    tx.meta.as_mut().unwrap().inner_instructions[0]
                        .instructions
                        .insert(
                            0,
                            yellowstone_grpc_proto::prelude::InnerInstruction {
                                program_id_index: original.program_id_index,
                                accounts: original.accounts,
                                data: original.data,
                                stack_height: Some(2),
                            },
                        );
                }
                load_addresses(&mut update, writable + usize::from(buy));
                let label = format!("loaded-{writable}-{inner}-{buy}");
                let raw = observed(&label, update)?.expect("supported loaded instruction");
                let base = parse(&f, "yellowstone")?.unwrap();
                assert_eq!(
                    (
                        raw.signer,
                        raw.signature,
                        raw.slot,
                        raw.ts_utc,
                        raw.token_in,
                        raw.amount_in,
                        raw.token_out,
                        raw.amount_out,
                        raw.exact_amounts
                    ),
                    (
                        base.signer,
                        base.signature,
                        base.slot,
                        base.ts_utc,
                        base.token_in,
                        base.amount_in,
                        base.token_out,
                        base.amount_out,
                        base.exact_amounts
                    )
                );
            }
        }
    }
    Ok(())
}

#[test]
fn yellowstone_invalid_indexes_keys_and_inner_parent_are_not_evidence() -> Result<()> {
    for variant in 0..8 {
        let mut update = proto::update(&fixture("g1_swap"));
        if variant >= 4 {
            load_addresses(&mut update, 7);
        }
        let tx = info(&mut update);
        let message = tx.transaction.as_mut().unwrap().message.as_mut().unwrap();
        let meta = tx.meta.as_mut().unwrap();
        match variant {
            0 => message.instructions[0].program_id_index = 999,
            1 => message.instructions[0].accounts.push(255),
            2 => message.account_keys[17] = vec![9; 31],
            3 => message.account_keys[2] = vec![9; 31],
            4 => meta.loaded_readonly_addresses[0] = vec![9; 31],
            5 => meta.loaded_writable_addresses[0] = vec![9; 31],
            6 => {
                let original = message.instructions[0].clone();
                message.instructions[0].data = vec![];
                meta.inner_instructions[0].index = 999;
                meta.inner_instructions[0].instructions =
                    vec![yellowstone_grpc_proto::prelude::InnerInstruction {
                        program_id_index: original.program_id_index,
                        accounts: original.accounts,
                        data: original.data,
                        stack_height: Some(2),
                    }];
            }
            7 => meta.loaded_readonly_addresses.clear(),
            _ => unreachable!(),
        }
        assert!(
            observed(&format!("bad-proto-{variant}"), update)?.is_none(),
            "variant {variant}"
        );
    }
    // An unrelated malformed hole never shifts any accepted account/program index.
    let mut update = proto::update(&fixture("g1_swap"));
    info(&mut update)
        .transaction
        .as_mut()
        .unwrap()
        .message
        .as_mut()
        .unwrap()
        .account_keys[1] = vec![];
    assert!(observed("unreferenced-proto-hole", update)?.is_some());
    Ok(())
}

fn json_case(f: &Value, expected: bool) -> Result<()> {
    for provider in ["rpc_backfill", "helius_fetch"] {
        assert_case(f, provider, expected)?;
    }
    Ok(())
}

#[test]
fn json_invalid_identity_encoding_accounts_and_compiled_forms_are_refused() -> Result<()> {
    for variant in 0..12 {
        let mut f = fixture("g1_swap");
        f["case"] = json!(format!("bad-json-{variant}"));
        let m = &mut f["result"]["transaction"]["message"];
        match variant {
            0 => m["instructions"][0]["data"] = json!("0not_base58"),
            1 => m["instructions"][0]["data"] = json!([super::cases::data(false), "base64"]),
            2 => m["instructions"][0]["accounts"][0] = json!(255),
            3 => m["instructions"][0]["accounts"][0] = json!(bs58::encode([98; 32]).into_string()),
            4 => m["accountKeys"][17] = Value::Null,
            5 => m["instructions"][0]["programId"] = json!("bad identity"),
            6 => m["instructions"][0]
                .as_object_mut()
                .unwrap()
                .remove("data")
                .map(|_| ())
                .unwrap(),
            7 => m["instructions"][0]["programIdIndex"] = json!(17),
            8 => {
                m["instructions"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("programId");
                m["instructions"][0]["programIdIndex"] = json!(999);
            }
            9 => {
                m["accountKeys"][1] = Value::Null;
                m["instructions"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("programId");
                m["instructions"][0]["programIdIndex"] = json!(16);
            }
            10 => {
                m["instructions"][0]["parsed"] = json!({"type":"sell"});
            }
            11 => {
                m["accountKeys"][2]["pubkey"] = json!(bs58::encode([9; 31]).into_string());
            }
            _ => unreachable!(),
        }
        json_case(&f, false)?;
    }
    Ok(())
}

#[test]
fn json_inner_parent_must_exist_and_parsed_loaded_pubkeys_are_accepted() -> Result<()> {
    let mut f = fixture("g1_swap");
    f["case"] = json!("bad-json-inner-parent");
    let pump = f["result"]["transaction"]["message"]["instructions"][0].clone();
    f["result"]["transaction"]["message"]["instructions"][0]["data"] = json!("");
    f["result"]["meta"]["innerInstructions"][0]["instructions"] = json!([pump]);
    f["result"]["meta"]["innerInstructions"][0]["index"] = json!(99);
    json_case(&f, false)?;
    f["result"]["meta"]["innerInstructions"][0]["index"] = json!(0);
    f["case"] = json!("parsed-loaded-keys");
    for (n, key) in f["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        key["source"] = json!(if n < 10 { "transaction" } else { "lookupTable" });
    }
    json_case(&f, true)?;
    Ok(())
}

#[test]
fn failed_transaction_and_missing_metadata_preserve_provider_semantics() -> Result<()> {
    let mut f = fixture("g1_swap");
    let mut update = proto::update(&f);
    info(&mut update).meta.as_mut().unwrap().err = Some(TransactionError { err: vec![1] });
    assert!(observed("meta-err", update)?.is_none());
    f["case"] = json!("meta-err");
    f["result"]["meta"]["err"] = json!({"InstructionError":[0,"Custom"]});
    json_case(&f, false)?;
    let mut update = proto::update(&fixture("g1_swap"));
    info(&mut update).meta = None;
    assert!(observed("missing-meta", update)
        .unwrap_err()
        .to_string()
        .contains("missing status"));
    f["case"] = json!("missing-meta");
    f["result"]["meta"] = Value::Null;
    json_case(&f, false)?;
    Ok(())
}
