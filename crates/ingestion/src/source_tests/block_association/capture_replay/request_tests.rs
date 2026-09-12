use super::*;
use revision_tests::{copied, destination, edited};
use yellowstone_grpc_proto::prelude::{CommitmentLevel, SubscribeRequest};

fn encoded(bytes: &[u8]) -> String {
    let value = tonic::metadata::BinaryMetadataValue::from_bytes(bytes);
    String::from_utf8(value.as_encoded_bytes().to_vec()).unwrap()
}
fn decode(m: &Value) -> SubscribeRequest {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "request-bin",
        reqwest::header::HeaderValue::from_str(
            m["request"]["subscribe_request_base64"].as_str().unwrap(),
        )
        .unwrap(),
    );
    let metadata = tonic::metadata::MetadataMap::from_headers(headers);
    SubscribeRequest::decode(metadata.get_bin("request-bin").unwrap().to_bytes().unwrap()).unwrap()
}
fn programs(m: &mut Value, r: &mut SubscribeRequest, ids: Vec<String>) {
    r.transactions
        .get_mut("copybot-swaps")
        .unwrap()
        .account_include = ids.clone();
    r.blocks
        .get_mut("copybot-association")
        .unwrap()
        .account_include = ids.clone();
    for pointer in [
        "/request/program_ids",
        "/request/transactions/account_include",
        "/request/blocks/account_include",
    ] {
        *m.pointer_mut(pointer).unwrap() = json!(ids);
    }
}
#[test]
#[ignore = "bounded recorded request and policy refusal controls"]
fn r1_recorded_request_description_and_policy_refusals() {
    let mut output = vec![];
    for name in [
        "protobuf",
        "request-bound",
        "commitment",
        "vote",
        "failed",
        "signature",
        "required",
        "exclude",
        "include-transactions",
        "include-accounts",
        "include-entries",
        "extra-slots",
        "extra-tx",
        "from-slot",
        "ping",
        "block-programs",
        "description-programs",
        "description-vote",
        "description-commitment",
        "description-include",
        "unknown-policy",
        "unknown-wire",
    ] {
        let d = copied(&format!("request-{name}"));
        edited(&d, |m| {
            let mut r = decode(m);
            match name {
                "commitment" => r.commitment = Some(CommitmentLevel::Processed as i32),
                "vote" => r.transactions.get_mut("copybot-swaps").unwrap().vote = Some(true),
                "failed" => r.transactions.get_mut("copybot-swaps").unwrap().failed = Some(true),
                "signature" => {
                    r.transactions.get_mut("copybot-swaps").unwrap().signature =
                        Some("filter".into())
                }
                "required" => {
                    r.transactions
                        .get_mut("copybot-swaps")
                        .unwrap()
                        .account_required = vec![policy::PUMPSWAP.into()]
                }
                "exclude" => {
                    r.transactions
                        .get_mut("copybot-swaps")
                        .unwrap()
                        .account_exclude = vec![policy::PUMPSWAP.into()]
                }
                "include-transactions" => {
                    r.blocks
                        .get_mut("copybot-association")
                        .unwrap()
                        .include_transactions = Some(false)
                }
                "include-accounts" => {
                    r.blocks
                        .get_mut("copybot-association")
                        .unwrap()
                        .include_accounts = Some(true)
                }
                "include-entries" => {
                    r.blocks
                        .get_mut("copybot-association")
                        .unwrap()
                        .include_entries = Some(true)
                }
                "extra-slots" => {
                    r.slots.insert("undeclared".into(), Default::default());
                }
                "extra-tx" => {
                    r.transactions.insert("extra".into(), Default::default());
                }
                "from-slot" => r.from_slot = Some(80),
                "ping" => r.ping = Some(Default::default()),
                "block-programs" => r
                    .blocks
                    .get_mut("copybot-association")
                    .unwrap()
                    .account_include
                    .clear(),
                "description-programs" => m["request"]["program_ids"] = json!([policy::PUMPSWAP]),
                "description-vote" => m["request"]["transactions"]["vote"] = json!(true),
                "description-commitment" => m["request"]["commitment"] = json!("processed"),
                "description-include" => {
                    m["request"]["blocks"]["include_transactions"] = json!(false)
                }
                "unknown-policy" => {
                    programs(m, &mut r, vec![bs58::encode([81u8; 32]).into_string()])
                }
                _ => {}
            }
            let mut bytes = r.encode_to_vec();
            if name == "unknown-wire" {
                bytes.extend([0xa0, 0x06, 0x01]);
            }
            m["request"]["subscribe_request_base64"] = json!(match name {
                "protobuf" => encoded(&[0xff]),
                "request-bound" => "A".repeat(16_385),
                _ => encoded(&bytes),
            });
        });
        let reason = reader::read(&d).err().expect(name).to_string();
        let expected = match name {
            "protobuf" => "invalid recorded request protobuf",
            "request-bound" => "recorded request size bound",
            "commitment" => "request commitment mismatch",
            "vote" | "failed" | "signature" | "required" | "exclude" => {
                "unsupported transaction filter options"
            }
            "include-transactions" | "include-accounts" | "include-entries" | "block-programs" => {
                "block filter mismatch"
            }
            "extra-slots" | "from-slot" | "ping" => "undeclared request subscription or option",
            "extra-tx" => "unsupported transaction/block subscription shape",
            "unknown-policy" => "unsupported decoder policy program",
            "unknown-wire" => "unsupported request wire representation",
            _ => "request description mismatch",
        };
        assert_eq!(reason, expected, "{name}");
        output.push(json!({"case":name,"refusal":reason,"association_started":false}));
    }
    write_json(&destination("request-refusals.json"), &json!(output));
}
#[test]
#[ignore = "decoder program policy derives only from the recorded supported subset"]
fn r1_supported_subset_has_no_fixture_policy_fallback() {
    let mut output = vec![];
    for (name, program, expected) in [
        ("pump-only", policy::PUMPSWAP, 1),
        ("ray-only", policy::RAYDIUM_V4, 0),
    ] {
        let d = copied(name);
        edited(&d, |m| {
            let mut r = decode(m);
            programs(m, &mut r, vec![program.into()]);
            m["request"]["subscribe_request_base64"] = json!(encoded(&r.encode_to_vec()));
        });
        let r = replay::analyze(&reader::read(&d).unwrap());
        assert_eq!(
            r["decoder_policy"]["interested_program_ids"],
            json!([program])
        );
        assert_eq!(r["counts"]["matched"], expected);
        assert_eq!(r["counts"]["raw_transactions"], 1);
        if name == "ray-only" {
            assert_eq!(r["decoder_policy"]["pumpswap_program_ids"], json!([]));
        }
        output.push(json!({"case":name,"analysis":r}));
    }
    write_json(&destination("policy-subsets.json"), &json!(output));
}
