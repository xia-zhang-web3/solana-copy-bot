//! Raw wire fixtures deliberately do not import any new profile Rust type.
use super::rpc::{self, RpcStub, CLASSIC, TOKEN_2022};
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
pub enum RawCase {
    Supported165,
    Supported170,
    MissingMint,
    Unsupported,
    Frozen,
    AmountMismatch,
    WalletMismatch,
    ProgramMismatch,
}

pub fn start(sol: u64, parsed_raw: u64, case: RawCase) -> RpcStub {
    RpcStub::start(move |request| {
        let response = match request["method"].as_str().unwrap() {
            "getBalance" => rpc::result(json!(sol)),
            "getTokenAccountsByOwner" if request["params"][1]["programId"] == CLASSIC => {
                rpc::result(json!([]))
            }
            "getTokenAccountsByOwner" => rpc::result(json!([rpc::account(
                &rpc::key('A'),
                &rpc::key('E'),
                &rpc::key('K'),
                TOKEN_2022,
                &parsed_raw.to_string(),
                3
            )])),
            "getMultipleAccounts" => {
                let keys = request["params"][0].as_array().unwrap();
                let rows: Vec<Value> = keys
                    .iter()
                    .map(|key| {
                        if key == &json!(rpc::key('E')) {
                            if matches!(case, RawCase::MissingMint) {
                                return Value::Null;
                            }
                            let mut data = vec![0; 82];
                            data[36..44].copy_from_slice(&1_000_000u64.to_le_bytes());
                            data[44] = 3;
                            data[45] = 1;
                            if matches!(case, RawCase::Unsupported) {
                                data.resize(166, 0);
                                data[165] = 1;
                                data.extend([1, 0, 0, 0]);
                            }
                            raw_row(data, TOKEN_2022)
                        } else {
                            assert_eq!(key, &json!(rpc::key('K')));
                            let mut data = vec![0; 165];
                            data[..32].copy_from_slice(&decode(&rpc::key('E')));
                            data[32..64].copy_from_slice(&decode(&rpc::key(
                                if matches!(case, RawCase::WalletMismatch) {
                                    'Z'
                                } else {
                                    'A'
                                },
                            )));
                            let amount =
                                parsed_raw + u64::from(matches!(case, RawCase::AmountMismatch));
                            data[64..72].copy_from_slice(&amount.to_le_bytes());
                            data[108] = if matches!(case, RawCase::Frozen) {
                                2
                            } else {
                                1
                            };
                            if matches!(case, RawCase::Supported170) {
                                data.extend([2, 7, 0, 0, 0]);
                            }
                            raw_row(
                                data,
                                if matches!(case, RawCase::ProgramMismatch) {
                                    CLASSIC
                                } else {
                                    TOKEN_2022
                                },
                            )
                        }
                    })
                    .collect();
                json!({"jsonrpc":"2.0", "id":1, "result":{"context":{"slot":124},"value":rows}})
            }
            other => panic!("unexpected LOCAL RPC method: {other}"),
        };
        response.into()
    })
}

fn raw_row(data: Vec<u8>, owner: &str) -> Value {
    json!({"owner":owner,"executable":false,"lamports":2039280,"data":[base64(&data),"base64"]})
}

fn base64(bytes: &[u8]) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        result.push(A[((n >> 18) & 63) as usize] as char);
        result.push(A[((n >> 12) & 63) as usize] as char);
        result.push(if chunk.len() > 1 {
            A[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        result.push(if chunk.len() > 2 {
            A[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    result
}

fn decode(text: &str) -> [u8; 32] {
    const A: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let mut result = [0u8; 32];
    for byte in text.bytes() {
        let mut carry = A.iter().position(|cell| *cell == byte).unwrap() as u16;
        for cell in result.iter_mut().rev() {
            carry += u16::from(*cell) * 58;
            *cell = carry as u8;
            carry >>= 8;
        }
        assert_eq!(carry, 0);
    }
    result
}

pub fn assert_calls(calls: &[Value], builds: usize) {
    assert_eq!(
        calls.len(),
        builds * 4,
        "one balance + two inventories + one raw batch per build"
    );
    for group in calls.chunks(4) {
        rpc::assert_requests(&group[..3], &rpc::key('A'), 3);
        assert_eq!(group[3]["method"], "getMultipleAccounts");
        let keys = group[3]["params"][0].as_array().unwrap();
        assert!(keys.len() <= 32);
        assert_eq!(keys, &vec![json!(rpc::key('E')), json!(rpc::key('K'))]);
        assert_eq!(
            group[3]["params"][1],
            json!({"encoding":"base64", "commitment":"confirmed", "minContextSlot":123})
        );
    }
}
