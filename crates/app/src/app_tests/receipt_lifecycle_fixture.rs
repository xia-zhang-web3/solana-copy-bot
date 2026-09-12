use super::receipt_reconciliation_fixture::*;
use serde_json::{json, Value};

pub(super) const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

pub(super) fn program_receipt(side: &str, program: &str) -> Value {
    let mut value = receipt(
        side,
        if side == "buy" {
            -900_000_000
        } else {
            1_200_000_000
        },
    );
    for name in ["preTokenBalances", "postTokenBalances"] {
        value["result"]["meta"][name][0]["programId"] = json!(program);
    }
    let keys = value["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap();
    keys.extend([
        json!({"pubkey":TOKEN,"signer":false,"writable":false}),
        json!({"pubkey":program,"signer":false,"writable":false}),
        json!({"pubkey":"pool-account","signer":false,"writable":true}),
        json!({"pubkey":"pool-owner","signer":true,"writable":true}),
        json!({"pubkey":"FixtureSwapProgram","signer":false,"writable":false}),
        json!({"pubkey":"SysvarRent111111111111111111111111111111111","signer":false,"writable":false}),
    ]);
    for name in ["preBalances", "postBalances"] {
        value["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .extend([
                json!(1_461_600),
                json!(1),
                json!(2_039_280),
                json!(5_000_000_000_u64),
                json!(1),
                json!(1),
            ]);
    }
    for (name, amount) in [
        ("preTokenBalances", "10000"),
        (
            "postTokenBalances",
            if side == "buy" { "3000" } else { "17000" },
        ),
    ] {
        value["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .push(json!({
            "accountIndex":4,"owner":"pool-owner","mint":TOKEN,"programId":program,
            "uiTokenAmount":{"amount":amount,"decimals":3}}));
    }
    let (source, destination) = if side == "buy" {
        ("pool-account", "token-account")
    } else {
        ("token-account", "pool-account")
    };
    value["result"]["transaction"]["message"]["instructions"] = json!([{
        "programId":program,"parsed":{"type":"transferChecked","info":{
            "source":source,"destination":destination,"mint":TOKEN,
            "authority":if side == "buy" { "pool-owner" } else { WALLET },
            "tokenAmount":{"amount":"7000","decimals":3}}}}]);
    value["result"]["meta"]["innerInstructions"] = json!([]);
    value
}

/// Matching successful token initialization/closure, never just a deleted row.
pub(super) fn lifecycle_receipt(side: &str, program: &str, inner: bool, prefunded: bool) -> Value {
    let mut value = program_receipt(side, program);
    let creation = side == "buy";
    value["result"]["meta"][if creation {
        "preTokenBalances"
    } else {
        "postTokenBalances"
    }]
    .as_array_mut()
    .unwrap()
    .remove(0);
    if !creation {
        value["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("20000");
    }
    value["result"]["meta"][if creation {
        "preBalances"
    } else {
        "postBalances"
    }][1] = json!(if creation && prefunded { 2_039_280 } else { 0 });
    let lifecycle = json!({"programId":program,"parsed": if creation {
        json!({"type":"initializeAccount3","info":{"account":"token-account","mint":TOKEN,"owner":WALLET}})
    } else {
        json!({"type":"closeAccount","info":{"account":"token-account","destination":WALLET,"owner":WALLET}})
    }});
    let transfer = &mut value["result"]["transaction"]["message"]["instructions"][0];
    if !creation {
        transfer["parsed"]["info"]["tokenAmount"]["amount"] = json!("10000");
    }
    let mut sequence = if creation {
        vec![lifecycle, transfer.clone()]
    } else {
        vec![transfer.clone(), lifecycle]
    };
    if creation && !prefunded {
        value["result"]["transaction"]["message"]["accountKeys"].as_array_mut().unwrap()
            .push(json!({"pubkey":"11111111111111111111111111111111","signer":false,"writable":false}));
        for name in ["preBalances", "postBalances"] {
            value["result"]["meta"][name]
                .as_array_mut()
                .unwrap()
                .push(json!(1));
        }
        sequence.insert(
            0,
            json!({"programId":"11111111111111111111111111111111",
            "parsed":{"type":"createAccount","info":{"source":WALLET,
                "newAccount":"token-account","lamports":2_039_280,"space":165,"owner":program}}}),
        );
    }
    if inner {
        value["result"]["transaction"]["message"]["instructions"] = json!([{
            "programId":"FixtureSwapProgram", "accounts":[WALLET,"token-account","pool-account"],"data":"1"}]);
        value["result"]["meta"]["innerInstructions"] = json!([{"index":0,"instructions":sequence}]);
    } else {
        value["result"]["transaction"]["message"]["instructions"] = json!(sequence);
    }
    value
}
