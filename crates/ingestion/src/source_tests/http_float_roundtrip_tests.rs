//! Run19: retained decimal input must survive JSON parsing before strict identity.
use crate::source::http_recovery::{
    anchor_diagnostic, identity, response, ConfirmedHttpRecovery, RawRecoveredBlock,
};
use prost::Message;
use serde_json::{json, Value};
use std::{fs, path::PathBuf, time::Duration};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

const LITERAL: &str = "16093581109.655773";
const EXPECTED_BITS: u64 = 0x420dfa0479ad3f06;

#[test]
fn http_float_roundtrip_json_retains_correct_ieee_bits() {
    let direct: f64 = LITERAL.parse().unwrap();
    let value: Value = serde_json::from_slice(LITERAL.as_bytes()).unwrap();
    let parsed = value.as_f64().unwrap();
    // The previous parser rounded the significand first, then divided by 10^6.
    let rounded_twice = (16_093_581_109_655_773_u64 as f64) / 1_000_000.0;
    eprintln!(
        "direct={:016x} json={:016x} cast_divide={:016x}",
        direct.to_bits(),
        parsed.to_bits(),
        rounded_twice.to_bits()
    );
    assert_eq!(direct.to_bits(), EXPECTED_BITS);
    assert_eq!(rounded_twice.to_bits(), EXPECTED_BITS - 1);
    assert_eq!(parsed.to_bits(), direct.to_bits());
}

#[test]
fn http_float_roundtrip_typed_normalization_keeps_the_original_number() {
    let mut result = super::raw_block();
    let ui = &mut result["transactions"][0]["meta"]["preTokenBalances"][0]["uiTokenAmount"];
    ui["amount"] = json!("16093581109655774");
    ui["decimals"] = json!(6);
    ui["uiAmountString"] = json!("16093581109.655774");
    ui["uiAmount"] = json!(0.0);
    let raw = serde_json::to_string(&json!({"jsonrpc":"2.0","id":1,"result":result}))
        .unwrap()
        .replacen("\"uiAmount\":0.0", &format!("\"uiAmount\":{LITERAL}"), 1);
    let result = response::interpret(200, 1, "getBlock", raw.as_bytes()).unwrap();
    let block = super::block::parse(10, &result).unwrap();
    let ui = block.transactions[0]
        .meta
        .as_ref()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_ref()
        .unwrap();
    assert_eq!(ui.ui_amount.to_bits(), EXPECTED_BITS);
    assert_eq!(ui.amount, "16093581109655774");
    assert_eq!(ui.decimals, 6);
    assert_eq!(ui.ui_amount_string, "16093581109.655774");
    // These two saved decimal spellings round to the same float. Change the
    // other witnesses explicitly to prove neither substitutes for uiAmount.
    let mut other = result.clone();
    other["transactions"][0]["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("1");
    other["transactions"][0]["meta"]["preTokenBalances"][0]["uiTokenAmount"]["uiAmountString"] =
        json!("1");
    let other = super::block::parse(10, &other).unwrap();
    assert_eq!(
        other.transactions[0]
            .meta
            .as_ref()
            .unwrap()
            .pre_token_balances[0]
            .ui_token_amount
            .as_ref()
            .unwrap()
            .ui_amount
            .to_bits(),
        EXPECTED_BITS
    );
}

#[test]
fn http_float_roundtrip_numeric_boundaries_remain_strict() {
    let max: Value = serde_json::from_str("18446744073709551615").unwrap();
    let min: Value = serde_json::from_str("-9223372036854775808").unwrap();
    assert_eq!(max.as_u64(), Some(u64::MAX));
    assert_eq!(min.as_i64(), Some(i64::MIN));
    for literal in [
        "-0.0",
        "0.0",
        "5e-324",
        "1.7976931348623157e308",
        "1.01",
        LITERAL,
    ] {
        let parsed: Value = serde_json::from_slice(literal.as_bytes()).unwrap();
        assert_eq!(
            parsed.as_f64().unwrap().to_bits(),
            literal.parse::<f64>().unwrap().to_bits()
        );
    }
    for raw in [b"1e309".as_slice(), b"NaN", b"Infinity", b"01.0"] {
        assert!(serde_json::from_slice::<Value>(raw).is_err());
    }
}

fn pair_root() -> PathBuf {
    PathBuf::from(
        std::env::var_os("COPYBOT_HTTP_FLOAT_PAIR_ROOT")
            .expect("explicit copied offline pair root required"),
    )
}

fn report(name: &str, value: &Value) {
    let root = PathBuf::from(
        std::env::var_os("COPYBOT_HTTP_FLOAT_RESULT_ROOT")
            .expect("explicit offline result directory required"),
    );
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join(name), serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn recovery() -> ConfirmedHttpRecovery {
    ConfirmedHttpRecovery::new(
        "http://127.0.0.1:1",
        None,
        1,
        32 << 20,
        Duration::from_secs(1),
    )
    .unwrap()
}

#[test]
#[ignore = "requires explicit copied saved run19 pair fixtures; performs no I/O requests"]
fn http_float_roundtrip_saved_pairs_match_through_actual_recovery_gate() {
    let recovery = recovery();
    let mut results = Vec::new();
    for number in [1, 2] {
        let root = pair_root().join(format!("pair-{number:02}"));
        let raw = fs::read(root.join("http_response.json")).unwrap();
        let envelope: Value = serde_json::from_slice(&raw).unwrap();
        let id = envelope["id"].as_u64().unwrap();
        let grpc =
            SubscribeUpdateBlock::decode(fs::read(root.join("grpc_typed.pb")).unwrap().as_slice())
                .unwrap();
        let old = SubscribeUpdateBlock::decode(
            fs::read(root.join("http_normalized.pb"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let mut recovered = recovery
            .normalize_raw_block(RawRecoveredBlock::from_response(
                grpc.slot,
                200,
                id,
                raw.clone(),
            ))
            .unwrap();
        assert_eq!(recovered.raw_response, raw);
        let comparison = identity::block_equivalent(&grpc, &recovered.block);
        let gate = anchor_diagnostic::admit_recovered(None, &grpc, &mut recovered);
        let ui = |block: &SubscribeUpdateBlock| {
            block
                .transactions
                .iter()
                .find(|tx| tx.index == 132)
                .unwrap()
                .meta
                .as_ref()
                .unwrap()
                .pre_token_balances[1]
                .ui_token_amount
                .as_ref()
                .unwrap()
                .ui_amount
                .to_bits()
        };
        results.push(json!({"pair":number,"slot":grpc.slot,
            "transactions":grpc.transactions.len(),"raw_bytes":raw.len(),
            "old_saved_matches":identity::block_equivalent(&grpc,&old),
            "comparison":comparison,"gate_admitted":gate.is_ok(),
            "gate_error":gate.err().map(|e|e.to_string()),
            "target_bits":if number==2 {Some(json!({"grpc":format!("{:016x}",ui(&grpc)),
                "old_http":format!("{:016x}",ui(&old)),"new_http":format!("{:016x}",ui(&recovered.block))}))}else{None}}));
    }
    report("SAVED_PAIRS.json", &json!(results));
    assert!(
        results
            .iter()
            .all(|r| r["comparison"] == true && r["gate_admitted"] == true),
        "saved full pairs must MATCH; see retained report"
    );
}

#[test]
#[ignore = "requires explicit copied saved run19 pair fixtures; performs no I/O requests"]
fn http_float_roundtrip_saved_pair_mutations_still_refuse() {
    let root = pair_root().join("pair-02");
    let grpc =
        SubscribeUpdateBlock::decode(fs::read(root.join("grpc_typed.pb")).unwrap().as_slice())
            .unwrap();
    let raw = fs::read(root.join("http_response.json")).unwrap();
    let id = serde_json::from_slice::<Value>(&raw).unwrap()["id"]
        .as_u64()
        .unwrap();
    let recovery = recovery();
    let mut cases = Vec::new();
    for name in [
        "raw_amount",
        "ui_amount",
        "ui_string",
        "decimals",
        "account_index",
        "mint",
        "owner",
        "execution_index",
        "signature",
        "blockhash",
        "parent_hash",
        "message",
        "balance",
        "fee",
    ] {
        let mut recovered = recovery
            .normalize_raw_block(RawRecoveredBlock::from_response(
                grpc.slot,
                200,
                id,
                raw.clone(),
            ))
            .unwrap();
        assert!(identity::block_equivalent(&grpc, &recovered.block));
        let block = &mut recovered.block;
        let tx = &mut block.transactions[132];
        let meta = tx.meta.as_mut().unwrap();
        let token = &mut meta.pre_token_balances[1];
        let ui = token.ui_token_amount.as_mut().unwrap();
        match name {
            "raw_amount" => ui.amount = "16093581109655775".into(),
            "ui_amount" => ui.ui_amount = f64::from_bits(ui.ui_amount.to_bits() - 1),
            "ui_string" => ui.ui_amount_string.push('1'),
            "decimals" => ui.decimals += 1,
            "account_index" => token.account_index += 1,
            "mint" => token.mint.push('1'),
            "owner" => token.owner.push('1'),
            "execution_index" => tx.index += 1,
            "signature" => tx.signature[0] ^= 1,
            "blockhash" => block.blockhash.push('1'),
            "parent_hash" => block.parent_blockhash.push('1'),
            "message" => {
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .recent_blockhash[0] ^= 1
            }
            "balance" => meta.pre_balances[0] += 1,
            "fee" => meta.fee += 1,
            _ => unreachable!(),
        }
        let error = anchor_diagnostic::admit_recovered(None, &grpc, &mut recovered).unwrap_err();
        assert_eq!(error.to_string(), "http_recovery_live_anchor_conflict");
        cases.push(json!({"mutation":name,"rejected":true,"reason":error.to_string()}));
    }
    report("STRICT_MUTATIONS.json", &json!(cases));
}
