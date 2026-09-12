use super::native_funding_fixture::*;
use crate::execution_priority_fee_wire::decode_priority_fee;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use sha2::{Digest, Sha256};

pub(super) fn corpus() -> Result<Vec<(String, String)>> {
    let good = payload(&budget())?;
    let bytes = STANDARD.decode(&good)?;
    let mut cases = vec![
        ("legacy".into(), good.clone()),
        ("v0".into(), version_zero(&good)?),
        ("bad-base64".into(), "!".into()),
        ("oversize".into(), STANDARD.encode([0; 1233])),
    ];
    for buy in [true, false] {
        for extension in [true, false] {
            cases.push((
                format!("direct-{buy}-{extension}"),
                payload(&direct(buy, extension, 10_000_000)?)?,
            ));
        }
    }
    for n in 0..bytes.len() {
        cases.push((format!("truncate-{n}"), STANDARD.encode(&bytes[..n])));
    }
    for n in 0..bytes.len() {
        for value in [0, 1, 2, 127, 128, 255] {
            let mut changed = bytes.clone();
            changed[n] = value;
            cases.push((format!("byte-{n}-{value}"), STANDARD.encode(changed)));
        }
    }
    let mut duplicate = bytes.clone();
    duplicate[101..133].copy_from_slice(&bytes[69..101]);
    cases.push(("duplicate-key".into(), STANDARD.encode(duplicate)));
    let mut noncanonical = bytes.clone();
    noncanonical.splice(0..1, [0x81, 0]);
    cases.push((
        "noncanonical-signature-count".into(),
        STANDARD.encode(noncanonical),
    ));
    let mut trailing = bytes.clone();
    trailing.push(0);
    cases.push(("trailing".into(), STANDARD.encode(trailing)));
    let mut alt = STANDARD.decode(version_zero(&good)?)?;
    *alt.last_mut().unwrap() = 1;
    alt.extend([0; 34]);
    cases.push(("ALT".into(), STANDARD.encode(alt)));
    for (limit, price) in [
        (0, 0),
        (1_400_001, 1),
        (1, 0),
        (1, u64::MAX),
        (1_400_000, u64::MAX),
    ] {
        cases.push((
            format!("budget-{limit}-{price}"),
            payload(&super::priority_fee_fixture::budget(limit, price))?,
        ));
    }
    for tag in [0, 1, 2, 3, 4, 5, 255] {
        for data in [
            vec![tag],
            [vec![tag], 32_768_u32.to_le_bytes().to_vec()].concat(),
        ] {
            let mut instructions = budget();
            let mut instruction = instructions[0].clone();
            instruction.data = data;
            instructions.push(instruction);
            let p = payload(&instructions)?;
            cases.push((
                format!("extra-budget-{tag}-{}", instructions[2].data.len()),
                p.clone(),
            ));
            let mut b = STANDARD.decode(p)?;
            b.push(0);
            cases.push((
                format!("error-order-{tag}-{}", instructions[2].data.len()),
                STANDARD.encode(b),
            ));
        }
    }
    let mut native_bad = budget();
    let mut t = transfer(WALLET, PEER, 3);
    t.data.pop();
    native_bad.push(t);
    cases.push((
        "native-invalid-does-not-gate-priority".into(),
        payload(&native_bad)?,
    ));
    cases.push((
        "foreign-signer".into(),
        foreign_signer_transfer(WALLET, u64::MAX)?,
    ));
    Ok(cases)
}

#[test]
fn wire_extraction_oracle_preserves_every_old_result_and_error() -> Result<()> {
    let records:Vec<_>=corpus()?.into_iter().map(|(name,payload)| {
        let result=match decode_priority_fee(&payload) {
            Ok(v)=>json!({"message":v.message_sha256,"transaction":v.transaction_sha256,"limit":v.limit.get(),"price":v.price,"total":v.total}),
            Err(e)=>json!({"error":format!("{e:#}")}),
        };
        json!({"case":name,"result":result})
    }).collect();
    let text = serde_json::to_string(&records)?;
    let digest = format!("{:x}", Sha256::digest(text.as_bytes()));
    if let Ok(path) = std::env::var("COPYBOT_B18_ORACLE_CAPTURE") {
        std::fs::write(
            std::path::Path::new(&path).join("priority-oracle-before.json"),
            &text,
        )?;
        std::fs::write(
            std::path::Path::new(&path).join("priority-oracle.sha256"),
            &digest,
        )?;
    } else {
        assert_eq!(
            digest,
            include_str!("wire_extraction_oracle.sha256").trim(),
            "priority decoder result/error drift"
        );
    }
    eprintln!(
        "B18 PRIORITY ORACLE cases={} sha256={digest}",
        records.len()
    );
    Ok(())
}
