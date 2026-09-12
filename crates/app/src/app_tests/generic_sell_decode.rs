//! Independent test wire reader. Does not call the production serializer/parser.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn verify(payload: &str, response: &Value, payer: &str) -> usize {
    let bytes = STANDARD.decode(payload).unwrap();
    let mut wire = Reader {
        bytes: &bytes,
        offset: 0,
    };
    assert_eq!(wire.short(), 1);
    assert_eq!(wire.take(64), &[0; 64]);
    let header = wire.take(3).to_vec();
    assert_eq!(&header[..2], &[1, 0]); // legacy, one writable signer
    let count = wire.short();
    let keys: Vec<_> = (0..count)
        .map(|_| bs58::encode(wire.take(32)).into_string())
        .collect();
    assert_eq!(keys[0], payer);
    let blockhash: Vec<u8> = response["blockhashWithMetadata"]["blockhash"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_u64().unwrap() as u8)
        .collect();
    assert_eq!(wire.take(32), blockhash);
    let mut expected: Vec<&Value> = Vec::new();
    for name in ["computeBudgetInstructions", "setupInstructions"] {
        expected.extend(response[name].as_array().unwrap());
    }
    expected.push(&response["swapInstruction"]);
    if !response["cleanupInstruction"].is_null() {
        expected.push(&response["cleanupInstruction"]);
    }
    assert!(response["tokenLedgerInstruction"].is_null());
    assert!(response["otherInstructions"].as_array().unwrap().is_empty());
    let mut privileges = BTreeMap::<String, (bool, bool)>::new();
    privileges.insert(payer.to_owned(), (true, true));
    for ix in &expected {
        privileges
            .entry(ix["programId"].as_str().unwrap().into())
            .or_default();
        for meta in ix["accounts"].as_array().unwrap() {
            let p = privileges
                .entry(meta["pubkey"].as_str().unwrap().into())
                .or_default();
            p.0 |= meta["isSigner"].as_bool().unwrap();
            p.1 |= meta["isWritable"].as_bool().unwrap();
        }
    }
    assert_eq!(keys.len(), privileges.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            privileges[key],
            (i == 0, i == 0 || i < count - header[2] as usize)
        );
    }
    assert_eq!(wire.short(), expected.len());
    for ix in expected {
        let program = wire.byte() as usize;
        assert_eq!(keys[program], ix["programId"].as_str().unwrap());
        let count = wire.short();
        let accounts = ix["accounts"].as_array().unwrap();
        assert_eq!(count, accounts.len());
        for meta in accounts {
            let index = wire.byte() as usize;
            assert_eq!(keys[index], meta["pubkey"].as_str().unwrap());
        }
        let count = wire.short();
        assert_eq!(
            wire.take(count),
            STANDARD.decode(ix["data"].as_str().unwrap()).unwrap()
        );
    }
    assert_eq!(wire.offset, bytes.len());
    bytes.len()
}
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let r = &self.bytes[self.offset..self.offset + n];
        self.offset += n;
        r
    }
    fn byte(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn short(&mut self) -> usize {
        let mut n = 0;
        for shift in (0..21).step_by(7) {
            let b = self.byte();
            n |= ((b & 127) as usize) << shift;
            if b & 128 == 0 {
                return n;
            }
        }
        panic!("bad shortvec")
    }
}
