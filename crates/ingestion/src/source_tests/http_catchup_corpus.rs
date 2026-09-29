//! Four immutable saved07 results; modeled parent/hash only for missing history.
use crate::source::http_recovery::normalize_confirmed_http_block;
use prost::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
pub(super) const CURSOR: u64 = 451_679_659;
pub(super) const ANCHOR: u64 = 451_679_839;
pub(super) const FIRST: u64 = CURSOR - 1;
pub(super) const PERIOD_MS: u64 = 417;
// Keep full-sized producer blocks flowing for a measured 60 seconds past ACK.
pub(super) const TAIL: u64 = ANCHOR + 400;
pub(super) fn digest(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}
pub(super) fn save(path: impl AsRef<Path>, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
pub(super) struct Corpus {
    pub templates: Vec<SubscribeUpdateBlock>,
    encoded_bytes: [usize; 4],
    hashes: BTreeMap<u64, String>,
}
impl Corpus {
    pub fn modeled_encoded_bytes(&self, slot: u64) -> usize {
        self.encoded_bytes[Self::index(slot)]
    }
    pub fn hash(&self, slot: u64) -> String {
        self.hashes
            .get(&slot)
            .cloned()
            .unwrap_or_else(|| bs58::encode(Sha256::digest(slot.to_le_bytes())).into_string())
    }
    pub fn index(slot: u64) -> usize {
        match slot {
            FIRST => 0,
            CURSOR => 1,
            s if s == CURSOR + 3 => 2,
            s if s == CURSOR + 4 => 3,
            _ => ((slot - FIRST) % 4) as usize,
        }
    }
    pub fn block(&self, slot: u64) -> SubscribeUpdateBlock {
        let mut b = self.templates[Self::index(slot)].clone();
        b.slot = slot;
        b.parent_slot = slot - 1;
        b.blockhash = self.hash(slot);
        b.parent_blockhash = self.hash(slot - 1);
        b
    }
}
pub(super) fn load(root: &Path) -> Corpus {
    let source = PathBuf::from(std::env::var("COPYBOT_PROBE07_HTTP_EVIDENCE_DIR").unwrap());
    std::fs::create_dir(root.join("corpus")).unwrap();
    let mut originals = BTreeMap::new();
    let mut results = vec![];
    let mut templates = vec![];
    let mut hashes = BTreeMap::new();
    let mut source_facts = vec![];
    for (id, slot, expected) in [
        (6, FIRST, 4_306_993),
        (13, CURSOR, 4_810_559),
        (14, CURSOR + 3, 9_301_608),
        (15, CURSOR + 4, 10_223_194),
    ] {
        let filename = format!("response-{id:06}.json");
        let raw = std::fs::read(source.join(&filename)).unwrap();
        let meta: Value = serde_json::from_slice(
            &std::fs::read(source.join(format!("response-{id:06}.meta.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(raw.len(), expected);
        assert_eq!(digest(&raw), meta["response_sha256"].as_str().unwrap());
        assert_eq!(meta["request"]["params"][0], slot);
        let v: Value = serde_json::from_slice(&raw).unwrap();
        let result = v["result"].clone();
        let b = normalize_confirmed_http_block(slot, &result).unwrap();
        hashes.insert(slot, b.blockhash.clone());
        hashes.insert(slot - 1, b.parent_blockhash.clone());
        source_facts.push(json!({"slot":slot,"source_file":filename,"raw_bytes":raw.len(),
            "original_sha256":digest(&raw),"transactions":b.transactions.len(),"normalized_bytes":b.encoded_len(),
            "fee_lamports_sum":b.transactions.iter().map(|t|u128::from(t.meta.as_ref().unwrap().fee)).sum::<u128>().to_string()}));
        originals.insert(slot, raw);
        results.push(result);
        templates.push(b);
    }
    let encoded_bytes = std::array::from_fn(|i| templates[i].encoded_len());
    let corpus = Corpus { templates, encoded_bytes, hashes };
    let mut models = vec![];
    for slot in FIRST..=ANCHOR + 16 {
        let index = Corpus::index(slot);
        let raw = if let Some(raw) = originals.get(&slot) {
            raw.clone()
        } else {
            let mut v = results[index].clone();
            v["parentSlot"] = json!(slot - 1);
            v["previousBlockhash"] = json!(corpus.hash(slot - 1));
            v["blockhash"] = json!(corpus.hash(slot));
            assert_eq!(v["transactions"], results[index]["transactions"]);
            assert_eq!(v["rewards"], results[index]["rewards"]);
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":v})).unwrap()
        };
        let block = corpus.block(slot);
        assert!(block.encoded_len() < 16 << 20);
        assert!(raw.len() < 16 << 20);
        models.push(json!({"slot":slot,"template_slot":corpus.templates[index].slot,
             "modeled":!originals.contains_key(&slot),"raw_bytes":raw.len(),"normalized_bytes":block.encoded_len(),
             "transactions":block.transactions.len(),"blockhash":block.blockhash,"parent_slot":block.parent_slot,
             "parent_hash":block.parent_blockhash}));
        std::fs::write(root.join(format!("corpus/{slot}.json")), raw).unwrap();
    }
    save(
        root.join("CORPUS_BINDING.json"),
        &json!({"originals":source_facts,"blocks":models,
        "cursor":CURSOR,"anchor":ANCHOR,"missing_slot_span":ANCHOR-CURSOR,
        "modeled_fields":["slot/protobuf delivery coordinate","parentSlot","previousBlockhash","blockhash"],
        "unchanged_fields":"all transaction, numeric, token, fee, instruction, signature, rewards, time facts; copied full blocks are a local load model, not extra real activity",
        "period_ms":PERIOD_MS,"preloaded_protobuf_templates":4}),
    );
    corpus
}

pub(super) fn ensure_tail(root: &Path, corpus: &Corpus, lower: u64, upper: u64) {
    let mut facts = vec![];
    for slot in lower..=upper {
        let path = root.join(format!("corpus/{slot}.json"));
        if path.exists() {
            continue;
        }
        let index = Corpus::index(slot);
        let raw = std::fs::read(root.join(format!("corpus/{}.json", corpus.templates[index].slot)))
            .unwrap();
        let envelope: Value = serde_json::from_slice(&raw).unwrap();
        let mut value = envelope["result"].clone();
        value["parentSlot"] = json!(slot - 1);
        value["previousBlockhash"] = json!(corpus.hash(slot - 1));
        value["blockhash"] = json!(corpus.hash(slot));
        assert_eq!(value["transactions"], envelope["result"]["transactions"]);
        let raw = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":value})).unwrap();
        facts.push(json!({"slot":slot,"modeled":true,"template_slot":corpus.templates[index].slot,"raw_bytes":raw.len(),"sha256":digest(&raw)}));
        std::fs::write(path, raw).unwrap();
    }
    save(
        root.join("RESTART_TAIL_MODEL.json"),
        &json!({"blocks":facts,"financial_facts":"copied unchanged full template; modeled block coordinates/parent/hash only"}),
    );
}
