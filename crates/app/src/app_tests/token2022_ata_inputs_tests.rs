use crate::execution_solana_tx::PubkeyBytes;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::path::PathBuf;

pub(super) const PAYER: &str = "FniEKNLfrjD1mJJkXwtLRW6iHNnFKpErgjvHxJmgrbT1";
pub(super) const MINT: &str = "DwVyHKdjuzK1MYRpUn4B2bfQ2VMsiv5t3o3dHBF8pump";
pub(super) const ATA: &str = "7HME8NcMkf1Vh1oizzL4t8C8cK2ngBMmnaG2Gp68vZxJ";
pub(super) const TOKEN2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub(super) const RESERVE: u64 = 50_000_001;
pub(super) fn key(s: &str) -> PubkeyBytes {
    bs58::decode(s).into_vec().unwrap().try_into().unwrap()
}
pub(super) fn output(name: &str) -> PathBuf {
    let p = std::env::var("B119_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!(
                "copybot-token2022-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            ))
        })
        .join(name);
    std::fs::create_dir_all(&p).unwrap();
    p
}
pub(super) fn save(dir: &std::path::Path, name: &str, data: &Value) {
    std::fs::write(dir.join(name), serde_json::to_vec_pretty(data).unwrap()).unwrap();
}
pub(super) fn frozen(num: u32) -> Result<Value> {
    let f: Value = serde_json::from_slice(&std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/token2022_ata/{num}.json")),
    )?)?;
    let payload = f["payload"].as_str().unwrap();
    let wire = STANDARD.decode(payload)?;
    assert_eq!(wire[0], 1);
    assert!(wire[1..65].iter().all(|b| *b == 0));
    let decoded = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    assert_eq!(decoded.binding.transaction_sha256, f["payload_sha256"]);
    assert_eq!(decoded.binding.message_sha256, f["message_sha256"]);
    assert_eq!(
        STANDARD.encode(&decoded.binding.message_bytes),
        f["message_base64"]
    );
    let keys: Vec<_> = decoded
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect();
    assert_eq!(json!(keys), f["keys"]);
    for (ix, expected) in decoded
        .instructions
        .iter()
        .zip(f["instructions"].as_array().unwrap())
    {
        assert_eq!(ix.index, expected["index"].as_u64().unwrap() as usize);
        assert_eq!(
            bs58::encode(ix.program.pubkey).into_string(),
            expected["program"]
        );
        assert_eq!(json!(ix.account_indices), expected["operand_indexes"]);
        assert_eq!(STANDARD.encode(&ix.data), expected["data_base64"]);
    }
    assert_eq!(
        decoded.instructions.len(),
        f["instructions"].as_array().unwrap().len()
    );
    Ok(f)
}

#[derive(Clone)]
pub(super) struct Spec {
    pub payload: String,
    pub wallet: PubkeyBytes,
    pub message: String,
    pub keys: Value,
    pub rows: Vec<Value>,
    pub fee: Option<u64>,
    pub rent: u64,
    pub rent170: u64,
}
impl Spec {
    pub fn buy(existing: bool) -> Result<Self> {
        let f = frozen(112)?;
        let mut s = Self::from_frozen(&f);
        assert_eq!(f["bytes"], 1197);
        assert_eq!(f["keys"][0], PAYER);
        assert_eq!(f["instructions"][5]["operands"][1], ATA);
        assert_eq!(f["instructions"][5]["operands"][3], MINT);
        assert_eq!(f["instructions"][5]["operands"][5], TOKEN2022);
        assert_eq!(
            crate::execution_pumpswap_accounts::associated_token_address(
                &key(PAYER),
                &key(MINT),
                &key(TOKEN2022)
            ),
            key(ATA)
        );
        crate::execution_native_floor::verify_final_native_floor(&s.payload, s.wallet, RESERVE)?;
        if existing {
            // Supplied Token2022 base-layout state, not classic SPL and not a network balance.
            let bytes = super::native_setup_fixture::token_bytes(key(MINT), key(PAYER), 0, None);
            assert_eq!(bytes.len(), 165);
            assert_eq!(bytes[108], 1);
            s.rows[2] = json!({"lamports":2039280,"owner":TOKEN2022,"executable":false,"data":[STANDARD.encode(bytes),"base64"]});
        }
        Ok(s)
    }
    pub fn sufficient(arm: &str) -> Result<Self> {
        let mut s = Self::buy(false)?;
        let f: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/token2022_ata/sufficient.json"
        ))?;
        assert_eq!(f["payload"], s.payload);
        assert_eq!(f["keys"], s.keys);
        s.rows = f["rows"].as_array().unwrap().clone();
        match arm {
            "absent" => {}
            "prefunded" => s.set(
                ATA,
                super::initial_sol_rpc_fixture::system(f["prefunded_lamports"].as_u64().unwrap()),
            ),
            "existing" => s.set(ATA, f["synthetic_existing_ata"].clone()),
            _ => panic!("invalid arm"),
        }
        Ok(s)
    }
    pub fn set(&mut self, key: &str, row: Value) {
        let i = self
            .keys
            .as_array()
            .unwrap()
            .iter()
            .position(|k| k == key)
            .unwrap();
        self.rows[i] = row;
    }
    pub fn row(&self, key: &str) -> &Value {
        &self.rows[self
            .keys
            .as_array()
            .unwrap()
            .iter()
            .position(|k| k == key)
            .unwrap()]
    }
    pub fn from_frozen(f: &Value) -> Self {
        let keys = f["keys"].clone();
        let mut rows = vec![Value::Null; keys.as_array().unwrap().len()];
        rows[0] = super::initial_sol_rpc_fixture::system(1_000_000_000);
        Self {
            payload: f["payload"].as_str().unwrap().into(),
            wallet: key(f["keys"][0].as_str().unwrap()),
            message: f["message_base64"].as_str().unwrap().into(),
            keys,
            rows,
            fee: Some(27_000),
            rent: 2_039_280,
            rent170: 2_074_080,
        }
    }
    pub fn synthetic(unknown: bool) -> Result<Self> {
        use super::native_ata_fixture::{budget, create, payload, WALLET};
        let mut ix = budget();
        ix.push(create([73; 32]));
        ix.push(super::native_funding_fixture::transfer(
            WALLET, [74; 32], 10_000_000,
        ));
        if unknown {
            ix.push(crate::execution_solana_tx::SolanaInstruction {
                program_id: crate::execution_pumpswap_accounts::system_program_id(),
                accounts: vec![],
                data: vec![255, 0, 0, 0],
            });
        }
        let p = payload(&ix)?;
        let d = crate::execution_transaction_wire::decode_message(&p, |_| Ok(()))?;
        let keys: Vec<_> = d
            .binding
            .accounts
            .iter()
            .map(|a| bs58::encode(a.pubkey).into_string())
            .collect();
        Ok(Self::from_frozen(
            &json!({"payload":p,"message_base64":STANDARD.encode(&d.binding.message_bytes),"keys":keys}),
        ))
    }
    pub fn save(&self, dir: &std::path::Path) {
        save(
            dir,
            "supplied-facts.json",
            &json!({"provenance":"synthetic loopback facts, not Bank/network/Token2022 rent proof",
            "payload":self.payload,"wallet":bs58::encode(self.wallet).into_string(),"message":self.message,
            "ordered_keys":self.keys,"ordered_accounts":self.rows,"fee":self.fee,"classic_rent165":self.rent}),
        );
    }
}
