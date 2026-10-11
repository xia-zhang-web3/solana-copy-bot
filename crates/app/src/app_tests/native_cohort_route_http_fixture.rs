//! Loopback-only provider replies and immutable request captures; no external I/O.
use anyhow::Result;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::execution_solana_tx::SolanaInstruction;
use base64::{engine::general_purpose::STANDARD, Engine};

pub(super) struct Server {
    pub url: String,
    pub calls: Arc<Mutex<Vec<String>>>,
    pub reply: Arc<Mutex<Value>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server { fn drop(&mut self) { self.task.abort(); } }
impl Server {
    pub async fn start(body: Value) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(Mutex::new(vec![]));
        let reply = Arc::new(Mutex::new(body));
        let (c, r) = (calls.clone(), reply.clone());
        let task = tokio::spawn(async move { loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![];
            while !buffer.windows(4).any(|v| v == b"\r\n\r\n") {
                let mut bytes = [0; 4096]; let n = socket.read(&mut bytes).await.unwrap();
                assert!(n > 0 && buffer.len() < 32_768); buffer.extend_from_slice(&bytes[..n]);
            }
            let request = String::from_utf8(buffer).unwrap();
            let first = request.lines().next().unwrap().to_owned();
            assert!(first.starts_with("GET /quote?"), "loopback supports quote GET only");
            c.lock().unwrap().push(first);
            let body = r.lock().unwrap().to_string();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
            socket.write_all(response.as_bytes()).await.unwrap();
        }});
        Ok(Self { url, calls, reply, task })
    }
    pub fn assert_profile(&self, count: usize, version: &str) {
        let calls = self.calls.lock().unwrap(); assert_eq!(calls.len(), count);
        for request in calls.iter() {
            let url = reqwest::Url::parse(&format!("http://localhost{}", request.split_whitespace().nth(1).unwrap())).unwrap();
            let pairs = url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            assert_eq!(pairs["instructionVersion"], version);
            assert_eq!(pairs["swapMode"], "ExactIn"); assert_eq!(pairs["slippageBps"], "50");
            if version == "V1" { assert_eq!(pairs["onlyDirectRoutes"], "true"); assert_eq!(pairs["dexes"], "Raydium"); }
            else { assert!(!pairs.contains_key("onlyDirectRoutes") && !pairs.contains_key("dexes")); }
        }
    }
}
pub(super) fn bundle(all: &[SolanaInstruction]) -> Value {
    let index = all.len()-2;
    let encode = |ix: &SolanaInstruction| json!({"programId":bs58::encode(ix.program_id).into_string(),
        "accounts":ix.accounts.iter().map(|a| json!({"pubkey":bs58::encode(a.pubkey).into_string(),
            "isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(),"data":STANDARD.encode(&ix.data)});
    json!({"tokenLedgerInstruction":null,"computeBudgetInstructions":all[..2].iter().map(encode).collect::<Vec<_>>(),
        "setupInstructions":all[2..index].iter().map(encode).collect::<Vec<_>>(),"swapInstruction":encode(&all[index]),
        "cleanupInstruction":encode(&all[index+1]),"otherInstructions":[],"addressLookupTableAddresses":[],
        "simulationError":null,"blockhashWithMetadata":{"blockhash":vec![9;32],"lastValidBlockHeight":1,
        "fetchedAt":{"secs_since_epoch":1,"nanos_since_epoch":0}}})
}
