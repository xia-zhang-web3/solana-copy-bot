//! Strict loopback: recorded R4 legacy response is bound to its exact payload only.
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;

use super::generic_sell_fixture::*;
#[derive(Clone)]
pub(super) struct Replies {
    pub instructions: String,
    pub instructions_status: u16,
    pub swap: String,
    pub simulation: String,
    pub simulation_status: u16,
    pub simulation_delay_ms: u64,
    pub expected_body: String,
}
impl Default for Replies {
    fn default() -> Self {
        Self {
            instructions: INSTRUCTIONS.into(),
            instructions_status: 200,
            swap: OLD_SWAP.into(),
            simulation: SIMULATION.into(),
            simulation_status: 200,
            simulation_delay_ms: 0,
            expected_body: REQUEST.into(),
        }
    }
}
#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct Call {
    pub path: String,
    pub kind: String,
    pub raw: String,
    pub body: Value,
}
pub(super) struct Server {
    pub base: String,
    calls: Arc<Mutex<Vec<Call>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}
impl Server {
    pub fn start(replies: Replies) -> Self {
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let base = format!("http://{}", socket.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (history, done) = (calls.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            while !done.load(Ordering::SeqCst) {
                let (mut stream, _) = match socket.accept() {
                    Ok(s) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("loopback accept: {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = Vec::new();
                let split = loop {
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0 && bytes.len() < 1_000_000);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(i) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let header = String::from_utf8(bytes[..split].to_vec()).unwrap();
                let len: usize = header
                    .lines()
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().unwrap())
                    })
                    .unwrap_or(0);
                while bytes.len() < split + len {
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let raw = String::from_utf8(bytes[split..split + len].to_vec()).unwrap();
                let body: Value = serde_json::from_str(&raw).unwrap();
                let path = header.split_whitespace().nth(1).unwrap().to_string();
                let method = header.split_whitespace().next().unwrap_or("");
                let expected_rpc: Value = serde_json::from_str(SIMULATION_REQUEST).unwrap();
                let old = old_v0();
                let mut old_rpc = expected_rpc.clone();
                old_rpc["params"][0] = json!(old);
                let (kind, status, response) = if method != "POST" {
                    ("unexpected", 599, "UNEXPECTED_METHOD")
                } else if path == "/rpc"
                    && raw == DIRECT_REQUEST
                    && !history
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|c: &Call| c.kind == "synthetic_direct_refusal")
                {
                    (
                        "synthetic_direct_refusal",
                        403,
                        "BATCH113_OFFLINE_SYNTHETIC_DIRECT_REFUSAL",
                    )
                } else if path == "/swap/v1/swap-instructions" && raw == replies.expected_body {
                    (
                        "instructions",
                        replies.instructions_status,
                        replies.instructions.as_str(),
                    )
                } else if path == "/swap/v1/swap" && raw == replies.expected_body {
                    ("old_swap", 200, replies.swap.as_str())
                } else if path == "/rpc" && body == expected_rpc {
                    (
                        "recorded_legacy_simulation",
                        replies.simulation_status,
                        replies.simulation.as_str(),
                    )
                } else if path == "/rpc" && body == old_rpc {
                    // No equivalence: never give the legacy response to old v0 bytes.
                    (
                        "old_v0_local_refusal",
                        403,
                        "BATCH116_OLD_V0_NO_RECORDED_SIMULATION",
                    )
                } else {
                    ("unexpected", 599, "UNEXPECTED_LOCAL_REQUEST")
                };
                history.lock().unwrap().push(Call {
                    path: path.clone(),
                    kind: kind.into(),
                    raw,
                    body: body.clone(),
                });
                if kind == "recorded_legacy_simulation" {
                    std::thread::sleep(Duration::from_millis(replies.simulation_delay_ms));
                }
                let response = format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len());
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self {
            base,
            calls,
            stop,
            thread: Some(thread),
        }
    }
    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
    pub fn count(&self, suffix: &str) -> usize {
        self.calls()
            .iter()
            .filter(|c| c.path.ends_with(suffix))
            .count()
    }
    pub fn count_kind(&self, kind: &str) -> usize {
        self.calls().iter().filter(|c| c.kind == kind).count()
    }
    pub fn simulations(&self) -> Vec<String> {
        self.calls()
            .iter()
            .filter(|c| c.body["method"] == "simulateTransaction")
            .map(|c| c.body["params"][0].as_str().unwrap().to_owned())
            .collect()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.thread.take().unwrap().join().unwrap();
    }
}
