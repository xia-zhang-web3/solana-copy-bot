//! Bounded localhost provider and explicitly synthetic client simulation replies.
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;

pub(super) const QUOTE: &str = include_str!("generic_buy_fixtures/quote.json");
pub(super) const INSTRUCTIONS: &str = include_str!("generic_buy_fixtures/instructions.json");
pub(super) const OLD_SWAP: &str = include_str!("generic_buy_fixtures/old-swap.json");
pub(super) const REQUEST: &str = include_str!("generic_buy_fixtures/request.json");

#[derive(Clone)]
pub(super) struct Replies {
    pub instructions: String,
    pub instructions_status: u16,
    pub swap: String,
    pub simulation: String,
    pub simulation_status: u16,
    pub simulation_delay_ms: u64,
}
impl Default for Replies {
    fn default() -> Self {
        Self {
            instructions: INSTRUCTIONS.into(),
            instructions_status: 200,
            swap: OLD_SWAP.into(),
            simulation: json!({"jsonrpc":"2.0","id":"execution-swap-transaction-simulate",
                "result":{"context":{"slot":446128191},"value":{"err":null,"logs":[]}}})
            .to_string(),
            simulation_status: 200,
            simulation_delay_ms: 0,
        }
    }
}
#[derive(Clone, Debug)]
pub(super) struct Call {
    pub path: String,
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
                history.lock().unwrap().push(Call {
                    path: path.clone(),
                    raw,
                    body: body.clone(),
                });
                let (status, response) = if path.ends_with("swap-instructions") {
                    (replies.instructions_status, replies.instructions.as_str())
                } else if path.ends_with("/swap") {
                    (200, replies.swap.as_str())
                } else if path == "/rpc" && body["method"] == "simulateTransaction" {
                    std::thread::sleep(Duration::from_millis(replies.simulation_delay_ms));
                    (replies.simulation_status, replies.simulation.as_str())
                } else {
                    // Existing direct selection is allowed to run, but no real account RPC.
                    (403, "local direct account-read refusal")
                };
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
