use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::Duration;

pub const CLASSIC: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

pub fn key(letter: char) -> String {
    letter.to_string().repeat(43)
}

pub fn account(
    wallet: &str,
    mint: &str,
    pubkey: &str,
    program: &str,
    raw: &str,
    decimals: u8,
) -> Value {
    json!({"pubkey": pubkey, "account": {"owner": program, "executable": false, "lamports": 2039280,
        "data": {"program": if program == CLASSIC { "spl-token" } else { "spl-token-2022" },
        "parsed": {"type": "account", "info": {"owner": wallet, "mint": mint, "state": "initialized",
        "tokenAmount": {"amount": raw, "decimals": decimals, "uiAmount": null, "uiAmountString": "display-only"}}}}}})
}

pub fn result(value: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 123}, "value": value}})
}

pub struct Reply {
    pub body: String,
    pub delay: Duration,
    pub status: u16,
}

impl From<Value> for Reply {
    fn from(value: Value) -> Self {
        Self {
            body: value.to_string(),
            delay: Duration::ZERO,
            status: 200,
        }
    }
}

pub struct RpcStub {
    pub url: String,
    stopped: Arc<AtomicBool>,
    captured: Arc<Mutex<Vec<Value>>>,
    thread: Option<JoinHandle<()>>,
}

impl RpcStub {
    pub fn start(mut reply: impl FnMut(&Value) -> Reply + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let stop = stopped.clone();
        let requests = captured.clone();
        let thread = std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("accept loopback RPC: {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut bytes = Vec::new();
                let request = loop {
                    let mut buffer = [0; 4096];
                    let n = stream.read(&mut buffer).expect("read loopback request");
                    assert!(n > 0, "incomplete request");
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                        let len: usize = headers
                            .lines()
                            .find_map(|line| {
                                let (k, v) = line.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse().unwrap())
                            })
                            .expect("content length");
                        if bytes.len() >= end + 4 + len {
                            break serde_json::from_slice::<Value>(&bytes[end + 4..end + 4 + len])
                                .unwrap();
                        }
                    }
                };
                requests.lock().unwrap().push(request.clone());
                let response = reply(&request);
                std::thread::sleep(response.delay);
                let data = format!("HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.status, response.body.len(), response.body);
                if response.delay.is_zero() {
                    stream
                        .write_all(data.as_bytes())
                        .expect("write loopback response");
                } else {
                    let _ = stream.write_all(data.as_bytes());
                } // Client intentionally timed out.
            }
        });
        Self {
            url,
            stopped,
            captured,
            thread: Some(thread),
        }
    }

    pub fn finish(mut self) -> Vec<Value> {
        self.stopped.store(true, Ordering::SeqCst);
        self.thread
            .take()
            .unwrap()
            .join()
            .expect("RPC background thread must succeed");
        self.captured.lock().unwrap().clone()
    }
}

impl Drop for RpcStub {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            if !std::thread::panicking() {
                result.expect("RPC background thread");
            }
        }
    }
}

pub fn assert_requests(calls: &[Value], wallet: &str, expected: usize) {
    assert_eq!(
        calls.len(),
        expected,
        "one balance + both programs; no retries"
    );
    for (i, request) in calls.iter().enumerate() {
        assert_eq!(request["params"][0], wallet);
        assert_eq!(request["jsonrpc"], "2.0");
        if i == 0 {
            assert_eq!(request["method"], "getBalance");
            assert_eq!(request["params"][1]["commitment"], "confirmed");
        } else {
            assert_eq!(request["method"], "getTokenAccountsByOwner");
            assert_eq!(
                request["params"][1]["programId"],
                if i == 1 { CLASSIC } else { TOKEN_2022 }
            );
            assert_eq!(
                request["params"][2],
                json!({"encoding":"jsonParsed", "commitment":"confirmed"})
            );
        }
    }
}
