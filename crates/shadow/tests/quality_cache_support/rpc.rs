use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

// Two bounded HTTP replies exercise the actual holders + age RPC path.
// Explicit finish checks the thread before any caller assertions can unwind.
pub struct Rpc {
    pub url: String,
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<Vec<String>>>>,
}
impl Rpc {
    pub fn start(healthy: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}", listener.local_addr()?);
        let cancel = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&cancel);
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(4);
            let mut methods = Vec::new();
            while methods.len() < 2 && !stop.load(Ordering::SeqCst) {
                if Instant::now() >= deadline {
                    bail!("bounded RPC accept deadline");
                }
                let mut stream = match listener.accept() {
                    Ok((s, _)) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };
                stream.set_nonblocking(false)?;
                stream.set_read_timeout(Some(Duration::from_millis(500)))?;
                stream.set_write_timeout(Some(Duration::from_millis(500)))?;
                let mut bytes = Vec::new();
                let mut part = [0u8; 1024];
                let request: Value = loop {
                    anyhow::ensure!(Instant::now() < deadline, "bounded RPC read deadline");
                    let n = stream.read(&mut part)?;
                    if n == 0 {
                        bail!("truncated RPC request");
                    }
                    bytes.extend_from_slice(&part[..n]);
                    if bytes.len() > 8192 {
                        bail!("RPC request too large");
                    }
                    if let Some(end) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
                        let len: usize = headers
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .context("request length")?
                            .trim()
                            .parse()?;
                        if bytes.len() >= end + 4 + len {
                            break serde_json::from_slice(&bytes[end + 4..end + 4 + len])?;
                        }
                    }
                };
                let method = request["method"]
                    .as_str()
                    .context("RPC method")?
                    .to_string();
                let body = if !healthy {
                    json!({"error":{"code":-32000,"message":"bounded failure"}})
                } else if method == "getProgramAccounts" {
                    json!({"result":(0..5).map(|i|json!({"account":{"data":{"parsed":{"info":{
                            "owner":format!("rpc-holder-{i}"),"tokenAmount":{"amount":"1"}}}}}})).collect::<Vec<_>>()})
                } else if method == "getSignaturesForAddress" {
                    json!({"result":[{"signature":"age","blockTime":1}]})
                } else {
                    bail!("unexpected method {method}");
                };
                let status = if healthy {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                };
                let body = body.to_string();
                write!(stream,"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())?;
                methods.push(method);
            }
            Ok(methods)
        });
        Ok(Self {
            url,
            cancel,
            handle: Some(handle),
        })
    }
    pub fn finish(mut self) -> Result<Vec<String>> {
        self.cancel.store(true, Ordering::SeqCst);
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|_| anyhow::anyhow!("RPC thread panic"))?
    }
}
impl Drop for Rpc {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
