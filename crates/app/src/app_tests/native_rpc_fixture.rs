use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Barrier};
use tokio::task::{JoinHandle, JoinSet};

pub(super) const FEE_ID: &str = "native-funding-fee";
pub(super) const ACCOUNTS_ID: &str = "native-funding-accounts";

#[derive(Clone, Copy)]
pub(super) enum Framing {
    Length,
    Chunked,
    Close,
}

pub(super) struct Reply {
    pub body: Vec<u8>,
    pub status: u16,
    pub framing: Framing,
    pub delay: Duration,
    pub wait_for_cancel: bool,
    pub headers_before_cancel: bool,
    pub declared_length: Option<usize>,
    pub location: Option<String>,
}

impl Reply {
    pub fn json(value: Value) -> Self {
        Self {
            body: serde_json::to_vec(&value).unwrap(),
            status: 200,
            framing: Framing::Length,
            delay: Duration::ZERO,
            wait_for_cancel: false,
            headers_before_cancel: false,
            declared_length: None,
            location: None,
        }
    }
}

pub(super) fn account(lamports: u64, data: &[u8]) -> Value {
    json!({"lamports": lamports, "owner": bs58::encode([171;32]).into_string(),
        "executable": false, "data": [STANDARD.encode(data), "base64"]})
}

pub(super) fn success(request: &Value) -> Value {
    let fee = request["method"] == "getFeeForMessage";
    let value = if fee {
        json!(19_000)
    } else {
        Value::Array(
            request["params"][0]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, _)| match i {
                    0 => account(u64::MAX, &[0, 255, 17]),
                    1 => Value::Null,
                    _ => account(0, &[]),
                })
                .collect(),
        )
    };
    json!({"jsonrpc":"2.0", "id": if fee { FEE_ID } else { ACCOUNTS_ID },
        "result":{"context":{"slot":if fee {70} else {72}}, "value":value}})
}

#[derive(Debug, Clone)]
pub(super) struct Trace {
    pub request: Value,
    pub received: Instant,
    pub completed: Option<Instant>,
    pub cancellation_seen: bool,
}

pub(super) struct Fixture {
    pub endpoint: String,
    stop: oneshot::Sender<()>,
    server: JoinHandle<Result<Vec<Trace>>>,
}

impl Fixture {
    pub async fn start(
        both_in_flight: bool,
        reply: impl Fn(&Value) -> Reply + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::start_with_in_flight(if both_in_flight { 2 } else { 0 }, reply).await
    }

    pub async fn start_with_in_flight(
        in_flight: usize,
        reply: impl Fn(&Value) -> Reply + Send + Sync + 'static,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/private-endpoint-secret", listener.local_addr()?);
        let (stop, mut stopped) = oneshot::channel();
        let reply = Arc::new(reply);
        let traces = Arc::new(Mutex::new(Vec::new()));
        let barrier = (in_flight > 0).then(|| Arc::new(Barrier::new(in_flight)));
        let server = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let (reply, traces, barrier) = (reply.clone(), traces.clone(), barrier.clone());
                        tasks.spawn(async move {
                            tokio::time::timeout(Duration::from_secs(3), serve(stream, reply, traces, barrier))
                                .await.context("fixture handler timeout")?
                        });
                    }
                }
            }
            // Always await every server handler, including errors and cancellation checks.
            let mut failure = None;
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result.map_err(anyhow::Error::from).and_then(|r| r) {
                    failure.get_or_insert(error);
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            let values = traces.lock().unwrap().clone();
            Ok(values)
        });
        Ok(Self {
            endpoint,
            stop,
            server,
        })
    }

    pub async fn finish(self) -> Result<Vec<Trace>> {
        // Catch immediate extra requests/retries before closing the listener.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = self.stop.send(());
        self.server.await.context("fixture server panicked")?
    }
}

async fn serve(
    mut stream: TcpStream,
    reply: Arc<impl Fn(&Value) -> Reply + Send + Sync>,
    traces: Arc<Mutex<Vec<Trace>>>,
    barrier: Option<Arc<Barrier>>,
) -> Result<()> {
    let request = read_request(&mut stream).await?;
    let index = {
        let mut list = traces.lock().unwrap();
        let index = list.len();
        list.push(Trace {
            request: request.clone(),
            received: Instant::now(),
            completed: None,
            cancellation_seen: false,
        });
        index
    };
    if let Some(barrier) = barrier {
        barrier.wait().await;
    }
    let reply = reply(&request);
    if reply.wait_for_cancel {
        if reply.headers_before_cancel {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n{")
                .await?;
        }
        let mut byte = [0; 1];
        let closed = match stream.read(&mut byte).await {
            Ok(0) => true,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                true
            }
            _ => false,
        };
        ensure!(closed, "pending HTTP was not cancelled");
        let mut list = traces.lock().unwrap();
        list[index].cancellation_seen = true;
        list[index].completed = Some(Instant::now());
        return Ok(());
    }
    tokio::time::sleep(reply.delay).await;
    let framing = match reply.framing {
        Framing::Length => format!(
            "Content-Length: {}\r\n",
            reply.declared_length.unwrap_or(reply.body.len())
        ),
        Framing::Chunked => "Transfer-Encoding: chunked\r\n".to_owned(),
        Framing::Close => String::new(),
    };
    let location = reply
        .location
        .map(|url| format!("Location: {url}\r\n"))
        .unwrap_or_default();
    let headers = format!("HTTP/1.1 {} Result\r\nContent-Type: application/json\r\nConnection: close\r\n{framing}{location}\r\n", reply.status);
    // Oversized bodies can be rejected before the server finishes sending them.
    let sent = async {
        stream.write_all(headers.as_bytes()).await?;
        if matches!(reply.framing, Framing::Chunked) {
            for chunk in reply.body.chunks(4096) {
                stream
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await?;
                stream.write_all(chunk).await?;
                stream.write_all(b"\r\n").await?;
            }
            stream.write_all(b"0\r\n\r\n").await?;
        } else {
            stream.write_all(&reply.body).await?;
        }
        stream.shutdown().await
    }
    .await;
    if let Err(error) = sent {
        ensure!(
            matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ),
            "fixture write error: {error}"
        );
    }
    traces.lock().unwrap()[index].completed = Some(Instant::now());
    Ok(())
}

pub(super) async fn read_request(stream: &mut TcpStream) -> Result<Value> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let count = stream.read(&mut buffer).await?;
        ensure!(count > 0, "request ended early");
        bytes.extend_from_slice(&buffer[..count]);
        ensure!(bytes.len() <= 16_384, "fixture request too large");
        if let Some(header_end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = std::str::from_utf8(&bytes[..header_end])?;
            ensure!(header.starts_with("POST "), "fixture expected POST");
            let length: usize = header
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then_some(value.trim())
                })
                .context("missing request content length")?
                .parse()?;
            if bytes.len() >= header_end + 4 + length {
                return Ok(serde_json::from_slice(
                    &bytes[header_end + 4..header_end + 4 + length],
                )?);
            }
        }
    }
}
