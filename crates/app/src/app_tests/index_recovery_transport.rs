//! Bounded loopback-only full block producers; no provider credentials or RPC.
use super::index_recovery_fixture::{self as f, Saved};
use anyhow::{ensure, Context, Result};
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};
use yellowstone_grpc_proto::prelude::*;

#[derive(Clone)]
struct Live(Vec<SubscribeUpdateBlock>);
#[tonic::async_trait]
impl geyser_server::Geyser for Live {
    type SubscribeStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;
    type SubscribeDeshredStream = Pin<
        Box<dyn futures_util::Stream<Item = Result<SubscribeUpdateDeshred, tonic::Status>> + Send>,
    >;
    async fn subscribe(
        &self,
        r: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let req = r.into_inner().message().await?.unwrap();
        assert_eq!(req.from_slot, None);
        assert_eq!(req.commitment, Some(CommitmentLevel::Confirmed as i32));
        let messages = stream::iter(self.0.clone().into_iter().map(|b| {
            Ok(SubscribeUpdate {
                update_oneof: Some(subscribe_update::UpdateOneof::Block(b)),
                ..Default::default()
            })
        }))
        .then(|item| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            item
        })
        .chain(stream::pending());
        Ok(tonic::Response::new(Box::pin(messages)))
    }
    async fn subscribe_deshred(
        &self,
        _: tonic::Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeDeshredStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn subscribe_replay_info(
        &self,
        _: tonic::Request<SubscribeReplayInfoRequest>,
    ) -> Result<tonic::Response<SubscribeReplayInfoResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn ping(
        &self,
        _: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PongResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_latest_blockhash(
        &self,
        _: tonic::Request<GetLatestBlockhashRequest>,
    ) -> Result<tonic::Response<GetLatestBlockhashResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_block_height(
        &self,
        _: tonic::Request<GetBlockHeightRequest>,
    ) -> Result<tonic::Response<GetBlockHeightResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_slot(
        &self,
        _: tonic::Request<GetSlotRequest>,
    ) -> Result<tonic::Response<GetSlotResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn is_blockhash_valid(
        &self,
        _: tonic::Request<IsBlockhashValidRequest>,
    ) -> Result<tonic::Response<IsBlockhashValidResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn get_version(
        &self,
        _: tonic::Request<GetVersionRequest>,
    ) -> Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
}
pub(super) struct Servers {
    pub grpc: String,
    pub http: String,
    pub calls: Arc<Mutex<Vec<Value>>>,
    tasks: Vec<JoinHandle<()>>,
}
impl Drop for Servers {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
pub(super) async fn start(
    saved: &Saved,
    grpc: SubscribeUpdateBlock,
    tail: bool,
) -> Result<Servers> {
    let mut blocks = vec![grpc.clone()];
    if tail {
        blocks.push(f::empty_child(&saved.grpc));
    }
    serve(blocks, saved.raw_http.clone(), saved.grpc.clone()).await
}
pub(super) async fn restart(saved: &Saved) -> Result<Servers> {
    let first = f::empty_child(&f::empty_child(&saved.grpc));
    serve(
        vec![first.clone(), f::empty_child(&first)],
        saved.raw_http.clone(),
        saved.grpc.clone(),
    )
    .await
}
async fn serve(
    blocks: Vec<SubscribeUpdateBlock>,
    raw: Vec<u8>,
    captured: SubscribeUpdateBlock,
) -> Result<Servers> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let grpc = format!("http://{}", listener.local_addr()?);
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let live = blocks.clone();
    // Inclusive restart overlap must include the committed predecessor. HTTP
    // retains the original saved anchor while gRPC may be deliberately corrupt.
    let seed = f::predecessor(&captured);
    let mut overlap = f::predecessor(&seed);
    overlap.parent_blockhash = bs58::encode([240; 32]).into_string();
    let mut history = vec![
        overlap,
        f::predecessor(&captured),
        captured.clone(),
        f::empty_child(&captured),
    ];
    for block in &blocks {
        if !history.iter().any(|b| b.slot == block.slot) {
            history.push(block.clone());
        }
    }
    let g = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Live(live)))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let http = format!("http://{}", listener.local_addr()?);
    let calls = Arc::new(Mutex::new(vec![]));
    let seen = calls.clone();
    let h = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await.unwrap();
            seen.lock().unwrap().push(request.clone());
            let body = respond(&request, &history, &raw, &captured).unwrap();
            let _ = socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await;
            let _ = socket.write_all(&body).await;
        }
    });
    Ok(Servers {
        grpc,
        http,
        calls,
        tasks: vec![g, h],
    })
}
async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<Value> {
    let mut bytes = vec![];
    loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await?;
        ensure!(n > 0 && bytes.len() + n < 65536, "fixture_request_bound");
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(at) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            let header = std::str::from_utf8(&bytes[..at])?;
            let size = header
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse::<usize>().unwrap())
                })
                .context("fixture_content_length")?;
            if bytes.len() >= at + 4 + size {
                return Ok(serde_json::from_slice(&bytes[at + 4..at + 4 + size])?);
            }
        }
    }
}
fn respond(
    request: &Value,
    blocks: &[SubscribeUpdateBlock],
    raw: &[u8],
    captured: &SubscribeUpdateBlock,
) -> Result<Vec<u8>> {
    let params = &request["params"];
    let range = request["method"] == "getBlocks";
    ensure!(
        params[if range { 2 } else { 1 }]["commitment"] == "confirmed",
        "fixture_confirmed"
    );
    let id = request["id"].as_u64().context("fixture_request_id")?;
    if !range {
        ensure!(
            request["method"] == "getBlock"
                && params[1]["encoding"] == "json"
                && params[1]["transactionDetails"] == "full"
                && params[1]["rewards"] == true
                && params[1]["maxSupportedTransactionVersion"] == 1,
            "fixture_full_options"
        );
        if params[0] == captured.slot {
            // Only the RPC envelope id changes for request correlation. Exact
            // captured result bytes and all numeric lexemes stay unmodified.
            return correlate_id(raw, id);
        }
    }
    let result = if range {
        let from = params[0].as_u64().unwrap();
        let to = params[1].as_u64().unwrap();
        json!(blocks
            .iter()
            .filter(|b| b.slot >= from && b.slot <= to)
            .map(|b| b.slot)
            .collect::<Vec<_>>())
    } else {
        let block = blocks
            .iter()
            .find(|b| Some(b.slot) == params[0].as_u64())
            .context("fixture_slot")?;
        // Model tails are empty; no saved transaction or metadata is converted.
        json!({"blockhash":block.blockhash,"previousBlockhash":block.parent_blockhash,"parentSlot":block.parent_slot,
            "blockTime":block.block_time.as_ref().map(|t|t.timestamp),"blockHeight":block.block_height.as_ref().map(|h|h.block_height),
            "transactions":[],"rewards":[],"numRewardPartitions":null})
    };
    Ok(serde_json::to_vec(
        &json!({"jsonrpc":"2.0","id":id,"result":result}),
    )?)
}
fn correlate_id(raw: &[u8], id: u64) -> Result<Vec<u8>> {
    let key = raw
        .windows(4)
        .position(|p| p == b"\"id\"")
        .context("captured_envelope_id")?;
    let colon = raw[key + 4..].iter().position(|b| *b == b':').unwrap() + key + 4;
    let start = raw[colon + 1..]
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap()
        + colon
        + 1;
    let end = raw[start..]
        .iter()
        .position(|b| !b.is_ascii_digit())
        .unwrap()
        + start;
    let mut output = raw[..start].to_vec();
    output.extend_from_slice(id.to_string().as_bytes());
    output.extend_from_slice(&raw[end..]);
    Ok(output)
}
