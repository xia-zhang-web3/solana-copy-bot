//! Loopback servers. Only JSON-RPC ids are rebound in modeled replay mode.
use super::super::{Fixture, Live, Servers};
use futures_util::stream;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use yellowstone_grpc_proto::prelude::*;

pub(super) async fn start(
    bodies: BTreeMap<u64, Vec<u8>>,
    exact_list: Option<Vec<u8>>,
    messages: Vec<SubscribeUpdateBlock>,
) -> Servers {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = calls.clone();
    let h = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
            let (end, len) = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header = std::str::from_utf8(&bytes[..end]).unwrap();
                    let len = header
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|s| s.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, len);
                }
                assert!(bytes.len() < 1 << 20);
            };
            while bytes.len() < end + len {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[end..end + len]).unwrap();
            seen.lock().unwrap().push(request.clone());
            let body = if let Some(list) = &exact_list {
                let exact = if request["method"] == "getBlocks" {
                    list.clone()
                } else {
                    bodies[&request["params"][0].as_u64().unwrap()].clone()
                };
                assert_eq!(
                    serde_json::from_slice::<Value>(&exact).unwrap()["id"],
                    request["id"]
                );
                exact
            } else {
                let result = if request["method"] == "getBlocks" {
                    let from = request["params"][0].as_u64().unwrap();
                    let to = request["params"][1].as_u64().unwrap();
                    json!(bodies
                        .keys()
                        .filter(|s| **s >= from && **s <= to)
                        .copied()
                        .collect::<Vec<_>>())
                } else {
                    assert_eq!(request["method"], "getBlock");
                    super::corpus::result(&bodies[&request["params"][0].as_u64().unwrap()])
                };
                serde_json::to_vec(&json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
                    .unwrap()
            };
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let g = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Live(Fixture {
                messages: messages
                    .into_iter()
                    .map(|b| SubscribeUpdate {
                        update_oneof: Some(subscribe_update::UpdateOneof::Block(b)),
                        ..Default::default()
                    })
                    .collect(),
                terminal: None,
                reject_subscribe: None,
            })))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Servers {
        http,
        grpc,
        tasks: vec![h, g],
        calls,
    }
}
