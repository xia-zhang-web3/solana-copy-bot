//! A real local tonic producer continues emitting while HTTP history is delayed.
use super::super::super::durable_transport_fixture::Fixture;
use futures_util::{stream, StreamExt};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::net::TcpListener;
use yellowstone_grpc_proto::prelude::*;
#[derive(Clone)]
struct Live {
    fixture: Fixture,
    sent: Arc<AtomicU64>,
}
#[tonic::async_trait]
impl geyser_server::Geyser for Live {
    type SubscribeStream = <Fixture as geyser_server::Geyser>::SubscribeStream;
    type SubscribeDeshredStream = <Fixture as geyser_server::Geyser>::SubscribeDeshredStream;
    async fn subscribe(
        &self,
        request: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let r = request.into_inner().message().await?.unwrap();
        assert_eq!(r.from_slot, None);
        assert_eq!(r.commitment, Some(CommitmentLevel::Confirmed as i32));
        let sent = self.sent.clone();
        let messages = self.fixture.messages.clone();
        let produced = stream::iter(messages.into_iter().enumerate())
            .then(move |(index, item)| {
                let sent = sent.clone();
                async move {
                    if index > 0 {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    sent.fetch_add(1, Ordering::Relaxed);
                    Ok(item)
                }
            })
            .chain(stream::pending());
        Ok(tonic::Response::new(Box::pin(produced)))
    }
    async fn subscribe_deshred(
        &self,
        r: tonic::Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeDeshredStream>, tonic::Status> {
        geyser_server::Geyser::subscribe_deshred(&self.fixture, r).await
    }
    async fn subscribe_replay_info(
        &self,
        r: tonic::Request<SubscribeReplayInfoRequest>,
    ) -> Result<tonic::Response<SubscribeReplayInfoResponse>, tonic::Status> {
        geyser_server::Geyser::subscribe_replay_info(&self.fixture, r).await
    }
    async fn ping(
        &self,
        r: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PongResponse>, tonic::Status> {
        geyser_server::Geyser::ping(&self.fixture, r).await
    }
    async fn get_latest_blockhash(
        &self,
        r: tonic::Request<GetLatestBlockhashRequest>,
    ) -> Result<tonic::Response<GetLatestBlockhashResponse>, tonic::Status> {
        geyser_server::Geyser::get_latest_blockhash(&self.fixture, r).await
    }
    async fn get_block_height(
        &self,
        r: tonic::Request<GetBlockHeightRequest>,
    ) -> Result<tonic::Response<GetBlockHeightResponse>, tonic::Status> {
        geyser_server::Geyser::get_block_height(&self.fixture, r).await
    }
    async fn get_slot(
        &self,
        r: tonic::Request<GetSlotRequest>,
    ) -> Result<tonic::Response<GetSlotResponse>, tonic::Status> {
        geyser_server::Geyser::get_slot(&self.fixture, r).await
    }
    async fn is_blockhash_valid(
        &self,
        r: tonic::Request<IsBlockhashValidRequest>,
    ) -> Result<tonic::Response<IsBlockhashValidResponse>, tonic::Status> {
        geyser_server::Geyser::is_blockhash_valid(&self.fixture, r).await
    }
    async fn get_version(
        &self,
        r: tonic::Request<GetVersionRequest>,
    ) -> Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        geyser_server::Geyser::get_version(&self.fixture, r).await
    }
}
pub(super) struct Server {
    pub url: String,
    pub sent: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) async fn start(messages: Vec<SubscribeUpdateBlock>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let sent = Arc::new(AtomicU64::new(0));
    let observer = sent.clone();
    let fixture = Fixture {
        messages: messages
            .into_iter()
            .map(|b| SubscribeUpdate {
                update_oneof: Some(subscribe_update::UpdateOneof::Block(b)),
                ..Default::default()
            })
            .collect(),
        terminal: None,
        reject_subscribe: None,
    };
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(Live {
                fixture,
                sent: observer,
            }))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Server { url, sent, task }
}
