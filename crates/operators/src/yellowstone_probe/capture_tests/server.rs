use futures_util::{Stream, StreamExt};
use std::{
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tonic::{Request, Response, Status, Streaming};
use yellowstone_grpc_proto::prelude::*;

pub(super) struct Server {
    pub messages: Vec<SubscribeUpdate>,
    pub mode: &'static str,
    pub output: PathBuf,
    pub requests: Arc<Mutex<Vec<SubscribeRequest>>>,
    pub subscriptions: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl geyser_server::Geyser for Server {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;
    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        let mut request = request.into_inner();
        let initial = request.message().await?.unwrap();
        self.requests.lock().unwrap().push(initial);
        if self.mode == "open-error" {
            return Err(Status::permission_denied("synthetic-secret private-query"));
        }
        if self.mode == "open-silent" {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        if self.mode == "output-failure" {
            std::fs::create_dir(self.output.join("000001.pb")).unwrap();
        }
        if self.mode == "output-existing-file" {
            std::fs::write(self.output.join("000001.pb"), b"prior envelope").unwrap();
        }
        if self.mode == "manifest-failure" {
            std::fs::write(self.output.join("manifest.json"), b"prior evidence").unwrap();
        }
        let messages = self.messages.clone();
        let mode = self.mode;
        let stream =
            futures_util::stream::iter(messages.into_iter().map(Ok)).then(move |item| async move {
                if mode != "window-fast" {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                item
            });
        let tail = futures_util::stream::once(async move {
            if mode == "silent" {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            if let Some(status) = super::terminal_cli_tests::server_status(mode) {
                Some(Err(status))
            } else if mode == "stream-error" {
                Some(Err(Status::internal("synthetic-secret private-query")))
            } else {
                None
            }
        })
        .filter_map(|v| async move { v });
        Ok(Response::new(Box::pin(stream.chain(tail))))
    }
    async fn subscribe_replay_info(
        &self,
        _: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_latest_blockhash(
        &self,
        _: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_block_height(
        &self,
        _: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_slot(
        &self,
        _: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn is_blockhash_valid(
        &self,
        _: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_version(
        &self,
        _: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}
