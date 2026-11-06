use super::{
    delivery_output::DeliveryFiles,
    delivery_report::{DeliveryReport, ProviderCreatedAt, Stage},
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::future::Future;
use tokio::time::{self, Instant, Interval};
use tonic::Status;
use yellowstone_grpc_proto::{
    prelude::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestPing, SubscribeUpdate,
    },
    prost::Message,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WaitFailure {
    Deadline,
    Artifact,
    ExternalStop,
}

pub(crate) async fn wait_stage<F: Future>(
    future: F,
    deadline: Instant,
    ticker: &mut Interval,
    report: &mut DeliveryReport,
    files: &DeliveryFiles,
    started: Instant,
) -> Result<F::Output, WaitFailure> {
    tokio::pin!(future);
    loop {
        if report.limits.stop_file.as_ref().is_some_and(|p| p.exists()) {
            return Err(WaitFailure::ExternalStop);
        }
        tokio::select! {
            biased;
            _ = time::sleep_until(deadline) => return Err(WaitFailure::Deadline),
            _ = ticker.tick() => {
                report.refresh(started);
                if report.limits.stop_file.as_ref().is_some_and(|p|p.exists()) {
                    return Err(WaitFailure::ExternalStop);
                }
                if report.elapsed_ms.saturating_sub(report.last_progress_elapsed_ms)>=report.limits.progress_ms && files.progress(report).is_err() {
                    report.artifact_errors.push("progress_write_failed");
                    report.fail(Stage::Artifact,"progress_write_failed",None);
                    return Err(WaitFailure::Artifact);
                }
            }
            result = &mut future => {
                if report.limits.stop_file.as_ref().is_some_and(|p|p.exists()) {
                    return Err(WaitFailure::ExternalStop);
                }
                return Ok(result);
            },
        }
    }
}

pub(crate) async fn pump<S, K>(
    stream: &mut S,
    sender: &mut K,
    deadline: Instant,
    ticker: &mut Interval,
    report: &mut DeliveryReport,
    files: &DeliveryFiles,
    started: Instant,
) -> &'static str
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
    K: Sink<SubscribeRequest> + Unpin,
{
    loop {
        if report.counts.decoded_updates >= report.limits.max_messages {
            report.possible_update_loss = true;
            return "message_count_limit";
        }
        let next = wait_stage(stream.next(), deadline, ticker, report, files, started).await;
        let message = match next {
            Err(WaitFailure::Deadline) => {
                report.full_window_observed = true;
                return "duration_limit";
            }
            Err(WaitFailure::ExternalStop) => return "external_stop",
            Err(WaitFailure::Artifact) => {
                report.possible_update_loss = true;
                return "artifact_failure";
            }
            Ok(None) => {
                report.possible_update_loss = true;
                return "stream_closed";
            }
            Ok(Some(Err(error))) => {
                report.counts.stream_errors += 1;
                let kind = if matches!(
                    error.code(),
                    tonic::Code::OutOfRange | tonic::Code::ResourceExhausted
                ) {
                    "transport_decode_limit_or_resource_error"
                } else {
                    "grpc_status_error"
                };
                report.fail(Stage::Stream, kind, Some(error.code() as i32));
                report.possible_update_loss = true;
                return "stream_error";
            }
            Ok(Some(Ok(message))) => message,
        };
        report.refresh(started);
        report.counts.decoded_updates += 1;
        report.last_message_elapsed_ms = Some(report.elapsed_ms);
        let bytes = message.encoded_len() as u64;
        report.counts.decoded_encoded_bytes =
            report.counts.decoded_encoded_bytes.saturating_add(bytes);
        match message.update_oneof.as_ref() {
            Some(UpdateOneof::Transaction(tx)) => {
                report.counts.transactions += 1;
                report.last_transaction_elapsed_ms = Some(report.elapsed_ms);
                report.last_transaction_slot = Some(tx.slot);
                report.last_transaction_provider_created_at =
                    message.created_at.as_ref().map(|v| ProviderCreatedAt {
                        seconds: v.seconds,
                        nanos: v.nanos,
                    });
                if report.counts.transactions == 1 {
                    report.first_transaction_elapsed_ms = Some(report.elapsed_ms);
                    report.first_transaction_slot = Some(tx.slot);
                    report.first_transaction_provider_created_at =
                        report.last_transaction_provider_created_at.clone();
                }
            }
            Some(UpdateOneof::Ping(_)) => report.counts.pings += 1,
            Some(UpdateOneof::Pong(_)) => report.counts.pongs += 1,
            _ => report.counts.other += 1,
        }
        if bytes > report.limits.max_message_bytes as u64 {
            report.possible_update_loss = true;
            return "decoded_message_size_limit";
        }
        if report.counts.decoded_encoded_bytes > report.limits.max_decoded_bytes {
            report.possible_update_loss = true;
            return "decoded_byte_limit";
        }
        if matches!(
            message.update_oneof.as_ref(),
            Some(UpdateOneof::Transaction(_))
        ) && files.sample(&message, report).is_err()
        {
            report.artifact_errors.push("sample_write_failed");
            report.fail(Stage::Artifact, "sample_write_failed", None);
            report.possible_update_loss = true;
            return "artifact_failure";
        }
        if matches!(message.update_oneof.as_ref(), Some(UpdateOneof::Ping(_))) {
            let request = SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: 1 }),
                ..Default::default()
            };
            match wait_stage(
                sender.send(request),
                deadline,
                ticker,
                report,
                files,
                started,
            )
            .await
            {
                Ok(Ok(())) => report.counts.ping_replies += 1,
                Ok(Err(_)) => {
                    report.fail(Stage::PingReply, "send_failed", None);
                    return "ping_reply_failed";
                }
                Err(WaitFailure::Deadline) => {
                    report.fail(Stage::PingReply, "send_deadline", None);
                    return "ping_reply_deadline";
                }
                Err(WaitFailure::Artifact) => return "artifact_failure",
                Err(WaitFailure::ExternalStop) => return "external_stop",
            }
        }
    }
}
