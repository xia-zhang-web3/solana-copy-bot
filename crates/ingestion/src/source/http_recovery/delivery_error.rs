//! Closed, typed request facts. URLs, headers, provider text and bodies are absent.
use std::{
    error::Error,
    fmt,
    io::ErrorKind,
    time::{Duration, Instant},
};

pub(super) struct ClientFailure {
    pub method: &'static str,
    pub id: u64,
    pub slot: u64,
    pub stage: &'static str,
    pub attempt: u8,
    pub elapsed_ms: u64,
    pub deadline_ms: u64,
    pub timeout: bool,
    pub connect: bool,
    pub body: bool,
    pub io_kind: Option<ErrorKind>,
    error: reqwest::Error,
}
fn millis(value: Duration) -> u64 {
    value.as_millis().min(u128::from(u64::MAX)) as u64
}
impl ClientFailure {
    pub fn new(
        error: reqwest::Error,
        method: &'static str,
        id: u64,
        slot: u64,
        stage: &'static str,
        attempt: u8,
        started: Instant,
        deadline: Duration,
    ) -> Self {
        let mut cause = error.source();
        let mut io_kind = None;
        while let Some(current) = cause {
            if let Some(io) = current.downcast_ref::<std::io::Error>() {
                io_kind = Some(io.kind());
                break;
            }
            cause = current.source();
        }
        Self {
            method,
            id,
            slot,
            stage,
            attempt,
            elapsed_ms: millis(started.elapsed()),
            deadline_ms: millis(deadline),
            timeout: error.is_timeout(),
            connect: error.is_connect(),
            body: error.is_body(),
            io_kind,
            error: error.without_url(),
        }
    }
    pub fn retryable(&self) -> bool {
        // The configured endpoint is HTTP loopback, so TLS/certificate failures
        // cannot become connect retries. Builder/status/decode failures never do.
        if self.error.is_builder() || self.error.is_status() {
            return false;
        }
        self.timeout
            || matches!(
                self.io_kind,
                Some(
                    ErrorKind::TimedOut
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionRefused
                        | ErrorKind::BrokenPipe
                        | ErrorKind::UnexpectedEof
                )
            )
    }
}
impl fmt::Display for ClientFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http_recovery_client_error method={} id={} slot={} stage={} attempt={} elapsed_ms={} deadline_ms={} timeout={} connect={} body={} io_kind={:?}",
            self.method,self.id,self.slot,self.stage,self.attempt,self.elapsed_ms,self.deadline_ms,
            self.timeout,self.connect,self.body,self.io_kind)
    }
}
impl fmt::Debug for ClientFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl Error for ClientFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Debug)]
pub(super) struct BrokerFailure {
    pub method: String,
    pub kind: String,
    pub stage: String,
    pub reason: String,
    pub cause: String,
    pub broker_status: u16,
    pub reservation: Option<u64>,
    pub upstream_status: Option<u64>,
    pub verify: Option<u64>,
    pub request_id: Option<u64>,
    pub slot: Option<u64>,
}
impl BrokerFailure {
    pub fn retryable(&self) -> bool {
        self.kind == "failed"
            && self.reservation.is_some()
            && matches!(
                self.stage.as_str(),
                "outbound"
                    | "http_response"
                    | "upstream_headers"
                    | "upstream_body"
                    | "transport"
                    | "unix_send"
                    | "front_headers"
                    | "front_body"
            )
            && matches!(
                (self.reason.as_str(), self.cause.as_str()),
                ("timeout", "TimeoutError")
                    | ("deadline_exhausted", "DeadlineExceededTimeout")
                    | (
                        "connection_reset",
                        "ConnectionResetError" | "ConnectionAbortedError" | "RemoteDisconnected"
                    )
                    | ("http_protocol", "RemoteDisconnected" | "IncompleteRead")
                    | (
                        "transport_error",
                        "EOFError" | "BrokenPipeError" | "ConnectionResetError"
                    )
            )
    }
}
fn number(value: Option<u64>) -> String {
    value
        .map(|n| n.to_string())
        .unwrap_or_else(|| "unknown".into())
}
impl fmt::Display for BrokerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,"http_recovery_broker_error method={} kind={} stage={} reason={} cause_type={} broker_http_status={} upstream_http_status={} reservation_id={} verify_code={} request_id={} slot={}",
            self.method,self.kind,self.stage,self.reason,self.cause,self.broker_status,
            number(self.upstream_status),number(self.reservation),number(self.verify),number(self.request_id),number(self.slot))
    }
}
impl Error for BrokerFailure {}

pub(super) fn retryable(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<ClientFailure>()
        .is_some_and(ClientFailure::retryable)
        || error
            .downcast_ref::<BrokerFailure>()
            .is_some_and(BrokerFailure::retryable)
}
pub(super) fn report(
    error: &anyhow::Error,
    method: &'static str,
    id: u64,
    slot: u64,
    attempt: u8,
    started: Instant,
    timeout: Duration,
    retry: bool,
) {
    // Typed facts are reported before any top-level anyhow stringification.
    if let Some(client) = error.downcast_ref::<ClientFailure>() {
        tracing::warn!(method,request_id=id,slot,attempt,stage=client.stage,
            elapsed_ms=client.elapsed_ms,deadline_ms=client.deadline_ms,
            timeout=client.timeout,connect=client.connect,body=client.body,
            io_kind=?client.io_kind,retry,"confirmed HTTP client attempt failed");
    } else if let Some(broker) = error.downcast_ref::<BrokerFailure>() {
        tracing::warn!(
            method,
            request_id = id,
            slot,
            attempt,
            stage = broker.stage,
            elapsed_ms = millis(started.elapsed()),
            deadline_ms = millis(timeout),
            reservation_id = broker.reservation,
            reason = broker.reason,
            cause_type = broker.cause,
            broker_http_status = broker.broker_status,
            upstream_http_status = broker.upstream_status,
            retry,
            "confirmed HTTP broker attempt failed"
        );
    }
}
