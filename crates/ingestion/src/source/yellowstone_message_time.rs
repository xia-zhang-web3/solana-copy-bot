use chrono::{DateTime, Utc};
use yellowstone_grpc_proto::prost_types::Timestamp;

/// Source is exclusively SubscribeUpdate.created_at: message metadata.
/// Available does not imply known chain/event time. No transaction-to-block
/// binding is supplied by this API, and no alternate clock is accepted here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum YellowstoneMessageTime {
    AvailableCreatedAt(DateTime<Utc>),
    UnresolvedCreatedAt(CreatedAtUnavailable),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CreatedAtUnavailable {
    Missing,
    InvalidNanos(i32),
    OutOfRangeSeconds(i64),
}

impl YellowstoneMessageTime {
    pub(super) fn from_created_at(timestamp: Option<&Timestamp>) -> Self {
        let Some(timestamp) = timestamp else {
            return Self::UnresolvedCreatedAt(CreatedAtUnavailable::Missing);
        };
        if timestamp.nanos < 0 || timestamp.nanos >= 1_000_000_000 {
            return Self::UnresolvedCreatedAt(CreatedAtUnavailable::InvalidNanos(timestamp.nanos));
        }
        match DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32) {
            Some(created_at) => Self::AvailableCreatedAt(created_at),
            None => Self::UnresolvedCreatedAt(CreatedAtUnavailable::OutOfRangeSeconds(
                timestamp.seconds,
            )),
        }
    }
}
