use chrono::{DateTime, Utc};
use yellowstone_grpc_proto::prost_types::Timestamp;

/// AvailableCreatedAt is exclusively SubscribeUpdate.created_at metadata.
/// Recovery provenance retains a separate block time without replacing this
/// clock or claiming a fresh stream message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum YellowstoneMessageTime {
    AvailableCreatedAt(DateTime<Utc>),
    UnresolvedCreatedAt(CreatedAtUnavailable),
    /// Recovery provenance. Block time is retained without inventing created_at.
    RecoveredBlock {
        block_time: Option<i64>,
    },
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
