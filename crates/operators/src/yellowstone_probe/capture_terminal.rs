use serde::Serialize;
use tonic::{Code, Status};

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TerminalStage {
    SubscribeOpen,
    StreamNext,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum StatusCategory {
    #[serde(rename = "tonic_0_14_4_decode_size_shape")]
    Tonic0_14_4DecodeSizeShape,
    #[serde(rename = "tonic_0_14_4_decompress_limit_shape")]
    Tonic0_14_4DecompressLimitShape,
    RangeOrResourceStatus,
    OtherStatus,
}

#[derive(Serialize)]
pub(crate) struct TerminalStatus {
    stage: TerminalStage,
    status_code: i32,
    category: StatusCategory,
    // Both client and server can supply identical status code/message shapes.
    origin: &'static str,
    configured_transport_limit_bytes: u64,
    message_length_bytes: Option<u64>,
    message_limit_bytes: Option<u64>,
}

impl TerminalStatus {
    pub fn from_status(stage: TerminalStage, status: &Status, limit: u64) -> Self {
        let code = status.code();
        let mut result = Self {
            stage,
            status_code: code as i32,
            category: if matches!(code, Code::OutOfRange | Code::ResourceExhausted) {
                StatusCategory::RangeOrResourceStatus
            } else {
                StatusCategory::OtherStatus
            },
            origin: "unknown",
            configured_transport_limit_bytes: limit,
            message_length_bytes: None,
            message_limit_bytes: None,
        };
        // Do not copy, serialize or scan unbounded provider messages. Only the
        // pinned Tonic 0.14.4 codec shapes admit numerical extraction below.
        let message = status.message();
        if message.len() > 160 {
            return result;
        }
        if code == Code::OutOfRange {
            if let Some(length) = decoded_length(message, limit) {
                result.category = StatusCategory::Tonic0_14_4DecodeSizeShape;
                result.message_length_bytes = Some(length);
                result.message_limit_bytes = Some(limit);
            }
        } else if code == Code::ResourceExhausted {
            if let Some(number) = message
                .strip_prefix("Error decompressing: size limit, of ")
                .and_then(|v| v.strip_suffix(" bytes, exceeded while decompressing message"))
                .and_then(decimal)
            {
                if number == limit {
                    result.category = StatusCategory::Tonic0_14_4DecompressLimitShape;
                    result.message_limit_bytes = Some(number);
                }
            }
        }
        result
    }
}

fn decoded_length(message: &str, limit: u64) -> Option<u64> {
    let (length, stated_limit) = message
        .strip_prefix("Error, decoded message length too large: found ")?
        .strip_suffix(" bytes")?
        .split_once(" bytes, the limit is: ")?;
    let length = decimal(length)?;
    // Tonic reads a u32 envelope length. These checks recognize only a shape
    // compatible with this configured decoder; they do not prove its origin.
    (decimal(stated_limit)? == limit && length > limit && length <= u32::MAX as u64)
        .then_some(length)
}

fn decimal(value: &str) -> Option<u64> {
    if value.is_empty() || value.starts_with('0') || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}
