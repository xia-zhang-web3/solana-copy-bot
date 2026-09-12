use super::super::super::capture_terminal::{TerminalStage, TerminalStatus};
use serde_json::Value;
use tonic::{Code, Status};

const LIMIT: u64 = 1_048_576;
const DECODE: &str =
    "Error, decoded message length too large: found 1048583 bytes, the limit is: 1048576 bytes";
const DECOMPRESS: &str =
    "Error decompressing: size limit, of 1048576 bytes, exceeded while decompressing message";

fn evidence(code: Code, message: &str) -> Value {
    let mut status =
        Status::with_details(code, message, b"synthetic-secret details".to_vec().into());
    status
        .metadata_mut()
        .insert("x-token", "synthetic-secret".parse().unwrap());
    let result = serde_json::to_value(TerminalStatus::from_status(
        TerminalStage::StreamNext,
        &status,
        LIMIT,
    ))
    .unwrap();
    let serialized = serde_json::to_string(&result).unwrap();
    assert!(serialized.len() < 512);
    assert!(!serialized.contains("synthetic-secret"));
    assert!(!serialized.contains("private.invalid"));
    assert_eq!(result["origin"], "unknown");
    assert_eq!(result["configured_transport_limit_bytes"], LIMIT);
    assert_eq!(result["status_code"], code as i32);
    result
}

#[test]
fn terminal_exact_pinned_shapes_only_record_numbers_without_origin_proof() {
    let decoded = evidence(Code::OutOfRange, DECODE);
    assert_eq!(decoded["category"], "tonic_0_14_4_decode_size_shape");
    assert_eq!(decoded["message_length_bytes"], 1_048_583);
    assert_eq!(decoded["message_limit_bytes"], LIMIT);
    let decompressed = evidence(Code::ResourceExhausted, DECOMPRESS);
    assert_eq!(
        decompressed["category"],
        "tonic_0_14_4_decompress_limit_shape"
    );
    assert!(decompressed["message_length_bytes"].is_null());
    assert_eq!(decompressed["message_limit_bytes"], LIMIT);
    assert_eq!(
        evidence(Code::OutOfRange, DECOMPRESS)["category"],
        "range_or_resource_status"
    );
    assert_eq!(
        evidence(Code::ResourceExhausted, DECODE)["category"],
        "range_or_resource_status"
    );
}

#[test]
fn terminal_malformed_overflow_and_unbounded_messages_retain_no_text() {
    for text in [
        String::new(),
        DECODE.replace("1048583", ""),
        DECODE.replace("1048583", "-1"),
        DECODE.replace("1048583", "+1048583"),
        DECODE.replace("1048583", "01048583"),
        DECODE.replace("1048583", "1048576"),
        DECODE.replace("1048583", "4294967296"),
        DECODE.replace("1048583", "18446744073709551616"),
        DECODE.replace("1048583", "synthetic-secret"),
        DECODE.replace("1048576", "1048575"),
        DECODE.replace("1048576", "0"),
        DECODE.replace("1048576", "18446744073709551616"),
        format!("{DECODE} synthetic-secret https://private.invalid"),
        format!("synthetic-secret {DECODE}"),
        DECODE.replace("1048583", "1048583\n"),
        "synthetic-secret https://private.invalid ".repeat(10_000),
        DECODE.replace("1048583", "١٠٤٨٥٨٣"),
    ] {
        let result = evidence(Code::OutOfRange, &text);
        assert_eq!(result["category"], "range_or_resource_status");
        assert!(result["message_length_bytes"].is_null());
        assert!(result["message_limit_bytes"].is_null());
    }
    for text in [
        DECOMPRESS.replace("1048576", ""),
        DECOMPRESS.replace("1048576", "01048576"),
        DECOMPRESS.replace("1048576", "18446744073709551616"),
        format!("{DECOMPRESS} synthetic-secret"),
    ] {
        let result = evidence(Code::ResourceExhausted, &text);
        assert_eq!(result["category"], "range_or_resource_status");
        assert!(result["message_limit_bytes"].is_null());
    }
}

#[test]
fn terminal_all_status_codes_stay_exact_with_unknown_origin() {
    for code in [
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::FailedPrecondition,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ] {
        let result = evidence(code, "synthetic-secret https://private.invalid");
        assert_eq!(
            result["category"],
            if code == Code::OutOfRange || code == Code::ResourceExhausted {
                "range_or_resource_status"
            } else {
                "other_status"
            }
        );
        assert!(result["message_length_bytes"].is_null());
        assert!(result["message_limit_bytes"].is_null());
    }
}

#[test]
fn terminal_stage_is_recorded_from_caller_without_status_origin_inference() {
    for (stage, expected) in [
        (TerminalStage::SubscribeOpen, "subscribe_open"),
        (TerminalStage::StreamNext, "stream_next"),
    ] {
        let value = serde_json::to_value(TerminalStatus::from_status(
            stage,
            &Status::out_of_range(DECODE),
            LIMIT,
        ))
        .unwrap();
        assert_eq!(value["stage"], expected);
        assert_eq!(value["origin"], "unknown");
    }
}
