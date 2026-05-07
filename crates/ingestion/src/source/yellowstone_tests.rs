use super::parse_proto_ui_amount;
use yellowstone_grpc_proto::prelude::UiTokenAmount;

#[test]
fn parse_proto_ui_amount_rejects_non_finite_strings() {
    let amount = UiTokenAmount {
        ui_amount: 0.0,
        decimals: 6,
        amount: String::new(),
        ui_amount_string: "NaN".to_string(),
    };

    assert!(
        parse_proto_ui_amount(Some(&amount)).is_none(),
        "NaN ui_amount_string must be rejected"
    );
}

#[test]
fn parse_proto_ui_amount_rejects_non_finite_raw_amounts() {
    let amount = UiTokenAmount {
        ui_amount: 0.0,
        decimals: 6,
        amount: "NaN".to_string(),
        ui_amount_string: String::new(),
    };

    assert!(
        parse_proto_ui_amount(Some(&amount)).is_none(),
        "NaN raw amount must be rejected"
    );
}

#[test]
fn parse_proto_ui_amount_rejects_non_finite_ui_amount_field() {
    let amount = UiTokenAmount {
        ui_amount: f64::INFINITY,
        decimals: 6,
        amount: String::new(),
        ui_amount_string: String::new(),
    };

    assert!(
        parse_proto_ui_amount(Some(&amount)).is_none(),
        "non-finite ui_amount field must be rejected"
    );
}

#[test]
fn parse_proto_ui_amount_rejects_decimals_out_of_u8_range() {
    let amount = UiTokenAmount {
        ui_amount: 1.0,
        decimals: u32::from(u8::MAX) + 1,
        amount: "1000000".to_string(),
        ui_amount_string: "1".to_string(),
    };

    assert!(
        parse_proto_ui_amount(Some(&amount)).is_none(),
        "out-of-range decimals must be rejected"
    );
}
