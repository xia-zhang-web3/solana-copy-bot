use crate::fee_hints::ResolvedFeeHints;
use serde_json::{json, Value};

pub(crate) struct SubmitSuccessPayloadInputs<'a> {
    pub(crate) route: &'a str,
    pub(crate) contract_version: &'a str,
    pub(crate) client_order_id: &'a str,
    pub(crate) request_id: &'a str,
    pub(crate) tx_signature: &'a str,
    pub(crate) submit_transport: &'a str,
    pub(crate) submitted_at_rfc3339: &'a str,
    pub(crate) slippage_bps: f64,
    pub(crate) requested_tip_lamports: u64,
    pub(crate) effective_tip_lamports: u64,
    pub(crate) tip_policy_code: Option<&'a str>,
    pub(crate) cu_limit: u32,
    pub(crate) cu_price_micro_lamports: u64,
    pub(crate) resolved_fee_hints: ResolvedFeeHints,
    pub(crate) submit_signature_verify: Value,
}

pub(crate) fn build_submit_success_payload(inputs: SubmitSuccessPayloadInputs<'_>) -> Value {
    let mut response = json!({
        "status": "ok",
        "ok": true,
        "accepted": true,
        "route": inputs.route,
        "contract_version": inputs.contract_version,
        "client_order_id": inputs.client_order_id,
        "request_id": inputs.request_id,
        "tx_signature": inputs.tx_signature,
        "submit_transport": inputs.submit_transport,
        "submitted_at": inputs.submitted_at_rfc3339,
        "slippage_bps": inputs.slippage_bps,
        "tip_lamports": inputs.effective_tip_lamports,
        "compute_budget": {
            "cu_limit": inputs.cu_limit,
            "cu_price_micro_lamports": inputs.cu_price_micro_lamports,
        },
        "network_fee_lamports": inputs.resolved_fee_hints.network_fee_lamports,
        "base_fee_lamports": inputs.resolved_fee_hints.base_fee_lamports,
        "priority_fee_lamports": inputs.resolved_fee_hints.priority_fee_lamports,
    });
    if let Some(policy_code) = inputs.tip_policy_code {
        response["tip_policy"] = json!({
            "policy_code": policy_code,
            "requested_tip_lamports": inputs.requested_tip_lamports,
            "effective_tip_lamports": inputs.effective_tip_lamports,
        });
    }
    if let Some(ata_create_rent_lamports) = inputs.resolved_fee_hints.ata_create_rent_lamports {
        response["ata_create_rent_lamports"] = json!(ata_create_rent_lamports);
    }
    response["submit_signature_verify"] = inputs.submit_signature_verify;
    response
}

#[cfg(test)]
mod tests {
    use super::{build_submit_success_payload, SubmitSuccessPayloadInputs};
    use crate::fee_hints::ResolvedFeeHints;
    use serde_json::{json, Value};

    fn sample_inputs<'a>(tip_policy_code: Option<&'a str>) -> SubmitSuccessPayloadInputs<'a> {
        SubmitSuccessPayloadInputs {
            route: "rpc",
            contract_version: "v1",
            client_order_id: "client-1",
            request_id: "request-1",
            tx_signature: "sig-1",
            submit_transport: "upstream_signature",
            submitted_at_rfc3339: "2026-02-24T00:00:00+00:00",
            slippage_bps: 10.0,
            requested_tip_lamports: 123,
            effective_tip_lamports: 0,
            tip_policy_code,
            cu_limit: 300_000,
            cu_price_micro_lamports: 1_000,
            resolved_fee_hints: ResolvedFeeHints {
                network_fee_lamports: 5_300,
                base_fee_lamports: 5_000,
                priority_fee_lamports: 300,
                ata_create_rent_lamports: None,
            },
            submit_signature_verify: json!({"status":"seen"}),
        }
    }

    #[test]
    fn submit_payload_includes_tip_policy_when_present() {
        let payload = build_submit_success_payload(sample_inputs(Some("rpc_tip_forced_zero")));
        assert_eq!(
            payload
                .get("tip_policy")
                .and_then(|value| value.get("policy_code"))
                .and_then(Value::as_str),
            Some("rpc_tip_forced_zero")
        );
    }

    #[test]
    fn submit_payload_omits_tip_policy_when_absent() {
        let payload = build_submit_success_payload(sample_inputs(None));
        assert!(
            payload.get("tip_policy").is_none(),
            "tip_policy must be absent when no policy code is provided"
        );
    }

    #[test]
    fn ata_submit_payload_omits_ata_create_rent_lamports_when_absent() {
        let payload = build_submit_success_payload(sample_inputs(None));
        assert!(
            payload.get("ata_create_rent_lamports").is_none(),
            "ATA fee hint must be omitted when absent"
        );
    }

    #[test]
    fn ata_submit_payload_includes_ata_create_rent_lamports_when_present() {
        let mut inputs = sample_inputs(None);
        inputs.resolved_fee_hints.ata_create_rent_lamports = Some(2_039_280);
        let payload = build_submit_success_payload(inputs);
        assert_eq!(
            payload
                .get("ata_create_rent_lamports")
                .and_then(Value::as_u64),
            Some(2_039_280)
        );
    }
}
