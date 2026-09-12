//! Transport-attempt binding, not DEX instruction semantics or quote freshness proof.
use crate::execution_instruction_bundle::{pubkey, InstructionBundle};
use crate::execution_submit_adapter::{ExecutionSubmitRequest, ExecutionTransactionPlan};
use crate::execution_swap_blueprint::{build_execution_swap_blueprint, ExecutionSwapBlueprint};
use crate::execution_swap_http_request::{disable_shared_accounts, swap_request_body};
use anyhow::{ensure, Context, Result};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BundleRequest {
    plan_id: String,
    request: ExecutionSubmitRequest,
    blueprint: ExecutionSwapBlueprint,
    submit_enabled: bool,
    body: Value,
}
impl BundleRequest {
    pub(crate) fn capture(plan: &ExecutionTransactionPlan) -> Result<Self> {
        let request = ExecutionSubmitRequest {
            order_id: plan.order_id.clone(),
            signal_id: plan.signal_id.clone(),
            client_order_id: plan.client_order_id.clone(),
            attempt: plan.attempt,
            route: plan.route.clone(),
            wallet_id: String::new(),
            token: plan.token.clone(),
            side: plan.side.clone(),
            buy_size_sol: plan.buy_size_sol,
            slippage_tolerance_bps: plan.slippage_tolerance_bps,
            wallet_pubkey: plan.wallet_pubkey.clone(),
            entry_route_plan_json: plan.entry_route_plan_json.clone(),
            metadata: {
                let mut metadata = plan.metadata.clone();
                metadata.protected_capital = None;
                metadata
            },
        };
        // wallet_id is absent from plans and irrelevant to fee/blueprint APIs. The
        // original request still goes through all existing presign alignment checks.
        let blueprint = build_execution_swap_blueprint(&request)?;
        ensure!(
            plan.swap_blueprint.as_ref() == Some(&blueprint),
            "instruction_bundle_blueprint_binding"
        );
        ensure!(
            blueprint.wallet_pubkey.as_deref() == Some(plan.wallet_pubkey.as_str()),
            "instruction_bundle_wallet_binding"
        );
        pubkey(&plan.wallet_pubkey)?;
        let mut body = swap_request_body(plan, &plan.wallet_pubkey, "instruction bundle")?;
        disable_shared_accounts(&mut body);
        validate_quote(&body["quoteResponse"], &blueprint, plan)?;
        Ok(Self {
            plan_id: plan.plan_id.clone(),
            request,
            blueprint,
            submit_enabled: plan.submit_enabled,
            body,
        })
    }
    pub(crate) fn buy_requested_lamports(&self) -> Result<u64> {
        ensure!(
            self.request.side.eq_ignore_ascii_case("buy"),
            "tiny_capital_buy_required"
        );
        let raw = self.body["quoteResponse"]["inAmount"]
            .as_str()
            .context("tiny_capital_requested_amount")?;
        ensure!(
            !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()),
            "tiny_capital_requested_amount"
        );
        let amount: u64 = raw.parse()?;
        ensure!(
            (1..=copybot_storage_core::TINY_BUY_LAMPORTS).contains(&amount),
            "tiny_capital_requested_amount"
        );
        Ok(amount)
    }
    pub(crate) fn verify_request(&self, request: &ExecutionSubmitRequest) -> Result<()> {
        let mut request = request.clone();
        request.wallet_id.clear();
        request.metadata.protected_capital = None;
        ensure!(request == self.request, "tiny_capital_request_binding");
        Ok(())
    }
    pub(crate) fn request_sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&self.body)?)
        ))
    }
    pub(crate) fn body(&self) -> &Value {
        &self.body
    }
    pub(crate) fn request(&self) -> &ExecutionSubmitRequest {
        &self.request
    }
    pub(crate) fn verify(&self, plan: &ExecutionTransactionPlan) -> Result<()> {
        ensure!(
            *self == Self::capture(plan)?,
            "instruction_bundle_attempt_binding"
        );
        Ok(())
    }
    pub(crate) fn bind(self, response: &Value) -> Result<BoundInstructionBundle> {
        let bundle = InstructionBundle::parse(response, pubkey(&self.request.wallet_pubkey)?)?;
        Ok(BoundInstructionBundle {
            request: self,
            bundle,
        })
    }
}
#[derive(Debug)]
pub(crate) struct BoundInstructionBundle {
    request: BundleRequest,
    bundle: InstructionBundle,
}
impl BoundInstructionBundle {
    pub(crate) fn verified_parts(
        &self,
        plan: &ExecutionTransactionPlan,
    ) -> Result<(&BundleRequest, &InstructionBundle)> {
        self.request.verify(plan)?;
        Ok((&self.request, &self.bundle))
    }
}
fn validate_quote(
    quote: &Value,
    blueprint: &ExecutionSwapBlueprint,
    plan: &ExecutionTransactionPlan,
) -> Result<()> {
    ensure!(quote.is_object(), "instruction_bundle_quote_type");
    for (field, expected) in [
        ("inputMint", blueprint.input_mint.as_str()),
        ("outputMint", blueprint.output_mint.as_str()),
        ("inAmount", blueprint.input_amount_raw.as_str()),
        ("outAmount", blueprint.output_amount_raw.as_str()),
        ("swapMode", "ExactIn"),
    ] {
        ensure!(
            quote[field].as_str() == Some(expected),
            "instruction_bundle_quote_binding:{field}"
        );
    }
    ensure!(
        quote["slippageBps"].as_u64() == Some(plan.slippage_tolerance_bps),
        "instruction_bundle_slippage_binding"
    );
    let route: Value = serde_json::from_str(
        plan.metadata
            .route_plan_json
            .as_deref()
            .context("instruction_bundle_route_missing")?,
    )?;
    ensure!(
        route.is_array() && quote["routePlan"] == route,
        "instruction_bundle_route_binding"
    );
    let threshold = quote["otherAmountThreshold"]
        .as_str()
        .context("instruction_bundle_threshold_type")?
        .parse::<u128>()
        .context("instruction_bundle_threshold_value")?;
    let out = blueprint.output_amount_raw.parse::<u128>()?;
    ensure!(
        threshold > 0 && threshold <= out,
        "instruction_bundle_threshold_value"
    );
    Ok(())
}
