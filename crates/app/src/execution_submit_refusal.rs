//! Diagnostic identity travels with the refusal; never infer it from last_error/order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreSubmitRefusal {
    order_id: String,
    reason: &'static str,
    count: usize,
}

impl PreSubmitRefusal {
    pub(crate) fn budget(order_id: &str, reason: &str) -> Box<Self> {
        let reason = match reason {
            "tiny_budget_inactive" => "tiny_budget_inactive",
            "tiny_budget_config_missing" => "tiny_budget_config_missing",
            "tiny_budget_stopped" => "tiny_budget_stopped",
            "tiny_budget_deadline" => "tiny_budget_deadline",
            "tiny_budget_clock" => "tiny_budget_clock",
            "tiny_budget_buy_slot" => "tiny_budget_buy_slot",
            "tiny_budget_sell_slots" => "tiny_budget_sell_slots",
            "tiny_budget_fee_exhausted" => "tiny_budget_fee_exhausted",
            "tiny_budget_transaction_fee" => "tiny_budget_transaction_fee",
            "tiny_budget_priority_fee" => "tiny_budget_priority_fee",
            "tiny_budget_buy_amount" => "tiny_budget_buy_amount",
            "tiny_budget_fee_unknown" => "tiny_budget_fee_unknown",
            "tiny_budget_identity" | "tiny_budget_activation_conflict" => "tiny_budget_identity",
            "tiny_budget_position_unconfirmed" | "tiny_budget_position_identity" => {
                "tiny_budget_position_identity"
            }
            _ => "tiny_budget_refused",
        };
        Box::new(Self {
            order_id: order_id.to_owned(),
            reason,
            count: 1,
        })
    }

    pub(crate) fn after_collection(order_id: &str, reason: &'static str) -> Box<Self> {
        // Closed, bounded labels only. Unknown diagnostics cannot leak SQL/URL/payload.
        let reason = match reason {
            "initial_sol_order_changed"
            | "tiny_submit_state_unavailable"
            | "initial_sol_safety_unavailable"
            | "initial_sol_failure_record_unavailable"
            | "kill_switch_active"
            | "entry_submit_disabled"
            | "confirmed_accounting_pending"
            | "max_open_positions"
            | "daily_loss_cap_zero"
            | "risk_decision_clock_unordered"
            | "max_daily_loss"
            | "cash_loss_unavailable" => reason,
            _ => "pre_submit_refused",
        };
        Box::new(Self {
            order_id: order_id.to_owned(),
            reason,
            count: 1,
        })
    }
}

/// Count pre-submit refusals only, not chain failures, fills, expenses or safety blocks.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PreSubmitRefusals {
    // A single pointer keeps diagnostic growth out of nested daemon futures.
    // Count and the last pair travel in the same allocation.
    last: Option<Box<PreSubmitRefusal>>,
}

impl PreSubmitRefusals {
    pub(crate) fn record(&mut self, refusal: Option<Box<PreSubmitRefusal>>) {
        if let Some(mut refusal) = refusal {
            refusal.count += self.count();
            self.last = Some(refusal);
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.record(other.last);
    }

    pub(crate) fn count(&self) -> usize {
        self.last.as_ref().map_or(0, |r| r.count)
    }
    pub(crate) fn order_id(&self) -> &str {
        self.last
            .as_ref()
            .map(|r| r.order_id.as_str())
            .unwrap_or("none")
    }
    pub(crate) fn reason(&self) -> &'static str {
        self.last.as_ref().map(|r| r.reason).unwrap_or("none")
    }
}
