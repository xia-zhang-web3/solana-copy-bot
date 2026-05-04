#[derive(Debug, Clone)]
pub(in crate::source) struct ParsedUiAmount {
    pub(in crate::source) amount: f64,
    pub(in crate::source) raw_amount: Option<String>,
    pub(in crate::source) decimals: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub(in crate::source::helius_parser) struct MintDelta {
    pub(in crate::source::helius_parser) amount_delta: f64,
    pub(in crate::source::helius_parser) raw_delta: Option<i128>,
    pub(in crate::source::helius_parser) decimals: Option<u8>,
    exact_unavailable: bool,
}

impl MintDelta {
    pub(in crate::source::helius_parser) fn apply_sub(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta -= amount.amount;
        self.apply_raw_delta(amount, -1);
    }

    pub(in crate::source::helius_parser) fn apply_add(&mut self, amount: &ParsedUiAmount) {
        self.amount_delta += amount.amount;
        self.apply_raw_delta(amount, 1);
    }

    fn apply_raw_delta(&mut self, amount: &ParsedUiAmount, sign: i8) {
        if self.exact_unavailable {
            return;
        }
        let Some(raw_amount) = amount.raw_amount.as_deref() else {
            self.invalidate_exact();
            return;
        };
        let Some(decimals) = amount.decimals else {
            self.invalidate_exact();
            return;
        };
        let Some(parsed_raw) = raw_amount.parse::<u64>().ok() else {
            self.invalidate_exact();
            return;
        };
        let parsed_raw = i128::from(parsed_raw);
        let Some(signed_raw) = parsed_raw.checked_mul(i128::from(sign)) else {
            self.invalidate_exact();
            return;
        };
        match self.decimals {
            Some(existing) if existing != decimals => {
                self.invalidate_exact();
            }
            Some(_) => {
                if let Some(current) = self.raw_delta {
                    match current.checked_add(signed_raw) {
                        Some(next) => self.raw_delta = Some(next),
                        None => self.invalidate_exact(),
                    }
                } else {
                    self.invalidate_exact();
                }
            }
            None => {
                self.decimals = Some(decimals);
                self.raw_delta = Some(signed_raw);
            }
        }
    }

    fn invalidate_exact(&mut self) {
        self.raw_delta = None;
        self.decimals = None;
        self.exact_unavailable = true;
    }

    pub(in crate::source::helius_parser) fn candidate(&self) -> ParsedUiAmount {
        ParsedUiAmount {
            amount: self.amount_delta.abs(),
            raw_amount: self.raw_delta.map(|value| value.abs().to_string()),
            decimals: self.decimals,
        }
    }
}
