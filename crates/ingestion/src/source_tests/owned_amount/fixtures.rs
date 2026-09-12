use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) enum Damage {
    Absent,
    Text,
    Nan,
    Infinite,
}

impl Damage {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Text => "text",
            Self::Nan => "nan",
            Self::Infinite => "infinite",
        }
    }
    pub(super) fn text(self) -> &'static str {
        match self {
            Self::Absent => unreachable!(),
            Self::Text => "not-an-amount",
            Self::Nan => "NaN",
            Self::Infinite => "inf",
        }
    }
}
pub(super) const DAMAGES: [Damage; 4] =
    [Damage::Absent, Damage::Text, Damage::Nan, Damage::Infinite];

pub(super) fn healthy(sell: bool, native: bool, split: bool, ray: bool) -> Value {
    let mut f = previous::base(sell, native);
    if split {
        previous::split_target(&mut f, sell);
    }
    let user_quote = f["roles"]["user_quote"].as_str().unwrap();
    let index = f["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .position(|k| k["pubkey"] == user_quote)
        .unwrap();
    // Add the same seven-token inventory to both sides. The supported swap
    // still transfers ten tokens, now BUY7->17 / partial SELL17->7.
    for field in ["preTokenBalances", "postTokenBalances"] {
        let row = f["result"]["meta"][field]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|r| r["accountIndex"] == index)
            .unwrap();
        let raw: u64 = row["uiTokenAmount"]["amount"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let raw = raw + 7_000_000;
        let ui = raw as f64 / 1e6;
        row["uiTokenAmount"] = json!({"amount":raw.to_string(),"decimals":6,"uiAmount":ui,"uiAmountString":ui.to_string()});
    }
    if ray {
        previous::raydium(&mut f);
    }
    super::super::controls::ledger(&f);
    f
}

pub(super) fn rename(f: &mut Value, label: &str) {
    previous::rename(f, label);
    let mut bytes = bs58::decode(f["signature"].as_str().unwrap())
        .into_vec()
        .unwrap();
    bytes[0] = 48;
    let signature = bs58::encode(bytes).into_string();
    f["signature"] = json!(signature);
    f["result"]["transaction"]["signatures"][0] = f["signature"].clone();
}

#[derive(Clone, Copy)]
pub(super) struct Operand {
    pub post: bool,
    pub foreign: bool,
    pub damage: Damage,
}

impl Operand {
    pub(super) fn field(self) -> &'static str {
        if self.post {
            "postTokenBalances"
        } else {
            "preTokenBalances"
        }
    }
    pub(super) fn owner<'a>(self, f: &'a Value) -> &'a str {
        f["roles"][if self.foreign { "pool" } else { "user" }]
            .as_str()
            .unwrap()
    }
    pub(super) fn row_index(self, f: &Value) -> usize {
        f["result"]["meta"][self.field()]
            .as_array()
            .unwrap()
            .iter()
            .position(|r| r["owner"] == self.owner(f) && r["mint"] == f["roles"]["quote_mint"])
            .unwrap()
    }
    pub(super) fn json(self, f: &Value) -> Value {
        let mut mutated = f.clone();
        let amount =
            &mut mutated["result"]["meta"][self.field()][self.row_index(f)]["uiTokenAmount"];
        match self.damage {
            Damage::Absent => *amount = Value::Null,
            damage => amount["uiAmountString"] = json!(damage.text()),
        }
        assert!(HeliusWsSource::parse_ui_amount_json(Some(amount)).is_none());
        let mut restored = mutated.clone();
        restored["result"]["meta"][self.field()][self.row_index(f)]["uiTokenAmount"] =
            f["result"]["meta"][self.field()][self.row_index(f)]["uiTokenAmount"].clone();
        assert_eq!(restored, *f); // Every other instruction/balance/identity field unchanged.
        mutated
    }
}
