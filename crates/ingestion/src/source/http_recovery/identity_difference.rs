//! First difference in the existing runtime predicate's field order.
//! This visitor describes that predicate; it never authorizes an admission.
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::*;

pub(super) struct Difference {
    pub path: String,
    pub grpc: Value,
    pub http: Value,
}
impl Difference {
    pub fn json(&self) -> Value {
        json!({"path":self.path,"grpc":self.grpc,"http":self.http})
    }
}
trait Compare {
    fn difference(&self, other: &Self, path: &str) -> Option<Difference>;
}
fn changed(path: &str, grpc: Value, http: Value) -> Option<Difference> {
    Some(Difference {
        path: path.into(),
        grpc,
        http,
    })
}
macro_rules! scalar {
    ($($kind:ty),*) => {$(impl Compare for $kind {
        fn difference(&self, other: &Self, path: &str) -> Option<Difference> {
            if self == other { None } else { changed(path, json!(self), json!(other)) }
        }
    })*};
}
scalar!(bool, u8, u32, u64, i32, i64, String);
impl Compare for f64 {
    fn difference(&self, other: &Self, path: &str) -> Option<Difference> {
        let value =
            |v: f64| json!({"bits":format!("0x{:016x}",v.to_bits()),"decimal":v.to_string()});
        if self.to_bits() == other.to_bits() {
            None
        } else {
            changed(path, value(*self), value(*other))
        }
    }
}
impl<T: Compare> Compare for Option<T> {
    fn difference(&self, other: &Self, path: &str) -> Option<Difference> {
        match (self, other) {
            (Some(a), Some(b)) => a.difference(b, path),
            (None, None) => None,
            _ => changed(
                &format!("{path}.presence"),
                json!(self.is_some()),
                json!(other.is_some()),
            ),
        }
    }
}
impl<T: Compare> Compare for Vec<T> {
    fn difference(&self, other: &Self, path: &str) -> Option<Difference> {
        if self.len() != other.len() {
            return changed(
                &format!("{path}.length"),
                json!(self.len()),
                json!(other.len()),
            );
        }
        self.iter()
            .zip(other)
            .enumerate()
            .find_map(|(i, (a, b))| a.difference(b, &format!("{path}[{i}]")))
    }
}
macro_rules! fields {
    ($kind:ty; $($field:ident),+) => {
        impl Compare for $kind {
            fn difference(&self, other: &Self, path: &str) -> Option<Difference> {
                $(if let Some(d) = self.$field.difference(&other.$field,&format!("{path}.{}",stringify!($field))) { return Some(d); })+
                None
            }
        }
    };
}
fields!(SubscribeUpdateTransactionInfo; signature,index,is_vote,transaction,meta);
fields!(Transaction; signatures,message);
fields!(Message; header,account_keys,recent_blockhash,instructions,versioned,address_table_lookups,config);
fields!(MessageHeader; num_required_signatures,num_readonly_signed_accounts,num_readonly_unsigned_accounts);
fields!(CompiledInstruction; program_id_index,accounts,data);
fields!(MessageAddressTableLookup; account_key,writable_indexes,readonly_indexes);
fields!(TransactionConfig; priority_fee,compute_unit_limit,loaded_accounts_data_size_limit,heap_size);
fields!(TransactionStatusMeta; err,fee,pre_balances,post_balances,inner_instructions,inner_instructions_none,log_messages,log_messages_none,pre_token_balances,post_token_balances,rewards,loaded_writable_addresses,loaded_readonly_addresses,return_data,return_data_none,compute_units_consumed,cost_units);
fields!(TransactionError; err);
fields!(InnerInstructions; index,instructions);
fields!(InnerInstruction; program_id_index,accounts,data,stack_height);
fields!(TokenBalance; account_index,mint,owner,program_id,ui_token_amount);
fields!(UiTokenAmount; ui_amount,amount,decimals,ui_amount_string);
fields!(ReturnData; program_id,data);
fields!(Reward; pubkey,lamports,post_balance,reward_type,commission,commission_bps);
fields!(Rewards; rewards,num_partitions);
fields!(NumPartitions; num_partitions);
fields!(UnixTimestamp; timestamp);
fields!(BlockHeight; block_height);

pub(super) fn first(
    grpc: &SubscribeUpdateBlock,
    http: &SubscribeUpdateBlock,
) -> Option<Difference> {
    macro_rules! field {
        ($field:ident) => {
            if let Some(d) = grpc.$field.difference(&http.$field, stringify!($field)) {
                return Some(d);
            }
        };
    }
    field!(slot);
    field!(parent_slot);
    field!(blockhash);
    field!(parent_blockhash);
    field!(block_time);
    field!(block_height);
    // Preserve the old predicate's None -> default Rewards semantics.
    let empty = Rewards::default();
    if let Some(d) = grpc
        .rewards
        .as_ref()
        .unwrap_or(&empty)
        .difference(http.rewards.as_ref().unwrap_or(&empty), "rewards")
    {
        return Some(d);
    }
    field!(executed_transaction_count);
    field!(transactions);
    None
}
