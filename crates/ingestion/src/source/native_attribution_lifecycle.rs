use super::{
    wire::{self, Op},
    Instruction, View, SOL_MINT, SYSTEM, TOKEN,
};
use sha2::{Digest, Sha256};

pub(super) fn prove(v: &View, parent: usize, a: &[usize], buy: bool, sol: u64) -> Option<()> {
    let top = v.top.as_ref()?;
    let temp = a[5];
    let trader = a[1];
    let relevant = [a[5], a[6], a[7], a[8]];
    let mut lifecycle = Vec::new();
    for (index, ix) in top.iter().enumerate() {
        if index == parent {
            continue;
        }
        let keys = ix.accounts.as_ref()?;
        if keys.iter().any(|i| relevant.contains(i)) {
            // No other use of either trading leg outside the selected parent.
            if keys.iter().any(|i| relevant.contains(i) && *i != temp) {
                return None;
            }
            lifecycle.push((index, ix));
        }
    }
    if lifecycle.len() != if buy { 5 } else { 3 } {
        return None;
    }
    let (create_index, create) = lifecycle[0];
    let (init_index, init) = lifecycle[1];
    let (close_index, close) = *lifecycle.last()?;
    if !(create_index < init_index && init_index < parent && parent < close_index) {
        return None;
    }
    let Op::Create {
        base,
        seed,
        lamports,
        space,
        owner,
    } = operation(create)?
    else {
        return None;
    };
    if base != v.key(trader)? || owner != TOKEN || space != 165 || lamports == 0 {
        return None;
    }
    let create_keys = create.accounts.as_ref()?;
    if create_keys.as_slice() != [trader, temp] && create_keys.as_slice() != [trader, temp, trader]
    {
        return None;
    }
    let mut hash = Sha256::new();
    hash.update(bs58::decode(&base).into_vec().ok()?);
    hash.update(seed.as_bytes());
    hash.update(bs58::decode(&owner).into_vec().ok()?);
    if bs58::encode(hash.finalize()).into_string() != v.key(temp)? {
        return None;
    }
    if operation(init)? != Op::Init(base)
        || init.accounts.as_ref()?.as_slice() != [temp, a[3]]
        || v.key(a[3])? != SOL_MINT
    {
        return None;
    }
    if operation(close)? != Op::Close
        || close.accounts.as_ref()?.as_slice() != [temp, trader, trader]
    {
        return None;
    }
    if buy {
        let (fund_index, fund) = lifecycle[2];
        let (sync_index, sync) = lifecycle[3];
        if !(init_index < fund_index && fund_index < sync_index && sync_index < parent) {
            return None;
        }
        if operation(fund)? != Op::Fund(sol) || fund.accounts.as_ref()?.as_slice() != [trader, temp]
        {
            return None;
        }
        if operation(sync)? != Op::Sync || sync.accounts.as_ref()?.as_slice() != [temp] {
            return None;
        }
    }
    Some(())
}

fn operation(ix: &Instruction) -> Option<Op> {
    let program = ix.program.as_deref()?;
    if program != SYSTEM && program != TOKEN {
        return None;
    }
    wire::decode(program, ix.data.as_ref()?)
}

pub(super) fn ata_compatible_operation(v: &View, ix: &Instruction, user_base: usize) -> bool {
    let Some(program) = ix.program.as_deref() else {
        return false;
    };
    if !matches!(program, SYSTEM | TOKEN) {
        return true;
    }
    let Some(keys) = &ix.accounts else {
        return false;
    };
    let Some(op) = operation(ix) else {
        return false;
    };
    // Validate the decoded operand layout before using absence from the account
    // list as negative evidence. None, empty and partial lists are all unknown.
    let bound = keys.iter().all(|i| v.key(*i).is_some())
        && match (&op, keys.as_slice()) {
            (Op::Create { base, .. }, [source, created, ..]) => {
                source != created
                    && (v.key(*source) == Some(base.as_str())
                        || keys.get(2).and_then(|i| v.key(*i)) == Some(base.as_str()))
            }
            (Op::CreateAccount { .. }, [source, created, ..]) => source != created,
            (Op::Fund(_) | Op::Init(_), [_, _, ..]) => true,
            (Op::Sync, [_, ..]) => true,
            (Op::Close | Op::Transfer(_), [_, _, _, ..]) => true,
            _ => false,
        };
    bound && (!keys.contains(&user_base) || matches!(op, Op::Fund(_) | Op::Sync | Op::Close))
}
