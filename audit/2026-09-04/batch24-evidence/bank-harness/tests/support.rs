// Based on accepted B23 support; this copy executes the B24 production constructor bytes.
use crate::cpi;
use crate::execution_native_floor::{prepare_final_native_floor, verify_final_native_floor};
use base64::{engine::general_purpose::STANDARD, Engine};

use crate::execution_solana_tx::{
    serialize_unsigned_legacy_transaction, SolanaAccountMeta, SolanaInstruction,
};
use anyhow::Result;
use solana_runtime::bank::Bank;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    fee_calculator::FeeRateGovernor,
    genesis_config::GenesisConfig,
    hash::hash,
    instruction::InstructionError,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction::SystemError,
    system_program,
    transaction::{Transaction, TransactionError},
};

pub const FEE: u64 = 5_000;
pub const RESERVE: u64 = 2_000_000;
pub const WALLET: u64 = 10_000_000;
pub const RECIPIENT: u64 = 3_000_000;

pub struct Fixture {
    pub wallet: Keypair,
    pub recipient: Pubkey,
    genesis: GenesisConfig,
}

pub struct Outcome {
    pub wallet: u64,
    pub recipient: u64,
    pub status: std::result::Result<(), TransactionError>,
    pub logs: Vec<String>,
}

impl Fixture {
    pub fn new(initial: u64) -> Self {
        // Synthetic keys live only in memory; no keys, seeds or keypair bytes are saved.
        let wallet = Keypair::new();
        let recipient = Pubkey::new_unique();
        let mut genesis = GenesisConfig::new(
            &[
                (
                    wallet.pubkey(),
                    AccountSharedData::new(initial, 0, &system_program::id()),
                ),
                (
                    recipient,
                    AccountSharedData::new(RECIPIENT, 0, &system_program::id()),
                ),
            ],
            &[],
        );
        genesis.creation_time = 0;
        genesis.fee_rate_governor = FeeRateGovernor::new(FEE, 0);
        assert!(initial >= genesis.rent.minimum_balance(0));
        assert!(RESERVE - 1 >= genesis.rent.minimum_balance(0));
        assert!(RECIPIENT >= genesis.rent.minimum_balance(0));
        Self {
            wallet,
            recipient,
            genesis,
        }
    }

    pub fn debit(&self, amount: u64, cpi: bool) -> SolanaInstruction {
        let mut instruction = transfer(self.wallet.pubkey(), self.recipient, amount);
        if cpi {
            instruction.program_id = cpi::PROGRAM.to_bytes();
            instruction.data = amount.to_le_bytes().to_vec();
            instruction
                .accounts
                .push(SolanaAccountMeta::readonly(system_program::id().to_bytes()));
        }
        instruction
    }

    pub fn run(
        &self,
        label: &str,
        instructions: &[SolanaInstruction],
        reserve: Option<u64>,
    ) -> Result<Outcome> {
        // New Bank from exactly the same genesis for every causal control: no replay/history.
        let mut bank = Bank::new_for_tests(&self.genesis);
        bank.add_mockup_builtin(cpi::PROGRAM, cpi::DebitBuiltin::vm);
        let (bank, _bank_forks) = bank.wrap_with_bank_forks_for_tests();
        let before = bank.get_account(&self.wallet.pubkey()).unwrap();
        let recipient_before = bank.get_account(&self.recipient).unwrap();
        assert_eq!(before.owner(), &system_program::id());
        assert!(before.data().is_empty() && !before.executable());
        let (unsigned, guard) = if let Some(reserve) = reserve {
            let proof = prepare_final_native_floor(
                self.wallet.pubkey().to_bytes(),
                bank.last_blockhash().to_bytes(),
                instructions,
                reserve,
            )?;
            assert_eq!(
                verify_final_native_floor(
                    proof.payload(),
                    self.wallet.pubkey().to_bytes(),
                    reserve
                )?,
                proof
            );
            assert_eq!(proof.wallet(), self.wallet.pubkey().to_bytes());
            assert_eq!(proof.reserve_lamports(), reserve);
            assert_eq!(proof.final_instruction_index(), instructions.len());
            let wire = STANDARD.decode(proof.payload())?;
            assert_eq!(&wire[65..], proof.binding().message_bytes);
            (wire, Some(proof.final_instruction_index()))
        } else {
            (
                serialize_unsigned_legacy_transaction(
                    self.wallet.pubkey().to_bytes(),
                    bank.last_blockhash().to_bytes(),
                    instructions,
                )?,
                None,
            )
        };
        let mut tx: Transaction = bincode::deserialize(&unsigned)?;
        assert_eq!(bincode::serialize(&tx)?, unsigned);
        assert_eq!(tx.message.header.num_required_signatures, 1);
        assert_eq!(tx.message.header.num_readonly_signed_accounts, 0);
        assert_eq!(
            tx.message
                .account_keys
                .iter()
                .filter(|k| **k == self.wallet.pubkey())
                .count(),
            1
        );
        if let Some(index) = guard {
            let compiled = &tx.message.instructions[index];
            assert_eq!(compiled.accounts, [0, 0]);
            assert_eq!(
                tx.message.account_keys[compiled.program_id_index as usize],
                system_program::id()
            );
            assert!(tx.message.is_signer(0));
            assert!(tx.message.is_maybe_writable(0, None));
        }
        let message_before = tx.message_data();
        tx.try_sign(&[&self.wallet], bank.last_blockhash())?;
        assert_eq!(tx.message_data(), message_before);
        tx.verify()?;
        // A signature over the pre-guard message cannot authenticate the guarded bytes.
        // Both signing operations use only this fixture's synthetic key, never app signing.
        if guard.is_some() {
            let previous_instructions = instructions.to_vec();
            let previous_wire = serialize_unsigned_legacy_transaction(
                self.wallet.pubkey().to_bytes(),
                bank.last_blockhash().to_bytes(),
                &previous_instructions,
            )?;
            let mut previous: Transaction = bincode::deserialize(&previous_wire)?;
            previous.try_sign(&[&self.wallet], bank.last_blockhash())?;
            previous.verify()?;
            let mut stale_signature = tx.clone();
            stale_signature.signatures = previous.signatures;
            assert_eq!(
                stale_signature.verify(),
                Err(TransactionError::SignatureFailure)
            );
            eprintln!("B24_SIGNATURE case={label} old_valid=true new_valid=true reused_old=SignatureFailure");
        }
        assert!(bincode::serialized_size(&tx)? <= 1232);
        let committed = bank.process_transaction_with_metadata(tx.clone())?;
        let actual_fee = committed.fee_details.total_fee();
        assert_eq!(actual_fee, FEE);
        assert_eq!(
            bank.get_signature_status(&tx.signatures[0]).unwrap(),
            committed.status
        );
        let after = bank.get_account(&self.wallet.pubkey()).unwrap();
        let recipient_after = bank.get_account(&self.recipient).unwrap();
        // The only persisted changes under investigation are exact native lamports.
        assert_eq!(
            (before.owner(), before.data(), before.executable()),
            (after.owner(), after.data(), after.executable())
        );
        assert_eq!(
            (
                recipient_before.owner(),
                recipient_before.data(),
                recipient_before.executable()
            ),
            (
                recipient_after.owner(),
                recipient_after.data(),
                recipient_after.executable()
            )
        );
        let logs = committed
            .log_messages
            .expect("runtime log recording enabled");
        let mut features: Vec<_> = bank
            .feature_set
            .active
            .iter()
            .map(|(id, slot)| (id.to_string(), *slot))
            .collect();
        features.sort();
        eprintln!("B24 case={label} W={} F={actual_fee} recipient_before={} wallet_after={} recipient_after={} bytes={} message_sha256={} status={:?} units={} rent_min={} features={:?}",
            before.lamports(), recipient_before.lamports(), after.lamports(), recipient_after.lamports(),
            unsigned.len(), hash(&message_before), committed.status, committed.executed_units,
            self.genesis.rent.minimum_balance(0), features);
        for line in &logs {
            eprintln!("B24_LOG case={label} {line}");
        }
        Ok(Outcome {
            wallet: after.lamports(),
            recipient: recipient_after.lamports(),
            status: committed.status,
            logs,
        })
    }
}

pub fn transfer(from: Pubkey, to: Pubkey, amount: u64) -> SolanaInstruction {
    SolanaInstruction {
        program_id: system_program::id().to_bytes(),
        accounts: vec![
            SolanaAccountMeta::signer_writable(from.to_bytes()),
            SolanaAccountMeta::writable(to.to_bytes()),
        ],
        data: [2_u32.to_le_bytes().to_vec(), amount.to_le_bytes().to_vec()].concat(),
    }
}

pub fn insufficient(index: u8) -> std::result::Result<(), TransactionError> {
    Err(TransactionError::InstructionError(
        index,
        InstructionError::from(SystemError::ResultWithNegativeLamports),
    ))
}
