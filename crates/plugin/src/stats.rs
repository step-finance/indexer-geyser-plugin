use hashbrown::{ HashMap, HashSet };
use solana_program::pubkey::Pubkey;
use solana_sdk::transaction::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::sender::Sender;

#[derive(Debug)]
pub(crate) struct Stats<'a> {
    producer: Sender,
}

impl Stats {
    pub fn new(producer: Sender) -> Self {
        Self { producer }
    }

    pub fn process(
        &self,
        stx: &SanitizedTransaction,
        meta: &TransactionStatusMeta,
        slot: u64,
        is_vote: bool,
        is_err: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

struct SlotStats {
    slot: u64,

    tx_success: u64,
    tx_fees_success: u64,
    tx_fees_success_priority: u64,

    tx_err: u64,
    tx_fees_err: u64,
    tx_fees_err_priority: u64,

    tx_vote: u64,
    tx_fees_vote: u64,

    tx_vote_err: u64,
    tx_fees_vote_err: u64,

    programs: HashMap<Pubkey, ProgramStats>,
    payers: HashSet<Pubkey>,
    new_token_accounts: HashMap<Pubkey, u64>,
    mints_fungible_new: u64,
    mints_nonfungible_new: u64,
}

struct ProgramStats {
    success: u64,
    failed: u64,
}