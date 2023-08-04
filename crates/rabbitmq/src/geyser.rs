//! Queue configuration for Solana Geyser plugins intended to communicate
//! with `holaplex-indexer`.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use serde::{Deserialize, Serialize};
pub use solana_program::pubkey::Pubkey;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use strum::Display;

use crate::{
    queue_type::{Binding, QueueProps, RetryProps},
    suffix::Suffix,
    Result,
};

/// Message data for an account update
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "UiAccountUpdate", from = "UiAccountUpdate")]
pub struct AccountUpdate {
    /// The account's public key
    pub key: Pubkey,
    /// The lamport balance of the account
    pub lamports: u64,
    /// The Solana program controlling this account
    pub owner: Pubkey,
    /// True if the account's data is an executable smart contract
    pub executable: bool,
    /// The next epoch for which this account will owe rent
    pub rent_epoch: u64,
    /// The binary data stored on this account
    pub data: Vec<u8>,
    /// Monotonic-increasing counter for sequencing on-chain writes
    pub write_version: u64,
    /// The slot in which this account was updated
    pub slot: u64,
    /// The block_time in which this account was updated
    /// this is not known during processing, but is filled out by confirmooor when slots confirm
    pub block_time: Option<i64>,
    /// True if this update was triggered by a validator startup
    pub is_startup: bool,
    /// First signature of the transaction caused this account modification
    pub txn_signature: Option<String>,
}

#[allow(clippy::from_over_into)]
impl Into<UiAccountUpdate> for AccountUpdate {
    fn into(self) -> UiAccountUpdate {
        UiAccountUpdate {
            key: self.key.to_string(),
            lamports: self.lamports,
            owner: self.owner.to_string(),
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data: base64::encode(self.data),
            write_version: self.write_version,
            slot: self.slot,
            block_time: self.block_time,
            is_startup: self.is_startup,
            txn_signature: self.txn_signature,
        }
    }
}

impl From<UiAccountUpdate> for AccountUpdate {
    fn from(ui: UiAccountUpdate) -> Self {
        Self {
            key: ui.key.parse().unwrap(),
            lamports: ui.lamports,
            owner: ui.owner.parse().unwrap(),
            executable: ui.executable,
            rent_epoch: ui.rent_epoch,
            data: base64::decode(ui.data).unwrap(),
            write_version: ui.write_version,
            slot: ui.slot,
            block_time: ui.block_time,
            is_startup: ui.is_startup,
            txn_signature: ui.txn_signature,
        }
    }
}

/// json sserialized version of accountupdate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiAccountUpdate {
    /// The account's public key
    pub key: String,
    /// The lamport balance of the account
    pub lamports: u64,
    /// The Solana program controlling this account
    pub owner: String,
    /// True if the account's data is an executable smart contract
    pub executable: bool,
    /// The next epoch for which this account will owe rent
    pub rent_epoch: u64,
    /// The binary data stored on this account
    pub data: String,
    /// Monotonic-increasing counter for sequencing on-chain writes
    pub write_version: u64,
    /// The slot in which this account was updated
    pub slot: u64,
    /// The block_time in which this account was updated
    /// this is not known during processing, but is filled out by confirmooor when slots confirm
    pub block_time: Option<i64>,
    /// True if this update was triggered by a validator startup
    pub is_startup: bool,
    /// First signature of the transaction caused this account modification
    pub txn_signature: Option<String>,
}

/// Message data for an instruction notification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "UiInstructionNotify", from = "UiInstructionNotify")]
pub struct InstructionNotify {
    /// The program this instruction was executed with
    pub program: Pubkey,
    /// The binary instruction opcode
    pub data: Vec<u8>,
    /// The account inputs to this instruction
    pub accounts: Vec<Pubkey>,
    /// The slot in which the transaction including this instruction was
    /// reported
    pub slot: u64,
    /// The block_time in which this account was updated
    /// this is not known during processing, but is filled out by confirmooor when slots confirm
    pub block_time: Option<i64>,
}

#[allow(clippy::from_over_into)]
impl Into<UiInstructionNotify> for InstructionNotify {
    fn into(self) -> UiInstructionNotify {
        UiInstructionNotify {
            program: self.program.to_string(),
            data: base64::encode(self.data),
            accounts: self
                .accounts
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            slot: self.slot,
            block_time: self.block_time,
        }
    }
}

impl From<UiInstructionNotify> for InstructionNotify {
    fn from(ui: UiInstructionNotify) -> Self {
        Self {
            program: ui.program.parse().unwrap(),
            data: base64::decode(ui.data).unwrap(),
            accounts: ui.accounts.iter().map(|a| a.parse().unwrap()).collect(),
            slot: ui.slot,
            block_time: ui.block_time,
        }
    }
}

/// json sserialized version of `InstructionNotify`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiInstructionNotify {
    /// The program this instruction was executed with
    pub program: String,
    /// The binary instruction opcode
    pub data: String,
    /// The account inputs to this instruction
    pub accounts: Vec<String>,
    /// The slot in which the transaction including this instruction was
    /// reported
    pub slot: u64,
    /// the time of the block
    /// this is not known during processing, but is filled out by confirmooor when slots confirm
    pub block_time: Option<i64>,
}

/// Message data for an instruction notification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransactionNotify {
    /// the transactions
    pub transaction: EncodedConfirmedTransactionWithStatusMeta,
}

/// Message data for an block metadata notification
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BlockMetadataNotify {
    /// the slot of the block
    pub slot: u64,
    /// the hash of the block
    pub blockhash: String,
    /// the time of the block
    pub block_time: i64,
    /// the height of the block
    pub block_height: u64,
}

/// Message data for an block metadata notification
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SlotStatusNotify {
    /// the slot
    pub slot: u64,
    /// the parent of the slot
    pub parent: Option<u64>,
    /// the status
    pub status: SlotStatus,
}

/// The current status of a slot
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Display)]
#[serde(rename_all = "camelCase")]
pub enum SlotStatus {
    /// The highest slot of the heaviest fork processed by the node. Ledger state at this slot is
    /// not derived from a confirmed or finalized block, but if multiple forks are present, is from
    /// the fork the validator believes is most likely to finalize.
    Processed,

    /// The highest slot having reached max vote lockout.
    Rooted,

    /// The highest slot that has been voted on by supermajority of the cluster, ie. is confirmed.
    Confirmed,
}

///statistics for a slot
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SlotStatistics {
    ///the slot these stats are for
    pub slot: u64,
    ///the blocktime of the slot
    /// this is not known for processing, but is later used in confirmooor
    pub block_time: Option<i64>,

    ///count of successful txs
    pub tx_success: u64,
    ///sum of the fees for successful txs
    pub tx_success_fees: u64,
    ///sum of the fees for successful txs with priority
    pub tx_success_fees_priority: u64,

    ///count of failed txs
    pub tx_err: u64,
    ///sum of the fees for failed txs
    pub tx_err_fees: u64,
    ///sum of the fees for failed txs with priority
    pub tx_err_fees_priority: u64,

    ///count of vote txs
    pub tx_vote: u64,
    ///sum of the fees for vote txs
    pub tx_vote_fees: u64,

    ///count of failed vote txs
    pub tx_vote_err: u64,
    ///sum of the fees for failed vote txs
    pub tx_vote_err_fees: u64,

    ///a map of programs to their stats
    pub programs: HashMap<String, ProgramStats>,
    ///a set of distinct payers
    pub payers: HashSet<String>,
    ///count of token accounts that were created by mint
    pub new_token_accounts: HashMap<String, u64>,
    ///count of fungible mints that were created
    pub mints_fungible_new: u64,
    ///count of non-fungible mints that were created
    pub mints_nonfungible_new: u64,

    ///random info about the slot or surrounding slots
    ///allows sending some debug or interesting info from the stats processor
    pub info: Vec<String>,
}

impl SlotStatistics {
    ///clear the stats (set all 0)
    pub fn clear(&mut self) {
        self.slot = 0;
        self.block_time = None;
        self.tx_success = 0;
        self.tx_success_fees = 0;
        self.tx_success_fees_priority = 0;
        self.tx_err = 0;
        self.tx_err_fees = 0;
        self.tx_err_fees_priority = 0;
        self.tx_vote = 0;
        self.tx_vote_fees = 0;
        self.tx_vote_err = 0;
        self.tx_vote_err_fees = 0;
        self.programs.clear();
        self.payers.clear();
        self.new_token_accounts.clear();
        self.mints_fungible_new = 0;
        self.mints_nonfungible_new = 0;
        self.info.clear();
    }
}

///stats by program
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
pub struct ProgramStats {
    ///count of successful txs
    pub success: u64,
    ///count of failed txs
    pub failed: u64,
}

/// A message transmitted by a Geyser plugin
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Message {
    /// Indicates an account should be updated
    AccountUpdate(AccountUpdate),
    /// Indicates an instruction was included in a **successful** transaction
    InstructionNotify(InstructionNotify),
    /// Indicates an instruction was included in a **successful** transaction
    TransactionNotify(Box<TransactionNotify>),
    /// indicates a block meta data has become available
    BlockMetadataNotify(BlockMetadataNotify),
    /// indicates the status of a slot has changed
    SlotStatusNotify(SlotStatusNotify),
    /// statistics for a slot are available
    SlotStatisticsNotify(SlotStatistics),
}

/// AMQP configuration for Geyser plugins
#[derive(Debug, Clone)]
pub struct QueueType {
    props: QueueProps,
}

/// Network hint for declaring exchange and queue names
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Network {
    /// Use the network ID `"mainnet"`
    Mainnet,
    /// Use the network ID `"devnet"`
    Devnet,
    /// Use the network ID `"testnet"`
    Testnet,
}

/// Startup message hint for declaring exchanges and queues
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum StartupType {
    /// Ignore startup messages
    Normal,
    /// Ignore non-startup messages
    Startup,
    /// Include all messages
    All,
}

/// Committment levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum CommittmentLevel {
    /// seen
    Processed,
    /// optimistic confirmation
    Confirmed,
    /// finalized
    Rooted,
}

impl StartupType {
    /// Construct a [`StartupType`] from the Geyser plugin `startup` filter.
    #[must_use]
    pub fn new(value: Option<bool>) -> Self {
        match value {
            None => Self::All,
            Some(false) => Self::Normal,
            Some(true) => Self::Startup,
        }
    }
}

impl QueueType {
    /// Construct a new queue configuration given the network this validator is
    /// connected to and queue suffix configuration
    ///
    /// # Errors
    /// This function fails if the given queue suffix is invalid.
    pub fn new(
        network: Network,
        startup_type: StartupType,
        exchange_suffix: &Suffix,
        queue_suffix: &Suffix,
        confirm_level: CommittmentLevel,
        routing_key: String,
    ) -> Result<Self> {
        let base_name = format!(
            "{}{}.{}.messages",
            network,
            match startup_type {
                StartupType::Normal => "",
                StartupType::Startup => ".startup",
                StartupType::All => ".startup-all",
            },
            confirm_level,
        );
        let exchange = exchange_suffix.format(base_name.clone())?;
        let queue = queue_suffix.format(base_name)?;

        Ok(Self {
            props: QueueProps {
                exchange,
                queue,
                binding: Binding::Topic(routing_key),
                prefetch: 32_768,
                max_len_bytes: if queue_suffix.is_debug() {
                    100 * 1024 * 1024 // 100 MiB
                } else {
                    8 * 1024 * 1024 * 1024 // 8 GiB
                },
                auto_delete: queue_suffix.is_debug(),
                retry: Some(RetryProps {
                    max_tries: 3,
                    delay_hint: Duration::from_millis(500),
                    max_delay: Duration::from_secs(10 * 60),
                }),
            },
        })
    }
}

impl crate::QueueType for QueueType {
    type Message = Message;

    #[inline]
    fn info(&self) -> crate::queue_type::QueueInfo {
        (&self.props).into()
    }
}

/// The type of a Geyser producer
#[cfg(feature = "producer")]
pub type Producer = crate::producer::Producer<QueueType>;
/// The type of a Geyser consumer
#[cfg(feature = "consumer")]
pub type Consumer = crate::consumer::Consumer<QueueType>;
