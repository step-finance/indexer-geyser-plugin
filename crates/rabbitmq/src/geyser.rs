//! Queue configuration for Solana Geyser plugins intended to communicate
//! with `holaplex-indexer`.

use std::time::Duration;

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
#[serde(into = "UiAccountUpdate")]
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
    /// True if this update was triggered by a validator startup
    pub is_startup: bool,
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
            is_startup: self.is_startup,
        }
    }
}

/// json sserialized version of accountupdate
#[derive(Debug, Clone, Serialize)]
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
    /// True if this update was triggered by a validator startup
    pub is_startup: bool,
}

/// Message data for an instruction notification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    /// indeicates the status of a slot has changed
    SlotStatusNotify(SlotStatusNotify),
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
                prefetch: 16_384,
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
