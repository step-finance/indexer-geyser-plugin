use hashbrown::HashMap;
use serde::Deserialize;
use veil::Redact;

use crate::{
    prelude::*,
    selectors::{AccountSelector, InstructionSelector, TransactionSelector},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    amqp: Amqp,
    jobs: Jobs,

    #[serde(default)]
    metrics: Metrics,

    #[serde(default)]
    chain_progress: ChainProgress,

    accounts: Accounts,
    instructions: Instructions,
    transactions: Transactions,

    /// Unused but required by the validator to load the plugin
    #[allow(dead_code)]
    libpath: String,
}

#[serde_with::serde_as]
#[derive(Redact, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Amqp {
    #[redact(partial)]
    pub address: String,

    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub network: indexer_rabbitmq::geyser::Network,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Jobs {
    pub limit: usize,

    #[serde(default)]
    pub blocking: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Metrics {
    pub config: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChainProgress {
    pub block_meta: Option<bool>,
    pub slot_status: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Accounts {
    #[serde(default)]
    pub owners: HashMap<String, String>,

    #[serde(default)]
    pub pubkeys: HashMap<String, String>,

    /// Filter for changing how to interpret the `is_startup` flag.
    ///
    /// This option has three states:
    ///  - `None`: Ignore the `is_startup` flag and send all updates.
    ///  - `Some(true)`: Only send updates when `is_startup` is `true`.
    ///  - `Some(false)`: Only send updates when `is_startup` is `false`.
    #[serde(default)]
    pub startup: Option<bool>,

    /// Set to true to disable heuristics to reduce the number of incoming
    /// token account updates.  Has no effect if the spl-token pubkey is not in
    /// the owners list.
    #[serde(default)]
    pub all_tokens: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Instructions {
    #[serde(default)]
    pub programs: HashMap<String, String>,

    /// Set to true to disable heuristics to reduce the number of incoming
    /// token instructions.  Has no effect if the spl-token pubkey is not in the
    /// programs list.  Currently the heuristics are tailored towards NFT burns,
    /// only passing through instructions whose data indicates a burn of amount
    /// 1.
    #[serde(default)]
    pub all_token_calls: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Transactions {
    #[serde(default)]
    pub programs: HashMap<String, String>,
    #[serde(default)]
    pub pubkeys: HashMap<String, String>,
}

impl Config {
    pub fn read(path: &str) -> Result<Self> {
        let f = std::fs::File::open(path).context("Failed to open config file")?;
        let cfg = serde_json::from_reader(f).context("Failed to parse config file")?;

        log::info!("{:?}", cfg);

        Ok(cfg)
    }

    pub fn into_parts(
        self,
    ) -> Result<(
        Amqp,
        Jobs,
        Metrics,
        ChainProgress,
        AccountSelector,
        InstructionSelector,
        TransactionSelector,
    )> {
        let Self {
            amqp,
            jobs,
            metrics,
            chain_progress,
            accounts,
            instructions,
            transactions,
            libpath: _,
        } = self;

        let acct =
            AccountSelector::from_config(accounts).context("Failed to create account selector")?;
        let ins = InstructionSelector::from_config(instructions)
            .context("Failed to create instruction selector")?;
        let txs = TransactionSelector::from_config(transactions)
            .context("Failed to create instruction selector")?;

        Ok((amqp, jobs, metrics, chain_progress, acct, ins, txs))
    }
}
