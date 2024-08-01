use hashbrown::HashMap;
use serde::Deserialize;
use veil::Redact;

use crate::{prelude::*, selectors::TransactionSelector};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    amqp: Amqp,
    jobs: Jobs,

    #[serde(default)]
    metrics: Metrics,

    #[serde(default)]
    chain_progress: ChainProgress,

    transactions: Transactions,

    /// Unused but required by the validator to load the plugin
    #[allow(dead_code)]
    libpath: String,

    /// Unused here but is in validator
    #[allow(dead_code)]
    datum_program_inclusions: Option<HashMap<String, DatumInclusion>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatumInclusion {
    #[allow(dead_code)]
    pre: Option<bool>,
    #[allow(dead_code)]
    post: Option<bool>,
    #[allow(dead_code)]
    length_exclusions: Option<Vec<usize>>,
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

    pub fn into_parts(self) -> Result<(Amqp, Jobs, Metrics, ChainProgress, TransactionSelector)> {
        let Self {
            amqp,
            jobs,
            metrics,
            chain_progress,
            transactions,
            libpath: _,
            datum_program_inclusions: _,
        } = self;

        let txs = TransactionSelector::from_config(transactions)
            .context("Failed to create instruction selector")?;

        Ok((amqp, jobs, metrics, chain_progress, txs))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use solana_program::pubkey::Pubkey;
    use std::{fs, path::PathBuf};

    use super::Config;

    fn validate_config(path: PathBuf) -> Result<()> {
        let file = fs::read(path)?;
        let json = serde_json::from_slice::<Config>(&file)?;
        for (key, _) in &json.datum_program_inclusions.unwrap_or_default() {
            key.parse::<Pubkey>()?;
        }
        Ok(())
    }

    #[test]
    fn validate_staging_config() -> Result<()> {
        validate_config("./triton_config_dev.json".into())
    }

    #[test]
    fn validate_live_config() -> Result<()> {
        validate_config("./triton_config.json".into())
    }
}
