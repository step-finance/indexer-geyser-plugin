use std::sync::Arc;

use hashbrown::HashMap;
use itertools::Itertools;
use solana_sdk::transaction::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::{
    config::Transactions,
    // plugin::TOKEN_KEY,
    prelude::*,
};

#[derive(Debug)]
pub struct TransactionSelector {
    programs: HashMap<Pubkey, Arc<String>>,
    pubkeys: HashMap<Pubkey, Arc<String>>,
    /// K = routing prefix, V = allows all?
    allows_all: HashMap<String, bool>,
    multi_routing_key: Arc<String>,
}

impl TransactionSelector {
    pub fn from_config(config: Transactions) -> Result<Self> {
        let Transactions { programs, pubkeys } = config;

        let programs = programs
            .into_iter()
            .filter_map(|s| {
                s.0.parse()
                    .map(|a| (a, Arc::new(format!("{}.transaction", s.1))))
                    .ok()
            })
            .collect::<_>();

        let pubkeys = pubkeys
            .into_iter()
            .map(|s| {
                s.0.parse()
                    .map(|a| (a, Arc::new(format!("{}.transaction", s.1))))
            })
            .collect::<Result<_, _>>()
            .context("Failed to parse tx pubkeys")?;

        Ok(Self {
            programs,
            pubkeys,
            multi_routing_key: Arc::new("multi.transaction".to_string()),
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.programs.is_empty() && self.pubkeys.is_empty()
    }

    #[inline]
    pub fn get_route(
        &self,
        tx: &SanitizedTransaction,
        meta: &TransactionStatusMeta,
    ) -> Option<&Arc<String>> {
        let msg = tx.message();
        let keys = msg.account_keys();

        let pubkey_routes = keys
            .iter()
            .filter_map(|a| self.pubkeys.get(a))
            .unique()
            .collect::<Vec<_>>();
        if pubkey_routes.len() > 1 {
            return Some(&self.multi_routing_key);
        }

        //check programs
        let program_routes = msg
            .instructions()
            .iter()
            .chain(
                meta.inner_instructions
                    .iter()
                    .flatten()
                    .flat_map(|ii| ii.instructions.iter().map(|i| &i.instruction)),
            )
            .map(|a| a.program_id_index.into())
            .unique()
            .filter_map(|a| keys.get(a))
            .filter_map(|a| self.programs.get(a))
            .unique()
            .take(2); //if > 1 then we use multi anyhow

        let mut routes = pubkey_routes.into_iter().chain(program_routes).unique();
        let first = routes.next();
        first?;
        let second = routes.next();
        if second.is_none() {
            first
        } else {
            Some(&self.multi_routing_key)
        }
    }
}
