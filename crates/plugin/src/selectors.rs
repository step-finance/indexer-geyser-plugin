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
    /// K = Program, V = routing_key
    programs: HashMap<Pubkey, Arc<String>>,
    /// K = Program, V = routing_key
    pubkeys: HashMap<Pubkey, Arc<String>>,
    /// Routing prefixes that support routing ALL programs
    allows_all_programs: Vec<Arc<String>>,
    multi_routing_key: Arc<String>,
}

impl TransactionSelector {
    pub fn from_config(config: Transactions) -> Result<Self> {
        let Transactions { programs, pubkeys } = config;

        let allows_all_programs = programs
            .iter()
            .filter_map(|(pk, rk)| if pk == "*" { Some(rk) } else { None })
            .map(|s| Self::make_routing_key(s))
            .collect();

        let programs = programs
            .into_iter()
            .map(|s| s.0.parse().map(|a| (a, Self::make_routing_key(&s.1))))
            .collect::<Result<_, _>>()
            .context("Failed to parse tx program keys")?;

        let pubkeys = pubkeys
            .into_iter()
            .map(|s| s.0.parse().map(|a| (a, Self::make_routing_key(&s.1))))
            .collect::<Result<_, _>>()
            .context("Failed to parse tx pubkeys")?;

        Ok(Self {
            programs,
            pubkeys,
            allows_all_programs,
            multi_routing_key: Arc::new("multi.transaction".to_string()),
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.programs.is_empty() && self.pubkeys.is_empty()
    }

    #[inline]
    fn make_routing_key(s: &str) -> Arc<String> {
        Arc::new(format!("{s}.transaction"))
    }

    #[inline]
    pub fn get_route(
        &self,
        tx: &SanitizedTransaction,
        meta: &TransactionStatusMeta,
    ) -> Option<&Arc<String>> {

        //we do not care about votes, for now.
        //technically this makes our sol balance
        //tracking for voting accounts incorrect
        if tx.is_simple_vote_transaction() {
            return None;
        }

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
            .chain(self.allows_all_programs.iter())
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
