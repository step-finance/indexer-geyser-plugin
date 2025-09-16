use hashbrown::HashMap;
use itertools::Itertools;
use solana_transaction::{versioned::VersionedTransaction, VersionedMessage};
use solana_transaction_status::TransactionStatusMeta;

use crate::{
    config::Transactions,
    // plugin::TOKEN_KEY,
    prelude::*,
};

#[derive(Debug)]
pub struct TransactionSelector {
    /// K = Program, V = `routing_key`
    programs: HashMap<Pubkey, String>,
    /// K = Program, V = `routing_key`
    pubkeys: HashMap<Pubkey, String>,
    /// Routing prefixes that support routing ALL programs
    allows_all_programs: Vec<String>,
    num_shards: u64,
}

impl TransactionSelector {
    pub fn from_config(config: Transactions, num_shards: u64) -> Result<Self> {
        let Transactions { programs, pubkeys } = config;

        let allows_all_programs = programs
            .iter()
            .filter_map(|(pk, rk)| if pk == "*" { Some(rk) } else { None })
            .map(|s| Self::make_routing_key(s))
            .collect();

        let programs = programs
            .into_iter()
            .filter_map(|s| s.0.parse().map(|a| (a, Self::make_routing_key(&s.1))).ok())
            .collect::<HashMap<_, _>>();

        let pubkeys = pubkeys
            .into_iter()
            .map(|s| s.0.parse().map(|a| (a, Self::make_routing_key(&s.1))))
            .collect::<Result<_, _>>()
            .context("Failed to parse tx pubkeys")?;

        Ok(Self {
            programs,
            pubkeys,
            allows_all_programs,
            num_shards,
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.programs.is_empty() && self.pubkeys.is_empty() && self.allows_all_programs.is_empty()
    }

    #[inline]
    fn make_routing_key(s: &str) -> String {
        format!("{s}.transaction")
    }

    #[inline]
    fn make_multi_routing_key(slot: u64, num_shards: u64) -> String {
        let shard = slot % num_shards;
        format!("multi.transaction.{shard}")
    }

    #[inline]
    pub fn get_route(
        &self,
        tx: &VersionedTransaction,
        meta: &TransactionStatusMeta,
        slot: u64,
        // votes should never make it here but do a sanity check
        is_vote: bool,
    ) -> Option<String> {
        //we do not care about votes, for now.
        //technically this makes our sol balance
        //tracking for voting accounts incorrect
        if is_vote {
            return None;
        }

        let instructions;
        let keys;
        match &tx.message {
            VersionedMessage::Legacy(msg) => {
                keys = &msg.account_keys;
                instructions = &msg.instructions;
            },
            VersionedMessage::V0(msg) => {
                keys = &msg.account_keys;
                instructions = &msg.instructions;
            },
        }

        let pubkey_routes = keys
            .iter()
            .filter_map(|a| self.pubkeys.get(a))
            .unique()
            .collect::<Vec<_>>();
        if pubkey_routes.len() > 1 {
            return Some(Self::make_multi_routing_key(slot, self.num_shards));
        }

        //check programs
        let program_routes = instructions
            .iter()
            .chain(
                meta.inner_instructions
                    .iter()
                    .flatten()
                    .flat_map(|ii| ii.instructions.iter().map(|i| &i.instruction)),
            )
            .map(|a| a.program_id_index)
            .unique()
            .filter_map(|a| Some(keys[a as usize]))
            .filter_map(|a| self.programs.get(&a))
            .chain(self.allows_all_programs.iter())
            .unique()
            .take(2); //if > 1 then we use multi anyhow

        let mut routes = pubkey_routes.into_iter().chain(program_routes).unique();
        let first = routes.next()?;
        let second = routes.next();
        if second.is_none() {
            let shard = slot % self.num_shards;
            Some(format!("{first}.{shard}"))
        } else {
            Some(Self::make_multi_routing_key(slot, self.num_shards))
        }
    }
}
