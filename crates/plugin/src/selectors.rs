use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use indexer_rabbitmq::geyser::StartupType;
use itertools::Itertools;
use solana_program::instruction::CompiledInstruction;
use solana_sdk::transaction::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::{
    config::{Accounts, Instructions, Transactions},
    // plugin::TOKEN_KEY,
    prelude::*,
};

#[derive(Debug)]
pub struct AccountSelector {
    owners: HashMap<[u8; 32], Arc<String>>,
    pubkeys: HashMap<[u8; 32], Arc<String>>,
    startup: Option<bool>,
    multi_routing_key: Arc<String>,
    token_addresses: Option<HashSet<Pubkey>>,
}

impl AccountSelector {
    pub fn from_config(config: Accounts) -> Result<Self> {
        let Accounts {
            owners,
            all_tokens,
            pubkeys,
            startup,
        } = config;

        let owners = owners
            .into_iter()
            .map(|s| {
                s.0.parse()
                    .map(Pubkey::to_bytes)
                    .map(|a| (a, Arc::new(format!("{}.account", s.1))))
            })
            .collect::<Result<_, _>>()
            .context("Failed to parse account owner keys")?;

        let pubkeys = pubkeys
            .into_iter()
            .map(|s| {
                s.0.parse()
                    .map(Pubkey::to_bytes)
                    .map(|a| (a, Arc::new(format!("{}.account", s.1))))
            })
            .collect::<Result<_, _>>()
            .context("Failed to parse account pubkeys")?;

        Ok(Self {
            owners,
            pubkeys,
            startup,
            multi_routing_key: Arc::new("multi.account".to_string()),
            token_addresses: if all_tokens {
                None
            } else {
                Some(HashSet::new())
            },
        })
    }

    /// Lazy-load the token addresses.  Fails if token addresses are not wanted
    /// or if they have already been loaded.
    pub fn init_tokens(&mut self, addrs: HashSet<Pubkey>) {
        assert!(self.token_addresses.as_ref().unwrap().is_empty());
        self.token_addresses = Some(addrs);
    }

    #[inline]
    pub fn startup(&self) -> StartupType {
        StartupType::new(self.startup)
    }

    #[inline]
    pub fn screen_tokens(&self) -> bool {
        self.token_addresses.is_some()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.owners.is_empty() && self.pubkeys.is_empty() && self.token_addresses.is_none()
    }

    #[inline]
    pub fn get_route(
        &self,
        acct_owner: &[u8],
        acct_pubkey: &[u8],
        is_startup: bool,
    ) -> Option<&Arc<String>> {
        if !self.startup.unwrap_or(false) && is_startup {
            return None;
        }

        match (self.owners.get(acct_owner), self.pubkeys.get(acct_pubkey)) {
            (Some(route), None) | (None, Some(route)) => Some(route),
            (Some(route1), Some(route2)) => {
                if route1 == route2 {
                    Some(route1)
                } else {
                    Some(&self.multi_routing_key)
                }
            },
            _ => None,
        }

        // commented out for step, we'll _probably_ never use this

        // if owner == TOKEN_KEY.as_ref() && data.len() == TokenAccount::get_packed_len() {
        //     if let Some(ref addrs) = self.token_addresses {
        //         let token_account = TokenAccount::unpack_from_slice(data);

        //         if let Ok(token_account) = token_account {
        //             if token_account.amount > 1 || addrs.contains(&token_account.mint) {
        //                 return false;
        //             }
        //         }
        //     }
        // }
    }
}

#[derive(Debug)]
pub struct InstructionSelector {
    pub programs: HashMap<Pubkey, Arc<String>>,
    // screen_tokens: bool,
}

impl InstructionSelector {
    pub fn from_config(config: Instructions) -> Result<Self> {
        let Instructions {
            programs,
            ..
            // all_token_calls,
        } = config;

        let programs = programs
            .into_iter()
            .map(|s| {
                s.0.parse()
                    .map(|a| (a, Arc::new(format!("{}.instruction", s.1))))
            })
            .collect::<Result<_, _>>()
            .context("Failed to parse instruction program keys")?;

        Ok(Self {
            programs,
            // screen_tokens: !all_token_calls,
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.programs.is_empty()
    }

    #[inline]
    pub fn get_route(&self, pgm: &Pubkey, _ins: &CompiledInstruction) -> Option<&Arc<String>> {
        self.programs.get(pgm)

        // if self.screen_tokens && *pgm == TOKEN_KEY {
        //     if let [8, rest @ ..] = ins.data.as_slice() {
        //         let amt = rest.try_into().map(u64::from_le_bytes);

        //         if !matches!(amt, Ok(1)) {
        //             return false;
        //         }

        //         debug_assert_eq!(
        //             ins.data,
        //             spl_token::instruction::TokenInstruction::Burn { amount: 1_u64 }.pack(),
        //         );
        //     } else {
        //         return false;
        //     }
        // }
    }
}

#[derive(Debug)]
pub struct TransactionSelector {
    programs: HashMap<Pubkey, Arc<String>>,
    pubkeys: HashMap<Pubkey, Arc<String>>,
    multi_routing_key: Arc<String>,
}

impl TransactionSelector {
    pub fn from_config(config: Transactions) -> Result<Self> {
        let Transactions { programs, pubkeys } = config;

        let programs = programs
            .into_iter()
            .map(|s| {
                s.0.parse()
                    .map(|a| (a, Arc::new(format!("{}.transaction", s.1))))
            })
            .collect::<Result<_, _>>()
            .context("Failed to parse tx program keys")?;

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
                    .flat_map(|ii| ii.instructions.iter()),
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
