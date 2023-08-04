use std::collections::HashSet;
use std::sync::{Arc, mpsc};

use indexer_rabbitmq::geyser::{SlotStatistics, Message};
use solana_program::instruction::CompiledInstruction;
use solana_sdk::transaction::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;
use crate::prelude::*;

use crate::sender::Sender;

const STAT_REQ_BUFFER_SIZE: usize = 2500;
const SLOT_BUFFER_SIZE: usize = 25;
const LAMPORTS_PER_SIG: u64 = 5000;

#[derive(Debug)]
pub(crate) struct StatsRequest {
    pub slot: u64,
    pub stx: SanitizedTransaction,
    pub meta: TransactionStatusMeta,
    pub is_vote: bool,
    pub is_err: bool,
}

#[derive(Debug)]
pub(crate) struct Stats {
    most_recent_slot_stats: (u64, usize),
    slot_stats: [SlotStatistics; SLOT_BUFFER_SIZE],
    producer: Arc<Sender>,
    rt: Arc<tokio::runtime::Runtime>,
    token_programs: HashSet<Pubkey>,
}

impl Stats {
    pub fn create_publisher(producer: Arc<Sender>, rt: Arc<tokio::runtime::Runtime>) -> mpsc::SyncSender<StatsRequest> {
        let (tx, rx) = mpsc::sync_channel::<StatsRequest>(STAT_REQ_BUFFER_SIZE);
        rt.clone().spawn_blocking( move || {
            let mut stats = Stats {
                most_recent_slot_stats: Default::default(),
                slot_stats: std::iter::repeat::<SlotStatistics>(Default::default()).take(SLOT_BUFFER_SIZE).collect::<Vec<SlotStatistics>>().try_into().unwrap(),
                producer,
                rt,
                token_programs: Self::get_token_programs(),
            };
            while let Ok(req) = rx.recv() {
                if let Err(e) = stats.process(req.slot, req.stx, req.meta, req.is_vote, req.is_err) {
                    error!("Stats processing error: {:?}", e);
                }
            };
        });
        tx
    }

    fn get_token_programs() -> HashSet<Pubkey> {
        let mut set = HashSet::<Pubkey>::new();
        set.insert(Pubkey::try_from("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap());
        set.insert(Pubkey::try_from("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap());
        set
    }

    //handle stats msg
    fn process(
        &mut self,
        slot: u64,
        stx: SanitizedTransaction,
        meta: TransactionStatusMeta,
        is_vote: bool,
        is_err: bool,
    ) -> anyhow::Result<()> {
        let Stats {
            token_programs,
            most_recent_slot_stats,
            slot_stats,
            producer,
            rt,
            ..
        } = self;
        //happy, fast path
        if slot == most_recent_slot_stats.0 {
            //quickly update stats
            let most_recent_stats = &mut slot_stats[most_recent_slot_stats.1];
            return process_slot(most_recent_stats, token_programs, stx, meta, is_vote, is_err);
        }
        //try and find slot in buffer
        let idx = (slot % SLOT_BUFFER_SIZE as u64) as usize;
        let buffer_slot_stats = &mut slot_stats[idx];

        //decent path; found by index
        if buffer_slot_stats.slot == slot {
            //update stats
            return process_slot(buffer_slot_stats, &token_programs, stx, meta, is_vote, is_err);
        }
        //else
        //stats in buffer doesn't match slot
        if buffer_slot_stats.slot > slot {
            //slot is too old, discard
            
            return Ok(());
        } else {
            //slot is newer than whats in the buffer
            //thus there must be at least 1 slot to send
            
            //send any stats that are >= SLOT_BUFFER_SIZE slots behind
            //this needs to account for skipping slots
            //so it has to scan array
            send_stats(slot_stats, slot, producer, rt)?;

            //now get stats for current slot (should have been reset by send_stats)
            let new_stats = &mut slot_stats[idx];
            new_stats.slot = slot;
            process_slot(new_stats, &token_programs, stx, meta, is_vote, is_err)?;

            //assign to most recent if applicable
            if slot > most_recent_slot_stats.0 {
                *most_recent_slot_stats = (slot, idx);
            }

            return Ok(());
        }
    }

}

///send stats for slots that are >= SLOT_BUFFER_SIZE slots behind
///this clears out needed slots for new stats
#[inline]
fn send_stats(slot_stats: &mut [SlotStatistics; SLOT_BUFFER_SIZE], slot: u64, producer: &Arc<Sender>, rt: &Arc<tokio::runtime::Runtime>) -> anyhow::Result<()> {
    let mut stats_to_send = Vec::<SlotStatistics>::with_capacity(4);
    let oldest_slot_not_allowed = slot - SLOT_BUFFER_SIZE as u64;
    for i in 0..SLOT_BUFFER_SIZE {
        let processing_slot = slot_stats[i].slot;
        if processing_slot > 0 && processing_slot <= oldest_slot_not_allowed {
            //send stats
            let stats_clone = slot_stats[i].clone();
            stats_to_send.push(stats_clone);

            //clear existing 
            slot_stats[i] = Default::default();
        }
    }
    let producer = producer.clone();
    rt.spawn(async move {
        for stats in stats_to_send {
            let stats_msg = Message::SlotStatisticsNotify(stats);
            producer.send(stats_msg, "multi.chain.slot_statistics").await;
        }
    });
    Ok(())
}

#[inline]
fn process_slot(
    stats: &mut SlotStatistics,
    token_programs: &HashSet<Pubkey>,
    stx: SanitizedTransaction,
    meta: TransactionStatusMeta,
    is_vote: bool,
    is_err: bool,
) -> anyhow::Result<()> {
    
    let fee = LAMPORTS_PER_SIG * stx.signatures().len() as u64;
    let fee_priority = meta.fee - fee; 

    if is_vote && is_err {
        stats.tx_vote_err += 1;
        stats.tx_vote_err_fees += fee;
    } else {
        if is_vote {
            stats.tx_vote += 1;
            stats.tx_vote_fees += fee;
        }  else if is_err {
            stats.tx_err += 1;
            stats.tx_err_fees += fee;
            stats.tx_err_fees_priority += fee_priority;
        } else {
            stats.tx_success += 1;
            stats.tx_success_fees += fee;
            stats.tx_success_fees_priority += fee_priority;
        }
    }

    let msg = stx.message();
    let accts = msg.account_keys();
    let inner_ixs: Vec<(&Pubkey, &CompiledInstruction)> = meta.inner_instructions.iter().flat_map(|ixss| {
        ixss.into_iter().flat_map(|ixs| {
            ixs.instructions.iter().map(|ix| {
                (&accts[ix.program_id_index as usize], ix)
            })
        })
    }).collect();
    let all_ixs = msg.program_instructions_iter().into_iter().chain(inner_ixs);
    for (pgm_ref, ix_ref) in all_ixs {
        let e = stats.programs.entry(pgm_ref.to_string()).or_default();
        if is_err {
            e.failed += 1;
        } else {
            e.success += 1;

            let data_len = ix_ref.data.len();

            if token_programs.contains(&pgm_ref) && data_len > 0 {
                let disc = ix_ref.data[0];
                match disc {
                    //initialize mint
                    0 | 20 => {
                        if data_len > 1 {
                            let dec = ix_ref.data[1];
                            if dec == 0 {
                                stats.mints_nonfungible_new += 1;
                            } else {
                                stats.mints_fungible_new += 1;
                            }
                        }
                    }
                    //initialize account
                    1 | 16 | 18 => {
                        if ix_ref.accounts.len() > 1 {
                            let mint = accts[ix_ref.accounts[1] as usize].to_string();
                            let val = stats.new_token_accounts.entry(mint).or_default();
                            *val += 1;
                        }
                    },
                    _ => {}
                }
            }
        }
    }

    stats.payers.insert(msg.fee_payer().to_string());

    Ok(())
}