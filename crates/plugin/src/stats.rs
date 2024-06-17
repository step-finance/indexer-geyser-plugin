use std::collections::HashSet;
use std::sync::{mpsc, Arc};

use crate::prelude::*;
use indexer_rabbitmq::geyser::{Message, SlotStatistics};
use solana_program::instruction::CompiledInstruction;
use solana_sdk::transaction::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;

use crate::sender::Sender;

const STAT_REQ_BUFFER_SIZE: usize = 2500;
const SLOT_BUFFER_SIZE: usize = 25;
const LAMPORTS_PER_SIG: u64 = 5000;

///the message sent to a producer to process
#[derive(Debug)]
pub(crate) struct StatsRequest {
    pub slot: u64,
    pub stx: SanitizedTransaction,
    pub meta: TransactionStatusMeta,
    pub is_vote: bool,
    pub is_err: bool,
}

///the main stats struct that holds everything the
///stats thread needs to do its job
#[derive(Debug)]
pub(crate) struct Stats {
    ///a quick shortcut into the slot_stats buffer for most recent slot
    ///this should allow a hot path of code execution
    #[allow(clippy::struct_field_names)]
    most_recent_slot_stats: (u64, usize),
    ///the buffer of slot stats; slots stats are sent to rabbitmq after they
    ///reach the size of the buffer. Thus, it's important to have the buffer large
    ///enough to catch trailing transactions, but small enough to send before
    ///the block is confirmed (so confirmooor can send it upon confirmation)
    #[allow(clippy::struct_field_names)]
    slot_stats: [SlotStatistics; SLOT_BUFFER_SIZE],
    ///the rabbitmq sender
    producer: Arc<Sender>,
    ///the tokio async runtime to use for sending messages
    rt: Arc<tokio::runtime::Runtime>,
    ///used to identify token program ixs
    token_programs: HashSet<Pubkey>,
}

impl Stats {
    /// Create a new stats thread and return a handle to send stats requests to it
    pub fn create_publisher(
        producer: Arc<Sender>,
        rt: Arc<tokio::runtime::Runtime>,
    ) -> mpsc::SyncSender<StatsRequest> {
        let (tx, rx) = mpsc::sync_channel::<StatsRequest>(STAT_REQ_BUFFER_SIZE);
        //we use a dedicated worker thread, we don't play in the async dancing sandbox
        //that the producer message sender uses
        rt.clone().spawn_blocking(move || {
            let mut stats = Stats {
                most_recent_slot_stats: Default::default(),
                slot_stats: std::iter::repeat::<SlotStatistics>(SlotStatistics::default())
                    .take(SLOT_BUFFER_SIZE)
                    .collect::<Vec<SlotStatistics>>()
                    .try_into()
                    .unwrap(),
                producer,
                rt,
                token_programs: Self::get_token_programs(),
            };
            //the thread's endless loop of message processing
            while let Ok(req) = rx.recv() {
                stats.process(req.slot, &req.stx, &req.meta, req.is_vote, req.is_err);
            }
        });
        //return the channel sender for the geyser thread to send messages to
        tx
    }

    fn get_token_programs() -> HashSet<Pubkey> {
        let mut set = HashSet::<Pubkey>::new();
        set.insert(Pubkey::try_from("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap());
        set.insert(Pubkey::try_from("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap());
        set
    }

    //handle stats msg
    #[allow(clippy::cast_possible_truncation)]
    fn process(
        &mut self,
        slot: u64,
        stx: &SanitizedTransaction,
        meta: &TransactionStatusMeta,
        is_vote: bool,
        is_err: bool,
    ) {
        //deconstruct the stats so we can borrow the peices mutably
        //independent of each other
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
            process_slot(
                most_recent_stats,
                token_programs,
                stx,
                meta,
                is_vote,
                is_err,
            );
            return;
        }
        //try and find slot in buffer
        let idx = (slot % SLOT_BUFFER_SIZE as u64) as usize;
        let buffer_slot_stats = &mut slot_stats[idx];

        //decent path; found by index
        if buffer_slot_stats.slot == slot {
            //update stats
            process_slot(
                buffer_slot_stats,
                token_programs,
                stx,
                meta,
                is_vote,
                is_err,
            );
            return;
        }
        //else
        //stats in buffer doesn't match slot
        if buffer_slot_stats.slot > slot {
            //throw some informational logging into the message that will be sent in
            //whatever slot this is that was in the place we expected to be
            //for debugging/informational purposes
            buffer_slot_stats
                .info
                .push(format!("tx for slot {slot} too old, discarding"));
            //slot is too old, discard
            return;
        }

        //slot is newer than whats in the buffer
        //thus there must be at least 1 slot to send
        //(definitely the one in our place in the buffer)

        //send any stats that are >= SLOT_BUFFER_SIZE slots behind
        //this needs to account for skipping slots
        //so it has to scan array
        send_stats(slot_stats, slot, producer, rt);

        //now get stats for current slot (should have been reset by send_stats)
        let new_stats = &mut slot_stats[idx];
        new_stats.slot = slot;
        process_slot(new_stats, token_programs, stx, meta, is_vote, is_err);

        //assign to most recent if applicable
        if slot > most_recent_slot_stats.0 {
            *most_recent_slot_stats = (slot, idx);
        }
    }
}

///send stats for slots that are >= `SLOT_BUFFER_SIZE` slots behind
///this clears out needed slots for new stats
#[inline]
fn send_stats(
    slot_stats: &mut [SlotStatistics; SLOT_BUFFER_SIZE],
    slot: u64,
    producer: &Arc<Sender>,
    rt: &Arc<tokio::runtime::Runtime>,
) {
    let mut stats_to_send = Vec::<SlotStatistics>::with_capacity(4);
    let oldest_slot_not_allowed = slot - SLOT_BUFFER_SIZE as u64;
    for slot_stat in slot_stats.iter_mut().take(SLOT_BUFFER_SIZE) {
        let processing_slot = slot_stat.slot;
        if processing_slot > 0 && processing_slot <= oldest_slot_not_allowed {
            //send stats
            let stats_clone = slot_stat.clone();
            stats_to_send.push(stats_clone);

            //clear existing
            slot_stat.clear();
        }
    }
    let producer = producer.clone();
    rt.spawn(async move {
        for stats in stats_to_send {
            let stats_msg = Message::SlotStatisticsNotify(stats);
            producer
                .send(stats_msg, "multi.chain.slot_statistics")
                .await;
        }
    });
}

///the main logic for updating stats
#[inline]
fn process_slot(
    stats: &mut SlotStatistics,
    token_programs: &HashSet<Pubkey>,
    stx: &SanitizedTransaction,
    meta: &TransactionStatusMeta,
    is_vote: bool,
    is_err: bool,
) {
    let fee = LAMPORTS_PER_SIG * stx.signatures().len() as u64;
    let fee_priority = meta.fee - fee;

    if is_vote && is_err {
        stats.tx_vote_err += 1;
        stats.tx_vote_err_fees += fee;
    } else if is_vote {
        stats.tx_vote += 1;
        stats.tx_vote_fees += fee;
    } else if is_err {
        stats.tx_err += 1;
        stats.tx_err_fees += fee;
        stats.tx_err_fees_priority += fee_priority;
    } else {
        stats.tx_success += 1;
        stats.tx_success_fees += fee;
        stats.tx_success_fees_priority += fee_priority;
    }

    let msg = stx.message();
    let accts = msg.account_keys();
    let inner_ixs: Vec<(&Pubkey, &CompiledInstruction)> = meta
        .inner_instructions
        .iter()
        .flat_map(|ixss| {
            ixss.iter().flat_map(|ixs| {
                ixs.instructions.iter().map(|ix| {
                    (
                        &accts[ix.instruction.program_id_index as usize],
                        &ix.instruction,
                    )
                })
            })
        })
        .collect();
    let all_ixs = msg.program_instructions_iter().chain(inner_ixs);
    for (pgm_ref, ix_ref) in all_ixs {
        let e = stats.programs.entry(pgm_ref.to_string()).or_default();
        if is_err {
            e.failed += 1;
        } else {
            e.success += 1;

            let data_len = ix_ref.data.len();

            if token_programs.contains(pgm_ref) && data_len > 0 {
                let disc = ix_ref.data[0];
                //reference:
                //  https://docs.rs/spl-token-2022/0.7.0/src/spl_token_2022/instruction.rs.html
                //  https://docs.rs/spl-token/4.0.0/src/spl_token/instruction.rs.html
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
                    },
                    //initialize account
                    1 | 16 | 18 => {
                        if ix_ref.accounts.len() > 1 {
                            let mint = accts[ix_ref.accounts[1] as usize].to_string();
                            let val = stats.new_token_accounts.entry(mint).or_default();
                            *val += 1;
                        }
                    },
                    _ => {},
                }
            }
        }
    }

    stats.payers.insert(msg.fee_payer().to_string());
}
