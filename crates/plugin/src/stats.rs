use std::sync::{Arc, mpsc};

use indexer_rabbitmq::geyser::{SlotStatistics, Message};
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
            };
            while let Ok(req) = rx.recv() {
                if let Err(e) = stats.process(req.slot, req.stx, req.meta, req.is_vote, req.is_err) {
                    error!("Stats processing error: {:?}", e);
                }
            };
        });
        tx
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
        //happy, fast path
        if slot == self.most_recent_slot_stats.0 {
            //quickly update stats
            return process_slot(self.get_most_recent_slot_stats(), stx, meta, is_vote, is_err);
        }
        //try and find slot in buffer
        let idx = (slot % SLOT_BUFFER_SIZE as u64) as usize;
        let buffer_slot_stats = &mut self.slot_stats[idx];

        //decent path; found by index
        if buffer_slot_stats.slot == slot {
            //update stats
            return process_slot(buffer_slot_stats, stx, meta, is_vote, is_err);
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
            self.send_stats(slot)?;

            //now get stats for current slot (should have been reset by send_stats)
            let new_stats = &mut self.slot_stats[idx];
            process_slot(new_stats, stx, meta, is_vote, is_err)?;

            //assign to most recent if applicable
            if slot > self.most_recent_slot_stats.0 {
                self.most_recent_slot_stats = (slot, idx);
            }

            return Ok(());
        }
    }

    #[inline]
    fn get_most_recent_slot_stats(&mut self) -> &mut SlotStatistics {
        &mut self.slot_stats[self.most_recent_slot_stats.1]
    }

    ///send stats for slots that are >= SLOT_BUFFER_SIZE slots behind
    ///this clears out needed slots for new stats
    #[inline]
    fn send_stats(&mut self, slot: u64) -> anyhow::Result<()> {
        let mut stats_to_send = Vec::<SlotStatistics>::new();
        let oldest_slot_not_allowed = slot - SLOT_BUFFER_SIZE as u64;
        for i in 0..SLOT_BUFFER_SIZE {
            let processing_slot = self.slot_stats[i].slot;
            if processing_slot > 0 && processing_slot <= oldest_slot_not_allowed {
                //send stats
                let stats_clone = self.slot_stats[i].clone();
                stats_to_send.push(stats_clone);

                //clear existing 
                self.slot_stats[i] = Default::default();
            }
        }
        let producer = self.producer.clone();
        self.rt.spawn(async move {
            for stats in stats_to_send {
                let stats_msg = Message::SlotStatisticsNotify(stats);
                producer.send(stats_msg, "multi.chain.slot_statistics").await;
            }
        });
        Ok(())
    }

}

#[inline]
fn process_slot(
    stats: &mut SlotStatistics,
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
    Ok(())
}