use std::{
    env,
    sync::{mpsc, Arc},
};

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaBlockInfoVersions, SlotStatus,
};
use indexer_rabbitmq::geyser::{
    BlockMetadataNotify, Message, SlotStatusNotify, StartupType, TransactionNotify,
};

// pub(crate) static TOKEN_KEY: Pubkey =
//     solana_program::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

use solana_sdk::transaction::SanitizedTransaction;

use solana_transaction_status::{
    ConfirmedTransactionWithStatusMeta, TransactionStatusMeta, TransactionWithStatusMeta,
    UiTransactionEncoding, VersionedTransactionWithStatusMeta,
};

use crate::{
    async_utils::run_future_on_new_thread,
    config::{ChainProgress, Config},
    interface::{GeyserPlugin, GeyserPluginError, ReplicaTransactionInfoVersions, Result},
    message_processor::run_message_publisher,
    metrics::{Counter, Metrics},
    prelude::*,
    selectors::TransactionSelector,
    sender::Sender,
    stats::{Stats, StatsRequest},
};

const UNINIT: &str = "RabbitMQ plugin not initialized yet!";

#[inline]
#[allow(clippy::needless_lifetimes)]
fn custom_err<'a, E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>>(
    counter: &'a Counter,
) -> impl FnOnce(E) -> GeyserPluginError + 'a {
    |e| {
        counter.log(1);
        GeyserPluginError::Custom(e.into())
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    sender: Arc<Sender>,
    amqp_sender: crossbeam::channel::Sender<(Message, String)>,
    tx_sel: TransactionSelector,
    metrics: Arc<Metrics>,
    chain_progress: ChainProgress,
    stats_sender: mpsc::SyncSender<StatsRequest>,
    num_shards: u64,
}

/// An instance of the plugin
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct GeyserPluginRabbitMq(Option<Inner>);

impl GeyserPluginRabbitMq {
    fn expect_inner(&self) -> &Inner {
        self.0.as_ref().expect(UNINIT)
    }

    fn get_shard_number(&self, slot: u64) -> u64 {
        slot % self.expect_inner().num_shards
    }
}

impl GeyserPlugin for GeyserPluginRabbitMq {
    fn name(&self) -> &'static str {
        "GeyserPluginRabbitMq"
    }

    fn on_load(&mut self, cfg: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");

        info!("Plugin loading");

        let metrics = Metrics::new_rc();

        let version;
        let host;

        {
            let ver = env!("CARGO_PKG_VERSION");
            let git = option_env!("META_GIT_HEAD");
            // TODO
            // let rem = option_env!("META_GIT_REMOTE");

            {
                use std::fmt::Write;

                let mut s = format!("v{ver}");

                if let Some(git) = git {
                    write!(s, "+git.{git}").unwrap();
                }

                version = s;
            }

            // TODO
            // let rustc_ver = env!("META_RUSTC_VERSION");
            // let build_host = env!("META_BUILD_HOST");
            // let target = env!("META_BUILD_TARGET");
            // let profile = env!("META_BUILD_PROFILE");
            // let platform = env!("META_BUILD_PLATFORM");

            host = hostname::get()
                .map_err(custom_err(&metrics.errs))?
                .into_string()
                .map_err(|_| anyhow!("Failed to parse system hostname"))
                .map_err(custom_err(&metrics.errs))?;
        }

        let (amqp, jobs, metrics_conf, chain_progress, tx_sel, num_shards, max_msg_buffer_size) =
            Config::read(cfg)
                .and_then(Config::into_parts)
                .map_err(custom_err(&metrics.errs))?;

        if let Some(config) = metrics_conf.config {
            const VAR: &str = "SOLANA_METRICS_CONFIG";

            if env::var_os(VAR).is_some() {
                warn!("Overriding existing value for {VAR}");
            }

            env::set_var(VAR, config);
        }

        info!("Build tokio runtime");
        let max_blocking_threads = jobs.blocking.unwrap_or(jobs.limit);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("geyser-rabbitmq")
            .worker_threads(jobs.limit)
            .max_blocking_threads(max_blocking_threads)
            .build()
            .map_err(custom_err(&metrics.errs))?;

        info!("Creating Sender");
        let s_sender = rt.block_on(async {
            let sender = Sender::new(
                amqp,
                format!("geyser-rabbitmq-{version}@{host}"),
                StartupType::Normal,
                Arc::clone(&metrics),
            )
            .await
            .map_err(custom_err(&metrics.errs))?;

            Result::<_>::Ok(sender)
        })?;
        let sender = Arc::new(s_sender);

        let (amqp_sender, amqp_receiver) = crossbeam::channel::bounded(max_msg_buffer_size);

        info!("Running processor thread");
        // start running amqp receiver in background, using the built tokio runtime
        run_future_on_new_thread(
            run_message_publisher(
                amqp_receiver,
                metrics.clone(),
                sender.clone(),
                max_blocking_threads,
            ),
            rt,
        );

        info!("Creating stats publisher");
        // create the stats processor
        let stats_sender = Stats::create_publisher(sender.clone(), amqp_sender.clone(), num_shards);

        info!("Setting inner");
        self.0 = Some(Inner {
            sender,
            amqp_sender,
            tx_sel,
            metrics,
            chain_progress,
            stats_sender,
            num_shards,
        });

        info!("Plugin loaded");

        Ok(())
    }

    /// The callback called right before a plugin is unloaded by the system
    /// Used for doing cleanup before unload.
    fn on_unload(&mut self) {
        log::info!("Plugin unloading");
        let Some(inner) = self.0.take() else {
            log::warn!("Plugin already unloaded");
            return;
        };
        log::info!("Shutting down plugin");
        inner.sender.stop();
        log::info!("Signaled producer to stop");

        let processor_queue = inner.amqp_sender;
        while !processor_queue.is_empty() {
            log::info!(
                "Waiting for processor queue to drain ({} messages left)",
                processor_queue.len()
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        log::info!("Plugin unloaded");
    }

    #[allow(clippy::too_many_lines)]
    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        #[inline]
        fn process_transaction(
            sel: &TransactionSelector,
            stx: &SanitizedTransaction,
            meta: &TransactionStatusMeta,
            slot: u64,
            index_in_block: usize,
        ) -> anyhow::Result<Option<(Message, String)>> {
            match sel.get_route(stx, meta, slot) {
                None => Ok(None),
                Some(route) => {
                    //compress the meta
                    let mut compressor = zstd::bulk::Compressor::new(2).unwrap();
                    let pre_datum_compressed = meta.pre_datum.as_ref().map(|all_datums| {
                        all_datums
                            .iter()
                            .map(|data| {
                                data.as_ref().map(|some_data| {
                                    if some_data.is_empty() {
                                        some_data.clone()
                                    } else {
                                        compressor.compress(some_data).unwrap()
                                    }
                                })
                            })
                            .collect()
                    });
                    let post_datum_compressed = meta.post_datum.as_ref().map(|all_datums| {
                        all_datums
                            .iter()
                            .map(|data| {
                                data.as_ref().map(|some_data| {
                                    if some_data.is_empty() {
                                        some_data.clone()
                                    } else {
                                        compressor.compress(some_data).unwrap()
                                    }
                                })
                            })
                            .collect()
                    });

                    let meta = TransactionStatusMeta {
                        status: meta.status.clone(),
                        fee: meta.fee,
                        pre_balances: meta.pre_balances.clone(),
                        post_balances: meta.post_balances.clone(),
                        pre_owners: meta.pre_owners.clone(),
                        post_owners: meta.post_owners.clone(),
                        pre_datum: pre_datum_compressed,
                        post_datum: post_datum_compressed,
                        inner_instructions: meta.inner_instructions.clone(),
                        log_messages: meta.log_messages.clone(),
                        pre_token_balances: meta.pre_token_balances.clone(),
                        post_token_balances: meta.post_token_balances.clone(),
                        rewards: meta.rewards.clone(),
                        loaded_addresses: meta.loaded_addresses.clone(),
                        return_data: meta.return_data.clone(),
                        compute_units_consumed: meta.compute_units_consumed,
                        cost_units: meta.cost_units,
                    };

                    //make it pretty
                    let full_tx = ConfirmedTransactionWithStatusMeta {
                        tx_with_meta: TransactionWithStatusMeta::Complete(
                            VersionedTransactionWithStatusMeta {
                                meta,
                                transaction: stx.to_versioned_transaction(),
                            },
                        ),
                        slot,
                        block_time: None,
                    };

                    let encoded_tx = full_tx.encode(
                        UiTransactionEncoding::JsonParsed,
                        Some(0),
                        index_in_block,
                    )?;

                    Ok(Some((
                        Message::TransactionNotify(Box::new(TransactionNotify {
                            transaction: encoded_tx,
                        })),
                        route,
                    )))
                },
            }
        }

        let this = self
            .0
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom(anyhow!(UNINIT).into()))?;
        if this.tx_sel.is_empty() {
            return Ok(());
        }

        this.metrics.recvs.log(1);

        let stx: &SanitizedTransaction;
        let meta: &TransactionStatusMeta;
        let is_vote: bool;
        let index_in_block: usize;

        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                stx = tx.transaction;
                meta = tx.transaction_status_meta;
                is_vote = tx.is_vote;
                index_in_block = 0;
            },
            ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                stx = tx.transaction;
                meta = tx.transaction_status_meta;
                is_vote = tx.is_vote;
                index_in_block = tx.index;
            },
        }

        let is_err = matches!(meta.status, Err(..));

        //send this tx to the stats thread
        this.stats_sender
            .send(StatsRequest {
                slot,
                stx: stx.clone(),
                meta: meta.clone(),
                is_vote,
                is_err,
            })
            .map_err(|_| GeyserPluginError::Custom(anyhow!(UNINIT).into()))?;

        //no downstream processing of errors or votes
        if is_err || is_vote {
            return Ok(());
        }

        //handle tx match
        if !this.tx_sel.is_empty() {
            match process_transaction(&this.tx_sel, stx, meta, slot, index_in_block) {
                Ok(Some(m)) => {
                    let message = m.0;
                    let route = m.1.clone();
                    this.amqp_sender.send((message, route)).unwrap();
                    this.metrics.sends.log(1);
                },
                Ok(None) => (),
                Err(e) => {
                    warn!("Error processing transaction: {e:?}");
                    this.metrics.errs.log(1);
                },
            }
        }

        Ok(())
    }

    /// Called when a slot status is updated
    #[allow(unused_variables)]
    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> Result<()> {
        let this = self
            .0
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom(anyhow!(UNINIT).into()))?;
        if !this.chain_progress.slot_status.unwrap_or(false) {
            return Ok(());
        }
        let msg = Message::SlotStatusNotify(SlotStatusNotify {
            slot,
            parent,
            status: match status {
                SlotStatus::Processed => indexer_rabbitmq::geyser::SlotStatus::Processed,
                SlotStatus::Confirmed => indexer_rabbitmq::geyser::SlotStatus::Confirmed,
                SlotStatus::Rooted => indexer_rabbitmq::geyser::SlotStatus::Rooted,
                _ => {
                    return Ok(());
                },
            },
        });
        let shard = self.get_shard_number(slot);
        this.amqp_sender
            .send((msg, format!("multi.chain.slot_status.{shard}")))
            .unwrap();
        this.metrics.sends.log(1);

        Ok(())
    }

    /// Called when block's metadata is updated.
    #[allow(unused_variables)]
    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        let this = self
            .0
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom(anyhow!(UNINIT).into()))?;
        if !this.chain_progress.block_meta.unwrap_or(false) {
            return Ok(());
        }
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(bi) => {
                let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                    blockhash: String::from(bi.blockhash),
                    slot: bi.slot,
                    block_time: bi.block_time.unwrap_or_default(),
                    block_height: bi.block_height.unwrap_or_default(),
                });
                let shard = self.get_shard_number(bi.slot);
                this.amqp_sender
                    .send((msg, format!("multi.chain.block_meta.{shard}")))
                    .unwrap();
                this.metrics.sends.log(1);
            },
            ReplicaBlockInfoVersions::V0_0_2(bi) => {
                let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                    blockhash: String::from(bi.blockhash),
                    slot: bi.slot,
                    block_time: bi.block_time.unwrap_or_default(),
                    block_height: bi.block_height.unwrap_or_default(),
                });
                let shard = self.get_shard_number(bi.slot);
                this.amqp_sender
                    .send((msg, format!("multi.chain.block_meta.{shard}")))
                    .unwrap();
                this.metrics.sends.log(1);
            },
            ReplicaBlockInfoVersions::V0_0_3(bi) => {
                let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                    blockhash: String::from(bi.blockhash),
                    slot: bi.slot,
                    block_time: bi.block_time.unwrap_or_default(),
                    block_height: bi.block_height.unwrap_or_default(),
                });
                let shard = self.get_shard_number(bi.slot);
                this.amqp_sender
                    .send((msg, format!("multi.chain.block_meta.{shard}")))
                    .unwrap();
                this.metrics.sends.log(1);
            },
            ReplicaBlockInfoVersions::V0_0_4(bi) => {
                let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                    blockhash: String::from(bi.blockhash),
                    slot: bi.slot,
                    block_time: bi.block_time.unwrap_or_default(),
                    block_height: bi.block_height.unwrap_or_default(),
                });
                let shard = self.get_shard_number(bi.slot);
                this.amqp_sender
                    .send((msg, format!("multi.chain.block_meta.{shard}")))
                    .unwrap();
                this.metrics.sends.log(1);
            },
        }
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        let this = self.expect_inner();
        !this.tx_sel.is_empty()
    }
}
