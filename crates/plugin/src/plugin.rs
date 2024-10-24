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
    config::{ChainProgress, Config},
    interface::{GeyserPlugin, GeyserPluginError, ReplicaTransactionInfoVersions, Result},
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
    rt: Arc<tokio::runtime::Runtime>,
    producer: Arc<Sender>,
    tx_sel: TransactionSelector,
    metrics: Arc<Metrics>,
    chain_progress: ChainProgress,
    stats_sender: mpsc::SyncSender<StatsRequest>,
}

impl Inner {
    pub fn spawn<F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(Arc<Self>) -> F,
    ) {
        self.rt.spawn(f(Arc::clone(self)));
    }
}

/// An instance of the plugin
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct GeyserPluginRabbitMq(Option<Arc<Inner>>);

impl GeyserPluginRabbitMq {
    fn expect_inner(&self) -> &Arc<Inner> {
        self.0.as_ref().expect(UNINIT)
    }

    #[inline]
    fn with_inner<T>(
        &self,
        uninit: impl FnOnce() -> GeyserPluginError,
        f: impl FnOnce(&Arc<Inner>) -> anyhow::Result<T>,
    ) -> Result<T> {
        match self.0 {
            Some(ref inner) => f(inner).map_err(custom_err(&inner.metrics.errs)),
            None => Err(uninit()),
        }
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

        let (amqp, jobs, metrics_conf, chain_progress, tx_sel) = Config::read(cfg)
            .and_then(Config::into_parts)
            .map_err(custom_err(&metrics.errs))?;

        if let Some(config) = metrics_conf.config {
            const VAR: &str = "SOLANA_METRICS_CONFIG";

            if env::var_os(VAR).is_some() {
                warn!("Overriding existing value for {}", VAR);
            }

            env::set_var(VAR, config);
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("geyser-rabbitmq")
            .worker_threads(jobs.limit)
            .max_blocking_threads(jobs.blocking.unwrap_or(jobs.limit))
            .build()
            .map_err(custom_err(&metrics.errs))?;
        let rt = Arc::new(rt);

        let s_producer = rt.block_on(async {
            let producer = Sender::new(
                amqp,
                format!("geyser-rabbitmq-{version}@{host}"),
                StartupType::Normal,
                Arc::clone(&metrics),
            )
            .await
            .map_err(custom_err(&metrics.errs))?;

            Result::<_>::Ok(producer)
        })?;
        let producer = Arc::new(s_producer);

        //create the stats processor
        let stats_sender = Stats::create_publisher(producer.clone(), rt.clone());

        self.0 = Some(Arc::new(Inner {
            rt,
            producer,
            tx_sel,
            metrics,
            chain_progress,
            stats_sender,
        }));

        info!("Plugin loaded");

        Ok(())
    }

    /// The callback called right before a plugin is unloaded by the system
    /// Used for doing cleanup before unload.
    fn on_unload(&mut self) {
        log::info!("Plugin unloading");
        let Some(mut inner) = self.0.take() else {
            log::warn!("Plugin already unloaded");
            return;
        };
        log::info!("Shutting down plugin");
        inner.producer.stop();
        log::info!("Signaled producer to stop");
        //loop trying to unwrap the inner until it's the last reference
        let inner = loop {
            let ref_count = Arc::strong_count(&inner);
            log::info!(
                "Waiting for all references to inner to drop ({} remaining)",
                ref_count
            );
            match Arc::try_unwrap(inner) {
                Ok(inner) => break inner,
                Err(arc) => {
                    inner = arc;
                },
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        log::info!("All references to inner dropped, shutting down runtime");
        let Inner { mut rt, .. } = inner;
        let rt = loop {
            let ref_count = Arc::strong_count(&rt);
            log::info!(
                "Waiting for all references to runtime to drop ({} remaining)",
                ref_count
            );
            match Arc::try_unwrap(rt) {
                Ok(inner) => break inner,
                Err(arc) => {
                    rt = arc;
                },
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        //shutdown the runtime
        rt.shutdown_background();
        log::info!("Plugin unloaded");
    }

    #[allow(clippy::too_many_lines)]
    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        #[inline]
        fn process_transaction<'a>(
            sel: &'a TransactionSelector,
            stx: &SanitizedTransaction,
            meta: &TransactionStatusMeta,
            slot: u64,
            index_in_block: usize,
        ) -> anyhow::Result<Option<(Message, &'a Arc<String>)>> {
            match sel.get_route(stx, meta) {
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

        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
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
                this.stats_sender.send(StatsRequest {
                    slot,
                    stx: stx.clone(),
                    meta: meta.clone(),
                    is_vote,
                    is_err,
                })?;

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
                            this.spawn(|this| async move {
                                this.producer.send(message, route.as_str()).await;
                                this.metrics.sends.log(1);

                                Ok(())
                            });
                        },
                        Ok(None) => (),
                        Err(e) => {
                            warn!("Error processing transaction: {:?}", e);
                            this.metrics.errs.log(1);
                        },
                    }
                }

                Ok(())
            },
        )
    }

    /// Called when a slot status is updated
    #[allow(unused_variables)]
    fn update_slot_status(&self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                if !this.chain_progress.slot_status.unwrap_or(false) {
                    return Ok(());
                };
                let msg = Message::SlotStatusNotify(SlotStatusNotify {
                    slot,
                    parent,
                    status: match status {
                        SlotStatus::Processed => indexer_rabbitmq::geyser::SlotStatus::Processed,
                        SlotStatus::Confirmed => indexer_rabbitmq::geyser::SlotStatus::Confirmed,
                        SlotStatus::Rooted => indexer_rabbitmq::geyser::SlotStatus::Rooted,
                    },
                });
                this.spawn(|this| async move {
                    this.producer.send(msg, "multi.chain.slot_status").await;
                    this.metrics.sends.log(1);

                    Ok(())
                });
                Ok(())
            },
        )
    }

    /// Called when block's metadata is updated.
    #[allow(unused_variables)]
    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                if !this.chain_progress.block_meta.unwrap_or(false) {
                    return Ok(());
                };
                match blockinfo {
                    ReplicaBlockInfoVersions::V0_0_1(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });
                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                    ReplicaBlockInfoVersions::V0_0_2(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });
                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                    ReplicaBlockInfoVersions::V0_0_3(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });

                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                    ReplicaBlockInfoVersions::V0_0_4(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });

                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                }
                Ok(())
            },
        )
    }

    fn account_data_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        let this = self.expect_inner();
        !this.tx_sel.is_empty()
    }
}
