use std::{sync::Arc, thread, time::Duration};

use indexer_rabbitmq::{
    geyser::{CommittmentLevel, Message, Producer, QueueType, StartupType},
    lapin::{Connection, ConnectionProperties},
    suffix::Suffix,
};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::{
    config,
    metrics::{Counter, Metrics},
};

#[derive(Debug)]
pub struct Sender {
    amqp: config::Amqp,
    name: String,
    startup_type: StartupType,
    producer: RwLock<Producer>,
    metrics: Arc<Metrics>,
}

impl Sender {
    pub async fn new(
        amqp: config::Amqp,
        name: String,
        startup_type: StartupType,
        metrics: Arc<Metrics>,
    ) -> Result<Self, indexer_rabbitmq::Error> {
        let producer = Self::create_producer(&amqp, name.as_str(), startup_type).await?;

        Ok(Self {
            amqp,
            name,
            startup_type,
            producer: RwLock::new(producer),
            metrics,
        })
    }

    async fn create_producer(
        amqp: &config::Amqp,
        name: impl Into<indexer_rabbitmq::lapin::types::LongString>,
        startup_type: StartupType,
    ) -> Result<Producer, indexer_rabbitmq::Error> {
        let amqp_name = name.into();
        let mut tries = 0;
        let producer = loop {
            let delay = Self::get_retry_delay(tries);
            thread::sleep(Duration::from_millis(delay));
            tries += 1;

            let Ok(conn) = Connection::connect(
                &amqp.address,
                ConnectionProperties::default()
                    .with_connection_name(amqp_name.clone())
                    .with_executor(tokio_executor_trait::Tokio::current())
                    .with_reactor(tokio_reactor_trait::Tokio),
            )
            .await
            else {
                continue;
            };

            let Ok(prod) = Producer::new(
                &conn,
                QueueType::new(
                    amqp.network,
                    startup_type,
                    &Suffix::ProductionUnchecked,
                    &Suffix::ProductionUnchecked,
                    CommittmentLevel::Processed,
                    "unused".to_string(),
                )?,
            )
            .await
            else {
                continue;
            };

            break prod;
        };

        Ok(producer)
    }

    async fn connect<'a>(
        &'a self,
    ) -> Result<RwLockReadGuard<'a, Producer>, indexer_rabbitmq::Error> {
        let mut producer = self.producer.write().await;

        if producer.chan.status().connected() {
            // This thread was in line for a write,
            // but another thread has already handled the reconnection.
            //
            // Downgrade to a read so we can retry to send our msg.
            let read_lock = producer.downgrade();
            return Ok(read_lock);
        }

        log::info!("Reconnecting to AMQP server...");

        // This thread now has the write lock,
        // all others should begin waiting for a read.
        // Either through a `send` call, or the `downgrade` check above.

        // Reconnect to AMQP server
        *producer =
            Self::create_producer(&self.amqp, self.name.as_str(), self.startup_type).await?;

        // Release the write lock, by downgrading, and handing a read lock to the original caller,
        // so they can send their message
        let producer = producer.downgrade();

        Ok(producer)
    }

    pub async fn send(&self, msg: Message, route: &str) {
        #[inline]
        fn log_err<E: std::fmt::Debug>(counter: &'_ Counter) -> impl FnOnce(E) + '_ {
            |err| {
                counter.log(1);
                log::error!("{:?}", err);
            }
        }

        let metrics = &self.metrics;
        // If we're in the middle of a reconnect, we'll be waiting here,
        // until we can get a read lock, which is what we want.
        let prod = self.producer.read().await;

        if prod
            .write(&msg, Some(route))
            .await
            .map_err(log_err(&metrics.errs))
            .is_err()
        {
            // Drop the read lock. This thread is going to attempt to "promote" itself,
            // and try to get a write lock to reconnect.
            // 
            // This will also help allow a writer to grab the Producer if one's waiting
            std::mem::drop(prod);

            loop {
                let Ok(new_prod) = self.connect().await.map_err(log_err(&metrics.errs)) else {
                    continue;
                };

                if new_prod
                    .write(&msg, Some(route))
                    .await
                    .map_err(log_err(&metrics.errs))
                    .is_ok()
                {
                    // All is well. Unravel this madness
                    break;
                }
            }
        }
    }

    // Linear backoff maxxing out at 1000ms
    fn get_retry_delay(tries: usize) -> u64 {
        (tries * 100).min(1000) as u64
    }
}
