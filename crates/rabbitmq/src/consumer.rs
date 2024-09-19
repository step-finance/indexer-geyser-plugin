//! An AMQP consumer configured from a [`QueueType`]

use std::{io::Seek, marker::PhantomData};

use futures_util::StreamExt;
use lapin::{acker::Acker, options::BasicNackOptions, Connection};

use crate::{serialize::deserialize, Error, QueueType, Result};

/// A consumer consisting of a configured AMQP consumer and queue config
#[derive(Debug)]
pub struct Consumer<Q> {
    // chan: Channel,
    consumer: lapin::Consumer,
    // ty: Q,
    _p: PhantomData<Q>,
}

impl<Q> Clone for Consumer<Q> {
    fn clone(&self) -> Self {
        let Self { consumer, .. } = self;

        Self {
            consumer: consumer.clone(),
            ..*self
        }
    }
}

/// Result of a read operation on a [`Consumer`]
#[derive(Debug)]
pub struct ReadResult<Q: QueueType> {
    /// the message data
    pub data: Q::Message,
    /// the routing key the message was delivered with
    pub routing_key: String,
    /// the acker for the message
    pub acker: Acker,
}

impl<Q: QueueType> Consumer<Q>
where
    Q::Message: for<'a> serde::Deserialize<'a>,
    Q::Message: std::fmt::Debug,
{
    /// Construct a new consumer from a [`QueueType`]
    ///
    /// # Errors
    /// This function fails if the consumer cannot be created and configured
    /// successfully.
    pub async fn new(conn: &Connection, ty: Q, tag: impl AsRef<str>) -> Result<Self> {
        let chan = conn.create_channel().await?;

        let consumer = ty.info().init_consumer(&chan, tag).await?;

        Ok(Self {
            // chan,
            consumer,
            // ty,
            _p: PhantomData,
        })
    }

    /// Receive a single message from this consumer
    ///
    /// # Errors
    /// This function fails if the delivery cannot be successfully performed or
    /// the payload cannot be deserialized.
    pub async fn read(&mut self) -> Result<Option<ReadResult<Q>>> {
        let delivery = match self.consumer.next().await {
            Some(d) => d?,
            None => return Ok(None),
        };

        let mut data_cursor: std::io::Cursor<Vec<u8>> = std::io::Cursor::new(delivery.data);
        let deser_result = deserialize(&mut data_cursor);
        if deser_result.is_err() {
            delivery.acker.nack(BasicNackOptions::default()).await?;
            if let Err(e) = data_cursor.seek(std::io::SeekFrom::Start(0)) {
                log::error!(
                    "Failed to rewind cursor during Failed to deserialize message: {}",
                    e
                );
            }
            log::error!(
                "Failed to deserialize message: {:?}.  Message is {:?}",
                deser_result,
                String::from_utf8(data_cursor.into_inner()),
            );
        }
        deser_result.map_err(Error::MsgDecode).map(|data| {
            Some(ReadResult {
                data,
                routing_key: delivery.routing_key.to_string(),
                acker: delivery.acker,
            })
        })
    }
}
