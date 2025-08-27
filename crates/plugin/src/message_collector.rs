use std::sync::{
    atomic::AtomicUsize,
    mpsc::{Receiver, SyncSender},
    Arc,
};

use indexer_rabbitmq::geyser::Message;

use crate::metrics::Metrics;

#[derive(Debug)]
pub struct AMQPMessageProcessor<'a> {
    buffer_size: AtomicUsize,
    msg_tx: SyncSender<(Message, &'a str)>,
    msg_rx: Receiver<(Message, &'a str)>,
}

impl<'a> AMQPMessageProcessor<'a> {
    pub fn new() -> Self {
        let (msg_tx, msg_rx) = std::sync::mpsc::sync_channel(0);
        Self {
            buffer_size: AtomicUsize::new(0),
            msg_tx,
            msg_rx,
        }
    }

    pub fn dispatch_msg(&'a self, message: Message, route: &'a str) {
        self.msg_tx
            .send((message, route))
            .expect("Msg channel closed");
        self.buffer_size
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_message(&self) -> Option<(Message, &'a str)> {
        self.msg_rx.recv().ok()
    }
}
