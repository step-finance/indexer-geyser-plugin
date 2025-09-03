use std::{sync::Arc, time::Duration};

use crossbeam::channel::RecvTimeoutError;
use futures::{stream::FuturesUnordered, StreamExt};
use indexer_rabbitmq::geyser::Message;

use crate::sender::Sender;

pub async fn run_message_publisher(
    receiver: crossbeam::channel::Receiver<(Message, String)>,
    sender: Arc<Sender>,
    max_running_futures: usize,
) {
    let mut futs = FuturesUnordered::new();

    loop {
        match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok((msg, route)) => {
                let fut = sender.send(msg, route);
                futs.push(fut);
                if futs.len() >= max_running_futures {
                    futs.next().await;
                }
            },
            Err(e) => match e {
                RecvTimeoutError::Timeout => {
                    if sender.is_stopped() {
                        // Plugin is stopped, drain remaining futures
                        while let Some(()) = futs.next().await {}
                        break;
                    }

                    // Validator stopped??
                    // Either way, just wait out the next future, so we're not limited by the max above
                    // and we also get as many messages sent out as possible
                    futs.next().await;
                },
                RecvTimeoutError::Disconnected => {
                    // Channel is closed, Drain remaining futures, and break out of loop
                    while let Some(()) = futs.next().await {}
                    break;
                },
            },
        }
    }

    log::warn!("Message publisher thread stopping");
}
