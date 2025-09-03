use std::{sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use indexer_rabbitmq::geyser::Message;

use crate::{async_utils::IntervalTimer, sender::Sender};

pub async fn run_message_publisher(
    receiver: crossbeam::channel::Receiver<(Message, String)>,
    sender: Arc<Sender>,
    max_running_futures: usize,
) {
    // timer to ensure we don't get stuck if the validator stops, and we have `< max_running_futures` in the queue
    let mut timer = IntervalTimer::new(Duration::from_millis(10));
    let mut futs = FuturesUnordered::new();

    // Loop that will automatically end when `receiver` closes due to shutdown signal
    while let Ok((msg, route)) = receiver.recv() {
        let fut = sender.send(msg, route);
        futs.push(fut);
        if futs.len() >= max_running_futures || timer.interval_hit() {
            futs.next().await;
        }
    }

    log::warn!("Message publisher thread stopping");
}
