use std::{sync::Arc, time::Duration};

use crossbeam::channel::RecvTimeoutError;
use futures::{stream::FuturesUnordered, StreamExt};
use indexer_rabbitmq::geyser::Message;

use crate::{metrics::Metrics, sender::Sender};

pub const QUEUE_DEPTH_REPORT_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run_message_publisher(
    receiver: crossbeam::channel::Receiver<(Message, String)>,
    metrics: Arc<Metrics>,
    sender: Arc<Sender>,
    max_running_futures: usize,
) {
    log::info!("Message publisher thread starting");
    let metric_rx_copy = receiver.clone();
    let metrics_sender_copy = sender.clone();
    std::thread::spawn(move || {
        log::info!("Queue depth reporting thread starting");
        loop {
            std::thread::sleep(metrics.queue_depth.report_interval);
            if metrics_sender_copy.is_stopped() {
                break;
            }
            metrics.queue_depth.log_value(metric_rx_copy.len());
        }
        log::info!("Queue depth reporting thread stopping");
    });

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
            Err(RecvTimeoutError::Timeout) => {
                if sender.is_stopped() {
                    // Plugin is stopped, drain remaining futures
                    while let Some(()) = futs.next().await {}
                    break;
                }

                // Validator stopped?? Or we're just quicker than the feed somehow (unlikely)
                // Either way, just wait out the next future, so we're not limited by the max above
                // and we also get as many messages sent out as possible
                futs.next().await;
            },
            Err(RecvTimeoutError::Disconnected) => {
                // Channel is closed, Drain remaining futures, and break out of loop
                while let Some(()) = futs.next().await {}
                break;
            },
        }
    }

    let remaining_futures = futs.len();
    let remaining_messages = receiver.len();

    if remaining_futures > 0 || remaining_messages > 0 {
        log::error!(
            "Message publisher thread stopping, {remaining_futures} futures remaining and {remaining_messages} messages in queue! There should be 0!"
        );
    } else {
        log::info!(
            "Message publisher thread stopping. No futures remaining and no messages in queue"
        );
    }
}
