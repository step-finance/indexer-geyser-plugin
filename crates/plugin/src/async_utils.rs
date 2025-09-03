use std::future::Future;

pub struct IntervalTimer {
    pub interval: std::time::Duration,
    last_timestamp: std::time::Instant,
}

impl IntervalTimer {
    pub fn new(interval: std::time::Duration) -> Self {
        Self {
            last_timestamp: std::time::Instant::now(),
            interval,
        }
    }

    pub fn interval_hit(&mut self) -> bool {
        let elapsed = self.last_timestamp.elapsed();
        if elapsed >= self.interval {
            self.last_timestamp = std::time::Instant::now();
            true
        } else {
            false
        }
    }
}

#[allow(clippy::unwrap_used)]
pub fn build_tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub fn run_future_on_new_thread<T: Send + 'static>(
    fut: impl Future<Output = T> + Send + 'static,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(move || {
        let runtime = build_tokio_runtime();
        runtime.block_on(fut)
    })
}
