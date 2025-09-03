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

pub fn run_future_on_new_thread<T: Send + 'static>(
    fut: impl Future<Output = T> + Send + 'static,
    runtime: tokio::runtime::Runtime,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(move || runtime.block_on(fut))
}
