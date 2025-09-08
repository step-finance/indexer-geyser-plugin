use std::future::Future;

pub fn run_future_on_new_thread<T: Send + 'static>(
    fut: impl Future<Output = T> + Send + 'static,
    runtime: tokio::runtime::Runtime,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(move || runtime.block_on(fut))
}
