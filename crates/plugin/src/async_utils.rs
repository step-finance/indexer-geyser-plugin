use std::future::Future;

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
