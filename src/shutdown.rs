use std::sync::Arc;
use tokio::sync::Notify;

pub fn ctrl_c_shutdown_signal() -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    tokio::spawn({
        let notify = Arc::clone(&notify);
        async move {
            let _ = tokio::signal::ctrl_c().await;
            notify.notify_waiters();
        }
    });
    notify
}
