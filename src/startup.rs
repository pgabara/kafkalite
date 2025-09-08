use crate::broker::{BrokerState, start_broker_server};
use crate::config::BrokerConfig;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

pub async fn run_broker(
    config: BrokerConfig,
    shutdown_signal: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    let broker_state = Arc::new(RwLock::new(BrokerState::default()));
    start_broker_server(config, shutdown_signal, broker_state).await
}
