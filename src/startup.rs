use crate::broker::Broker;
use crate::config::BrokerConfig;
use crate::server::start_broker_server;
use std::sync::Arc;

pub async fn run_broker(
    config: BrokerConfig,
    shutdown_signal: Arc<tokio::sync::Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    let broker = Broker::default();
    start_broker_server(broker, config, shutdown_signal).await
}
