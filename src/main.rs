use kafkalite::config::BrokerConfig;
use std::time::Duration;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BrokerConfig::new(9000, Duration::from_secs(10));
    let shutdown_signal = kafkalite::shutdown::ctrl_c_shutdown_signal();
    kafkalite::startup::run_broker(config, shutdown_signal).await
}
