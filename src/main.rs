use kafkalite::config::BrokerConfig;
use std::time::Duration;

#[tokio::main]
pub async fn main() -> tokio::io::Result<()> {
    let config = BrokerConfig::new(9000, Duration::from_secs(10));
    let shutdown_signal = kafkalite::shutdown::ctrl_c_shutdown_signal();
    kafkalite::broker::start_broker(config, shutdown_signal).await
}
