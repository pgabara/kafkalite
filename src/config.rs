use std::time::Duration;

pub struct BrokerConfig {
    pub port: u16,
    pub connection_timeout: Duration,
}

impl BrokerConfig {
    pub fn new(port: u16, connection_timeout: Duration) -> Self {
        BrokerConfig {
            port,
            connection_timeout,
        }
    }
}
