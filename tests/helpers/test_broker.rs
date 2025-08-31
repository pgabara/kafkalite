use kafkalite::config::BrokerConfig;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct TestBroker {
    join: JoinHandle<()>,
    shutdown_signal: Arc<Notify>,
    pub socket_addr: SocketAddr,
}

impl TestBroker {
    pub async fn start() -> Self {
        let socket_addr = get_socket_addr();
        let config = BrokerConfig::new(socket_addr.port(), Duration::from_secs(1));
        Self::start_with_config(config).await
    }

    pub async fn start_with_config(config: BrokerConfig) -> Self {
        let mut config = config;
        if config.port == 0 {
            let socket_addr = get_socket_addr();
            config.port = socket_addr.port();
        }

        let shutdown_signal = Arc::new(Notify::new());
        let broker_port = config.port;
        let join = tokio::spawn({
            let shutdown_signal = Arc::clone(&shutdown_signal);
            async move {
                kafkalite::broker::start_broker(config, shutdown_signal)
                    .await
                    .expect("Failed to start broker");
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let socket_addr = ([127, 0, 0, 1], broker_port).into();
        Self {
            join,
            shutdown_signal,
            socket_addr,
        }
    }

    pub async fn stop(self) {
        self.shutdown_signal.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), self.join)
            .await
            .expect("Timed out waiting for broker to stop")
            .expect("Test broker field to shut down");
    }
}

fn get_socket_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind random port");
    listener.local_addr().expect("Failed to get local address")
}
