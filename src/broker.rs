use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::config::BrokerConfig;
use crate::protocol::{Request, RequestCodec, Response, ResponseCodec};

pub async fn start_broker(
    config: BrokerConfig,
    shutdown_signal: Arc<Notify>,
) -> tokio::io::Result<()> {
    let config = Arc::new(config);
    let listener = TcpListener::bind(("0.0.0.0", config.port)).await?;
    tracing::info!("Broker listening on port {}", config.port);

    loop {
        tokio::select! {
            accepted_socket = listener.accept() => {
                match accepted_socket {
                    Ok((socket, addr)) => {
                        tracing::debug!("Accepted connection from {}", addr);
                        tokio::spawn({
                            let config = Arc::clone(&config);
                            async move {
                                if let Err(e) = handle_connection(socket, &config).await {
                                    tracing::error!("Failed to handle connection: {}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept incoming connection {}", e);
                        break;
                    }
                }
            }
            _ = shutdown_signal.notified() => {
                tracing::info!("Shutting down broker requested");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_connection(socket: TcpStream, config: &BrokerConfig) -> tokio::io::Result<()> {
    let (read_half, write_half) = tokio::io::split(socket);
    let mut reader = FramedRead::new(read_half, RequestCodec);
    let mut writer = FramedWrite::new(write_half, ResponseCodec);

    let timeout = config.connection_timeout;
    while let Some(request) = tokio::time::timeout(timeout, reader.next()).await? {
        let request = request?;
        tracing::debug!("Received request: {:?}", request);
        match request {
            Request::Ping => writer.send(Response::Pong).await?,
        }
    }

    Ok(())
}
