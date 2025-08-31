use bytes::BytesMut;
use futures_util::SinkExt;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::config::BrokerConfig;

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
    let mut framed_socket = Framed::new(socket, LengthDelimitedCodec::new());

    let timeout = config.connection_timeout;
    while let Some(bytes) = tokio::time::timeout(timeout, framed_socket.next()).await? {
        let bytes = bytes?;
        tracing::debug!("Received bytes: {:?}", bytes);
        let response = if &bytes[..] == b"PING" {
            BytesMut::from("PONG").freeze()
        } else {
            BytesMut::from("ERR").freeze()
        };
        framed_socket.send(response).await?;
    }

    Ok(())
}
