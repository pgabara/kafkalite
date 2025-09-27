use crate::broker::Broker;
use crate::config::BrokerConfig;
use crate::protocol::request::RequestCodec;
use crate::protocol::response::{Response, ResponseCodec};
use crate::router;
use crate::topic::Subscription;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::io::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{FramedRead, FramedWrite};

pub async fn start_broker_server(
    broker: Broker,
    config: BrokerConfig,
    shutdown_signal: Arc<tokio::sync::Notify>,
) -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(broker);
    let config = Arc::new(config);
    let listener = TcpListener::bind(("0.0.0.0", config.port)).await?;
    tracing::info!("Broker listening on port {}", config.port);

    loop {
        tokio::select! {
            accepted_socket = listener.accept() => {
                match accepted_socket {
                    Ok((socket, client_addr)) => {
                        tracing::debug!("Accepted connection from {client_addr}");
                        tokio::spawn({
                            let config = Arc::clone(&config);
                            let broker = Arc::clone(&broker);
                            async move {
                                if let Err(e) = handle_connection(socket, &broker, &config).await {
                                    tracing::error!("Failed to handle connection with {client_addr} due to: {}", e);
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

async fn handle_connection(
    socket: TcpStream,
    broker: &Broker,
    config: &BrokerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_addr = socket.peer_addr()?;
    let (read_half, write_half) = tokio::io::split(socket);
    let mut reader = FramedRead::new(read_half, RequestCodec);

    let mut sender = BrokerSender::init(write_half);

    loop {
        tokio::select! {
            accepted_request = reader.next() => {
                match accepted_request {
                    Some(request) => {
                        let response = router::route_broker_request(request?, broker).await;
                        sender.send(response)?;

                    }
                    None => {
                        tracing::debug!("Connection with {client_addr} closed");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(config.connection_timeout) => {
                tracing::warn!("Connection with {client_addr} timed out");
                break;
            }
        }
    }

    // todo: unsubscribe client from all topics it is subscribed to
    Ok(())
}

pub enum BrokerResponse {
    BasicResponse(Response),
    StreamedResponse(Subscription),
}

struct BrokerSender {
    sender: UnboundedSender<Response>,
}

impl BrokerSender {
    fn init(write: WriteHalf<TcpStream>) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Response>();

        tokio::spawn({
            let mut framed_write = FramedWrite::new(write, ResponseCodec);
            async move {
                while let Some(response) = receiver.recv().await {
                    if let Err(e) = framed_write.send(response).await {
                        tracing::error!("Error sending response: {e}");
                        break;
                    }
                }
            }
        });

        Self { sender }
    }

    fn send(&mut self, response: BrokerResponse) -> Result<(), Box<dyn std::error::Error>> {
        match response {
            BrokerResponse::BasicResponse(response) => self.send_basic_response(response),
            BrokerResponse::StreamedResponse(subscription) => {
                self.send_streamed_response(subscription)
            }
        }
    }

    fn send_basic_response(
        &mut self,
        response: Response,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender.send(response)?;
        Ok(())
    }

    fn send_streamed_response(
        &mut self,
        mut subscription: Subscription,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender.send(Response::Ack)?;
        tokio::spawn({
            let sender = self.sender.clone();
            async move {
                while let Some(message) = subscription.receiver.recv().await {
                    let response = Response::Message {
                        topic: subscription.topic_name.to_string(),
                        payload: message.payload,
                        offset: message.offset,
                    };
                    let _ = sender.send(response);
                }
            }
        });
        Ok(())
    }
}
