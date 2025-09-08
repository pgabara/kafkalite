use crate::config::BrokerConfig;
use crate::protocol::request::{Request, RequestCodec};
use crate::protocol::response::{Response, ResponseCodec};
use crate::topic::{Subscription, Topic};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, RwLock};
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Default)]
pub struct BrokerState {
    topics: HashMap<String, Arc<RwLock<Topic>>>,
}

impl BrokerState {
    pub fn add_topic(&mut self, topic_name: &str) -> bool {
        if !self.topics.contains_key(topic_name) {
            let topic_name = topic_name.to_string();
            let topic = Arc::new(RwLock::new(Topic::new(&topic_name)));
            self.topics.insert(topic_name, Arc::clone(&topic));
            return true;
        }
        false
    }

    pub fn get_or_create_topic(&mut self, topic_name: &str) -> Arc<RwLock<Topic>> {
        self.topics
            .entry(topic_name.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(Topic::new(topic_name))))
            .clone()
    }

    pub fn get_topic(&self, topic_name: &str) -> Option<Arc<RwLock<Topic>>> {
        self.topics.get(topic_name).cloned()
    }
}

pub async fn start_broker_server(
    config: BrokerConfig,
    shutdown_signal: Arc<Notify>,
    broker_state: Arc<RwLock<BrokerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
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
                            let broker_state = Arc::clone(&broker_state);
                            async move {
                                if let Err(e) = handle_connection(socket, &config, &broker_state).await {
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

async fn handle_connection(
    socket: TcpStream,
    config: &BrokerConfig,
    broker_state: &RwLock<BrokerState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (read_half, write_half) = tokio::io::split(socket);
    let mut reader = FramedRead::new(read_half, RequestCodec);
    let mut writer = FramedWrite::new(write_half, ResponseCodec);

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Response>();
    tokio::spawn(async move {
        while let Some(res) = receiver.recv().await {
            let _ = writer.send(res).await;
        }
    });

    let timeout = config.connection_timeout;
    while let Some(request) = tokio::time::timeout(timeout, reader.next()).await? {
        let request = request?;
        tracing::debug!("Received request: {:?}", request);
        match request {
            Request::Ping => sender.send(Response::Pong)?,
            Request::AddTopic { topic } => {
                let is_topic_added = add_topic(&topic, broker_state).await;
                if is_topic_added {
                    sender.send(Response::Ack)?;
                } else {
                    sender.send(Response::Nack)?;
                }
            }
            Request::Publish { topic, payload } => {
                publish(&topic, payload, broker_state).await;
                sender.send(Response::Ack)?;
            }
            Request::Subscribe { topic, client_id } => {
                let mut subscription = subscribe(&topic, &client_id, broker_state).await;
                tokio::spawn({
                    let sender = sender.clone();
                    async move {
                        while let Some(message) = subscription.receiver.recv().await {
                            let response = Response::Message {
                                topic: topic.to_string(),
                                payload: message.payload,
                            };
                            let _ = sender.send(response);
                        }
                    }
                });
                sender.send(Response::Ack)?;
            }
        }
    }

    Ok(())
}

async fn add_topic(topic: &str, broker_state: &RwLock<BrokerState>) -> bool {
    tracing::debug!("Adding topic: {}", topic);
    let is_topic_added = {
        let mut broker_state_guard = broker_state.write().await;
        broker_state_guard.add_topic(topic)
    };
    if !is_topic_added {
        tracing::warn!("Topic with name {} already exists", topic);
    }
    is_topic_added
}

async fn publish(topic: &str, payload: Vec<u8>, broker_state: &RwLock<BrokerState>) {
    tracing::debug!("Publishing message to topic {}", topic);
    let topic = {
        let mut broker_state_guard = broker_state.write().await;
        // todo: to be replaced with .get_topic() after AddTopic api is added
        broker_state_guard.get_or_create_topic(topic)
    };
    {
        let mut topic_guard = topic.write().await;
        let _ = topic_guard.publish(payload);
    }
}

async fn subscribe(
    topic: &str,
    client_id: &str,
    broker_state: &RwLock<BrokerState>,
) -> Subscription {
    tracing::debug!("Subscribing to topic {}", topic);
    let topic = {
        let mut broker_state_guard = broker_state.write().await;
        // todo: to be replaced with .get_topic() after AddTopic api is added
        broker_state_guard.get_or_create_topic(topic)
    };
    let mut topic_guard = topic.write().await;
    topic_guard.subscribe(client_id)
}
