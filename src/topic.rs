use std::collections::HashMap;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub trait TopicManager {
    async fn add_topic(&self, topic_name: &TopicName) -> bool;
}

pub trait TopicPublisher {
    async fn publish(
        &self,
        topic_name: &TopicName,
        payload: Message,
    ) -> Result<(), TopicPublishError>;
    async fn subscribe(
        &self,
        topic_name: &TopicName,
        client_id: &ClientId,
    ) -> Result<Subscription, TopicSubscribeError>;
}

pub enum TopicPublishError {
    TopicNotFound(TopicName),
    SendError(SendError<Message>),
}

pub enum TopicSubscribeError {
    TopicNotFound(TopicName),
}

pub struct Topic {
    pub topic_name: String,
    subscribers: HashMap<ClientId, UnboundedSender<Message>>,
}

impl Topic {
    pub fn new(topic_name: &str) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            subscribers: HashMap::new(),
        }
    }
}

pub type ClientId = String;

pub type TopicName = String;

impl Topic {
    pub fn subscribe(&mut self, client_id: &str) -> Subscription {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Message>();
        self.subscribers
            .entry(client_id.to_string())
            .or_insert(sender);
        Subscription::new(self.topic_name.to_string(), receiver)
    }

    pub fn publish(&mut self, payload: Vec<u8>) -> Result<(), SendError<Message>> {
        for subscriber in self.subscribers.values() {
            subscriber.send(Message::new(payload.clone()))?
        }
        Ok(())
    }
}

pub struct Subscription {
    pub topic_name: String,
    pub receiver: UnboundedReceiver<Message>,
}

impl Subscription {
    pub fn new(topic_name: TopicName, receiver: UnboundedReceiver<Message>) -> Self {
        Subscription {
            topic_name,
            receiver,
        }
    }
}

pub struct Message {
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(payload: Vec<u8>) -> Self {
        Message { payload }
    }
}
