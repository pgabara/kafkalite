use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub trait TopicManager {
    async fn add_topic(&self, topic_name: &TopicName) -> bool;
    async fn delete_topic(&self, topic_name: &TopicName) -> bool;
    async fn list_topics(&self) -> Vec<TopicName>;
}

pub trait TopicPublisher {
    async fn publish(
        &self,
        topic_name: &TopicName,
        payload: Message,
    ) -> Result<(), TopicPublishError>;
}

pub enum TopicPublishError {
    TopicNotFound(TopicName),
}

pub trait TopicSubscriber {
    async fn subscribe(
        &self,
        topic_name: &TopicName,
        client_id: ClientId,
    ) -> Result<Subscription, TopicSubscribeError>;

    async fn unsubscribe(
        &self,
        topic_name: &TopicName,
        client_id: ClientId,
    ) -> Result<(), TopicSubscribeError>;
}

pub enum TopicSubscribeError {
    TopicNotFound(TopicName),
}

pub struct Topic {
    pub topic_name: TopicName,
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

pub type ClientId = Uuid;

pub type TopicName = String;

impl Topic {
    pub fn subscribe(&mut self, client_id: ClientId) -> Subscription {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Message>();
        self.subscribers.entry(client_id).or_insert(sender);
        Subscription::new(self.topic_name.to_string(), receiver)
    }

    pub fn unsubscribe(&mut self, client_id: ClientId) {
        self.subscribers.remove(&client_id);
    }

    pub fn publish(&mut self, payload: Vec<u8>) {
        let mut dead_subscribers = vec![];

        for (&client_id, subscriber) in self.subscribers.iter() {
            if subscriber.send(Message::new(payload.clone())).is_err() {
                dead_subscribers.push(client_id);
            }
        }

        for client_id in dead_subscribers {
            self.subscribers.remove(&client_id);
        }
    }
}

pub struct Subscription {
    pub topic_name: TopicName,
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
