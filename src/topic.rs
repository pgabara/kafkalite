use std::collections::HashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

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

type ClientId = String;

impl Topic {
    pub fn subscribe(&mut self, client_id: &str) -> Subscription {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Message>();
        self.subscribers
            .entry(client_id.to_string())
            .or_insert(sender);
        Subscription::new(client_id.to_string(), receiver)
    }

    pub fn publish(&mut self, payload: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        for subscriber in self.subscribers.values() {
            subscriber.send(Message::new(payload.clone()))?
        }
        Ok(())
    }
}

pub struct Subscription {
    pub client_id: ClientId,
    pub receiver: UnboundedReceiver<Message>,
}

impl Subscription {
    pub fn new(client_id: ClientId, receiver: UnboundedReceiver<Message>) -> Self {
        Subscription {
            client_id,
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
