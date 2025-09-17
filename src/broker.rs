use crate::topic::{Message, Subscription, Topic, TopicManager, TopicName, TopicPublisher};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct Broker {
    topics: RwLock<HashMap<TopicName, Arc<RwLock<Topic>>>>,
}

impl TopicManager for Broker {
    async fn add_topic(&self, topic_name: String) -> Result<bool, Box<dyn Error>> {
        {
            let mut topics = self.topics.write().await;
            if topics.contains_key(&topic_name) {
                return Ok(false);
            }
            let topic = Arc::new(RwLock::new(Topic::new(&topic_name)));
            topics.insert(topic_name, topic);
        }
        Ok(true)
    }
}

impl TopicPublisher for Broker {
    async fn publish(&self, topic_name: TopicName, message: Message) -> Result<(), Box<dyn Error>> {
        let topic = {
            let topics = self.topics.read().await;
            topics.get(&topic_name).cloned()
        };
        let topic = topic.ok_or(BrokerError::new("Topic not found"))?;
        let mut topic_guard = topic.write().await;
        topic_guard.publish(message.payload)
    }

    async fn subscribe(
        &self,
        topic_name: TopicName,
        client_id: String,
    ) -> Result<Subscription, Box<dyn Error>> {
        let topic = {
            let topics = self.topics.read().await;
            topics.get(&topic_name).cloned()
        };
        let topic = topic.ok_or(BrokerError::new("Topic not found"))?;
        let mut topic_guard = topic.write().await;
        let subscription = topic_guard.subscribe(&client_id);
        Ok(subscription)
    }
}

#[derive(Debug)]
struct BrokerError {
    details: String,
}

impl BrokerError {
    pub fn new(details: &str) -> Self {
        Self {
            details: details.to_string(),
        }
    }
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for BrokerError {}
