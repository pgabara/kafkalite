use crate::topic::{
    ClientId, Message, Subscription, Topic, TopicManager, TopicName, TopicPublishError,
    TopicPublisher, TopicSubscribeError,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct Broker {
    topics: RwLock<HashMap<TopicName, Arc<RwLock<Topic>>>>,
}

impl TopicManager for Broker {
    async fn add_topic(&self, topic_name: &TopicName) -> bool {
        {
            let mut topics = self.topics.write().await;
            if topics.contains_key(topic_name) {
                return false;
            }
            let topic = Arc::new(RwLock::new(Topic::new(topic_name)));
            topics.insert(topic_name.clone(), topic);
        }
        true
    }
}

impl TopicPublisher for Broker {
    async fn publish(
        &self,
        topic_name: &TopicName,
        message: Message,
    ) -> Result<(), TopicPublishError> {
        let topic = {
            let topics = self.topics.read().await;
            topics.get(topic_name).cloned()
        };
        let topic = topic.ok_or(TopicPublishError::TopicNotFound(topic_name.to_string()))?;
        let mut topic_guard = topic.write().await;
        topic_guard
            .publish(message.payload)
            .map_err(TopicPublishError::SendError)
    }

    async fn subscribe(
        &self,
        topic_name: &TopicName,
        client_id: &ClientId,
    ) -> Result<Subscription, TopicSubscribeError> {
        let topic = {
            let topics = self.topics.read().await;
            topics.get(topic_name).cloned()
        };
        let topic = topic.ok_or(TopicSubscribeError::TopicNotFound(topic_name.to_string()))?;
        let mut topic_guard = topic.write().await;
        let subscription = topic_guard.subscribe(client_id);
        Ok(subscription)
    }
}
