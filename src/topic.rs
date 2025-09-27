use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub trait TopicManager {
    async fn add_topic(&self, topic_name: &TopicName, retention: u64) -> bool;
    async fn delete_topic(&self, topic_name: &TopicName) -> bool;
    async fn list_topics(&self) -> Vec<TopicName>;
}

pub trait TopicPublisher {
    async fn publish(
        &self,
        topic_name: &TopicName,
        message_payload: Vec<u8>,
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
    subscribers: HashMap<ClientId, SubscriberHandle>,
    log: VecDeque<MessageRecord>,
    retention: u64,
    next_offset: u64,
}

impl Topic {
    pub fn new(topic_name: &str, retention: u64) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            subscribers: HashMap::new(),
            log: VecDeque::with_capacity(retention as usize),
            retention,
            next_offset: 0,
        }
    }
}

pub type ClientId = Uuid;

pub type TopicName = String;

impl Topic {
    pub fn subscribe(&mut self, client_id: ClientId) -> Subscription {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<MessageRecord>();

        for message in self.log.iter() {
            let _ = sender.send(message.clone());
        }

        self.subscribers
            .entry(client_id)
            .or_insert(SubscriberHandle::new(sender));

        Subscription::new(self.topic_name.to_string(), receiver)
    }

    pub fn unsubscribe(&mut self, client_id: ClientId) {
        self.subscribers.remove(&client_id);
    }

    pub fn publish(&mut self, payload: Vec<u8>) {
        let offset = self.get_next_offset();
        let message_record = MessageRecord::new(offset, payload);

        self.persist_message(message_record.clone());

        let mut dead_subscribers = vec![];

        for (&client_id, subscriber_handle) in self.subscribers.iter() {
            if subscriber_handle
                .sender
                .send(message_record.clone())
                .is_err()
            {
                dead_subscribers.push(client_id);
            }
        }

        for client_id in dead_subscribers {
            self.subscribers.remove(&client_id);
        }
    }

    fn persist_message(&mut self, message: MessageRecord) {
        self.log.push_back(message);
        if self.log.len() > self.retention as usize {
            self.log.pop_front();
        }
    }

    fn get_next_offset(&mut self) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;
        offset
    }
}

pub struct Subscription {
    pub topic_name: TopicName,
    pub receiver: UnboundedReceiver<MessageRecord>,
}

impl Subscription {
    pub fn new(topic_name: TopicName, receiver: UnboundedReceiver<MessageRecord>) -> Self {
        Subscription {
            topic_name,
            receiver,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl MessageRecord {
    pub fn new(offset: u64, payload: Vec<u8>) -> Self {
        MessageRecord { offset, payload }
    }
}

pub struct SubscriberHandle {
    sender: UnboundedSender<MessageRecord>,
}

impl SubscriberHandle {
    fn new(sender: UnboundedSender<MessageRecord>) -> Self {
        Self { sender }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishing_new_messages_into_topic_increases_its_offset() {
        let mut topic = Topic::new("topic-1", 5);
        assert_eq!(topic.next_offset, 0);

        topic.publish(b"test-message-1".to_vec());
        assert_eq!(topic.next_offset, 1);
        topic.publish(b"test-message-2".to_vec());
        assert_eq!(topic.next_offset, 2);
        topic.publish(b"test-message-3".to_vec());
        assert_eq!(topic.next_offset, 3);

        let offsets = topic.log.iter().map(|m| m.offset).collect::<Vec<_>>();
        assert_eq!(offsets, vec![0, 1, 2]);
    }

    #[test]
    fn publishing_drops_old_messages_based_on_retention() {
        let mut topic = Topic::new("topic-1", 3);

        topic.publish(vec![1]);
        topic.publish(vec![2]);
        topic.publish(vec![3]);
        topic.publish(vec![4]);
        topic.publish(vec![5]);

        let messages: Vec<u8> = topic.log.iter().map(|m| m.payload[0]).collect();
        assert_eq!(messages, vec![3, 4, 5]);
        assert_eq!(topic.log.len(), 3);
        assert_eq!(topic.next_offset, 5);
    }

    #[test]
    fn replies_retained_messages_when_new_client_subscribe_to_topic() {
        let mut topic = Topic::new("topic-1", 3);

        topic.publish(vec![1]);
        topic.publish(vec![2]);

        let mut subscription = topic.subscribe(ClientId::new_v4());

        let mut messages = vec![];
        subscription.receiver.blocking_recv_many(&mut messages, 2);

        let payloads: Vec<u8> = messages.iter().map(|m| m.payload[0]).collect();
        assert_eq!(payloads, vec![1, 2]);
    }
}
