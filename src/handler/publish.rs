use crate::protocol::response::Response;
use crate::server::BrokerResponse;
use crate::topic::{Message, TopicPublisher};

pub async fn handle_request<P>(
    topic: String,
    payload: Vec<u8>,
    publisher: &P,
) -> Result<BrokerResponse, Box<dyn std::error::Error>>
where
    P: TopicPublisher,
{
    tracing::debug!("Publishing to {}", topic);
    let message = Message::new(payload);
    publisher.publish(topic, message).await?;
    Ok(BrokerResponse::BasicResponse(Response::Ack))
}
