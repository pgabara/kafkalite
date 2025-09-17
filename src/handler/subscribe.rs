use crate::server::BrokerResponse;
use crate::topic::TopicPublisher;

pub async fn handle_request<P>(
    topic: String,
    client_id: String,
    publisher: &P,
) -> Result<BrokerResponse, Box<dyn std::error::Error>>
where
    P: TopicPublisher,
{
    tracing::debug!("Subscribing new client {} to topic {}", client_id, topic);
    let subscription = publisher.subscribe(topic, client_id).await?;
    Ok(BrokerResponse::StreamedResponse(subscription))
}
