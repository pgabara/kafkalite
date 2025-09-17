use crate::protocol::response::Response;
use crate::server::BrokerResponse;
use crate::topic::TopicManager;

pub async fn handle_request<T>(
    topic: String,
    topic_manager: &T,
) -> Result<BrokerResponse, Box<dyn std::error::Error>>
where
    T: TopicManager,
{
    tracing::debug!("Adding new topic: {}", topic);
    let is_topic_added = topic_manager.add_topic(topic).await?;
    let response = if is_topic_added {
        Response::Ack
    } else {
        Response::Nack
    };
    Ok(BrokerResponse::BasicResponse(response))
}
