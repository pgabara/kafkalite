use crate::protocol::response::Response;
use crate::server::BrokerResponse;
use crate::topic::TopicManager;

pub async fn handle_request<T>(topic_manager: &T) -> BrokerResponse
where
    T: TopicManager,
{
    tracing::debug!("Listing all topics");
    let topics = topic_manager.list_topics().await;
    BrokerResponse::BasicResponse(Response::TopicsList { topics })
}
