use crate::protocol::response::Response;
use crate::router::IntoResponse;
use crate::server::BrokerResponse;
use crate::topic::{TopicManager, TopicName};

pub async fn handle_request<T>(
    topic_name: TopicName,
    topic_manager: &T,
) -> Result<BrokerResponse, DeleteTopicError>
where
    T: TopicManager,
{
    tracing::debug!("Deleting topic: {}", topic_name);
    let is_topic_deleted = topic_manager.delete_topic(&topic_name).await;
    if is_topic_deleted {
        Ok(BrokerResponse::BasicResponse(Response::Ack))
    } else {
        Err(DeleteTopicError(format!("Topic {} not found", topic_name)))
    }
}

pub struct DeleteTopicError(String);

impl IntoResponse for DeleteTopicError {
    fn into_response(self) -> Response {
        Response::Error { message: self.0 }
    }
}
