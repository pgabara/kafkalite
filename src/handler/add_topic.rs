use crate::protocol::response::Response;
use crate::router::IntoResponse;
use crate::server::BrokerResponse;
use crate::topic::{TopicManager, TopicName};

pub async fn handle_request<T>(
    topic_name: TopicName,
    retention: u64,
    topic_manager: &T,
) -> Result<BrokerResponse, AddTopicError>
where
    T: TopicManager,
{
    tracing::debug!("Adding new topic: {}", topic_name);
    let is_topic_added = topic_manager.add_topic(&topic_name, retention).await;
    if is_topic_added {
        Ok(BrokerResponse::BasicResponse(Response::Ack))
    } else {
        Err(AddTopicError(format!(
            "Topic {} already exists",
            topic_name
        )))
    }
}

pub struct AddTopicError(String);

impl IntoResponse for AddTopicError {
    fn into_response(self) -> Response {
        Response::Error { message: self.0 }
    }
}
