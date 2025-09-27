use crate::protocol::response::Response;
use crate::router::IntoResponse;
use crate::server::BrokerResponse;
use crate::topic::{TopicName, TopicPublishError, TopicPublisher};

pub async fn handle_request<P>(
    topic: TopicName,
    payload: Vec<u8>,
    publisher: &P,
) -> Result<BrokerResponse, PublishError>
where
    P: TopicPublisher,
{
    tracing::debug!("Publishing to {}", topic);
    publisher.publish(&topic, payload).await?;
    Ok(BrokerResponse::BasicResponse(Response::Ack))
}

pub struct PublishError(String);

impl From<TopicPublishError> for PublishError {
    fn from(e: TopicPublishError) -> Self {
        match e {
            TopicPublishError::TopicNotFound(topic_name) => {
                PublishError(format!("Topic {} not found", topic_name))
            }
        }
    }
}

impl IntoResponse for PublishError {
    fn into_response(self) -> Response {
        Response::Error { message: self.0 }
    }
}
