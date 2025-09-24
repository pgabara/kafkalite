use crate::protocol::response::Response;
use crate::router::IntoResponse;
use crate::server::BrokerResponse;
use crate::topic::{ClientId, TopicName, TopicSubscribeError, TopicSubscriber};

pub async fn handle_request<S>(
    topic_name: TopicName,
    client_id: ClientId,
    subscriber: &S,
) -> Result<BrokerResponse, UnsubscribeError>
where
    S: TopicSubscriber,
{
    tracing::debug!(
        "Unsubscribing client {} from topic {}",
        client_id,
        topic_name
    );
    subscriber.unsubscribe(&topic_name, client_id).await?;
    Ok(BrokerResponse::BasicResponse(Response::Ack))
}

pub struct UnsubscribeError(String);

impl From<TopicSubscribeError> for UnsubscribeError {
    fn from(e: TopicSubscribeError) -> Self {
        match e {
            TopicSubscribeError::TopicNotFound(topic_name) => {
                UnsubscribeError(format!("Topic {} not found", topic_name))
            }
        }
    }
}

impl IntoResponse for UnsubscribeError {
    fn into_response(self) -> Response {
        Response::Error { message: self.0 }
    }
}
