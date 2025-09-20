use crate::protocol::response::Response;
use crate::router::IntoResponse;
use crate::server::BrokerResponse;
use crate::topic::{ClientId, TopicName, TopicPublisher, TopicSubscribeError};

pub async fn handle_request<P>(
    topic_name: TopicName,
    client_id: ClientId,
    publisher: &P,
) -> Result<BrokerResponse, SubscribeError>
where
    P: TopicPublisher,
{
    tracing::debug!(
        "Subscribing new client {} to topic {}",
        client_id,
        topic_name
    );
    let subscription = publisher.subscribe(&topic_name, &client_id).await?;
    Ok(BrokerResponse::StreamedResponse(subscription))
}

pub struct SubscribeError(String);

impl From<TopicSubscribeError> for SubscribeError {
    fn from(e: TopicSubscribeError) -> Self {
        match e {
            TopicSubscribeError::TopicNotFound(topic_name) => {
                SubscribeError(format!("Topic {} not found", topic_name))
            }
        }
    }
}

impl IntoResponse for SubscribeError {
    fn into_response(self) -> Response {
        Response::Error { message: self.0 }
    }
}
