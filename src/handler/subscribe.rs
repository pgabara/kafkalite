use crate::protocol::response::Response;
use crate::router::IntoResponse;
use crate::server::BrokerResponse;
use crate::topic::{ClientId, TopicName, TopicSubscribeError, TopicSubscriber};

pub async fn handle_request<S>(
    topic_name: TopicName,
    client_id: ClientId,
    from_offset: Option<u64>,
    subscriber: &S,
) -> Result<BrokerResponse, SubscribeError>
where
    S: TopicSubscriber,
{
    tracing::debug!(
        "Subscribing new client {} to topic {}",
        client_id,
        topic_name
    );
    let subscription = subscriber
        .subscribe(&topic_name, from_offset, client_id)
        .await?;
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
