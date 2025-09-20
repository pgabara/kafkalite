use crate::handler::{add_topic, ping, publish, subscribe};
use crate::protocol::request::Request;
use crate::protocol::response::Response;
use crate::server::BrokerResponse;
use crate::topic::{TopicManager, TopicPublisher};

pub async fn route_broker_request<B>(request: Request, broker: &B) -> BrokerResponse
where
    B: TopicManager + TopicPublisher,
{
    match request {
        Request::Ping => ping().await,
        Request::AddTopic { topic } => unwrap_response(add_topic(topic, broker).await),
        Request::Publish { topic, payload } => {
            unwrap_response(publish(topic, payload, broker).await)
        }
        Request::Subscribe { topic, client_id } => {
            unwrap_response(subscribe(topic, client_id, broker).await)
        }
    }
}

fn unwrap_response<E: IntoResponse>(maybe_response: Result<BrokerResponse, E>) -> BrokerResponse {
    maybe_response.unwrap_or_else(|e| BrokerResponse::BasicResponse(e.into_response()))
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}
