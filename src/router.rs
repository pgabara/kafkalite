use crate::handler::{add_topic, ping, publish, subscribe};
use crate::protocol::request::Request;
use crate::server::BrokerResponse;
use crate::topic::{TopicManager, TopicPublisher};

pub async fn route_broker_request<B>(
    request: Request,
    broker: &B,
) -> Result<BrokerResponse, Box<dyn std::error::Error>>
where
    B: TopicManager + TopicPublisher,
{
    match request {
        Request::Ping => ping().await,
        Request::AddTopic { topic } => add_topic(topic, broker).await,
        Request::Publish { topic, payload } => publish(topic, payload, broker).await,
        Request::Subscribe { topic, client_id } => subscribe(topic, client_id, broker).await,
    }
}
