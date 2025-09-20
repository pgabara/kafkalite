use crate::protocol::response::Response;
use crate::server::BrokerResponse;

pub async fn handle_request() -> BrokerResponse {
    tracing::debug!("Handling PING request");
    BrokerResponse::BasicResponse(Response::Pong)
}
