use crate::protocol::response::Response;
use crate::server::BrokerResponse;

pub async fn handle_request() -> Result<BrokerResponse, Box<dyn std::error::Error>> {
    tracing::debug!("Handling PING request");
    Ok(BrokerResponse::BasicResponse(Response::Pong))
}
