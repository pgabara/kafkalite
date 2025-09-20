pub mod helpers;

use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;

#[tokio::test]
async fn broker_returns_error_when_client_tries_to_publish_to_unknown_topic() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut publisher = test_client::TestClient::connect(test_broker.socket_addr).await;

    let publish = Request::Publish {
        topic: "test-topic".to_string(),
        payload: b"test message".to_vec(),
    };
    let ack = publisher.send_and_receive(publish).await;
    assert_eq!(
        ack,
        Response::Error {
            message: "Topic test-topic not found".to_string()
        }
    );
}
