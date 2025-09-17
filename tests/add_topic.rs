pub mod helpers;

use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;

#[tokio::test]
async fn broker_returns_ack_on_adding_new_topic_test() {
    let test_broker = test_broker::TestBroker::start().await;
    let mut test_client = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
    };
    let ack = test_client.send_and_receive(add_topic).await;
    assert_eq!(ack, Response::Ack);

    test_broker.stop().await;
}

#[tokio::test]
async fn broker_returns_nack_on_adding_two_times_the_same_topic_test() {
    let test_broker = test_broker::TestBroker::start().await;
    let mut test_client = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
    };
    let ack = test_client.send_and_receive(add_topic).await;
    assert_eq!(ack, Response::Ack);

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
    };
    let nack = test_client.send_and_receive(add_topic).await;
    assert_eq!(nack, Response::Nack);

    test_broker.stop().await;
}
