pub mod helpers;

use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn broker_returns_error_when_client_tries_to_unsubscribe_from_not_existing_topic() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let unsubscribe = Request::Unsubscribe {
        topic: "test-topic".to_string(),
        client_id: Uuid::new_v4(),
    };

    let response = subscriber.send_and_receive(unsubscribe).await;
    assert_eq!(
        response,
        Response::Error {
            message: "Topic test-topic not found".to_string()
        }
    );
}

#[tokio::test]
async fn broker_unsubscribes_from_existing_topic() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut publisher = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
        retention: 1,
    };
    let response = publisher.send_and_receive(add_topic).await;
    assert_eq!(response, Response::Ack);

    let subscribe = Request::Subscribe {
        topic: "test-topic".to_string(),
        client_id: subscriber.client_id,
        from_offset: Some(0),
    };
    let response = subscriber.send_and_receive(subscribe).await;
    assert_eq!(response, Response::Ack);

    let publish = Request::Publish {
        topic: "test-topic".to_string(),
        payload: b"test-1".to_vec(),
    };
    let response = publisher.send_and_receive(publish).await;
    assert_eq!(response, Response::Ack);

    let message = subscriber.receive(1).await;
    assert!(!message.is_empty());

    let unsubscribe = Request::Unsubscribe {
        topic: "test-topic".to_string(),
        client_id: subscriber.client_id,
    };
    let response = subscriber.send_and_receive(unsubscribe).await;
    assert_eq!(response, Response::Ack);

    let publish = Request::Publish {
        topic: "test-topic".to_string(),
        payload: b"test-2".to_vec(),
    };
    let response = publisher.send_and_receive(publish).await;
    assert_eq!(response, Response::Ack);

    let no_messages_received = subscriber
        .receive_no_messages(Duration::from_millis(100))
        .await;
    assert!(no_messages_received);

    test_broker.stop().await;
}
