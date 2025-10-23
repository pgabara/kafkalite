pub mod helpers;

use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;
use uuid::Uuid;

#[tokio::test]
async fn broker_returns_error_when_client_tries_to_subscribe_to_unknown_topic() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let subscribe = Request::Subscribe {
        topic: "test-topic".to_string(),
        client_id: Uuid::new_v4(),
        from_offset: Some(0),
    };
    let response = subscriber.send_and_receive(subscribe).await;
    assert_eq!(
        response,
        Response::Error {
            message: "Topic test-topic not found".to_string()
        }
    );
}

#[tokio::test]
async fn broker_sends_messages_from_subscribed_topic_to_client_test() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut publisher = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
        retention: 1,
    };
    let ack = publisher.send_and_receive(add_topic).await;
    assert_eq!(ack, Response::Ack);

    let subscribe = Request::Subscribe {
        topic: "test-topic".to_string(),
        client_id: Uuid::new_v4(),
        from_offset: Some(0),
    };
    let ack = subscriber.send_and_receive(subscribe).await;
    assert_eq!(ack, Response::Ack);

    let messages = tokio::spawn(async move { subscriber.receive(5).await });

    for i in 0..5 {
        let publish = Request::Publish {
            topic: "test-topic".to_string(),
            payload: format!("test-payload-{}", i).into_bytes(),
        };
        let ack = publisher.send_and_receive(publish).await;
        assert_eq!(ack, Response::Ack);
    }

    let messages = messages.await.expect("Failed to receive messages");
    assert_eq!(messages.len(), 5);

    test_broker.stop().await;
}

#[tokio::test]
async fn broker_replies_retained_messages_first_when_client_subscribes_to_topic() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut publisher = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
        retention: 3,
    };
    let ack = publisher.send_and_receive(add_topic).await;
    assert_eq!(ack, Response::Ack);

    for n in 0..5 {
        let publish = Request::Publish {
            topic: "test-topic".to_string(),
            payload: vec![n],
        };
        let ack = publisher.send_and_receive(publish).await;
        assert_eq!(ack, Response::Ack);
    }

    let subscribe = Request::Subscribe {
        topic: "test-topic".to_string(),
        client_id: Uuid::new_v4(),
        from_offset: Some(0),
    };
    let ack = subscriber.send_and_receive(subscribe).await;
    assert_eq!(ack, Response::Ack);

    let messages = subscriber.receive(3).await;

    let expected_messages: Vec<Response> = (2..5)
        .map(|n| Response::Message {
            topic: "test-topic".to_string(),
            payload: vec![n],
            offset: n as u64,
        })
        .collect();
    assert_eq!(messages, expected_messages);

    test_broker.stop().await;
}

#[tokio::test]
async fn broker_replies_retained_messages_from_given_offset_first_when_client_subscribes_to_topic()
{
    let test_broker = test_broker::TestBroker::start().await;

    let mut publisher = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
        retention: 5,
    };
    let ack = publisher.send_and_receive(add_topic).await;
    assert_eq!(ack, Response::Ack);

    for n in 0..5 {
        let publish = Request::Publish {
            topic: "test-topic".to_string(),
            payload: vec![n],
        };
        let ack = publisher.send_and_receive(publish).await;
        assert_eq!(ack, Response::Ack);
    }

    let subscribe = Request::Subscribe {
        topic: "test-topic".to_string(),
        client_id: Uuid::new_v4(),
        from_offset: Some(2),
    };
    let ack = subscriber.send_and_receive(subscribe).await;
    assert_eq!(ack, Response::Ack);

    let messages = subscriber.receive(3).await;

    let expected_messages: Vec<Response> = (2..5)
        .map(|n| Response::Message {
            topic: "test-topic".to_string(),
            payload: vec![n],
            offset: n as u64,
        })
        .collect();
    assert_eq!(messages, expected_messages);

    test_broker.stop().await;
}
