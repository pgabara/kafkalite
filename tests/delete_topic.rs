pub mod helpers;

use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;

#[tokio::test]
async fn return_error_if_topic_is_not_found() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut test_client = test_client::TestClient::connect(test_broker.socket_addr).await;

    let delete_topic = Request::DeleteTopic {
        topic: "test-topic".to_string(),
    };
    let response = test_client.send_and_receive(delete_topic).await;

    let expected_response = Response::Error {
        message: "Topic test-topic not found".to_string(),
    };
    assert_eq!(expected_response, response);

    test_broker.stop().await;
}

#[tokio::test]
async fn delete_topic() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut test_client_1 = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut test_client_2 = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic = Request::AddTopic {
        topic: "test-topic".to_string(),
    };
    let ack = test_client_1.send_and_receive(add_topic).await;
    assert_eq!(Response::Ack, ack);

    let delete_topic = Request::DeleteTopic {
        topic: "test-topic".to_string(),
    };
    let ack = test_client_2.send_and_receive(delete_topic).await;
    assert_eq!(Response::Ack, ack);

    let expected_topics = Response::TopicsList { topics: vec![] };

    let topics = test_client_1.send_and_receive(Request::ListTopics).await;
    assert_eq!(expected_topics, topics);

    let topics = test_client_2.send_and_receive(Request::ListTopics).await;
    assert_eq!(expected_topics, topics);

    test_broker.stop().await;
}
