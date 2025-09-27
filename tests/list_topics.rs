pub mod helpers;

use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;

#[tokio::test]
async fn return_empty_list_of_topics() {
    let test_broker = test_broker::TestBroker::start().await;
    let mut test_client = test_client::TestClient::connect(test_broker.socket_addr).await;

    let response = test_client.send_and_receive(Request::ListTopics).await;
    assert_list_of_topics(response, &[]);

    test_broker.stop().await;
}

#[tokio::test]
async fn return_list_of_topics() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut test_client_1 = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut test_client_2 = test_client::TestClient::connect(test_broker.socket_addr).await;

    let add_topic_1 = Request::AddTopic {
        topic: "test-topic-1".to_string(),
    };
    let ack = test_client_1.send_and_receive(add_topic_1).await;
    assert_eq!(ack, Response::Ack);

    let add_topic_2 = Request::AddTopic {
        topic: "test-topic-2".to_string(),
    };
    let ack = test_client_2.send_and_receive(add_topic_2).await;
    assert_eq!(ack, Response::Ack);

    let expected_topics = vec!["test-topic-1".to_string(), "test-topic-2".to_string()];

    let response = test_client_1.send_and_receive(Request::ListTopics).await;
    assert_list_of_topics(response, &expected_topics);

    let response = test_client_2.send_and_receive(Request::ListTopics).await;
    assert_list_of_topics(response, &expected_topics);

    test_broker.stop().await;
}

fn assert_list_of_topics(response: Response, expected_topics: &[String]) {
    if let Response::TopicsList { mut topics } = response {
        topics.sort();
        assert_eq!(expected_topics, topics);
    } else {
        panic!("Received non TopicsList response: {:?}", response);
    }
}
