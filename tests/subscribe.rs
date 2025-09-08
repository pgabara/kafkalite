use crate::helpers::{test_broker, test_client};
use kafkalite::protocol::request::Request;
use kafkalite::protocol::response::Response;

mod helpers;

#[tokio::test]
async fn broker_sends_messages_from_subscribed_topic_to_client_test() {
    let test_broker = test_broker::TestBroker::start().await;

    let mut publisher = test_client::TestClient::connect(test_broker.socket_addr).await;
    let mut subscriber = test_client::TestClient::connect(test_broker.socket_addr).await;

    let subscribe = Request::Subscribe {
        topic: "test-topic".to_string(),
        client_id: "test-client".to_string(),
    };
    let ack = subscriber.send_and_receive(subscribe).await;
    assert_eq!(ack, Response::Ack);

    let messages = tokio::spawn(async move { subscriber.receive(5).await });

    for i in 0..5 {
        let publish = Request::Publish {
            topic: "test-topic".to_string(),
            payload: format!("test-payload-{}", i).into_bytes(),
        };
        publisher.send_and_receive(publish).await;
    }

    let messages = messages.await.expect("Failed to receive messages");
    assert_eq!(messages.len(), 5);

    test_broker.stop().await;
}
