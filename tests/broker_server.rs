mod helpers;

use crate::helpers::{test_broker, test_client};
use std::time::Duration;

#[tokio::test]
async fn ping_pong_test() {
    let test_broker = test_broker::TestBroker::start().await;
    let mut test_client = test_client::TestClient::connect(test_broker.socket_addr).await;

    let request = bytes::Bytes::from("PING");
    let response = test_client.send(request).await;
    assert_eq!(response, bytes::Bytes::from("PONG"));

    test_broker.stop().await;
}

#[tokio::test]
async fn broker_disconnects_inactive_client_after_timeout_test() {
    let config = kafkalite::config::BrokerConfig::new(0, Duration::from_millis(10));
    let test_broker = test_broker::TestBroker::start_with_config(config).await;

    let mut test_client = test_client::TestClient::connect(test_broker.socket_addr).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        test_client.check_is_connection_closed().await,
        "Expected connection to be closed due to timeout, but send succeeded"
    );
}
