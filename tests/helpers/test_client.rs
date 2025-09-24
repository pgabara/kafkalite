use futures::{SinkExt, StreamExt};
use kafkalite::protocol::request::{Request, RequestCodec};
use kafkalite::protocol::response::{Response, ResponseCodec};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

pub struct TestClient {
    pub client_id: Uuid,
    receiver: Receiver<Response>,
    sender: Sender<Request>,
}

impl TestClient {
    pub async fn connect(addr: SocketAddr) -> Self {
        let socket = TcpStream::connect(addr)
            .await
            .expect("Failed to connect to broker");
        let (read_half, write_half) = tokio::io::split(socket);
        let receiver = Self::register_response_receiver(read_half);
        let sender = Self::register_request_sender(write_half);

        let client_id = Uuid::new_v4();

        Self {
            client_id,
            receiver,
            sender,
        }
    }

    fn register_response_receiver(read_half: ReadHalf<TcpStream>) -> Receiver<Response> {
        let mut reader = FramedRead::new(read_half, ResponseCodec);
        let (sender, receiver) = tokio::sync::mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                if let Some(response) = reader.next().await {
                    let response = response.expect("Failed to receive response");
                    sender
                        .send(response)
                        .await
                        .expect("Failed to send response");
                } else {
                    break;
                }
            }
        });
        receiver
    }

    fn register_request_sender(write_half: WriteHalf<TcpStream>) -> Sender<Request> {
        let mut writer = FramedWrite::new(write_half, RequestCodec);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                writer.send(request).await.expect("Failed to send request");
            }
        });
        sender
    }

    pub async fn send_and_receive(&mut self, request: Request) -> Response {
        self.sender
            .send(request)
            .await
            .expect("Failed to send data to broker");
        tokio::time::timeout(Duration::from_secs(1), self.receiver.recv())
            .await
            .expect("Timed out waiting for response")
            .expect("Returned end of stream")
    }

    pub async fn receive(&mut self, num_of_messages: u8) -> Vec<Response> {
        let mut responses = Vec::new();
        for _ in 0..num_of_messages {
            let response = tokio::time::timeout(Duration::from_secs(1), self.receiver.recv())
                .await
                .expect("Timed out waiting for response")
                .expect("Returned end of stream");
            responses.push(response);
        }
        responses
    }

    pub async fn receive_no_messages(&mut self, wait_time: Duration) -> bool {
        tokio::time::timeout(wait_time, self.receiver.recv())
            .await
            .is_err()
    }

    pub async fn check_is_connection_closed(&mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(1), self.receiver.recv())
            .await
            .expect("Timed out waiting for response")
            .is_none()
    }
}
