use futures_util::SinkExt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct TestClient {
    socket: Framed<TcpStream, LengthDelimitedCodec>,
}

impl TestClient {
    pub async fn connect(addr: SocketAddr) -> Self {
        let socket = TcpStream::connect(addr)
            .await
            .expect("Failed to connect to broker");
        let socket = Framed::new(socket, LengthDelimitedCodec::new());
        Self { socket }
    }

    pub async fn send(&mut self, data: bytes::Bytes) -> bytes::Bytes {
        self.socket
            .send(data)
            .await
            .expect("Failed to send data to broker");
        tokio::time::timeout(Duration::from_secs(1), self.socket.next())
            .await
            .expect("Timed out waiting for response")
            .expect("Returned empty response")
            .expect("Failed to read response")
            .freeze()
    }

    pub async fn check_is_connection_closed(&mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(1), self.socket.next())
            .await
            .expect("Timed out waiting for response")
            .is_none()
    }
}
