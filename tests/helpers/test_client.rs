use futures::{SinkExt, StreamExt};
use kafkalite::protocol::{Request, RequestCodec, Response, ResponseCodec};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

pub struct TestClient {
    reader: FramedRead<ReadHalf<TcpStream>, ResponseCodec>,
    writer: FramedWrite<WriteHalf<TcpStream>, RequestCodec>,
}

impl TestClient {
    pub async fn connect(addr: SocketAddr) -> Self {
        let socket = TcpStream::connect(addr)
            .await
            .expect("Failed to connect to broker");
        let (read_half, write_half) = tokio::io::split(socket);
        let reader = FramedRead::new(read_half, ResponseCodec);
        let writer = FramedWrite::new(write_half, RequestCodec);
        Self { reader, writer }
    }

    pub async fn send(&mut self, request: Request) -> Response {
        self.writer
            .send(request)
            .await
            .expect("Failed to send data to broker");
        tokio::time::timeout(Duration::from_secs(1), self.reader.next())
            .await
            .expect("Timed out waiting for response")
            .expect("Returned empty response")
            .expect("Failed to read response")
    }

    pub async fn check_is_connection_closed(&mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(1), self.reader.next())
            .await
            .expect("Timed out waiting for response")
            .is_none()
    }
}
