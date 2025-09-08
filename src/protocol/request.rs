use crate::protocol::codec::{
    get_u16_as_string, get_u32_as_vec, put_u16_len_string, put_u32_len_vec,
};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(PartialEq, Debug)]
pub enum Request {
    Ping,
    Publish { topic: String, payload: Vec<u8> },
    Subscribe { topic: String, client_id: String },
}

pub struct RequestCodec;

impl Decoder for RequestCodec {
    type Item = Request;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let request_type = src.get_u8();
        match request_type {
            0x01 => Ok(Some(Request::Ping)),
            0x03 => {
                let topic = get_u16_as_string(src, "topic")?;
                let payload = get_u32_as_vec(src, "payload")?;
                let request = Request::Publish { topic, payload };
                Ok(Some(request))
            }
            0x05 => {
                let topic = get_u16_as_string(src, "topic")?;
                let client_id = get_u16_as_string(src, "client_id")?;
                let request = Request::Subscribe { topic, client_id };
                Ok(Some(request))
            }
            _ => {
                let unknown_request_type =
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown request type");
                Err(unknown_request_type)
            }
        }
    }
}

impl Encoder<Request> for RequestCodec {
    type Error = std::io::Error;

    fn encode(&mut self, request: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match request {
            Request::Ping => dst.put_u8(0x01),
            Request::Publish { topic, payload } => {
                dst.put_u8(0x03);
                put_u16_len_string(dst, &topic);
                put_u32_len_vec(dst, &payload);
            }
            Request::Subscribe { topic, client_id } => {
                dst.put_u8(0x05);
                put_u16_len_string(dst, &topic);
                put_u16_len_string(dst, &client_id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_on_decoding_unsupported_request_test() {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::from(vec![0xFF].as_slice());
        let request = codec.decode(&mut bytes);
        assert!(request.is_err());
    }

    #[test]
    fn decode_ping_request_test() {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::from(vec![0x01].as_slice());
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode PING request")
            .expect("Empty request");
        assert_eq!(Request::Ping, request);
    }

    #[test]
    fn decode_publish_request_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut bytes = BytesMut::from(vec![0x03].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_u32(payload.len() as u32);
        bytes.put_slice(payload.as_slice());

        let mut codec = RequestCodec;
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode PUBLISH request")
            .expect("Empty request");

        assert_eq!(Request::Publish { topic, payload }, request);
    }

    #[test]
    fn decode_subscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = "test-client-id".to_string();

        let mut bytes = BytesMut::from(vec![0x05].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_u16(client_id.len() as u16);
        bytes.put_slice(client_id.as_bytes());

        let mut codec = RequestCodec;
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode SUBSCRIBE request")
            .expect("Empty request");

        assert_eq!(Request::Subscribe { topic, client_id }, request);
    }

    #[test]
    fn encode_ping_request_test() {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Ping, &mut bytes)
            .expect("Failed to encode PING request");
        assert_eq!(vec![0x01], bytes);
    }

    #[test]
    fn encode_publish_request_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut request = BytesMut::from(vec![0x03].as_slice());
        request.put_u16(topic.len() as u16);
        request.put_slice(topic.as_bytes());
        request.put_u32(payload.len() as u32);
        request.put_slice(payload.as_slice());
        let request = request.freeze();

        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Publish { topic, payload }, &mut bytes)
            .expect("Failed to encode PUBLISH request");

        assert_eq!(request, bytes);
    }

    #[test]
    fn encode_subscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = "test-client-id".to_string();

        let mut request = BytesMut::from(vec![0x05].as_slice());
        request.put_u16(topic.len() as u16);
        request.put_slice(topic.as_bytes());
        request.put_u16(client_id.len() as u16);
        request.put_slice(client_id.as_bytes());
        let request = request.freeze();

        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Subscribe { topic, client_id }, &mut bytes)
            .expect("Failed to encode SUBSCRIBE request");

        assert_eq!(request, bytes);
    }
}
