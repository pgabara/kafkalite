use crate::protocol::codec::{
    get_u16_as_string, get_u32_as_vec, put_u16_len_string, put_u32_len_vec,
};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(PartialEq, Debug)]
pub enum Response {
    Pong,
    Ack,
    Message { topic: String, payload: Vec<u8> },
}

pub struct ResponseCodec;

impl Decoder for ResponseCodec {
    type Item = Response;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let response_type = src.get_u8();
        match response_type {
            0x02 => Ok(Some(Response::Pong)),
            0x04 => Ok(Some(Response::Ack)),
            0x06 => {
                let topic = get_u16_as_string(src, "topic")?;
                let payload = get_u32_as_vec(src, "payload")?;
                let response = Response::Message { topic, payload };
                Ok(Some(response))
            }
            _ => {
                let unknown_request_type =
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown response type");
                Err(unknown_request_type)
            }
        }
    }
}

impl Encoder<Response> for ResponseCodec {
    type Error = std::io::Error;

    fn encode(&mut self, response: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match response {
            Response::Pong => dst.put_u8(0x02),
            Response::Ack => dst.put_u8(0x04),
            Response::Message { topic, payload } => {
                dst.put_u8(0x06);
                put_u16_len_string(dst, &topic);
                put_u32_len_vec(dst, &payload);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_on_decoding_unsupported_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::from(vec![0xFF].as_slice());
        let response = codec.decode(&mut bytes);
        assert!(response.is_err());
    }

    #[test]
    fn decode_pong_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::from(vec![0x02].as_slice());
        let response = codec
            .decode(&mut bytes)
            .expect("Failed to decode PONG response")
            .expect("Empty response");
        assert_eq!(Response::Pong, response);
    }

    #[test]
    fn decode_ack_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::from(vec![0x04].as_slice());
        let response = codec
            .decode(&mut bytes)
            .expect("Failed to decode ACK response")
            .expect("Empty response");
        assert_eq!(Response::Ack, response);
    }

    #[test]
    fn decode_message_response_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut bytes = BytesMut::from(vec![0x06].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_u32(payload.len() as u32);
        bytes.put_slice(payload.as_slice());

        let mut codec = ResponseCodec;
        let response = codec
            .decode(&mut bytes)
            .expect("Failed to decode MESSAGE response")
            .expect("Empty response");

        assert_eq!(Response::Message { topic, payload }, response);
    }

    #[test]
    fn encode_pong_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Response::Pong, &mut bytes)
            .expect("Failed to encode PING response");
        assert_eq!(vec![0x02], bytes);
    }

    #[test]
    fn encode_ack_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Response::Ack, &mut bytes)
            .expect("Failed to encode ACK response");
        assert_eq!(vec![0x04], bytes);
    }

    #[test]
    fn encode_message_response_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut response = BytesMut::from(vec![0x06].as_slice());
        response.put_u16(topic.len() as u16);
        response.put_slice(topic.as_bytes());
        response.put_u32(payload.len() as u32);
        response.put_slice(payload.as_slice());
        let response = response.freeze();

        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Response::Message { topic, payload }, &mut bytes)
            .expect("Failed to encode MESSAGE response");

        assert_eq!(response, bytes);
    }
}
