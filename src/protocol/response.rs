use crate::protocol::codec::{
    get_u16_as_string, get_u32_as_vec, get_vec_of_strings, put_u16_len_string, put_u32_len_vec,
    put_vec_of_strings,
};
use crate::topic::TopicName;
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(PartialEq, Debug, Clone)]
pub enum Response {
    Error { message: String },
    Pong,
    Ack,
    Nack,
    Message { topic: String, payload: Vec<u8> },
    TopicsList { topics: Vec<TopicName> },
}

const ERROR_TYPE: u8 = 0x00;
const PONG_TYPE: u8 = 0x02;
const ACK_TYPE: u8 = 0x04;
const NACK_TYPE: u8 = 0x06;
const MESSAGE_TYPE: u8 = 0x08;
const TOPICS_LIST_TYPE: u8 = 0x10;

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
            ERROR_TYPE => {
                let message = get_u16_as_string(src, "message")?;
                Ok(Some(Response::Error { message }))
            }
            PONG_TYPE => Ok(Some(Response::Pong)),
            ACK_TYPE => Ok(Some(Response::Ack)),
            NACK_TYPE => Ok(Some(Response::Nack)),
            MESSAGE_TYPE => {
                let topic = get_u16_as_string(src, "topic")?;
                let payload = get_u32_as_vec(src, "payload")?;
                let response = Response::Message { topic, payload };
                Ok(Some(response))
            }
            TOPICS_LIST_TYPE => {
                let topics = get_vec_of_strings(src, "topics")?;
                let response = Response::TopicsList { topics };
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
            Response::Error { message } => {
                dst.put_u8(ERROR_TYPE);
                put_u16_len_string(dst, &message);
            }
            Response::Pong => dst.put_u8(PONG_TYPE),
            Response::Ack => dst.put_u8(ACK_TYPE),
            Response::Nack => dst.put_u8(NACK_TYPE),
            Response::Message { topic, payload } => {
                dst.put_u8(MESSAGE_TYPE);
                put_u16_len_string(dst, &topic);
                put_u32_len_vec(dst, &payload);
            }
            Response::TopicsList { topics } => {
                dst.put_u8(TOPICS_LIST_TYPE);
                put_vec_of_strings(dst, topics.as_slice());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn failed_on_decoding_unsupported_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::from(vec![0xFF].as_slice());
        let response = codec.decode(&mut bytes);
        assert!(response.is_err());
    }

    #[test]
    fn decode_pong_response_test() {
        let mut bytes = BytesMut::from(vec![PONG_TYPE].as_slice());
        decode_response_test(&mut bytes, Response::Pong);
    }

    #[test]
    fn decode_ack_response_test() {
        let mut bytes = BytesMut::from(vec![ACK_TYPE].as_slice());
        decode_response_test(&mut bytes, Response::Ack);
    }

    #[test]
    fn decode_nack_response_test() {
        let mut bytes = BytesMut::from(vec![NACK_TYPE].as_slice());
        decode_response_test(&mut bytes, Response::Nack);
    }

    #[test]
    fn decode_message_response_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut bytes = BytesMut::from(vec![MESSAGE_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_u32(payload.len() as u32);
        bytes.put_slice(payload.as_slice());

        decode_response_test(&mut bytes, Response::Message { topic, payload });
    }

    #[test]
    fn decode_topics_list_response_test() {
        let topics = vec![
            TopicName::from("test-topic-name-1"),
            TopicName::from("test-topic-name-2"),
            TopicName::from("test-topic-name-3"),
        ];

        let mut bytes = BytesMut::from(vec![TOPICS_LIST_TYPE].as_slice());
        bytes.put_u16(topics.len() as u16);
        for topic in topics.iter() {
            bytes.put_u16(topic.len() as u16);
            bytes.put_slice(topic.as_bytes());
        }

        decode_response_test(&mut bytes, Response::TopicsList { topics });
    }

    #[test]
    fn encode_pong_response_test() {
        let expected_bytes = BytesMut::from(vec![PONG_TYPE].as_slice()).freeze();
        encode_response_test(Response::Pong, expected_bytes);
    }

    #[test]
    fn encode_ack_response_test() {
        let expected_bytes = BytesMut::from(vec![ACK_TYPE].as_slice()).freeze();
        encode_response_test(Response::Ack, expected_bytes);
    }

    #[test]
    fn encode_nack_response_test() {
        let expected_bytes = BytesMut::from(vec![NACK_TYPE].as_slice()).freeze();
        encode_response_test(Response::Nack, expected_bytes);
    }

    #[test]
    fn encode_message_response_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut expected_bytes = BytesMut::from(vec![MESSAGE_TYPE].as_slice());
        expected_bytes.put_u16(topic.len() as u16);
        expected_bytes.put_slice(topic.as_bytes());
        expected_bytes.put_u32(payload.len() as u32);
        expected_bytes.put_slice(payload.as_slice());
        let expected_bytes = expected_bytes.freeze();

        encode_response_test(Response::Message { topic, payload }, expected_bytes);
    }

    #[test]
    fn encode_topics_list_response_test() {
        let topics = vec![
            TopicName::from("test-topic-name-1"),
            TopicName::from("test-topic-name-2"),
            TopicName::from("test-topic-name-3"),
        ];

        let mut expected_bytes = BytesMut::from(vec![TOPICS_LIST_TYPE].as_slice());
        expected_bytes.put_u16(topics.len() as u16);
        for topic in topics.iter() {
            expected_bytes.put_u16(topic.len() as u16);
            expected_bytes.put_slice(topic.as_bytes());
        }
        let expected_bytes = expected_bytes.freeze();

        encode_response_test(Response::TopicsList { topics }, expected_bytes);
    }

    fn decode_response_test(bytes: &mut BytesMut, expected_response: Response) {
        let mut codec = ResponseCodec;
        let request = codec
            .decode(bytes)
            .expect("Failed to decode response")
            .expect("Empty response");
        assert_eq!(
            expected_response, request,
            "failed to decode {:?} response",
            request
        );
    }

    fn encode_response_test(response: Response, expected_bytes: Bytes) {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(response.clone(), &mut bytes)
            .expect("Failed to encode response");
        assert_eq!(
            expected_bytes, bytes,
            "failed to encode {:?} response",
            response
        );
    }
}
