use crate::protocol::codec::{
    get_u16_as_string, get_u32_as_vec, get_uuid, put_u16_len_string, put_u32_len_vec, put_uuid,
};
use crate::topic::{ClientId, TopicName};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(PartialEq, Debug, Clone)]
pub enum Request {
    Ping,
    AddTopic {
        topic: TopicName,
    },
    ListTopics,
    DeleteTopic {
        topic: TopicName,
    },
    Publish {
        topic: TopicName,
        payload: Vec<u8>,
    },
    Subscribe {
        topic: TopicName,
        client_id: ClientId,
    },
    Unsubscribe {
        topic: TopicName,
        client_id: ClientId,
    },
}

const PING_TYPE: u8 = 0x01;
const ADD_TOPIC_TYPE: u8 = 0x03;
const LIST_TOPICS_TYPE: u8 = 0x05;
const DELETE_TOPIC_TYPE: u8 = 0x07;
const PUBLISH_TYPE: u8 = 0x09;
const SUBSCRIBE_TYPE: u8 = 0x11;
const UNSUBSCRIBE_TYPE: u8 = 0x13;

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
            PING_TYPE => Ok(Some(Request::Ping)),
            ADD_TOPIC_TYPE => {
                let topic = get_u16_as_string(src, "topic")?;
                Ok(Some(Request::AddTopic { topic }))
            }
            LIST_TOPICS_TYPE => Ok(Some(Request::ListTopics)),
            DELETE_TOPIC_TYPE => {
                let topic = get_u16_as_string(src, "topic")?;
                Ok(Some(Request::DeleteTopic { topic }))
            }
            PUBLISH_TYPE => {
                let topic = get_u16_as_string(src, "topic")?;
                let payload = get_u32_as_vec(src, "payload")?;
                let request = Request::Publish { topic, payload };
                Ok(Some(request))
            }
            SUBSCRIBE_TYPE => {
                let topic = get_u16_as_string(src, "topic")?;
                let client_id = get_uuid(src, "client_id")?;
                let request = Request::Subscribe { topic, client_id };
                Ok(Some(request))
            }
            UNSUBSCRIBE_TYPE => {
                let topic = get_u16_as_string(src, "topic")?;
                let client_id = get_uuid(src, "client_id")?;
                let request = Request::Unsubscribe { topic, client_id };
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
            Request::Ping => dst.put_u8(PING_TYPE),
            Request::AddTopic { topic } => {
                dst.put_u8(ADD_TOPIC_TYPE);
                put_u16_len_string(dst, &topic);
            }
            Request::ListTopics => {
                dst.put_u8(LIST_TOPICS_TYPE);
            }
            Request::DeleteTopic { topic } => {
                dst.put_u8(DELETE_TOPIC_TYPE);
                put_u16_len_string(dst, &topic);
            }
            Request::Publish { topic, payload } => {
                dst.put_u8(PUBLISH_TYPE);
                put_u16_len_string(dst, &topic);
                put_u32_len_vec(dst, &payload);
            }
            Request::Subscribe { topic, client_id } => {
                dst.put_u8(SUBSCRIBE_TYPE);
                put_u16_len_string(dst, &topic);
                put_uuid(dst, client_id);
            }
            Request::Unsubscribe { topic, client_id } => {
                dst.put_u8(UNSUBSCRIBE_TYPE);
                put_u16_len_string(dst, &topic);
                put_uuid(dst, client_id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    #[test]
    fn failed_on_decoding_unsupported_request_test() {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::from(vec![0xFF].as_slice());
        let request = codec.decode(&mut bytes);
        assert!(request.is_err());
    }

    #[test]
    fn decode_ping_request_test() {
        let mut bytes = BytesMut::from(vec![PING_TYPE].as_slice());
        decode_request_test(&mut bytes, Request::Ping);
    }

    #[test]
    fn decode_add_topic_request_test() {
        let topic = "test-topic-name".to_string();

        let mut bytes = BytesMut::from(vec![ADD_TOPIC_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());

        decode_request_test(&mut bytes, Request::AddTopic { topic });
    }

    #[test]
    fn decode_list_topics_request_test() {
        let mut bytes = BytesMut::from(vec![LIST_TOPICS_TYPE].as_slice());
        decode_request_test(&mut bytes, Request::ListTopics);
    }

    #[test]
    fn decode_delete_topic_request_test() {
        let topic = "test-topic-name".to_string();

        let mut bytes = BytesMut::from(vec![DELETE_TOPIC_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());

        decode_request_test(&mut bytes, Request::DeleteTopic { topic });
    }

    #[test]
    fn decode_publish_request_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut bytes = BytesMut::from(vec![PUBLISH_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_u32(payload.len() as u32);
        bytes.put_slice(payload.as_slice());

        decode_request_test(&mut bytes, Request::Publish { topic, payload });
    }

    #[test]
    fn decode_subscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = Uuid::new_v4();

        let mut bytes = BytesMut::from(vec![SUBSCRIBE_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_slice(client_id.as_bytes());

        decode_request_test(&mut bytes, Request::Subscribe { topic, client_id });
    }

    #[test]
    fn decode_unsubscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = Uuid::new_v4();

        let mut bytes = BytesMut::from(vec![UNSUBSCRIBE_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_slice(client_id.as_bytes());

        decode_request_test(&mut bytes, Request::Unsubscribe { topic, client_id });
    }

    #[test]
    fn encode_ping_request_test() {
        let expected_bytes = BytesMut::from(vec![PING_TYPE].as_slice()).freeze();
        encode_request_test(Request::Ping, expected_bytes)
    }

    #[test]
    fn encode_add_topic_request_test() {
        let topic = "test-topic-name".to_string();

        let mut expected_bytes = BytesMut::from(vec![ADD_TOPIC_TYPE].as_slice());
        expected_bytes.put_u16(topic.len() as u16);
        expected_bytes.put_slice(topic.as_bytes());
        let expected_bytes = expected_bytes.freeze();

        encode_request_test(Request::AddTopic { topic }, expected_bytes);
    }

    #[test]
    fn encode_list_topics_request_test() {
        let expected_bytes = BytesMut::from(vec![LIST_TOPICS_TYPE].as_slice()).freeze();
        encode_request_test(Request::ListTopics, expected_bytes)
    }

    #[test]
    fn encode_delete_topic_request_test() {
        let topic = "test-topic-name".to_string();

        let mut expected_bytes = BytesMut::from(vec![DELETE_TOPIC_TYPE].as_slice());
        expected_bytes.put_u16(topic.len() as u16);
        expected_bytes.put_slice(topic.as_bytes());
        let expected_bytes = expected_bytes.freeze();

        encode_request_test(Request::DeleteTopic { topic }, expected_bytes);
    }

    #[test]
    fn encode_publish_request_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut expected_bytes = BytesMut::from(vec![PUBLISH_TYPE].as_slice());
        expected_bytes.put_u16(topic.len() as u16);
        expected_bytes.put_slice(topic.as_bytes());
        expected_bytes.put_u32(payload.len() as u32);
        expected_bytes.put_slice(payload.as_slice());
        let expected_bytes = expected_bytes.freeze();

        encode_request_test(Request::Publish { topic, payload }, expected_bytes);
    }

    #[test]
    fn encode_subscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = Uuid::new_v4();

        let mut expected_bytes = BytesMut::from(vec![SUBSCRIBE_TYPE].as_slice());
        expected_bytes.put_u16(topic.len() as u16);
        expected_bytes.put_slice(topic.as_bytes());
        expected_bytes.put_slice(client_id.as_bytes());
        let expected_bytes = expected_bytes.freeze();

        encode_request_test(Request::Subscribe { topic, client_id }, expected_bytes);
    }

    #[test]
    fn encode_unsubscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = Uuid::new_v4();

        let mut expected_bytes = BytesMut::from(vec![UNSUBSCRIBE_TYPE].as_slice());
        expected_bytes.put_u16(topic.len() as u16);
        expected_bytes.put_slice(topic.as_bytes());
        expected_bytes.put_slice(client_id.as_bytes());
        let expected_bytes = expected_bytes.freeze();

        encode_request_test(Request::Unsubscribe { topic, client_id }, expected_bytes);
    }

    fn decode_request_test(bytes: &mut BytesMut, expected_request: Request) {
        let mut codec = RequestCodec;
        let request = codec
            .decode(bytes)
            .expect("Failed to decode request")
            .expect("Empty request");
        assert_eq!(
            expected_request, request,
            "failed to decode {:?} request",
            request
        );
    }

    fn encode_request_test(request: Request, expected_bytes: Bytes) {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(request.clone(), &mut bytes)
            .expect("Failed to encode request");
        assert_eq!(
            expected_bytes, bytes,
            "failed to encode {:?} request",
            request
        );
    }
}
