use crate::protocol::codec::{
    get_u16_as_string, get_u32_as_vec, get_uuid, put_u16_len_string, put_u32_len_vec, put_uuid,
};
use crate::topic::{ClientId, TopicName};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(PartialEq, Debug)]
pub enum Request {
    Ping,
    AddTopic {
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
const PUBLISH_TYPE: u8 = 0x05;
const SUBSCRIBE_TYPE: u8 = 0x07;
const UNSUBSCRIBE_TYPE: u8 = 0x09;

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
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::from(vec![PING_TYPE].as_slice());
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode PING request")
            .expect("Empty request");
        assert_eq!(Request::Ping, request);
    }

    #[test]
    fn decode_add_topic_request_test() {
        let topic = "test-topic-name".to_string();

        let mut bytes = BytesMut::from(vec![ADD_TOPIC_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());

        let mut codec = RequestCodec;
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode ADD_TOPIC request")
            .expect("Empty request");

        assert_eq!(Request::AddTopic { topic }, request);
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
        let client_id = Uuid::new_v4();

        let mut bytes = BytesMut::from(vec![SUBSCRIBE_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_slice(client_id.as_bytes());

        let mut codec = RequestCodec;
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode SUBSCRIBE request")
            .expect("Empty request");

        assert_eq!(Request::Subscribe { topic, client_id }, request);
    }

    #[test]
    fn decode_unsubscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = Uuid::new_v4();

        let mut bytes = BytesMut::from(vec![UNSUBSCRIBE_TYPE].as_slice());
        bytes.put_u16(topic.len() as u16);
        bytes.put_slice(topic.as_bytes());
        bytes.put_slice(client_id.as_bytes());

        let mut codec = RequestCodec;
        let request = codec
            .decode(&mut bytes)
            .expect("Failed to decode SUBSCRIBE request")
            .expect("Empty request");

        assert_eq!(Request::Unsubscribe { topic, client_id }, request);
    }

    #[test]
    fn encode_ping_request_test() {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Ping, &mut bytes)
            .expect("Failed to encode PING request");
        assert_eq!(vec![PING_TYPE], bytes);
    }

    #[test]
    fn encode_add_topic_request_test() {
        let topic = "test-topic-name".to_string();

        let mut request = BytesMut::from(vec![ADD_TOPIC_TYPE].as_slice());
        request.put_u16(topic.len() as u16);
        request.put_slice(topic.as_bytes());
        let request = request.freeze();

        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::AddTopic { topic }, &mut bytes)
            .expect("Failed to encode ADD_TOPIC request");

        assert_eq!(request, bytes);
    }

    #[test]
    fn encode_publish_request_test() {
        let topic = "test-topic-name".to_string();
        let payload = b"test-payload".to_vec();

        let mut request = BytesMut::from(vec![PUBLISH_TYPE].as_slice());
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
        let client_id = Uuid::new_v4();

        let mut request = BytesMut::from(vec![SUBSCRIBE_TYPE].as_slice());
        request.put_u16(topic.len() as u16);
        request.put_slice(topic.as_bytes());
        request.put_slice(client_id.as_bytes());
        let request = request.freeze();

        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Subscribe { topic, client_id }, &mut bytes)
            .expect("Failed to encode SUBSCRIBE request");

        assert_eq!(request, bytes);
    }

    #[test]
    fn encode_unsubscribe_request_test() {
        let topic = "test-topic-name".to_string();
        let client_id = Uuid::new_v4();

        let mut request = BytesMut::from(vec![UNSUBSCRIBE_TYPE].as_slice());
        request.put_u16(topic.len() as u16);
        request.put_slice(topic.as_bytes());
        request.put_slice(client_id.as_bytes());
        let request = request.freeze();

        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Unsubscribe { topic, client_id }, &mut bytes)
            .expect("Failed to encode SUBSCRIBE request");

        assert_eq!(request, bytes);
    }
}
