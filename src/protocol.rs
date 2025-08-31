use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(PartialEq, Debug)]
pub enum Request {
    Ping,
}

#[derive(PartialEq, Debug)]
pub enum Response {
    Pong,
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
        }
        Ok(())
    }
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
    fn encode_ping_request_test() {
        let mut codec = RequestCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Request::Ping, &mut bytes)
            .expect("Failed to encode PING request");
        assert_eq!(vec![0x01], bytes);
    }

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
    fn encode_pong_response_test() {
        let mut codec = ResponseCodec;
        let mut bytes = BytesMut::new();
        codec
            .encode(Response::Pong, &mut bytes)
            .expect("Failed to encode PING response");
        assert_eq!(vec![0x02], bytes);
    }
}
