use bytes::{Buf, BufMut, BytesMut};

pub fn get_u16_as_string(src: &mut BytesMut, name: &str) -> std::io::Result<String> {
    let value_len = src.get_u16() as usize;
    if src.len() < value_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Buffer too short for {name}"),
        ));
    }
    let value = src.split_to(value_len);
    String::from_utf8(value.to_vec()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8 in {name}"),
        )
    })
}

pub fn get_u32_as_vec(src: &mut BytesMut, name: &str) -> std::io::Result<Vec<u8>> {
    let value_len = src.get_u32() as usize;
    if src.len() < value_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Buffer too short for {name}"),
        ));
    }
    Ok(src.split_to(value_len).to_vec())
}

pub fn put_u16_len_string(dst: &mut BytesMut, value: &str) {
    dst.put_u16(value.len() as u16);
    dst.put_slice(value.as_bytes());
}

pub fn put_u32_len_vec(dst: &mut BytesMut, value: &[u8]) {
    dst.put_u32(value.len() as u32);
    dst.put_slice(value);
}
