use bytes::{Buf, BufMut, BytesMut};
use uuid::Uuid;

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

pub fn get_u64_option(src: &mut BytesMut, name: &str) -> std::io::Result<Option<u64>> {
    let get_u64 = |src: &mut BytesMut| -> std::io::Result<u64> {
        src.try_get_u64().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Buffer too short for {name}"),
            )
        })
    };

    get_option(src, get_u64)
}

pub fn get_option<T, F>(src: &mut BytesMut, f: F) -> std::io::Result<Option<T>>
where
    F: FnOnce(&mut BytesMut) -> std::io::Result<T>,
{
    let is_some = src
        .try_get_u8()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Buffer too short"));

    let value = if is_some? > 0 {
        let value = f(src)?;
        Some(value)
    } else {
        None
    };

    Ok(value)
}

pub fn get_vec_of_strings(src: &mut BytesMut, name: &str) -> std::io::Result<Vec<String>> {
    let vec_len = src.get_u16() as usize;
    let mut values = Vec::with_capacity(vec_len);

    for _ in 0..vec_len {
        let value = get_u16_as_string(src, name)?;
        values.push(value);
    }

    Ok(values)
}

pub fn get_uuid(src: &mut BytesMut, name: &str) -> std::io::Result<Uuid> {
    let uuid_v4_len = 16;
    if src.len() < uuid_v4_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Buffer too short for {name}"),
        ));
    }
    let value = src.split_to(uuid_v4_len);
    Uuid::from_slice(&value).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UUID V4 in {name}"),
        )
    })
}

pub fn put_u16_len_string(dst: &mut BytesMut, value: &str) {
    dst.put_u16(value.len() as u16);
    dst.put_slice(value.as_bytes());
}

pub fn put_u32_len_vec(dst: &mut BytesMut, value: &[u8]) {
    dst.put_u32(value.len() as u32);
    dst.put_slice(value);
}

pub fn put_vec_of_strings(dst: &mut BytesMut, values: &[String]) {
    dst.put_u16(values.len() as u16);

    for value in values {
        put_u16_len_string(dst, value);
    }
}

pub fn put_uuid(dst: &mut BytesMut, uuid: Uuid) {
    dst.put_slice(uuid.as_bytes());
}

pub fn put_u64_option(dst: &mut BytesMut, value: Option<u64>) {
    let put_u64 = |dst: &mut BytesMut, value: u64| {
        dst.put_u64(value);
    };
    put_option(dst, value, put_u64)
}

pub fn put_option<T, F>(dst: &mut BytesMut, value: Option<T>, f: F)
where
    F: FnOnce(&mut BytesMut, T),
{
    match value {
        Some(value) => {
            dst.put_u8(1);
            f(dst, value)
        }
        None => dst.put_u8(0),
    }
}
