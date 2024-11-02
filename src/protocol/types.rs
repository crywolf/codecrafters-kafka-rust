use bytes::{Buf, BufMut, Bytes, BytesMut};

// https://kafka.apache.org/protocol.html#protocol_types

pub trait Serialize {
    fn serialize(&mut self) -> Bytes;
}

pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> T;
}

/// Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
/// Then N bytes follow which are the UTF-8 encoding of the character sequence.
pub struct CompactString;

impl CompactString {
    pub fn serialize(s: &str) -> Bytes {
        let mut b = BytesMut::new();
        let len = s.len() as u8 + 1;
        b.put_u8(len);
        b.put(s.as_bytes());
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> String {
        let len = VarInt::deserialize(src); // string length + 1
        let string_len = if len > 1 { len as usize - 1 } else { 0 };
        let bytes = src.slice(..string_len);
        src.advance(string_len);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl Deserialize<String> for CompactString {
    fn deserialize(src: &mut Bytes) -> String {
        Self::deserialize(src)
    }
}

/// Represents a sequence of characters or null. For non-null strings, first the length N is given as an INT16.
/// Then N bytes follow which are the UTF-8 encoding of the character sequence.
/// A null value is encoded with length of -1 and there are no following bytes.
pub struct NullableString;

impl NullableString {
    pub fn deserialize(src: &mut Bytes) -> String {
        let len = src.get_i16();
        let string_len = if len == -1 { 0 } else { len as usize };
        let bytes = src.slice(..string_len);
        src.advance(string_len);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

/// Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure.
/// First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow.
/// A null array is represented with a length of 0.
pub struct CompactArray;

impl CompactArray {
    pub fn serialize<T: Serialize>(items: &mut [T]) -> Bytes {
        let mut b = BytesMut::new();
        // COMPACT ARRAY: N+1, because null array is represented as 0, empty array (actual length of 0) is represented as 1
        let len = items.len() as u8 + 1;
        b.put_u8(len);

        for item in items.iter_mut() {
            b.put(item.serialize());
        }

        b.freeze()
    }

    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Vec<T> {
        let len = VarInt::deserialize(src); // array length + 1
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = U::deserialize(src);
            items.push(item);
        }

        items
    }
}

/// Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure.
/// First, the length N is given as an INT32. Then N instances of type T follow. A null array is represented with a length of -1.
#[allow(dead_code)]
pub struct Array;

#[allow(dead_code)]
impl Array {
    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Vec<T> {
        let len = src.get_i32();
        let items_len = if len == -1 { 0 } else { len as usize };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = U::deserialize(src);
            items.push(item);
        }

        items
    }
}

/// Represents a raw sequence of bytes or null.
/// For non-null values, first the length N is given as an INT32. Then N bytes follow.
/// A null value is encoded with length of -1 and there are no following bytes.
pub struct NullableBytes;

#[allow(dead_code)]
impl NullableBytes {
    pub fn serialize(bytes: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        let len = bytes.len() as i32 + 1;
        b.put_i32(len);
        b.put(bytes);
        b.freeze()
    }

    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Vec<T> {
        let len = src.get_i32();
        let items_len = if len == -1 { 0 } else { len as usize };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = U::deserialize(src);
            items.push(item);
        }
        items
    }
}

/// Represents a raw sequence of bytes.
/// First the length N+1 is given as an UNSIGNED_VARINT. Then N bytes follow. A null object is represented with a length of 0.
pub struct CompactNullableBytes;

#[allow(dead_code)]
impl CompactNullableBytes {
    pub fn serialize(bytes: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        let len = bytes.len() as u8 + 1; // should be varint
        b.put_u8(len);
        b.put(bytes);
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> Vec<u8> {
        let len = VarInt::deserialize(src);
        let bytes_len = if len > 1 { len as usize - 1 } else { 0 };
        let bytes = src.slice(..bytes_len);
        src.advance(bytes_len);
        Vec::from(bytes)
    }
}

pub struct Uuid;

impl Uuid {
    pub fn serialize(s: &str) -> Bytes {
        let mut b = BytesMut::with_capacity(32);
        b.extend_from_slice(&hex::decode(s.replace('-', "")).expect("valid UUID string"));
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> String {
        // 00000000-0000-0000-0000-000000000000
        let mut s = hex::encode(src.slice(..16));
        src.advance(16);
        s.insert(8, '-');
        s.insert(13, '-');
        s.insert(18, '-');
        s.insert(23, '-');
        s
    }
}

pub struct TaggedFields;

impl TaggedFields {
    // Tag buffer - In this challenge an empty tagged field array, represented by a single byte of value 0x00.
    pub fn serialize() -> Bytes {
        let mut b = BytesMut::with_capacity(1);
        b.put_u8(0); // tag buffer
        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> u8 {
        src.get_u8() // tag buffer
    }
}

/// https://protobuf.dev/programming-guides/encoding/#varints
pub struct VarInt;

impl VarInt {
    pub(crate) fn deserialize<T>(buf: &mut T) -> i64
    where
        T: bytes::Buf,
    {
        const MAX_BYTES: usize = 10;
        if buf.remaining() == 0 {
            panic!("buffer is empty")
        }

        let buf_len = buf.remaining();

        let mut b0 = buf.get_i8() as i64;
        let mut res = b0 & 0b0111_1111; // drop the MSB (continuation bit)
        let mut n_bytes = 1;

        while b0 & 0b1000_0000 != 0 && n_bytes <= MAX_BYTES {
            // highest bit (continuation bit) in the first byte is one, get another byte

            if buf.remaining() == 0 {
                if buf_len >= MAX_BYTES {
                    panic!("invalid varint")
                }
                panic!("buffer is too short ({} bytes) or invalid varint", buf_len)
            }

            let b1 = buf.get_i8() as i64;
            if buf.remaining() == 0 && b1 & 0b1000_0000 != 0 {
                // last byte still starts with 1

                if buf_len >= 8 {
                    panic!("invalid varint")
                }
                panic!("buffer is too short ({} bytes) or invalid varint", buf_len)
            }

            // drop the continuation bit and convert to big-endian
            res += (b1 & 0b0111_1111) << 7;

            n_bytes += 1;

            b0 = b1;
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::VarInt;

    #[test]
    #[should_panic]
    fn varint_empty_buf() {
        let mut buf = &[][..];
        assert_eq!(buf.len(), 0);
        _ = VarInt::deserialize(&mut buf);
    }

    #[test]
    fn varint_1_byte() {
        let mut buf = &[0b01101000][..];
        assert_eq!(buf.len(), 1);
        let r = VarInt::deserialize(&mut buf);
        assert_eq!(r, 104);

        let mut buf = &[0b01101000, 0b01101000][..];
        assert_eq!(buf.len(), 2);
        let r = VarInt::deserialize(&mut buf);
        assert_eq!(r, 104);
    }

    #[test]
    fn varint_2_bytes() {
        let mut buf: &[u8] = &[0b10010110, 0b00000001][..];
        assert_eq!(buf.len(), 2);
        let r = VarInt::deserialize(&mut buf);
        assert_eq!(r, 150);
    }
}
