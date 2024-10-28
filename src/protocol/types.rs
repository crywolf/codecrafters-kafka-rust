use bytes::{Buf, BufMut, Bytes, BytesMut};

// https://kafka.apache.org/protocol.html#protocol_types

/// Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT.
/// Then N bytes follow which are the UTF-8 encoding of the character sequence.
pub struct CompactString;

impl CompactString {
    pub fn serialize(s: &str) -> Bytes {
        let len = s.len() as u8 + 1;

        let mut b = BytesMut::new();
        b.put_u8(len);
        b.put(s.as_bytes());

        b.freeze()
    }

    pub fn deserialize(src: &mut Bytes) -> String {
        let len = src.get_u8(); // string length + 1
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

// Represents a sequence of objects of a given type T. Type T can be either a primitive type (e.g. STRING) or a structure.
// First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow.
// A null array is represented with a length of 0.
pub struct CompactArray;

impl CompactArray {
    pub fn serialize<T: Serialize>(items: &mut [T]) -> Bytes {
        // COMPACT ARRAY: N+1, because null array is represented as 0, empty array (actual length of 0) is represented as 1
        let len = items.len() as u8 + 1;

        let mut b = BytesMut::new();
        b.put_u8(len);
        for item in items.iter_mut() {
            b.put(item.serialize());
        }

        b.freeze()
    }

    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Vec<T> {
        let len = src.get_i8(); // array length + 1
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let item = U::deserialize(src);
            items.push(item);
            _ = src.get_u8(); // tag buffer
        }

        items
    }
}

pub trait Serialize {
    fn serialize(&mut self) -> Bytes;
}

pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> T;
}
