pub mod request;
pub mod response;
pub mod types;

use bytes::{BufMut, BytesMut};
use num_enum::{IntoPrimitive, TryFromPrimitive};

/// https://kafka.apache.org/protocol.html#protocol_api_keys
#[derive(Clone, Copy, IntoPrimitive, TryFromPrimitive)]
#[repr(i16)]
pub enum ApiKey {
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

/// https://kafka.apache.org/protocol.html#protocol_error_codes
#[derive(Clone, Copy, IntoPrimitive)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
}

/// Response Message is a wrapper around API response with prepended message size
// https://kafka.apache.org/protocol.html#protocol_common
pub struct ResponseMessage {
    bytes: BytesMut,
}

impl ResponseMessage {
    /// Calculates the size of the source API response and prepends it to the response
    pub fn from_bytes(src: &[u8]) -> Self {
        let mut bytes = BytesMut::with_capacity(src.len() + 4);

        let msg_size = 0; // placeholder; will be counted later
        bytes.put_i32(msg_size);

        bytes.extend_from_slice(src);

        let resp_size = bytes.len() as i32 - 4;

        let msg_size_ref = bytes
            .first_chunk_mut::<4>()
            .expect("message size element is present in response header");
        *msg_size_ref = (resp_size).to_be_bytes();

        Self { bytes }
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub trait Response {
    fn as_bytes(&self) -> &[u8];
}
