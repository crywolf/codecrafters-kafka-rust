use bytes::{BufMut, BytesMut};

use crate::protocol::{
    types::{self, CompactArray},
    ErrorCode, Response,
};

use super::HeaderV1;

pub struct FetchResponseV16 {
    header: HeaderV1,
    throttle_time_ms: i32,
    error_code: ErrorCode,
    session_id: u32,
    bytes: BytesMut,
}

impl FetchResponseV16 {
    pub fn new(correlation_id: i32, session_id: u32) -> Self {
        let header = HeaderV1::new(correlation_id);

        let mut resp = Self {
            header,
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id,
            bytes: BytesMut::new(),
        };

        resp.serialize();
        resp
    }

    /// Fills the internal `bytes` field with byte representation of the response
    // https://kafka.apache.org/protocol.html#The_Messages_Fetch
    fn serialize(&mut self) {
        // HEADER
        self.bytes.put(self.header.serialize());

        // BODY
        self.bytes.put_i32(self.throttle_time_ms);
        self.bytes.put_i16(self.error_code.into());
        self.bytes.put_u32(self.session_id);
        let mut responses: Vec<ResponseTopic> = Vec::new();
        self.bytes.put(CompactArray::serialize(&mut responses));
        // TODO
        self.bytes.put_u8(0); // tag buffer
    }
}

impl Response for FetchResponseV16 {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

struct ResponseTopic;

impl types::Serialize for ResponseTopic {
    fn serialize(&mut self) -> bytes::Bytes {
        todo!()
    }
}
