use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    types::{self, CompactArray},
    ApiKey, ErrorCode, Response,
};

use super::HeaderV0;

// The APIVersions response uses the "v0" header format, while all other responses use the "v1" header format.
// The response header format (v0) is 4 bytes long, and contains exactly one field: correlation_id
// The response header format (v1) contains an additional tag_buffer field.
// https://kafka.apache.org/protocol.html#protocol_messages
// https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
pub struct ApiVersionsResponseV3 {
    header: HeaderV0,
    error_code: ErrorCode,
    api_keys_vec: Vec<ApiVersionsApiKeys>,
    throttle_time_ms: i32,
    bytes: BytesMut,
}

impl ApiVersionsResponseV3 {
    pub fn new(correlation_id: i32, request_api_version: i16) -> Self {
        let header = HeaderV0::new(correlation_id);

        let api_keys_vec = vec![
            ApiVersionsApiKeys {
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 4,
            },
            ApiVersionsApiKeys {
                api_key: ApiKey::DescribeTopicPartitions,
                min_version: 0,
                max_version: 0,
            },
            ApiVersionsApiKeys {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 16,
            },
        ];

        let mut error_code = ErrorCode::None;
        match request_api_version {
            0..=4 => {}
            _ => error_code = ErrorCode::UnsupportedVersion,
        }

        let mut resp = Self {
            header,
            error_code,
            api_keys_vec,
            throttle_time_ms: 0,
            bytes: BytesMut::new(),
        };

        resp.serialize();
        resp
    }

    /// Fills the internal `bytes` field with byte representation of the response
    // https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    fn serialize(&mut self) {
        // HEADER v0
        self.bytes.put(self.header.serialize());

        // BODY - ApiVersions Response (Version: 3)
        self.bytes.put_i16(self.error_code.into());
        self.bytes
            .put(CompactArray::serialize(&mut self.api_keys_vec));
        self.bytes.put_i32(self.throttle_time_ms);
        self.bytes.put_u8(0); // tag buffer
    }
}

impl Response for ApiVersionsResponseV3 {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct ApiVersionsApiKeys {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

impl types::Serialize for ApiVersionsApiKeys {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_i16(self.api_key.into());
        b.put_i16(self.min_version);
        b.put_i16(self.max_version);
        b.put_u8(0); // tag buffer
        b.freeze()
    }
}
