use bytes::{BufMut, BytesMut};

use crate::protocol::{ApiKey, ErrorCode, Response};

use super::HeaderV0;

pub struct ApiVersionsApiKeys {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

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
        self.bytes.put_i32(self.header.correlation_id);

        // BODY - ApiVersions Response (Version: 3)
        self.bytes.put_i16(self.error_code.into());

        let mut api_keys = BytesMut::new();
        for item in self.api_keys_vec.iter() {
            api_keys.put_i16(item.api_key.into());
            api_keys.put_i16(item.min_version);
            api_keys.put_i16(item.max_version);
            api_keys.put_u8(0); // _tagged_fields
        }

        // COMPACT_ARRAY: N+1, because null array is represented as 0, empty array (actual length of 0) is represented as 1
        let num_api_keys = self.api_keys_vec.len() as u8 + 1;

        self.bytes.put_u8(num_api_keys);
        self.bytes.put(api_keys);
        self.bytes.put_i32(self.throttle_time_ms);
        self.bytes.put_u8(0); // _tagged_fields
    }
}

impl Response for ApiVersionsResponseV3 {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
