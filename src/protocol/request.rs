pub mod api_versions;
pub mod describe_topic_partitions;

use bytes::{Buf, Bytes};

use super::types::NullableString;

/// Request Header v2
// https://kafka.apache.org/protocol.html#protocol_messages
#[derive(Debug)]
#[allow(dead_code)]
pub struct HeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

impl HeaderV2 {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let request_api_key = src.get_i16(); // https://kafka.apache.org/protocol.html#protocol_api_keys
        let request_api_version = src.get_i16();
        let correlation_id = src.get_i32();
        let client_id = NullableString::deserialize(src);

        /*
        + tagged_fields: Optional tagged fields
            This can be ignored for now, they're optional tagged fields used to introduce additional features over time
                (https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields).
            The value for this will always be a null byte in this challenge (i.e. no tagged fields are present)
        */
        _ = src.get_u8(); // tag buffer - An empty tagged field array, represented by a single byte of value 0x00.

        Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        }
    }
}
