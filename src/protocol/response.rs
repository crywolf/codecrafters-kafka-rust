pub mod api_versions;
pub mod describe_topic_partitions;

// The APIVersions response uses the "v0" header format, while all other responses use the "v1" header format.
// The response header format (v0) is 4 bytes long, and contains exactly one field: correlation_id
// The response header format (v1) contains an additional tag_buffer field.
// https://kafka.apache.org/protocol.html#protocol_messages

struct HeaderV0 {
    correlation_id: i32,
}

impl HeaderV0 {
    fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }
}

struct HeaderV1 {
    correlation_id: i32,
    tag_buffer: u8,
}

impl HeaderV1 {
    fn new(correlation_id: i32) -> Self {
        Self {
            correlation_id,
            tag_buffer: 0, // tag buffer - An empty tagged field array, represented by a single byte of value 0x00.
        }
    }
}
