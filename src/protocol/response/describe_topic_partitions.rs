use bytes::{BufMut, BytesMut};

use crate::protocol::{ErrorCode, Response};

use super::HeaderV1;

pub struct DescribeTopicPartitionsResponseV0 {
    header: HeaderV1,
    throttle_time_ms: i32,
    topics: Vec<Topic>,
    next_cursor: u8,
    bytes: BytesMut,
}

impl DescribeTopicPartitionsResponseV0 {
    pub fn new(correlation_id: i32, topics: Vec<Topic>) -> Self {
        let header = HeaderV1::new(correlation_id);

        let mut resp = Self {
            header,
            throttle_time_ms: 0,
            topics,
            next_cursor: 0xFF,
            bytes: BytesMut::new(),
        };

        resp.serialize();
        resp
    }

    /// Fills the internal `bytes` field with byte representation of the response
    // https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions
    fn serialize(&mut self) {
        // HEADER v1
        self.bytes.put_i32(self.header.correlation_id);
        self.bytes.put_u8(self.header.tag_buffer);

        // BODY
        self.bytes.put_i32(self.throttle_time_ms);

        // topics: COMPACT ARRAY
        let mut topics = BytesMut::new();
        for item in self.topics.iter() {
            topics.put_i16(item.error_code.into());
            // COMPACT_STRING
            let len = item.name.len() as u8 + 1;
            topics.put_u8(len);
            topics.put(item.name.as_bytes());

            topics.put(
                hex::decode(item.topic_id.replace('-', ""))
                    .expect("valid UUID string")
                    .as_ref(),
            );
            topics.put_u8(item.is_internal.into());

            // empty COMPACT_ARRAY
            let num_partitions = item.partitions.len() as u8 + 1;
            topics.put_u8(num_partitions);
            topics.put_i32(item.topic_authorized_operations);
            topics.put_u8(0); // tag buffer
        }

        // COMPACT_ARRAY: N+1, because null array is represented as 0, empty array (actual length of 0) is represented as 1
        let num_topics = self.topics.len() as u8 + 1;
        self.bytes.put_u8(num_topics);
        self.bytes.put(topics);

        self.bytes.put_u8(self.next_cursor);
        self.bytes.put_u8(0); // tag buffer
    }
}

impl Response for DescribeTopicPartitionsResponseV0 {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct Topic {
    pub error_code: ErrorCode,
    pub name: String,     // COMPACT_NULLABLE_STRING
    pub topic_id: String, // UUID
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    pub topic_authorized_operations: i32, // A 4-byte integer (bitfield) representing the authorized operations for this topic.
}

pub struct Partition;
