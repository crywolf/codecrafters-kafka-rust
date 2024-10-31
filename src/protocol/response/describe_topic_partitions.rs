use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    types::{self, *},
    ErrorCode, Response,
};

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
        // HEADER
        self.bytes.put(self.header.serialize());
        // BODY
        self.bytes.put_i32(self.throttle_time_ms);
        self.bytes.put(CompactArray::serialize(&mut self.topics));
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

impl types::Serialize for Topic {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(self.error_code.serialize());
        b.put(CompactString::serialize(&self.name));
        b.put(Uuid::serialize(&self.topic_id));
        b.put_u8(self.is_internal.into());
        b.put(CompactArray::serialize(&mut self.partitions));
        b.put_i32(self.topic_authorized_operations);
        b.put(TaggedFields::serialize()); //
        b.freeze()
    }
}

pub struct Partition {
    error_code: ErrorCode,
    partition_index: u32,
    leader_id: u32,
    leader_epoch: u32,
    replicas: Vec<u32>,
    in_sync_replicas: Vec<u32>,
    eligible_leader_replicas: Vec<u32>,
    last_known_eligible_leader_replicas: Vec<u32>,
    off_line_replicas: Vec<u32>,
}

impl Partition {
    pub fn new(
        error_code: ErrorCode,
        partition_index: u32,
        leader_id: u32,
        leader_epoch: u32,
        replicas: Vec<u32>,
        in_sync_replicas: Vec<u32>,
        eligible_leader_replicas: Vec<u32>,
        last_known_eligible_leader_replicas: Vec<u32>,
        off_line_replicas: Vec<u32>,
    ) -> Self {
        Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replicas,
            in_sync_replicas,
            eligible_leader_replicas,
            last_known_eligible_leader_replicas,
            off_line_replicas,
        }
    }
}

impl types::Serialize for Partition {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(self.error_code.serialize());
        b.put_u32(self.partition_index);
        b.put_u32(self.leader_id);
        b.put_u32(self.leader_epoch);
        b.put(CompactArray::serialize(&mut self.replicas));
        b.put(CompactArray::serialize(&mut self.in_sync_replicas));
        b.put(CompactArray::serialize(&mut self.eligible_leader_replicas));
        b.put(CompactArray::serialize(
            &mut self.last_known_eligible_leader_replicas,
        ));
        b.put(CompactArray::serialize(&mut self.off_line_replicas));
        b.put(TaggedFields::serialize()); // tag buffer
        b.freeze()
    }
}

impl types::Serialize for u32 {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::with_capacity(4);
        b.extend_from_slice(&self.to_be_bytes());
        b.freeze()
    }
}
