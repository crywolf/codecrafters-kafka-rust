use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    self,
    types::{self, CompactArray, Serialize, TaggedFields, Uuid},
    ErrorCode,
};

use super::HeaderV1;

pub struct FetchResponseV16 {
    header: HeaderV1,
    throttle_time_ms: i32,
    error_code: ErrorCode,
    session_id: u32,
    responses: Vec<TopicResponse>,
    bytes: BytesMut,
}

impl FetchResponseV16 {
    pub fn new(correlation_id: i32, session_id: u32, responses: Vec<TopicResponse>) -> Self {
        let header = HeaderV1::new(correlation_id);

        let mut resp = Self {
            header,
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id,
            responses,
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
        self.bytes.put(self.error_code.serialize());
        self.bytes.put_u32(self.session_id);
        self.bytes.put(CompactArray::serialize(&mut self.responses));
        self.bytes.put(TaggedFields::serialize()); // tag buffer
    }
}

impl protocol::Response for FetchResponseV16 {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct TopicResponse {
    topic_id: String, // UUID
    partitions: Vec<TopicPartition>,
}

impl TopicResponse {
    pub fn new(topic_id: String, partitions: Vec<TopicPartition>) -> Self {
        Self {
            topic_id,
            partitions,
        }
    }
}

impl types::Serialize for TopicResponse {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(Uuid::serialize(&self.topic_id));
        b.put(CompactArray::serialize(&mut self.partitions));
        b.put(TaggedFields::serialize()); // tag buffer
        b.freeze()
    }
}

pub struct BatchBytes {
    pub bytes: Bytes,
}

impl types::Serialize for BatchBytes {
    fn serialize(&mut self) -> Bytes {
        self.bytes.clone()
    }
}

pub struct TopicPartition {
    pub partition_index: u32,
    pub error_code: ErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub preferred_read_replica: i32,
    pub record_batches: Vec<BatchBytes>,
}

impl types::Serialize for TopicPartition {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32(self.partition_index);
        b.put(self.error_code.serialize());
        b.put_i64(self.high_watermark);
        b.put_i64(self.last_stable_offset);
        b.put_i64(self.log_start_offset);
        b.put(CompactArray::serialize(&mut self.aborted_transactions));
        b.put_i32(self.preferred_read_replica);
        b.put(CompactArray::serialize(&mut self.record_batches));
        b.put(TaggedFields::serialize()); // tag buffer
        b.freeze()
    }
}

#[allow(dead_code)]
pub struct AbortedTransaction {
    producer_id: u64,
    first_offset: u64,
}

impl types::Serialize for AbortedTransaction {
    fn serialize(&mut self) -> Bytes {
        todo!()
    }
}
