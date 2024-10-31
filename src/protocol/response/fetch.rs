use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::{
    self,
    types::{self, CompactArray, CompactNullableBytes, Uuid},
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
        self.bytes.put_i16(self.error_code.into());
        self.bytes.put_u32(self.session_id);
        self.bytes.put(CompactArray::serialize(&mut self.responses));
        self.bytes.put_u8(0); // tag buffer
    }
}

impl protocol::Response for FetchResponseV16 {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[allow(dead_code)]
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
        b.put_u8(0); // tag buffer
        b.freeze()
    }
}

#[allow(dead_code)]
pub struct TopicPartition {
    pub partition_index: u32,
    pub error_code: ErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub preferred_read_replica: i32,
    pub records: Vec<Record>, // NULLABLE_BYTES
}

impl types::Serialize for TopicPartition {
    fn serialize(&mut self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32(self.partition_index);
        b.put_i16(self.error_code.into());
        b.put_i64(self.high_watermark);
        b.put_i64(self.last_stable_offset);
        b.put_i64(self.log_start_offset);
        b.put(CompactArray::serialize(&mut self.aborted_transactions));
        b.put_i32(self.preferred_read_replica);
        b.put(CompactNullableBytes::serialize(&mut self.records));
        b.put_u8(0); // tag buffer
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
        //b.put_u8(0); // tag buffer
        todo!()
    }
}

pub struct Record;

impl types::Serialize for Record {
    fn serialize(&mut self) -> Bytes {
        Bytes::new()
    }
}
