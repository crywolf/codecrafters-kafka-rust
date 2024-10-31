use std::vec;

use bytes::{Buf, Bytes};

use crate::protocol::{
    response::fetch::{FetchResponseV16, TopicPartition, TopicResponse},
    types::{self, CompactArray, CompactString, Uuid},
    ErrorCode,
};

use super::HeaderV2;

#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchRequestV16 {
    pub header: HeaderV2,
    /// The maximum time in milliseconds to wait for the response.
    max_wait_ms: u32,
    /// The minimum bytes to accumulate in the response.
    min_bytes: u32,
    /// The maximum bytes to fetch.
    max_bytes: u32,
    isolation_level: u8,
    /// The fetch session ID.
    session_id: u32,
    /// The fetch session epoch, which is used for ordering requests in a session.
    session_epoch: u32,
    /// The topics to fetch.
    topics: Vec<TopicRequest>,
    /// In an incremental fetch request, the partitions to remove.
    forgotten_topics_data: Vec<ForgottenTopicData>,
    rack_id: String,
}

impl FetchRequestV16 {
    // https://kafka.apache.org/protocol.html#The_Messages_Fetch
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let header = HeaderV2::from_bytes(src);

        let max_wait_ms = src.get_u32();
        let min_bytes = src.get_u32();
        let max_bytes = src.get_u32();
        let isolation_level = src.get_u8();
        let session_id = src.get_u32();
        let session_epoch = src.get_u32();
        let topics = CompactArray::deserialize::<TopicRequest, Self>(src);
        let forgotten_topics_data = CompactArray::deserialize::<ForgottenTopicData, Self>(src);
        let rack_id = CompactString::deserialize(src);
        _ = src.get_u8(); // tag buffer

        Self {
            header,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        }
    }

    pub fn process(self) -> FetchResponseV16 {
        if self.topics.is_empty() {
            let responses = vec![];
            return FetchResponseV16::new(self.header.correlation_id, self.session_id, responses);
        };

        let responses = if let Some(topic) = self.topics.first() {
            // topic does not exist
            let error_code = ErrorCode::UnknownTopicId;

            let topic_id = topic.topic_id.clone();
            let partition = TopicPartition {
                partition_index: 0,
                error_code,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: Vec::new(),
                preferred_read_replica: 0,
                records: Vec::new(),
            };
            let partitions = vec![partition];
            let topic_response = TopicResponse::new(topic_id, partitions);
            vec![topic_response]
        } else {
            vec![]
        };

        FetchResponseV16::new(self.header.correlation_id, self.session_id, responses)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct TopicRequest {
    topic_id: String,
    partitions: Vec<Partition>,
}

impl types::Deserialize<TopicRequest> for FetchRequestV16 {
    fn deserialize(src: &mut Bytes) -> TopicRequest {
        let topic_id = Uuid::deserialize(src);
        let partitions = CompactArray::deserialize::<Partition, TopicRequest>(src);
        _ = src.get_u8(); // tag buffer
        TopicRequest {
            topic_id,
            partitions,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct ForgottenTopicData {
    topic_id: String,     // UUID
    partitions: Vec<u32>, // The partitions indexes to forget.
}

impl types::Deserialize<ForgottenTopicData> for FetchRequestV16 {
    fn deserialize(src: &mut Bytes) -> ForgottenTopicData {
        let ftd = ForgottenTopicData {
            topic_id: Uuid::deserialize(src),
            partitions: CompactArray::deserialize::<u32, ForgottenTopicData>(src),
        };
        _ = src.get_u8(); // tag buffer
        ftd
    }
}

impl types::Deserialize<u32> for ForgottenTopicData {
    fn deserialize(src: &mut Bytes) -> u32 {
        src.get_u32()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct Partition {
    partition: u32,
    current_leader_epoch: u32,
    fetch_offset: u64,
    last_fetched_epoch: u32,
    log_start_offset: u64,
    partition_max_bytes: u32,
}

impl types::Deserialize<Partition> for TopicRequest {
    fn deserialize(src: &mut Bytes) -> Partition {
        let p = Partition {
            partition: src.get_u32(),
            current_leader_epoch: src.get_u32(),
            fetch_offset: src.get_u64(),
            last_fetched_epoch: src.get_u32(),
            log_start_offset: src.get_u64(),
            partition_max_bytes: src.get_u32(),
        };
        _ = src.get_u8(); // tag buffer
        p
    }
}
