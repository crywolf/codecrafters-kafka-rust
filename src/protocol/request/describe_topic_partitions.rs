use bytes::{Buf, Bytes};

use super::HeaderV2;
use crate::protocol::types::{self, CompactArray, CompactString, TaggedFields};

#[allow(dead_code)]
pub struct DescribeTopicPartitionsRequestV0 {
    pub header: HeaderV2,
    pub topics: Vec<String>,
    response_partition_limit: i32,
    cursor: u8,
}

impl DescribeTopicPartitionsRequestV0 {
    // https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let header = HeaderV2::from_bytes(src);

        let topics = CompactArray::deserialize::<_, Topic>(src);
        let response_partition_limit = src.get_i32();
        let cursor = src.get_u8(); // A nullable field that can be used for pagination. Here, it is 0xff, indicating a null value
        _ = TaggedFields::deserialize(src); // tag buffer

        Self {
            header,
            topics,
            response_partition_limit,
            cursor,
        }
    }
}

struct Topic;

impl types::Deserialize<String> for Topic {
    fn deserialize(src: &mut Bytes) -> String {
        let s = CompactString::deserialize(src);
        _ = TaggedFields::deserialize(src); // tag buffer
        s
    }
}
