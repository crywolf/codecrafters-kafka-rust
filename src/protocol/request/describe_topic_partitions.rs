use bytes::{Buf, Bytes};

use super::HeaderV2;
use crate::protocol::types::{CompactArray, CompactString};

#[derive(Debug)]
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

        let topics = CompactArray::deserialize::<_, CompactString>(src);
        let response_partition_limit = src.get_i32();
        let cursor = src.get_u8(); // A nullable field that can be used for pagination. Here, it is 0xff, indicating a null value
        _ = src.get_u8(); // tag buffer

        Self {
            header,
            topics,
            response_partition_limit,
            cursor,
        }
    }
}
