use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes};

use crate::protocol::{
    response::describe_topic_partitions::{DescribeTopicPartitionsResponseV0, Topic},
    types::{CompactArray, CompactString},
    ErrorCode,
};

use super::HeaderV2;

#[derive(Debug)]
#[allow(dead_code)]
pub struct DescribeTopicPartitionsRequestV0 {
    header: HeaderV2,
    topics: Vec<String>,
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

    pub fn process(self) -> Result<DescribeTopicPartitionsResponseV0> {
        let topic_name = self.topics.first().ok_or(anyhow!("missing topic name"))?;
        let topic_id = "00000000-0000-0000-0000-000000000000".to_string(); // UUID: 00000000-0000-0000-0000-000000000000
        let partitions = Vec::new();

        let topic_authorized_operations = 0x0DF;
        /*
        Here, the value is 0x00000df8, which is the following in binary 0000 1101 1111 1000
        This corresponds to the following operations:
            READ (bit index 3 from the right)
            WRITE (bit index 4 from the right)
            CREATE (bit index 5 from the right)
            DELETE (bit index 6 from the right)
            ALTER (bit index 7 from the right)
            DESCRIBE (bit index 8 from the right)
            DESCRIBE_CONFIGS (bit index 10 from the right)
            ALTER_CONFIGS (bit index 11 from the right)
            The full list of operations can be found here:
            https://github.com/apache/kafka/blob/1962917436f463541f9bb63791b7ed55c23ce8c1/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java#L44
        */

        let topic = Topic {
            error_code: ErrorCode::UnknownTopicOrPartition,
            name: topic_name.to_string(),
            topic_id,
            is_internal: false,
            partitions,
            topic_authorized_operations,
        };
        let topics = vec![topic];

        Ok(DescribeTopicPartitionsResponseV0::new(
            self.header.correlation_id,
            topics,
        ))
    }
}
