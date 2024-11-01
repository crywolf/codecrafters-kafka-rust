use anyhow::Result;
use bytes::{Buf, BytesMut};

use crate::protocol::{
    record_batch::{RecordBatch, RecordValue},
    request::describe_topic_partitions::DescribeTopicPartitionsRequestV0,
    response::describe_topic_partitions::{DescribeTopicPartitionsResponseV0, Partition, Topic},
    ErrorCode,
};

const DEFAULT_UNKNOWN_TOPIC_UUID: &str = "00000000-0000-0000-0000-000000000000";

/// https://kafka.apache.org/documentation/#log
const CLUSTER_METADATA_LOG_FILE: &str =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

pub fn process(req: DescribeTopicPartitionsRequestV0) -> Result<DescribeTopicPartitionsResponseV0> {
    let file_bytes = std::fs::read(CLUSTER_METADATA_LOG_FILE)?;

    let mut data = BytesMut::with_capacity(file_bytes.len());
    data.extend_from_slice(&file_bytes);
    let mut data = data.freeze();

    // default response UUID
    let mut topic_id = DEFAULT_UNKNOWN_TOPIC_UUID.to_string();
    // default error response
    let mut topic_error_code = ErrorCode::UnknownTopicOrPartition;

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

    let mut topics = Vec::new();

    while data.remaining() > 0 {
        let record_batch = RecordBatch::from_bytes(&mut data);

        for topic_name in &req.topics {
            topic_id = DEFAULT_UNKNOWN_TOPIC_UUID.to_string();
            let mut partitions = Vec::new();

            // find topic id and partition info in the records
            for rec in &record_batch.records {
                let record_type = &rec.value;
                if let Some(id) = match record_type {
                    RecordValue::Topic(ref topic) if topic.topic_name == *topic_name => {
                        Some(topic.topic_id.clone())
                    }
                    _ => None,
                } {
                    topic_id = id;
                    topic_error_code = ErrorCode::None;
                };

                match record_type {
                    RecordValue::Partition(p) if p.topic_id == topic_id => {
                        partitions.push(Partition::new(
                            ErrorCode::None,
                            p.partition_id,
                            p.leader_id,
                            p.leader_epoch,
                            p.replicas.clone(),
                            p.in_sync_replicas.clone(),
                            p.adding_replicas.clone(),
                            Vec::new(),
                            p.removing_replicas.clone(),
                        ));
                    }
                    _ => {}
                }
            }

            if !partitions.is_empty() {
                let topic = Topic {
                    error_code: topic_error_code,
                    name: topic_name.to_string(),
                    topic_id: topic_id.clone(),
                    is_internal: false,
                    partitions,
                    topic_authorized_operations,
                };
                topics.push(topic);
            }
        }
    }

    for requested_topic in req.topics {
        let mut topic_found = false;
        for topic in &topics {
            if topic.name == requested_topic {
                topic_found = true;
            }
        }
        if !topic_found {
            let error_topic = Topic {
                error_code: ErrorCode::UnknownTopicOrPartition,
                name: requested_topic.to_string(),
                topic_id: topic_id.clone(),
                is_internal: false,
                partitions: Vec::new(),
                topic_authorized_operations,
            };
            topics.push(error_topic);
        }
    }

    Ok(DescribeTopicPartitionsResponseV0::new(
        req.header.correlation_id,
        topics,
    ))
}
