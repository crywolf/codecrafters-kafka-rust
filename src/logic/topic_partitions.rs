use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};

use crate::protocol::{
    request::describe_topic_partitions::DescribeTopicPartitionsRequestV0,
    response::describe_topic_partitions::{DescribeTopicPartitionsResponseV0, Partition, Topic},
    types::{self, CompactArray, CompactNullableBytes, CompactString, NullableBytes, Uuid, VarInt},
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

/// A record batch is the format that Kafka uses to store multiple records.
#[derive(Debug)]
#[allow(dead_code)]
struct RecordBatch {
    /// Base Offset is a 8-byte big-endian integer indicating the offset of the first record in this batch.
    base_offset: i64,
    /// Batch Length is a 4-byte big-endian integer indicating the length of the entire record batch in bytes.
    /// This value excludes the Base Offset (8 bytes) and the Batch Length (4 bytes) itself, but includes all other bytes in the record batch.
    batch_length: i32,
    /// Partition Leader Epoch is a 4-byte big-endian integer indicating the epoch of the leader for this partition.
    /// It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes.
    /// This value is used to detect out of order writes.
    partition_leader_epoch: i32,
    /// Magic Byte is a 1-byte big-endian integer indicating the version of the record batch format.
    /// This value is used to evolve the record batch format in a backward-compatible way.
    magic: i8, // (current magic value is 2)
    /// CRC is a 4-byte big-endian integer indicating the CRC32-C checksum of the record batch.
    /// The CRC is computed over the data following the CRC field to the end of the record batch. The CRC32-C (Castagnoli) polynomial is used for the computation.
    crc: u32,
    /// Attributes is a bitmask of the following flags:
    /// bit 0~2:
    /// 0: no compression
    /// 1: gzip
    /// 2: snappy
    /// 3: lz4
    /// 4: zstd
    /// bit 3: timestampType
    /// bit 4: isTransactional (0 means not transactional)
    /// bit 5: isControlBatch (0 means not a control batch)
    /// bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
    /// bit 7~15: unused
    attributes: i16,
    /// Last Offset Delta is a 4-byte big-endian integer indicating the difference between the last offset of this record batch and the base offset.
    last_offset_delta: i32,
    /// Base Timestamp is a 8-byte big-endian integer indicating the timestamp of the first record in this batch.
    base_timestamp: i64,
    /// Max Timestamp is a 8-byte big-endian integer indicating the maximum timestamp of the records in this batch.
    max_timestamp: i64,
    /// Producer ID is a 8-byte big-endian integer indicating the ID of the producer that produced the records in this batch.
    producer_id: i64, // In this case, the value is 0xffffffffffffffff, which is -1 in decimal. This is a special value that indicates that the producer ID is not set.
    /// Producer Epoch is a 2-byte big-endian integer indicating the epoch of the producer that produced the records in this batch.
    producer_epoch: i16, // In this case, the value is 0xffff, which is -1 in decimal. This is a special value that indicates that the producer epoch is not applicable.
    /// Base Sequence is a 4-byte big-endian integer indicating the sequence number of the first record in a batch.
    /// It is used to ensure the correct ordering and deduplication of messages produced by a Kafka producer.
    base_sequence: i32,

    records: Vec<Record>, // NULLABLE_BYTES
}

impl RecordBatch {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let base_offset = src.get_i64();
        let batch_length = src.get_i32();
        let partition_leader_epoch = src.get_i32();
        let magic = src.get_i8();
        let crc = src.get_u32();
        let attributes = src.get_i16();
        let last_offset_delta = src.get_i32();
        let base_timestamp = src.get_i64();
        let max_timestamp = src.get_i64();
        let producer_id = src.get_i64();
        let producer_epoch = src.get_i16();
        let base_sequence = src.get_i32();
        let records = NullableBytes::deserialize::<Record, RecordBatch>(src);

        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }
}

impl types::Deserialize<Record> for RecordBatch {
    fn deserialize(src: &mut Bytes) -> Record {
        Record::from_bytes(src)
    }
}

/// A record is the format that Kafka uses to store a single record.
#[derive(Debug)]
#[allow(dead_code)]
struct Record {
    /// Length is a signed variable size integer indicating the length of the record, the length is calculated from the attributes field to the end of the record.
    length: i64,
    /// Attributes is a 1-byte big-endian integer indicating the attributes of the record. Currently, this field is unused in the protocol.
    attributes: i8,
    /// Timestamp Delta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.
    timestamp_delta: i64,
    /// Offset Delta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.
    offset_delta: i64,
    /// Key is a byte array indicating the key of the record.
    key: Vec<u8>,
    /// Value Length is a signed variable size integer indicating the length of the value of the record.
    value_length: i64,
    /// Value is a byte array indicating the value of the record.
    value: RecordValue,
    headers: Vec<Header>,
}

impl Record {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        let length = VarInt::deserialize(src);
        let attributes = src.get_i8();
        let timestamp_delta = VarInt::deserialize(src);
        let offset_delta = VarInt::deserialize(src);
        let key = CompactNullableBytes::deserialize(src);
        let value_length = VarInt::deserialize(src);
        let value = RecordValue::from_bytes(src);
        let headers = CompactArray::deserialize::<Header, Record>(src);

        Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_length,
            value,
            headers,
        }
    }
}

impl types::Deserialize<Header> for Record {
    fn deserialize(_src: &mut Bytes) -> Header {
        // we assume that headers array is empty, so this would not be called
        Header
    }
}

#[derive(Debug)]
struct Header;

#[derive(Debug)]
#[allow(dead_code)]
enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

#[derive(Debug)]
pub struct TopicValue {
    topic_name: String,
    topic_id: String, // UUID
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PartitionValue {
    partition_id: u32,
    topic_id: String,
    replicas: Vec<u32>,
    /// The in-sync replicas of this partition
    in_sync_replicas: Vec<u32>,
    /// The replicas that we are in the process of removing
    removing_replicas: Vec<u32>,
    adding_replicas: Vec<u32>,
    leader_id: u32,
    leader_epoch: u32,
    partition_epoch: u32,
    directories: Vec<String>,
}

impl types::Deserialize<u32> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> u32 {
        src.get_u32()
    }
}

impl types::Deserialize<String> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> String {
        Uuid::deserialize(src)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FeatureLevelValue {
    name: String,
    level: u16,
}

impl RecordValue {
    pub fn from_bytes(src: &mut Bytes) -> Self {
        // Frame Version is indicating the version of the format of the record.
        let frame_version = src.get_u8();
        assert_eq!(frame_version, 1);

        let record_type = src.get_u8();
        match record_type {
            2 => {
                // Topic Record Value
                let version = src.get_u8();
                assert_eq!(version, 0);
                let topic_name = CompactString::deserialize(src);
                let topic_id = Uuid::deserialize(src);

                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);
                RecordValue::Topic(TopicValue {
                    topic_name,
                    topic_id,
                })
            }
            3 => {
                // Partition Record Value
                let version = src.get_u8();
                assert_eq!(version, 1);
                let partition_id = src.get_u32();
                let topic_id = Uuid::deserialize(src);

                let replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let in_sync_replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let removing_replicas = CompactArray::deserialize::<_, PartitionValue>(src);
                let adding_replicas = CompactArray::deserialize::<_, PartitionValue>(src);

                let leader_id = src.get_u32();
                let leader_epoch = src.get_u32();
                let partition_epoch = src.get_u32();

                let directories = CompactArray::deserialize::<String, PartitionValue>(src);

                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);

                RecordValue::Partition(PartitionValue {
                    partition_id,
                    topic_id,
                    replicas,
                    in_sync_replicas,
                    removing_replicas,
                    adding_replicas,
                    leader_id,
                    leader_epoch,
                    partition_epoch,
                    directories,
                })
            }

            12 => {
                // Feature Level Record Value
                let version = src.get_u8();
                assert_eq!(version, 0);
                let name = CompactString::deserialize(src);
                let level = src.get_u16();
                let tagged_fields_count = VarInt::deserialize(src);
                assert_eq!(tagged_fields_count, 0);
                RecordValue::FeatureLevel(FeatureLevelValue { name, level })
            }

            _ => unimplemented!(),
        }
    }
}
